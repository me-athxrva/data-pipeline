/**
 * src/cloud/postgres_storage.js
 *
 * PostgreSQL Cloud Storage Adapter for MayaCardio pipeline.
 *
 * Compatible with any standard PostgreSQL instance, including free-tier cloud databases:
 * - Supabase (free tier)
 * - Neon.tech (free tier serverless Postgres)
 * - Render / Railway (free PostgreSQL)
 * - Local PostgreSQL
 *
 * Features:
 * - Auto-initializes schema with primary key deduplication
 * - Fast batch insertion with ON CONFLICT DO NOTHING
 * - Watermark tracking for catch-up synchronization
 * - Streaming export compliant with `pipeline export --from cloud`
 */

'use strict';

const { Pool } = require('pg');

class PostgresStorage {
  constructor(connectionString = process.env.DATABASE_URL) {
    this.connectionString = connectionString;
    const isLocalhost = connectionString && (connectionString.includes('localhost') || connectionString.includes('127.0.0.1'));
    const sslDisabled = connectionString && connectionString.includes('sslmode=disable');
    const sslRequired = connectionString && connectionString.includes('sslmode=require');
    
    let ssl = false;
    if (sslRequired || (!isLocalhost && !sslDisabled)) {
      ssl = { rejectUnauthorized: false };
    }

    this.pool = new Pool({
      connectionString,
      ssl,
      max: 10,
      idleTimeoutMillis: 30000
    });
    this.initialized = false;
  }

  async initSchema() {
    if (this.initialized) return;

    const client = await this.pool.connect();
    try {
      await client.query(`
        CREATE TABLE IF NOT EXISTS samples (
          session_id VARCHAR(64) NOT NULL,
          sensor SMALLINT NOT NULL,
          sample_index INT NOT NULL,
          values INT[] NOT NULL,
          created_at TIMESTAMPTZ DEFAULT NOW(),
          PRIMARY KEY (session_id, sensor, sample_index)
        );

        CREATE TABLE IF NOT EXISTS losses (
          id SERIAL PRIMARY KEY,
          session_id VARCHAR(64) NOT NULL,
          sensor SMALLINT NOT NULL,
          from_index INT NOT NULL,
          to_index INT,
          reason TEXT,
          created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS window_results (
          session_id VARCHAR(64) NOT NULL,
          window_index INT NOT NULL,
          missing_1 INT NOT NULL,
          missing_2 INT NOT NULL,
          complete BOOLEAN NOT NULL,
          mean_1 REAL[],
          min_1 INT[],
          max_1 INT[],
          mean_2 REAL[],
          min_2 INT[],
          max_2 INT[],
          magnitude REAL,
          heart_rate REAL,
          mode VARCHAR(16) NOT NULL,
          created_at TIMESTAMPTZ DEFAULT NOW(),
          PRIMARY KEY (session_id, window_index, mode)
        );

        CREATE INDEX IF NOT EXISTS idx_samples_lookup ON samples (session_id, sensor, sample_index);
        CREATE INDEX IF NOT EXISTS idx_losses_lookup ON losses (session_id);
        CREATE INDEX IF NOT EXISTS idx_results_lookup ON window_results (session_id, window_index);
      `);
      this.initialized = true;
    } finally {
      client.release();
    }
  }

  /**
   * Save a batch of samples with ON CONFLICT DO NOTHING (idempotent deduplication).
   */
  async saveSamples(sessionId, sensor, firstIndex, count, data) {
    if (count === 0 || !data || data.length === 0) return;
    await this.initSchema();

    // Construct multi-row parameterized insert
    const rows = [];
    const params = [sessionId, sensor];
    let paramIdx = 3;

    for (let i = 0; i < count; i++) {
      const sampleIdx = firstIndex + i;
      rows.push(`($1, $2, $${paramIdx}, $${paramIdx + 1})`);
      params.push(sampleIdx, data[i]);
      paramIdx += 2;
    }

    const query = `
      INSERT INTO samples (session_id, sensor, sample_index, values)
      VALUES ${rows.join(', ')}
      ON CONFLICT (session_id, sensor, sample_index) DO NOTHING;
    `;

    await this.pool.query(query, params);
  }

  /**
   * Record a lost range.
   */
  async recordLoss(sessionId, sensor, fromIndex, toIndex, reason) {
    await this.initSchema();
    const query = `
      INSERT INTO losses (session_id, sensor, from_index, to_index, reason)
      VALUES ($1, $2, $3, $4, $5);
    `;
    await this.pool.query(query, [sessionId, sensor, fromIndex, toIndex, reason]);
  }

  /**
   * Save 1-second window calculation result.
   */
  async saveResult(sessionId, result) {
    await this.initSchema();
    const query = `
      INSERT INTO window_results (
        session_id, window_index, missing_1, missing_2, complete,
        mean_1, min_1, max_1, mean_2, min_2, max_2,
        magnitude, heart_rate, mode
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
      ON CONFLICT (session_id, window_index, mode) DO UPDATE SET
        missing_1 = EXCLUDED.missing_1,
        missing_2 = EXCLUDED.missing_2,
        complete = EXCLUDED.complete,
        mean_1 = EXCLUDED.mean_1,
        min_1 = EXCLUDED.min_1,
        max_1 = EXCLUDED.max_1,
        mean_2 = EXCLUDED.mean_2,
        min_2 = EXCLUDED.min_2,
        max_2 = EXCLUDED.max_2,
        magnitude = EXCLUDED.magnitude,
        heart_rate = EXCLUDED.heart_rate;
    `;

    await this.pool.query(query, [
      sessionId,
      result.window,
      result.missing_1,
      result.missing_2,
      result.complete,
      result.mean_1,
      result.min_1,
      result.max_1,
      result.mean_2,
      result.min_2,
      result.max_2,
      result.magnitude,
      result.heart_rate,
      result.mode
    ]);
  }

  /**
   * Get the maximum sample index in cloud for a sensor.
   */
  async getWatermark(sessionId, sensor) {
    await this.initSchema();
    const res = await this.pool.query(`
      SELECT COALESCE(MAX(sample_index), -1) AS max_index
      FROM samples
      WHERE session_id = $1 AND sensor = $2;
    `, [sessionId, sensor]);

    const maxIdx = parseInt(res.rows[0].max_index, 10);
    return { acknowledged_index: maxIdx, max_index: maxIdx };
  }

  /**
   * Load samples for a sensor into a Map (for window processing).
   */
  async loadSensorSamples(sessionId, sensor) {
    await this.initSchema();
    const res = await this.pool.query(`
      SELECT sample_index, values
      FROM samples
      WHERE session_id = $1 AND sensor = $2
      ORDER BY sample_index ASC;
    `, [sessionId, sensor]);

    const map = new Map();
    for (const row of res.rows) {
      map.set(row.sample_index, row.values);
    }
    return map;
  }

  /**
   * Load recorded losses for a session.
   */
  async loadLosses(sessionId) {
    await this.initSchema();
    const res = await this.pool.query(`
      SELECT sensor, from_index, to_index, reason
      FROM losses
      WHERE session_id = $1
      ORDER BY id ASC;
    `, [sessionId]);
    return res.rows;
  }

  /**
   * Load window results for a session.
   */
  async loadResults(sessionId) {
    await this.initSchema();
    const res = await this.pool.query(`
      SELECT
        window_index AS window,
        missing_1,
        missing_2,
        complete,
        mean_1,
        min_1,
        max_1,
        mean_2,
        min_2,
        max_2,
        magnitude,
        heart_rate,
        mode
      FROM window_results
      WHERE session_id = $1
      ORDER BY window_index ASC;
    `, [sessionId]);
    return res.rows;
  }

  /**
   * Export all data matching `pipeline export --from cloud`.
   */
  async exportSession(sessionId, onLine) {
    await this.initSchema();

    // Stream Sensor 1
    const s1Res = await this.pool.query(`
      SELECT 1 AS sensor, sample_index AS index, values
      FROM samples
      WHERE session_id = $1 AND sensor = 1
      ORDER BY sample_index ASC;
    `, [sessionId]);
    for (const r of s1Res.rows) {
      onLine(JSON.stringify(r));
    }

    // Stream Sensor 2
    const s2Res = await this.pool.query(`
      SELECT 2 AS sensor, sample_index AS index, values
      FROM samples
      WHERE session_id = $1 AND sensor = 2
      ORDER BY sample_index ASC;
    `, [sessionId]);
    for (const r of s2Res.rows) {
      onLine(JSON.stringify(r));
    }

    // Stream Losses
    const losses = await this.loadLosses(sessionId);
    for (const l of losses) {
      onLine(JSON.stringify(l));
    }
  }

  /**
   * Count total unique saved samples in the database for a session.
   */
  async getSavedSampleCount(sessionId) {
    await this.initSchema();
    const res = await this.pool.query(`
      SELECT COUNT(*) AS total
      FROM samples
      WHERE session_id = $1;
    `, [sessionId]);
    return parseInt(res.rows[0].total, 10);
  }

  async close() {
    await this.pool.end();
  }
}

module.exports = { PostgresStorage };
