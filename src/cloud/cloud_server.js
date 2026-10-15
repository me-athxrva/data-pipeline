/**
 * src/cloud/cloud_server.js
 *
 * Cloud Service for MayaCardio pipeline with PostgreSQL support.
 *
 * Supports:
 * - PostgreSQL backend when DATABASE_URL is set (e.g. Supabase, Neon, Render, Railway)
 * - Local file WAL backend when DATABASE_URL is omitted (local dev / tests)
 * - Endpoints: /health, /ingest, /watermark, /export, /results, /count, /sync_losses
 * - Idempotent sample deduplication
 * - Near real-time (< 5s) cloud window processing in online mode
 */

'use strict';

require('../common/env');
const http = require('node:http');
const path = require('node:path');
const { SessionStorage } = require('../common/storage');
const { PostgresStorage } = require('./postgres_storage');
const { HeartRateDetector, processWindow } = require('../common/processing');

const CLOUD_DATA_DIR = path.resolve(process.env.CLOUD_DATA_DIR || path.join(__dirname, '..', '..', 'data', 'cloud'));
const PORT = parseInt(process.env.CLOUD_PORT || '8080', 10);
const HOST = process.env.CLOUD_HOST || '0.0.0.0';

class CloudService {
  constructor(port = PORT, host = HOST, dataDir = CLOUD_DATA_DIR, databaseUrl = process.env.DATABASE_URL) {
    this.port = port;
    this.host = host;
    this.databaseUrl = databaseUrl;

    if (this.databaseUrl) {
      console.log(`[cloud-service] Connecting to PostgreSQL database...`);
      this.storage = new PostgresStorage(this.databaseUrl);
      this.isPostgres = true;
    } else {
      this.storage = new SessionStorage(dataDir);
      this.isPostgres = false;
    }

    this.server = null;
    this.sessionProcessors = new Map();
    this.simulateNetworkDown = false;
  }

  async getProcessor(sessionId) {
    if (!this.sessionProcessors.has(sessionId)) {
      let lastW = -1;
      let s1 = new Map();
      let s2 = new Map();

      try {
        const existingResults = await this.storage.loadResults(sessionId);
        for (const r of existingResults) {
          if (r.window > lastW) lastW = r.window;
        }
        // Pre-load existing samples from storage into memory maps
        s1 = await this.storage.loadSensorSamples(sessionId, 1);
        s2 = await this.storage.loadSensorSamples(sessionId, 2);
      } catch (_) {}

      this.sessionProcessors.set(sessionId, {
        s1Map: s1,
        s2Map: s2,
        hrDetector: new HeartRateDetector(),
        lastProcessedWindow: lastW
      });
    }
    return this.sessionProcessors.get(sessionId);
  }

  async processAvailableWindows(sessionId) {
    const proc = await this.getProcessor(sessionId);
    if (proc.s1Map.size === 0 || proc.s2Map.size === 0) return;

    let maxS1 = -1;
    for (const k of proc.s1Map.keys()) {
      if (k > maxS1) maxS1 = k;
    }
    let maxS2 = -1;
    for (const k of proc.s2Map.keys()) {
      if (k > maxS2) maxS2 = k;
    }
    if (maxS1 < 0 || maxS2 < 0) return;

    const maxSample = Math.min(maxS1, maxS2);
    const maxWindow = Math.floor(maxSample / 400);

    for (let w = proc.lastProcessedWindow + 1; w < maxWindow; w++) {
      const result = processWindow({
        windowIndex: w,
        sensor1Map: proc.s1Map,
        sensor2Map: proc.s2Map,
        hrDetector: proc.hrDetector,
        mode: 'online'
      });

      await this.storage.saveResult(sessionId, result);
      proc.lastProcessedWindow = w;

      // Safely prune in-memory samples older than 5s (2000 samples)
      // to keep memory bounded and fast at scale
      if (w >= 6) {
        const pruneThreshold = 400 * (w - 5);
        for (const k of proc.s1Map.keys()) {
          if (k < pruneThreshold) proc.s1Map.delete(k);
        }
        for (const k of proc.s2Map.keys()) {
          if (k < pruneThreshold) proc.s2Map.delete(k);
        }
      }
    }
  }

  async handleRequest(req, res) {
    const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);

    // CORS headers
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

    if (req.method === 'OPTIONS') {
      res.writeHead(204);
      res.end();
      return;
    }

    if (req.method === 'POST' && url.pathname === '/simulate/network_down') {
      let body = '';
      req.on('data', chunk => { body += chunk.toString('utf8'); });
      req.on('end', () => {
        let ms = 4500;
        try {
          const parsed = JSON.parse(body);
          if (parsed.durationMs) ms = parsed.durationMs;
        } catch (_) {}
        this.simulateNetworkDown = true;
        setTimeout(() => {
          this.simulateNetworkDown = false;
        }, ms);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: 'simulating_outage', durationMs: ms }));
      });
      return;
    }

    if (this.simulateNetworkDown) {
      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Service Unavailable (Simulated Network Outage)' }));
      return;
    }

    if (req.method === 'GET' && url.pathname === '/health') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        status: 'ok',
        storage: this.isPostgres ? 'postgresql' : 'file',
        timestamp: Date.now()
      }));
      return;
    }

    if (req.method === 'GET' && url.pathname === '/watermark') {
      const sessionId = url.searchParams.get('session_id');
      const sensor = parseInt(url.searchParams.get('sensor') || '1', 10);
      if (!sessionId) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Missing session_id' }));
        return;
      }

      if (this.isPostgres) {
        const wm = await this.storage.getWatermark(sessionId, sensor);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(wm));
      } else {
        const proc = await this.getProcessor(sessionId);
        const map = sensor === 1 ? proc.s1Map : proc.s2Map;
        let contiguous = -1;
        while (map.has(contiguous + 1)) {
          contiguous++;
        }
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ acknowledged_index: contiguous, max_index: contiguous }));
      }
      return;
    }

    if (req.method === 'POST' && url.pathname === '/ingest') {
      let body = '';
      req.on('data', chunk => { body += chunk.toString('utf8'); });
      req.on('end', async () => {
        try {
          const payload = JSON.parse(body);
          const { session_id, sensor, first_index, count, data, lost } = payload;

          if (!session_id || !sensor || !Array.isArray(data)) {
            res.writeHead(400, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: 'Invalid payload' }));
            return;
          }

          // Save to PostgreSQL or File WAL
          await this.storage.saveSamples(session_id, sensor, first_index, count, data);

          // Update in-memory map for fast window calculations
          const proc = await this.getProcessor(session_id);
          const targetMap = sensor === 1 ? proc.s1Map : proc.s2Map;
          for (let i = 0; i < count; i++) {
            targetMap.set(first_index + i, data[i]);
          }

          // Record lost ranges
          if (Array.isArray(lost)) {
            for (const item of lost) {
              const fromIdx = item.from_index !== undefined ? item.from_index : item.start_index;
              const toIdx = item.to_index !== undefined ? item.to_index : item.end_index;
              await this.storage.recordLoss(session_id, sensor, fromIdx, toIdx, item.reason || 'network_loss');
            }
          }

          // Trigger online window processing
          await this.processAvailableWindows(session_id);

          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({
            status: 'accepted',
            storage: this.isPostgres ? 'postgresql' : 'file',
            acknowledged_index: first_index + count - 1
          }));
        } catch (err) {
          res.writeHead(500, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: err.message }));
        }
      });
      return;
    }

    if (req.method === 'POST' && url.pathname === '/sync_losses') {
      let body = '';
      req.on('data', chunk => { body += chunk.toString('utf8'); });
      req.on('end', async () => {
        try {
          const { session_id, losses } = JSON.parse(body);
          if (session_id && Array.isArray(losses)) {
            for (const l of losses) {
              await this.storage.recordLoss(session_id, l.sensor, l.from_index, l.to_index, l.reason);
            }
          }
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ status: 'losses_synced' }));
        } catch (err) {
          res.writeHead(500, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: err.message }));
        }
      });
      return;
    }

    if (req.method === 'GET' && url.pathname === '/export') {
      const sessionId = url.searchParams.get('session');
      if (!sessionId) {
        res.writeHead(400, { 'Content-Type': 'text/plain' });
        res.end('Missing ?session=ID parameter');
        return;
      }

      res.writeHead(200, { 'Content-Type': 'application/x-ndjson' });
      await this.storage.exportSession(sessionId, (line) => {
        res.write(line + '\n');
      });
      res.end();
      return;
    }

    if (req.method === 'GET' && url.pathname === '/results') {
      const sessionId = url.searchParams.get('session');
      if (!sessionId) {
        res.writeHead(400, { 'Content-Type': 'text/plain' });
        res.end('Missing ?session=ID parameter');
        return;
      }

      const results = await this.storage.loadResults(sessionId);
      res.writeHead(200, { 'Content-Type': 'application/x-ndjson' });
      for (const r of results) {
        res.write(JSON.stringify(r) + '\n');
      }
      res.end();
      return;
    }

    if (req.method === 'GET' && url.pathname === '/count') {
      const sessionId = url.searchParams.get('session');
      const count = sessionId ? await this.storage.getSavedSampleCount(sessionId) : 0;
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ session: sessionId, count, storage: this.isPostgres ? 'postgresql' : 'file' }));
      return;
    }

    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Not Found' }));
  }

  start(callback) {
    this.server = http.createServer((req, res) => this.handleRequest(req, res));
    this.server.listen(this.port, this.host, () => {
      console.log(`[cloud-service] Listening on http://${this.host}:${this.port} (Backend: ${this.isPostgres ? 'PostgreSQL' : 'File WAL'})`);
      if (typeof callback === 'function') callback();
    });
  }

  stop(callback) {
    if (this.server) {
      this.server.close(async () => {
        if (this.storage.close) {
          await this.storage.close();
        }
        if (typeof callback === 'function') callback();
      });
    } else if (typeof callback === 'function') {
      callback();
    }
  }
}

// Direct CLI invocation
if (require.main === module) {
  const service = new CloudService();
  service.start();

  const shutdown = () => {
    console.log('\n[cloud-service] Shutting down...');
    service.stop(() => process.exit(0));
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

module.exports = { CloudService };
