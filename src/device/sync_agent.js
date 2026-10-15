/**
 * src/device/sync_agent.js
 *
 * Continuous Stream & Backlog Synchronizer.
 *
 * Guarantees:
 * - Strict in-order sample transmission to cloud
 * - Seamless automatic catch-up after network outages
 * - Zero race conditions between live streaming and historical backlog
 * - Accurate cloud watermark and backlog tracking for `pipeline status`
 */

'use strict';

const http = require('node:http');

class SyncAgent {
  constructor(cloudUrl = 'http://127.0.0.1:8080', storage, networkMonitor = null) {
    this.cloudUrl = cloudUrl;
    this.storage = storage;
    this.networkMonitor = networkMonitor;
    this.cloudWatermarks = { 1: -1, 2: -1 };
    this.activeSessionId = null;
    this.isOnline = false;
    this.syncTimer = null;
    this.syncing = false;
  }

  setSession(sessionId) {
    if (this.activeSessionId !== sessionId) {
      this.activeSessionId = sessionId;
      this.cloudWatermarks = { 1: -1, 2: -1 };
    }
  }

  setOnline(online) {
    this.isOnline = online;
    if (online) {
      this.startSyncLoop();
    } else {
      this.stopSyncLoop();
    }
  }

  startSyncLoop() {
    if (this.syncTimer) return;
    // Fast periodic drain: drains live and backlogged samples continuously
    this.syncTimer = setInterval(() => {
      this.drainBacklog();
    }, 50);
  }

  stopSyncLoop() {
    if (this.syncTimer) {
      clearInterval(this.syncTimer);
      this.syncTimer = null;
    }
  }

  /**
   * Drain un-synced samples from local disk storage to the cloud in strict order.
   */
  async drainBacklog() {
    if (this.syncing || !this.isOnline || !this.activeSessionId) return;
    this.syncing = true;

    try {
      // Step 1: Initialize watermarks from cloud if needed
      if (this.cloudWatermarks[1] < 0 || this.cloudWatermarks[2] < 0) {
        await this.refreshCloudWatermarks();
      }

      // Step 2: Sync Sensor 1 and Sensor 2
      for (const sensor of [1, 2]) {
        await this.drainSensor(sensor);
      }

      // Step 3: Sync any un-synced loss records
      await this.syncLosses();
    } catch (_) {
      // Re-tries on next tick
    } finally {
      this.syncing = false;
    }
  }

  async drainSensor(sensor) {
    const currentWatermark = this.cloudWatermarks[sensor];
    const unSyncedSamples = this.storage.loadSensorSamplesAfter(this.activeSessionId, sensor, currentWatermark, 4000);
    if (!unSyncedSamples || unSyncedSamples.length === 0) return;

    // Send in chunks of up to 400 samples (1 second of 400 Hz data)
    const BATCH_SIZE = 400;
    for (let i = 0; i < unSyncedSamples.length; i += BATCH_SIZE) {
      if (!this.isOnline) break;

      const chunk = unSyncedSamples.slice(i, i + BATCH_SIZE);
      const firstIdx = chunk[0].index;
      const count = chunk.length;
      const data = chunk.map(item => item.values);

      const success = await this.postBatch(sensor, firstIdx, count, data);
      if (success) {
        this.cloudWatermarks[sensor] = firstIdx + count - 1;
      } else {
        break; // Stop draining if network fails
      }
    }
  }

  postBatch(sensor, firstIndex, count, data, lost = []) {
    return new Promise((resolve) => {
      const url = new URL('/ingest', this.cloudUrl);
      const payload = JSON.stringify({
        session_id: this.activeSessionId,
        sensor,
        first_index: firstIndex,
        count,
        data,
        lost
      });

      const req = http.request({
        hostname: url.hostname,
        port: url.port || 80,
        path: url.pathname,
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(payload)
        },
        timeout: 2000
      }, (res) => {
        res.resume();
        const ok = res.statusCode >= 200 && res.statusCode < 300;
        if (!ok && this.networkMonitor) {
          this.networkMonitor.recordFailure();
        }
        resolve(ok);
      });

      req.on('error', () => {
        if (this.networkMonitor) this.networkMonitor.recordFailure();
        resolve(false);
      });
      req.on('timeout', () => {
        req.destroy();
        if (this.networkMonitor) this.networkMonitor.recordFailure();
        resolve(false);
      });

      req.write(payload);
      req.end();
    });
  }

  refreshCloudWatermarks() {
    const fetchWatermark = (sensor) => new Promise((resolve) => {
      const url = new URL(`/watermark?session_id=${this.activeSessionId}&sensor=${sensor}`, this.cloudUrl);
      const req = http.request(url, { timeout: 1500 }, (res) => {
        let body = '';
        res.on('data', c => { body += c; });
        res.on('end', () => {
          try {
            const data = JSON.parse(body);
            if (typeof data.acknowledged_index === 'number') {
              this.cloudWatermarks[sensor] = data.acknowledged_index;
            }
          } catch (_) {}
          resolve();
        });
      });
      req.on('error', () => resolve());
      req.end();
    });

    return Promise.all([fetchWatermark(1), fetchWatermark(2)]);
  }

  syncLosses() {
    return new Promise((resolve) => {
      const losses = this.storage.loadLosses(this.activeSessionId);
      if (losses.length === 0) return resolve();

      const url = new URL('/sync_losses', this.cloudUrl);
      const payload = JSON.stringify({
        session_id: this.activeSessionId,
        losses
      });

      const req = http.request(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(payload)
        },
        timeout: 2000
      }, (res) => {
        res.resume();
        resolve();
      });

      req.on('error', () => resolve());
      req.write(payload);
      req.end();
    });
  }

  getCloudSampleCount() {
    let count = 0;
    if (this.cloudWatermarks[1] >= 0) count += (this.cloudWatermarks[1] + 1);
    if (this.cloudWatermarks[2] >= 0) count += (this.cloudWatermarks[2] + 1);
    return count;
  }
}

module.exports = { SyncAgent };
