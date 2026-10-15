/**
 * src/device/device_service.js
 *
 * Core coordinator service running on the device.
 *
 * Coordinates:
 * 1. IngestionWorker: Pulls data from mock, checks CRC, stores to WAL
 * 2. NetworkMonitor: Detects network online/offline transitions within 10s
 * 3. SyncAgent: Real-time cloud streaming and backlog catch-up
 * 4. Offline Window Processor: Processes 1-second windows when offline
 * 5. Status publisher: Keeps pipeline status up to date
 */

'use strict';

const fs = require('node:fs');
const path = require('node:path');
const { SessionStorage } = require('../common/storage');
const { IngestionWorker } = require('./ingestion');
const { NetworkMonitor } = require('./network_monitor');
const { SyncAgent } = require('./sync_agent');
const { HeartRateDetector, processWindow } = require('../common/processing');

class DeviceService {
  constructor(options = {}) {
    this.mockHost = options.mockHost || '127.0.0.1';
    this.mockPort = options.mockPort || 7100;
    this.cloudUrl = options.cloudUrl || 'http://127.0.0.1:8080';
    this.dataDir = options.dataDir || path.resolve(path.join(__dirname, '..', '..', 'data'));

    this.storage = new SessionStorage(this.dataDir);
    this.ingestion = new IngestionWorker({
      host: this.mockHost,
      port: this.mockPort,
      storage: this.storage
    });

    this.networkMonitor = new NetworkMonitor(`${this.cloudUrl}/health`);
    this.syncAgent = new SyncAgent(this.cloudUrl, this.storage, this.networkMonitor);

    // Offline window processor state: sessionId -> { s1Map, s2Map, hrDetector, lastProcessedWindow }
    this.offlineProcessors = new Map();
    this.statusTimer = null;
    this.currentMode = 'offline';
  }

  getOfflineProcessor(sessionId) {
    if (!this.offlineProcessors.has(sessionId)) {
      this.offlineProcessors.set(sessionId, {
        s1Map: new Map(),
        s2Map: new Map(),
        hrDetector: new HeartRateDetector(),
        lastProcessedWindow: -1
      });
    }
    return this.offlineProcessors.get(sessionId);
  }

  start() {
    console.log(`[device-service] Starting MayaCardio pipeline device daemon...`);
    console.log(`[device-service] Target mock: ${this.mockHost}:${this.mockPort}`);
    console.log(`[device-service] Target cloud: ${this.cloudUrl}`);

    // Ingestion Events
    this.ingestion.on('session_start', (sessionId) => {
      console.log(`[device-service] Ingestion session started: ${sessionId}`);
      this.syncAgent.setSession(sessionId);
    });

    this.ingestion.on('session_restart', ({ oldSession, newSession }) => {
      console.log(`[device-service] Ingestion session restarted: ${oldSession} -> ${newSession}`);
      this.syncAgent.setSession(newSession);
    });

    this.ingestion.on('samples', ({ sessionId, sensor, firstIndex, count, data }) => {
      // Always feed the in-memory cache for offline processing
      const proc = this.getOfflineProcessor(sessionId);
      const targetMap = sensor === 1 ? proc.s1Map : proc.s2Map;
      for (let i = 0; i < count; i++) {
        targetMap.set(firstIndex + i, data[i]);
      }

      // If in ONLINE mode: trigger immediate drain
      if (this.currentMode === 'online') {
        this.syncAgent.drainBacklog();
      } else {
        // If in OFFLINE mode: process windows locally on device
        this.processOfflineWindows(sessionId);
      }
    });

    // Network Monitor Events
    this.networkMonitor.on('mode_change', (newMode) => {
      console.log(`[device-service] Network state transition: ${this.currentMode} -> ${newMode}`);
      this.currentMode = newMode;
      this.syncAgent.setOnline(newMode === 'online');

      // If switched to offline: process any newly available windows locally
      if (newMode === 'offline') {
        const sessionId = this.ingestion.getCurrentSession();
        if (sessionId) {
          this.processOfflineWindows(sessionId);
        }
      }

      // Immediately write updated status file
      this.updateStatusFile();
    });

    this.networkMonitor.start();
    this.ingestion.start();

    // Periodically write status JSON file for CLI `pipeline status`
    this.statusTimer = setInterval(() => this.updateStatusFile(), 500);
  }

  processOfflineWindows(sessionId) {
    const proc = this.getOfflineProcessor(sessionId);
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
        mode: 'offline'
      });

      this.storage.saveResult(sessionId, result);
      proc.lastProcessedWindow = w;

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

  updateStatusFile() {
    const status = this.getStatus();
    const statusPath = path.join(this.dataDir, 'status.json');
    try {
      fs.writeFileSync(statusPath, JSON.stringify(status) + '\n', 'utf8');
    } catch (_) {}
  }

  getStatus() {
    const sessionId = this.ingestion.getCurrentSession();
    const samplesSaved = sessionId ? this.storage.getSavedSampleCount(sessionId) : 0;
    const samplesInCloud = this.syncAgent.getCloudSampleCount();
    const backlog = Math.max(0, samplesSaved - samplesInCloud);

    return {
      mode: this.currentMode,
      session_id: sessionId || null,
      samples_saved: samplesSaved,
      samples_in_cloud: samplesInCloud,
      backlog
    };
  }

  stop(callback) {
    if (this.statusTimer) clearInterval(this.statusTimer);
    this.networkMonitor.stop();
    this.ingestion.stop();
    this.storage.close();
    console.log(`[device-service] Device daemon stopped.`);
    if (typeof callback === 'function') callback();
  }
}

module.exports = { DeviceService };
