/**
 * src/device/ingestion.js
 *
 * Ingestion Worker for connecting to biosignal-mock TCP server.
 *
 * Features:
 * - Real-time reading of Sensor 1 and Sensor 2
 * - CRC-32 validation over raw data text
 * - Immediate re-request on corrupt CRC ({"cmd":"read","sensor":s,"from_index":i})
 * - Session tracking and unread loss recording on unexpected restart
 * - Honest loss logging for reported buffer overflows
 */

'use strict';

const net = require('node:net');
const EventEmitter = require('node:events');
const { verifyCrc, extractDataText } = require('../common/protocol');

class IngestionWorker extends EventEmitter {
  constructor(options = {}) {
    super();
    this.host = options.host || '127.0.0.1';
    this.port = options.port || 7100;
    this.storage = options.storage;

    this.socket = null;
    this.buffer = '';
    this.running = false;
    this.activeSessionId = null;

    // Highest acknowledged index saved for each sensor in current session
    this.sensorCursors = { 1: 0, 2: 0 };
    // Track in-flight read request timestamp per sensor: { 1: timestamp, 2: timestamp }
    this.inFlight = { 1: 0, 2: 0 };
    this.lastDataTime = Date.now();

    this.pollTimer = null;
    this.watchdogTimer = null;
  }

  start() {
    this.running = true;
    this.connect();
    this.startWatchdog();
  }

  stop() {
    this.running = false;
    if (this.pollTimer) {
      clearInterval(this.pollTimer);
      this.pollTimer = null;
    }
    if (this.watchdogTimer) {
      clearInterval(this.watchdogTimer);
      this.watchdogTimer = null;
    }
    if (this.socket) {
      this.socket.destroy();
      this.socket = null;
    }
  }

  startWatchdog() {
    if (this.watchdogTimer) clearInterval(this.watchdogTimer);
    // Watchdog checks every 2s: if mock stopped answering for > 6s while connected, reset socket
    this.watchdogTimer = setInterval(() => {
      if (!this.running || !this.socket) return;
      const elapsed = Date.now() - this.lastDataTime;
      if (elapsed > 6000) {
        this.emit('warning', `Mock inactive for ${Math.round(elapsed / 1000)}s, resetting socket connection...`);
        this.socket.destroy();
        this.socket = null;
        this.lastDataTime = Date.now();
        this.inFlight = { 1: 0, 2: 0 };
        setTimeout(() => this.connect(), 500);
      }
    }, 2000);
  }

  connect() {
    if (!this.running) return;

    this.lastDataTime = Date.now();
    this.inFlight = { 1: 0, 2: 0 };

    this.socket = net.connect({ host: this.host, port: this.port }, () => {
      this.lastDataTime = Date.now();
      this.emit('connected');
      this.scheduleReads();
    });

    this.socket.on('data', (chunk) => {
      this.lastDataTime = Date.now();
      this.buffer += chunk.toString('utf8');
      let idx;
      while ((idx = this.buffer.indexOf('\n')) !== -1) {
        const line = this.buffer.slice(0, idx).trim();
        this.buffer = this.buffer.slice(idx + 1);
        if (line.length > 0) {
          this.handleResponseLine(line);
        }
      }
    });

    this.socket.on('error', (err) => {
      this.emit('error', err);
    });

    this.socket.on('close', () => {
      this.socket = null;
      this.inFlight = { 1: 0, 2: 0 };
      this.emit('disconnected');
      if (this.running) {
        // Retry connection after brief backoff
        setTimeout(() => this.connect(), 1000);
      }
    });
  }

  scheduleReads() {
    if (this.pollTimer) clearInterval(this.pollTimer);

    // Actively query both sensors sequentially
    this.pollTimer = setInterval(() => {
      if (!this.socket || this.socket.destroyed || !this.running) return;
      this.requestNext(1);
      this.requestNext(2);
    }, 50); // 50ms polling satisfies real-time streaming well within 10s buffer
  }

  requestNext(sensor) {
    if (!this.socket || this.socket.destroyed) return;

    // In-flight guard: don't flood TCP buffer if mock is busy / stalling, retry after 1500ms
    const now = Date.now();
    if (this.inFlight[sensor] > 0 && (now - this.inFlight[sensor] < 1500)) {
      return;
    }

    const fromIndex = this.sensorCursors[sensor];
    const req = { cmd: 'read', sensor };
    if (fromIndex > 0) {
      req.from_index = fromIndex;
    }
    this.inFlight[sensor] = now;
    this.socket.write(JSON.stringify(req) + '\n');
  }

  requestReread(sensor, fromIndex) {
    if (!this.socket || this.socket.destroyed) return;
    this.inFlight[sensor] = Date.now();
    const req = { cmd: 'read', sensor, from_index: fromIndex };
    this.socket.write(JSON.stringify(req) + '\n');
  }

  handleResponseLine(rawLine) {
    let resp;
    try {
      resp = JSON.parse(rawLine);
    } catch (err) {
      this.emit('warning', `Malformed JSON response from mock: ${rawLine}`);
      return;
    }

    if (resp.error) {
      this.emit('warning', `Mock returned error: ${resp.error} - ${resp.message}`);
      return;
    }

    const { session_id, sensor, first_index, count, lost, session_end, crc32, data } = resp;

    // Reset in-flight marker for this sensor
    if (sensor && this.inFlight[sensor] !== undefined) {
      this.inFlight[sensor] = 0;
    }

    // Check CRC-32 over exact data substring
    const dataText = extractDataText(rawLine);
    if (!dataText || !verifyCrc(dataText, crc32)) {
      this.emit('corrupt_packet', { sensor, first_index, count, expectedCrc: crc32 });
      // Re-read immediately from the expected index
      this.requestReread(sensor, this.sensorCursors[sensor]);
      return;
    }

    // Handle Session Lifecycle
    if (this.activeSessionId === null) {
      this.activeSessionId = session_id;
      this.emit('session_start', session_id);
    } else if (this.activeSessionId !== session_id) {
      // Session has restarted!
      // Old session unread data is lost: record with to_index: null
      const oldSession = this.activeSessionId;
      this.storage.recordLoss(oldSession, 1, this.sensorCursors[1], null, 'mock_process_restart');
      this.storage.recordLoss(oldSession, 2, this.sensorCursors[2], null, 'mock_process_restart');

      this.activeSessionId = session_id;
      this.sensorCursors = { 1: 0, 2: 0 };
      this.emit('session_restart', { oldSession, newSession: session_id });
    }

    // Process reported loss ranges
    if (Array.isArray(lost) && lost.length > 0) {
      for (const item of lost) {
        const fromIdx = item.from_index !== undefined ? item.from_index : item.start_index;
        const toIdx = item.to_index !== undefined ? item.to_index : item.end_index;
        const reason = item.reason || 'buffer_overflow';
        this.storage.recordLoss(this.activeSessionId, sensor, fromIdx, toIdx, reason);
        this.emit('loss_detected', { sensor, fromIdx, toIdx, reason });

        // Advance cursor past known loss range so we do not re-request lost samples
        if (typeof toIdx === 'number') {
          this.sensorCursors[sensor] = Math.max(this.sensorCursors[sensor], toIdx + 1);
        }
      }
    }

    // Handle duplicate or overlapping reply (Judge Fault: mock sends same reply twice)
    if (count > 0 && Array.isArray(data)) {
      const currentCursor = this.sensorCursors[sensor];

      if (first_index + count <= currentCursor) {
        // Completely duplicate reply: already received and saved
        this.emit('duplicate_ignored', { sensor, first_index, count });
        return;
      }

      let saveFirstIndex = first_index;
      let saveData = data;
      let saveCount = count;

      // Unannounced gap detection (e.g. mock skipped samples without listing in lost)
      if (first_index > currentCursor) {
        const gapFrom = currentCursor;
        const gapTo = first_index - 1;
        this.storage.recordLoss(this.activeSessionId, sensor, gapFrom, gapTo, 'buffer_overflow');
        this.emit('loss_detected', { sensor, fromIdx: gapFrom, toIdx: gapTo, reason: 'buffer_overflow' });
        this.sensorCursors[sensor] = first_index;
      } else if (first_index < currentCursor) {
        // Partial overlap: slice off already-saved samples
        const overlap = currentCursor - first_index;
        saveFirstIndex = currentCursor;
        saveData = data.slice(overlap);
        saveCount = saveData.length;
      }

      // Save only new samples to disk safely
      if (saveCount > 0) {
        this.storage.saveSamples(this.activeSessionId, sensor, saveFirstIndex, saveCount, saveData);
        this.sensorCursors[sensor] = Math.max(this.sensorCursors[sensor], saveFirstIndex + saveCount);
      }
    } else if (count === 0 && typeof first_index === 'number') {
      // Mock returned 0 samples at first_index: update cursor if it advances
      this.sensorCursors[sensor] = Math.max(this.sensorCursors[sensor], first_index);
    }

    this.emit('samples', {
      sessionId: this.activeSessionId,
      sensor,
      firstIndex: first_index,
      count,
      data,
      sessionEnd: !!session_end
    });
  }

  getCurrentSession() {
    return this.activeSessionId;
  }

  getCursors() {
    return { ...this.sensorCursors };
  }
}

module.exports = { IngestionWorker };
