/**
 * src/device/network_monitor.js
 *
 * Monitors cloud network connectivity and orchestrates automatic
 * switching between 'online' and 'offline' modes within 10 seconds.
 */

'use strict';

const http = require('node:http');
const EventEmitter = require('node:events');

class NetworkMonitor extends EventEmitter {
  constructor(cloudUrl = 'http://127.0.0.1:8080/health', options = {}) {
    super();
    this.cloudUrl = cloudUrl;
    this.intervalMs = options.intervalMs || 1000; // 1 second fast probe
    this.failThreshold = options.failThreshold || 2; // 2 fails = 2s (<= 10s SLA)
    this.mode = 'offline';
    this.consecutiveFailures = 0;
    this.timer = null;
    this.running = false;
  }

  recordFailure() {
    this.handleFailure();
  }

  start() {
    this.running = true;
    this.check();
    this.timer = setInterval(() => this.check(), this.intervalMs);
  }

  stop() {
    this.running = false;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  check() {
    if (!this.running) return;

    try {
      const url = new URL(this.cloudUrl);
      const req = http.request({
        hostname: url.hostname,
        port: url.port || 80,
        path: url.pathname,
        method: 'GET',
        timeout: 1500
      }, (res) => {
        if (res.statusCode >= 200 && res.statusCode < 300) {
          this.handleSuccess();
        } else {
          this.handleFailure();
        }
        res.resume();
      });

      req.on('error', () => this.handleFailure());
      req.on('timeout', () => {
        req.destroy();
        this.handleFailure();
      });

      req.end();
    } catch (_) {
      this.handleFailure();
    }
  }

  handleSuccess() {
    this.consecutiveFailures = 0;
    if (this.mode !== 'online') {
      this.mode = 'online';
      this.emit('mode_change', 'online');
    }
  }

  handleFailure() {
    this.consecutiveFailures++;
    if (this.consecutiveFailures >= this.failThreshold) {
      if (this.mode !== 'offline') {
        this.mode = 'offline';
        this.emit('mode_change', 'offline');
      }
    }
  }

  getMode() {
    return this.mode;
  }
}

module.exports = { NetworkMonitor };
