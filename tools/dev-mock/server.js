/**
 * tools/dev-mock/server.js
 *
 * TCP server implementation for biosignal-mock.
 * Handles client connections (max 1 concurrent), session lifecycle, cursor management,
 * sample streaming, fault injection, and protocol compliance.
 */

'use strict';

const net = require('node:net');
const http = require('node:http');
const crypto = require('node:crypto');
const { generateChunk, SAMPLE_RATE } = require('./generator');
const { createLineBuffer, parseAndValidateRequest, buildResponse, buildErrorResponse } = require('./protocol');

/**
 * Generate a unique session ID for the mock server process.
 * Format: dev-session-<timestamp>-<hex>
 */
function generateSessionId() {
  const timestamp = Date.now();
  const rand = crypto.randomBytes(4).toString('hex');
  return `dev-session-${timestamp}-${rand}`;
}

/**
 * Create and configure the biosignal mock TCP server.
 *
 * @param {object} config - Configuration object from config.js
 * @returns {object} Server controller with start() and stop() methods
 */
function createBiosignalServer(config) {
  let sessionId = generateSessionId();
  const totalSamples = config.duration * SAMPLE_RATE;

  // Independent cursors for sensor 1 and sensor 2
  let sensor1Cursor = 0;
  let sensor2Cursor = 0;

  // Dynamic fault injection triggers (for live testing)
  const dynamicFaults = {
    corruptCrcRemaining: 0,
    duplicateRemaining: 0,
    stallMs: 0,
    sensorLoss: null
  };

  // Single active client reference
  let activeSocket = null;
  let server = null;
  let controlServer = null;
  let restartTimer = null;

  function log(message) {
    console.log(`[biosignal-mock] ${message}`);
  }

  function logRead(sensor, from, count) {
    if (!config.quiet) {
      log(`READ sensor=${sensor} from=${from} count=${count}`);
    }
  }

  /**
   * Process a single valid read request.
   */
  function handleReadRequest(socket, req) {
    const sensor = req.sensor;
    const isReread = (req.from_index !== undefined);
    const startIndex = isReread ? req.from_index : (sensor === 1 ? sensor1Cursor : sensor2Cursor);

    // Calculate available samples up to session limit
    const available = Math.max(0, totalSamples - startIndex);
    let count = Math.min(config.chunkSize, available);

    // Update cursors according to specification:
    // If sequential read: advance cursor by count.
    // If from_index reread: update cursor ONLY if requested range extends beyond current cursor.
    if (!isReread) {
      if (sensor === 1) {
        sensor1Cursor += count;
      } else {
        sensor2Cursor += count;
      }
    } else {
      if (sensor === 1) {
        sensor1Cursor = Math.max(sensor1Cursor, startIndex + count);
      } else {
        sensor2Cursor = Math.max(sensor2Cursor, startIndex + count);
      }
    }

    const sessionEnd = (startIndex + count >= totalSamples);

    // Handle fault injection: Sensor loss
    let lost = [];
    if (dynamicFaults.sensorLoss && dynamicFaults.sensorLoss.sensor === sensor) {
      const dropCount = Math.min(count, dynamicFaults.sensorLoss.count);
      lost = [{
        sensor,
        from_index: startIndex,
        to_index: startIndex + dropCount - 1,
        reason: 'buffer_overflow'
      }];
      dynamicFaults.sensorLoss.count -= dropCount;
      if (dynamicFaults.sensorLoss.count <= 0) dynamicFaults.sensorLoss = null;
      if (!config.quiet) {
        log(`FAULT INJECTION (Dynamic): Sensor ${sensor} loss injected from=${startIndex} count=${dropCount}`);
      }
    } else if (config.sensorLossRate > 0 && Math.random() < config.sensorLossRate && count > 0) {
      lost = [{
        sensor,
        from_index: startIndex,
        to_index: startIndex + count - 1,
        reason: 'buffer_overflow'
      }];
    }

    // Generate samples on demand
    const data = generateChunk(sensor, startIndex, count, config.mode, config.seed);

    // Handle fault injection: Corrupt CRC
    let overrideCrc = null;
    if (dynamicFaults.corruptCrcRemaining > 0) {
      overrideCrc = 'DEADBEEF';
      dynamicFaults.corruptCrcRemaining--;
      if (!config.quiet) {
        log(`FAULT INJECTION (Dynamic): Corrupt CRC injected for sensor=${sensor} from=${startIndex}`);
      }
    } else if (config.corruptCrcRate > 0 && Math.random() < config.corruptCrcRate) {
      overrideCrc = 'DEADBEEF';
      if (!config.quiet) {
        log(`FAULT INJECTION: Corrupt CRC injected for sensor=${sensor} from=${startIndex}`);
      }
    }

    // Build the formatted response with exact dataText CRC-32
    const { responseText } = buildResponse({
      sessionId,
      sensor,
      firstIndex: startIndex,
      count,
      lost,
      sessionEnd,
      data,
      overrideCrc
    });

    logRead(sensor, startIndex, count);

    // Helper to send data to socket (with duplicate injection if enabled)
    const send = () => {
      if (socket.destroyed) return;
      socket.write(responseText);

      // Fault injection: Duplicate response
      if (dynamicFaults.duplicateRemaining > 0) {
        dynamicFaults.duplicateRemaining--;
        if (!config.quiet) {
          log(`FAULT INJECTION (Dynamic): Sending duplicate response for sensor=${sensor} from=${startIndex}`);
        }
        socket.write(responseText);
      } else if (config.duplicateRate > 0 && Math.random() < config.duplicateRate) {
        if (!config.quiet) {
          log(`FAULT INJECTION: Sending duplicate response for sensor=${sensor} from=${startIndex}`);
        }
        socket.write(responseText);
      }
    };

    // Handle fault injection: Stall / delay
    if (dynamicFaults.stallMs > 0) {
      const delay = dynamicFaults.stallMs;
      dynamicFaults.stallMs = 0;
      if (!config.quiet) {
        log(`FAULT INJECTION (Dynamic): Stalling response for ${delay}ms`);
      }
      setTimeout(send, delay);
    } else if (config.stallRate > 0 && Math.random() < config.stallRate) {
      if (!config.quiet) {
        log(`FAULT INJECTION: Stalling response for ${config.stallMs}ms`);
      }
      setTimeout(send, config.stallMs);
    } else {
      send();
    }
  }

  /**
   * Handle incoming connection from a client.
   */
  function handleConnection(socket) {
    // Enforcement: accept exactly one client at a time
    if (activeSocket !== null) {
      log('Rejecting additional client');
      socket.destroy();
      return;
    }

    activeSocket = socket;
    log('Client connected');

    const lineBuffer = createLineBuffer();

    socket.on('data', (chunk) => {
      const lines = lineBuffer.push(chunk);

      for (const line of lines) {
        const result = parseAndValidateRequest(line, totalSamples);

        if (!result.valid) {
          const errResp = buildErrorResponse(result.error, result.message);
          socket.write(errResp);
          continue;
        }

        handleReadRequest(socket, result.request);
      }
    });

    let cleanedUp = false;
    const cleanup = () => {
      if (cleanedUp) return;
      cleanedUp = true;
      if (activeSocket === socket) {
        activeSocket = null;
        lineBuffer.reset();
        log('Client disconnected');
      }
    };

    socket.on('close', cleanup);
    socket.on('end', cleanup);
    socket.on('error', () => {
      cleanup();
    });
  }

  function startControlServer() {
    const controlPort = config.controlPort || (config.port + 1);
    controlServer = http.createServer((req, res) => {
      res.setHeader('Content-Type', 'application/json');

      if (req.method === 'POST' && req.url === '/fault') {
        let body = '';
        req.on('data', chunk => { body += chunk.toString('utf8'); });
        req.on('end', () => {
          try {
            const payload = JSON.parse(body);
            if (payload.fault === 'corrupt_crc') {
              dynamicFaults.corruptCrcRemaining = payload.count || 1;
              log(`Control: Injected corrupt CRC for next ${dynamicFaults.corruptCrcRemaining} responses`);
            } else if (payload.fault === 'duplicate') {
              dynamicFaults.duplicateRemaining = payload.count || 1;
              log(`Control: Injected duplicate response for next ${dynamicFaults.duplicateRemaining} responses`);
            } else if (payload.fault === 'stall') {
              dynamicFaults.stallMs = payload.ms || 2000;
              log(`Control: Injected ${dynamicFaults.stallMs}ms stall on next response`);
            } else if (payload.fault === 'sensor_loss') {
              dynamicFaults.sensorLoss = { sensor: payload.sensor || 1, count: payload.count || 200 };
              log(`Control: Injected loss of ${dynamicFaults.sensorLoss.count} samples on sensor ${dynamicFaults.sensorLoss.sensor}`);
            } else if (payload.fault === 'restart') {
              // Simulate mock process restart: new session ID, reset cursors, drop client
              const oldSession = sessionId;
              sessionId = generateSessionId();
              sensor1Cursor = 0;
              sensor2Cursor = 0;
              log(`Control: Simulated mock restart: ${oldSession} -> ${sessionId}`);
              if (activeSocket) {
                activeSocket.destroy();
                activeSocket = null;
              }
              res.end(JSON.stringify({ status: 'restarted', old_session_id: oldSession, new_session_id: sessionId }));
              return;
            }
            res.end(JSON.stringify({ status: 'fault_injected', dynamicFaults }));
          } catch (err) {
            res.writeHead(400);
            res.end(JSON.stringify({ error: err.message }));
          }
        });
        return;
      }

      if (req.method === 'GET' && req.url === '/info') {
        res.end(JSON.stringify({
          sessionId,
          sensor1Cursor,
          sensor2Cursor,
          totalSamples,
          clientConnected: activeSocket !== null,
          dynamicFaults
        }));
        return;
      }

      res.writeHead(404);
      res.end(JSON.stringify({ error: 'Endpoint not found' }));
    });

    controlServer.listen(controlPort, config.host, () => {
      log(`Control API listening on http://${config.host}:${controlPort}`);
    });
    controlServer.on('error', () => {
      // Ignore control server port conflicts
    });
  }

  function start(callback) {
    server = net.createServer(handleConnection);

    server.listen(config.port, config.host, () => {
      log(`Listening on ${config.host}:${config.port}`);
      log(`Mode: ${config.mode}`);
      log(`Seed: ${config.seed}`);
      log(`Duration: ${config.duration}s (${totalSamples} samples @ ${SAMPLE_RATE} Hz)`);
      log(`Session: ${sessionId}`);

      // Start companion control API
      startControlServer();

      // Handle optional auto-restart/exit
      if (config.restartAfter > 0) {
        log(`Scheduled auto-termination after ${config.restartAfter}s`);
        restartTimer = setTimeout(() => {
          log(`Auto-terminating process after ${config.restartAfter}s (fault injection)`);
          process.exit(0);
        }, config.restartAfter * 1000);
      }

      if (typeof callback === 'function') {
        callback(null, { server, sessionId });
      }
    });

    server.on('error', (err) => {
      log(`Server error: ${err.message}`);
      if (typeof callback === 'function') {
        callback(err);
      }
    });

    return server;
  }

  function stop(callback) {
    if (restartTimer) {
      clearTimeout(restartTimer);
      restartTimer = null;
    }

    if (activeSocket) {
      activeSocket.destroy();
      activeSocket = null;
    }

    if (controlServer) {
      try {
        controlServer.close();
      } catch (_) {}
      controlServer = null;
    }

    if (server) {
      server.close(() => {
        log('Server stopped');
        if (typeof callback === 'function') callback();
      });
    } else if (typeof callback === 'function') {
      callback();
    }
  }

  return {
    start,
    stop,
    getSessionId: () => sessionId,
    getCursors: () => ({ sensor1: sensor1Cursor, sensor2: sensor2Cursor }),
    getConfig: () => ({ ...config })
  };
}

module.exports = {
  createBiosignalServer,
  generateSessionId
};
