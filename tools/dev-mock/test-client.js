/**
 * tools/dev-mock/test-client.js
 *
 * Comprehensive automated verification script for biosignal-mock.
 * Verifies all 11 requirements from Section 23:
 *  1. Connects to mock
 *  2. Requests sensor 1
 *  3. Requests sensor 2
 *  4. Verifies first_index
 *  5. Verifies count
 *  6. Verifies ramp values against exact mathematical formulas
 *  7. Independently calculates CRC-32
 *  8. Verifies CRC matching
 *  9. Requests specific from_index
 * 10. Verifies returned sample starts exactly at that index
 * 11. Verifies session_end behavior
 * Also tests single-client concurrency rejection and protocol error handling.
 */

'use strict';

const net = require('node:net');
const { crc32 } = require('./crc32');
const { createBiosignalServer } = require('./server');

function parseArgs() {
  const args = process.argv.slice(2);
  const opts = { host: '127.0.0.1', port: 7100 };
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--port' && args[i + 1]) opts.port = parseInt(args[++i], 10);
    if (args[i] === '--host' && args[i + 1]) opts.host = args[++i];
  }
  return opts;
}

class MockClient {
  constructor(host, port) {
    this.host = host;
    this.port = port;
    this.socket = null;
    this.buffer = '';
    this.queue = [];
    this.closed = false;
  }

  connect() {
    return new Promise((resolve, reject) => {
      this.socket = net.connect({ host: this.host, port: this.port }, () => {
        resolve();
      });

      this.socket.on('data', (chunk) => {
        this.buffer += chunk.toString('utf8');
        let idx;
        while ((idx = this.buffer.indexOf('\n')) !== -1) {
          const line = this.buffer.slice(0, idx).trim();
          this.buffer = this.buffer.slice(idx + 1);
          if (line.length > 0) {
            if (this.queue.length > 0) {
              const { resolve: res } = this.queue.shift();
              res(line);
            }
          }
        }
      });

      this.socket.on('error', (err) => {
        this.closed = true;
        if (this.queue.length > 0) {
          const { reject: rej } = this.queue.shift();
          rej(err);
        } else {
          reject(err);
        }
      });

      this.socket.on('close', () => {
        this.closed = true;
      });
    });
  }

  request(obj) {
    return new Promise((resolve, reject) => {
      this.queue.push({ resolve, reject });
      this.socket.write(JSON.stringify(obj) + '\n');
    });
  }

  close() {
    return new Promise((resolve) => {
      if (!this.socket || this.closed) {
        return resolve();
      }
      this.socket.once('close', resolve);
      this.socket.end();
      // Safety timeout
      setTimeout(() => {
        if (!this.closed && this.socket) {
          this.socket.destroy();
        }
        resolve();
      }, 500);
    });
  }
}

function extractDataText(rawJsonLine) {
  const marker = '"data":';
  const startIdx = rawJsonLine.indexOf(marker);
  if (startIdx === -1) {
    throw new Error('Response JSON does not contain "data": field');
  }
  const arrayStart = rawJsonLine.indexOf('[', startIdx + marker.length);
  const lastBrace = rawJsonLine.lastIndexOf('}');
  const arrayEnd = rawJsonLine.lastIndexOf(']', lastBrace);
  return rawJsonLine.slice(arrayStart, arrayEnd + 1);
}

function assert(condition, message) {
  if (!condition) {
    console.error(`\x1b[31m[FAIL]\x1b[0m ${message}`);
    throw new Error(message);
  }
}

async function runTests() {
  const { host, port } = parseArgs();
  console.log(`\n========================================================`);
  console.log(`biosignal-mock Automated Test Suite`);
  console.log(`Target: ${host}:${port}`);
  console.log(`========================================================\n`);

  let localServer = null;
  const client = new MockClient(host, port);

  try {
    await client.connect();
    console.log(`[*] Connected to running mock server at ${host}:${port}.`);
  } catch (err) {
    console.log(`[*] No active server detected at ${host}:${port} (${err.code || err.message}).`);
    console.log(`[*] Spawning dedicated test mock server instance (ramp mode, duration 10s)...`);
    localServer = createBiosignalServer({
      mode: 'ramp',
      seed: 7,
      duration: 10, // 10s * 400 = 4000 samples
      port,
      host,
      chunkSize: 100,
      quiet: true,
      duplicateRate: 0,
      corruptCrcRate: 0,
      stallRate: 0,
      stallMs: 0,
      sensorLossRate: 0,
      restartAfter: 0
    });

    await new Promise((res, rej) => {
      localServer.start((startErr) => {
        if (startErr) rej(startErr);
        else res();
      });
    });

    await client.connect();
    console.log(`[*] Connected to newly spawned test server.`);
  }

  console.log(`[PASS] 1. Connected to mock server successfully`);

  try {
    // -------------------------------------------------------------
    // Test 2: Request Sensor 1
    // -------------------------------------------------------------
    const s1Raw = await client.request({ cmd: 'read', sensor: 1 });
    const s1 = JSON.parse(s1Raw);

    assert(typeof s1.session_id === 'string' && s1.session_id.length > 0, 'session_id must be non-empty string');
    assert(s1.sensor === 1, 'sensor must be 1');
    console.log(`[PASS] 2. Requested Sensor 1 (session_id="${s1.session_id}")`);

    // -------------------------------------------------------------
    // Test 4: Verify first_index
    // -------------------------------------------------------------
    assert(typeof s1.first_index === 'number' && s1.first_index >= 0, `first_index must be non-negative integer`);
    const initialS1FirstIndex = s1.first_index;
    console.log(`[PASS] 4. Verified first_index == ${initialS1FirstIndex} for initial sensor 1 read`);

    let testS1 = s1;
    let s1RawText = s1Raw;
    if (s1.count === 0 && s1.session_end) {
      console.log(`[*] Mock server cursor has reached session end. Using from_index=0 to verify samples.`);
      s1RawText = await client.request({ cmd: 'read', sensor: 1, from_index: 0 });
      testS1 = JSON.parse(s1RawText);
    }

    assert(testS1.count === testS1.data.length, `count (${testS1.count}) must match data.length (${testS1.data.length})`);
    assert(testS1.count > 0, `count must be greater than 0`);
    console.log(`[PASS] 5. Verified count == ${testS1.count} and data.length == ${testS1.data.length}`);

    // -------------------------------------------------------------
    // Test 6: Verify Ramp Values against formula
    // Sensor 1 formula: (3*i + c) mod 8388608
    // -------------------------------------------------------------
    for (let i = 0; i < testS1.count; i++) {
      const absIndex = testS1.first_index + i;
      for (let c = 0; c < 3; c++) {
        const expected = (3 * absIndex + c) % 8388608;
        assert(testS1.data[i][c] === expected,
          `Sensor 1 mismatch at index ${absIndex}, channel ${c}: expected ${expected}, got ${testS1.data[i][c]}`);
      }
    }
    console.log(`[PASS] 6a. Verified Sensor 1 ramp values match exact formula (3*i + c) mod 8,388,608`);

    // -------------------------------------------------------------
    // Test 7 & 8: Independently calculate CRC-32 on exact raw text and verify
    // -------------------------------------------------------------
    const rawDataTextS1 = extractDataText(s1RawText);
    const independentCrcS1 = crc32(rawDataTextS1);
    assert(testS1.crc32.toLowerCase() === independentCrcS1.toLowerCase(),
      `CRC-32 mismatch! Response: "${testS1.crc32}", Calculated: "${independentCrcS1}"`);
    console.log(`[PASS] 7 & 8. Independently calculated CRC-32 ("${independentCrcS1}") and verified match`);

    // -------------------------------------------------------------
    // Test 3: Request Sensor 2
    // -------------------------------------------------------------
    const s2Raw = await client.request({ cmd: 'read', sensor: 2 });
    const s2 = JSON.parse(s2Raw);

    assert(s2.sensor === 2, 'sensor must be 2');
    let testS2 = s2;
    let s2RawText = s2Raw;
    if (s2.count === 0 && s2.session_end) {
      s2RawText = await client.request({ cmd: 'read', sensor: 2, from_index: 0 });
      testS2 = JSON.parse(s2RawText);
    }

    assert(testS2.count === testS2.data.length, `Sensor 2 count must match data.length`);
    assert(testS2.data[0].length === 2, `Sensor 2 must have 2 channels`);

    // Sensor 2 formula: (2*i + c) mod 8388608
    for (let i = 0; i < testS2.count; i++) {
      const absIndex = testS2.first_index + i;
      for (let c = 0; c < 2; c++) {
        const expected = (2 * absIndex + c) % 8388608;
        assert(testS2.data[i][c] === expected,
          `Sensor 2 mismatch at index ${absIndex}, channel ${c}: expected ${expected}, got ${testS2.data[i][c]}`);
      }
    }

    const rawDataTextS2 = extractDataText(s2RawText);
    const independentCrcS2 = crc32(rawDataTextS2);
    assert(testS2.crc32.toLowerCase() === independentCrcS2.toLowerCase(), `Sensor 2 CRC-32 mismatch`);
    console.log(`[PASS] 3 & 6b. Requested Sensor 2, verified 2 channels and formula (2*i + c) mod 8,388,608`);

    // -------------------------------------------------------------
    // Test 9 & 10: Request with specific `from_index`
    // -------------------------------------------------------------
    const rereadRaw = await client.request({ cmd: 'read', sensor: 1, from_index: 450 });
    const reread = JSON.parse(rereadRaw);

    assert(reread.first_index === 450, `Expected first_index 450, got ${reread.first_index}`);
    assert(reread.data[0][0] === (3 * 450 + 0) % 8388608,
      `Expected sample at 450 to match (3*450 + 0), got ${reread.data[0][0]}`);
    console.log(`[PASS] 9 & 10. Requested from_index=450 and verified returned data starts at index 450`);

    // -------------------------------------------------------------
    // Test 11: Session End behavior
    // -------------------------------------------------------------
    // Query out of bounds to detect total session samples limit
    const probeLimitRaw = await client.request({ cmd: 'read', sensor: 1, from_index: 999999999 });
    const probeLimit = JSON.parse(probeLimitRaw);
    assert(probeLimit.error === 'invalid_request', 'Out-of-bounds from_index must return invalid_request');

    const limitMatch = probeLimit.message && probeLimit.message.match(/limit:\s*(\d+)/);
    const totalSamples = limitMatch ? parseInt(limitMatch[1], 10) : 4000;
    console.log(`[*] Detected session sample limit: ${totalSamples}`);

    // Request samples ending exactly at the boundary
    const nearEndRaw = await client.request({ cmd: 'read', sensor: 1, from_index: totalSamples - 50 });
    const nearEnd = JSON.parse(nearEndRaw);
    assert(nearEnd.first_index === totalSamples - 50, `Expected first_index ${totalSamples - 50}`);
    assert(nearEnd.count === 50, `Expected count 50, got ${nearEnd.count}`);
    assert(nearEnd.session_end === true, `Expected session_end == true on reaching boundary`);
    console.log(`[PASS] 11. Verified session_end == true and boundary clamp`);

    // -------------------------------------------------------------
    // Test 12: Single Client Enforcement - Rejecting second client
    // -------------------------------------------------------------
    let secondClientRejected = false;
    try {
      const client2 = new MockClient(host, port);
      await client2.connect();
      // Server will immediately close/destroy the socket
      await client2.request({ cmd: 'read', sensor: 1 });
    } catch (err) {
      secondClientRejected = true;
    }
    assert(secondClientRejected, 'Second concurrent client must be rejected');
    console.log(`[PASS] 12. Verified single-client enforcement (second client rejected)`);

    // -------------------------------------------------------------
    // Test 13: Error Handling
    // -------------------------------------------------------------
    const malformedRaw = await client.request({ cmd: 'unsupported_command', sensor: 1 });
    const malformed = JSON.parse(malformedRaw);
    assert(malformed.error === 'invalid_request', `Expected invalid_request, got ${malformed.error}`);

    const badSensorRaw = await client.request({ cmd: 'read', sensor: 99 });
    const badSensor = JSON.parse(badSensorRaw);
    assert(badSensor.error === 'invalid_request', `Expected invalid_request for bad sensor`);
    console.log(`[PASS] 13. Verified error responses for malformed & unsupported requests`);

    console.log(`\n========================================================`);
    console.log(`\x1b[32mALL TESTS PASSED SUCCESSFULLY!\x1b[0m`);
    console.log(`========================================================\n`);
  } finally {
    await client.close();
    if (localServer) {
      await new Promise((res) => localServer.stop(res));
    }
  }
}

runTests().catch((err) => {
  console.error(`\x1b[31mTest Suite Failed:\x1b[0m`, err);
  process.exit(1);
});
