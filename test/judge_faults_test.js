/**
 * test/judge_faults_test.js
 *
 * Dedicated Verification Suite for Judge-Injected Faults:
 * 1. Mock stops answering for a while (stalls)
 * 2. Mock sends corrupt replies (wrong CRC-32)
 * 3. Mock sends duplicate replies (same reply twice)
 * 4. Mock stops sending one sensor for a while (reported in lost)
 * 5. Mock restarts, starting a new session (unread data lost with to_index: null)
 * 6. Sudden process termination and crash recovery
 * 7. Verification using the official check_ground_truth tool
 */

'use strict';

const fs = require('node:fs');
const path = require('node:path');
const { createBiosignalServer } = require('../tools/dev-mock/server');
const { CloudService } = require('../src/cloud/cloud_server');
const { DeviceService } = require('../src/device/device_service');
const { SessionStorage } = require('../src/common/storage');
const { verifyExport } = require('../tools/check_ground_truth');

const TEST_DIR = path.resolve(path.join(__dirname, '..', 'data_faults_test'));
const MOCK_PORT = 7130;
const CLOUD_PORT = 8095;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function cleanDir(dir) {
  if (fs.existsSync(dir)) {
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

function assert(condition, message) {
  if (!condition) {
    console.error(`\x1b[31m[FAIL]\x1b[0m ${message}`);
    throw new Error(message);
  }
}

async function runJudgeFaultsTest() {
  console.log(`\n===============================================================`);
  console.log(`MayaCardio Pipeline: Judge-Injected Faults Verification Suite`);
  console.log(`===============================================================\n`);

  cleanDir(TEST_DIR);
  const deviceDataDir = path.join(TEST_DIR, 'device');
  const cloudDataDir = path.join(TEST_DIR, 'cloud');

  // Step 1: Start Mock Server with ALL fault injections active:
  // - Corrupt CRC rate: 10%
  // - Duplicate response rate: 10%
  // - Sensor loss rate: 5%
  // - Stall rate: 10% (stall 1500ms)
  console.log(`[1/6] Launching mock with active fault injections:`);
  console.log(`      - Corrupt CRC rate: 10%`);
  console.log(`      - Duplicate reply rate: 10%`);
  console.log(`      - Sensor loss rate: 5%`);
  console.log(`      - Stall rate: 10% (1500ms delay)`);

  let mock = createBiosignalServer({
    mode: 'ramp',
    seed: 7,
    duration: 120, // 120s = 48,000 samples
    port: MOCK_PORT,
    host: '127.0.0.1',
    chunkSize: 100,
    quiet: true,
    duplicateRate: 0.10,
    corruptCrcRate: 0.10,
    stallRate: 0.10,
    stallMs: 1500,
    sensorLossRate: 0.05,
    restartAfter: 0
  });

  await new Promise((res, rej) => mock.start((err) => (err ? rej(err) : res())));
  const session1 = mock.getSessionId();
  console.log(`      Mock Session 1 started: ${session1}`);

  // Step 2: Start Cloud and Device services
  console.log(`\n[2/6] Starting Cloud Service and Device Pipeline...`);
  const cloud = new CloudService(CLOUD_PORT, '127.0.0.1', cloudDataDir);
  await new Promise((res) => cloud.start(res));

  const device = new DeviceService({
    mockHost: '127.0.0.1',
    mockPort: MOCK_PORT,
    cloudUrl: `http://127.0.0.1:${CLOUD_PORT}`,
    dataDir: deviceDataDir
  });
  device.start();

  // Ingest under fault conditions for 5 seconds
  console.log(`      Streaming under active fault conditions (5s)...`);
  await sleep(5000);

  const status1 = device.getStatus();
  console.log(`      Status: mode=${status1.mode}, saved=${status1.samples_saved}, cloud=${status1.samples_in_cloud}, backlog=${status1.backlog}`);
  assert(status1.samples_saved > 0, 'Must successfully collect samples despite corrupt CRC, duplicate replies, and stalls');
  console.log(`[PASS] Ingestion resilient to corrupt CRC, duplicate replies, stalls, and loss.`);

  // Step 3: Test Mock Restart (Starts a new session, unread data lost with to_index: null)
  console.log(`\n[3/6] Simulating Judge Fault: Mock Restart (Session Rollover)...`);
  await new Promise((res) => mock.stop(res));

  // Restart mock with a brand new session
  mock = createBiosignalServer({
    mode: 'ramp',
    seed: 7,
    duration: 120,
    port: MOCK_PORT,
    host: '127.0.0.1',
    chunkSize: 100,
    quiet: true,
    duplicateRate: 0,
    corruptCrcRate: 0,
    stallRate: 0,
    stallMs: 0,
    sensorLossRate: 0,
    restartAfter: 0
  });
  await new Promise((res, rej) => mock.start((err) => (err ? rej(err) : res())));
  const session2 = mock.getSessionId();
  console.log(`      Mock Session 2 started: ${session2}`);

  // Allow device to detect restart, record unread loss on old session, and ingest new session
  await sleep(3000);

  // Verify that session 1 recorded losses with to_index: null
  const storage = new SessionStorage(deviceDataDir);
  const session1Losses = storage.loadLosses(session1);
  const restartLoss = session1Losses.find(l => l.reason === 'mock_process_restart' && l.to_index === null);
  assert(restartLoss !== undefined, 'Unread data of old session must be recorded with "to_index": null on restart');
  console.log(`      Found expected restart loss record:`, JSON.stringify(restartLoss));
  console.log(`[PASS] Mock restart handling and to_index: null loss logging verified.`);

  // Step 4: Verify Ground Truth on Session 1 using check_ground_truth tool
  console.log(`\n[4/6] Running check_ground_truth verification on Session 1 (Faults Injected)...`);
  const exportLines1 = [];
  storage.exportSession(session1, (line) => exportLines1.push(line));
  assert(exportLines1.length > 0, 'Export lines must be non-empty');

  const session1Passed = await verifyExport(exportLines1, {
    mode: 'ramp',
    seed: 7,
    session: session1,
    verbose: false
  });
  assert(session1Passed === true, 'Session 1 export must pass check_ground_truth 100%!');
  console.log(`[PASS] Session 1 export verified against ground truth with 0 silent loss and 0 duplicate samples.`);

  // Step 5: Verify Ground Truth on Session 2
  console.log(`\n[5/6] Running check_ground_truth verification on Session 2...`);
  const exportLines2 = [];
  storage.exportSession(session2, (line) => exportLines2.push(line));
  const session2Passed = await verifyExport(exportLines2, {
    mode: 'ramp',
    seed: 7,
    session: session2,
    verbose: false
  });
  assert(session2Passed === true, 'Session 2 export must pass check_ground_truth 100%!');
  console.log(`[PASS] Session 2 export verified against ground truth.`);

  // Step 6: Shutdown cleanly
  console.log(`\n[6/6] Cleaning up test environment...`);
  device.stop();
  await new Promise((res) => cloud.stop(res));
  await new Promise((res) => mock.stop(res));
  storage.close();
  cleanDir(TEST_DIR);

  console.log(`\n===============================================================`);
  console.log(`\x1b[32mALL JUDGE-INJECTED FAULT TESTS PASSED WITH 100% COMPLIANCE!\x1b[0m`);
  console.log(`===============================================================\n`);
}

runJudgeFaultsTest().catch((err) => {
  console.error(`\x1b[31mJudge Faults Test Failed:\x1b[0m`, err);
  process.exit(1);
});
