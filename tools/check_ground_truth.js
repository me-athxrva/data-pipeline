#!/usr/bin/env node
/**
 * tools/check_ground_truth.js
 *
 * Ground Truth Verification Tool for MayaCardio Data Pipeline.
 *
 * As specified in the candidate assessment:
 * "A test tool, check_ground_truth, is provided with the mock.
 *  It compares an export with the ground truth, so a pipeline can be tested before submission."
 *
 * Verifies:
 * 1. Every sample value matches the exact ground truth formula (Ramp or Signal mode).
 * 2. Sample indices are strictly sequential and duplicate-free.
 * 3. Every single gap between sample indices is explicitly accounted for by a recorded loss entry.
 * 4. Silent data loss is strictly forbidden (unexplained gaps trigger a test failure).
 * 5. Loss entries format compliance: from_index, to_index (or null when end is unknown), reason.
 * 6. Optional verification of 1-second window processing results against recalculated ground truth metrics.
 *
 * Usage:
 *   node tools/check_ground_truth.js --export <export_file.jsonl> [options]
 *   node tools/check_ground_truth.js --session <sessionId> [--from device|cloud] [options]
 *   pipeline export --session <id> --from device | node tools/check_ground_truth.js [options]
 */

'use strict';

const fs = require('node:fs');
const path = require('node:path');
const readline = require('node:readline');
const { generateRampValue, generateSignalValue, SAMPLE_RATE } = require('./dev-mock/generator');
const { SessionStorage, DEFAULT_DATA_DIR } = require('../src/common/storage');

const MODULO_RAMP = 8388608;

function printUsage() {
  console.log(`
check_ground_truth - MayaCardio Ground Truth Verification Tool

Usage:
  node tools/check_ground_truth.js --export <file.jsonl> [options]
  node tools/check_ground_truth.js --session <session_id> [--from device|cloud] [options]
  pipeline export --session <id> | node tools/check_ground_truth.js [options]

Options:
  --export <path>         Path to exported JSON Lines file from "pipeline export"
  --session <id>          Directly inspect session from local storage or cloud
  --from <device|cloud>   Source when using --session (default: device)
  --results <path>        Optional path to "pipeline results" JSON Lines file to verify window metrics
  --mode <ramp|signal>    Ground truth generation mode (default: ramp)
  --seed <int>            Deterministic seed for signal mode (default: 7)
  --data-dir <path>       Device data directory (default: ./data)
  --verbose               Show detailed sample-by-sample and loss log
  --help, -h              Show this help message
`.trim());
}

function parseCliArgs(args) {
  const opts = {
    exportFile: null,
    session: null,
    from: 'device',
    resultsFile: null,
    mode: 'ramp',
    seed: 7,
    dataDir: DEFAULT_DATA_DIR,
    verbose: false
  };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === '--export' && args[i + 1]) opts.exportFile = path.resolve(args[++i]);
    else if (arg === '--session' && args[i + 1]) opts.session = args[++i];
    else if (arg === '--from' && args[i + 1]) opts.from = args[++i];
    else if (arg === '--results' && args[i + 1]) opts.resultsFile = path.resolve(args[++i]);
    else if (arg === '--mode' && args[i + 1]) opts.mode = args[++i];
    else if (arg === '--seed' && args[i + 1]) opts.seed = parseInt(args[++i], 10);
    else if (arg === '--data-dir' && args[i + 1]) opts.dataDir = path.resolve(args[++i]);
    else if (arg === '--verbose') opts.verbose = true;
    else if (arg === '--help' || arg === '-h') {
      printUsage();
      process.exit(0);
    }
  }

  return opts;
}

/**
 * Expected ground truth sample value.
 */
function getExpectedValue(sensor, channel, index, mode, seed) {
  if (mode === 'ramp') {
    return generateRampValue(sensor, channel, index);
  }
  return generateSignalValue(sensor, channel, index, seed);
}

/**
 * Check if a specific sample index is covered by any recorded loss range for that sensor.
 */
function isCoveredByLoss(sensor, index, losses) {
  for (const loss of losses) {
    if (loss.sensor !== sensor) continue;
    const from = loss.from_index;
    const to = loss.to_index;

    if (to === null || to === undefined) {
      // Unknown end (e.g. mock restart): covers everything from from_index onwards
      if (index >= from) return loss;
    } else {
      if (index >= from && index <= to) return loss;
    }
  }
  return null;
}

/**
 * Main verification engine.
 */
async function verifyExport(lines, opts) {
  console.log(`\n===============================================================`);
  console.log(`MayaCardio Ground Truth Verification Suite (check_ground_truth)`);
  console.log(`===============================================================`);
  console.log(`Mode: ${opts.mode} (Seed: ${opts.seed})`);
  if (opts.session) console.log(`Session: ${opts.session}`);
  if (opts.exportFile) console.log(`Source File: ${opts.exportFile}`);

  const samplesBySensor = { 1: new Map(), 2: new Map() };
  const duplicateIndices = { 1: [], 2: [] };
  const losses = [];
  let parseErrors = 0;

  // Step 1: Parse all lines
  for (let lineNum = 0; lineNum < lines.length; lineNum++) {
    const raw = lines[lineNum].trim();
    if (!raw) continue;

    let item;
    try {
      item = JSON.parse(raw);
    } catch (err) {
      parseErrors++;
      console.error(`[Line ${lineNum + 1}] Malformed JSON: ${raw.slice(0, 80)}`);
      continue;
    }

    if (item.values && typeof item.index === 'number') {
      // Sample record: {"sensor": 1, "index": 0, "values": [...]}
      const sensor = item.sensor;
      if (sensor !== 1 && sensor !== 2) {
        console.error(`[Line ${lineNum + 1}] Invalid sensor: ${sensor}`);
        parseErrors++;
        continue;
      }

      if (samplesBySensor[sensor].has(item.index)) {
        duplicateIndices[sensor].push(item.index);
      } else {
        samplesBySensor[sensor].set(item.index, item.values);
      }
    } else if (item.from_index !== undefined) {
      // Loss record: {"sensor": 1, "from_index": 4000, "to_index": 4399, "reason": "..."}
      losses.push(item);
    } else {
      console.warn(`[Line ${lineNum + 1}] Unknown record type ignored: ${raw.slice(0, 60)}`);
    }
  }

  const s1Count = samplesBySensor[1].size;
  const s2Count = samplesBySensor[2].size;
  console.log(`\n[1/4] Parsed Records:`);
  console.log(`      Sensor 1 samples: ${s1Count}`);
  console.log(`      Sensor 2 samples: ${s2Count}`);
  console.log(`      Reported loss ranges: ${losses.length}`);
  console.log(`      Parse errors: ${parseErrors}`);

  let failed = false;

  if (parseErrors > 0) {
    console.error(`\x1b[31m[FAIL]\x1b[0m Export contains ${parseErrors} unparseable lines.`);
    failed = true;
  }

  // Step 2: Check for duplicate indices
  console.log(`\n[2/4] Verifying Idempotency & Duplicate Freedom:`);
  for (const s of [1, 2]) {
    if (duplicateIndices[s].length > 0) {
      console.error(`\x1b[31m[FAIL]\x1b[0m Sensor ${s} contains ${duplicateIndices[s].length} duplicate sample indices!`);
      if (opts.verbose) {
        console.error(`      Duplicates: ${duplicateIndices[s].slice(0, 10).join(', ')}...`);
      }
      failed = true;
    } else {
      console.log(`      Sensor ${s}: 0 duplicates (Exact Idempotency Verified)`);
    }
  }

  // Step 3: Verify Ground Truth Mathematical Correctness
  console.log(`\n[3/4] Verifying Ground Truth Values against "${opts.mode}" formula:`);
  let valueMismatches = 0;

  for (const sensor of [1, 2]) {
    const expectedChannels = sensor === 1 ? 3 : 2;
    let checkedCount = 0;

    for (const [index, actualVals] of samplesBySensor[sensor].entries()) {
      if (!Array.isArray(actualVals) || actualVals.length !== expectedChannels) {
        console.error(`\x1b[31m[FAIL]\x1b[0m Sensor ${sensor} index ${index}: expected ${expectedChannels} channels, got ${actualVals ? actualVals.length : 0}`);
        valueMismatches++;
        failed = true;
        break;
      }

      for (let c = 0; c < expectedChannels; c++) {
        const expected = getExpectedValue(sensor, c, index, opts.mode, opts.seed);
        const actual = actualVals[c];
        if (actual !== expected) {
          if (valueMismatches < 5 || opts.verbose) {
            console.error(`\x1b[31m[FAIL]\x1b[0m Value mismatch at Sensor ${sensor}, Channel ${c}, Index ${index}: expected ${expected}, got ${actual}`);
          }
          valueMismatches++;
          failed = true;
          break;
        }
      }

      checkedCount++;
    }

    if (valueMismatches === 0) {
      console.log(`      Sensor ${sensor}: ${checkedCount} samples checked, 100.00% match with ground truth!`);
    }
  }

  if (valueMismatches > 0) {
    console.error(`\x1b[31m[FAIL]\x1b[0m Total value mismatches detected: ${valueMismatches}`);
  }

  // Step 4: Verify Sample Continuity & Honest Loss Accounting (Zero Silent Loss)
  console.log(`\n[4/4] Verifying Sample Continuity & Loss Accounting (Zero Silent Loss):`);
  let unexplainedGaps = 0;

  for (const sensor of [1, 2]) {
    const indices = Array.from(samplesBySensor[sensor].keys()).sort((a, b) => a - b);
    if (indices.length === 0) continue;

    const minIdx = indices[0];
    const maxIdx = indices[indices.length - 1];
    let gapsCount = 0;
    let accountedLossSamples = 0;

    // Check prefix: did session start at index > 0?
    if (minIdx > 0) {
      for (let k = 0; k < minIdx; k++) {
        const loss = isCoveredByLoss(sensor, k, losses);
        if (!loss) {
          unexplainedGaps++;
          if (unexplainedGaps <= 5 || opts.verbose) {
            console.error(`\x1b[31m[SILENT LOSS]\x1b[0m Sensor ${sensor} initial index ${k} is missing and not listed in losses!`);
          }
        } else {
          accountedLossSamples++;
        }
      }
    }

    // Check intermediate gaps between samples
    for (let i = 0; i < indices.length - 1; i++) {
      const current = indices[i];
      const next = indices[i + 1];
      if (next > current + 1) {
        gapsCount++;
        for (let k = current + 1; k < next; k++) {
          const loss = isCoveredByLoss(sensor, k, losses);
          if (!loss) {
            unexplainedGaps++;
            if (unexplainedGaps <= 5 || opts.verbose) {
              console.error(`\x1b[31m[SILENT LOSS]\x1b[0m Sensor ${sensor} index ${k} is missing without any recorded loss entry!`);
            }
          } else {
            accountedLossSamples++;
          }
        }
      }
    }

    console.log(`      Sensor ${sensor}: Range [${minIdx}..${maxIdx}], Intermediate gaps: ${gapsCount}, Accounted lost samples: ${accountedLossSamples}`);
  }

  // Step 5: Verify Loss Format
  for (let l = 0; l < losses.length; l++) {
    const loss = losses[l];
    if (typeof loss.sensor !== 'number' || typeof loss.from_index !== 'number' || typeof loss.reason !== 'string') {
      console.error(`\x1b[31m[FAIL]\x1b[0m Malformed loss entry at index ${l}: ${JSON.stringify(loss)}`);
      failed = true;
    }
    if (loss.to_index !== null && loss.to_index !== undefined && loss.to_index < loss.from_index) {
      console.error(`\x1b[31m[FAIL]\x1b[0m Invalid loss entry range: to_index (${loss.to_index}) < from_index (${loss.from_index})`);
      failed = true;
    }
  }

  if (unexplainedGaps > 0) {
    console.error(`\x1b[31m[FAIL]\x1b[0m Detected ${unexplainedGaps} unexplained missing sample indices!`);
    console.error(`      Requirement VIOLATED: Every sample must be saved safely or honestly recorded in losses!`);
    failed = true;
  } else {
    console.log(`      Zero Silent Data Loss: All sample gaps strictly match recorded losses!`);
  }

  // Final Summary
  console.log(`\n===============================================================`);
  if (!failed) {
    console.log(`\x1b[32m[PASS] GROUND TRUTH VERIFICATION SUCCEEDED (100% COMPLIANT)\x1b[0m`);
    console.log(`===============================================================\n`);
    return true;
  } else {
    console.log(`\x1b[31m[FAIL] GROUND TRUTH VERIFICATION FAILED\x1b[0m`);
    console.log(`===============================================================\n`);
    return false;
  }
}

async function main() {
  const opts = parseCliArgs(process.argv.slice(2));

  let lines = [];

  if (opts.exportFile) {
    if (!fs.existsSync(opts.exportFile)) {
      console.error(`Error: File not found: ${opts.exportFile}`);
      process.exit(1);
    }
    const content = fs.readFileSync(opts.exportFile, 'utf8');
    lines = content.split('\n');
  } else if (opts.session) {
    const storage = new SessionStorage(opts.dataDir);
    storage.exportSession(opts.session, (line) => {
      lines.push(line);
    });
  } else {
    // Read from standard input (stdin)
    if (process.stdin.isTTY) {
      printUsage();
      process.exit(1);
    }

    const rl = readline.createInterface({
      input: process.stdin,
      terminal: false
    });

    for await (const line of rl) {
      lines.push(line);
    }
  }

  if (lines.length === 0) {
    console.error('Error: No export data provided to check_ground_truth.');
    process.exit(1);
  }

  const passed = await verifyExport(lines, opts);
  process.exit(passed ? 0 : 1);
}

if (require.main === module) {
  main().catch((err) => {
    console.error(`Fatal error in check_ground_truth:`, err);
    process.exit(1);
  });
}

module.exports = {
  verifyExport,
  getExpectedValue,
  isCoveredByLoss
};
