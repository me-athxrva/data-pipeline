#!/usr/bin/env node
/**
 * src/cli/pipeline.js
 *
 * Official CLI command runner for MayaCardio candidate assessment:
 * 1. pipeline status
 * 2. pipeline export --session ID --from device|cloud
 * 3. pipeline results --session ID
 * 4. pipeline start [--mock-port 7100] [--cloud-url http://...]
 */

'use strict';

require('../common/env');
const fs = require('node:fs');
const path = require('node:path');
const http = require('node:http');
const { DeviceService } = require('../device/device_service');
const { SessionStorage, DEFAULT_DATA_DIR } = require('../common/storage');

function printUsage() {
  const usage = `
pipeline - MayaCardio Bedside Device Data Pipeline CLI

Usage:
  pipeline status
  pipeline export --session <ID> --from <device|cloud>
  pipeline results --session <ID>
  pipeline start [options]

Commands:
  status                                 Print current mode, samples saved, in cloud, and backlog as one JSON line.
  export --session ID --from device|cloud Export session samples and losses as JSON lines.
  results --session ID                   Print 1-second window processing results as JSON lines.
  start                                  Start the pipeline device daemon.

Start Options:
  --mock-port <int>                      Port of biosignal-mock (default: 7100)
  --mock-host <string>                   Host of biosignal-mock (default: 127.0.0.1)
  --cloud-url <string>                   URL of cloud service (default: http://127.0.0.1:8080)
  --data-dir <path>                      Directory for persistent WAL storage
`;
  console.log(usage.trim());
}

function parseCliArgs(args) {
  const parsed = {
    command: args[0],
    session: null,
    from: 'device',
    mockPort: parseInt(process.env.MOCK_PORT || '7100', 10),
    mockHost: process.env.MOCK_HOST || '127.0.0.1',
    cloudUrl: process.env.CLOUD_URL || 'http://127.0.0.1:8080',
    dataDir: process.env.DATA_DIR ? path.resolve(process.env.DATA_DIR) : DEFAULT_DATA_DIR
  };

  for (let i = 1; i < args.length; i++) {
    const arg = args[i];
    if (arg === '--session' && args[i + 1]) parsed.session = args[++i];
    else if (arg === '--from' && args[i + 1]) parsed.from = args[++i];
    else if (arg === '--mock-port' && args[i + 1]) parsed.mockPort = parseInt(args[++i], 10);
    else if (arg === '--mock-host' && args[i + 1]) parsed.mockHost = args[++i];
    else if (arg === '--cloud-url' && args[i + 1]) parsed.cloudUrl = args[++i];
    else if (arg === '--data-dir' && args[i + 1]) parsed.dataDir = path.resolve(args[++i]);
    else if (arg === '--help' || arg === '-h') {
      printUsage();
      process.exit(0);
    }
  }

  return parsed;
}

async function handleStatus(opts) {
  const statusPath = path.join(opts.dataDir, 'status.json');
  if (fs.existsSync(statusPath)) {
    try {
      const raw = fs.readFileSync(statusPath, 'utf8').trim();
      const obj = JSON.parse(raw);
      // Required format: current mode, samples saved, samples in the cloud, and backlog. Printed as one JSON line.
      console.log(JSON.stringify({
        mode: obj.mode || 'offline',
        samples_saved: obj.samples_saved || 0,
        samples_in_cloud: obj.samples_in_cloud || 0,
        backlog: obj.backlog || 0
      }));
      return;
    } catch (_) {}
  }

  // Fallback if daemon is not running
  console.log(JSON.stringify({
    mode: 'offline',
    samples_saved: 0,
    samples_in_cloud: 0,
    backlog: 0
  }));
}

async function handleExport(opts) {
  if (!opts.session) {
    console.error('Error: --session ID is required.');
    process.exit(1);
  }

  if (opts.from === 'cloud') {
    // Fetch from cloud service
    const url = new URL(`/export?session=${opts.session}`, opts.cloudUrl);
    http.get(url, (res) => {
      if (res.statusCode !== 200) {
        console.error(`Cloud export failed with HTTP ${res.statusCode}`);
        process.exit(1);
      }
      res.pipe(process.stdout);
    }).on('error', (err) => {
      console.error(`Failed to reach cloud service: ${err.message}`);
      process.exit(1);
    });
  } else {
    // Export from local device storage
    const storage = new SessionStorage(opts.dataDir);
    storage.exportSession(opts.session, (line) => {
      process.stdout.write(line + '\n');
    });
  }
}

async function handleResults(opts) {
  if (!opts.session) {
    console.error('Error: --session ID is required.');
    process.exit(1);
  }

  const storage = new SessionStorage(opts.dataDir);
  const results = storage.loadResults(opts.session);
  for (const r of results) {
    process.stdout.write(JSON.stringify(r) + '\n');
  }
}

function handleStart(opts) {
  const service = new DeviceService({
    mockHost: opts.mockHost,
    mockPort: opts.mockPort,
    cloudUrl: opts.cloudUrl,
    dataDir: opts.dataDir
  });

  service.start();

  const shutdown = () => {
    console.log('\n[pipeline] Stopping daemon...');
    service.stop(() => process.exit(0));
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

// Main execution
const opts = parseCliArgs(process.argv.slice(2));

switch (opts.command) {
  case 'status':
    handleStatus(opts);
    break;

  case 'export':
    handleExport(opts);
    break;

  case 'results':
    handleResults(opts);
    break;

  case 'start':
    handleStart(opts);
    break;

  default:
    printUsage();
    process.exit((opts.command && opts.command !== '--help' && opts.command !== '-h') ? 1 : 0);
}
