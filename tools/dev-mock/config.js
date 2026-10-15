/**
 * tools/dev-mock/config.js
 *
 * Command-line argument parser and configuration validator for biosignal-mock.
 * Uses only Node.js standard library APIs.
 */

'use strict';

const DEFAULTS = {
  mode: 'signal',
  seed: 7,
  duration: 3600,
  port: 7100,
  host: '127.0.0.1',
  chunkSize: 100,
  quiet: false,
  duplicateRate: 0,
  corruptCrcRate: 0,
  stallRate: 0,
  stallMs: 500,
  sensorLossRate: 0,
  restartAfter: 0
};

/**
 * Print CLI help documentation.
 */
function printHelp() {
  const helpText = `
biosignal-mock: Local development mock server for MayaCardio candidate assessment.
(Note: This is a temporary test replacement and NOT the official MayaCardio executable)

Usage:
  node tools/dev-mock/index.js [options]

Core Options:
  --mode <signal|ramp>      Signal generation mode (default: ${DEFAULTS.mode})
  --seed <int>              Deterministic seed for signal generation (default: ${DEFAULTS.seed})
  --duration <seconds>      Total session duration in seconds (default: ${DEFAULTS.duration})
  --port <int>              TCP listen port (default: ${DEFAULTS.port})
  --host <string>           TCP listen address (default: ${DEFAULTS.host})
  --chunk-size <int>        Default samples returned per read request (default: ${DEFAULTS.chunkSize})
  --quiet                   Suppress per-read logs (default: false)
  --help                    Show this help message

Fault Injection Options (Testing & Resilience):
  --duplicate-rate <0..1>   Rate of sending duplicate identical responses (default: 0)
  --corrupt-crc-rate <0..1> Rate of corrupting the CRC-32 checksum (default: 0)
  --stall-rate <0..1>       Rate of delaying responses (default: 0)
  --stall-ms <int>          Delay in milliseconds when stall is triggered (default: ${DEFAULTS.stallMs})
  --sensor-loss-rate <0..1> Rate of reporting lost sample ranges (default: 0)
  --restart-after <seconds> Automatically exit after N seconds (default: 0 / disabled)

Examples:
  node tools/dev-mock/index.js --mode ramp --seed 7 --duration 60 --port 7100
  node tools/dev-mock/index.js --mode signal --seed 7 --duration 3600 --port 7100
`;
  console.log(helpText.trim());
}

/**
 * Parse and validate CLI arguments.
 * @param {string[]} argv - process.argv array.
 * @returns {object} Parsed configuration object.
 */
function parseConfig(argv = process.argv.slice(2)) {
  const config = { ...DEFAULTS };

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];

    if (arg === '--help' || arg === '-h') {
      printHelp();
      process.exit(0);
    }

    if (arg === '--quiet') {
      config.quiet = true;
      continue;
    }

    if (arg.startsWith('--')) {
      const key = arg.slice(2);
      const val = argv[++i];

      if (val === undefined || val.startsWith('--')) {
        throw new Error(`Missing value for argument: ${arg}`);
      }

      switch (key) {
        case 'mode':
          if (val !== 'signal' && val !== 'ramp') {
            throw new Error(`Invalid mode "${val}". Must be "signal" or "ramp".`);
          }
          config.mode = val;
          break;

        case 'seed': {
          const num = parseInt(val, 10);
          if (isNaN(num)) throw new Error(`Invalid seed: "${val}". Must be an integer.`);
          config.seed = num;
          break;
        }

        case 'duration': {
          const num = parseInt(val, 10);
          if (isNaN(num) || num <= 0) throw new Error(`Invalid duration: "${val}". Must be a positive integer.`);
          config.duration = num;
          break;
        }

        case 'port': {
          const num = parseInt(val, 10);
          if (isNaN(num) || num < 1 || num > 65535) {
            throw new Error(`Invalid port: "${val}". Must be between 1 and 65535.`);
          }
          config.port = num;
          break;
        }

        case 'host':
          config.host = val;
          break;

        case 'chunk-size': {
          const num = parseInt(val, 10);
          if (isNaN(num) || num <= 0) throw new Error(`Invalid chunk-size: "${val}". Must be a positive integer.`);
          config.chunkSize = num;
          break;
        }

        case 'duplicate-rate': {
          const num = parseFloat(val);
          if (isNaN(num) || num < 0 || num > 1) {
            throw new Error(`Invalid duplicate-rate: "${val}". Must be between 0 and 1.`);
          }
          config.duplicateRate = num;
          break;
        }

        case 'corrupt-crc-rate': {
          const num = parseFloat(val);
          if (isNaN(num) || num < 0 || num > 1) {
            throw new Error(`Invalid corrupt-crc-rate: "${val}". Must be between 0 and 1.`);
          }
          config.corruptCrcRate = num;
          break;
        }

        case 'stall-rate': {
          const num = parseFloat(val);
          if (isNaN(num) || num < 0 || num > 1) {
            throw new Error(`Invalid stall-rate: "${val}". Must be between 0 and 1.`);
          }
          config.stallRate = num;
          break;
        }

        case 'stall-ms': {
          const num = parseInt(val, 10);
          if (isNaN(num) || num < 0) throw new Error(`Invalid stall-ms: "${val}". Must be non-negative.`);
          config.stallMs = num;
          break;
        }

        case 'sensor-loss-rate': {
          const num = parseFloat(val);
          if (isNaN(num) || num < 0 || num > 1) {
            throw new Error(`Invalid sensor-loss-rate: "${val}". Must be between 0 and 1.`);
          }
          config.sensorLossRate = num;
          break;
        }

        case 'restart-after': {
          const num = parseInt(val, 10);
          if (isNaN(num) || num < 0) throw new Error(`Invalid restart-after: "${val}". Must be non-negative.`);
          config.restartAfter = num;
          break;
        }

        default:
          throw new Error(`Unknown option: "${arg}". Use --help for usage.`);
      }
    } else {
      throw new Error(`Unexpected positional argument: "${arg}". Use --help for usage.`);
    }
  }

  return config;
}

module.exports = {
  DEFAULTS,
  parseConfig,
  printHelp
};
