#!/usr/bin/env node
/**
 * tools/dev-mock/index.js
 *
 * CLI Entrypoint for biosignal-mock (Temporary Local Development Server).
 *
 * IMPORTANT:
 * This is a temporary development/testing mock created because the official
 * MayaCardio executable was not provided. It is NOT the official executable.
 */

'use strict';

const { parseConfig } = require('./config');
const { createBiosignalServer } = require('./server');

try {
  const config = parseConfig(process.argv.slice(2));
  const mockServer = createBiosignalServer(config);

  mockServer.start((err) => {
    if (err) {
      console.error(`[biosignal-mock] Fatal error starting server:`, err.message);
      process.exit(1);
    }
  });

  // Handle graceful termination
  const shutdown = () => {
    console.log('\n[biosignal-mock] Shutting down server...');
    mockServer.stop(() => {
      process.exit(0);
    });
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
} catch (err) {
  console.error(`[biosignal-mock] Startup error: ${err.message}`);
  process.exit(1);
}
