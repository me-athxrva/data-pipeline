/**
 * src/common/env.js
 *
 * Lightweight, zero-dependency .env loader.
 * Loads variables from .env into process.env without overriding existing environment variables.
 */

'use strict';

const fs = require('node:fs');
const path = require('node:path');

function loadEnv(envPath = path.resolve(__dirname, '..', '..', '.env')) {
  if (!fs.existsSync(envPath)) return;

  try {
    const content = fs.readFileSync(envPath, 'utf8');
    const lines = content.split('\n');

    for (const rawLine of lines) {
      const line = rawLine.trim();
      // Ignore empty lines and comments
      if (!line || line.startsWith('#')) continue;

      const eqIdx = line.indexOf('=');
      if (eqIdx === -1) continue;

      const key = line.slice(0, eqIdx).trim();
      let val = line.slice(eqIdx + 1).trim();

      // Remove surrounding quotes if present
      if ((val.startsWith('"') && val.endsWith('"')) || (val.startsWith("'") && val.endsWith("'"))) {
        val = val.slice(1, -1);
      }

      // Do not overwrite existing process.env variables
      if (process.env[key] === undefined) {
        process.env[key] = val;
      }
    }
  } catch (err) {
    console.warn(`[env] Warning: failed to parse ${envPath}: ${err.message}`);
  }
}

// Automatically load on require
loadEnv();

module.exports = { loadEnv };
