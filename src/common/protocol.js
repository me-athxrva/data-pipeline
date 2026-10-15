/**
 * src/common/protocol.js
 *
 * Common protocol definitions, IEEE 802.3 CRC-32 checksums,
 * message framing, and constants for MayaCardio pipeline.
 */

'use strict';

const SAMPLE_RATE = 400; // 400 Hz
const WINDOW_SIZE = 400; // 1 second = 400 samples
const SENSOR_CHANNELS = {
  1: 3,
  2: 2
};

// Standard IEEE 802.3 CRC-32 table (reversed polynomial 0xEDB88320)
const POLYNOMIAL = 0xEDB88320;
const CRC_TABLE = new Uint32Array(256);
for (let i = 0; i < 256; i++) {
  let c = i;
  for (let k = 0; k < 8; k++) {
    c = (c & 1) ? (POLYNOMIAL ^ (c >>> 1)) : (c >>> 1);
  }
  CRC_TABLE[i] = c >>> 0;
}

/**
 * Calculate CRC-32 as 8-character lowercase hexadecimal string.
 * @param {string|Buffer} input
 * @returns {string} 8-character hex string
 */
function crc32(input) {
  const buf = typeof input === 'string' ? Buffer.from(input, 'utf8') : input;
  let crc = 0xFFFFFFFF;
  for (let i = 0; i < buf.length; i++) {
    crc = (CRC_TABLE[(crc ^ buf[i]) & 0xFF] ^ (crc >>> 8)) >>> 0;
  }
  crc = (crc ^ 0xFFFFFFFF) >>> 0;
  return crc.toString(16).toLowerCase().padStart(8, '0');
}

/**
 * Verify CRC-32 case-insensitively against dataText.
 * @param {string} dataText - The raw substring between '[' and ']' of the data array.
 * @param {string} expectedCrc - The checksum reported in the response.
 * @returns {boolean} True if checksum matches.
 */
function verifyCrc(dataText, expectedCrc) {
  if (!expectedCrc || typeof expectedCrc !== 'string') return false;
  const computed = crc32(dataText);
  return computed === expectedCrc.toLowerCase();
}

/**
 * Extract raw data text from a biosignal response line without re-serializing.
 * The assessment requires CRC over exact text from opening '[' of data to matching ']'.
 * @param {string} line - Full JSON response line.
 * @returns {string|null} Extracted data JSON array string.
 */
function extractDataText(line) {
  const marker = '"data":';
  const startIdx = line.indexOf(marker);
  if (startIdx === -1) return null;
  const arrayStart = line.indexOf('[', startIdx + marker.length);
  if (arrayStart === -1) return null;

  // Bracket counter to find exact matching closing bracket
  let depth = 0;
  let inString = false;
  let escape = false;

  for (let i = arrayStart; i < line.length; i++) {
    const ch = line[i];
    if (escape) {
      escape = false;
      continue;
    }
    if (ch === '\\') {
      escape = true;
      continue;
    }
    if (ch === '"') {
      inString = !inString;
      continue;
    }
    if (!inString) {
      if (ch === '[') depth++;
      else if (ch === ']') {
        depth--;
        if (depth === 0) {
          return line.slice(arrayStart, i + 1);
        }
      }
    }
  }
  return null;
}

module.exports = {
  SAMPLE_RATE,
  WINDOW_SIZE,
  SENSOR_CHANNELS,
  crc32,
  verifyCrc,
  extractDataText
};
