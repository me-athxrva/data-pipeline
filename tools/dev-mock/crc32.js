/**
 * tools/dev-mock/crc32.js
 *
 * Standard IEEE 802.3 CRC-32 implementation using the 0xEDB88320 reversed polynomial
 * (identical to Ethernet, PNG, zlib, and gzip).
 *
 * Provides deterministic 8-character uppercase hex output.
 */

'use strict';

// Generator polynomial 0x04C11DB7 reversed: 0xEDB88320
const POLYNOMIAL = 0xEDB88320;

// Precompute 256-entry lookup table for CRC-32
const TABLE = new Uint32Array(256);
for (let i = 0; i < 256; i++) {
  let c = i;
  for (let k = 0; k < 8; k++) {
    c = (c & 1) ? (POLYNOMIAL ^ (c >>> 1)) : (c >>> 1);
  }
  TABLE[i] = c >>> 0;
}

/**
 * Calculate CRC-32 as an unsigned 32-bit integer.
 * @param {string|Buffer} input - Input text or buffer.
 * @returns {number} Unsigned 32-bit integer CRC.
 */
function crc32Int(input) {
  const buf = typeof input === 'string' ? Buffer.from(input, 'utf8') : input;
  let crc = 0xFFFFFFFF;
  for (let i = 0; i < buf.length; i++) {
    crc = (TABLE[(crc ^ buf[i]) & 0xFF] ^ (crc >>> 8)) >>> 0;
  }
  return (crc ^ 0xFFFFFFFF) >>> 0;
}

/**
 * Calculate CRC-32 formatted as an 8-character uppercase hexadecimal string.
 * Example: "4A17C2F9"
 * @param {string|Buffer} input - Input text or buffer.
 * @returns {string} 8-character uppercase hex string.
 */
function crc32(input) {
  const intVal = crc32Int(input);
  return intVal.toString(16).toLowerCase().padStart(8, '0');
}

module.exports = {
  crc32,
  crc32Int,
  POLYNOMIAL
};
