/**
 * tools/dev-mock/protocol.js
 *
 * Wire protocol handling for biosignal-mock:
 * - Line-delimited JSON stream framing (TCP chunk buffering)
 * - Request validation and error responses
 * - Exact-byte JSON response formatting with embedded CRC-32 data text
 */

'use strict';

const { crc32 } = require('./crc32');

/**
 * Creates a stateful line buffer to handle chunked TCP data streams.
 * Accumulates incoming byte chunks and emits complete newline-delimited strings.
 */
function createLineBuffer() {
  let buffer = '';

  return {
    /**
     * Push incoming data chunk and extract any completed lines.
     * @param {Buffer|string} chunk
     * @returns {string[]} Array of complete request lines.
     */
    push(chunk) {
      buffer += typeof chunk === 'string' ? chunk : chunk.toString('utf8');
      const lines = [];
      let newlineIdx;

      while ((newlineIdx = buffer.indexOf('\n')) !== -1) {
        let line = buffer.slice(0, newlineIdx);
        // Trim optional trailing carriage return (\r)
        if (line.endsWith('\r')) {
          line = line.slice(0, -1);
        }
        buffer = buffer.slice(newlineIdx + 1);

        const trimmed = line.trim();
        if (trimmed.length > 0) {
          lines.push(trimmed);
        }
      }

      return lines;
    },

    /**
     * Clear the buffer on connection reset/close.
     */
    reset() {
      buffer = '';
    }
  };
}

/**
 * Validate a parsed request object against protocol specifications.
 *
 * @param {string} line - Raw input line.
 * @param {number} totalSamples - Max samples allowed in current session (duration * 400).
 * @returns {{ valid: boolean, request?: object, error?: string, message?: string }}
 */
function parseAndValidateRequest(line, totalSamples) {
  let req;
  try {
    req = JSON.parse(line);
  } catch (err) {
    return {
      valid: false,
      error: 'invalid_json',
      message: `Failed to parse request JSON: ${err.message}`
    };
  }

  if (typeof req !== 'object' || req === null || Array.isArray(req)) {
    return {
      valid: false,
      error: 'invalid_request',
      message: 'Request must be a JSON object.'
    };
  }

  if (!req.cmd) {
    return {
      valid: false,
      error: 'invalid_request',
      message: 'Missing required field: "cmd".'
    };
  }

  if (req.cmd !== 'read') {
    return {
      valid: false,
      error: 'invalid_request',
      message: `Unsupported command: "${req.cmd}". Only "read" is supported.`
    };
  }

  if (req.sensor !== 1 && req.sensor !== 2) {
    return {
      valid: false,
      error: 'invalid_request',
      message: `Invalid sensor: ${req.sensor}. Must be 1 or 2.`
    };
  }

  if (req.from_index !== undefined) {
    if (typeof req.from_index !== 'number' || !Number.isInteger(req.from_index)) {
      return {
        valid: false,
        error: 'invalid_request',
        message: 'Field "from_index" must be an integer.'
      };
    }

    if (req.from_index < 0) {
      return {
        valid: false,
        error: 'invalid_request',
        message: 'Field "from_index" cannot be negative.'
      };
    }

    if (req.from_index >= totalSamples) {
      return {
        valid: false,
        error: 'invalid_request',
        message: `from_index (${req.from_index}) is beyond session end (limit: ${totalSamples}).`
      };
    }
  }

  return {
    valid: true,
    request: req
  };
}

/**
 * Build a valid protocol response string while preserving exact data text for CRC-32.
 *
 * Steps:
 * 1. Stringify `data` array to JSON string (`dataText`).
 * 2. Compute CRC-32 over `dataText`.
 * 3. Embed `dataText` verbatim in the final newline-terminated JSON response.
 *
 * @param {object} params
 * @param {string} params.sessionId
 * @param {number} params.sensor
 * @param {number} params.firstIndex
 * @param {number} params.count
 * @param {Array} params.lost
 * @param {boolean} params.sessionEnd
 * @param {Array<Array<number>>} params.data
 * @param {string} [params.overrideCrc] - Optional override for fault injection
 * @returns {{ responseText: string, dataText: string, crc32: string }}
 */
function buildResponse({
  sessionId,
  sensor,
  firstIndex,
  count,
  lost = [],
  sessionEnd = false,
  data,
  overrideCrc = null
}) {
  // Step 1: Generate exact data JSON string
  const dataText = JSON.stringify(data);

  // Step 2: Calculate CRC-32 over exact data text
  const calculatedCrc = crc32(dataText);
  const finalCrc = overrideCrc !== null ? overrideCrc : calculatedCrc;

  // Step 3: Insert exact dataText into the final response JSON
  // We assemble with template literal so dataText is verbatim without re-serialization.
  const responseText =
    `{"session_id":${JSON.stringify(sessionId)},"sensor":${sensor},"first_index":${firstIndex},"count":${count},"lost":${JSON.stringify(lost)},"session_end":${sessionEnd},"crc32":${JSON.stringify(finalCrc)},"data":${dataText}}\n`;

  return {
    responseText,
    dataText,
    crc32: finalCrc
  };
}

/**
 * Build a standardized error response.
 *
 * @param {string} error - Error code (e.g. 'invalid_request', 'invalid_json')
 * @param {string} [message] - Descriptive message
 * @returns {string} Newline-terminated JSON error string
 */
function buildErrorResponse(error, message) {
  const payload = { error };
  if (message) {
    payload.message = message;
  }
  return JSON.stringify(payload) + '\n';
}

module.exports = {
  createLineBuffer,
  parseAndValidateRequest,
  buildResponse,
  buildErrorResponse
};
