# `biosignal-mock` — Temporary Local Development Server

> **IMPORTANT DISCLAIMER**
> This implementation is a **temporary local development and testing mock server** created for the MayaCardio Software Systems Engineering Candidate Assessment.
>
> The official `biosignal-mock` executable was intended to be provided with the assessment materials, but was not included. **This tool is NOT the official MayaCardio executable.** It implements the documented wire protocol, sensor configurations, and mathematical behaviors as faithfully as possible to unblock development and automated testing of the ingestion pipeline.
>
> This directory is strictly isolated under `tools/dev-mock/` so that once the official binary is provided, this entire directory can be removed without modifying the core ingestion pipeline.

---

## 1. Architecture & Pipeline Isolation

The ingestion pipeline must interact with the biosignal source strictly through a decoupled network client interface:

```javascript
// Example of clean pipeline abstraction
const { BiosignalSource } = require('./pipeline/source');

const source = new BiosignalSource({
  host: '127.0.0.1',
  port: 7100
});

await source.connect();
const chunk = await source.read({ sensor: 1 });
```

The production ingestion pipeline must **never** import or rely on any code inside `tools/dev-mock/`. When the official binary is provided:
1. Stop running this mock.
2. Launch the official binary.
3. Keep the ingestion pipeline pointed at the same host and port.
4. Delete `tools/dev-mock/`.

---

## 2. Directory Structure

```text
tools/
└── dev-mock/
    ├── index.js        # CLI executable entrypoint
    ├── server.js       # TCP server, session management, client isolation
    ├── generator.js    # Ramp and Signal sample generation logic
    ├── crc32.js        # IEEE 802.3 CRC-32 checksum computation
    ├── protocol.js     # Wire protocol framing, JSON serialization, validation
    ├── config.js       # CLI argument parsing, defaults, and validation
    ├── test-client.js  # Automated protocol and verification test suite
    └── README.md       # Full documentation and usage guide
```

---

## 3. Wire Protocol

The server communicates over **TCP** using newline-delimited JSON (`\n`).

### 3.1 Single Client Enforcement
The server accepts **exactly one TCP client at a time**:
- When the first client connects, the session begins.
- Any subsequent connection attempt while a client is connected is immediately rejected/closed, and logged as:
  ```text
  [biosignal-mock] Rejecting additional client
  ```
- When the active client disconnects, another client is permitted to connect.

### 3.2 Request Format
The client sends single-line JSON requests terminated by `\n`:

- **Sequential Read (Sensor 1):**
  ```json
  {"cmd":"read","sensor":1}
  ```
- **Sequential Read (Sensor 2):**
  ```json
  {"cmd":"read","sensor":2}
  ```
- **Reread / Seek from Absolute Index:**
  ```json
  {"cmd":"read","sensor":1,"from_index":1200}
  ```

### 3.3 Response Format
The server responds with a single-line JSON object terminated by `\n`:

```json
{
  "session_id": "dev-session-1792085729770-66b55d08",
  "sensor": 1,
  "first_index": 0,
  "count": 100,
  "lost": [],
  "session_end": false,
  "crc32": "B8ED35EF",
  "data": [
    [0, 1, 2],
    [3, 4, 5],
    ...
  ]
}
```

#### Fields Description:
| Field | Type | Description |
|---|---|---|
| `session_id` | `string` | Unique identifier generated when the server starts (`dev-session-<timestamp>-<rand>`). Remains constant throughout the process lifetime. |
| `sensor` | `number` | `1` (3 channels) or `2` (2 channels). |
| `first_index` | `number` | Absolute sample index of the first item in `data`. |
| `count` | `number` | Number of samples contained in this response chunk. |
| `lost` | `array` | List of lost sample ranges, e.g. `[{"sensor":1,"start_index":500,"end_index":599,"reason":"injected_loss"}]`. Empty in normal operation. |
| `session_end` | `boolean` | `true` when this response reaches or exceeds the configured total session duration. |
| `crc32` | `string` | 8-character uppercase hex CRC-32 computed strictly over the raw text of the `data` array (`[...]`). |
| `data` | `array` | Array of multi-channel sample arrays. |

### 3.4 Error Responses
Malformed or unsupported requests return a JSON error line:
```json
{"error":"invalid_request","message":"sensor must be 1 or 2"}
```
```json
{"error":"invalid_json","message":"Failed to parse request JSON"}
```

---

## 4. Cursor & `from_index` Behavior

The server maintains two independent sequential cursors:
- `sensor1Cursor` (starts at `0`)
- `sensor2Cursor` (starts at `0`)

### Normal Sequential Reads
When `from_index` is omitted:
- The read begins at the sensor's current cursor.
- The cursor advances by `count` samples.

### `from_index` Reread Requests
When `from_index` is supplied:
- The read starts at the exact requested `from_index`.
- The sequential cursor is updated **only if** `from_index + count` extends beyond the current cursor (`cursor = Math.max(cursor, from_index + count)`).
- Rereading earlier data does **not** move the cursor backwards or create duplicates in the normal stream.

---

## 5. Sampling Rate & Channels

Both sensors operate at **400 Hz** (400 samples/second):
- Total samples per session = `duration * 400`.
- Valid sample indices range from `0` to `(duration * 400) - 1`.

### Sensor Configuration:
- **Sensor 1 (3 channels):** Channels `0`, `1`, and `2`. Sample format: `[ch0, ch1, ch2]`
- **Sensor 2 (2 channels):** Channels `0` and `1`. Sample format: `[ch0, ch1]`

---

## 6. Generation Modes

### 6.1 Ramp Mode (`--mode ramp`)
Implements the exact mathematical specification from the assessment:

- **Sensor 1:**
  $$\text{value}(sensor=1, channel=c, index=i) = (3 \times i + c) \pmod{8388608}$$
- **Sensor 2:**
  $$\text{value}(sensor=2, channel=c, index=i) = (2 \times i + c) \pmod{8388608}$$

Where:
- $c \in \{0, 1, 2\}$ for sensor 1; $c \in \{0, 1\}$ for sensor 2.
- $i$ is the **absolute sample index**.
- $8388608 = 2^{23}$ (24-bit range).

#### Reference Values:
- **Sensor 1, index 0:** `[0, 1, 2]`
- **Sensor 1, index 1:** `[3, 4, 5]`
- **Sensor 1, index 2:** `[6, 7, 8]`
- **Sensor 2, index 0:** `[0, 1]`
- **Sensor 2, index 1:** `[2, 3]`
- **Sensor 2, index 2:** `[4, 5]`

### 6.2 Signal Mode (`--mode signal`)
Synthetic physiological waveform simulation designed for pipeline and algorithm testing:
- **Sampling Rate:** 400 Hz.
- **Sensor 1, Channel 2:** Prominent, sharp periodic peaks (simulating R-peaks) on top of a baseline, repeating at an approximately stable heart rate between 65 and 85 BPM (derived deterministically from `--seed`). This allows testing heart-rate extraction algorithms.
- **Sensor 1, Channels 0 & 1:** Bio-potential baseline signals with subtle respiratory wander (0.25 Hz).
- **Sensor 2, Channels 0 & 1:** Optical pulsatile waveforms (PPG-like) with realistic systolic peaks and dicrotic notches.
- **Deterministic Noise:** High-frequency noise derived deterministically from `(seed, sensor, channel, index)`.

> **Note:** This signal mode is an approximation designed to enable end-to-end testing of peak detection and signal ingestion. It is **not** MayaCardio's proprietary generator algorithm.

---

## 7. CRC-32 Verification

To guarantee data integrity across the wire:
1. The server serializes `data` to a JSON string:
   ```javascript
   const dataText = JSON.stringify(data);
   ```
2. It computes standard **IEEE 802.3 CRC-32** (polynomial `0xEDB88320`, initial `0xFFFFFFFF`, final XOR `0xFFFFFFFF`) directly over the UTF-8 bytes of `dataText`.
3. It formats the checksum as an **8-character uppercase hexadecimal string** (e.g. `"B8ED35EF"`).
4. It embeds `dataText` verbatim into the response line:
   ```javascript
   `{...,"crc32":"B8ED35EF","data":${dataText}}\n`
   ```

Clients must calculate CRC-32 over the exact substring from the opening `[` of `"data":` to the closing `]`.

---

## 8. CLI Usage & Flags

### 8.1 Starting the Server

```bash
# Ramp mode (deterministic validation):
node tools/dev-mock/index.js --mode ramp --seed 7 --duration 60 --port 7100

# Signal mode (physiological testing):
node tools/dev-mock/index.js --mode signal --seed 7 --duration 3600 --port 7100

# View help:
node tools/dev-mock/index.js --help
```

### 8.2 Command Line Options

| Flag | Default | Description |
|---|---|---|
| `--mode <signal\|ramp>` | `signal` | Generation mode |
| `--seed <int>` | `7` | Integer seed for deterministic generation |
| `--duration <seconds>` | `3600` | Duration in seconds ($duration \times 400$ total samples) |
| `--port <int>` | `7100` | TCP port to listen on |
| `--host <string>` | `127.0.0.1` | TCP host address to bind to |
| `--chunk-size <int>` | `100` | Default samples per read response |
| `--quiet` | `false` | Suppress per-read terminal logging |
| `--help` | — | Display help menu |

### 8.3 Fault Injection Flags (Resilience & Pipeline Testing)

| Flag | Default | Description |
|---|---|---|
| `--duplicate-rate <0..1>` | `0` | Probability of sending an identical duplicate response (tests idempotency) |
| `--corrupt-crc-rate <0..1>` | `0` | Probability of sending an invalid CRC32 (`DEADBEEF`) (tests rejection) |
| `--stall-rate <0..1>` | `0` | Probability of delaying responses (tests timeout and retry logic) |
| `--stall-ms <int>` | `500` | Delay duration in milliseconds when stall is triggered |
| `--sensor-loss-rate <0..1>` | `0` | Probability of reporting a lost range in the `lost` array |
| `--restart-after <seconds>` | `0` | Auto-terminate after $N$ seconds (tests crash recovery & session ID change) |

Example enabling fault injection:
```bash
node tools/dev-mock/index.js --mode signal --stall-rate 0.05 --stall-ms 1000 --corrupt-crc-rate 0.02
```

---

## 9. Automated Test Client

An automated verification test script is provided:

```bash
node tools/dev-mock/test-client.js
```

You can also test against a running instance on another port or host:
```bash
node tools/dev-mock/test-client.js --port 7100 --host 127.0.0.1
```

The test script validates:
1. TCP connection handshake.
2. Sensor 1 reading and channel count (3 channels).
3. Sensor 2 reading and channel count (2 channels).
4. `first_index` tracking.
5. Chunk `count` matching data length.
6. Exact ramp values matching formulas for both sensors.
7. Independent calculation of CRC-32 on raw `data` text.
8. Exact CRC-32 comparison.
9. Rereading specific ranges via `from_index`.
10. Correct cursor positioning after `from_index`.
11. `session_end` handling and boundary clamping.
12. Single-client concurrency rejection.
13. Protocol error responses.

---

## 10. Memory & Performance

The mock generates all data **on demand** and does not pre-allocate or cache the full dataset in memory. It operates with a minimal memory footprint (< 30 MB RSS), well within the resource constraints of Raspberry Pi-class devices (max 4 CPU cores, 2 GB RAM).
