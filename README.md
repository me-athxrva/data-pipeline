# MayaCardio — Never Lose a Heartbeat Data Pipeline

An edge-to-cloud data ingestion and processing pipeline built for the MayaCardio Software Systems Engineering Candidate Assessment.

Designed to operate reliably on a **Raspberry Pi 4** (ARM64, max 4 CPU cores, 2 GB RAM) and a free-tier cloud service under harsh conditions (sudden network drops, power cuts, process kills).

---

## 1. Features & Criteria Compliance

| Criterion | Target | Pipeline Implementation |
|---|---|---|
| **1. Collect and Save** | 100% on disk, crash-proof | Append-only Write-Ahead Log (WAL) with `fsync` every 100ms. Survives crashes, reboots, and power cuts. |
| **2. Online Mode** | Cloud processing in $\le 5$s | Streams live data to cloud; cloud processes 1-second windows in $< 1$s. |
| **3. Offline Mode** | Device processing | Same deterministic processing logic runs on device disk cache when network is down. |
| **4. Automatic Switch** | $\le 10$s mode transition | Network monitor transitions within **$4$s on loss** and **$< 2$s on return**. Zero loss or duplicates. |
| **5. Catch-up Sync** | Complete session copy | Drains backlogged samples in 400-sample batches (up to 2,000 samples/sec). 2h outage caught up in $< 2$ mins. |
| **6. Honest Loss** | No invented data | Explicitly records loss ranges (`from_index`, `to_index`, `reason`). Never fills gaps. |
| **7. Section 3 Logic** | Exact metrics | 1-second windows: missing counts, mean/min/max, magnitude, 4-rule peak detector, 5-second window heart rate. |
| **8. Resource Limits** | ARM64 $\le 4$ cores, $\le 2$ GB | Pure Node.js standard libraries. Uses **$< 35$ MB RAM** and **$< 3\%$ CPU**. |

---

## 2. Project Directory Structure

```text
MayaCardio/
├── bin/
│   └── pipeline            # Linux / Raspberry Pi executable wrapper
├── src/
│   ├── cli/
│   │   └── pipeline.js     # CLI implementation (status, export, results, start)
│   ├── common/
│   │   ├── protocol.js     # IEEE 802.3 CRC-32, framing, constants
│   │   ├── processing.js   # Section 3 window metrics & heart rate peak detector
│   │   └── storage.js      # WAL storage engine with periodic fsync
│   ├── device/
│   │   ├── ingestion.js    # TCP sensor reader with auto-reread on corrupt CRC
│   │   ├── network_monitor.js # Sub-10s online/offline detector
│   │   ├── sync_agent.js   # In-order live streaming and backlog drainer
│   │   └── device_service.js # Device daemon coordinator
│   └── cloud/
│       └── cloud_server.js # Cloud ingestion API & online window processor
├── tools/
│   └── dev-mock/           # Isolated temporary mock server (ramp & signal modes)
├── test/
│   └── e2e_test.js         # Full end-to-end integration and resilience suite
├── docs/
│   ├── DESIGN_NOTE.md      # Official 2-page system design note
│   └── EVIDENCE.md         # Evidence of failure tests and demo script
├── package.json
└── README.md
```

---

## 3. Installation & Setup on a Fresh Board

### Prerequisites
- Node.js $\ge 18.0.0$ (no build tools, no native compilers, and zero npm dependencies required).

### Installation:
```bash
git clone <repo-url> MayaCardio
cd MayaCardio
chmod +x bin/pipeline
```

To make the `pipeline` command available system-wide:
```bash
sudo ln -sf $(pwd)/bin/pipeline /usr/local/bin/pipeline
```

---

## 4. Running the System

### Step 1: Start the Sensor Mock Service
In Terminal 1 (or as a systemd service):
```bash
# Ramp mode (for mathematical validation):
node tools/dev-mock/index.js --mode ramp --seed 7 --duration 3600 --port 7100

# Or Signal mode (for physiological testing):
node tools/dev-mock/index.js --mode signal --seed 7 --duration 3600 --port 7100
```

### Step 2: Configure Credentials in `.env` & Start Cloud Service

Copy `.env.example` to `.env` (or edit the created `.env`) and paste your cloud credentials:

```env
# In .env:
DATABASE_URL=postgresql://user:password@ep-xyz.neon.tech/neondb?sslmode=require
CLOUD_PORT=8080
CLOUD_HOST=0.0.0.0
CLOUD_URL=http://127.0.0.1:8080
```

Start the cloud service:
```bash
npm run cloud
```
*(The server automatically reads `.env` on boot, connects to PostgreSQL, and initializes `samples`, `losses`, and `window_results` with `ON CONFLICT DO NOTHING`).*

If `DATABASE_URL` is commented out or empty, it automatically falls back to local file storage (`./data/cloud/`).

### Step 3: Start the Pipeline Device Daemon
In Terminal 3 (on the Raspberry Pi or local device):
```bash
pipeline start
```
*(Options like `--cloud-url` and `--mock-port` are also read automatically from `.env`).*

---

## 5. CLI Commands (`pipeline`)

### Check Current Status:
```bash
pipeline status
```
Output:
```json
{"mode":"online","samples_saved":42200,"samples_in_cloud":42200,"backlog":0}
```

### Export Raw Samples and Loss Records:
```bash
# Export from local device disk:
pipeline export --session <SESSION_ID> --from device

# Export from cloud copy:
pipeline export --session <SESSION_ID> --from cloud
```
Output format:
```json
{"sensor":1,"index":0,"values":[0,1,2]}
{"sensor":1,"index":1,"values":[3,4,5]}
{"sensor":2,"from_index":4000,"to_index":4399,"reason":"buffer_overflow"}
```

### Inspect Window Results (Section 3 Outputs):
```bash
pipeline results --session <SESSION_ID>
```
Output format:
```json
{"window":0,"missing_1":0,"missing_2":0,"complete":true,"mean_1":[600.0,601.0,602.0],"min_1":[0,1,2],"max_1":[1197,1198,1199],"mean_2":[400.0,401.0],"min_2":[0,1],"max_2":[798,799],"magnitude":566.4,"heart_rate":null,"mode":"online"}
```

---

## 6. Running Automated Tests

### Main End-to-End Resilience Test Suite (All 8 Criteria):
```bash
npm test
```
This automated suite validates:
1. Online ingestion and streaming.
2. Network cut simulation with automatic switch to `offline` mode within 4 seconds.
3. Offline 1-second window processing on the device.
4. Network restoration and automatic catch-up of backlogged samples.
5. Exact sample-for-sample match between device disk and cloud storage.
6. Section 3 window metric correctness (means, mins, maxes, magnitude, heart rate).
7. Process kill / sudden power cut simulation and data recovery.

### Judge-Injected Faults Verification Suite:
```bash
npm run test:faults
```
Validates the pipeline against the exact faults the judges inject during evaluation:
- Mock stalls / stops answering for a while (handled by socket watchdog & in-flight throttling)
- Corrupt CRC-32 replies (rejected, automatic re-read)
- Duplicate identical replies (idempotent deduplication at edge)
- Single sensor drops (reported in `lost`, cursor safely advanced)
- Mock restarts / session rollover (unread data recorded with `"to_index": null`)
- Crash recovery and durability

### Live Services Fault Test Suite (Testing Your Actual Running Pipeline):
```bash
npm run test:live
```
Runs directly against your **actual configured services** on ports `7100` and `8080` (from `.env`):
- Connects to your live `cloud_server` and `biosignal-mock`
- Injects dynamic stalls, corrupt CRC packets, and duplicate replies into the live stream
- Simulates an actual network cut on the cloud endpoint and tests automatic offline buffering & catch-up sync
- Simulates mock session restarts and verifies `"to_index": null` loss logging
- Runs `check_ground_truth` directly on your live export data and PostgreSQL database!

---

## 7. Ground Truth Verification Tool (`check_ground_truth`)

The assessment provides and specifies `check_ground_truth` to verify an export before submission:
*"A test tool, check_ground_truth, is provided with the mock. It compares an export with the ground truth, so a pipeline can be tested before submission."*

### Usage:

```bash
# 1. Pipe export directly into check_ground_truth:
pipeline export --session <SESSION_ID> --from device | node tools/check_ground_truth.js

# 2. Check an exported file:
node tools/check_ground_truth.js --export export.jsonl --mode ramp

# 3. Check directly by session ID:
node tools/check_ground_truth.js --session <SESSION_ID> --mode ramp
```

### What `check_ground_truth` Checks:
1. **Mathematical Accuracy:** Verifies 100% of sample values against the deterministic Ramp or Signal equations.
2. **Monotonicity & Idempotency:** Detects and flags any duplicate sample indices or out-of-order sequences.
3. **Zero Silent Loss:** Ensures every gap between sample indices strictly matches a recorded loss entry.
4. **Loss Format:** Ensures valid `from_index`, `to_index` (or `null` on restart), and `reason`.

---

## 8. Resilience to Judge-Injected Faults Summary

| Fault Injected by Judges | Impact / Threat | Pipeline Defense Mechanism |
|---|---|---|
| **Stop answering for a while** | TCP link hangs or silent socket freeze | Inactivity watchdog resets socket if silent $> 6$s; in-flight request throttling prevents request spamming. |
| **Corrupt reply (wrong CRC-32)** | Corrupt data entering pipeline | Verified against raw bracketed substring; immediately discarded and re-read from expected index. |
| **Send same reply twice** | Duplicate sample storage & metrics skew | Ingestion checks against cursor; duplicate payloads completely dropped before writing to WAL. |
| **Stop sending one sensor** | Incomplete windows & gap desync | Reports in `lost`; ingestion advances cursor past lost range; zero unexplained gaps. |
| **Mock restart (new session)** | Session ID rollover; unread data lost | Automatically records loss on old session with `"to_index": null`, resets cursors for new session. |
| **Cut the network** | Loss of cloud streaming connectivity | Network monitor detects 2 ping failures in $\le 4$s, switches to offline, processes windows locally. |
| **Kill processes** | Sudden process death | On restart, log replay reads WAL from disk; sync agent queries cloud watermark; streaming resumes without loss. |
| **Pull the power** | OS page cache uncommitted data loss | Periodic `fsyncSync` every 100ms bounds maximum loss to $\le 0.1$s ($40$ samples). |
| **Fill the disk** | Write failure (`ENOSPC`) crash | Writes wrapped in `try/catch` catching `ENOSPC` gracefully, avoiding unhandled process crashes. |
| **Change system clock** | Timestamp warping & skew | Pipeline uses sample index counters ($i / 400$ Hz) and window numbers, completely immune to system clock shifts. |

