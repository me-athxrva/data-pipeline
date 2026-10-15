/**
 * tools/dev-mock/generator.js
 *
 * Deterministic sample generator for biosignal-mock.
 * Supports:
 *  1. 'ramp' mode: exact mathematical formulas from assessment requirements.
 *  2. 'signal' mode: deterministic synthetic physiological waveforms with periodic peaks.
 *
 * Sampling rate: 400 Hz
 * Sensor 1: 3 channels (channel 0, channel 1, channel 2)
 * Sensor 2: 2 channels (channel 0, channel 1)
 */

'use strict';

const SAMPLE_RATE = 400; // 400 Hz
const MODULO_RAMP = 8388608; // 2^23 (0x800000)

const SENSOR_CHANNELS = {
  1: 3,
  2: 2
};

/**
 * Deterministic pseudo-random noise generator based on seed, sensor, channel, and index.
 * Returns a value in [-1.0, 1.0].
 */
function deterministicNoise(seed, sensor, channel, index) {
  let h = ((seed * 1103515245 + 12345) ^ (sensor * 104729) ^ (channel * 7919) ^ (index * 4001)) >>> 0;
  h = Math.imul(h ^ (h >>> 16), 0x45d9f3b) >>> 0;
  h = (h ^ (h >>> 15)) >>> 0;
  return ((h & 0x7FFFFFFF) / 1073741823.5) - 1.0;
}

/**
 * Generate a single sample value for Ramp Mode.
 *
 * Requirements:
 * For sensor 1: value(sensor=1, channel=c, index=i) = (3*i + c) mod 8,388,608
 * For sensor 2: value(sensor=2, channel=c, index=i) = (2*i + c) mod 8,388,608
 *
 * @param {number} sensor - 1 or 2.
 * @param {number} channel - channel index (0..2 for sensor 1, 0..1 for sensor 2).
 * @param {number} index - absolute sample index.
 * @returns {number} Integer sample value.
 */
function generateRampValue(sensor, channel, index) {
  const multiplier = sensor === 1 ? 3 : 2;
  // JavaScript's % operator on safe integers: (multiplier * index + channel) % 8388608
  return (multiplier * index + channel) % MODULO_RAMP;
}

/**
 * Generate a single sample value for Signal Mode.
 *
 * Approximate synthetic physiological waveform for local testing & development.
 * NOTE: This is NOT the official MayaCardio signal generator (which was not provided).
 *
 * Features:
 * - 400 Hz sampling rate
 * - Deterministic output derived purely from (seed, sensor, channel, index)
 * - Sensor 1, Channel 2 contains clear, prominent periodic peaks for heart-rate detection (approx 68-85 BPM)
 * - Sensor 2 produces pulse/photoplethysmogram-like waveforms with dicrotic morphology
 * - Deterministic high-frequency noise derived from seed
 *
 * @param {number} sensor - 1 or 2.
 * @param {number} channel - channel index.
 * @param {number} index - absolute sample index.
 * @param {number} seed - integer seed.
 * @returns {number} Integer sample value.
 */
function generateSignalValue(sensor, channel, index, seed) {
  // 6-minute repeating cycle (360 seconds * 400 Hz = 144,000 samples per cycle)
  const CYCLE_SAMPLES = 360 * SAMPLE_RATE;
  const cycleIndex = index % CYCLE_SAMPLES;
  const minute = Math.floor(cycleIndex / (60 * SAMPLE_RATE));

  // HR schedule: min 0-2 -> 60 BPM, min 2-4 -> 90 BPM, min 4-6 -> 72 BPM
  let bpm;
  if (minute < 2) {
    bpm = 60;
  } else if (minute < 4) {
    bpm = 90;
  } else {
    bpm = 72;
  }

  const samplesPerBeat = Math.round((SAMPLE_RATE * 60) / bpm);
  const phase = (index % samplesPerBeat) / samplesPerBeat; // 0.0 to 1.0

  // Seed-based subtle deterministic noise
  const noise = deterministicNoise(seed, sensor, channel, index) * 15;

  if (sensor === 1) {
    // Sensor 1, Channel 2 is the ECG waveform with sharp peak
    const rDist = phase - 0.25;
    const rPeak = Math.exp(-Math.pow(rDist / 0.012, 2)) * 3500; // sharp peak
    const pWave = Math.exp(-Math.pow((phase - 0.15) / 0.025, 2)) * 180;
    const tWave = Math.exp(-Math.pow((phase - 0.45) / 0.05, 2)) * 450;
    const baseline = 1000;
    const ch2Val = Math.round(baseline + rPeak + pWave + tWave + noise);

    if (channel === 2) {
      return Math.max(0, Math.min(MODULO_RAMP - 1, ch2Val));
    } else if (channel === 0) {
      // Channel 1 (index 0) = channel 2 * 0.6
      const val = Math.round(ch2Val * 0.6);
      return Math.max(0, Math.min(MODULO_RAMP - 1, val));
    } else {
      // Channel 3 (index 1) = channel 2 * 1.3
      const val = Math.round(ch2Val * 1.3);
      return Math.max(0, Math.min(MODULO_RAMP - 1, val));
    }
  } else {
    // Sensor 2: channel 1 (index 0) near 400,000 and channel 2 (index 1) near 150,000, varying slowly
    const slowVariation = Math.sin(2 * Math.PI * (index / (SAMPLE_RATE * 12))) * 2500;
    if (channel === 0) {
      const val = Math.round(400000 + slowVariation + noise * 5);
      return Math.max(0, Math.min(MODULO_RAMP - 1, val));
    } else {
      const val = Math.round(150000 + slowVariation * 0.8 + noise * 5);
      return Math.max(0, Math.min(MODULO_RAMP - 1, val));
    }
  }
}

/**
 * Functional sample generator: generate(sensor, channel, absoluteIndex, seed, mode)
 */
function generate(sensor, channel, absoluteIndex, seed, mode = 'signal') {
  if (mode === 'ramp') {
    return generateRampValue(sensor, channel, absoluteIndex);
  }
  return generateSignalValue(sensor, channel, absoluteIndex, seed);
}

/**
 * Generate a chunk of multi-channel samples on demand.
 *
 * @param {number} sensor - 1 or 2
 * @param {number} startIndex - Starting absolute sample index
 * @param {number} count - Number of samples to generate
 * @param {string} mode - 'signal' | 'ramp'
 * @param {number} seed - Integer seed
 * @returns {Array<Array<number>>} Array of sample arrays
 */
function generateChunk(sensor, startIndex, count, mode, seed) {
  const numChannels = SENSOR_CHANNELS[sensor];
  if (!numChannels) {
    throw new Error(`Invalid sensor ${sensor}. Expected 1 or 2.`);
  }

  const chunk = new Array(count);
  for (let i = 0; i < count; i++) {
    const absIndex = startIndex + i;
    const sample = new Array(numChannels);
    for (let c = 0; c < numChannels; c++) {
      sample[c] = generate(sensor, c, absIndex, seed, mode);
    }
    chunk[i] = sample;
  }
  return chunk;
}

module.exports = {
  SAMPLE_RATE,
  MODULO_RAMP,
  SENSOR_CHANNELS,
  generate,
  generateRampValue,
  generateSignalValue,
  generateChunk,
  deterministicNoise
};
