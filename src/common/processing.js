/**
 * src/common/processing.js
 *
 * Section 3 Processing Logic (Identical implementation for Device and Cloud).
 * Handles:
 * - 1-second window aggregation (400 samples/window)
 * - Missing sample counts and completeness flags
 * - Channel statistics: mean, min, max
 * - Sensor 2 magnitude: mean of sqrt(ch1^2 + ch2^2)
 * - Heart rate extraction via deterministic peak detection rule
 */

'use strict';

const { SAMPLE_RATE, WINDOW_SIZE } = require('./protocol');

/**
 * Round a number to 1 decimal place.
 * Returns null if input is null or undefined or NaN.
 */
function round1(val) {
  if (val === null || val === undefined || isNaN(val)) return null;
  return Math.round(val * 10) / 10;
}

/**
 * Stateful Peak Detector and Heart Rate Calculator for Sensor 1 Channel 2.
 */
class HeartRateDetector {
  constructor() {
    this.lastPeakIndex = -Infinity;
    // Map of discovered peaks: absoluteIndex -> true
    this.peaks = new Set();
    this.maxEvaluatedIndex = -1;
  }

  /**
   * Evaluate samples up to targetIndex for peaks.
   *
   * Peak Rule:
   * Sample i is a peak when all 4 hold:
   * 1. 800 samples before it (i-800 to i-1) and sample after (i+1) are available.
   * 2. x[i] > x[i-1] and x[i] > x[i+1].
   * 3. x[i] > 0.5 * max(x[i-800 ... i-1]).
   * 4. i - prev_peak >= 100.
   *
   * @param {Map<number, Array<number>>} sensor1Samples - Map of index -> [ch0, ch1, ch2]
   * @param {number} upToIndex - Max sample index to evaluate
   */
  evaluatePeaks(sensor1Samples, upToIndex) {
    const startIndex = Math.max(800, this.maxEvaluatedIndex + 1);

    for (let i = startIndex; i <= upToIndex; i++) {
      // Must have sample i, sample i+1, and all 800 prior samples
      const curr = sensor1Samples.get(i);
      const next = sensor1Samples.get(i + 1);
      if (!curr || !next) continue;

      const val = curr[2]; // Sensor 1, Channel 2 (0-indexed channel 2)
      const prevVal = sensor1Samples.get(i - 1);
      if (!prevVal || val <= prevVal[2] || val <= next[2]) {
        continue; // Rule 2 failed: not strictly greater than both neighbours
      }

      // Check rule 4 first: distance from previous peak >= 100
      if (i - this.lastPeakIndex < 100) {
        continue;
      }

      // Check rule 1 & 3: 800 samples before it must all be available and compute max
      let maxPrior = -Infinity;
      let allPriorAvailable = true;

      for (let p = i - 800; p < i; p++) {
        const priorSample = sensor1Samples.get(p);
        if (!priorSample) {
          allPriorAvailable = false;
          break;
        }
        if (priorSample[2] > maxPrior) {
          maxPrior = priorSample[2];
        }
      }

      if (!allPriorAvailable) continue;

      // Rule 3: larger than half of the maximum of 800 samples before it
      if (val > 0.5 * maxPrior) {
        this.peaks.add(i);
        this.lastPeakIndex = i;
      }
    }

    this.maxEvaluatedIndex = Math.max(this.maxEvaluatedIndex, upToIndex);
  }

  /**
   * Calculate heart rate for window w.
   *
   * Rule:
   * For window w, take the peaks in the last 5 seconds (samples 400(w-4) to 400(w+1)-1).
   * heart_rate = 60 / (mean gap between consecutive peaks, in seconds).
   * Null if:
   * - w < 4
   * - any sample in those 5 seconds is missing
   * - fewer than 2 peaks in those 5 seconds
   *
   * @param {number} windowIndex - Window number w
   * @param {Map<number, Array<number>>} sensor1Samples - Samples map
   * @returns {number|null} Heart rate rounded to 1 decimal place, or null
   */
  calculateRateForWindow(windowIndex, sensor1Samples) {
    if (windowIndex < 4) return null;

    const startSample = 400 * (windowIndex - 4);
    const endSample = 400 * (windowIndex + 1) - 1;

    // Check if ANY sample in the 5-second interval is missing
    for (let i = startSample; i <= endSample; i++) {
      if (!sensor1Samples.has(i)) {
        return null;
      }
    }

    // Collect all peaks falling within [startSample, endSample]
    const windowPeaks = [];
    for (let i = startSample; i <= endSample; i++) {
      if (this.peaks.has(i)) {
        windowPeaks.push(i);
      }
    }

    if (windowPeaks.length < 2) {
      return null;
    }

    // Calculate mean gap in seconds
    let totalGapSamples = 0;
    for (let k = 0; k < windowPeaks.length - 1; k++) {
      totalGapSamples += (windowPeaks[k + 1] - windowPeaks[k]);
    }

    const meanGapSamples = totalGapSamples / (windowPeaks.length - 1);
    const meanGapSeconds = meanGapSamples / SAMPLE_RATE;

    if (meanGapSeconds <= 0) return null;

    const hr = 60 / meanGapSeconds;
    return round1(hr);
  }
}

/**
 * Process a single 1-second window according to Section 3.
 *
 * @param {object} params
 * @param {number} params.windowIndex - Window index w (0, 1, 2...)
 * @param {Map<number, Array<number>>} params.sensor1Map - Available samples for Sensor 1
 * @param {Map<number, Array<number>>} params.sensor2Map - Available samples for Sensor 2
 * @param {HeartRateDetector} params.hrDetector - Persistent detector instance
 * @param {string} params.mode - 'online' | 'offline'
 * @returns {object} Standardized window result object
 */
function processWindow({ windowIndex, sensor1Map, sensor2Map, hrDetector, mode = 'offline' }) {
  const startSample = windowIndex * WINDOW_SIZE;
  const endSample = startSample + WINDOW_SIZE - 1;

  let missing_1 = 0;
  let missing_2 = 0;

  // Sensor 1 metrics: 3 channels
  const s1Sums = [0, 0, 0];
  const s1Mins = [Infinity, Infinity, Infinity];
  const s1Maxs = [-Infinity, -Infinity, -Infinity];
  let s1Count = 0;

  for (let i = startSample; i <= endSample; i++) {
    const s1 = sensor1Map.get(i);
    if (!s1) {
      missing_1++;
    } else {
      s1Count++;
      for (let c = 0; c < 3; c++) {
        const v = s1[c];
        s1Sums[c] += v;
        if (v < s1Mins[c]) s1Mins[c] = v;
        if (v > s1Maxs[c]) s1Maxs[c] = v;
      }
    }
  }

  // Sensor 2 metrics: 2 channels
  const s2Sums = [0, 0];
  const s2Mins = [Infinity, Infinity];
  const s2Maxs = [-Infinity, -Infinity];
  let s2MagnitudeSum = 0;
  let s2Count = 0;

  for (let i = startSample; i <= endSample; i++) {
    const s2 = sensor2Map.get(i);
    if (!s2) {
      missing_2++;
    } else {
      s2Count++;
      for (let c = 0; c < 2; c++) {
        const v = s2[c];
        s2Sums[c] += v;
        if (v < s2Mins[c]) s2Mins[c] = v;
        if (v > s2Maxs[c]) s2Maxs[c] = v;
      }
      // Magnitude: sqrt(ch1^2 + ch2^2)
      const ch1 = s2[0];
      const ch2 = s2[1];
      s2MagnitudeSum += Math.sqrt(ch1 * ch1 + ch2 * ch2);
    }
  }

  const complete = (missing_1 === 0 && missing_2 === 0);

  // Compute stats
  const mean_1 = s1Count > 0 ? s1Sums.map(sum => round1(sum / s1Count)) : null;
  const min_1 = s1Count > 0 ? s1Mins : null;
  const max_1 = s1Count > 0 ? s1Maxs : null;

  const mean_2 = s2Count > 0 ? s2Sums.map(sum => round1(sum / s2Count)) : null;
  const min_2 = s2Count > 0 ? s2Mins : null;
  const max_2 = s2Count > 0 ? s2Maxs : null;

  const magnitude = s2Count > 0 ? round1(s2MagnitudeSum / s2Count) : null;

  // Evaluate peaks up to end of 5-second interval + 1 sample for heart rate calculation
  const hrEndSample = 400 * (windowIndex + 1) - 1;
  hrDetector.evaluatePeaks(sensor1Map, hrEndSample);
  const heart_rate = hrDetector.calculateRateForWindow(windowIndex, sensor1Map);

  return {
    window: windowIndex,
    missing_1,
    missing_2,
    complete,
    mean_1,
    min_1,
    max_1,
    mean_2,
    min_2,
    max_2,
    magnitude,
    heart_rate,
    mode
  };
}

module.exports = {
  HeartRateDetector,
  processWindow,
  round1
};
