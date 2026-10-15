/**
 * test/postgres_test.js
 *
 * Verification script for PostgreSQL Cloud Integration.
 *
 * Usage:
 *   DATABASE_URL="postgres://user:pass@host:5432/db" node test/postgres_test.js
 */

'use strict';

require('../src/common/env');
const { PostgresStorage } = require('../src/cloud/postgres_storage');

async function testPostgres() {
  const dbUrl = process.env.DATABASE_URL;
  if (!dbUrl) {
    console.log(`[!] No DATABASE_URL found. To test with PostgreSQL, run:`);
    console.log(`    DATABASE_URL="postgresql://user:pass@host:5432/dbname" node test/postgres_test.js`);
    process.exit(0);
  }

  console.log(`[*] Connecting to PostgreSQL: ${dbUrl.replace(/:[^:@]+@/, ':****@')}`);
  const storage = new PostgresStorage(dbUrl);

  try {
    await storage.initSchema();
    console.log(`[PASS] 1. Schema initialized successfully.`);

    const testSession = `test-session-${Date.now()}`;

    // Test Sample Insertion & Deduplication
    const sampleBatch = [
      [100, 200, 300],
      [101, 201, 301]
    ];
    await storage.saveSamples(testSession, 1, 0, 2, sampleBatch);
    console.log(`[PASS] 2. Saved sample batch.`);

    // Test Duplicate Insertion (idempotency)
    await storage.saveSamples(testSession, 1, 0, 2, sampleBatch);
    console.log(`[PASS] 3. Deduplicated duplicate insertion.`);

    // Check count
    const count = await storage.getSavedSampleCount(testSession);
    console.log(`      Sample count for ${testSession}: ${count} (expected 2)`);
    if (count !== 2) throw new Error(`Expected count 2, got ${count}`);

    // Test Loss Recording
    await storage.recordLoss(testSession, 1, 100, 199, 'test_loss');
    const losses = await storage.loadLosses(testSession);
    console.log(`[PASS] 4. Recorded and loaded loss range:`, losses[0].reason);

    // Test Watermark
    const wm = await storage.getWatermark(testSession, 1);
    console.log(`[PASS] 5. Watermark:`, wm);

    console.log(`\n\x1b[32mPostgreSQL Cloud Storage Verified Successfully!\x1b[0m\n`);
  } finally {
    await storage.close();
  }
}

testPostgres().catch(err => {
  console.error(`\x1b[31mPostgreSQL Test Failed:\x1b[0m`, err);
  process.exit(1);
});
