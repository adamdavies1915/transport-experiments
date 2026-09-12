import test from 'node:test';
import assert from 'node:assert/strict';
import { DuckDBInstance } from '@duckdb/node-api';
import { initializeOtp } from '../src/otp-worker';
import { query } from '../src/local-store';
import { queueServerHistoryOtp } from './import-server-history';

test('imported OTP dates use only retained covering schedules and leave unrelated completed requests intact', async () => {
  const db = await DuckDBInstance.create(':memory:');
  const c = await db.connect();
  try {
    await initializeOtp(c);
    await c.run(`INSERT INTO otp_schedules VALUES
      ('older','retained','2026-09-10T00:00:00Z','2026-09-07','2026-09-10','retained'),
      ('same-start-older-fetch','retained','2026-09-08T00:00:00Z','2026-09-08','2026-09-10','retained'),
      ('latest-usable','retained','2026-09-09T00:00:00Z','2026-09-08','2026-09-10','retained'),
      ('current-only','retained','2026-09-12T00:00:00Z','2026-09-12','2026-12-31','retained')`);
    await c.run(`INSERT INTO otp_backfill_days VALUES
      ('2026-09-07','older','completed-outside-import','2026-09-11T12:00:00Z'),
      ('2026-09-08','older','completed-import','2026-09-11T13:00:00Z')`);
    const dates = ['2026-09-12', '2026-09-11', '2026-09-09', '2026-09-08', '2026-09-09', '2026-09-06'];
    const result = await queueServerHistoryOtp(c, dates);
    assert.deepEqual(result, {
      queued: [
        { service_date: '2026-09-08', schedule_hash: 'latest-usable' },
        { service_date: '2026-09-09', schedule_hash: 'latest-usable' },
        { service_date: '2026-09-12', schedule_hash: 'current-only' },
      ], no_covering_schedule: ['2026-09-06', '2026-09-11'],
    });
    const queued = () => query(c, `SELECT service_date::VARCHAR AS date,schedule_hash,applied_revision,
      epoch(last_completed_at) AS last_completed_at FROM otp_backfill_days ORDER BY service_date`);
    const expected = [
      { date: '2026-09-07', schedule_hash: 'older', applied_revision: 'completed-outside-import', last_completed_at: Date.parse('2026-09-11T12:00:00Z') / 1000 },
      { date: '2026-09-08', schedule_hash: 'latest-usable', applied_revision: null, last_completed_at: Date.parse('2026-09-11T13:00:00Z') / 1000 },
      { date: '2026-09-09', schedule_hash: 'latest-usable', applied_revision: null, last_completed_at: null },
      { date: '2026-09-12', schedule_hash: 'current-only', applied_revision: null, last_completed_at: null },
    ];
    assert.deepEqual(await queued(), expected);
    assert.deepEqual(await queueServerHistoryOtp(c, dates), result);
    assert.deepEqual(await queued(), expected);
    await assert.rejects(queueServerHistoryOtp(c, ['2026-02-30', '2026-09-07']), /valid YYYY-MM-DD/);
    assert.deepEqual(await queued(), expected);
  } finally { c.closeSync(); db.closeSync(); }
});
