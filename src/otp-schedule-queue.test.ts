import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DuckDBInstance } from '@duckdb/node-api';
import { reconcileBackfillSchedules } from './otp-worker';
import { query } from './local-store';

test('a newly retained schedule replaces queued dates only from its actual usable date', async () => {
  const db = await DuckDBInstance.create(':memory:');
  const c = await db.connect();
  try {
    await c.run(`CREATE TABLE otp_schedules(hash VARCHAR,usable_from DATE,valid_to DATE,fetched_at TIMESTAMPTZ);
      INSERT INTO otp_schedules VALUES ('old','2026-09-01','2026-10-01','2026-09-01T12:00:00Z'),
        ('new','2026-09-22','2026-10-01','2026-09-22T12:00:00Z');
      CREATE TABLE otp_backfill_days(service_date DATE,schedule_hash VARCHAR,applied_revision VARCHAR);
      INSERT INTO otp_backfill_days VALUES ('2026-09-21','old','done'),('2026-09-22','old','done'),('2026-09-23','new','done');`);
    await reconcileBackfillSchedules(c);
    const expected = [
      { day: '2026-09-21', schedule_hash: 'old', applied_revision: 'done' },
      { day: '2026-09-22', schedule_hash: 'new', applied_revision: null },
      { day: '2026-09-23', schedule_hash: 'new', applied_revision: 'done' },
    ];
    assert.deepEqual(await query(c, 'SELECT service_date::VARCHAR AS day,schedule_hash,applied_revision FROM otp_backfill_days ORDER BY service_date'), expected);
    await reconcileBackfillSchedules(c);
    assert.deepEqual(await query(c, 'SELECT service_date::VARCHAR AS day,schedule_hash,applied_revision FROM otp_backfill_days ORDER BY service_date'), expected);
  } finally { c.closeSync(); db.closeSync(); }
});
