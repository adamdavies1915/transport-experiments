import test from 'node:test';
import assert from 'node:assert/strict';
import { appendFile, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { DuckDBConnection } from '@duckdb/node-api';
import { openLocalStore, query, sql, ident } from './local-store';
import { captureStreetcarSnapshots, persistStreetcarSnapshots } from './streetcar-snapshots';
import { importServerHistory, type ServerHistoryFiles } from './server-history-import';

async function fixture(run: (args: {
  source: Awaited<ReturnType<typeof openLocalStore>>; target: Awaited<ReturnType<typeof openLocalStore>>;
  files: ServerHistoryFiles; exportFiles: (rawSql?: string, snapshotSql?: string) => Promise<void>;
}) => Promise<void>) {
  const directory = await mkdtemp(join(tmpdir(), 'server-history-import-'));
  const source = await openLocalStore(join(directory, 'source')), target = await openLocalStore(join(directory, 'target'));
  const files = { transitDataFile: join(directory, "raw's history.parquet"), streetcarSnapshotsFile: join(directory, 'snapshots.parquet') };
  const exportFiles = async (rawSql = 'SELECT * FROM transit_data', snapshotSql = 'SELECT * FROM streetcar_snapshots') => {
    await source.c.run(`COPY (${rawSql}) TO ${sql(files.transitDataFile)} (FORMAT PARQUET)`);
    await source.c.run(`COPY (${snapshotSql}) TO ${sql(files.streetcarSnapshotsFile)} (FORMAT PARQUET)`);
  };
  try { await run({ source, target, files, exportFiles }); }
  finally { source.c.closeSync(); source.db.closeSync(); target.c.closeSync(); target.db.closeSync(); await rm(directory, { recursive: true, force: true }); }
}
async function raw(c: DuckDBConnection, vid: string, options: { copies?: number; rich?: boolean; noClock?: boolean } = {}) {
  for (let i = 0; i < (options.copies ?? 1); i++) await c.run(`INSERT INTO transit_data
    (vid,timestamp,lat,lon,route,trip_id,speed,is_delayed,is_off_route,observed_at,gtfs_trip_id,destination) VALUES
    (${sql(vid)},${options.noClock ? 'NULL' : "TIMESTAMP '2026-09-08 10:00:00'"},29.95,-90.1,'12','legacy',
    ${options.rich ? 0 : 'NULL'},${options.rich ? 'false' : 'NULL'},false,
    ${options.noClock ? 'NULL' : "TIMESTAMPTZ '2026-09-08 15:00:00.123456Z'"},${options.rich ? "'000001'" : 'NULL'},'Canal')`);
}
async function snapshot(c: DuckDBConnection, id: string, previousDay = false) {
  await persistStreetcarSnapshots(c, captureStreetcarSnapshots([{
    vid: '460', rt: '12', tmstmp: previousDay ? '2026-09-06T23:59:59.123456Z' : '2026-09-08T15:00:00.123456Z',
    lat: '29.95', lon: '-90.1', tatripid: 'legacy', tripid: '000001', custom_field: 'original evidence',
  }], { receipt_id: id, received_at: previousDay ? '2026-09-07T06:00:00.456789Z' : '2026-09-08T15:00:10.456789Z', source_url: 'https://example.org/sse' }));
  await c.run(`UPDATE streetcar_snapshots SET ingested_at=TIMESTAMPTZ '2026-09-08 15:00:20.123456Z' WHERE snapshot_id=${sql(id + ':0')}`);
}
const size = async (c: DuckDBConnection, table: string) => (await query<{ n: number }>(c, `SELECT COUNT(*) AS n FROM ${ident(table)}`))[0].n;
async function cleanTemporary(c: DuckDBConnection) {
  assert.equal((await query<{ n: number }>(c, "SELECT COUNT(*) AS n FROM duckdb_tables() WHERE temporary AND table_name LIKE 'server_history_%'"))[0].n, 0);
}

test('history import preserves raw multiplicity, richer overlap, snapshot clocks, local extras and replay idempotency', async () => {
  await fixture(async ({ source, target, files, exportFiles }) => {
    await raw(source.c, '460', { copies: 3 }); await raw(source.c, '460', { rich: true });
    await raw(source.c, '461'); await raw(source.c, 'unknown-clock', { noClock: true });
    await raw(target.c, '460'); await raw(target.c, 'local-only', { copies: 2 });
    await snapshot(source.c, 'shared'); await snapshot(source.c, 'stale-provider', true);
    await snapshot(target.c, 'shared'); await snapshot(target.c, 'local-only');
    const rawColumns = (await query<{ column_name: string }>(source.c, 'DESCRIBE transit_data')).map(r => r.column_name).reverse();
    await exportFiles(`SELECT ${rawColumns.map(ident).join(',')} FROM transit_data`);
    const result = await importServerHistory(target.c, files);
    assert.deepEqual(result.transit_data, { source_rows: 6, source_distinct_rows: 4, source_duplicate_rows: 2,
      target_rows_before: 3, inserted_rows: 5, replay_rows: 1, target_rows_after: 8, study_dates: ['2026-09-08'], rows_without_study_date: 1 });
    assert.deepEqual(result.streetcar_snapshots, { source_rows: 2, source_distinct_rows: 2, source_duplicate_rows: 0,
      target_rows_before: 2, inserted_rows: 1, replay_rows: 1, target_rows_after: 3, study_dates: ['2026-09-06', '2026-09-08'], rows_without_study_date: 0 });
    assert.match(result.files.transit_data.sha256, /^[a-f0-9]{64}$/); assert.ok(result.files.transit_data.bytes > 0);
    assert.equal((await query<{ n: number }>(target.c, "SELECT COUNT(*) AS n FROM transit_data WHERE vid='460' AND gtfs_trip_id IS NULL"))[0].n, 3);
    const rich = (await query(target.c, "SELECT gtfs_trip_id,speed,is_delayed FROM transit_data WHERE vid='460' AND gtfs_trip_id IS NOT NULL"))[0];
    assert.deepEqual(rich, { gtfs_trip_id: '000001', speed: 0, is_delayed: false });
    const clocks = (await query(target.c, `SELECT strftime(timezone('UTC',received_at),'%Y-%m-%dT%H:%M:%S.%fZ') AS receipt,
      strftime(timezone('UTC',provider_observed_at),'%Y-%m-%dT%H:%M:%S.%fZ') AS provider,speed,raw_payload
      FROM streetcar_snapshots WHERE snapshot_id='stale-provider:0'`))[0];
    assert.equal(clocks.receipt, '2026-09-07T06:00:00.456789Z'); assert.equal(clocks.provider, '2026-09-06T23:59:59.123456Z');
    assert.equal(clocks.speed, null); assert.equal(JSON.parse(String(clocks.raw_payload)).custom_field, 'original evidence');
    const replay = await importServerHistory(target.c, files);
    assert.equal(replay.transit_data.inserted_rows, 0); assert.equal(replay.transit_data.replay_rows, 6);
    assert.equal(replay.streetcar_snapshots.inserted_rows, 0); assert.equal(replay.streetcar_snapshots.replay_rows, 2);
    assert.equal(await size(target.c, 'transit_data'), 8); assert.equal(await size(target.c, 'streetcar_snapshots'), 3);
    await cleanTemporary(target.c);
  });
});

test('different evidence under an existing snapshot ID fails before either target table changes', async () => {
  await fixture(async ({ source, target, files, exportFiles }) => {
    await raw(source.c, 'new-raw'); await snapshot(source.c, 'same'); await snapshot(target.c, 'same');
    await target.c.run("UPDATE streetcar_snapshots SET raw_payload='different local evidence'");
    const before = await query(target.c, 'SELECT * FROM streetcar_snapshots');
    await exportFiles(); await assert.rejects(importServerHistory(target.c, files), /Conflicting snapshot_id/);
    assert.equal(await size(target.c, 'transit_data'), 0);
    assert.deepEqual(await query(target.c, 'SELECT * FROM streetcar_snapshots'), before);
    await cleanTemporary(target.c);
  });
});

test('duplicate or null exported snapshot identities and incompatible schemas are rejected without field loss', async () => {
  for (const [rawSql, snapshotSql, expected] of [
    ['SELECT * FROM transit_data', 'SELECT * FROM streetcar_snapshots UNION ALL SELECT * FROM streetcar_snapshots', /duplicate snapshot_id/],
    ['SELECT * FROM transit_data', 'SELECT * REPLACE (NULL::VARCHAR AS snapshot_id) FROM streetcar_snapshots', /invalid or duplicate snapshot_id/],
    ["SELECT *, 'richer future evidence' AS new_field FROM transit_data", 'SELECT * FROM streetcar_snapshots', /Incompatible.*schema/],
    ['SELECT * REPLACE (pid::VARCHAR AS pid) FROM transit_data', 'SELECT * FROM streetcar_snapshots', /Incompatible.*schema/],
  ] as const) await fixture(async ({ source, target, files, exportFiles }) => {
    await raw(source.c, 'source'); await snapshot(source.c, 'source'); await raw(target.c, 'local-only');
    await exportFiles(rawSql, snapshotSql);
    await assert.rejects(importServerHistory(target.c, files), expected);
    assert.equal(await size(target.c, 'transit_data'), 1); assert.equal(await size(target.c, 'streetcar_snapshots'), 0);
    await cleanTemporary(target.c);
  });
});

test('an interrupted transaction or changed export rolls back both tables and remains safely replayable', async () => {
  for (const failure of ['interrupt', 'changed-file'] as const) await fixture(async ({ source, target, files, exportFiles }) => {
    await raw(source.c, 'source'); await snapshot(source.c, 'source'); await exportFiles();
    await assert.rejects(importServerHistory(target.c, files, async progress => {
      if (progress.phase !== 'inserted' || progress.table !== 'transit_data') return;
      if (failure === 'interrupt') throw new Error('Simulated interruption');
      await appendFile(files.transitDataFile, 'changed');
    }), failure === 'interrupt' ? /Simulated interruption/ : /export changed/);
    assert.equal(await size(target.c, 'transit_data'), 0); assert.equal(await size(target.c, 'streetcar_snapshots'), 0);
    await cleanTemporary(target.c);
    if (failure === 'interrupt') {
      const retry = await importServerHistory(target.c, files);
      assert.equal(retry.transit_data.inserted_rows, 1); assert.equal(retry.streetcar_snapshots.inserted_rows, 1);
    }
  });
});
