import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { openLocalStore, ingestBatch, query, sql } from './local-store';
import { mergeCollectedHistory, type CollectionMergeProgress } from './local-merge';
import type { CollectionBatch } from './observation-types';

const AT = 1788840000;
function frame(id: string, source: 'sse' | 'lepass' = 'sse', observed = AT, received = observed + 1): CollectionBatch {
  return { schema_version: 1, batch_id: id, source, received_at: new Date(received * 1000).toISOString(), observations: [{
    observation: { source, observation_id: `${id}:0`, vehicle_id: `${source}:460`, provider_vehicle_id: '460', route_id: '12',
      trip_id: source === 'sse' ? 'trip' : 'lepass:9007199254740993', observed_at: observed, received_at: received,
      lat: 29.95, lon: -90.1, speed_mph: null, off_route: false, location_source: 'provider_gps',
      timestamp_precision_seconds: source === 'sse' ? 60 : .001, direction_id: null, pattern_id: null, mapping_confidence: 'verified' },
    raw: source === 'sse' ? { vid: '460', rt: '12', tripid: 'trip', tmstmp: new Date(observed * 1000).toISOString(), lat: '29.95', lon: '-90.1' }
      : { vehicle_location: { id: '460', sample_ms: observed * 1000 }, unknown_speed: null },
  }], predictions: source === 'lepass' ? [{ trip_id: '9007199254740993', realtime_departure_at: null }] : [],
  provenance: { query_id: `${source}-fixture`, provider_revision: '123' } };
}
async function fixture(run: (value: {
  source: Awaited<ReturnType<typeof openLocalStore>>; target: Awaited<ReturnType<typeof openLocalStore>>;
  sourceFile: string; closeSource: () => Promise<void>;
}) => Promise<void>) {
  const directory = await mkdtemp(join(tmpdir(), 'transit-merge-'));
  const source = await openLocalStore(join(directory, 'source')), target = await openLocalStore(join(directory, 'target'));
  let closed = false;
  const closeSource = async () => { await source.c.run('CHECKPOINT'); source.c.closeSync(); source.db.closeSync(); closed = true; };
  try { await run({ source, target, sourceFile: join(directory, 'source', 'transit.duckdb'), closeSource }); }
  finally {
    if (!closed) { source.c.closeSync(); source.db.closeSync(); }
    target.c.closeSync(); target.db.closeSync(); await rm(directory, { recursive: true, force: true });
  }
}
async function detached(c: Parameters<typeof query>[0]) {
  assert.equal((await query<{ n: number }>(c, "SELECT COUNT(*) AS n FROM duckdb_databases() WHERE database_name LIKE 'collected_merge_%'"))[0].n, 0);
}

test('offline merge preserves exact ledgers, predictions and clocks across 200-batch pages and repeat merges', async () => {
  await fixture(async ({ source, target, sourceFile, closeSource }) => {
    const batches = [frame('first'), frame('repeat', 'sse', AT, AT + 15), frame('next', 'sse', AT + 60, AT + 61),
      frame('lepass', 'lepass', AT + .123, AT + 3.456789), frame('lepass-repeat', 'lepass', AT + .123, AT + 25)];
    batches[3].received_at = '2026-09-08T04:00:03.456789Z';
    for (let i = 0; i < 201; i++) batches.push({ ...frame(`prediction-only-${String(i).padStart(3, '0')}`, 'lepass'), observations: [] });
    for (const batch of batches) await ingestBatch(source.c, batch);
    const history = frame('existing-history', 'sse', AT - 3600, AT - 3599);
    history.observations[0].observation.vehicle_id = 'sse:999'; history.observations[0].observation.provider_vehicle_id = '999';
    history.observations[0].raw.vid = '999';
    await ingestBatch(target.c, history);
    const before = await query(source.c, 'SELECT observation_id,received_at,body::VARCHAR AS body,raw::VARCHAR AS raw FROM collected_observations ORDER BY observation_id');
    await closeSource();
    const progress: CollectionMergeProgress[] = [];
    const merged = await mergeCollectedHistory(target.c, sourceFile, value => { progress.push(value); });
    assert.deepEqual(merged, { source_batches: 206, source_payloads: 3, source_receipts: 5, inserted_batches: 206, skipped_batches: 0 });
    assert.deepEqual(progress.filter(p => p.phase === 'preflight').map(p => p.processed_batches), [200, 206]);
    assert.deepEqual(progress.filter(p => p.phase === 'merge').map(p => p.processed_batches), [200, 206]);
    assert.deepEqual(await query(target.c, "SELECT observation_id,received_at,body::VARCHAR AS body,raw::VARCHAR AS raw FROM collected_observations WHERE batch_id<>'existing-history' ORDER BY observation_id"), before);
    const metadata = await query(target.c, "SELECT predictions::VARCHAR AS predictions,provenance::VARCHAR AS provenance,strftime(timezone('UTC',received_at),'%Y-%m-%dT%H:%M:%S.%fZ') AS clock FROM collection_batches WHERE batch_id='lepass'");
    assert.deepEqual(JSON.parse(String(metadata[0].predictions)), batches[3].predictions);
    assert.deepEqual(JSON.parse(String(metadata[0].provenance)), batches[3].provenance);
    assert.equal(metadata[0].clock, '2026-09-08T04:00:03.456789Z');
    assert.equal((await query<{ n: number }>(target.c, 'SELECT COUNT(*) AS n FROM transit_data'))[0].n, 3);
    assert.ok((await query(target.c, 'SELECT speed FROM transit_data')).every(r => r.speed === null));
    assert.equal((await query(target.c, "SELECT epoch(observed_at) AS clock FROM local_watermarks WHERE vid='460'"))[0].clock, AT + 60);
    const again = await mergeCollectedHistory(target.c, sourceFile);
    assert.equal(again.inserted_batches, 0); assert.equal(again.skipped_batches, 206);
    assert.equal((await query<{ n: number }>(target.c, 'SELECT COUNT(*) AS n FROM transit_data'))[0].n, 3);
    assert.equal((await query<{ n: number }>(target.c, 'SELECT COUNT(*) AS n FROM collection_batches'))[0].n, 207);
    await detached(target.c);
  });
});

test('incomplete, corrupt and archived sources fail preflight without mutating the destination', async () => {
  for (const [mutation, expected] of [
    ["DELETE FROM collection_receipts", /incomplete/],
    ["UPDATE collection_payloads SET raw='{}'::JSON", /corrupt/],
    ["UPDATE collection_receipts SET payload_id='missing'", /orphaned/],
    ["INSERT INTO local_archive_catalog VALUES ('2026-09-07','/not-read',1,0,'hash',now())", /archive migration/],
  ] as const) await fixture(async ({ source, target, sourceFile, closeSource }) => {
    await ingestBatch(source.c, frame('source'));
    await source.c.run(mutation); await closeSource();
    await assert.rejects(mergeCollectedHistory(target.c, sourceFile), expected);
    for (const table of ['collection_batches', 'collection_receipts', 'collection_payloads', 'transit_data'])
      assert.equal((await query<{ n: number }>(target.c, `SELECT COUNT(*) AS n FROM ${table}`))[0].n, 0, table);
    await detached(target.c);
  });
});

test('conflicting existing payload, receipt and batch IDs are rejected without overwriting any target row', async () => {
  for (const [table, mutation] of [
    ['collection_payloads', "UPDATE collection_payloads SET raw='{}'::JSON"],
    ['collection_receipts', 'UPDATE collection_receipts SET received_at=received_at+1'],
    ['collection_batches', "UPDATE collection_batches SET predictions='[1]'::JSON"],
  ] as const) await fixture(async ({ source, target, sourceFile, closeSource }) => {
    await ingestBatch(source.c, frame('same'));
    await ingestBatch(source.c, frame('new', 'sse', AT + 60));
    await ingestBatch(target.c, frame('same'));
    await target.c.run(mutation);
    const before = await query(target.c, `SELECT * FROM ${table}`);
    await closeSource();
    await assert.rejects(mergeCollectedHistory(target.c, sourceFile), /Conflicting existing collection identity/);
    assert.deepEqual(await query(target.c, `SELECT * FROM ${table}`), before);
    assert.equal((await query<{ n: number }>(target.c, 'SELECT COUNT(*) AS n FROM collection_batches'))[0].n, 1);
    await detached(target.c);
  });
});

test('balanced global counts cannot hide per-batch missing receipts or noncanonical metadata', async () => {
  for (const mutation of [
    "UPDATE collection_batches SET observations=CASE WHEN batch_id='a' THEN 0 ELSE 2 END",
    `UPDATE collection_batches SET provenance=${sql('{ "noncanonical": true }')}::JSON`,
  ]) await fixture(async ({ source, target, sourceFile, closeSource }) => {
    await ingestBatch(source.c, frame('a')); await ingestBatch(source.c, frame('b'));
    await source.c.run(mutation); await closeSource();
    await assert.rejects(mergeCollectedHistory(target.c, sourceFile), /incomplete or invalid batch|metadata cannot be reconstructed exactly/);
    assert.equal((await query<{ n: number }>(target.c, 'SELECT COUNT(*) AS n FROM collection_batches'))[0].n, 0);
    await detached(target.c);
  });
});
