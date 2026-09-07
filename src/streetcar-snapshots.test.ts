import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DuckDBInstance, type DuckDBConnection } from '@duckdb/node-api';
import { captureStreetcarSnapshots, initializeStreetcarSnapshots, persistStreetcarSnapshots,
  SnapshotRetryBuffer, snapshotSourceUrl } from './streetcar-snapshots';

const vehicle = { vid: '900', rt: '12', tmstmp: '2026-09-07T13:02:00-05:00', lat: '29.94', lon: '-90.08',
  tatripid: 'legacy-17', tripid: 12345, des: "Canal at O'Keefe", hdg: '0', srvtmstmp: '2026-09-07T13:02:00-05:00' };
function capture(id: string, receivedAt = '2026-09-07T18:02:10.123Z', entry: unknown = vehicle) {
  return captureStreetcarSnapshots([entry], { receipt_id: id, received_at: receivedAt,
    feed_event_id: 'upstream-frame', source_url: 'https://nolatransit.fly.dev/sse' });
}

test('snapshot ledger retains moved and stationary receipts within the same provider minute', () => {
  const first = capture('receipt-a')[0];
  const stationary = capture('receipt-b', '2026-09-07T18:02:20.456Z')[0];
  const moved = capture('receipt-c', '2026-09-07T18:02:30.789Z', { ...vehicle, lat: '29.94015', spd: '4.5' })[0];
  assert.equal(new Set([first, stationary, moved].map(s => s.snapshot_id)).size, 3);
  assert.equal(new Set([first, stationary, moved].map(s => s.provider_timestamp)).size, 1);
  assert.equal(first.provider_timestamp, vehicle.tmstmp);
  assert.equal(first.provider_observed_at, '2026-09-07T18:02:00Z');
  assert.equal(first.received_at, '2026-09-07T18:02:10.123Z');
  assert.equal(first.lat, stationary.lat); assert.notEqual(first.lat, moved.lat);
  assert.equal(moved.speed, 4.5);
  assert.equal(first.legacy_trip_id, 'legacy-17'); assert.equal(first.gtfs_trip_id, '12345');
  assert.equal(first.feed_event_id, stationary.feed_event_id); // A repeated event ID is still a new receipt.
  const withoutExplicitIds = [0, 1].flatMap(() => captureStreetcarSnapshots([vehicle], {
    received_at: '2026-09-07T18:02:10Z', source_url: 'https://nolatransit.fly.dev/sse' }));
  assert.notEqual(withoutExplicitIds[0].snapshot_id, withoutExplicitIds[1].snapshot_id);
});

test('snapshots preserve absent values as null, explicit zero/false, raw fields and provider clock uncertainty', () => {
  const missing = capture('missing', undefined, { ...vehicle, hdg: undefined, custom_field: 'original-source-value' })[0];
  assert.equal(missing.speed, null); assert.equal(missing.heading, null);
  assert.equal(missing.is_delayed, null); assert.equal(missing.is_off_route, null);
  assert.equal(missing.pdist, null); assert.equal(missing.pid, null);
  assert.equal(JSON.parse(missing.raw_payload).custom_field, 'original-source-value');
  const zeros = capture('zeros', undefined, { ...vehicle, spd: '0', dly: false, or: false, pdist: '0', pid: '0007' })[0];
  assert.equal(zeros.speed, 0); assert.equal(zeros.heading, 0);
  assert.equal(zeros.is_delayed, false); assert.equal(zeros.is_off_route, false);
  assert.equal(zeros.pdist, 0); assert.equal(zeros.pid, '0007');
  for (const tmstmp of ['bad', '2026-09-07 13:02:00', '2026-02-30T13:02:00-06:00']) {
    const row = capture('bad-clock', undefined, { ...vehicle, tmstmp, lat: 'bad', lon: '-999', spd: 'N/A', dly: 'true', or: 'false' })[0];
    assert.equal(row.provider_timestamp, tmstmp); assert.equal(row.provider_observed_at, null);
    assert.equal(row.lat, null); assert.equal(row.lon, null); assert.equal(row.speed, null);
    assert.equal(row.is_delayed, null); assert.equal(row.is_off_route, null);
  }
  assert.throws(() => capture('bad-receipt', 'not-a-time'));
  assert.equal(snapshotSourceUrl('https://user:secret@example.org/sse?token=secret#fragment'), 'https://example.org/sse');
  assert.equal(snapshotSourceUrl('invalid'), null);
  const selected = captureStreetcarSnapshots([null, vehicle, { ...vehicle, rt: '46' }, { ...vehicle, rt: '47' },
    { ...vehicle, rt: '48' }, { ...vehicle, rt: '49' }, { ...vehicle, rt: '3' }], {
    receipt_id: 'frame', received_at: '2026-09-07T18:02:00Z', source_url: 'https://example.org/sse' });
  assert.deepEqual(selected.map(row => row.route), ['12', '46', '47', '48']);
  assert.deepEqual(selected.map(row => row.snapshot_id), ['frame:1', 'frame:2', 'frame:3', 'frame:4']);
});

test('snapshot table is additive, keeps repeated receipts and retries a partly committed batch idempotently', async () => {
  const db = await DuckDBInstance.create(':memory:'); const c = await db.connect();
  try {
    await c.run("CREATE TABLE transit_data AS SELECT 'unchanged' AS marker");
    await initializeStreetcarSnapshots(c); await initializeStreetcarSnapshots(c);
    const records = Array.from({ length: 502 }, (_, i) => capture(`receipt-${String(i).padStart(4, '0')}`)[0]);
    let batches = 0;
    const flaky = { run: async (...args: Parameters<DuckDBConnection['run']>) => {
      batches++;
      if (batches === 2) throw new Error('Simulated connection loss after first committed chunk');
      return c.run(...args);
    } } as unknown as DuckDBConnection;
    await assert.rejects(persistStreetcarSnapshots(flaky, records), /connection loss/);
    assert.equal((await c.runAndReadAll('SELECT COUNT(*)::INTEGER AS count FROM streetcar_snapshots')).getRowObjectsJson()[0].count, 500);
    await persistStreetcarSnapshots(c, records); await persistStreetcarSnapshots(c, records);
    const count = (await c.runAndReadAll('SELECT COUNT(*)::INTEGER AS count FROM streetcar_snapshots')).getRowObjectsJson()[0].count;
    assert.equal(count, 502);
    const row = (await c.runAndReadAll(`SELECT provider_timestamp,epoch(provider_observed_at) AS provider_epoch,
      epoch(received_at) AS received_epoch,speed,is_delayed,is_off_route,raw_payload,destination,ingested_at IS NOT NULL AS ingested
      FROM streetcar_snapshots ORDER BY snapshot_id LIMIT 1`)).getRowObjectsJson()[0];
    assert.equal(row.provider_timestamp, vehicle.tmstmp);
    assert.ok(Math.abs(Number(row.received_epoch) - Number(row.provider_epoch) - 10.123) < 0.000001);
    assert.equal(row.speed, null); assert.equal(row.is_delayed, null); assert.equal(row.is_off_route, null);
    assert.equal(row.destination, vehicle.des); assert.deepEqual(JSON.parse(String(row.raw_payload)), vehicle);
    assert.equal(row.ingested, true);
    const unusualText = capture('unusual-text', undefined, { ...vehicle, des: "O'Keefe\u0000; SELECT 1;" });
    await persistStreetcarSnapshots(c, unusualText);
    assert.equal((await c.runAndReadAll("SELECT destination FROM streetcar_snapshots WHERE snapshot_id='unusual-text:0'")).getRowObjectsJson()[0].destination,
      "O'Keefe\u0000; SELECT 1;");
    assert.deepEqual((await c.runAndReadAll('SELECT * FROM transit_data')).getRowObjectsJson(), [{ marker: 'unchanged' }]);
  } finally { c.closeSync(); db.closeSync(); }
});

test('snapshot retry queue serializes drains and bounds restored and newly received records', async () => {
  const dropped: number[] = []; const queue = new SnapshotRetryBuffer(3, count => dropped.push(count));
  const records = ['a', 'b', 'c', 'd', 'e'].map(id => capture(id)[0]);
  queue.add(records.slice(0, 2));
  let rejectWrite!: (error: Error) => void; let writes = 0;
  const write = async () => { writes++; await new Promise<void>((_resolve, reject) => { rejectWrite = reject; }); };
  const first = queue.flush(write); const overlapping = queue.flush(write);
  queue.add(records.slice(2));
  assert.equal(writes, 1); assert.equal(queue.size, 3);
  rejectWrite(new Error('offline'));
  await Promise.all([assert.rejects(first, /offline/), assert.rejects(overlapping, /offline/)]);
  assert.deepEqual(dropped, [2]); assert.equal(queue.size, 3);
  let retried: string[] = [];
  const flushed = await queue.flush(async pending => { retried = pending.map(row => row.snapshot_id); });
  assert.equal(flushed, 3); assert.equal(queue.size, 0);
  assert.deepEqual(retried, ['c:0', 'd:0', 'e:0']);
  assert.equal(await queue.flush(async () => { throw new Error('Should not write an empty queue'); }), 0);
});
