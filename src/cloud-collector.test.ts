import { test } from 'node:test';
import assert from 'node:assert/strict';
import EventSource from 'eventsource';
import { DuckDBInstance } from '@duckdb/node-api';
import { createCloudCollector } from './cloud-collector';
import { collectorMode, launchCollector } from './collector-entry';
import { initializeTransitSchema, transitInsertSql } from './motherduck';
import { processVehicle } from './vehicle';
import type { RawVehicle, TransitRecord } from './types';
import type { StreetcarSnapshot } from './streetcar-snapshots';

const vehicle: RawVehicle = { vid: '900', rt: '12', tmstmp: '2026-09-13T08:01:00-05:00',
  lat: '29.94', lon: '-90.08', tatripid: 'legacy-17', tripid: '12345', des: "Canal at O'Keefe" };

function fakeSource() {
  const stub = {
    readyState: EventSource.OPEN, onopen: null, onmessage: null, onerror: null,
    close() { stub.readyState = EventSource.CLOSED; },
  };
  const source = stub as unknown as EventSource;
  return {
    source,
    receive(rows: RawVehicle[]) {
      source.onmessage?.({ data: JSON.stringify(rows), lastEventId: 'provider-frame' } as MessageEvent<string>);
    },
  };
}

test('collector entry selects only the requested runtime and rejects invalid modes before launch', async () => {
  assert.equal(collectorMode(undefined), 'local');
  assert.equal(collectorMode('local'), 'local'); assert.equal(collectorMode('cloud'), 'cloud');
  const launched: string[] = [];
  const launchers = { local: async () => { launched.push('local'); }, cloud: async () => { launched.push('cloud'); } };
  await launchCollector('cloud', launchers);
  assert.deepEqual(launched, ['cloud']);
  for (const value of ['', 'CLOUD', 'server']) await assert.rejects(launchCollector(value, launchers), /COLLECTOR_MODE/);
  assert.deepEqual(launched, ['cloud']);
  await launchCollector('local', launchers);
  assert.deepEqual(launched, ['cloud', 'local']);
});

test('cloud collection persists original raw watermarks and dense receipts without starting analysis', async () => {
  const feed = fakeSource(); const raw: TransitRecord[] = [], snapshots: StreetcarSnapshot[] = [];
  const periods: number[] = []; let initialized = 0, closed = 0;
  const collector = createCloudCollector({
    initialize: async () => { initialized++; }, close: async () => { closed++; },
    insertRecords: async rows => { raw.push(...rows); }, insertSnapshots: async rows => { snapshots.push(...rows); },
    createEventSource: url => { assert.equal(url, 'https://nolatransit.fly.dev/sse'); return feed.source; },
    setInterval: ((callback: () => void, milliseconds: number) => {
      periods.push(milliseconds); return setInterval(callback, 2_000_000_000);
    }) as typeof setInterval,
  }, { MOTHER_DUCK_API_KEY: 'test-only' });
  try {
    await collector.start();
    assert.equal(initialized, 1);
    assert.deepEqual(periods, [60_000, 60_000, 60_000]);
    assert.deepEqual(collector.health().analysis, { enabled: false, running: false });
    feed.receive([vehicle, { ...vehicle, vid: 'bus', rt: '3' }, { ...vehicle, vid: 'riverfront', rt: '49' }]);
    feed.receive([{ ...vehicle, lat: '29.9402' }]);
    feed.receive([{ ...vehicle, tmstmp: '2026-09-13T08:02:00-05:00' }]);
    await collector.flush();
    assert.equal(raw.length, 4); assert.equal(snapshots.length, 3);
    assert.equal(raw[0].timestamp, vehicle.tmstmp);
    assert.equal(raw[0].trip_id, 'legacy-17'); assert.equal(raw[0].gtfs_trip_id, '12345');
    assert.equal(raw[0].speed, null);
    assert.deepEqual(snapshots.map(row => row.route), ['12', '12', '12']);
    assert.equal(snapshots[0].provider_timestamp, snapshots[1].provider_timestamp);
    assert.notEqual(snapshots[0].lat, snapshots[1].lat);
    assert.notEqual(snapshots[0].snapshot_id, snapshots[1].snapshot_id);
    assert.ok(snapshots.every(row => row.received_at && row.feed_event_id === 'provider-frame'));
    assert.equal(collector.health().stats.vehiclesDeduped, 1);
    assert.equal(collector.health().pending_records, 0);
    assert.equal(collector.health().pending_snapshots, 0);
    assert.ok(collector.health().last_persisted_at);
  } finally { await collector.shutdown('test'); }
  assert.equal(closed, 1);
});

test('cloud outages retain bounded retry buffers and resume writing the retained records', async () => {
  const feed = fakeSource(); let failing = true; const raw: TransitRecord[] = [], snapshots: StreetcarSnapshot[] = [];
  const collector = createCloudCollector({
    initialize: async () => {}, close: async () => {}, createEventSource: () => feed.source,
    insertRecords: async rows => { if (failing) throw new Error('offline'); raw.push(...rows); },
    insertSnapshots: async rows => { if (failing) throw new Error('offline'); snapshots.push(...rows); },
  }, { MOTHER_DUCK_API_KEY: 'test-only', MAX_BUFFER_RECORDS: '2', MAX_SNAPSHOT_BUFFER_RECORDS: '2' });
  try {
    await collector.start();
    feed.receive(['a', 'b', 'c'].map(vid => ({ ...vehicle, vid })));
    await collector.flush();
    assert.equal(collector.health().pending_records, 2); assert.equal(collector.health().pending_snapshots, 2);
    assert.equal(collector.health().stats.recordsDropped, 1); assert.equal(collector.health().stats.snapshotsDropped, 1);
    assert.equal(collector.health().last_persisted_at, null);
    failing = false; await collector.flush();
    assert.deepEqual(raw.map(row => row.vid), ['b', 'c']);
    assert.deepEqual(snapshots.map(row => row.vid), ['b', 'c']);
    assert.equal(collector.health().pending_records, 0); assert.equal(collector.health().pending_snapshots, 0);
  } finally { await collector.shutdown('test'); }
});

test('cloud shutdown waits for the active upload and drains later receipts before closing the database', async () => {
  const feed = fakeSource(), written: string[] = []; let finish!: () => void, writing = false, closed = false;
  const collector = createCloudCollector({
    initialize: async () => {}, close: async () => { closed = true; }, createEventSource: () => feed.source,
    insertSnapshots: async () => {},
    insertRecords: async rows => {
      assert.equal(closed, false); assert.equal(writing, false); writing = true;
      if (!written.length) await new Promise<void>(resolve => { finish = resolve; });
      written.push(...rows.map(row => row.vid)); writing = false;
    },
  }, { MOTHER_DUCK_API_KEY: 'test-only' });
  await collector.start();
  feed.receive([vehicle]);
  const flush = collector.flush();
  feed.receive([{ ...vehicle, vid: 'later' }]);
  const shutdown = collector.shutdown('test');
  assert.equal(closed, false);
  finish(); await Promise.all([flush, shutdown]);
  assert.deepEqual(written, ['900', 'later']); assert.equal(closed, true);
});

test('failed MotherDuck initialization does not connect the feed or start timers', async () => {
  const collector = createCloudCollector({
    initialize: async () => { throw new Error('database unavailable'); },
    createEventSource: () => { throw new Error('must not connect'); },
    setInterval: (() => { throw new Error('must not start timers'); }) as typeof setInterval,
  }, { MOTHER_DUCK_API_KEY: 'test-only' });
  await assert.rejects(collector.start(), /database unavailable/);
  assert.equal(collector.health().status, 'starting');
});

test('cloud SQL inserts absent and invalid speed as NULL while preserving zero, timestamps and trip IDs', async () => {
  const db = await DuckDBInstance.create(':memory:'); const c = await db.connect();
  try {
    await initializeTransitSchema(c, 'memory');
    const records = [undefined, 'N/A', '0', '17'].map((spd, index) => processVehicle({ ...vehicle, vid: String(index), spd })!);
    await c.run(transitInsertSql(records, 'memory'));
    const rows = (await c.runAndReadAll(`SELECT vid,speed,trip_id,gtfs_trip_id,destination,
      epoch(observed_at) AS observed_epoch FROM transit_data ORDER BY vid`)).getRowObjectsJson();
    assert.deepEqual(rows.map(row => row.speed), [null, null, 0, 17]);
    for (const row of rows) {
      assert.equal(row.trip_id, 'legacy-17'); assert.equal(row.gtfs_trip_id, '12345');
      assert.equal(row.destination, vehicle.des);
      assert.equal(Number(row.observed_epoch), Date.parse(vehicle.tmstmp) / 1000);
    }
  } finally { c.closeSync(); db.closeSync(); }
});
