import assert from 'node:assert/strict';
import test from 'node:test';
import { DuckDBInstance } from '@duckdb/node-api';
import { analyzeStudyDay, legacyStudyObservation, prepareStudyDay, LEGACY_STUDY_DATE_SQL, pendingStudyDaysSql } from './local-worker-logic';
import { rowStudyCells, signalStudyCells } from './transit-study-summary';
import type { StudyObservation } from './observation-types';
import type { StudyCatalog } from './transit-study-types';
import { LONGITUDE_SCALE } from './transit-study-geometry';
import { restoreImportedStudyKeys } from './local-schema-repair';
import { compactStudyPublication } from './local-publication';
import { rowCell, rowData, signalCell, signalData } from '../dashboard/src/study-fixtures';

const DATE = '2026-09-08', AT = Date.parse(`${DATE}T13:00:00Z`) / 1000;
const point = (x: number) => ({ lat: 29.95, lon: -90.1 + x / LONGITUDE_SCALE });
const catalog: StudyCatalog = { version: 'worker-test', generated_at: DATE, schedule_hash: 'fixture', sources: [], limitations: [], row_sections: [],
  paths: [{ id: 'out', route_id: '12', direction_id: '0', mode: 'streetcar', name: 'Outbound', points: [point(0), point(2000)], stop_ids: [] }],
  sites: [{ id: 'signal', kind: 'signal', name: 'Signal', ...point(500), route_ids: ['12'], source_ids: ['osm/node/1'], verification: 'osm_unverified' }] };
function observation(x: number, t: number, patch: Partial<StudyObservation> = {}): StudyObservation {
  return { source: 'sse', observation_id: `o:${x}:${t}`, vehicle_id: 'sse:460', provider_vehicle_id: '460', route_id: '12', trip_id: 'trip',
    observed_at: AT + t, received_at: AT + t, ...point(x), speed_mph: 0, off_route: false, location_source: 'provider_gps',
    timestamp_precision_seconds: 60, direction_id: '0', pattern_id: null, mapping_confidence: 'verified', ...patch };
}
const blank = { historical: [], dense: [], legacy: [] };

test('provider-only history creates complete passages but never a wait estimate or encounter denominator', () => {
  const rows = [[300, 0], [500, 60], [500, 120], [500, 180], [700, 240]].map(([x, t]) => observation(x, t));
  const result = analyzeStudyDay(catalog, DATE, { ...blank, legacy: rows });
  assert.equal(result.passages.length, 1); assert.equal(result.encounters.length, 0);
  assert.equal(result.quality.reduce((n, q) => n + q.complete_encounters, 0), 0);
  assert.equal(rowStudyCells(catalog, result.passages).reduce((n, c) => n + c.passages, 0), 1);
  assert.equal(signalStudyCells(catalog, result.encounters).length, 0);
  assert.ok(result.quality.some(q => q.excluded.legacy_missing_receipt_history === rows.length));
});
test('retained historical and normalized copies plus legacy points count one physical run, regardless of changed trip mapping', () => {
  const dense = [observation(300, 0), observation(700, 60)];
  const legacy = dense.map(o => ({ ...o, observation_id: 'legacy:' + o.observation_id, trip_id: 'old-TA-trip', vehicle_id: '460' }));
  const result = analyzeStudyDay(catalog, DATE, { dense, historical: [...dense].reverse(), legacy });
  assert.equal(result.passages.length, 1); assert.equal(result.encounters.length, 1);
  assert.equal(result.quality.reduce((n, q) => n + (q.excluded.duplicate_receipt_copy ?? 0), 0), 2);
  assert.equal(result.quality.reduce((n, q) => n + (q.excluded.legacy_receipt_overlap ?? 0), 0), 2);
  assert.deepEqual(analyzeStudyDay(catalog, DATE, { dense: [...dense].reverse(), historical: dense, legacy: [...legacy].reverse() }).passages, result.passages);
});
test('independent delayed recorder copies cannot replay a passage or double a detected wait', () => {
  const dense = [observation(300, 0), ...[10, 20, 30].map(t => observation(500, 0, {
    observation_id: `local-wait:${t}`, received_at: AT + t,
  })), observation(700, 60)];
  const historical = dense.map(o => ({ ...o, observation_id: `server:${o.observation_id}`,
    received_at: o.received_at + 70, trip_id: 'legacy-trip-name', vehicle_id: '460' }));
  const input = { dense, historical, legacy: [] }, before = JSON.stringify(input);
  const local = analyzeStudyDay(catalog, DATE, { ...blank, dense });
  const server = analyzeStudyDay(catalog, DATE, { ...blank, historical });
  assert.equal(local.passages.length, 1); assert.equal(server.passages.length, 1);
  assert.equal(local.encounters.length, 1); assert.equal(server.encounters.length, 1);
  assert.equal(local.encounters[0].wait_events, 1); assert.equal(local.encounters[0].wait_seconds, 20);
  assert.equal(server.encounters[0].wait_seconds, 20);
  const result = analyzeStudyDay(catalog, DATE, input);
  assert.deepEqual(result.passages, local.passages); assert.deepEqual(result.encounters, local.encounters);
  assert.equal(result.quality.reduce((n, q) => n + (q.excluded.historical_dense_overlap ?? 0), 0), historical.length);
  const reordered = analyzeStudyDay(catalog, DATE, { ...input, dense: [...dense].reverse(), historical: [...historical].reverse() });
  assert.deepEqual(reordered.passages, result.passages); assert.deepEqual(reordered.encounters, result.encounters);
  assert.equal(JSON.stringify(input), before, 'recorder selection must not rewrite raw evidence or clocks');
});
test('server snapshots fill long local recording gaps and entire missing dates without bridging recorder transitions', () => {
  const dense = [observation(300, 0), observation(700, 60), observation(300, 600), observation(700, 660)];
  const middle = [observation(300, 240), observation(700, 300)];
  const historical = [...dense, ...middle, observation(800, 120)].map(o => ({ ...o,
    observation_id: `server:${o.observation_id}`, received_at: o.received_at + 5 }));
  const selected = prepareStudyDay(DATE, { ...blank, dense, historical });
  const keptServer = selected.receipts.filter(o => o.observation_id.startsWith('server:'));
  assert.deepEqual(keptServer.map(o => o.observed_at), [AT + 240, AT + 300]);
  assert.equal(selected.excluded.historical_dense_overlap, 5, 'include the conservative 90-second transition margin');
  const result = analyzeStudyDay(catalog, DATE, { ...blank, dense, historical });
  assert.equal(result.passages.length, 3); assert.equal(result.encounters.length, 3);
  const nextDate = '2026-09-09';
  const nextDay = middle.map(o => ({ ...o, observed_at: o.observed_at! + 86400, received_at: o.received_at + 86400 }));
  const recovered = analyzeStudyDay(catalog, nextDate, { ...blank, dense, historical: nextDay });
  assert.equal(recovered.passages.length, 1); assert.equal(recovered.encounters.length, 1);
  assert.ok(recovered.quality.every(q => q.date === nextDate));
});
test('recorder precedence is source, route, vehicle and provider-day specific, with valid local clocks required', () => {
  const historical = [observation(300, 0, { observation_id: 'server' })];
  for (const patch of [
    { source: 'lepass' as const, vehicle_id: 'lepass:460' },
    { route_id: '47' }, { provider_vehicle_id: '461', vehicle_id: 'sse:461' },
    { received_at: AT + 121 }, { received_at: AT - 1 },
  ]) {
    const dense = [observation(300, 0, { observation_id: 'local', ...patch })];
    assert.ok(prepareStudyDay(DATE, { ...blank, dense, historical }).receipts.some(o => o.observation_id === 'server'));
  }
  const midnight = Date.parse('2026-09-09T05:00:00Z') / 1000;
  const server = observation(300, 0, { observation_id: 'server', observed_at: midnight - 30, received_at: midnight - 20 });
  const local = { ...server, observation_id: 'local', received_at: midnight + 10 };
  const selected = prepareStudyDay(DATE, { ...blank, dense: [local], historical: [server] });
  assert.deepEqual(selected.receipts.map(o => o.observation_id), ['local']);
  assert.equal(selected.receipts[0].received_at, midnight + 10);
  assert.equal(prepareStudyDay('2026-09-09', { ...blank, dense: [local], historical: [server] }).receipts.length, 0);
});
test('overlap ranges sort provider time, ignore trip renaming and never suppress a different source', () => {
  const dense = [observation(600, 240), observation(300, 0), observation(400, 60)];
  const legacy = [observation(320, 20, { trip_id: null }), observation(650, 250), observation(900, 900)];
  const selected = prepareStudyDay(DATE, { ...blank, dense, legacy });
  assert.deepEqual(selected.provider_only.map(o => o.observed_at), [AT + 900]);
  const lepass = dense.map(o => ({ ...o, source: 'lepass' as const, vehicle_id: 'lepass:460' }));
  assert.equal(prepareStudyDay(DATE, { ...blank, dense: lepass, legacy }).provider_only.length, 3);
});
test('contradictory provider-only coordinates reject that sample and break surrounding passage continuity', () => {
  const legacy = [observation(300, 0), observation(450, 20), observation(550, 20), observation(700, 40)];
  const result = analyzeStudyDay(catalog, DATE, { ...blank, legacy });
  assert.equal(result.passages.length, 0);
  assert.equal(result.quality.reduce((n, q) => n + (q.excluded.ambiguous_legacy_provider_minute ?? 0), 0), 2);
});
test('fresh next-receipt-day samples belong only to their Chicago provider day', () => {
  const midnight = Date.parse('2026-09-09T05:00:00Z') / 1000;
  const dense = [observation(300, 0, { source: 'lepass', observed_at: midnight - 50, received_at: midnight + 10, timestamp_precision_seconds: .001 }),
    observation(700, 0, { source: 'lepass', observed_at: midnight - 30, received_at: midnight + 20, timestamp_precision_seconds: .001 })];
  const correct = analyzeStudyDay(catalog, '2026-09-08', { ...blank, dense });
  assert.equal(correct.passages.length, 1); assert.ok(correct.quality.every(q => q.date === '2026-09-08'));
  assert.equal(analyzeStudyDay(catalog, '2026-09-09', { ...blank, dense }).passages.length, 0);
  assert.equal(correct.passages[0].vehicle_id, 'lepass:460');
});
test('legacy conversion preserves UTC instants, parses old Chicago wall time, retains missing fields and has stable identity', () => {
  const row = { vid: '460', route: '12', trip: null, instant: null, wall_time: '2026-09-08 08:00:00', lat: null, lon: null, is_off_route: false, pid: null };
  const a = legacyStudyObservation(row)!;
  assert.equal(a.observed_at, AT); assert.equal(a.lat, null); assert.equal(a.speed_mph, null);
  assert.equal(legacyStudyObservation({ ...row, instant: AT, wall_time: 'untrusted wall clock' })!.observed_at, AT);
  assert.equal(legacyStudyObservation({ ...row })!.observation_id, a.observation_id);
  assert.equal(legacyStudyObservation({ ...row, wall_time: 'invalid' }), null);
});
test('explicit Not in Service observations cannot enter either current or historical passage metrics', () => {
  const dense = [observation(300, 0, { in_service: false }), observation(700, 60, { in_service: false })];
  assert.equal(analyzeStudyDay(catalog, DATE, { ...blank, dense }).passages.length, 0);
  assert.equal(analyzeStudyDay(catalog, DATE, { ...blank, legacy: dense }).passages.length, 0);
});
test('SQL revisions use local provider dates and invalidate the preceding day only for fresh after-midnight receipts', async () => {
  const db = await DuckDBInstance.create(':memory:'), c = await db.connect();
  try {
    await c.run('CREATE TABLE transit_data(timestamp TIMESTAMP,observed_at TIMESTAMPTZ)');
    await c.run('CREATE TABLE collection_batches(received_at TIMESTAMPTZ,observations INTEGER)');
    await c.run('CREATE TABLE streetcar_snapshots(provider_observed_at TIMESTAMPTZ,received_at TIMESTAMPTZ)');
    await c.run('CREATE TABLE study_dates(date DATE,source_revision VARCHAR,method_revision VARCHAR)');
    await c.run("INSERT INTO transit_data VALUES ('2026-09-08 04:00:00','2026-09-08T04:00:00Z'),('2026-09-07 23:00:00',NULL)");
    const days = (await c.runAndReadAll(`SELECT ${LEGACY_STUDY_DATE_SQL}::VARCHAR AS day FROM transit_data`)).getRowObjectsJS();
    assert.deepEqual(days.map(d => d.day), ['2026-09-07', '2026-09-07']);
    const pending = async () => (await c.runAndReadAll(pendingStudyDaysSql('method'))).getRowObjectsJS() as Array<{ date: string; revision: string }>;
    const first = await pending(); assert.equal(first.length, 1); assert.equal(first[0].date, '2026-09-07');
    await c.run("INSERT INTO collection_batches VALUES ('2026-09-08T05:00:20Z',10)");
    const midnight = await pending(); assert.equal(midnight.length, 2);
    assert.notEqual(midnight.find(d => d.date === '2026-09-07')!.revision, first[0].revision);
    const prior = midnight.find(d => d.date === '2026-09-07')!.revision;
    await c.run("INSERT INTO collection_batches VALUES ('2026-09-08T15:00:00Z',10)");
    assert.equal((await pending()).find(d => d.date === '2026-09-07')!.revision, prior);
    await c.run("INSERT INTO streetcar_snapshots VALUES ('2026-09-08T04:59:30Z','2026-09-08T05:00:05Z')");
    const after = await pending(); assert.notEqual(after.find(d => d.date === '2026-09-07')!.revision, prior);
    assert.ok(after.find(d => d.date === '2026-09-07')!.revision.startsWith('historical-local-precedence-v1:'));
    assert.ok(!after.find(d => d.date === '2026-09-08')!.revision.startsWith('historical-local-precedence-v1:'));
    for (const d of after) await c.run(`INSERT INTO study_dates VALUES ('${d.date}','${d.revision}','method')`);
    assert.equal((await pending()).length, 0);
    // A deployment with previously imported overlap must rerun that date, but
    // unchanged dates with only one recorder retain their accepted revision.
    await c.run("UPDATE study_dates SET source_revision=replace(source_revision,'historical-local-precedence-v1:','') WHERE date='2026-09-07'");
    assert.deepEqual((await pending()).map(d => d.date), ['2026-09-07']);
    assert.equal((await c.runAndReadAll("SELECT regexp_matches('not  in service','not\\s+in\\s+service') AS excluded")).getRowObjectsJS()[0].excluded, true);
  } finally { c.closeSync(); db.closeSync(); }
});

test('imported derived tables regain conflict keys without changing original rows or silently choosing duplicates', async () => {
  const db = await DuckDBInstance.create(':memory:'), c = await db.connect();
  try {
    await c.run("CREATE TABLE streetcar_networks AS SELECT 'old'::VARCHAR AS version,'original'::VARCHAR AS network_json");
    await restoreImportedStudyKeys(c); await restoreImportedStudyKeys(c);
    await c.run("INSERT INTO streetcar_networks VALUES ('old','updated') ON CONFLICT(version) DO UPDATE SET network_json=excluded.network_json");
    assert.deepEqual((await c.runAndReadAll('SELECT * FROM streetcar_networks')).getRowObjectsJS(), [{ version: 'old', network_json: 'updated' }]);
    await c.run("CREATE TABLE streetcar_priority_days AS SELECT '2026-09-08'::DATE AS date FROM range(2)");
    await assert.rejects(restoreImportedStudyKeys(c));
    assert.equal((await c.runAndReadAll('SELECT COUNT(*) AS n FROM streetcar_priority_days')).getRowObjectsJS()[0].n, 2n);
  } finally { c.closeSync(); db.closeSync(); }
});

test('public compaction preserves unknown coverage dimensions and signal denominators without mutating full daily evidence', () => {
  const cells = Array.from({ length: 100 }, (_, i) => rowCell({ row_class: 'unknown', path_id: `path-${i}`, passages: i + 1,
    hour: i % 2 ? 8 : 16, time_band: i % 2 ? 2 : 4, source: i % 3 ? 'sse' : 'lepass',
    run_ids: Array.from({ length: 20 }, (_, j) => `run:${i}:${j}`), window_ids: [`window:${i}`] }));
  cells.push(rowCell({ row_class: 'reserved', passages: 9 }));
  const row = rowData(cells), signals = signalData([signalCell({ evaluable_encounters: 0, detected_wait_encounters: 0, wait_events: 0, wait_seconds: 0 })]);
  const before = JSON.stringify({ row, signals }), compact = compactStudyPublication(row, signals);
  assert.equal(JSON.stringify({ row, signals }), before);
  assert.equal(compact.row.cells.length, 1); assert.equal(compact.row.cells[0].row_class, 'reserved');
  assert.equal(compact.row.cells[0].run_ids, undefined); assert.equal(compact.signal.cells[0].run_ids, undefined);
  assert.equal(compact.row.coverage.unknown_passages, 5050); assert.equal(compact.row.coverage.passages, 5059);
  assert.equal(compact.row.coverage_cells!.length, 4);
  assert.ok(compact.row.coverage_cells!.every(c => !('path_id' in c) && !('context' in c)));
  for (const source of ['sse', 'lepass'] as const) for (const hour of [8, 16]) {
    const expected = cells.filter(c => c.source === source && c.hour === hour && c.row_class === 'unknown').reduce((n, c) => n + c.passages, 0);
    const actual = compact.row.coverage_cells!.filter(c => c.source === source && c.hour === hour).reduce((n, c) => n + c.passages, 0);
    assert.equal(actual, expected);
  }
  assert.deepEqual(compact.signal.cells.map(({ run_ids: _ids, ...c }) => c), signals.cells.map(({ run_ids: _ids, ...c }) => c));
  assert.ok(JSON.stringify(compact).length < before.length / 3);
  assert.deepEqual(compactStudyPublication(compact.row, compact.signal), compact);
});
