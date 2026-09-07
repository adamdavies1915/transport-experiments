import { test } from 'node:test';
import assert from 'node:assert/strict';
import { zipSync, strToU8 } from 'fflate';
import { DuckDBInstance } from '@duckdb/node-api';
import { activeServices, classifyInterval, estimateOtp, gtfsSeconds, observationEpoch,
  readSchedule, serviceEpoch, type Observation, type Schedule } from './otp';
import { calculateDay, initializeOtp, refreshMappedBackfill, saveSchedule } from './otp-worker';
import { totals } from '../dashboard/src/otp-data';
import { otpDaysSql } from '../dashboard/src/otp-query';
import { processVehicle } from './vehicle';
import { importCrosswalk, loadCrosswalk, mappedTrip } from './trip-crosswalk';
import { validateReconstruction } from './otp-validation';

const day = '2026-09-07';
const base = serviceEpoch(day, 'America/Chicago');
test('collector preserves the verified tripid and unknown flags independently of legacy tatripid', () => {
  const v = { vid: '325', tmstmp: '2026-09-07T13:02:00-05:00', lat: '29.96', lon: '-90.1',
    rt: '3', tripid: 1298070, tatripid: '3748674' };
  const record = processVehicle(v)!;
  assert.equal(record.gtfs_trip_id, '1298070');
  assert.equal(record.trip_id, '3748674');
  assert.equal(record.is_delayed, null);
  assert.equal(processVehicle({ ...v, dly: false })!.is_delayed, false);
  assert.equal(processVehicle({ ...v, dly: true })!.is_delayed, true);
  assert.equal(processVehicle({ ...v, lat: 'bad' }), null);
  assert.equal(processVehicle({ ...v, tmstmp: 'bad' }), null);
});
function schedule(): Schedule {
  return { hash: 'test', timezone: 'America/Chicago', start: day, end: '2026-09-08',
    calendar: [], exceptions: [{ service_id: 'holiday', date: '20260907', exception_type: '1' }],
    trips: [{ id: 't1', route: '12', service: 'holiday', block: '42', headsign: 'Downtown', stops: [
      { id: 'a', sequence: 1, lat: 30, lon: -90, arrival: 36000, departure: 36000, timepoint: true },
      { id: 'b', sequence: 2, lat: 30.01, lon: -90, arrival: 36600, departure: 36600, timepoint: true },
      { id: 'c', sequence: 3, lat: 30.02, lon: -90, arrival: 37200, departure: 37200, timepoint: true },
    ] }] };
}
function pings(deviation = 0): Observation[] {
  return [0, 20].map((seconds, i) => ({
    vid: 'v1', trip_id: 't1', route: '12', block: '42', destination: 'Downtown',
    exact_id: true,
    at: base + 36600 + deviation + seconds, lat: 30.01 + i * .001, lon: -90, off_route: false,
  }));
}
test('early/on-time/late boundaries and uncertain intervals', () => {
  assert.equal(classifyInterval(-61, -60.1), 'early');
  assert.equal(classifyInterval(-60, 300), 'on_time');
  assert.equal(classifyInterval(300.1, 301), 'late');
  assert.equal(classifyInterval(-61, -60), 'uncertain');
  assert.equal(classifyInterval(300, 301), 'uncertain');
});
test('counts stop events once regardless of repeated pings or dly flags', () => {
  const raw = pings();
  const once = estimateOtp(schedule(), day, raw);
  const duplicates = estimateOtp(schedule(), day, [...raw, ...raw, ...raw]);
  assert.deepEqual(duplicates, once);
  assert.equal(once.events.length, 1);
  assert.equal(once.events[0].status, 'on_time');
  assert.equal(once.coverage[0].scheduled_timepoints, 3);
  assert.equal(once.coverage[0].classified_timepoints, 1);
});
test('early and severely late exact-ID trips are measured without nearest-time snapping', () => {
  assert.equal(estimateOtp(schedule(), day, pings(-300)).events[0].status, 'early');
  assert.equal(estimateOtp(schedule(), day, pings(7200)).events[0].status, 'late');
});
test('missing data, off-route vehicles, long gaps, and invalid coordinates yield no OTP events', () => {
  assert.equal(estimateOtp(schedule(), day, []).events.length, 0);
  for (const raw of [pings().map(p => ({ ...p, off_route: true })),
    pings().map(p => ({ ...p, lat: NaN })), pings().map(p => ({ ...p, trip_id: null })),
    pings().map((p, i) => ({ ...p, at: p.at + i * 300 }))]) {
    assert.equal(estimateOtp(schedule(), day, raw).events.length, 0);
  }
});
test('a boundary-straddling departure remains uncertain', () => {
  const result = estimateOtp(schedule(), day, pings(290));
  assert.equal(result.events[0].status, 'uncertain');
  assert.equal(result.coverage[0].observed_timepoints, 1);
  assert.equal(result.coverage[0].classified_timepoints, 0);
});
test('uses departure after dwelling, and arrival at the terminal', () => {
  const raw = pings(600);
  raw.unshift({ ...raw[0], at: base + 36600 });
  const result = estimateOtp(schedule(), day, raw);
  assert.equal(result.events[0].status, 'late');
  const terminal = pings().map((p, i) => ({ ...p, at: base + 37200 + i * 20, lat: i === 0 ? 30.019 : 30.02 }));
  assert.equal(estimateOtp(schedule(), day, terminal).events[0].stop_id, 'c');
});
test('unique block/headsign matches work; ambiguous or conflicting assignments stay unknown', () => {
  const raw = pings().map(p => ({ ...p, trip_id: 'bustime-id', exact_id: false }));
  assert.equal(estimateOtp(schedule(), day, raw).events[0].match_method, 'block');
  const ambiguous = schedule();
  ambiguous.trips.push({ ...ambiguous.trips[0], id: 't2' });
  assert.equal(estimateOtp(ambiguous, day, raw).events.length, 0);
  assert.equal(estimateOtp(schedule(), day, raw.map(p => ({ ...p, destination: 'Other direction' }))).events.length, 0);
  assert.equal(estimateOtp(schedule(), day, [...pings(), ...pings().map(p => ({ ...p, vid: 'v2' }))]).events.length, 0);
});
test('a legacy ID string collision is never promoted to an exact schedule match', () => {
  const events = estimateOtp(schedule(), day, pings().map(p => ({ ...p, exact_id: false }))).events;
  assert.equal(events[0].match_method, 'block');
});
test('loops with indistinguishable stop coordinates do not fabricate events', () => {
  const s = schedule(); s.trips[0].stops[2].lat = 30.01;
  assert.equal(estimateOtp(s, day, pings()).events.length, 0);
});
test('holiday additions/removals override weekday calendars', () => {
  const s = schedule();
  s.calendar.push({ service_id: 'weekday', start_date: '20260901', end_date: '20260930', monday: '1' });
  s.exceptions.push({ service_id: 'weekday', date: '20260907', exception_type: '2' });
  assert.deepEqual([...activeServices(s, day)], ['holiday']);
  assert.deepEqual([...activeServices(s, '2026-09-09')], []);
});
test('after-midnight trips stay on their GTFS service day, including crossings of 03:00', () => {
  for (const offset of [15 * 3600, 17 * 3600]) {
    const s = schedule();
    s.trips[0].stops = s.trips[0].stops.map(p => ({ ...p, arrival: p.arrival + offset, departure: p.departure + offset }));
    const result = estimateOtp(s, day, pings().map(p => ({ ...p, at: p.at + offset })));
    assert.equal(result.events.length, 1);
    assert.equal(result.events[0].service_date, day);
    assert.equal(result.events[0].status, 'on_time');
  }
  assert.equal(gtfsSeconds('25:10:00'), 90600);
});
test('timezone conversion respects offsets and rejects ambiguous legacy DST timestamps', () => {
  assert.equal(observationEpoch('2026-09-07 10:10:00', 'America/Chicago'), base + 36600);
  assert.equal(observationEpoch('2026-09-07T10:10:00-05:00', 'America/Chicago'), base + 36600);
  assert.equal(observationEpoch('2026-11-01 01:30:00', 'America/Chicago'), null);
  assert.equal(observationEpoch('2026-03-08 02:30:00', 'America/Chicago'), null);
  assert.equal(new Date(serviceEpoch('2026-03-08', 'America/Chicago') * 1000).toISOString(), '2026-03-08T05:00:00.000Z');
});
test('current-day coverage counts only scheduled events due so far', () => {
  const r = estimateOtp(schedule(), day, pings(), base + 36300);
  assert.equal(r.coverage[0].scheduled_timepoints, 1);
  assert.equal(r.events.length, 0);
});
test('monthly totals weight events, preserve zero OTP, and leave empty selections null', () => {
  const blank = { date: day, route: '12', scheduled: 100, observed: 100, classified: 100, early: 0,
    late: 0, uncertain: 0, block_events: 0, crosswalk_events: 0, observed_trips: 1, matched_trips: 1, block_matched_trips: 0, updated_at: '' };
  assert.equal(totals([{ ...blank, on_time: 100 }, { ...blank, classified: 1, on_time: 0, late: 1 }]).on_time_pct, 99.01);
  assert.equal(totals([{ ...blank, on_time: 0, late: 100 }]).on_time_pct, 0);
  assert.equal(totals([]).on_time_pct, null);
});

const archive = () => zipSync(Object.fromEntries(Object.entries({
  'agency.txt': 'agency_timezone\nAmerica/Chicago\n',
  'feed_info.txt': 'feed_start_date,feed_end_date\n20260907,20260908\n',
  'routes.txt': 'route_id,route_short_name,route_type\nr,12,0\n',
  'trips.txt': 'trip_id,route_id,service_id,block_id,trip_headsign\nt1,r,holiday,42,"Downtown, CBD"\n',
  'stops.txt': 'stop_id,stop_lat,stop_lon\na,30,-90\nb,30.01,-90\nc,30.02,-90\n',
  'stop_times.txt': 'trip_id,arrival_time,departure_time,stop_id,stop_sequence,timepoint\nt1,10:00:00,10:00:00,a,1,1\nt1,10:10:00,10:10:00,b,2,1\nt1,10:20:00,10:20:00,c,3,1\n',
  'calendar_dates.txt': 'service_id,date,exception_type\nholiday,20260907,1\n',
}).map(([k, v]) => [k, strToU8(v)])));
test('parses ZIP/CSV, quoted destinations, calendar-only service, and feed validity', () => {
  const s = readSchedule(archive());
  assert.equal(s.trips[0].headsign, 'Downtown, CBD');
  assert.equal(s.trips[0].stops.length, 3);
  assert.equal(s.start, day);
  assert.equal(s.hash.length, 64);
});
test('database calculation/backfill is idempotent and preserves schedule provenance', async () => {
  const instance = await DuckDBInstance.create(':memory:');
  const connection = await instance.connect();
  try {
    await initializeOtp(connection);
    const s = await saveSchedule(connection, archive(), 'test-fixture.zip', day);
    await connection.run(`CREATE TABLE transit_data AS SELECT 'v1' AS vid, 't1' AS trip_id,
      '12' AS route, '42' AS tablockid, 'Downtown' AS destination, 30.01 AS lat,
      -90.0 AS lon, false AS is_off_route, TIMESTAMP '2026-09-07 10:10:00' AS timestamp,
      TIMESTAMPTZ '2026-09-07 10:10:00-05:00' AS observed_at, 't1' AS gtfs_trip_id
      UNION ALL SELECT 'v1', 't1', '12', '42', 'Downtown', 30.011, -90, false,
      TIMESTAMP '2026-09-07 10:10:20', TIMESTAMPTZ '2026-09-07 10:10:20-05:00', 't1'`);
    await calculateDay(connection, day, s, base + 86400);
    await calculateDay(connection, day, s, base + 86400);
    const events = (await connection.runAndReadAll('SELECT * FROM otp_events')).getRowObjectsJson();
    assert.equal(events.length, 1);
    assert.equal(events[0].status, 'on_time');
    assert.equal(events[0].schedule_hash, s.hash);
    const counts = (await connection.runAndReadAll('SELECT * FROM otp_coverage')).getRowObjectsJson();
    assert.equal(counts[0].classified_timepoints, 1);
    assert.equal(counts[0].scheduled_timepoints, 3);
    const catalog = String((await connection.runAndReadAll('SELECT current_database() AS name')).getRowObjectsJson()[0].name);
    let api = (await connection.runAndReadAll(otpDaysSql(catalog))).getRowObjectsJson();
    assert.equal(api[0].on_time, 1);
    assert.equal(api[0].classified, 1);
    assert.equal(api[0].scheduled, 3);
    // Even a very good inferred historical result cannot inflate reported OTP.
    await connection.run("UPDATE otp_events SET match_method = 'block'");
    api = (await connection.runAndReadAll(otpDaysSql(catalog))).getRowObjectsJson();
    assert.equal(api[0].on_time, 0);
    assert.equal(api[0].classified, 0);
    assert.equal(api[0].block_events, 1);
  } finally { connection.closeSync(); instance.closeSync(); }
});

test('observed mappings recover legacy records, preserve raw data, and reject conflicts/version mismatches', async () => {
  const instance = await DuckDBInstance.create(':memory:');
  const connection = await instance.connect();
  try {
    await initializeOtp(connection);
    const s = readSchedule(archive());
    const evidence = { schedule_sha256: s.hash, evidence_date: day,
      mappings: [{ route: '12', tatripid: 'legacy-1', tripid: 't1' }] };
    await assert.rejects(importCrosswalk(connection, s, { ...evidence, schedule_sha256: 'other-version' }));
    await assert.rejects(importCrosswalk(connection, s, { ...evidence, evidence_date: '2026-09-08' }));
    await importCrosswalk(connection, s, evidence);
    await importCrosswalk(connection, s, evidence); // repeat imports do not manufacture extra evidence
    const mapping = await loadCrosswalk(connection, s);
    assert.equal(mappedTrip(mapping, '12', 'legacy-1', '42', 'Downtown, CBD')?.id, 't1');
    assert.equal(mappedTrip(mapping, '12', 'legacy-1', '99', 'Downtown, CBD'), undefined);
    assert.equal(mappedTrip(mapping, '12', 'legacy-1', null, 'Other destination'), undefined);
    assert.equal(mappedTrip(mapping, '9', 'legacy-1', null, null), undefined);
    await connection.run(`CREATE TABLE transit_data AS SELECT 'v1' AS vid, 'legacy-1' AS trip_id,
      NULL::VARCHAR AS gtfs_trip_id, '12' AS route, '42' AS tablockid, 'Downtown, CBD' AS destination,
      30.01 AS lat, -90.0 AS lon, false AS is_off_route,
      TIMESTAMP '2026-09-07 10:10:00' AS timestamp, NULL::TIMESTAMPTZ AS observed_at
      UNION ALL SELECT 'v1', 'legacy-1', NULL, '12', '42', 'Downtown, CBD', 30.011, -90, false,
      TIMESTAMP '2026-09-07 10:10:20', NULL`);
    await calculateDay(connection, day, s, base + 86400);
    const events = (await connection.runAndReadAll('SELECT * FROM otp_events')).getRowObjectsJson();
    assert.equal(events.length, 1);
    assert.equal(events[0].match_method, 'crosswalk');
    assert.equal(events[0].mapping_legacy_id, 'legacy-1');
    assert.equal((await connection.runAndReadAll('SELECT gtfs_trip_id FROM transit_data LIMIT 1')).getRowObjectsJson()[0].gtfs_trip_id, null);
    const catalog = String((await connection.runAndReadAll('SELECT current_database() AS name')).getRowObjectsJson()[0].name);
    let api = (await connection.runAndReadAll(otpDaysSql(catalog))).getRowObjectsJson();
    assert.equal(api[0].crosswalk_events, 1);
    assert.equal(api[0].on_time, 1);
    s.trips.push({ ...s.trips[0], id: 't2' });
    await importCrosswalk(connection, s, { ...evidence, mappings: [{ route: '12', tatripid: 'legacy-1', tripid: 't2' }] });
    assert.equal((await loadCrosswalk(connection, s)).size, 0);
    // The API rejects stale reconstructed events as soon as it queries the new evidence,
    // without waiting for historical recalculation to finish.
    api = (await connection.runAndReadAll(otpDaysSql(catalog))).getRowObjectsJson();
    assert.equal(api[0].crosswalk_events, 0);
    assert.equal(api[0].classified, 0);
  } finally { connection.closeSync(); instance.closeSync(); }
});

test('temporal validation hides held-out IDs and reports wrong trip assignments', () => {
  const s = schedule();
  const heldOut = pings().map(p => ({ ...p, legacy_trip_id: 'legacy' }));
  const training = [{ ...heldOut[0], at: heldOut[0].at - 600 }];
  const split = heldOut[0].at;
  const correct = validateReconstruction(s, day, [...training, ...heldOut], split);
  assert.equal(correct.crosswalk.correct, 1);
  assert.equal(correct.block.correct, 1);
  // A legacy ID seen only in the held-out period cannot train its own mapping.
  const unseen = validateReconstruction(s, day, [...training,
    ...heldOut.map(p => ({ ...p, legacy_trip_id: 'unseen' }))], split);
  assert.equal(unseen.crosswalk.events, 0);
  s.trips.push({ ...s.trips[0], id: 't2' });
  const wrong = validateReconstruction(s, day, [...training,
    ...heldOut.map(p => ({ ...p, trip_id: 't2' }))], split);
  assert.equal(wrong.crosswalk.correct, 0);
  assert.equal(wrong.crosswalk.wrong, 1);
  assert.equal(wrong.crosswalk.mismatches[0].actual, 't2');
  assert.equal(wrong.block.events, 0); // ambiguous trips are not guessed
});

test('mapping refresh only recalculates requested eligible dates and is resumable', async () => {
  const instance = await DuckDBInstance.create(':memory:');
  const connection = await instance.connect();
  try {
    await initializeOtp(connection);
    const s = readSchedule(archive());
    await connection.run(`CREATE TABLE transit_data AS SELECT 'v1' AS vid, 'legacy' AS trip_id,
      't1' AS gtfs_trip_id, '12' AS route, '42' AS tablockid, 'Downtown, CBD' AS destination,
      30.01 AS lat, -90.0 AS lon, false AS is_off_route,
      TIMESTAMP '2026-09-07 10:10:00' AS timestamp,
      TIMESTAMPTZ '2026-09-07 10:10:00-05:00' AS observed_at
      UNION ALL SELECT 'v1', 'legacy', NULL, '12', '42', 'Downtown, CBD', 30.011, -90, false,
      TIMESTAMP '2026-09-07 10:10:20', NULL`);
    assert.deepEqual(await refreshMappedBackfill(connection, s, day), []);
    await connection.run(`INSERT INTO otp_backfill_days VALUES ('${day}', '${s.hash}', NULL, NULL),
      ('2026-09-06', '${s.hash}', NULL, NULL)`);
    assert.deepEqual(await refreshMappedBackfill(connection, s, day), [day]);
    assert.deepEqual(await refreshMappedBackfill(connection, s, day), []);
    const results = (await connection.runAndReadAll('SELECT service_date::VARCHAR AS day, last_completed_at IS NOT NULL AS completed FROM otp_backfill_days ORDER BY service_date')).getRowObjectsJson();
    assert.equal(results[0].completed, false);
    assert.equal(results[1].completed, true);
    assert.equal((await connection.runAndReadAll('SELECT COUNT(*) AS n FROM transit_data WHERE gtfs_trip_id IS NULL')).getRowObjectsJson()[0].n, '1');
  } finally { connection.closeSync(); instance.closeSync(); }
});
