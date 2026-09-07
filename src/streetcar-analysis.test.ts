import assert from 'node:assert/strict';
import test from 'node:test';
import { analyzeStreetcarIntervals as analyzeStreetcars, analyzeStreetcars as analyzePassages, METHOD, type StreetcarObservation } from './streetcar-analysis';
import type { GeoPoint, StreetcarNetwork, StreetcarPath, StreetcarSite } from '../dashboard/src/streetcar-data';

const DATE = '2026-09-07';
const AT = Date.parse(`${DATE}T13:00:00Z`) / 1000;
const METERS_PER_DEGREE = Math.PI * 6371000 / 180;
const LONGITUDE_SCALE = METERS_PER_DEGREE * Math.cos(29.95 * Math.PI / 180);
function point(x: number, y = 0): GeoPoint { return { lat: 29.95 + y / METERS_PER_DEGREE, lon: -90.1 + x / LONGITUDE_SCALE }; }
function path(overrides: Partial<StreetcarPath> = {}): StreetcarPath {
  return { id: 'out', corridor: 'st_charles', route: '12', direction: '0', headsign: 'Outbound',
    points: Array.from({ length: 11 }, (_, n) => point(n * 200)), stop_ids: ['stop-a'], ...overrides };
}
function site(id: string, kind: StreetcarSite['kind'], x: number, y = 0, overrides: Partial<StreetcarSite> = {}): StreetcarSite {
  return { id, kind, corridor: 'st_charles', name: id, routes: ['12'], source_ids: [id],
    verification: kind === 'stop' ? 'gtfs' : 'osm_unverified', ...point(x, y), ...overrides };
}
function network(sites: StreetcarSite[] = [], paths: StreetcarPath[] = [path()]): StreetcarNetwork {
  return { version: 'test', generated_at: DATE, schedule_hash: 'test',
    corridors: [{ id: 'st_charles', name: 'St. Charles', routes: ['12'] }],
    paths, sites, sources: [], mapillary_status: 'Not verified' };
}
function obs(x: number, seconds = 0, y = 0, overrides: Partial<StreetcarObservation> = {}): StreetcarObservation {
  return { vid: 'car1', route: '12', trip_id: 'trip1', at: AT + seconds,
    ...point(x, y), off_route: false, ...overrides };
}
function close(actual: number, expected: number, tolerance = 0.01): void {
  assert.ok(Math.abs(actual - expected) < tolerance, `Expected ${actual} to be within ${tolerance} of ${expected}`);
}

test('a traffic signal crossed between distant endpoints classifies the whole GPS interval', () => {
  const result = analyzeStreetcars(network([site('light', 'signal', 500)]), DATE, [obs(400), obs(600, 60)]);
  assert.equal(result.bins.length, 1);
  assert.equal(result.bins[0].category, 'signal_only');
  assert.equal(result.bins[0].hour, 8);
  assert.equal(result.bins[0].day_type, 'weekday');
  close(result.bins[0].distance_meters, 200);
  assert.equal(result.bins[0].duration_seconds, 60);
  assert.equal(result.site_bins[0].site_id, 'light');
});

test('stop and signal exposures remain combined and multi-site bins explicitly overlap', () => {
  const result = analyzeStreetcars(network([site('light', 'signal', 500), site('stop-a', 'stop', 550)]), DATE,
    [obs(400), obs(650, 60)]);
  assert.equal(result.bins[0].category, 'both');
  assert.equal(result.bins[0].intervals, 1);
  assert.equal(result.site_bins.length, 2);
  assert.ok(result.site_bins.every(bin => bin.category === 'both' && bin.duration_seconds === 60));
  assert.equal(result.quality[0].accepted_intervals, 1);
});

test('signals on a parallel road beyond the feature radius do not classify streetcar exposure', () => {
  const result = analyzeStreetcars(network([site('other-road', 'signal', 500, 45)]), DATE, [obs(400), obs(600, 60)]);
  assert.equal(result.bins[0].category, 'neither');
  assert.equal(result.site_bins.length, 0);
});

test('opposite direction boarding platforms are excluded using the scheduled stop IDs', () => {
  const result = analyzeStreetcars(network([site('other-platform', 'stop', 500, 10),
    site('stop-a', 'stop', 900, 10, { source_ids: ['gtfs:stop:stop-a'] })]), DATE,
  [obs(400), obs(600, 60), obs(1000, 120)]);
  assert.deepEqual(result.bins.map(bin => bin.category).sort(), ['neither', 'stop_only']);
  assert.deepEqual(result.site_bins.map(bin => bin.site_id), ['stop-a']);
});

test('distance follows curved track and does not cut diagonally across an intersection', () => {
  const curve = path({ points: [point(0), point(500), point(500, 500), point(1000, 500)] });
  const result = analyzeStreetcars(network([site('light', 'signal', 500, 75)], [curve]), DATE,
    [obs(400), obs(500, 60, 100)]);
  assert.equal(result.bins[0].category, 'signal_only');
  close(result.bins[0].distance_meters, 200);
});

test('stationary pairs remain zero speed and aggregation preserves duration weights and unique vehicles', () => {
  const result = analyzeStreetcars(network([site('light', 'signal', 500)]), DATE,
    [obs(500), obs(500, 60), obs(620, 90)]);
  const bin = result.bins[0];
  assert.equal(bin.intervals, 2);
  assert.equal(bin.duration_seconds, 90);
  close(bin.distance_meters, 120);
  assert.equal(bin.slow_seconds, 60);
  assert.deepEqual(bin.vehicle_ids, ['car1']);
  close(bin.distance_meters / bin.duration_seconds, 120 / 90);
});

test('feed speed has no effect, actual vehicle timestamps are deduplicated, input order is irrelevant', () => {
  const n = network();
  const a = { ...obs(400), speed: 0 }; const b = { ...obs(700, 60), speed: 0 };
  const result = analyzeStreetcars(n, DATE, [b, a, { ...a }]);
  close(result.bins[0].distance_meters, 300);
  assert.equal(result.bins[0].intervals, 1);
  assert.equal(result.quality[0].excluded.duplicate_points, 1);
  const repeated = analyzeStreetcars(n, DATE, [a, b]);
  assert.deepEqual(repeated.bins, result.bins);
});

test('contradictory duplicate coordinates invalidate adjacent intervals instead of choosing a location', () => {
  const result = analyzeStreetcars(network(), DATE, [obs(400), obs(401), obs(700, 60)]);
  assert.equal(result.bins.length, 0);
  assert.equal(result.quality[0].excluded.conflicting_duplicate, 1);
});

test('missing trip IDs can use geography, but a trip switch cannot become a travel interval', () => {
  const accepted = analyzeStreetcars(network(), DATE, [obs(400, 0, 0, { trip_id: null }), obs(600, 60, 0, { trip_id: null })]);
  assert.equal(accepted.quality[0].accepted_intervals, 1);
  const switched = analyzeStreetcars(network(), DATE, [obs(400), obs(600, 60, 0, { trip_id: 'trip2' })]);
  assert.equal(switched.quality[0].excluded.trip_change, 1);
  assert.equal(switched.bins.length, 0);
});

test('gaps, off-route data, invalid coordinates, impossible speed and unmatched track are excluded', () => {
  for (const [observations, reason] of [
    [[obs(400), obs(600, 120)], 'observation_gap'],
    [[obs(400), obs(600, 60, 0, { off_route: true })], 'off_route'],
    [[obs(400), obs(600, 60, 0, { lat: Number.NaN })], 'invalid_coordinates_or_time'],
    [[obs(400), obs(1900, 1)], 'impossible_speed'],
    [[obs(400, 0, 100), obs(600, 60, 100)], 'unmatched_track'],
  ] as const) {
    const result = analyzeStreetcars(network(), DATE, [...observations]);
    assert.equal(result.bins.length, 0, reason);
    assert.equal(result.quality[0].excluded[reason], 1, reason);
    assert.equal(result.quality[0].candidate_intervals, 1);
  }
});

test('terminals and railway signals are excluded rather than labelled traffic-light delay', () => {
  const terminal = analyzeStreetcars(network(), DATE, [obs(30), obs(200, 60)]);
  assert.equal(terminal.quality[0].excluded.terminal_proximity, 1);
  const rail = analyzeStreetcars(network([site('train-control', 'rail_signal', 500)]), DATE, [obs(400), obs(600, 60)]);
  assert.equal(rail.quality[0].excluded.rail_signal_exposure, 1);
  assert.equal(rail.bins.length, 0);
});

test('movement determines direction, nearby moving pairs can resolve a stationary pair', () => {
  const forward = path();
  const backward = path({ id: 'in', direction: '1', headsign: 'Inbound', points: [...forward.points].reverse() });
  const n = network([site('light', 'signal', 500)], [forward, backward]);
  const result = analyzeStreetcars(n, DATE, [obs(400), obs(500, 60), obs(500, 120)]);
  assert.equal(result.quality[0].accepted_intervals, 2);
  assert.ok(result.bins.every(bin => bin.direction === '0'));
  assert.equal(result.bins[0].slow_seconds, 60);
  const reverse = analyzeStreetcars(n, DATE, [obs(600), obs(400, 60)]);
  assert.equal(reverse.bins[0].direction, '1');
  const stationary = analyzeStreetcars(n, DATE, [obs(500), obs(500, 60)]);
  assert.equal(stationary.quality[0].excluded.ambiguous_direction, 1);
});

test('repeated track in a loop is ambiguous, and equivalent duplicate shapes do not double-count', () => {
  const loop = path({ points: [point(0), point(1000), point(0), point(1000)] });
  const ambiguous = analyzeStreetcars(network([], [loop]), DATE, [obs(400), obs(600, 60)]);
  assert.equal(ambiguous.bins.length, 0);
  assert.equal(ambiguous.quality[0].excluded.ambiguous_track, 1);
  const duplicate = analyzeStreetcars(network([], [path(), path({ id: 'same-track' })]), DATE, [obs(400), obs(600, 60)]);
  assert.equal(duplicate.bins[0].intervals, 1);
});

test('dense adjacent GTFS vertices represent one local projection rather than spurious loop ambiguity', () => {
  const dense = path({ points: Array.from({ length: 1001 }, (_, n) => point(n * 2)) });
  const result = analyzeStreetcars(network([], [dense]), DATE, [obs(400, 0, 15), obs(600, 60, 15)]);
  assert.equal(result.quality[0].accepted_intervals, 1);
  close(result.bins[0].distance_meters, 200);
});

test('different stop applicability on equally plausible shapes is excluded', () => {
  const result = analyzeStreetcars(network([site('stop-a', 'stop', 500)], [path(), path({ id: 'express', stop_ids: [] })]), DATE,
    [obs(400), obs(600, 60)]);
  assert.equal(result.quality[0].excluded.ambiguous_feature_exposure, 1);
  assert.equal(result.bins.length, 0);
});

test('empty analysis has coverage diagnostics but no invented zero-speed baseline', () => {
  const result = analyzeStreetcars(network(), DATE, []);
  assert.deepEqual(result.bins, []);
  assert.deepEqual(result.site_bins, []);
  assert.equal(result.quality[0].raw_points, 0);
  assert.equal(result.quality[0].candidate_intervals, 0);
  assert.ok(METHOD.limitations.some(line => line.includes('signal phases')));
});

test('quality reconciles candidate intervals, accepted intervals and interval exclusions independently of point duplicates', () => {
  const result = analyzeStreetcars(network(), DATE, [obs(400), obs(400), obs(500, 60), obs(600, 180), obs(800, 240)]);
  const q = result.quality[0];
  assert.equal(q.raw_points, 5);
  assert.equal(q.excluded.duplicate_points, 1);
  const excludedIntervals = Object.entries(q.excluded).filter(([reason]) => !reason.endsWith('_points')).reduce((sum, [, count]) => sum + count, 0);
  assert.equal(q.candidate_intervals, q.accepted_intervals + excludedIntervals);
  assert.equal(q.accepted_intervals, 2);
});

test('fixed spatial categories do not change when faster GPS intervals cross more features', () => {
  const n = network([site('light', 'signal', 500), site('stop-a', 'stop', 700)]);
  const fast = analyzePassages(n, DATE, [obs(250), obs(850, 60)]);
  const slow = analyzePassages(n, DATE, Array.from({ length: 7 }, (_, i) => obs(250 + i * 100, i * 60)));
  assert.deepEqual(fast.bins.map(b => [b.category, b.intervals, b.distance_meters]),
    [['signal_only', 1, 200], ['stop_only', 1, 200]]);
  assert.deepEqual(slow.bins.map(b => [b.category, b.intervals, b.distance_meters]),
    fast.bins.map(b => [b.category, b.intervals, b.distance_meters]));
  close(fast.bins[0].duration_seconds, 20);
  close(slow.bins[0].duration_seconds, 120);
  assert.equal(fast.quality[0].accepted_intervals, 1);
  assert.equal(slow.quality[0].accepted_intervals, 6);
});

test('interpolated passage duration retains GPS boundary uncertainty', () => {
  const result = analyzePassages(network(), DATE, [obs(300), obs(500, 60), obs(700, 120)]);
  const bin = result.bins[0];
  assert.equal(bin.intervals, 1);
  close(bin.duration_seconds, 60);
  assert.equal(bin.duration_lower_seconds, 0);
  assert.equal(bin.duration_upper_seconds, 120);
  assert.ok(bin.duration_seconds >= bin.duration_lower_seconds! && bin.duration_seconds <= bin.duration_upper_seconds!);
});

test('a whole window between two pings has a zero lower duration bound, not false precision or selection against fast passages', () => {
  const result = analyzePassages(network(), DATE, [obs(300), obs(700, 60)]);
  const bin = result.bins[0];
  assert.equal(bin.intervals, 1);
  close(bin.duration_seconds, 30);
  assert.equal(bin.duration_lower_seconds, 0);
  assert.equal(bin.duration_upper_seconds, 60);
});

test('exact boundary positions retain arrival uncertainty since the prior outside GPS observation', () => {
  const result = analyzePassages(network(), DATE, [obs(300, -60), obs(400), obs(600, 60)]);
  assert.equal(result.bins[0].duration_seconds, 60);
  assert.equal(result.bins[0].duration_lower_seconds, 0);
  assert.equal(result.bins[0].duration_upper_seconds, 120);
});

test('fully stationary and incomplete trajectories create no invented no-feature baseline passages', () => {
  const stationary = analyzePassages(network(), DATE, Array.from({ length: 10 }, (_, i) => obs(500, i * 60)));
  assert.equal(stationary.quality[0].accepted_intervals, 9);
  assert.deepEqual(stationary.bins, []);
  const partial = analyzePassages(network(), DATE, [obs(400), obs(550, 60)]);
  assert.equal(partial.quality[0].accepted_intervals, 1);
  assert.deepEqual(partial.bins, []);
});

test('a pause during a completed passage contributes travel time and estimated slow-passage time', () => {
  const result = analyzePassages(network(), DATE, [obs(300, -60), obs(400), obs(500, 60), obs(500, 120), obs(500, 180), obs(600, 240)]);
  assert.equal(result.bins[0].duration_seconds, 240);
  assert.equal(result.bins[0].slow_seconds, 240);
  assert.equal(result.bins[0].distance_meters, 200);
  assert.equal(result.bins[0].intervals, 1);
});

test('a passage cannot bridge an excluded observation gap', () => {
  const result = analyzePassages(network(), DATE, [obs(300), obs(450, 60), obs(550, 180), obs(700, 240)]);
  assert.equal(result.quality[0].excluded.observation_gap, 1);
  assert.deepEqual(result.bins, []);
});

test('a pause at a window boundary belongs to one following passage without doubled dwell', () => {
  const result = analyzePassages(network(), DATE, [obs(300, -60), obs(400), obs(600, 60), obs(600, 120), obs(600, 180), obs(700, 240), obs(800, 300)]);
  const bin = result.bins[0];
  assert.equal(bin.intervals, 2);
  assert.equal(bin.distance_meters, 400);
  assert.equal(bin.duration_seconds, 300);
  assert.equal(bin.slow_seconds, 240);
  assert.equal(bin.duration_lower_seconds, 180);
  assert.equal(bin.duration_upper_seconds, 420);
});

test('a fixed combined window occurs once in corridor totals and at each applicable site', () => {
  const result = analyzePassages(network([site('light', 'signal', 500), site('stop-a', 'stop', 550)]), DATE, [obs(300, -60), obs(400), obs(600, 60)]);
  assert.equal(result.bins[0].category, 'both');
  assert.equal(result.bins[0].intervals, 1);
  assert.equal(result.site_bins.length, 2);
  assert.ok(result.site_bins.every(b => b.intervals === 1 && b.distance_meters === 200 && b.duration_seconds === 60));
  assert.equal(METHOD.window_meters, 200);
});

test('an initial observation exactly at the entry boundary is left-censored, while later bracketed windows remain usable', () => {
  const censored = analyzePassages(network(), DATE, [obs(400), obs(600, 60)]);
  assert.deepEqual(censored.bins, []);
  const later = analyzePassages(network(), DATE, [obs(400), obs(600, 60), obs(800, 120)]);
  assert.equal(later.bins[0].intervals, 1);
  assert.equal(later.bins[0].distance_meters, 200);
  assert.equal(later.bins[0].duration_seconds, 60);
  assert.equal(later.bins[0].duration_lower_seconds, 0);
  assert.equal(later.bins[0].duration_upper_seconds, 120);
});

test('a first GPS boundary hit followed by dwell does not erase uncertain arrival time', () => {
  const result = analyzePassages(network(), DATE, [obs(300), obs(400, 60), obs(400, 120), obs(500, 180), obs(600, 240)]);
  const bin = result.bins[0];
  assert.equal(bin.intervals, 1);
  assert.equal(bin.duration_seconds, 180);
  assert.equal(bin.duration_lower_seconds, 120);
  assert.equal(bin.duration_upper_seconds, 240);
  // Actual arrival at 400 m after 10 s and at 600 m after 230 s gives
  // a 220 s passage. Exact-hit bounds of [180,180] would falsely exclude it.
  assert.ok(bin.duration_lower_seconds! <= 220 && bin.duration_upper_seconds! >= 220);
});
