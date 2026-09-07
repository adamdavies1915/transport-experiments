import assert from 'node:assert/strict';
import test from 'node:test';
import { detectStreetcarWaits, type WaitSnapshot } from './streetcar-waits';
import type { GeoPoint, StreetcarNetwork, StreetcarSite } from '../dashboard/src/streetcar-data';

const AT = Date.parse('2026-09-07T13:00:00Z') / 1000;
const DEGREE_METERS = Math.PI * 6371000 / 180;
const LONGITUDE_SCALE = DEGREE_METERS * Math.cos(29.95 * Math.PI / 180);
const point = (x: number, y = 0): GeoPoint => ({ lat: 29.95 + y / DEGREE_METERS, lon: -90.1 + x / LONGITUDE_SCALE });
const site = (id: string, kind: StreetcarSite['kind'], x: number, y = 0): StreetcarSite => ({
  id, kind, corridor: 'st_charles', name: id, routes: ['12'], source_ids: [id],
  verification: kind === 'stop' ? 'gtfs' : 'osm_unverified', ...point(x, y),
});
const network = (sites: StreetcarSite[] = [site('light', 'signal', 500)]): StreetcarNetwork => ({
  version: 'test', generated_at: '2026-09-07', schedule_hash: 'test',
  corridors: [{ id: 'st_charles', name: 'St. Charles', routes: ['12'] }],
  paths: [{ id: 'out', corridor: 'st_charles', route: '12', direction: '0', headsign: 'Outbound',
    points: [point(0), point(2000)], stop_ids: ['stop'] }], sites, sources: [], mapillary_status: 'Not checked',
});
function snapshot(x: number, seconds: number, overrides: Partial<WaitSnapshot> = {}): WaitSnapshot {
  return { vid: 'car1', route: '12', trip_id: 'trip1', received_at: AT + seconds,
    provider_at: AT + Math.floor(seconds / 60) * 60, ...point(x), off_route: false, speed: 0, ...overrides };
}
const episode = () => [snapshot(450, 0), snapshot(500, 10), snapshot(501, 20), snapshot(500, 30), snapshot(550, 40)];

test('fresh repeated zero-speed positions bracketed by movement produce one candidate on receipt time', () => {
  const rows = episode().map((row, n) => ({ ...row, speed: n === 0 || n === 4 ? 13 : 0 }));
  const events = detectStreetcarWaits(network(), rows);
  assert.equal(events.length, 1);
  assert.equal(events[0].context, 'signal_only');
  assert.equal(events[0].site_id, 'light');
  assert.equal(events[0].started_at, AT + 10);
  assert.equal(events[0].ended_at, AT + 30);
  assert.equal(events[0].duration_seconds, 20);
});

test('cached positions with positive or unknown speed never become zero-speed waits', () => {
  for (const speed of [13, 0.1, null, NaN, -1]) {
    assert.deepEqual(detectStreetcarWaits(network(), episode().map(row => ({ ...row, speed }))), []);
  }
  // An unnormalized legacy field must not stand in for the required known speed.
  const legacy = episode().map(row => ({ ...row, speed: null, spd: 0 }));
  assert.deepEqual(detectStreetcarWaits(network(), legacy), []);
});

test('any contradictory or missing speed vetoes the whole stable cluster, including longer zero-speed subsets', () => {
  const rows = [snapshot(450, 0), ...Array.from({ length: 7 }, (_, n) => snapshot(500, 10 + n * 10)), snapshot(550, 80)];
  for (const index of [1, 4, 7]) for (const speed of [6, null]) {
    assert.deepEqual(detectStreetcarWaits(network(), rows.map((row, n) => n === index ? { ...row, speed } : row)), []);
  }
});

test('mixed contexts assign one episode to the nearest road signal without double counting stops or other signals', () => {
  const events = detectStreetcarWaits(network([site('stop', 'stop', 500), site('far-light', 'signal', 525), site('near-light', 'signal', 510)]), episode());
  assert.equal(events.length, 1);
  assert.equal(events[0].site_id, 'near-light');
  assert.equal(events[0].context, 'both');
  const stops = detectStreetcarWaits(network([site('stop', 'stop', 500)]), episode());
  assert.equal(stops[0].context, 'stop_only');
});

test('movement through a signal, brief pauses and chained moving points do not become waits', () => {
  for (const rows of [
    [snapshot(450, 0), snapshot(475, 10), snapshot(500, 20), snapshot(525, 30), snapshot(550, 40)],
    [snapshot(450, 0), snapshot(500, 10), snapshot(500, 20), snapshot(550, 30)],
    Array.from({ length: 15 }, (_, n) => snapshot(450 + n * 10, n * 10)),
  ]) assert.deepEqual(detectStreetcarWaits(network(), rows), []);
});

test('left- or right-censored stable positions are not complete episodes', () => {
  assert.deepEqual(detectStreetcarWaits(network(), episode().slice(1)), []);
  assert.deepEqual(detectStreetcarWaits(network(), episode().slice(0, -1)), []);
  assert.deepEqual(detectStreetcarWaits(network(), Array.from({ length: 10 }, (_, n) => snapshot(500, n * 10))), []);
});

test('receipt gaps, trip switches, off-route data, missing timestamps and stale provider data split episodes', () => {
  const gap = episode().map((row, n) => n >= 2 ? { ...row, received_at: row.received_at + 30 } : row);
  const trip = episode().map((row, n) => n >= 2 ? { ...row, trip_id: 'trip2' } : row);
  const offRoute = episode().map((row, n) => n === 2 ? { ...row, off_route: true } : row);
  const missing = episode().map(row => ({ ...row, provider_at: null }));
  const stale = episode().map(row => ({ ...row, provider_at: AT - 180 }));
  const future = episode().map(row => ({ ...row, provider_at: AT + 600 }));
  for (const rows of [gap, trip, offRoute, missing, stale, future]) assert.deepEqual(detectStreetcarWaits(network(), rows), []);
});

test('a frozen provider timestamp beyond the freshness limit cannot extend a cached position into a wait', () => {
  const rows = [snapshot(450, 0), ...Array.from({ length: 14 }, (_, n) => snapshot(500, (n + 1) * 10, { provider_at: AT })), snapshot(550, 150)];
  assert.deepEqual(detectStreetcarWaits(network(), rows), []);
});

test('railway controls, terminals, off-track positions and places with no mapped applicable site are excluded', () => {
  assert.deepEqual(detectStreetcarWaits(network([site('rail', 'rail_signal', 500), site('light', 'signal', 505)]), episode()), []);
  const terminalRows = episode().map(row => ({ ...row, lon: row.lon - 450 / LONGITUDE_SCALE }));
  assert.deepEqual(detectStreetcarWaits(network([site('light', 'signal', 50)]), terminalRows), []);
  const offTrack = episode().map(row => ({ ...row, lat: row.lat + 60 / DEGREE_METERS }));
  assert.deepEqual(detectStreetcarWaits(network([site('light', 'signal', 500, 60)]), offTrack), []);
  assert.deepEqual(detectStreetcarWaits(network([]), episode()), []);
  assert.deepEqual(detectStreetcarWaits(network([{ ...site('other-route', 'signal', 500), routes: ['47'] }]), episode()), []);
});

test('duplicate receipts do not inflate an episode, while contradictory receipts invalidate it', () => {
  const rows = episode();
  const expected = detectStreetcarWaits(network(), rows);
  assert.deepEqual(detectStreetcarWaits(network(), [...rows, { ...rows[2] }]), expected);
  assert.deepEqual(detectStreetcarWaits(network(), [...rows, { ...rows[2], ...point(650) }]), []);
  assert.deepEqual(detectStreetcarWaits(network(), [...rows, { ...rows[2], speed: 13 }]), []);
});

test('event IDs and ordering are deterministic and separate distinct vehicles and separate stops', () => {
  const n = network();
  const rows = [...episode(), ...episode().map(row => ({ ...row, vid: 'car2' })),
    ...episode().map(row => ({ ...row, received_at: row.received_at + 120, provider_at: row.provider_at! + 120 }))];
  const result = detectStreetcarWaits(n, rows);
  assert.equal(result.length, 3);
  assert.equal(new Set(result.map(event => event.id)).size, 3);
  assert.deepEqual(detectStreetcarWaits(n, [...rows].reverse()), result);
});
