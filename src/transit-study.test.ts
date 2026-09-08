import assert from 'node:assert/strict';
import test from 'node:test';
import type { StudyObservation } from './observation-types';
import type { StudyCatalog, StudyPassage, StudyRowSection, StudySite } from './transit-study-types';
import { analyzeTransitStudy, classifyStudyRow, TRANSIT_STUDY_METHOD } from './transit-study';
import { compareRowCells, rowStudyCells, summarizeRowStudy, summarizeSignalStudy } from './transit-study-summary';
import { LONGITUDE_SCALE, METERS_PER_DEGREE } from './transit-study-geometry';

const DATE = '2026-09-08', AT = Date.parse(`${DATE}T13:00:00Z`) / 1000;
const point = (x: number, y = 0) => ({ lat: 29.95 + y / METERS_PER_DEGREE, lon: -90.1 + x / LONGITUDE_SCALE });
function catalog(sites: StudySite[] = [], mode: 'bus' | 'streetcar' = 'streetcar'): StudyCatalog {
  return { version: 'test-network', generated_at: DATE, schedule_hash: 'schedule', sources: [], limitations: [], row_sections: [],
    paths: [{ id: 'out', route_id: '12', direction_id: '0', mode, name: 'Outbound', points: [point(0), point(2000)], stop_ids: ['stop'] }], sites };
}
function site(id: string, kind: StudySite['kind'], x: number, y = 0): StudySite {
  return { id, kind, name: id, ...point(x, y), route_ids: ['12'], source_ids: [id], verification: kind === 'stop' ? 'gtfs' : 'osm_unverified' };
}
function observation(x: number, seconds: number, patch: Partial<StudyObservation> = {}): StudyObservation {
  return { source: 'lepass', observation_id: `o-${x}-${seconds}`, vehicle_id: 'vehicle', provider_vehicle_id: 'vehicle:provider',
    route_id: '12', trip_id: 'trip', observed_at: AT + seconds, received_at: AT + seconds + 0.1, ...point(x),
    speed_mph: null, off_route: false, location_source: 'provider_gps', timestamp_precision_seconds: 0.001,
    direction_id: '0', pattern_id: 'pattern', mapping_confidence: 'verified', ...patch };
}
function section(from: number, to: number, row_class: 'shared' | 'reserved', patch: Partial<StudyRowSection> = {}): StudyRowSection {
  return { id: `${row_class}:${from}`, path_id: 'out', from_meters: from, to_meters: to, row_class,
    reviewed_at: DATE, valid_from: DATE, valid_to: null, evidence_urls: ['https://www.openstreetmap.org/way/1'], notes: 'Test evidence', ...patch };
}
const near = (actual: number, expected: number, error = 0.01) => assert.ok(Math.abs(actual - expected) < error, `${actual} != ${expected}`);
const stopped = () => [[350, 0], [450, 10], [500, 20], [500, 30], [500, 40], [600, 50], [700, 60]].map(([x, t]) => observation(x, t));

test('same vehicle in two sources produces independent complete passages, never a merged trajectory', () => {
  const rows = [observation(300, 0), observation(700, 20),
    observation(300, 0, { source: 'sse', timestamp_precision_seconds: 60 }), observation(700, 60, { source: 'sse', timestamp_precision_seconds: 60 })];
  const result = analyzeTransitStudy(catalog(), rows);
  assert.equal(result.passages.length, 2);
  assert.equal(new Set(result.passages.map(p => p.id)).size, 2);
  assert.equal(new Set(result.passages.map(p => p.run_id)).size, 2);
  const a = result.passages.find(p => p.source === 'lepass')!, b = result.passages.find(p => p.source === 'sse')!;
  near(a.duration_seconds, 10); near(b.duration_seconds, 30);
  near(a.duration_upper_seconds, 20.001); near(b.duration_upper_seconds, 120);
  assert.deepEqual(analyzeTransitStudy(catalog(), [...rows].reverse()).passages, result.passages);
});

test('ROW needs full reviewed date-applicable coverage; gaps, boundaries and conflicts remain unknown', () => {
  const c = catalog(); c.row_sections = [section(200, 600, 'reserved'), section(600, 1000, 'shared')];
  assert.equal(classifyStudyRow(c, 'out', 400, 600, DATE).row_class, 'reserved');
  assert.equal(classifyStudyRow(c, 'out', 600, 800, DATE).row_class, 'shared');
  assert.equal(classifyStudyRow(c, 'out', 500, 700, DATE).row_class, 'unknown');
  assert.equal(classifyStudyRow(c, 'out', 400, 600, '2026-08-01').row_class, 'unknown');
  c.row_sections = [section(200, 490, 'reserved'), section(500, 900, 'reserved')];
  assert.equal(classifyStudyRow(c, 'out', 400, 600, DATE).row_class, 'unknown');
  c.row_sections = [section(200, 900, 'reserved', { evidence_urls: [] })];
  assert.equal(classifyStudyRow(c, 'out', 400, 600, DATE).row_class, 'unknown');
});

test('null, estimated, off-route, stale and unverified observations break rather than bridge trajectories', () => {
  for (const patch of [{ lat: null }, { observed_at: null }, { location_source: 'estimated' as const }, { off_route: true },
    { observed_at: AT - 500 }, { mapping_confidence: 'candidate' as const }]) {
    const rows = [observation(300, 0), observation(500, 30, patch), observation(700, 60)];
    const result = analyzeTransitStudy(catalog(), rows);
    assert.equal(result.passages.length, 0, JSON.stringify(patch));
    assert.ok(Object.keys(result.quality[0].excluded).length);
  }
});

test('provider sample collisions are excluded for Le Pass; a repeated poll creates no new GPS sample', () => {
  const a = observation(500, 20), repeat = { ...a, observation_id: 'repeat', received_at: a.received_at + 5 };
  const rows = [observation(300, 0), a, repeat, observation(700, 60)];
  const result = analyzeTransitStudy(catalog(), rows);
  assert.equal(result.passages.length, 1); assert.equal(result.quality[0].excluded.repeated_sample, 1);
  const bad = analyzeTransitStudy(catalog(), [...rows, { ...repeat, ...point(550), observation_id: 'collision' }]);
  assert.equal(bad.passages.length, 0); assert.ok(bad.quality[0].excluded.conflicting_sample_timestamp > 0);
});

test('delayed Le Pass query replays cannot duplicate passages or encounters', () => {
  const c = catalog([site('light', 'signal', 500)]), rows = stopped();
  const baseline = analyzeTransitStudy(c, rows);
  const replay = rows.map(o => ({ ...o, observation_id: `other-query:${o.observation_id}`, received_at: o.received_at + 70 }));
  const result = analyzeTransitStudy(c, [...rows, ...replay]);
  assert.deepEqual(result.passages, baseline.passages);
  assert.deepEqual(result.encounters, baseline.encounters);
  assert.equal(result.quality[0].excluded.repeated_sample, rows.length);
  assert.equal(result.quality[0].usable_observations, rows.length);
});

test('Le Pass matches and brackets waits on provider chronology despite late query receipts', () => {
  const c = catalog([site('light', 'signal', 500)]), rows = stopped();
  const late = rows.map((o, i) => ({ ...o, received_at: o.received_at + (i === 0 ? 90 : i % 2 ? 70 : 0) }));
  const baseline = analyzeTransitStudy(c, rows), result = analyzeTransitStudy(c, late);
  assert.equal(result.passages.length, baseline.passages.length);
  near(result.passages[0].duration_seconds, baseline.passages[0].duration_seconds);
  assert.equal(result.encounters.length, 1);
  assert.equal(result.encounters[0].wait_status, 'detected');
  near(result.encounters[0].wait_seconds, 20);
  assert.deepEqual(analyzeTransitStudy(c, [...late].reverse()), result);
});

test('conflicting arrival directions and patterns invalidate the shared Le Pass GPS sample', () => {
  for (const patch of [{ direction_id: '1' }, { pattern_id: 'other-pattern' }]) {
    const c = catalog(); c.paths.push({ ...c.paths[0], id: 'in', direction_id: '1', points: [...c.paths[0].points].reverse() });
    const middle = observation(500, 20);
    const result = analyzeTransitStudy(c, [observation(300, 0), middle,
      { ...middle, ...patch, observation_id: 'conflicting-query', received_at: middle.received_at + 30 }, observation(700, 60)]);
    assert.equal(result.passages.length, 0);
    assert.equal(result.quality[0].excluded.conflicting_sample_timestamp, 2);
  }
});

test('trip and direction changes split same-vehicle trajectories before a complete window can be invented', () => {
  for (const patch of [{ trip_id: 'new-trip' }, { direction_id: '1' }]) {
    const c = catalog(); c.paths.push({ ...c.paths[0], id: 'in', direction_id: '1', points: [...c.paths[0].points].reverse() });
    const result = analyzeTransitStudy(c, [observation(300, 0), observation(500, 20, patch), observation(700, 40, patch)]);
    assert.equal(result.passages.length, 0);
    assert.ok(result.quality[0].excluded.trajectory_break > 0);
  }
});

test('equidistant signal heads cannot both receive the same stationary episode', () => {
  const c = catalog([site('light-a', 'signal', 500, 5), site('light-b', 'signal', 500, -5)]);
  const result = analyzeTransitStudy(c, stopped());
  assert.equal(result.encounters.length, 2);
  assert.ok(result.encounters.every(e => e.wait_events === 0));
  assert.equal(result.quality[0].excluded.ambiguous_wait_site, 1);
});

test('SSE within-minute position updates keep the earliest received passage sample and explicit timing uncertainty', () => {
  const rows = [[300, 0], [500, 20], [700, 60]].map(([x, t]) => observation(x, t, {
    source: 'sse', observed_at: AT + Math.floor(t / 60) * 60, timestamp_precision_seconds: 60 }));
  const result = analyzeTransitStudy(catalog(), rows);
  assert.equal(result.passages.length, 1); near(result.passages[0].duration_seconds, 30);
  assert.equal(result.quality[0].excluded.positions_within_reported_minute, 1);
  assert.equal(result.passages[0].duration_lower_seconds, 0); near(result.passages[0].duration_upper_seconds, 120);
});

test('directional signal encounters include an isolated detected wait and a no-wait traversal', () => {
  const c = catalog([site('light', 'signal', 500)]);
  const moving = [[350, 0], [400, 5], [450, 10], [500, 15], [550, 20], [600, 25], [650, 30]].map(([x, t]) => observation(x, t, { vehicle_id: 'moving' }));
  const result = analyzeTransitStudy(c, [...stopped(), ...moving]);
  assert.equal(result.encounters.length, 2);
  assert.ok(result.encounters.every(e => e.direction_id === '0' && e.approach_id === 'out:light'));
  assert.deepEqual(result.encounters.map(e => e.wait_status).sort(), ['detected', 'not_detected']);
  const summary = summarizeSignalStudy(c, result.encounters).signals[0];
  assert.equal(summary.encounters, 2); assert.equal(summary.detected_wait_probability, 0.5);
  near(summary.mean_detected_wait_seconds!, 20); near(summary.detected_wait_seconds_per_encounter!, 10);
  assert.deepEqual(summary.recovery_seconds_per_encounter.map(s => s.seconds), [2.5, 5, 7.5]);
});

test('bus passages use GTFS road geometry and mixed boarding waits have no recovery scenario', () => {
  const c = catalog([site('light', 'signal', 500), site('stop', 'stop', 510)], 'bus');
  const result = analyzeTransitStudy(c, stopped());
  assert.equal(result.encounters[0].mode, 'bus'); assert.equal(result.encounters[0].context, 'both');
  assert.equal(result.encounters[0].wait_status, 'detected');
  assert.ok(summarizeSignalStudy(c, result.encounters).signals[0].recovery_seconds_per_encounter.every(s => s.seconds === null));
});

test('cached Le Pass positions and positive speed contradictions cannot manufacture waits', () => {
  const c = catalog([site('light', 'signal', 500)]);
  const cached = stopped().map(o => o.lat === point(500).lat && o.lon === point(500).lon ? { ...o, observed_at: AT + 20 } : o);
  assert.ok(analyzeTransitStudy(c, cached).encounters.every(e => e.wait_events === 0));
  const movingSpeed = stopped().map(o => ({ ...o, speed_mph: 8 }));
  assert.ok(analyzeTransitStudy(c, movingSpeed).encounters.every(e => e.wait_events === 0));
});

test('SSE receipt waits require explicit zero speeds, even when within-minute coordinates repeat', () => {
  const c = catalog([site('light', 'signal', 500)]);
  const rows = [...stopped(), observation(900, 120)].map(o => ({ ...o, source: 'sse' as const,
    observed_at: AT + Math.floor((o.observed_at! - AT) / 60) * 60, speed_mph: 0, timestamp_precision_seconds: 60 }));
  const result = analyzeTransitStudy(c, rows);
  assert.equal(result.encounters[0].wait_events, 1); assert.equal(result.encounters[0].wait_clock, 'collector_receipt');
  near(result.encounters[0].wait_seconds, 20);
  const missing = analyzeTransitStudy(c, rows.map(o => ({ ...o, speed_mph: null })));
  assert.ok(missing.encounters.every(e => e.wait_events === 0));
});

test('sparse completed encounters remain in the denominator with insufficient-sampling status', () => {
  const c = catalog([site('light', 'signal', 500)]), result = analyzeTransitStudy(c, [observation(300, 0), observation(700, 60)]);
  assert.equal(result.encounters.length, 1); assert.equal(result.encounters[0].wait_status, 'insufficient_sampling');
  const summary = summarizeSignalStudy(c, result.encounters);
  assert.equal(summary.signals[0].encounters, 1); assert.equal(summary.signals[0].evaluable_encounters, 0);
  assert.equal(summary.status, 'collecting');
});

test('feature buffer overlap does not duplicate one signal encounter in adjacent windows', () => {
  const c = catalog([site('light', 'signal', 590)]), result = analyzeTransitStudy(c, [observation(250, 0), observation(850, 60)]);
  assert.equal(result.passages.length, 2); assert.equal(result.encounters.length, 1);
  assert.equal(new Set(result.passages.map(p => `${p.source}:${p.run_id}:${p.window_id}`)).size, result.passages.length);
});

test('feature context uses actual circular exposure rather than a square around the projected anchor', () => {
  const result = analyzeTransitStudy(catalog([site('offset-light', 'signal', 620, 39)]), [observation(300, 0), observation(700, 60)]);
  assert.equal(result.passages[0].context, 'neither');
  assert.equal(result.encounters.length, 0);
});

test('rail controls, terminal proximity, gaps and left-censored entries do not produce invented passages', () => {
  assert.equal(analyzeTransitStudy(catalog([site('rail', 'rail_signal', 500)]), stopped()).passages.length, 0);
  assert.equal(analyzeTransitStudy(catalog(), [observation(400, 0), observation(600, 60)]).passages.length, 0);
  assert.equal(analyzeTransitStudy(catalog(), [observation(300, 0), observation(700, 180)]).passages.length, 0);
});

function passage(date: string, row_class: 'reserved' | 'shared', n: number, seconds: number, patch: Partial<StudyPassage> = {}): StudyPassage {
  return { id: `${date}:${row_class}:${n}`, date, source: 'sse', mode: 'streetcar', route_id: '12', direction_id: '0', path_id: 'out',
    hour: 8, day_type: 'weekday', time_band: 2, run_id: `${date}:${n}`, vehicle_id: 'v', trip_id: 't', window_id: row_class,
    from_meters: 200, to_meters: 400, distance_meters: 200, entry_at: AT, exit_at: AT + seconds,
    duration_seconds: seconds, duration_lower_seconds: seconds - 5, duration_upper_seconds: seconds + 5,
    timestamp_precision_seconds: 60, row_class, row_section_ids: [], context: 'neither', signal_ids: [], stop_ids: [],
    network_version: 'test-network', method: TRANSIT_STUDY_METHOD, ...patch };
}

test('ROW comparison matches common dates/exposures and uses equal date weights rather than composition-biased ping means', () => {
  const rows: StudyPassage[] = [];
  for (const [date, r, s, n] of [['2026-09-08', 20, 40, 30], ['2026-09-09', 40, 60, 3], ['2026-09-10', 60, 80, 3], ['2026-09-11', 40, 60, 3], ['2026-09-14', 40, 60, 3], ['2026-09-15', 40, 60, 3], ['2026-09-16', 40, 60, 3]] as const)
    for (let i = 0; i < n; i++) rows.push(passage(date, 'reserved', i, r), passage(date, 'shared', i, s));
  rows.push(passage('2026-09-17', 'shared', 1, 900)); // Unmatched date must not inflate shared cost.
  rows.push(passage('2026-09-08', 'shared', 2, 900, { id: 'other-context', context: 'both', signal_ids: ['signal'], stop_ids: ['stop'] }));
  const summary = summarizeRowStudy(catalog(), rows), compared = summary.comparisons.find(c => c.context === 'neither')!;
  assert.equal(compared.status, 'ready'); assert.equal(compared.matched_dates, 7);
  near(compared.reserved_seconds_per_km!, 200); near(compared.shared_seconds_per_km!, 300);
  near(compared.shared_extra_seconds_per_km!, 100);
  near(compared.shared_extra_lower_seconds_per_km!, 50); near(compared.shared_extra_upper_seconds_per_km!, 150);
  assert.equal(compared.shared_passages, 48);
  near(compared.shared_extra_ci_lower_seconds_per_km!, 100); near(compared.shared_extra_ci_upper_seconds_per_km!, 100);
  assert.equal(summary.comparisons.find(c => c.context === 'both')!.status, 'insufficient_data');
  assert.deepEqual(compareRowCells(rowStudyCells(catalog(), [...rows, rows[0]])), summary.comparisons);
});

test('source, direction, filters, network version and unknown ROW cannot leak into a ready comparison', () => {
  const rows = Array.from({ length: 25 }, (_, i) => passage('2026-09-08', 'reserved', i, 20));
  rows.push(...rows.map(p => ({ ...p, id: `l:${p.id}`, source: 'lepass' as const, row_class: 'shared' as const })));
  const summary = summarizeRowStudy(catalog(), rows);
  assert.ok(summary.comparisons.every(c => c.status === 'insufficient_data'));
  assert.equal(summarizeRowStudy(catalog(), rows, [], { source: 'sse' }).cells.length, 1);
  assert.equal(summarizeRowStudy(catalog(), rows, [], { direction_id: '1' }).cells.length, 0);
  assert.equal(summarizeRowStudy(catalog(), rows, [], { hour_from: 12 }).cells.length, 0);
  assert.equal(summarizeRowStudy(catalog(), rows.map(p => ({ ...p, network_version: 'old' }))).cells.length, 0);
  const unknown = summarizeRowStudy(catalog(), rows.map(p => ({ ...p, row_class: 'unknown' as const })));
  assert.equal(unknown.coverage.unknown_passages, 50); assert.equal(unknown.comparisons.length, 0);
});
