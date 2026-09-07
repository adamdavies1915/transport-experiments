import { test } from 'node:test';
import assert from 'node:assert/strict';
import { renderToStaticMarkup } from 'react-dom/server';
import PriorityPanel from './PriorityPanel';
import { PRIORITY_METHOD, type PriorityData, type PriorityProfile, type PriorityWindow } from './priority-data';

const window: PriorityWindow = { id: 'w1', path_id: 'p1', route: '12', direction: '0', from_meters: 0, to_meters: 200,
  category: 'signal_only', signal_ids: ['s1'], stop_ids: [], name: 'Jackson signal', passages: 30, days: 5, eligible: true,
  observed_seconds: 90, baseline_seconds: 30, extra_seconds: 60, extra_p10_seconds: 70, extra_p30_seconds: 50 };
const profile: PriorityProfile = { path_id: 'p1', route: '12', direction: '0', headsign: 'Uptown', route_meters: 2000,
  covered_meters: 800, coverage_pct: 40, eligible_windows: 4, total_windows_observed: 7, passages: 120,
  observed_seconds: 600, baseline_seconds: 390, signal_only_extra_seconds: 60, mixed_extra_seconds: 120, other_extra_seconds: 30,
  windows: [window, { ...window, id: 'w2', from_meters: 200, to_meters: 400, category: 'both', name: 'Napoleon signal and stop', stop_ids: ['stop1'],
    observed_seconds: 180, baseline_seconds: 60, extra_seconds: 120, extra_p10_seconds: 130, extra_p30_seconds: 100 }],
};
function fixture(): PriorityData {
  return { status: 'ready', profiles: [profile], available_from: '2026-08-01', available_to: '2026-09-07', selected_from: '2026-09-01', selected_to: '2026-09-07', updated_at: '2026-09-07',
    method: PRIORITY_METHOD, network: { version: 'test', generated_at: '2026-09-07', schedule_hash: 's',
      corridors: [{ id: 'st_charles', name: 'St. Charles', routes: ['12'] }],
      paths: [{ id: 'p1', corridor: 'st_charles', route: '12', direction: '0', headsign: 'Uptown', points: [{ lat: 29.94, lon: -90.09 }, { lat: 29.95, lon: -90.10 }], stop_ids: ['stop1'] }],
      sites: [{ id: 's1', corridor: 'st_charles', kind: 'signal', lat: 29.944, lon: -90.094, name: 'Jackson signal', routes: ['12'], source_ids: ['osm/node/1'], verification: 'osm_unverified' }],
      sources: [], mapillary_status: 'Nearby detections corroborate mapped locations; streetcar control is unverified.',
    }, waits: { status: 'collecting', snapshots: 0, from: null, to: null, events: 0, signal_only_seconds: 0, mixed_seconds: 0, stop_only_seconds: 0, sites: [], clock: 'collector_receipt' },
  };
}

function metric(html: string, label: string) {
  const rest = html.split(`>${label}</p>`)[1];
  assert.ok(rest, `Missing metric ${label}`);
  return rest.split('</div>')[0];
}

test('leads with covered one-way travel and labels savings as an editable scenario without extrapolation', () => {
  const html = renderToStaticMarkup(<PriorityPanel data={fixture()} />);
  assert.match(html, /Covered portion of a one-way journey/);
  assert.match(html, /40% of the route covered/);
  assert.match(metric(html, 'Observed travel time'), /10\.0 min/);
  assert.match(metric(html, 'With your priority assumptions'), /9\.3 min/);
  assert.match(metric(html, 'Illustrative time saved'), /0\.8 min/); // (60 + 120) * 50% * 50% = 45 s.
  assert.match(html, /3\.0 min of extra time near signals/);
  assert.match(html, /starting 50% \/ 50% values are illustrative assumptions, not measured effects/);
  assert.match(html, /No saving is extrapolated onto unobserved parts/);
  assert.doesNotMatch(html, /25\.0 min/); // 40% track coverage is not extrapolated to full-route time.
});

test('excluding passenger-stop overlap changes the scenario without removing mixed evidence', () => {
  const html = renderToStaticMarkup(<PriorityPanel data={fixture()} initialScenario={{ includeOverlap: false }} />);
  assert.match(html, /1\.0 min of extra time near signals/);
  assert.match(metric(html, 'Illustrative time saved'), /0\.3 min/); // 60 * 25% = 15 s.
  assert.match(html, /2\.0 min of mixed extra time/);
  assert.match(html, /Signal \+ passenger stop/);
  assert.match(html, /boarding and signal effects cannot be separated/);
});

test('zero assumed recovery produces zero saving, while missing estimates remain unavailable', () => {
  const zero = renderToStaticMarkup(<PriorityPanel data={fixture()} initialScenario={{ recovery: 0 }} />);
  assert.match(metric(zero, 'Illustrative time saved'), /0\.0 min/);
  assert.match(metric(zero, 'With your priority assumptions'), /10\.0 min/);
  const data = fixture();
  data.profiles = [{ ...profile, observed_seconds: null, baseline_seconds: null, signal_only_extra_seconds: null, mixed_extra_seconds: null,
    other_extra_seconds: null, covered_meters: 0, coverage_pct: 0, eligible_windows: 0, windows: [] }];
  const missing = renderToStaticMarkup(<PriorityPanel data={data} />);
  assert.match(missing, /Not enough repeated passages/);
  assert.match(missing, /Not enough repeated observations to quantify extra time near signals/);
  assert.doesNotMatch(missing, /Illustrative time saved<\/p>/);
  data.profiles = [{ ...profile, mixed_extra_seconds: null }];
  const incomplete = renderToStaticMarkup(<PriorityPanel data={data} />);
  assert.match(metric(incomplete, 'Illustrative time saved'), /Unavailable/);
  assert.doesNotMatch(incomplete, /aria-label="Observed and illustrative travel-time comparison"/);
});

test('route profiles stay separate by direction and branch', () => {
  const data = fixture();
  data.profiles.push({ ...profile, path_id: 'p2', direction: '1', headsign: 'Downtown', observed_seconds: 1200, signal_only_extra_seconds: 120, mixed_extra_seconds: 0 });
  const html = renderToStaticMarkup(<PriorityPanel data={data} initialProfileId="p2" />);
  assert.match(metric(html, 'Observed travel time'), /20\.0 min/);
  assert.doesNotMatch(metric(html, 'Observed travel time'), /30\.0 min/);
  assert.match(metric(html, 'Illustrative time saved'), /0\.5 min/);
  assert.match(html, /Route 12 · Downtown/);
});

test('ranks distinct qualifying windows and keeps small or stop-only samples out of signal ranking', () => {
  const data = fixture();
  data.profiles = [{ ...profile, windows: [...profile.windows,
    { ...window, id: 'w3', name: 'Insufficient sample', eligible: false, extra_seconds: 9999 },
    { ...window, id: 'w4', name: 'Stop without signal', category: 'stop_only', signal_ids: [], stop_ids: ['stop1'], extra_seconds: 9999 },
  ] }];
  const html = renderToStaticMarkup(<PriorityPanel data={data} />);
  const table = html.split('<tbody>')[1].split('</tbody>')[0];
  assert.ok(table.indexOf('Napoleon signal and stop') < table.indexOf('Jackson signal'));
  assert.doesNotMatch(table, /Insufficient sample|Stop without signal/);
  assert.equal((table.match(/<tr/g) ?? []).length, 2);
  assert.match(html, /Each track window counts once/);
  assert.match(table, /Alternative P10 \/ P30 faster-passage benchmarks give 100 s to 130 s/);
});

test('stationary wait totals remain separate from per-journey scenario savings', () => {
  const data = fixture();
  data.waits = { ...data.waits, status: 'ready', events: 3, snapshots: 50, signal_only_seconds: 120, mixed_seconds: 180, stop_only_seconds: 60 };
  data.waits.sites = [{ site_id: 's1', name: 'Jackson candidate wait', context: 'signal_only', events: 2, total_seconds: 120, mean_seconds: 60 },
    { site_id: 's2', name: 'Napoleon candidate wait', context: 'both', events: 1, total_seconds: 180, mean_seconds: 180 }];
  const html = renderToStaticMarkup(<PriorityPanel data={data} />);
  assert.match(html, /Totals across recorded vehicles, not minutes per journey/);
  assert.match(html, /nearby does not prove the light was red/);
  assert.match(html, /collector receipt times/);
  assert.match(metric(html, 'Illustrative time saved'), /0\.8 min/);
  assert.match(html, /3 episodes from 50 snapshots/);
  assert.match(html, /All recorded vehicles on St\. Charles, across all directions/);
  assert.match(html, /not specific to the one-way route selected above/);
  assert.match(html, /Mean per detected wait/);
  assert.ok(html.indexOf('Napoleon candidate wait') < html.indexOf('Jackson candidate wait'));
  assert.match(html, /<section[^>]*aria-labelledby="candidate-waits-heading"/);
});

test('default screen keeps diagnostics collapsed and unavailable stationary data honest', () => {
  const html = renderToStaticMarkup(<PriorityPanel data={fixture()} />);
  assert.match(html, /No qualifying stationary episodes are available yet/);
  assert.match(html, /Show advanced speed and timing diagnostics/);
  assert.doesNotMatch(html, /Mean travel-time bounds|Loading streetcar observations/);
  assert.match(html, /Show stops and signals on the map/);
  const loading = renderToStaticMarkup(<PriorityPanel />);
  assert.match(loading, /Loading signal-priority estimates/);
  assert.doesNotMatch(loading, /Loading the transit overview/);
});
