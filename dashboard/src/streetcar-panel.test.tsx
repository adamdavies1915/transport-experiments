import { test } from 'node:test';
import assert from 'node:assert/strict';
import { renderToStaticMarkup } from 'react-dom/server';
import StreetcarPanel from './StreetcarPanel';
import type { StreetcarBin, StreetcarData } from './streetcar-data';

const row: StreetcarBin = { date: '2026-09-01', corridor: 'st_charles', route: '12', direction: '0',
  hour: 8, day_type: 'weekday', category: 'signal_only', intervals: 1,
  duration_seconds: 30, duration_lower_seconds: 0, duration_upper_seconds: 60,
  distance_meters: 200, slow_seconds: 0, vehicle_ids: ['a'] };
function fixture(bins: StreetcarBin[] = []): StreetcarData {
  return {
    status: 'ready', available_from: '2026-08-01', available_to: '2026-09-07', selected_from: '2026-09-01', selected_to: '2026-09-07',
    network: { version: 'test', generated_at: '2026-09-07', schedule_hash: 'schedule',
      corridors: [{ id: 'st_charles', name: 'St. Charles', routes: ['12'] }, { id: 'canal', name: 'Canal', routes: ['47', '48'] }, { id: 'rampart', name: 'Rampart', routes: ['49'] }],
      paths: [{ id: 'track-0', corridor: 'st_charles', route: '12', direction: '0', headsign: 'Uptown', points: [{ lat: 29.93, lon: -90.11 }, { lat: 29.95, lon: -90.08 }], stop_ids: ['stop-1'] },
        { id: 'track-1', corridor: 'st_charles', route: '12', direction: '1', headsign: 'Downtown', points: [{ lat: 29.95, lon: -90.08 }, { lat: 29.93, lon: -90.11 }], stop_ids: ['stop-1'] }],
      sites: [{ id: 'signal-1', corridor: 'st_charles', kind: 'signal', name: 'Jackson signal', routes: ['12'], source_ids: ['osm:node:1'], verification: 'osm_unverified', lat: 29.94, lon: -90.095 },
        { id: 'stop-1', corridor: 'st_charles', kind: 'stop', name: 'Jackson passenger stop', routes: ['12'], source_ids: ['gtfs:1'], verification: 'gtfs', lat: 29.9401, lon: -90.0951 }],
      sources: [{ name: 'OpenStreetMap', url: 'https://www.openstreetmap.org/copyright', attribution: '© OpenStreetMap contributors', fetched_at: '2026-09-07' }],
      mapillary_status: 'Sample vehicle-light detections corroborate two mapped signal locations; remaining sites unverified.',
    }, bins, site_bins: [], quality: [{ date: '2026-09-01', corridor: 'st_charles', raw_points: 400, candidate_intervals: 390, accepted_intervals: 300, excluded: { gap_too_long: 45, outside_track: 45 } }],
    updated_at: '2026-09-07 14:00:00', method: { name: 'test', window_meters: 200, timestamp_quantization_seconds: 60, max_gap_seconds: 120, feature_radius_meters: 40,
      track_tolerance_meters: 50, terminal_radius_meters: 100, slow_mph: 3, limitations: [] },
  };
}
function card(html: string, label: string) {
  const content = html.split(`>${label}</h3>`)[1];
  assert.ok(content, `Missing category card: ${label}`);
  return content.split('</div>')[0];
}

test('averages GPS travel-time bounds per completed passage and estimates speed from total distance and time', () => {
  const data = fixture([row, { ...row, intervals: 3, duration_seconds: 180, duration_lower_seconds: 120, duration_upper_seconds: 240,
    distance_meters: 600, vehicle_ids: ['a', 'b'] }]);
  const html = renderToStaticMarkup(<StreetcarPanel data={data} />);
  const signal = card(html, 'Signal only');
  assert.match(signal, /30\.0–75\.0 s/); // Sum lower/upper durations divided by four passages.
  assert.match(signal, /Estimated speed: 8\.5 mph/); // 800 metres / 210 seconds, not an average of bin speeds.
  assert.match(signal, /Speed bounds: 6\.0–14\.9 mph/);
  assert.match(signal, /4 completed passages/);
  assert.match(signal, /2 vehicles/);
  assert.doesNotMatch(signal, /11\.2 mph/);
  assert.match(html, /per 200 m passage/);
});

test('keeps all four fixed-window exposure classes exclusive and explains overlapping timing bounds', () => {
  const data = fixture([row, { ...row, category: 'both', intervals: 2, distance_meters: 400,
    duration_seconds: 120, duration_lower_seconds: 60, duration_upper_seconds: 180 },
  { ...row, category: 'neither', intervals: 3, distance_meters: 600, duration_seconds: 60, duration_upper_seconds: 120 }]);
  const html = renderToStaticMarkup(<StreetcarPanel data={data} />);
  assert.match(card(html, 'Signal only'), /1 completed passages/);
  assert.match(card(html, 'Stop + signal'), /2 completed passages/);
  assert.match(card(html, 'Stop + signal'), /30\.0–90\.0 s/);
  assert.match(card(html, 'Stop only'), /Unavailable/);
  assert.match(card(html, 'Neither nearby'), /3 completed passages/);
  assert.match(html, /Stop \+ signal passages cannot isolate a traffic-light effect/);
  assert.match(html, /The timing bounds overlap/);
  assert.doesNotMatch(html, /mph (?:slower|faster)/);
});

test('comparison filters exclude other corridors, dates, directions, day types, and hours', () => {
  const data = fixture([row,
    { ...row, corridor: 'canal', route: '47', intervals: 777 },
    { ...row, date: '2026-08-31', intervals: 777 },
    { ...row, date: '2026-09-03', intervals: 777 },
    { ...row, direction: '1', intervals: 777 },
    { ...row, day_type: 'weekend', intervals: 777 },
    { ...row, hour: 7, intervals: 777 },
    { ...row, hour: 10, intervals: 777 },
    { ...row, date: '2026-09-02', hour: 9, intervals: 3 },
  ]);
  const html = renderToStaticMarkup(<StreetcarPanel data={data} initialFilters={{ from: '2026-09-01', to: '2026-09-02', direction: '0', dayType: 'weekday', hourFrom: 8, hourTo: 9 }} />);
  assert.match(card(html, 'Signal only'), /4 completed passages/);
  assert.doesNotMatch(html, /777/);
  assert.match(html, /Weekdays include weekday holidays/);
});

test('draws separate accessible passenger-stop and signal markers and labels mixed site observations', () => {
  const data = fixture([row]);
  data.site_bins = [{ ...row, site_id: 'signal-1' }, { ...row, site_id: 'signal-1', category: 'both', intervals: 3 }];
  const html = renderToStaticMarkup(<StreetcarPanel data={data} />);
  assert.match(html, /aria-label="Inspect Jackson signal: road signal"/);
  assert.match(html, /aria-label="Inspect Jackson passenger stop: passenger stop"/);
  assert.match(html, /<polyline/);
  assert.match(html, /<rect/);
  assert.match(html, /<circle/);
  assert.match(html, /Sites can share completed passages; site counts must not be added together/);
  const siteTable = html.split('<tbody>')[1];
  assert.match(siteTable, /Signal only/);
  assert.match(siteTable, /Stop \+ signal/);
  assert.match(siteTable, />4<\/td>/);
});

test('shows attribution, actual imagery verification metadata, and limits on causal claims', () => {
  const html = renderToStaticMarkup(<StreetcarPanel data={fixture([row])} />);
  assert.match(html, /OpenStreetMap contributors/);
  assert.match(html, /Sample vehicle-light detections corroborate two mapped signal locations/);
  assert.match(html, /not a human imagery review or proof that the signal controls a streetcar movement/);
  assert.match(html, /No historical signal-phase data is available/);
  assert.match(html, /gap too long: 45/);
  assert.match(html, /Exclusions below cover whole dates/);
  assert.match(html, /All available dates/);
  assert.match(html, /not statistical confidence intervals/);
  assert.match(html, /Neither category is assumed to represent free-flow travel/);
  assert.doesNotMatch(html, /mph (?:slower|faster)/);
});

test('unprepared and empty datasets keep missing measurements unavailable', () => {
  const data = fixture();
  data.status = 'not_ready';
  const html = renderToStaticMarkup(<StreetcarPanel data={data} />);
  assert.match(html, /Streetcar observations are being prepared/);
  assert.match(html, /No completed passages/);
  assert.match(card(html, 'Signal only'), /Unavailable/);
  assert.doesNotMatch(card(html, 'Signal only'), /0\.0 mph/);
  assert.match(html, /value="2026-09-01"/); // Server-selected dates survive an empty result.
  assert.match(html, /Incomplete observations are excluded/);
});

test('zero lower duration has an unbounded speed upper limit, not an invented finite estimate', () => {
  const html = renderToStaticMarkup(<StreetcarPanel data={fixture([row])} />);
  const signal = card(html, 'Signal only');
  assert.match(signal, /0\.0–60\.0 s/);
  assert.match(signal, /Estimated speed: 14\.9 mph/);
  assert.match(signal, /7\.5 mph to an unbounded upper speed/);
  assert.doesNotMatch(signal, /Infinity|NaN/);
});

test('a missing timing bound is unavailable rather than averaging only the bounded subset', () => {
  for (const missing of [null, undefined]) {
    const data = fixture([row, { ...row, duration_lower_seconds: missing }]);
    const html = renderToStaticMarkup(<StreetcarPanel data={data} />);
    assert.match(card(html, 'Signal only'), /Unavailable/);
    assert.match(card(html, 'Signal only'), /Speed bounds: Unavailable/);
    assert.match(card(html, 'Signal only'), /2 completed passages/);
    assert.doesNotMatch(card(html, 'Signal only'), /0\.0–60\.0 s/);
  }
});

test('independent API loading does not require OTP data', () => {
  const html = renderToStaticMarkup(<StreetcarPanel />);
  assert.match(html, /Loading streetcar observations and mapped signals/);
  assert.match(html, /St\. Charles/);
  assert.match(html, /Canal/);
  assert.match(html, /Rampart/);
});

test('links only actually reviewed Mapillary imagery and distinguishes detection-only corroboration', () => {
  const data = fixture([row]);
  data.network.sites[0].verification = 'mapillary';
  data.network.sites[0].source_ids.push('mapillary/feature/12345');
  const detectionOnly = renderToStaticMarkup(<StreetcarPanel data={data} initialSelectedSite="signal-1" />);
  assert.match(detectionOnly, /Mapillary detection corroborated; imagery unreviewed/);
  assert.doesNotMatch(detectionOnly, /href="https:\/\/www\.mapillary\.com\/app\/\?pKey=/);
  data.network.sites[0].source_ids.push('mapillary/image/987654321');
  const reviewed = renderToStaticMarkup(<StreetcarPanel data={data} initialSelectedSite="signal-1" />);
  assert.match(reviewed, /Mapillary location corroborated; imagery reviewed/);
  assert.match(reviewed, /href="https:\/\/www\.mapillary\.com\/app\/\?pKey=987654321"/);
  assert.match(reviewed, /View reviewed Mapillary imagery/);
  assert.doesNotMatch(reviewed, /pKey=12345/);
});

test('rail-tagged signals retain visible uncertainty and explain their headline exclusion', () => {
  const data = fixture();
  data.network.sites[0].kind = 'rail_signal';
  const html = renderToStaticMarkup(<StreetcarPanel data={data} initialSelectedSite="signal-1" />);
  assert.match(html, /Rail signal — function unverified/);
  assert.match(html, /Passages near this rail-tagged signal are excluded from headline comparisons until its function is verified/);
  assert.match(html, /It may control streetcar traffic or a track switch/);
});

test('explains sampling and timestamp precision separately without applying the API padding twice', () => {
  const html = renderToStaticMarkup(<StreetcarPanel data={fixture([row])} />);
  assert.match(html, /report spacing and 60-second timestamp precision/);
  assert.match(html, /Provider timestamps have minute precision even when relay polls are more frequent/);
  assert.match(html, /additional 60 seconds per passage on either side for timestamp truncation/);
  assert.match(html, /do not bound GPS position error, track-assignment error, or delay caused by a signal/);
  assert.doesNotMatch(html, /Positions are typically reported about once a minute/);
  assert.match(card(html, 'Signal only'), /0\.0–60\.0 s/); // Values have already been padded by the API.
  assert.doesNotMatch(card(html, 'Signal only'), /0\.0–120\.0 s/);
});
