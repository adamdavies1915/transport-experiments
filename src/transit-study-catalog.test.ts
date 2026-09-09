import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';
import { zipSync, strToU8 } from 'fflate';
import { buildTransitStudyCatalog, type StudyOsmInput } from './transit-study-catalog';
import { classifyStudyRow } from './transit-study';
import { pathLength, projectSegment, distance } from './transit-study-geometry';
import type { StudyCatalog, StudyRowSection } from './transit-study-types';

const gtfs = zipSync(Object.fromEntries(Object.entries({
  'routes.txt': 'route_id,route_type,route_long_name\n12,0,Streetcar\n91,3,Bus\nF,4,Ferry\n',
  'trips.txt': 'route_id,trip_id,shape_id,direction_id,trip_headsign\n12,t1,s,0,Outbound\n91,t2,s,0,Bus outbound\n91,t3,s,0,Bus variant\n91,deadhead,nis,0,Not in Service\nF,ferry,s,0,Ferry\n',
  'stops.txt': 'stop_id,stop_name,stop_lat,stop_lon\na,Stop A,29.950,-90.10\nb,Stop B,29.950,-90.09\nc,Stop C,29.950,-90.095\n',
  'stop_times.txt': 'trip_id,stop_id,stop_sequence\nt1,a,1\nt1,b,2\nt2,a,1\nt2,b,2\nt3,a,1\nt3,c,2\nt3,b,3\ndeadhead,a,1\ndeadhead,b,2\nferry,a,1\nferry,b,2\n',
  'shapes.txt': 'shape_id,shape_pt_sequence,shape_pt_lat,shape_pt_lon\ns,1,29.950,-90.10\ns,2,29.950,-90.09\nnis,1,29.950,-90.10\nnis,2,29.950,-90.09\n',
}).map(([name, value]) => [name, strToU8(value)])));
const osm: StudyOsmInput = {
  fetched_at: '2026-09-08', source_url: 'https://www.openstreetmap.org', attribution: 'OSM contributors',
  elements: [
    { id: 1, lat: 29.950, lon: -90.096, tags: { highway: 'traffic_signals' } },
    { id: 2, lat: 29.950, lon: -90.095, tags: { highway: 'traffic_signals' } },
    { id: 3, lat: 29.950, lon: -90.094, tags: { railway: 'signal' } },
    { id: 4, lat: 29.960, lon: -90.093, tags: { highway: 'traffic_signals' } },
  ],
  context_ways: [{ id: 1, nodes: [1, 4], tags: { highway: 'residential' } }, { id: 2, nodes: [2], tags: { highway: 'footway' } }],
};
test('catalog includes bus and streetcar passenger stopping variants, excludes nonpassenger trips and other modes', () => {
  const c = buildTransitStudyCatalog(gtfs, osm);
  assert.equal(c.paths.length, 3); assert.equal(c.paths.filter(p => p.mode === 'bus').length, 2);
  assert.ok(c.paths.every(p => !p.id.includes('nis') && p.route_id !== 'F'));
  assert.equal(new Set(c.paths.filter(p => p.mode === 'bus').map(p => p.stop_ids.length)).size, 2);
  assert.ok(c.row_sections.every(s => s.row_class === 'unknown'));
  assert.equal(c.sites.filter(s => s.kind === 'signal').length, 1);
  assert.equal(c.sites.filter(s => s.kind === 'rail_signal').length, 1);
  assert.deepEqual(c.sites.find(s => s.source_ids.includes('osm/node/1'))!.route_ids, ['12', '91']);
  assert.equal(buildTransitStudyCatalog(gtfs, osm, { excluded_osm_node_ids: [1] }).sites.filter(s => s.kind === 'signal').length, 0);
});
test('ROW catalog fills explicit unknown gaps and rejects invalid, overlapping or undated reviews', () => {
  const path = buildTransitStudyCatalog(gtfs, osm).paths[0];
  const review: StudyRowSection = { id: 'review', path_id: path.id, from_meters: 200, to_meters: 400, row_class: 'reserved',
    valid_from: '2026-09-08', valid_to: null, reviewed_at: '2026-09-08', evidence_urls: ['https://www.openstreetmap.org/way/1'], notes: 'Fixture' };
  const c = buildTransitStudyCatalog(gtfs, osm, { row_sections: [review] });
  const sections = c.row_sections.filter(s => s.path_id === path.id);
  assert.deepEqual(sections.map(s => s.row_class), ['unknown', 'reserved', 'unknown']);
  assert.equal(sections[0].from_meters, 0); assert.equal(sections.at(-1)!.to_meters, pathLength(path.points));
  for (const patch of [{ reviewed_at: null }, { valid_from: '2026-02-30' }, { evidence_urls: [] }, { to_meters: 9000 }])
    assert.throws(() => buildTransitStudyCatalog(gtfs, osm, { row_sections: [{ ...review, ...patch }] }));
  assert.throws(() => buildTransitStudyCatalog(gtfs, osm, { row_sections: [review, { ...review, id: 'overlap', from_meters: 300 }] }), /Overlapping/);
  assert.equal(buildTransitStudyCatalog(gtfs, osm, { generated_at: '2026-09-09' }).version,
    buildTransitStudyCatalog(gtfs, osm, { generated_at: '2026-09-10' }).version);
});
test('checked-in catalog supplies all passenger routes and bounded date-applicable ROW evidence without inventing bus ROW', () => {
  const c = JSON.parse(readFileSync(new URL('./data/transit-study-network.json', import.meta.url), 'utf8')) as StudyCatalog;
  assert.equal(new Set(c.paths.filter(p => p.mode === 'bus').map(p => p.route_id)).size, 28);
  assert.equal(new Set(c.paths.filter(p => p.mode === 'streetcar').map(p => p.route_id)).size, 5);
  assert.ok(c.sites.some(s => s.kind === 'signal' && s.route_ids.includes('91')));
  const reviewed = c.row_sections.filter(s => s.row_class !== 'unknown');
  assert.equal(reviewed.length, 6); assert.ok(reviewed.every(s => c.paths.find(p => p.id === s.path_id)!.route_id === '12'));
  for (const s of reviewed) {
    assert.equal(classifyStudyRow(c, s.path_id, s.from_meters, s.to_meters, '2026-08-01').row_class, 'unknown');
    assert.equal(classifyStudyRow(c, s.path_id, s.from_meters, s.to_meters, '2026-09-08').row_class, s.row_class);
  }
  const excluded = ['115873908', '115873911', '116009021', '116009319'];
  assert.ok(c.sites.every(s => !s.source_ids.some(id => excluded.some(n => id === `osm/node/${n}`))));
});
test('reviewed path-distance intervals stay on the saved OSM alignment; shared reviews require actual embedded-road tags', () => {
  const c = JSON.parse(readFileSync(new URL('./data/transit-study-network.json', import.meta.url), 'utf8')) as StudyCatalog;
  const evidence = JSON.parse(readFileSync(new URL('./data/transit-row-osm-evidence.json', import.meta.url), 'utf8')) as {
    ways: Array<{ elements: Array<{ type: string; id: number; lat: number; lon: number; nodes: number[]; tags: Record<string, string> }> }>;
  };
  for (const review of c.row_sections.filter(s => s.row_class !== 'unknown')) {
    const wayId = Number(review.evidence_urls[0].split('/').at(-1));
    const input = evidence.ways.find(w => w.elements.some(e => e.type === 'way' && e.id === wayId))!;
    const way = input.elements.find(e => e.type === 'way')!, nodes = new Map(input.elements.filter(e => e.type === 'node').map(e => [e.id, e]));
    if (review.row_class === 'shared') { assert.equal(way.tags.embedded_rails, 'tram'); assert.ok(way.tags.highway); }
    const points = way.nodes.map(id => nodes.get(id)!);
    const path = c.paths.find(p => p.id === review.path_id)!;
    let along = 0, checked = 0;
    for (let i = 1; i < path.points.length; i++) {
      along += distance(path.points[i - 1], path.points[i]);
      if (along < review.from_meters || along > review.to_meters) continue;
      const nearest = Math.min(...points.slice(1).map((p, j) => projectSegment(path.points[i], points[j], p).distance));
      assert.ok(nearest <= 25, `${review.id}: GTFS point is ${nearest}m from evidence`); checked++;
    }
    assert.ok(checked > 2);
  }
});
