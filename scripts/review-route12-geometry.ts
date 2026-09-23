/** Review existing geographic study windows against a newly retained schedule.
 * This reports correspondence; it never changes classifications or validity dates. */
import { readFile } from 'node:fs/promises';
import { unzipSync, strFromU8 } from 'fflate';
import { parse } from 'csv-parse/sync';
import { createHash } from 'node:crypto';
import { distance, projectPath } from '../src/transit-study-geometry';
import type { StudyCatalog, StudyPath, StudyPoint } from '../src/transit-study-types';

const zipPath = process.argv[2];
if (!zipPath) throw new Error('Usage: node --import tsx scripts/review-route12-geometry.ts retained-GTFS.zip');
const bytes = await readFile(zipPath), zip = unzipSync(bytes);
const csv = (name: string): Record<string, string>[] => parse(strFromU8(zip[name]), { columns: true, skip_empty_lines: true });
const catalog: StudyCatalog = JSON.parse(await readFile(new URL('../src/data/transit-study-network.json', import.meta.url), 'utf8'));
const trips = csv('trips.txt').filter(t => t.route_id === '12' && !/not\s+in\s+service/i.test(t.trip_headsign));
const shapes = csv('shapes.txt');
const paths: StudyPath[] = [...new Map(trips.map(t => [`${t.shape_id}|${t.direction_id}`, t])).values()].map(t => ({
  id: `${t.shape_id}|12|${t.direction_id}`, route_id: '12', direction_id: t.direction_id, mode: 'streetcar',
  name: t.trip_headsign, stop_ids: [], points: shapes.filter(s => s.shape_id === t.shape_id)
    .sort((a, b) => Number(a.shape_pt_sequence) - Number(b.shape_pt_sequence))
    .map(s => ({ lat: Number(s.shape_pt_lat), lon: Number(s.shape_pt_lon) })),
}));
function pointAt(path: StudyPath, at: number): StudyPoint {
  let traversed = 0;
  for (let i = 1; i < path.points.length; i++) {
    const a = path.points[i - 1], b = path.points[i], length = distance(a, b);
    if (traversed + length >= at) { const f = (at - traversed) / length; return { lat: a.lat + f * (b.lat - a.lat), lon: a.lon + f * (b.lon - a.lon) }; }
    traversed += length;
  }
  throw new Error('Section exceeds retained geometry');
}
const sections = catalog.row_sections.filter(s => s.row_class !== 'unknown' && catalog.paths.find(p => p.id === s.path_id)?.route_id === '12').map(s => {
  const original = catalog.paths.find(p => p.id === s.path_id)!;
  const positions = [s.from_meters, s.to_meters];
  for (let at = s.from_meters + 10; at < s.to_meters; at += 10) positions.push(at);
  const candidates = paths.filter(p => p.direction_id === original.direction_id).map(p => {
    const matches = positions.map(at => projectPath(p, pointAt(original, at), 10));
    return { path_id: p.id, matched_points: matches.filter(Boolean).length, checked_points: positions.length,
      maximum_distance_meters: Math.max(...matches.flatMap(m => m ? [m.distance] : [])),
      complete: matches.every(Boolean) };
  });
  return { section_id: s.id, row_class: s.row_class, entry: pointAt(original, s.from_meters), exit: pointAt(original, s.to_meters),
    represented_in_current_schedule: candidates.some(c => c.complete), candidates };
});
console.log(JSON.stringify({ checked_at: new Date().toISOString(), historical_catalog: catalog.version,
  current_gtfs_sha256: createHash('sha256').update(bytes).digest('hex'), sections,
  interpretation: '10 m-spaced geographic checks with a 10 m tolerance. Correspondence does not verify traffic restrictions, signal control, stop exposure, or historical service. Unmatched sections require review; no roadway classifications were expanded.' }, null, 2));
