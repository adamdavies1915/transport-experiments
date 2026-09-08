/** Offline rebuild from explicit saved inputs. Does not fetch data or start jobs. */
import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { dirname } from 'node:path';
import { parseArgs } from 'node:util';
import { createHash } from 'node:crypto';
import { buildTransitStudyCatalog, type StudyOsmInput } from '../src/transit-study-catalog';
import type { StreetcarNetwork } from '../dashboard/src/streetcar-data';
import type { StudyRowSection } from '../src/transit-study-types';

const { values } = parseArgs({ options: {
  gtfs: { type: 'string', default: '/tmp/norta-gtfs.zip' },
  osm: { type: 'string', default: 'src/data/streetcar-signals-osm.json' },
  streetcars: { type: 'string', default: 'src/data/streetcar-network.json' },
  reviews: { type: 'string', default: 'src/data/transit-row-reviews.json' },
  'signal-reviews': { type: 'string', default: 'src/data/streetcar-mapillary-reviews.json' },
  output: { type: 'string', default: 'src/data/transit-study-network.json' },
}, strict: true });
const [gtfs, osmBytes, networkBytes, reviewBytes, signalBytes] = await Promise.all([
  readFile(values.gtfs!), readFile(values.osm!), readFile(values.streetcars!), readFile(values.reviews!), readFile(values['signal-reviews']!),
]);
const network = JSON.parse(networkBytes.toString()) as StreetcarNetwork;
const osm = JSON.parse(osmBytes.toString()) as StudyOsmInput & { elements: Array<{ id: number; version?: number }> };
const reviews = JSON.parse(reviewBytes.toString()) as {
  schedule_hash: string; streetcar_network_version: string; sections: StudyRowSection[];
  sources: StreetcarNetwork['sources']; notes: string[];
};
const signalReviews = JSON.parse(signalBytes.toString()) as {
  exclusions: Array<{ osm_node_versions: Record<string, number> }>;
};
const hash = createHash('sha256').update(gtfs).digest('hex');
if (network.schedule_hash !== hash || reviews.schedule_hash !== hash || reviews.streetcar_network_version !== network.version)
  throw new Error('Schedule or streetcar geometry changed: rebuild the streetcar network and re-review path-distance ROW sections first.');
const nodeVersions = new Map(osm.elements.map(n => [String(n.id), n.version]));
const exclusions = signalReviews.exclusions.flatMap(review => {
  const entries = Object.entries(review.osm_node_versions);
  if (!entries.every(([id, version]) => nodeVersions.get(id) === version))
    throw new Error('A disputed signal has changed OSM revisions: review it before rebuilding.');
  return entries.map(([id]) => Number(id));
});
const started = performance.now();
const catalog = buildTransitStudyCatalog(gtfs, osm, { streetcar_network: network, row_sections: reviews.sections,
  excluded_osm_node_ids: exclusions });
catalog.sources.push(...reviews.sources);
catalog.limitations.push(...reviews.notes);
await mkdir(dirname(values.output!), { recursive: true });
await writeFile(values.output!, JSON.stringify(catalog) + '\n');
console.log(JSON.stringify({ output: values.output, version: catalog.version, paths: catalog.paths.length,
  bus_routes: new Set(catalog.paths.filter(p => p.mode === 'bus').map(p => p.route_id)).size,
  streetcar_routes: new Set(catalog.paths.filter(p => p.mode === 'streetcar').map(p => p.route_id)).size,
  sites: catalog.sites.length, reviewed_row_sections: catalog.row_sections.filter(s => s.row_class !== 'unknown').length,
  elapsed_seconds: (performance.now() - started) / 1000 }));
