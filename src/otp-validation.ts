import { readFile } from 'node:fs/promises';
import { pathToFileURL } from 'node:url';
import { DuckDBInstance } from '@duckdb/node-api';
import 'dotenv/config';
import { activeServices, addDays, estimateOtp, observationEpoch, readSchedule,
  type Observation, type Schedule, type ScheduledTrip } from './otp';
import { mappedTrip } from './trip-crosswalk';

// Learn only before the split; the held-out GTFS IDs are used exclusively for
// scoring. A correct stop/time with the wrong trip is still a failed prediction.
export function validateReconstruction(schedule: Schedule, day: string, known: Observation[], split: number) {
  const active = activeServices(schedule, day);
  const trips = new Map(schedule.trips.filter(t => active.has(t.service)).map(t => [t.id, t]));
  const eligible = known.filter(p => p.exact_id && p.legacy_trip_id &&
    trips.get(p.trip_id ?? '')?.route === p.route);
  const training = eligible.filter(p => p.at < split);
  const heldOut = eligible.filter(p => p.at >= split);
  const pairs = new Map<string, Set<string>>();
  for (const p of training) {
    const key = `${p.route}|${p.legacy_trip_id}`;
    const ids = pairs.get(key) ?? new Set<string>();
    ids.add(p.trip_id!); pairs.set(key, ids);
  }
  const mapping = new Map<string, ScheduledTrip>();
  for (const [key, ids] of pairs) if (ids.size === 1) mapping.set(key, trips.get([...ids][0])!);
  const truth = new Map<string, Set<string>>();
  for (const p of heldOut) {
    const key = `${p.vid}|${p.route}|${p.at}`;
    const ids = truth.get(key) ?? new Set<string>();
    ids.add(p.trip_id!); truth.set(key, ids);
  }
  const score = (useMapping: boolean) => {
    const masked = heldOut.map(p => {
      const mapped = useMapping ? mappedTrip(mapping, p.route, p.legacy_trip_id!, p.block, p.destination) : undefined;
      return { ...p, trip_id: mapped?.id ?? p.legacy_trip_id!, exact_id: !!mapped,
        id_source: mapped ? 'crosswalk' as const : 'trip_id' as const };
    });
    const events = estimateOtp(schedule, day, masked).events
      .filter(e => e.match_method === (useMapping ? 'crosswalk' : 'block'));
    const results = events.map(e => {
      const before = truth.get(`${e.vid}|${e.route}|${e.observed_from}`);
      const after = truth.get(`${e.vid}|${e.route}|${e.observed_to}`);
      const scorable = before?.size === 1 && after?.size === 1 && [...before][0] === [...after][0];
      return { route: e.route, predicted: e.trip_id, actual: scorable ? [...before!][0] : null,
        correct: !!scorable && before!.has(e.trip_id), vid: e.vid };
    });
    const scorable = results.filter(r => r.actual !== null);
    const correct = scorable.filter(r => r.correct).length;
    return { events: results.length, scorable: scorable.length, correct,
      wrong: scorable.length - correct, unscorable: results.length - scorable.length,
      accuracy_pct: scorable.length ? 100 * correct / scorable.length : null,
      distinct_actual_trips: new Set(scorable.map(r => `${r.route}|${r.actual}`)).size,
      routes: [...new Set(scorable.map(r => r.route))].sort(),
      mismatches: scorable.filter(r => !r.correct) };
  };
  return { schedule_hash: schedule.hash, day, split_epoch: split,
    first_observation_epoch: eligible.length ? eligible.reduce((v, p) => Math.min(v, p.at), Infinity) : null,
    last_observation_epoch: eligible.length ? eligible.reduce((v, p) => Math.max(v, p.at), -Infinity) : null,
    training_readings: training.length, held_out_readings: heldOut.length,
    held_out_trips: new Set(heldOut.map(p => `${p.route}|${p.trip_id}`)).size,
    direct_events: estimateOtp(schedule, day, heldOut).events.filter(e => e.match_method === 'trip_id').length,
    crosswalk: score(true), block: score(false),
    limitation: 'Temporal holdout on this service day only; not proof of historical mapping stability or other service patterns.' };
}

async function main() {
  const args = process.argv.slice(2);
  const get = (flag: string) => { const i = args.indexOf(flag); return i < 0 ? undefined : args[i + 1]; };
  const file = get('--gtfs'), day = get('--day');
  if (!file || !day || addDays(day, 0) !== day) throw new Error('Usage: npm run otp:validate -- --gtfs archive.zip --day YYYY-MM-DD [--split ISO_TIMESTAMP]');
  const schedule = readSchedule(new Uint8Array(await readFile(file)));
  if (day < schedule.start || day > schedule.end) throw new Error('Day outside archive validity');
  if (!process.env.MOTHER_DUCK_API_KEY) throw new Error('Missing MOTHER_DUCK_API_KEY');
  const db = await DuckDBInstance.create(`md:${process.env.MOTHERDUCK_DATABASE || 'my_db'}?motherduck_token=${process.env.MOTHER_DUCK_API_KEY}`);
  const c = await db.connect();
  try {
    const rows = (await c.runAndReadAll(`SELECT DISTINCT vid, route, trip_id, gtfs_trip_id,
      tablockid, destination, lat, lon, is_off_route, epoch(observed_at) AS at FROM transit_data
      WHERE timestamp >= '${day}'::DATE AND timestamp < '${addDays(day, 1)}'::DATE
      AND gtfs_trip_id IS NOT NULL AND observed_at IS NOT NULL`)).getRowObjectsJson();
    const known: Observation[] = rows.filter(r => r.lat != null && r.lon != null).map(r => ({
      vid: String(r.vid), route: String(r.route), trip_id: String(r.gtfs_trip_id),
      legacy_trip_id: r.trip_id == null ? undefined : String(r.trip_id), exact_id: true,
      block: r.tablockid == null ? null : String(r.tablockid),
      destination: r.destination == null ? null : String(r.destination),
      lat: Number(r.lat), lon: Number(r.lon), at: Number(r.at), off_route: r.is_off_route === true,
    }));
    if (!known.length) throw new Error('No paired observations available');
    const times = known.map(p => p.at).sort((a, b) => a - b);
    const split = get('--split') ? observationEpoch(get('--split')!, schedule.timezone) : times[Math.floor(times.length / 2)];
    if (split == null || split <= times[0] || split > times[times.length - 1]) throw new Error('Split must leave both training and held-out observations');
    console.log(JSON.stringify(validateReconstruction(schedule, day, known, split), null, 2));
  } finally { c.closeSync(); db.closeSync(); }
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch(err => { console.error(String(err)); process.exitCode = 1; });
}
