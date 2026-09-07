import { createHash } from 'node:crypto';
import { unzipSync, strFromU8 } from 'fflate';
import { parse } from 'csv-parse/sync';
import { Temporal } from '@js-temporal/polyfill';

// An independently estimated timepoint OTP, not the BusTime dly flag.
export const OTP_METHOD = 'timepoint-departure-v2';
export const EARLY_SECONDS = -60;
export const LATE_SECONDS = 300;
export const MAX_GAP_SECONDS = 120;
export const STOP_RADIUS_METERS = 35;
// Used only to reject ambiguous block matches, never to pick the nearest trip.
export const BLOCK_WINDOW_SECONDS = 90 * 60;

type CsvRow = Record<string, string>;
export interface ScheduledStop {
  id: string; sequence: number; lat: number; lon: number;
  arrival: number; departure: number; timepoint: boolean;
}
export interface ScheduledTrip {
  id: string; route: string; service: string; block: string; headsign: string;
  stops: ScheduledStop[];
}
export interface Schedule {
  hash: string; timezone: string; start: string; end: string;
  trips: ScheduledTrip[]; calendar: CsvRow[]; exceptions: CsvRow[];
}
export interface Observation {
  vid: string; trip_id: string | null; route: string; block: string | null;
  exact_id: boolean; // Verified raw tripid or a conflict-free observed ID mapping.
  id_source?: 'trip_id' | 'crosswalk';
  legacy_trip_id?: string;
  destination: string | null; at: number; lat: number; lon: number; off_route: boolean;
}
export interface OtpEvent {
  service_date: string; route: string; trip_id: string; stop_sequence: number;
  stop_id: string; scheduled_at: number; observed_from: number; observed_to: number;
  deviation_seconds: number; status: 'early' | 'on_time' | 'late' | 'uncertain';
  match_method: 'trip_id' | 'crosswalk' | 'block'; vid: string;
  mapping_legacy_id: string | null;
}
export interface OtpCoverage {
  service_date: string; route: string; scheduled_timepoints: number;
  observed_timepoints: number; classified_timepoints: number;
  observed_trips: number; matched_trips: number; block_matched_trips: number;
}

export function gtfsSeconds(value: string): number {
  const m = /^(\d{2,}):([0-5]\d):([0-5]\d)$/.exec(value);
  if (!m) throw new Error(`Invalid GTFS time: ${value}`);
  return Number(m[1]) * 3600 + Number(m[2]) * 60 + Number(m[3]);
}
function date(value: string): string {
  if (!/^\d{8}$/.test(value)) throw new Error(`Invalid GTFS date: ${value}`);
  return Temporal.PlainDate.from(`${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6)}`).toString();
}
export function readSchedule(bytes: Uint8Array): Schedule {
  const files = unzipSync(bytes);
  const rows = (name: string, required = true): CsvRow[] => {
    if (!files[name]) {
      if (required) throw new Error(`Missing GTFS ${name}`);
      return [];
    }
    return parse(strFromU8(files[name]), { columns: true, bom: true, skip_empty_lines: true });
  };
  const agencies = rows('agency.txt');
  const timezone = agencies[0]?.agency_timezone;
  if (!timezone || agencies.some(a => a.agency_timezone !== timezone)) throw new Error('Expected one agency timezone');
  const info = rows('feed_info.txt')[0];
  if (!info?.feed_start_date || !info.feed_end_date) throw new Error('GTFS must declare its validity dates');
  const routes = new Map(rows('routes.txt').filter(r => ['0', '3'].includes(r.route_type)).map(r => [r.route_id, r.route_short_name]));
  const stops = new Map(rows('stops.txt').map(s => [s.stop_id, s]));
  const frequencies = new Set(rows('frequencies.txt', false).map(r => r.trip_id));
  const tripStops = new Map<string, ScheduledStop[]>();
  for (const row of rows('stop_times.txt')) {
    const stop = stops.get(row.stop_id);
    if (!stop || !row.arrival_time || !row.departure_time) continue;
    const s: ScheduledStop = {
      id: row.stop_id, sequence: Number(row.stop_sequence),
      lat: Number(stop.stop_lat), lon: Number(stop.stop_lon),
      arrival: gtfsSeconds(row.arrival_time), departure: gtfsSeconds(row.departure_time),
      timepoint: row.timepoint !== '0', // GTFS defaults an omitted timepoint to 1.
    };
    if (!stop.stop_lat || !stop.stop_lon || !Number.isFinite(s.lat) || !Number.isFinite(s.lon)) throw new Error('Invalid stop coordinates');
    const group = tripStops.get(row.trip_id) ?? [];
    group.push(s); tripStops.set(row.trip_id, group);
  }
  const trips: ScheduledTrip[] = [];
  for (const t of rows('trips.txt')) {
    const route = routes.get(t.route_id);
    const ordered = tripStops.get(t.trip_id)?.sort((a, b) => a.sequence - b.sequence);
    if (!route || !ordered?.length || frequencies.has(t.trip_id)) continue;
    if (ordered.some((s, i) => s.departure < s.arrival || (i > 0 && s.arrival < ordered[i - 1].departure))) {
      throw new Error(`Non-monotonic schedule for trip ${t.trip_id}`);
    }
    trips.push({ id: t.trip_id, route, service: t.service_id, block: t.block_id,
      headsign: t.trip_headsign, stops: ordered });
  }
  if (!trips.length) throw new Error('No scheduled bus/streetcar trips in feed');
  return { hash: createHash('sha256').update(bytes).digest('hex'), timezone,
    start: date(info.feed_start_date), end: date(info.feed_end_date), trips,
    calendar: rows('calendar.txt', false), exceptions: rows('calendar_dates.txt', false) };
}

export function activeServices(schedule: Schedule, day: string): Set<string> {
  const active = new Set<string>();
  if (day < schedule.start || day > schedule.end) return active;
  const weekday = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday'][Temporal.PlainDate.from(day).dayOfWeek - 1];
  for (const c of schedule.calendar) {
    if (day >= date(c.start_date) && day <= date(c.end_date) && c[weekday] === '1') active.add(c.service_id);
  }
  for (const e of schedule.exceptions) {
    if (date(e.date) === day) {
      if (e.exception_type === '1') active.add(e.service_id);
      if (e.exception_type === '2') active.delete(e.service_id);
    }
  }
  return active;
}

// GTFS times are seconds since local noon minus 12 hours, including DST days.
export function serviceEpoch(day: string, timezone: string): number {
  return Temporal.PlainDate.from(day).toZonedDateTime({ timeZone: timezone, plainTime: '12:00' })
    .subtract({ hours: 12 }).epochMilliseconds / 1000;
}
export function localDay(at: number, timezone: string): string {
  return Temporal.Instant.fromEpochMilliseconds(Math.round(at * 1000)).toZonedDateTimeISO(timezone).toPlainDate().toString();
}
export function addDays(day: string, amount: number): string {
  return Temporal.PlainDate.from(day).add({ days: amount }).toString();
}
// Legacy TIMESTAMP values lost the original UTC offset. Reject ambiguous or
// nonexistent wall times rather than inventing an observation during DST changes.
export function observationEpoch(value: string, timezone: string): number | null {
  try {
    if (/(Z|[+-]\d\d:\d\d)$/.test(value)) return Temporal.Instant.from(value.replace(' ', 'T')).epochMilliseconds / 1000;
    return Temporal.PlainDateTime.from(value.replace(' ', 'T')).toZonedDateTime(timezone, { disambiguation: 'reject' }).epochMilliseconds / 1000;
  } catch { return null; }
}
function distance(a: { lat: number; lon: number }, b: { lat: number; lon: number }): number {
  const rad = Math.PI / 180;
  const x = (b.lon - a.lon) * rad * Math.cos((a.lat + b.lat) * rad / 2);
  const y = (b.lat - a.lat) * rad;
  return Math.hypot(x, y) * 6371000;
}
export function classifyInterval(from: number, to: number): OtpEvent['status'] {
  if (to < EARLY_SECONDS) return 'early';
  if (from > LATE_SECONDS) return 'late';
  if (from >= EARLY_SECONDS && to <= LATE_SECONDS) return 'on_time';
  return 'uncertain';
}
function headsign(value: string | null): string { return (value ?? '').trim().toLowerCase().replace(/\s+/g, ' '); }

export function estimateOtp(schedule: Schedule, day: string, observations: Observation[], asOf = Infinity): { events: OtpEvent[]; coverage: OtpCoverage[] } {
  const services = activeServices(schedule, day);
  const trips = schedule.trips.filter(t => services.has(t.service));
  const epoch = serviceEpoch(day, schedule.timezone);
  const byId = new Map(trips.map(t => [t.id, t]));
  const blockTrips = new Map<string, ScheduledTrip[]>();
  const coverage = new Map<string, OtpCoverage>();
  for (const trip of trips) {
    const key = `${trip.route}|${trip.block}|${headsign(trip.headsign)}`;
    const group = blockTrips.get(key) ?? []; group.push(trip); blockTrips.set(key, group);
    const c = coverage.get(trip.route) ?? { service_date: day, route: trip.route,
      scheduled_timepoints: 0, observed_timepoints: 0, classified_timepoints: 0,
      observed_trips: 0, matched_trips: 0, block_matched_trips: 0 };
    c.scheduled_timepoints += trip.stops.filter((s, i) => s.timepoint && epoch + (i === trip.stops.length - 1 ? s.arrival : s.departure) <= asOf).length;
    coverage.set(trip.route, c);
  }
  const groups = new Map<string, Observation[]>();
  // Include the next calendar day for after-midnight GTFS times. Group by the
  // service-day candidate, then demand unique matching across neighboring days.
  for (const p of observations) {
    if (!coverage.has(p.route) || !p.trip_id || ['N/A', '0'].includes(p.trip_id) || p.off_route ||
        !Number.isFinite(p.at) || p.at > asOf || !Number.isFinite(p.lat) || !Number.isFinite(p.lon) ||
        Math.abs(p.lat) > 90 || Math.abs(p.lon) > 180 || (p.lat === 0 && p.lon === 0)) continue;
    const key = `${p.vid}|${p.route}|${p.trip_id}`;
    const group = groups.get(key) ?? []; group.push(p); groups.set(key, group);
  }
  // A trip can cross midnight (or 03:00); never split on a fixed clock cutoff.
  // Separate repeated uses of a trip ID only after a six-hour observation gap.
  const runs: Observation[][] = [];
  for (const group of groups.values()) {
    let run: Observation[] = [];
    for (const p of group.sort((a, b) => a.at - b.at)) {
      if (run.length && p.at - run[run.length - 1].at > 6 * 3600) { runs.push(run); run = []; }
      run.push(p);
    }
    if (run.length) runs.push(run);
  }
  const candidates: Array<{ trip: ScheduledTrip; pings: Observation[]; method: OtpEvent['match_method'] }> = [];
  for (const group of runs) {
    const pings = [...new Map(group.sort((a, b) => a.at - b.at).map(p => [p.at, p])).values()];
    const first = pings[0], last = pings[pings.length - 1];
    if (new Set(pings.filter(p => p.id_source === 'crosswalk').map(p => p.legacy_trip_id)).size > 1) continue;
    const c = coverage.get(first.route)!;
    // Only count groups overlapping this service day's schedule envelope.
    if (last.at < epoch || first.at >= epoch + 48 * 3600) continue;
    const direct = first.exact_id && pings.every(p => p.exact_id) ? byId.get(first.trip_id!) : undefined;
    let trip: ScheduledTrip | undefined;
    let method: OtpEvent['match_method'] = pings.some(p => p.id_source === 'crosswalk') ? 'crosswalk' : 'trip_id';
    const overlaps = (t: ScheduledTrip, base: number, window: number) =>
      first.at >= base + t.stops[0].arrival - window &&
      last.at <= base + t.stops[t.stops.length - 1].departure + window;
    if (direct && direct.route === first.route && overlaps(direct, epoch, 6 * 3600)) {
      // Exact trip IDs permit very late/early service, but must belong to a
      // unique service date (trip IDs repeat across days).
      const alternatives = [-1, 1].some(offset => {
        const otherDay = addDays(day, offset);
        return activeServices(schedule, otherDay).has(direct.service) && overlaps(direct, serviceEpoch(otherDay, schedule.timezone), 6 * 3600);
      });
      if (!alternatives) trip = direct;
    } else if (!direct && first.block && first.destination && pings.every(p => p.block === first.block && headsign(p.destination) === headsign(first.destination))) {
      method = 'block';
      const key = `${first.route}|${first.block}|${headsign(first.destination)}`;
      const possible = (blockTrips.get(key) ?? []).filter(t => overlaps(t, epoch, BLOCK_WINDOW_SECONDS));
      // Never choose the nearest departure: two plausible trips means unknown.
      const neighbors = [-1, 1].some(offset => {
        const otherDay = addDays(day, offset), active = activeServices(schedule, otherDay);
        return schedule.trips.some(t => active.has(t.service) && `${t.route}|${t.block}|${headsign(t.headsign)}` === key &&
          overlaps(t, serviceEpoch(otherDay, schedule.timezone), BLOCK_WINDOW_SECONDS));
      });
      if (possible.length === 1 && !neighbors) trip = possible[0];
    }
    if (trip || localDay(first.at, schedule.timezone) === day) c.observed_trips++;
    if (trip) candidates.push({ trip, pings, method });
  }
  const events: OtpEvent[] = [];
  // Conflicting vehicles/trip assignments remain unknown; no double counting.
  const counts = new Map<string, number>();
  for (const c of candidates) counts.set(c.trip.id, (counts.get(c.trip.id) ?? 0) + 1);
  for (const { trip, pings, method } of candidates) {
    if (counts.get(trip.id) !== 1) continue;
    const c = coverage.get(trip.route)!;
    c.matched_trips++; if (method === 'block') c.block_matched_trips++;
    let lastEventAt = -Infinity;
    for (let index = 0; index < trip.stops.length; index++) {
      const stop = trip.stops[index];
      if (!stop.timepoint) continue;
      // Repeated/nearby stops on a loop cannot be distinguished by this GPS
      // geofence. Leave them unobserved until a shape-based matcher is available.
      if (trip.stops.some((s, i) => i !== index && distance(s, stop) < 2 * STOP_RADIUS_METERS)) continue;
      const terminal = index === trip.stops.length - 1;
      const visits: Array<[number, number]> = [];
      for (let i = 1; i < pings.length; i++) {
        const before = pings[i - 1], after = pings[i];
        const gap = after.at - before.at;
        if (gap <= 0 || gap > MAX_GAP_SECONDS || distance(before, after) / gap > 45) continue;
        const insideBefore = distance(before, stop) <= STOP_RADIUS_METERS;
        const insideAfter = distance(after, stop) <= STOP_RADIUS_METERS;
        // Departure: last in-geofence to first out. Final stop: arrival instead.
        if (terminal ? (!insideBefore && insideAfter) : (insideBefore && !insideAfter)) visits.push([before.at, after.at]);
      }
      if (visits.length !== 1 || visits[0][0] < lastEventAt) continue;
      const [from, to] = visits[0];
      const scheduled = epoch + (terminal ? stop.arrival : stop.departure);
      if (scheduled > asOf) continue;
      lastEventAt = to;
      const status = classifyInterval(from - scheduled, to - scheduled);
      events.push({ service_date: day, route: trip.route, trip_id: trip.id,
        stop_sequence: stop.sequence, stop_id: stop.id, scheduled_at: scheduled,
        observed_from: from, observed_to: to, deviation_seconds: (from + to) / 2 - scheduled,
        status, match_method: method, vid: pings[0].vid,
        mapping_legacy_id: method === 'crosswalk' ? pings.find(p => p.id_source === 'crosswalk')?.legacy_trip_id ?? null : null });
      c.observed_timepoints++; if (status !== 'uncertain') c.classified_timepoints++;
    }
  }
  return { events, coverage: [...coverage.values()] };
}
