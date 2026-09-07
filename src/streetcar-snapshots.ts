import { randomUUID } from 'node:crypto';
import { Temporal } from '@js-temporal/polyfill';
import type { DuckDBConnection } from '@duckdb/node-api';

export const STREETCAR_SNAPSHOT_ROUTES = new Set(['12', '46', '47', '48']);

/** One vehicle entry in one received SSE frame. Receipt time is not GPS fix time. */
export interface StreetcarSnapshot {
  snapshot_id: string;
  receipt_id: string;
  feed_event_id: string | null;
  received_at: string;
  provider_timestamp: string | null;
  provider_observed_at: string | null;
  vid: string | null;
  route: string;
  legacy_trip_id: string | null;
  gtfs_trip_id: string | null;
  destination: string | null;
  lat: number | null;
  lon: number | null;
  heading: number | null;
  speed: number | null;
  is_delayed: boolean | null;
  is_off_route: boolean | null;
  pdist: number | null;
  pid: string | null;
  rid: string | null;
  tablockid: string | null;
  srvtmstmp: string | null;
  source_url: string | null;
  raw_payload: string;
}
export interface SnapshotReceipt {
  received_at: string;
  source_url: string;
  receipt_id?: string;
  feed_event_id?: string | null;
}

function text(value: unknown): string | null {
  return typeof value === 'string' ? value : typeof value === 'number' && Number.isFinite(value) ? String(value) : null;
}
function identifier(value: unknown): string | null {
  const id = text(value)?.trim();
  return id && !['0', 'n/a', 'null'].includes(id.toLowerCase()) ? id : null;
}
function numeric(value: unknown): number | null {
  if ((typeof value !== 'number' && typeof value !== 'string') || typeof value === 'string' && !value.trim()) return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}
function instant(value: string | null): string | null {
  // Do not infer an offset for provider wall times, and do not call a receipt
  // timestamp a fresh GPS observation. The original source text is retained.
  if (!value || !/(?:Z|[+-]\d{2}:?\d{2})$/i.test(value)) return null;
  try { return Temporal.Instant.from(value.replace(' ', 'T')).toString(); }
  catch { return null; }
}
export function snapshotSourceUrl(value: string): string | null {
  try {
    const url = new URL(value);
    // Configured feeds can carry credentials in userinfo or query parameters.
    // Record the endpoint without publishing those credentials.
    return `${url.protocol}//${url.host}${url.pathname}`;
  } catch { return null; }
}

/** Capture before the legacy provider-minute dedup. Never dedup across receipts. */
export function captureStreetcarSnapshots(payload: unknown, receipt: SnapshotReceipt): StreetcarSnapshot[] {
  if (!Array.isArray(payload)) throw new Error('Expected an SSE vehicle array');
  const receivedAt = instant(receipt.received_at);
  if (!receivedAt) throw new Error('Snapshot receipt must have a valid timestamp with timezone');
  const receiptId = receipt.receipt_id ?? randomUUID();
  const source = snapshotSourceUrl(receipt.source_url);
  const snapshots: StreetcarSnapshot[] = [];
  for (let index = 0; index < payload.length; index++) {
    const entry = payload[index];
    if (entry === null || typeof entry !== 'object' || Array.isArray(entry)) continue;
    const v = entry as Record<string, unknown>;
    const route = text(v.rt)?.trim();
    if (!route || !STREETCAR_SNAPSHOT_ROUTES.has(route)) continue;
    let lat = numeric(v.lat), lon = numeric(v.lon);
    if (lat !== null && Math.abs(lat) > 90) lat = null;
    if (lon !== null && Math.abs(lon) > 180) lon = null;
    if (lat === 0 && lon === 0) { lat = null; lon = null; }
    const heading = numeric(v.hdg), speed = numeric(v.spd);
    const providerTimestamp = text(v.tmstmp);
    snapshots.push({
      snapshot_id: `${receiptId}:${index}`, receipt_id: receiptId,
      feed_event_id: receipt.feed_event_id || null, received_at: receivedAt,
      provider_timestamp: providerTimestamp, provider_observed_at: instant(providerTimestamp),
      vid: identifier(v.vid), route, legacy_trip_id: identifier(v.tatripid), gtfs_trip_id: identifier(v.tripid),
      destination: text(v.des), lat, lon,
      heading: heading !== null && heading >= 0 && heading < 360 ? heading : null,
      speed: speed !== null && speed >= 0 ? speed : null,
      is_delayed: typeof v.dly === 'boolean' ? v.dly : null,
      is_off_route: typeof v.or === 'boolean' ? v.or : null,
      pdist: numeric(v.pdist), pid: text(v.pid), rid: text(v.rid), tablockid: text(v.tablockid),
      srvtmstmp: text(v.srvtmstmp), source_url: source, raw_payload: JSON.stringify(v),
    });
  }
  return snapshots;
}

const COLUMNS = [
  'snapshot_id', 'receipt_id', 'feed_event_id', 'received_at', 'provider_timestamp', 'provider_observed_at',
  'vid', 'route', 'legacy_trip_id', 'gtfs_trip_id', 'destination', 'lat', 'lon', 'heading', 'speed',
  'is_delayed', 'is_off_route', 'pdist', 'pid', 'rid', 'tablockid', 'srvtmstmp', 'source_url', 'raw_payload',
] as const;
function table(database?: string): string {
  return `${database ? `"${database.replace(/"/g, '""')}".` : ''}streetcar_snapshots`;
}
export async function initializeStreetcarSnapshots(connection: DuckDBConnection, database?: string): Promise<void> {
  await connection.run(`CREATE TABLE IF NOT EXISTS ${table(database)} (
    snapshot_id VARCHAR PRIMARY KEY, receipt_id VARCHAR NOT NULL, feed_event_id VARCHAR,
    received_at TIMESTAMPTZ NOT NULL, provider_timestamp VARCHAR, provider_observed_at TIMESTAMPTZ,
    vid VARCHAR, route VARCHAR NOT NULL, legacy_trip_id VARCHAR, gtfs_trip_id VARCHAR, destination VARCHAR,
    lat DOUBLE, lon DOUBLE, heading DOUBLE, speed DOUBLE, is_delayed BOOLEAN, is_off_route BOOLEAN,
    pdist DOUBLE, pid VARCHAR, rid VARCHAR, tablockid VARCHAR, srvtmstmp VARCHAR,
    source_url VARCHAR, raw_payload VARCHAR NOT NULL, ingested_at TIMESTAMPTZ DEFAULT now()
  )`);
}
export async function persistStreetcarSnapshots(connection: DuckDBConnection, records: StreetcarSnapshot[], database?: string): Promise<void> {
  for (let start = 0; start < records.length; start += 500) {
    const batch = records.slice(start, start + 500);
    const placeholders = batch.map(() => `(${COLUMNS.map(() => '?').join(',')})`).join(',');
    const values = batch.flatMap(record => COLUMNS.map(column => record[column]));
    // Stable receipt IDs make retrying even a partly committed batch safe.
    // Separate receipts, including identical stationary payloads, retain new IDs.
    await connection.run(`INSERT INTO ${table(database)} (${COLUMNS.join(',')}) VALUES ${placeholders} ON CONFLICT(snapshot_id) DO NOTHING`, values);
  }
}

/** Bounded pending queue with one in-flight drain and oldest-first retry. */
export class SnapshotRetryBuffer {
  private pending: StreetcarSnapshot[] = [];
  private inFlight: Promise<number> | undefined;
  constructor(private readonly maximum: number, private readonly dropped: (count: number) => void = () => {}) {
    if (!Number.isSafeInteger(maximum) || maximum < 1) throw new Error('Invalid snapshot buffer limit');
  }
  get size(): number { return this.pending.length; }
  add(records: StreetcarSnapshot[]): void {
    for (const record of records) this.pending.push(record);
    this.cap();
  }
  private cap(): void {
    const overflow = this.pending.length - this.maximum;
    if (overflow > 0) { this.pending.splice(0, overflow); this.dropped(overflow); }
  }
  async flush(write: (records: StreetcarSnapshot[]) => Promise<void>): Promise<number> {
    if (this.inFlight) return this.inFlight;
    if (!this.pending.length) return 0;
    const records = this.pending;
    this.pending = [];
    const attempt = (async () => {
      try { await write(records); return records.length; }
      catch (error) { this.pending = [...records, ...this.pending]; this.cap(); throw error; }
    })();
    this.inFlight = attempt;
    try { return await attempt; }
    finally { if (this.inFlight === attempt) this.inFlight = undefined; }
  }
}
