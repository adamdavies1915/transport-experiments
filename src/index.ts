import 'dotenv/config';
import EventSource from 'eventsource';
import { initMotherDuck, insertRecords, closeMotherDuck } from './motherduck';
import { findSegment } from './segments';
import type { RawVehicle, TransitRecord } from './types';

const SSE_URL = process.env.SSE_URL || 'https://nolatransit.fly.dev/sse';
const UPLOAD_INTERVAL = parseInt(process.env.UPLOAD_INTERVAL ?? '') || 60000; // 1 minute (MotherDuck handles batching)
const RECONNECT_DELAY = parseInt(process.env.RECONNECT_DELAY ?? '') || 5000;
// Cap on the retry buffer; when exceeded we drop the OLDEST records (Task 3).
const MAX_BUFFER_RECORDS = parseInt(process.env.MAX_BUFFER_RECORDS ?? '') || 500000;
// If no SSE message arrives within this window, force a reconnect (Task 3).
const STALE_FEED_THRESHOLD = parseInt(process.env.STALE_FEED_THRESHOLD ?? '') || 300000; // 5 min
// When the MotherDuck token is absent we run without writing (local smoke test).
const DRY_RUN = !process.env.MOTHER_DUCK_API_KEY;

let buffer: TransitRecord[] = [];
let currentES: EventSource | undefined;
let lastMessageAt = Date.now();
let sampleLogged = false;
// In-memory dedup: last-seen tmstmp per vehicle id (Task 2).
const lastSeenTmstmp = new Map<string, string>();
const stats = {
  messagesReceived: 0,
  vehiclesBuffered: 0,
  vehiclesDeduped: 0,
  uploadsCompleted: 0,
  recordsDropped: 0,
  errors: 0,
  startTime: new Date(),
};

// Parse an integer-ish SSE field, preserving null when absent/unparseable so
// we never coerce a missing value to a misleading 0.
function parseIntOrNull(value: string | undefined): number | null {
  if (value == null || value === '') return null;
  const n = parseInt(value, 10);
  return Number.isNaN(n) ? null : n;
}

function nullIfEmpty(value: string | undefined): string | null {
  return value == null || value === '' ? null : value;
}

function logStats(): void {
  const uptime = Math.round((Date.now() - stats.startTime.getTime()) / 1000);
  console.log(`[Stats] Uptime: ${uptime}s | Messages: ${stats.messagesReceived} | Buffered: ${buffer.length} | Uploads: ${stats.uploadsCompleted} | Deduped: ${stats.vehiclesDeduped} | Dropped: ${stats.recordsDropped} | Errors: ${stats.errors}`);
}

// Enforce MAX_BUFFER_RECORDS by dropping the OLDEST records. Loud on purpose.
function enforceBufferCap(): void {
  if (buffer.length <= MAX_BUFFER_RECORDS) return;
  const overflow = buffer.length - MAX_BUFFER_RECORDS;
  buffer.splice(0, overflow);
  stats.recordsDropped += overflow;
  console.error(`[DROP] Retry buffer exceeded ${MAX_BUFFER_RECORDS}; dropped ${overflow} oldest record(s). Total dropped: ${stats.recordsDropped}`);
}

function processVehicle(v: RawVehicle): TransitRecord | null {
  // Skip vehicles with invalid coordinates
  if (v.lat === '0' && v.lon === '0') return null;

  const lat = parseFloat(v.lat);
  const lon = parseFloat(v.lon);
  const segment = findSegment(v.rt, lat, lon);

  return {
    vid: v.vid,
    timestamp: v.tmstmp,
    lat,
    lon,
    heading: parseInt(v.hdg ?? '') || 0,
    route: v.rt,
    trip_id: v.tatripid ?? null,
    destination: v.des || null,
    speed: parseInt(v.spd ?? '') || 0,
    is_delayed: v.dly === true,
    is_off_route: v.or === true,
    pdist: parseIntOrNull(v.pdist),
    pid: parseIntOrNull(v.pid),
    rid: nullIfEmpty(v.rid),
    tablockid: nullIfEmpty(v.tablockid),
    srvtmstmp: nullIfEmpty(v.srvtmstmp),
    ...segment
  };
}

function processMessage(data: string): void {
  try {
    const vehicles = JSON.parse(data) as RawVehicle[];
    stats.messagesReceived++;
    lastMessageAt = Date.now();

    for (const v of vehicles) {
      // Dedup: skip pings whose timestamp hasn't advanced for this vehicle.
      const prev = lastSeenTmstmp.get(v.vid);
      if (prev !== undefined && v.tmstmp <= prev) {
        stats.vehiclesDeduped++;
        continue;
      }

      const record = processVehicle(v);
      if (record) {
        // Only advance the dedup watermark for pings we actually keep.
        lastSeenTmstmp.set(v.vid, v.tmstmp);
        if (!sampleLogged) {
          console.log('[Sample] First parsed record:', JSON.stringify(record));
          sampleLogged = true;
        }
        buffer.push(record);
      }
    }
    enforceBufferCap();
  } catch (err) {
    stats.errors++;
    console.error('Error processing message:', (err as Error).message);
  }
}

async function uploadBuffer(): Promise<void> {
  if (buffer.length === 0) {
    console.log('Buffer empty, skipping insert');
    return;
  }

  const toInsert = buffer;
  buffer = []; // Clear buffer immediately to avoid data loss

  if (DRY_RUN) {
    stats.uploadsCompleted++;
    stats.vehiclesBuffered += toInsert.length;
    console.log(`[DryRun] Would insert ${toInsert.length} records (MOTHER_DUCK_API_KEY unset)`);
    return;
  }

  try {
    await insertRecords(toInsert);
    stats.uploadsCompleted++;
    stats.vehiclesBuffered += toInsert.length;
  } catch (err) {
    stats.errors++;
    console.error('Insert failed:', (err as Error).message);
    // Put failed records back at the front to retry next time, then re-cap.
    buffer = [...toInsert, ...buffer];
    enforceBufferCap();
  }
}

function connectSSE(): EventSource {
  console.log(`Connecting to SSE endpoint: ${SSE_URL}`);

  const es = new EventSource(SSE_URL);
  currentES = es;

  es.onopen = () => {
    console.log('SSE connection established');
    lastMessageAt = Date.now(); // reset freshness clock on (re)connect
  };

  es.onmessage = (event: MessageEvent<string>) => {
    processMessage(event.data);
  };

  es.onerror = (err) => {
    stats.errors++;
    console.error('SSE connection error:', (err as { message?: string }).message || 'Unknown error');

    if (es.readyState === EventSource.CLOSED) {
      console.log(`Reconnecting in ${RECONNECT_DELAY}ms...`);
      es.close();
      setTimeout(connectSSE, RECONNECT_DELAY);
    }
  };

  return es;
}

// Freshness self-check: if the feed goes silent, log loudly and reconnect.
function checkFeedFreshness(): void {
  const silentFor = Date.now() - lastMessageAt;
  if (silentFor < STALE_FEED_THRESHOLD) return;

  console.error(`[STALE FEED] No SSE message in ${Math.round(silentFor / 1000)}s (threshold ${STALE_FEED_THRESHOLD / 1000}s). Forcing reconnect.`);
  lastMessageAt = Date.now(); // avoid a reconnect storm before the new connection settles
  if (currentES) currentES.close();
  connectSSE();
}

async function shutdown(signal: string): Promise<void> {
  console.log(`\nReceived ${signal}. Shutting down gracefully...`);

  // Insert any remaining buffered data
  if (buffer.length > 0) {
    console.log(`Inserting ${buffer.length} buffered records...`);
    try {
      await uploadBuffer();
    } catch (err) {
      console.error('Error inserting buffer on shutdown:', (err as Error).message);
    }
  }

  await closeMotherDuck();
  logStats();
  process.exit(0);
}

async function main(): Promise<void> {
  console.log('NOLA Transit Scraper (MotherDuck version) starting...');
  console.log(`SSE URL: ${SSE_URL}`);
  console.log(`Insert interval: ${UPLOAD_INTERVAL / 1000}s`);
  console.log(`Max buffer: ${MAX_BUFFER_RECORDS} | Stale threshold: ${STALE_FEED_THRESHOLD / 1000}s`);

  if (DRY_RUN) {
    console.warn('MOTHER_DUCK_API_KEY not set — running in DRY RUN mode (inserts are stubbed).');
  } else {
    // Initialize MotherDuck
    try {
      await initMotherDuck();
    } catch (err) {
      console.error('Failed to initialize MotherDuck:', (err as Error).message);
      process.exit(1);
    }
  }

  connectSSE();

  // Insert buffer periodically
  setInterval(uploadBuffer, UPLOAD_INTERVAL);

  // Log stats every 60 seconds
  setInterval(logStats, 60000);

  // Feed freshness watchdog
  setInterval(checkFeedFreshness, Math.min(STALE_FEED_THRESHOLD, 60000));

  // Handle graceful shutdown
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

main();
