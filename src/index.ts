import 'dotenv/config';
import EventSource from 'eventsource';
import { initMotherDuck, insertRecords, insertStreetcarSnapshotRecords, closeMotherDuck } from './motherduck';
import { processVehicle } from './vehicle';
import { captureStreetcarSnapshots, SnapshotRetryBuffer, snapshotSourceUrl } from './streetcar-snapshots';
import type { RawVehicle, TransitRecord } from './types';
import { startOtpWorker } from './otp-worker';
import { startStreetcarWorker } from './streetcar-worker';

const SSE_URL = process.env.SSE_URL || 'https://nolatransit.fly.dev/sse';
const UPLOAD_INTERVAL = parseInt(process.env.UPLOAD_INTERVAL ?? '') || 60000; // 1 minute (MotherDuck handles batching)
const RECONNECT_DELAY = parseInt(process.env.RECONNECT_DELAY ?? '') || 5000;
// Cap on the retry buffer; when exceeded we drop the OLDEST records (Task 3).
const MAX_BUFFER_RECORDS = parseInt(process.env.MAX_BUFFER_RECORDS ?? '') || 500000;
const MAX_SNAPSHOT_BUFFER_RECORDS = Math.max(1, parseInt(process.env.MAX_SNAPSHOT_BUFFER_RECORDS ?? '') || 100000);
// If no SSE message arrives within this window, force a reconnect (Task 3).
const STALE_FEED_THRESHOLD = parseInt(process.env.STALE_FEED_THRESHOLD ?? '') || 300000; // 5 min
// When the MotherDuck token is absent we run without writing (local smoke test).
const DRY_RUN = !process.env.MOTHER_DUCK_API_KEY;

let buffer: TransitRecord[] = [];
let currentES: EventSource | undefined;
let lastMessageAt = Date.now();
let sampleLogged = false;
let stopOtpWorker: (() => void) | undefined;
let stopStreetcarWorker: (() => void) | undefined;
let shuttingDown = false;
let uploadInFlight: Promise<void> | undefined;
let uploadTimer: ReturnType<typeof setInterval> | undefined;
let statsTimer: ReturnType<typeof setInterval> | undefined;
let freshnessTimer: ReturnType<typeof setInterval> | undefined;
let reconnectTimer: ReturnType<typeof setTimeout> | undefined;
// In-memory dedup: last-seen tmstmp per vehicle id (Task 2).
const lastSeenTmstmp = new Map<string, number>();
const stats = {
  messagesReceived: 0,
  vehiclesBuffered: 0,
  vehiclesDeduped: 0,
  uploadsCompleted: 0,
  recordsDropped: 0,
  snapshotsPersisted: 0,
  snapshotsDropped: 0,
  errors: 0,
  startTime: new Date(),
};
const snapshotBuffer = new SnapshotRetryBuffer(MAX_SNAPSHOT_BUFFER_RECORDS, count => {
  stats.snapshotsDropped += count;
  console.error(`[DROP] Snapshot retry buffer exceeded ${MAX_SNAPSHOT_BUFFER_RECORDS}; dropped ${count} oldest snapshot(s). Total dropped: ${stats.snapshotsDropped}`);
});

function logStats(): void {
  const uptime = Math.round((Date.now() - stats.startTime.getTime()) / 1000);
  console.log(`[Stats] Uptime: ${uptime}s | Messages: ${stats.messagesReceived} | Buffered: ${buffer.length} | Uploads: ${stats.uploadsCompleted} | Deduped: ${stats.vehiclesDeduped} | Dropped: ${stats.recordsDropped} | Errors: ${stats.errors}`);
  console.log(`[SnapshotStats] Pending: ${snapshotBuffer.size} | Persisted: ${stats.snapshotsPersisted} | Dropped: ${stats.snapshotsDropped}`);
}

// Enforce MAX_BUFFER_RECORDS by dropping the OLDEST records. Loud on purpose.
function enforceBufferCap(): void {
  if (buffer.length <= MAX_BUFFER_RECORDS) return;
  const overflow = buffer.length - MAX_BUFFER_RECORDS;
  buffer.splice(0, overflow);
  stats.recordsDropped += overflow;
  console.error(`[DROP] Retry buffer exceeded ${MAX_BUFFER_RECORDS}; dropped ${overflow} oldest record(s). Total dropped: ${stats.recordsDropped}`);
}

function processMessage(data: string, receivedAt: string, feedEventId?: string): void {
  if (shuttingDown) return;
  try {
    const vehicles = JSON.parse(data) as RawVehicle[];
    if (!Array.isArray(vehicles)) throw new Error('Expected an SSE vehicle array');
    stats.messagesReceived++;
    lastMessageAt = Date.now();

    // This additive ledger retains every target vehicle in every received
    // frame, including moved coordinates within one provider minute and
    // repeated stationary frames. The existing OTP/raw watermark stays below.
    snapshotBuffer.add(captureStreetcarSnapshots(vehicles, {
      received_at: receivedAt, source_url: SSE_URL, feed_event_id: feedEventId,
    }));

    for (const v of vehicles) {
      // Dedup: skip pings whose timestamp hasn't advanced for this vehicle.
      const prev = lastSeenTmstmp.get(v.vid);
      if (prev !== undefined && Date.parse(v.tmstmp) <= prev) {
        stats.vehiclesDeduped++;
        continue;
      }

      const record = processVehicle(v);
      if (record) {
        // Only advance the dedup watermark for pings we actually keep.
        lastSeenTmstmp.set(v.vid, Date.parse(v.tmstmp));
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

async function uploadLegacyBuffer(): Promise<void> {
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

async function uploadBuffers(): Promise<void> {
  await uploadLegacyBuffer();
  try {
    const count = await snapshotBuffer.flush(async snapshots => {
      if (DRY_RUN) console.log(`[DryRun] Would persist ${snapshots.length} streetcar SSE snapshots`);
      else await insertStreetcarSnapshotRecords(snapshots);
    });
    stats.snapshotsPersisted += count;
  } catch (err) {
    stats.errors++;
    console.error('Snapshot insert failed; retained for retry:', (err as Error).message);
  }
}

async function uploadBuffer(): Promise<void> {
  if (uploadInFlight) return uploadInFlight;
  const attempt = uploadBuffers();
  uploadInFlight = attempt;
  try { await attempt; }
  finally { if (uploadInFlight === attempt) uploadInFlight = undefined; }
}

function connectSSE(): EventSource | undefined {
  if (shuttingDown) return;
  clearTimeout(reconnectTimer);
  reconnectTimer = undefined;
  currentES?.close();
  console.log(`Connecting to SSE endpoint: ${snapshotSourceUrl(SSE_URL) ?? '(configured feed)'}`);

  const es = new EventSource(SSE_URL);
  currentES = es;

  es.onopen = () => {
    if (currentES !== es || shuttingDown) return;
    console.log('SSE connection established');
    lastMessageAt = Date.now(); // reset freshness clock on (re)connect
  };

  es.onmessage = (event: MessageEvent<string>) => {
    if (currentES !== es || shuttingDown) return;
    const receivedAt = new Date().toISOString();
    processMessage(event.data, receivedAt, event.lastEventId);
  };

  es.onerror = (err) => {
    if (currentES !== es || shuttingDown) return;
    stats.errors++;
    console.error('SSE connection error:', (err as { message?: string }).message || 'Unknown error');

    if (es.readyState === EventSource.CLOSED) {
      console.log(`Reconnecting in ${RECONNECT_DELAY}ms...`);
      es.close();
      clearTimeout(reconnectTimer);
      reconnectTimer = setTimeout(connectSSE, RECONNECT_DELAY);
    }
  };

  return es;
}

// Freshness self-check: if the feed goes silent, log loudly and reconnect.
function checkFeedFreshness(): void {
  if (shuttingDown) return;
  const silentFor = Date.now() - lastMessageAt;
  if (silentFor < STALE_FEED_THRESHOLD) return;

  console.error(`[STALE FEED] No SSE message in ${Math.round(silentFor / 1000)}s (threshold ${STALE_FEED_THRESHOLD / 1000}s). Forcing reconnect.`);
  lastMessageAt = Date.now(); // avoid a reconnect storm before the new connection settles
  if (currentES) currentES.close();
  connectSSE();
}

async function shutdown(signal: string): Promise<void> {
  if (shuttingDown) return;
  shuttingDown = true;
  currentES?.close();
  clearTimeout(reconnectTimer);
  clearInterval(uploadTimer);
  clearInterval(statsTimer);
  clearInterval(freshnessTimer);
  stopOtpWorker?.();
  stopStreetcarWorker?.();
  console.log(`\nReceived ${signal}. Shutting down gracefully...`);

  // Insert any remaining buffered data
  try {
    // Await any timer drain, then flush rows received while it was in flight.
    // A failed drain restores its rows, so this also makes one final retry.
    await uploadBuffer();
    if (buffer.length || snapshotBuffer.size) await uploadBuffer();
  } catch (err) { console.error('Error inserting buffers on shutdown:', (err as Error).message); }
  if (buffer.length || snapshotBuffer.size) console.error(`[DROP] Shutdown left ${buffer.length} raw records and ${snapshotBuffer.size} snapshots unpersisted in memory.`);

  await closeMotherDuck();
  logStats();
  process.exit(0);
}

async function main(): Promise<void> {
  console.log('NOLA Transit Scraper (MotherDuck version) starting...');
  console.log(`SSE URL: ${snapshotSourceUrl(SSE_URL) ?? '(configured feed)'}`);
  console.log(`Insert interval: ${UPLOAD_INTERVAL / 1000}s`);
  console.log(`Max buffer: ${MAX_BUFFER_RECORDS} | Stale threshold: ${STALE_FEED_THRESHOLD / 1000}s`);
  console.log(`Streetcar snapshot retry cap: ${MAX_SNAPSHOT_BUFFER_RECORDS} | received_at records receipt, not GPS fix time`);

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
  if (!DRY_RUN) { stopOtpWorker = startOtpWorker(); stopStreetcarWorker = startStreetcarWorker(); }

  // Insert buffer periodically
  uploadTimer = setInterval(() => { void uploadBuffer().catch(err => console.error('Unexpected upload failure:', String(err))); }, UPLOAD_INTERVAL);

  // Log stats every 60 seconds
  statsTimer = setInterval(logStats, 60000);

  // Feed freshness watchdog
  freshnessTimer = setInterval(checkFeedFreshness, Math.min(STALE_FEED_THRESHOLD, 60000));

  // Handle graceful shutdown
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

main();
