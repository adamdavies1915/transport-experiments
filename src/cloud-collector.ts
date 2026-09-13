import 'dotenv/config';
import EventSource from 'eventsource';
import { createServer } from 'node:http';
import { initMotherDuck, insertRecords, insertStreetcarSnapshotRecords, closeMotherDuck } from './motherduck';
import { processVehicle } from './vehicle';
import { captureStreetcarSnapshots, SnapshotRetryBuffer, snapshotSourceUrl } from './streetcar-snapshots';
import type { RawVehicle, TransitRecord } from './types';
import { safeError } from './log-safety';
import { createSummaryTransferHandler } from './summary-transfer';

// Compatibility mode retains the deployed collector's observation and retry
// semantics. It has no analysis-worker imports, local database, or disk journal.
const LEGACY_SNAPSHOT_ROUTES = new Set(['12', '46', '47', '48']);
export interface CloudCollectorDependencies {
  initialize: typeof initMotherDuck;
  insertRecords: typeof insertRecords;
  insertSnapshots: typeof insertStreetcarSnapshotRecords;
  close: typeof closeMotherDuck;
  createEventSource: (url: string) => EventSource;
  setInterval: typeof setInterval;
  setTimeout: typeof setTimeout;
}
const defaults: CloudCollectorDependencies = {
  initialize: initMotherDuck, insertRecords, insertSnapshots: insertStreetcarSnapshotRecords,
  close: closeMotherDuck, createEventSource: url => new EventSource(url), setInterval, setTimeout,
};

export function createCloudCollector(
  overrides: Partial<CloudCollectorDependencies> = {}, environment: NodeJS.ProcessEnv = process.env,
) {
  const dependencies = { ...defaults, ...overrides };
  const SSE_URL = environment.SSE_URL || 'https://nolatransit.fly.dev/sse';
  const UPLOAD_INTERVAL = parseInt(environment.UPLOAD_INTERVAL ?? '') || 60000; // 1 minute (MotherDuck handles batching)
  const RECONNECT_DELAY = parseInt(environment.RECONNECT_DELAY ?? '') || 5000;
  // Cap on the retry buffer; when exceeded we drop the OLDEST records (Task 3).
  const MAX_BUFFER_RECORDS = parseInt(environment.MAX_BUFFER_RECORDS ?? '') || 500000;
  const MAX_SNAPSHOT_BUFFER_RECORDS = Math.max(1, parseInt(environment.MAX_SNAPSHOT_BUFFER_RECORDS ?? '') || 100000);
  // If no SSE message arrives within this window, force a reconnect (Task 3).
  const STALE_FEED_THRESHOLD = parseInt(environment.STALE_FEED_THRESHOLD ?? '') || 300000; // 5 min
  // When the MotherDuck token is absent we run without writing (local smoke test).
  const DRY_RUN = !environment.MOTHER_DUCK_API_KEY;

  let buffer: TransitRecord[] = [];
  let currentES: EventSource | undefined;
  let lastMessageAt = Date.now();
  let sampleLogged = false;
  let shuttingDown = false;
  let started = false;
  let lastReceivedAt: string | null = null;
  let lastPersistedAt: string | null = null;
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
      lastReceivedAt = receivedAt;

      // This additive ledger retains every target vehicle in every received
      // frame, including moved coordinates within one provider minute and
      // repeated stationary frames. The existing OTP/raw watermark stays below.
      snapshotBuffer.add(captureStreetcarSnapshots(vehicles, {
        received_at: receivedAt, source_url: SSE_URL, feed_event_id: feedEventId,
      }, LEGACY_SNAPSHOT_ROUTES));

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
      console.error('Error processing message:', safeError(err));
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
      await dependencies.insertRecords(toInsert);
      lastPersistedAt = new Date().toISOString();
      stats.uploadsCompleted++;
      stats.vehiclesBuffered += toInsert.length;
    } catch (err) {
      stats.errors++;
      console.error('Insert failed:', safeError(err));
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
        else await dependencies.insertSnapshots(snapshots);
      });
      stats.snapshotsPersisted += count;
      if (count && !DRY_RUN) lastPersistedAt = new Date().toISOString();
    } catch (err) {
      stats.errors++;
      console.error('Snapshot insert failed; retained for retry:', safeError(err));
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

    const es = dependencies.createEventSource(SSE_URL);
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
        reconnectTimer = dependencies.setTimeout(connectSSE, RECONNECT_DELAY);
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
    console.log(`\nReceived ${signal}. Shutting down gracefully...`);

    // Insert any remaining buffered data
    try {
      // Await any timer drain, then flush rows received while it was in flight.
      // A failed drain restores its rows, so this also makes one final retry.
      await uploadBuffer();
      if (buffer.length || snapshotBuffer.size) await uploadBuffer();
    } catch (err) { console.error('Error inserting buffers on shutdown:', safeError(err)); }
    if (buffer.length || snapshotBuffer.size) console.error(`[DROP] Shutdown left ${buffer.length} raw records and ${snapshotBuffer.size} snapshots unpersisted in memory.`);

    await dependencies.close();
    logStats();
  }

  async function start(): Promise<void> {
    if (started || shuttingDown) throw new Error('Cloud collector can only be started once');
    console.log('NOLA Transit Scraper: cloud collection only; analysis workers disabled');
    console.log(`SSE URL: ${snapshotSourceUrl(SSE_URL) ?? '(configured feed)'}`);
    console.log(`Insert interval: ${UPLOAD_INTERVAL / 1000}s`);
    console.log(`Max buffer: ${MAX_BUFFER_RECORDS} | Stale threshold: ${STALE_FEED_THRESHOLD / 1000}s`);
    console.log(`Streetcar snapshot retry cap: ${MAX_SNAPSHOT_BUFFER_RECORDS} | received_at records receipt, not GPS fix time`);

    if (DRY_RUN) {
      console.warn('MOTHER_DUCK_API_KEY not set — running in DRY RUN mode (inserts are stubbed).');
    } else {
      // Initialize MotherDuck
      try {
        await dependencies.initialize();
      } catch (err) {
        console.error('Failed to initialize MotherDuck:', safeError(err));
        throw err;
      }
    }

    started = true;
    connectSSE();

    // Insert buffer periodically
    uploadTimer = dependencies.setInterval(() => { void uploadBuffer().catch(err => console.error('Unexpected upload failure:', safeError(err))); }, UPLOAD_INTERVAL);

    // Log stats every 60 seconds
    statsTimer = dependencies.setInterval(logStats, 60000);

    // Feed freshness watchdog
    freshnessTimer = dependencies.setInterval(checkFeedFreshness, Math.min(STALE_FEED_THRESHOLD, 60000));

  }

  return {
    start, shutdown, flush: uploadBuffer,
    health: () => ({
      status: shuttingDown ? 'stopping' : !started ? 'starting' : DRY_RUN ? 'dry_run' : 'ready',
      mode: 'cloud', analysis: { enabled: false, running: false },
      connected: currentES?.readyState === EventSource.OPEN,
      last_received_at: lastReceivedAt,
      last_persisted_at: lastPersistedAt,
      pending_records: buffer.length, pending_snapshots: snapshotBuffer.size,
      stats: { ...stats, startTime: stats.startTime.toISOString() },
    }),
  };
}

/** Cloud deployments retain only the bounded latest workstation summary on disk. */
export async function runCloudCollector(): Promise<void> {
  const collector = createCloudCollector();
  const summaryTransfer = createSummaryTransferHandler({
    directory: process.env.TRANSIT_DATA_DIR || './runtime-data',
    readToken: process.env.TRANSIT_SUMMARY_TOKEN || undefined,
    publishToken: process.env.TRANSIT_SUMMARY_PUBLISH_TOKEN || undefined,
  });
  const server = createServer(async (req, res) => {
    try {
      res.setHeader('Content-Type', 'application/json');
      res.setHeader('Cache-Control', 'no-store');
      if (await summaryTransfer(req, res)) return;
      if (req.method !== 'GET' || !['/health', '/api/health'].includes(req.url || '')) {
        res.statusCode = 404; res.end('{"error":"Not found"}'); return;
      }
      const health = collector.health();
      res.statusCode = health.status === 'ready' ? 200 : 503;
      res.end(JSON.stringify(health));
    } catch (error) {
      console.error('[Cloud collector HTTP]', safeError(error));
      if (!res.headersSent) {
        res.statusCode = 503; res.end('{"error":"Collector endpoint temporarily unavailable"}');
      } else res.destroy();
    }
  });
  try {
    await collector.start();
    await new Promise<void>((resolve, reject) => {
      server.once('error', reject);
      server.listen(Number(process.env.PORT) || 3000, '0.0.0.0', () => {
        server.off('error', reject); resolve();
      });
    });
  } catch (error) {
    server.close();
    await collector.shutdown('startup failure');
    throw error;
  }
  let stopping = false;
  const shutdown = async (signal: string) => {
    if (stopping) return; stopping = true;
    server.close();
    await collector.shutdown(signal);
  };
  for (const signal of ['SIGTERM', 'SIGINT'] as const) {
    process.once(signal, () => { void shutdown(signal).catch(error => {
      console.error('[Cloud collector shutdown]', safeError(error)); process.exitCode = 1;
    }); });
  }
}
