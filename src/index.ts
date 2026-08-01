import 'dotenv/config';
import EventSource from 'eventsource';
import { initMotherDuck, insertRecords, closeMotherDuck } from './motherduck';
import { findSegment } from './segments';
import type { RawVehicle, TransitRecord } from './types';

const SSE_URL = process.env.SSE_URL || 'https://nolatransit.fly.dev/sse';
const UPLOAD_INTERVAL = parseInt(process.env.UPLOAD_INTERVAL ?? '') || 60000; // 1 minute (MotherDuck handles batching)
const RECONNECT_DELAY = parseInt(process.env.RECONNECT_DELAY ?? '') || 5000;

let buffer: TransitRecord[] = [];
const stats = {
  messagesReceived: 0,
  vehiclesBuffered: 0,
  uploadsCompleted: 0,
  errors: 0,
  startTime: new Date(),
};

function logStats(): void {
  const uptime = Math.round((Date.now() - stats.startTime.getTime()) / 1000);
  console.log(`[Stats] Uptime: ${uptime}s | Messages: ${stats.messagesReceived} | Buffered: ${buffer.length} | Uploads: ${stats.uploadsCompleted} | Errors: ${stats.errors}`);
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
    ...segment
  };
}

function processMessage(data: string): void {
  try {
    const vehicles = JSON.parse(data) as RawVehicle[];
    stats.messagesReceived++;

    for (const v of vehicles) {
      const record = processVehicle(v);
      if (record) {
        buffer.push(record);
      }
    }
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

  try {
    await insertRecords(toInsert);
    stats.uploadsCompleted++;
    stats.vehiclesBuffered += toInsert.length;
  } catch (err) {
    stats.errors++;
    console.error('Insert failed:', (err as Error).message);
    // Put records back in buffer to retry next time
    buffer = [...toInsert, ...buffer];
  }
}

function connectSSE(): EventSource {
  console.log(`Connecting to SSE endpoint: ${SSE_URL}`);

  const es = new EventSource(SSE_URL);

  es.onopen = () => {
    console.log('SSE connection established');
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

  // Validate MotherDuck config
  if (!process.env.MOTHER_DUCK_API_KEY) {
    console.error('Missing MOTHER_DUCK_API_KEY environment variable');
    process.exit(1);
  }

  // Initialize MotherDuck
  try {
    await initMotherDuck();
  } catch (err) {
    console.error('Failed to initialize MotherDuck:', (err as Error).message);
    process.exit(1);
  }

  connectSSE();

  // Insert buffer periodically
  setInterval(uploadBuffer, UPLOAD_INTERVAL);

  // Log stats every 60 seconds
  setInterval(logStats, 60000);

  // Handle graceful shutdown
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

main();
