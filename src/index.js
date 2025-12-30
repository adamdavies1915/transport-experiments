import EventSource from 'eventsource';
import { writeParquetToR2 } from './storage.js';
import { findSegment } from './segments.js';

const SSE_URL = process.env.SSE_URL || 'https://nolatransit.fly.dev/sse';
const UPLOAD_INTERVAL = parseInt(process.env.UPLOAD_INTERVAL) || 3600000; // 1 hour default
const RECONNECT_DELAY = parseInt(process.env.RECONNECT_DELAY) || 5000;

let buffer = [];
let stats = {
  messagesReceived: 0,
  vehiclesBuffered: 0,
  uploadsCompleted: 0,
  errors: 0,
  startTime: new Date(),
};

function logStats() {
  const uptime = Math.round((Date.now() - stats.startTime.getTime()) / 1000);
  console.log(`[Stats] Uptime: ${uptime}s | Messages: ${stats.messagesReceived} | Buffered: ${buffer.length} | Uploads: ${stats.uploadsCompleted} | Errors: ${stats.errors}`);
}

function processVehicle(v) {
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
    heading: parseInt(v.hdg) || 0,
    route: v.rt,
    trip_id: v.tatripid,
    destination: v.des || null,
    speed: parseInt(v.spd) || 0,
    is_delayed: v.dly === true,
    is_off_route: v.or === true,
    ...segment
  };
}

function processMessage(data) {
  try {
    const vehicles = JSON.parse(data);
    stats.messagesReceived++;

    for (const v of vehicles) {
      const record = processVehicle(v);
      if (record) {
        buffer.push(record);
      }
    }
  } catch (err) {
    stats.errors++;
    console.error('Error processing message:', err.message);
  }
}

async function uploadBuffer() {
  if (buffer.length === 0) {
    console.log('Buffer empty, skipping upload');
    return;
  }

  const toUpload = buffer;
  buffer = []; // Clear buffer immediately to avoid data loss

  try {
    await writeParquetToR2(toUpload);
    stats.uploadsCompleted++;
    stats.vehiclesBuffered += toUpload.length;
  } catch (err) {
    stats.errors++;
    console.error('Upload failed:', err.message);
    // Put records back in buffer to retry next time
    buffer = [...toUpload, ...buffer];
  }
}

function connectSSE() {
  console.log(`Connecting to SSE endpoint: ${SSE_URL}`);

  const es = new EventSource(SSE_URL);

  es.onopen = () => {
    console.log('SSE connection established');
  };

  es.onmessage = (event) => {
    processMessage(event.data);
  };

  es.onerror = (err) => {
    stats.errors++;
    console.error('SSE connection error:', err.message || 'Unknown error');

    if (es.readyState === EventSource.CLOSED) {
      console.log(`Reconnecting in ${RECONNECT_DELAY}ms...`);
      es.close();
      setTimeout(connectSSE, RECONNECT_DELAY);
    }
  };

  return es;
}

async function shutdown(signal) {
  console.log(`\nReceived ${signal}. Shutting down gracefully...`);

  // Upload any remaining buffered data
  if (buffer.length > 0) {
    console.log(`Uploading ${buffer.length} buffered records...`);
    try {
      await uploadBuffer();
    } catch (err) {
      console.error('Error uploading buffer on shutdown:', err.message);
    }
  }

  logStats();
  process.exit(0);
}

async function main() {
  console.log('NOLA Transit Scraper (R2 version) starting...');
  console.log(`SSE URL: ${SSE_URL}`);
  console.log(`Upload interval: ${UPLOAD_INTERVAL / 1000}s`);
  console.log(`R2 Bucket: ${process.env.R2_BUCKET || 'nola-transit'}`);

  // Validate R2 config
  if (!process.env.R2_ACCOUNT_ID || !process.env.R2_ACCESS_KEY_ID || !process.env.R2_SECRET_ACCESS_KEY) {
    console.error('Missing R2 credentials. Set R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY');
    process.exit(1);
  }

  connectSSE();

  // Upload buffer periodically
  setInterval(uploadBuffer, UPLOAD_INTERVAL);

  // Log stats every 60 seconds
  setInterval(logStats, 60000);

  // Handle graceful shutdown
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

main();
