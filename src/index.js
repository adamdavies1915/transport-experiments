import EventSource from 'eventsource';
import { initializeDatabase, insertVehiclePositions, pool } from './db.js';

const SSE_URL = process.env.SSE_URL || 'https://nolatransit.fly.dev/sse';
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 1;
const RECONNECT_DELAY = parseInt(process.env.RECONNECT_DELAY) || 5000;

let messageBuffer = [];
let stats = {
  messagesReceived: 0,
  vehiclesLogged: 0,
  errors: 0,
  startTime: new Date(),
};

function logStats() {
  const uptime = Math.round((Date.now() - stats.startTime.getTime()) / 1000);
  console.log(`[Stats] Uptime: ${uptime}s | Messages: ${stats.messagesReceived} | Vehicles logged: ${stats.vehiclesLogged} | Errors: ${stats.errors}`);
}

async function processMessage(data) {
  try {
    const vehicles = JSON.parse(data);
    stats.messagesReceived++;

    if (BATCH_SIZE <= 1) {
      await insertVehiclePositions(vehicles);
      stats.vehiclesLogged += vehicles.length;
    } else {
      messageBuffer.push(...vehicles);
      if (messageBuffer.length >= BATCH_SIZE) {
        await insertVehiclePositions(messageBuffer);
        stats.vehiclesLogged += messageBuffer.length;
        messageBuffer = [];
      }
    }
  } catch (err) {
    stats.errors++;
    console.error('Error processing message:', err.message);
  }
}

function connectSSE() {
  console.log(`Connecting to SSE endpoint: ${SSE_URL}`);

  const es = new EventSource(SSE_URL);

  es.onopen = () => {
    console.log('SSE connection established');
  };

  es.onmessage = async (event) => {
    await processMessage(event.data);
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

  // Flush any remaining buffered messages
  if (messageBuffer.length > 0) {
    console.log(`Flushing ${messageBuffer.length} buffered messages...`);
    try {
      await insertVehiclePositions(messageBuffer);
    } catch (err) {
      console.error('Error flushing buffer:', err.message);
    }
  }

  logStats();
  await pool.end();
  console.log('Database connection closed');
  process.exit(0);
}

async function main() {
  console.log('NOLA Transit Scraper starting...');
  console.log(`SSE URL: ${SSE_URL}`);
  console.log(`Batch size: ${BATCH_SIZE}`);

  try {
    await initializeDatabase();
  } catch (err) {
    console.error('Failed to initialize database:', err.message);
    process.exit(1);
  }

  connectSSE();

  // Log stats every 60 seconds
  setInterval(logStats, 60000);

  // Handle graceful shutdown
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

main();
