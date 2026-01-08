import 'dotenv/config';
import cron from 'node-cron';
import { spawn } from 'child_process';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __dirname = dirname(fileURLToPath(import.meta.url));

function runConsolidation() {
  console.log(`[${new Date().toISOString()}] Starting consolidation...`);

  const child = spawn('node', [join(__dirname, 'consolidate.js')], {
    stdio: 'inherit'
  });

  child.on('close', (code) => {
    console.log(`[${new Date().toISOString()}] Consolidation finished with code ${code}`);
  });
}

// Run at 2 AM UTC daily (after midnight data collection settles)
cron.schedule('0 2 * * *', () => {
  runConsolidation();
});

console.log('Consolidation scheduler started');
console.log('Scheduled to run daily at 2:00 AM UTC');

// Also run once on startup if CONSOLIDATE_ON_START is set
if (process.env.CONSOLIDATE_ON_START === 'true') {
  console.log('Running initial consolidation...');
  runConsolidation();
}

// Keep process alive
process.on('SIGTERM', () => {
  console.log('Received SIGTERM, shutting down...');
  process.exit(0);
});
