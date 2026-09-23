import 'dotenv/config';
import { readFile } from 'node:fs/promises';

// Journaling remains independent of the finite daily database writer.
for (const name of ['LEPASS_API_KEY', 'LEPASS_ENCRYPTION_KEY', 'TRANSIT_SUMMARY_TOKEN']) {
  const path = process.env[`${name}_FILE`];
  if (path) process.env[name] = (await readFile(path, 'utf8')).trim();
}
process.env.COLLECTOR_MODE = 'local';
process.env.LOCAL_ANALYSIS_ENABLED = 'false';
process.env.MOTHERDUCK_BOOTSTRAP = 'false';
process.env.MOTHERDUCK_CLOUD_WRITES = 'false';
process.env.PROCESSING_SERVER_ENABLED = 'false';
process.env.LEPASS_ALLOW_GUEST_BOOTSTRAP = 'false';
await (await import('../src/collector-entry')).launchCollector('local');
