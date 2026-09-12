// Filename retained for existing deployment commands. This server has no database access.
import 'dotenv/config';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { SummaryStore, summaryStaleMs } from './server/summary-store';
import { createSummaryApp } from './server/summary-server';

const store=new SummaryStore({url:process.env.TRANSIT_SUMMARY_URL,token:process.env.TRANSIT_SUMMARY_TOKEN,
  cacheFile:process.env.TRANSIT_SUMMARY_CACHE_FILE??resolve('data/transit-summary.json'),
  staleMs:summaryStaleMs(process.env.TRANSIT_SUMMARY_STALE_MS)});
await store.load();
const app=createSummaryApp(store,fileURLToPath(new URL('./dist/',import.meta.url)));
const server=app.listen(Number(process.env.PORT??3000),'0.0.0.0',()=>{
  console.log('Dashboard listening; serving saved collector summaries.');store.start();
});
for(const signal of ['SIGINT','SIGTERM'] as const)process.once(signal,()=>{store.stop();server.close(()=>process.exit(0));});
