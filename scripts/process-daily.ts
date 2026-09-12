import 'dotenv/config';
import { parseArgs } from 'node:util';
import { hostname } from 'node:os';
import { runDailyProcessing } from '../src/daily-processing';
import { createProcessingSeed, restoreProcessingSeed } from '../src/processing-seed';
import { safeError } from '../src/log-safety';

async function main(){
  const {values}=parseArgs({options:{'data-dir':{type:'string'},'server':{type:'string'},'worker-id':{type:'string'},
    'seed-create':{type:'string'},'seed-restore':{type:'string'},help:{type:'boolean'}},strict:true});
  if(values.help){console.log(`Usage: npm run process:daily -- --data-dir /persistent/workstation-data [--server https://collector.example]
       npm run process:daily -- --data-dir /stopped/database --seed-create /new/seed-package
       npm run process:daily -- --data-dir /new/workstation-data --seed-restore /verified/seed-package

The normal command attempts one daily job, then exits. Schedule it every 15 minutes
on both workstations; the server grants only one lease and catches up missed days.
Use TRANSIT_PROCESSING_TOKEN for the private transport. Each machine must use a
verified copy of the same seed and this code revision. No LePass/MotherDuck token
is needed. Set LOCAL_DB_THREADS, LOCAL_DB_MEMORY and NODE_OPTIONS for that machine.`);return;}
  const directory=values['data-dir']||process.env.TRANSIT_DATA_DIR;
  if(!directory)throw new Error('--data-dir or TRANSIT_DATA_DIR is required');
  if(values['seed-create']&&values['seed-restore'])throw new Error('Choose one seed operation');
  if(values['seed-create']||values['seed-restore']){
    const seed=values['seed-create']?await createProcessingSeed(directory,values['seed-create']):await restoreProcessingSeed(values['seed-restore']!,directory);
    console.log(JSON.stringify({baseline_id:seed.baseline_id,files:seed.files.length,bytes:seed.files.reduce((n,f)=>n+f.bytes,0)}));return;
  }
  const server=values.server||process.env.PROCESSING_SERVER_URL;
  if(!server)throw new Error('--server or PROCESSING_SERVER_URL is required');
  console.log(JSON.stringify(await runDailyProcessing({dataDirectory:directory,serverUrl:server,token:process.env.TRANSIT_PROCESSING_TOKEN||'',workerId:values['worker-id']||process.env.PROCESSING_WORKER_ID||hostname()})));
}
main().catch(error=>{console.error(safeError(error));process.exitCode=1;});
