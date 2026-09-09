import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, readdir, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { LocalJournal } from './local-journal';
import { openLocalStore, ingestBatch, query, loadObservations } from './local-store';
import { archiveDay, usageAllowsCloud } from './cloud-archive';
import type { CollectionBatch } from './observation-types';

function frame(id:string,received=1788840000):CollectionBatch {
  return {schema_version:1,batch_id:id,source:'sse',received_at:new Date(received*1000).toISOString(),observations:[{
    observation:{source:'sse',observation_id:id+':0',vehicle_id:'sse:460',provider_vehicle_id:'460',route_id:'12',trip_id:'trip',observed_at:1788840000,received_at:received,lat:29.95,lon:-90.1,speed_mph:null,off_route:false,location_source:'provider_gps',timestamp_precision_seconds:60,direction_id:null,pattern_id:null,mapping_confidence:'verified'},
    raw:{vid:'460',rt:'12',tripid:'trip',tmstmp:'2026-09-08T04:00:00Z',lat:'29.95',lon:'-90.1'},
  }]};
}
test('durable frames replay idempotently and payload compaction preserves every receipt and missing speed',async()=>{
  const dir=await mkdtemp(join(tmpdir(),'transit-storage-'));
  let store=await openLocalStore(dir);
  try {
    const journal=new LocalJournal(dir);await journal.init();
    for(const batch of [frame('a'),frame('b',1788840010),frame('c',1788840045)])await journal.append(batch);
    const paths=await journal.files();assert.equal(paths.length,3);
    for(const path of paths)await ingestBatch(store.c,await journal.read(path));
    store.c.closeSync();store.db.closeSync();store=await openLocalStore(dir);
    assert.equal(await ingestBatch(store.c,await journal.read(paths[0])),false,'ambiguous previous commit replay does not duplicate');
    assert.equal((await query<{n:number}>(store.c,'SELECT COUNT(*) n FROM collection_payloads'))[0].n,1);
    const retained=await loadObservations(store.c,'2026-09-07');
    assert.deepEqual(retained.map(o=>o.received_at),[1788840000,1788840010,1788840045]);
    assert.equal(retained[0].speed_mph,null);
    assert.equal((await query(store.c,'SELECT speed FROM transit_data'))[0].speed,null);
    assert.equal((await query<{n:number}>(store.c,'SELECT COUNT(*) n FROM transit_data'))[0].n,1);
    // Different coordinates within the same provider minute are unique evidence.
    const changed=frame('d',1788840050);changed.observations[0].raw.lon='-90.1001';changed.observations[0].observation.lon=-90.1001;
    await ingestBatch(store.c,changed);
    assert.equal((await query<{n:number}>(store.c,'SELECT COUNT(*) n FROM collection_payloads'))[0].n,2);
    await archiveDay(store.c,dir,'2026-09-07');
    const manifest=JSON.parse(await readFile(join(dir,'archives','2026-09-07','manifest.json'),'utf8'));
    assert.equal(manifest.rows,4);assert.match(manifest.sha256,/^[a-f0-9]{64}$/);
    const restored=await query<{n:number}>(store.c,`SELECT COUNT(*) n FROM read_parquet('${manifest.file}')`);
    assert.equal(restored[0].n,4);
    for(const path of paths)await journal.acknowledge(path);
    assert.equal((await journal.files()).length,0);
  }finally{store.c.closeSync();store.db.closeSync();await rm(dir,{recursive:true,force:true});}
});
test('budget refuses unknown, stale, over-budget, and wrong-month usage',()=>{
  const now=Date.parse('2026-09-08T04:00:00Z');
  const usage={measured_at:'2026-09-08T03:00:00Z',month:'2026-09',compute_cu_hours:2,plan:'lite' as const,billing_mode:'free' as const};
  assert.equal(usageAllowsCloud(usage,1_000_000_000,now),null);
  assert.match(usageAllowsCloud(undefined,0,now)!,/verified/);
  assert.match(usageAllowsCloud({...usage,month:'2026-08'},0,now)!,/verified/);
  assert.match(usageAllowsCloud({...usage,compute_cu_hours:8},0,now)!,/compute/);
  assert.match(usageAllowsCloud({...usage,measured_at:'2026-09-07T03:00:00Z'},0,now)!,/stale/);
  assert.match(usageAllowsCloud(usage,8_000_000_000,now)!,/storage/);
});
