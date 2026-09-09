import test from 'node:test';
import assert from 'node:assert/strict';
import { mergeSourceQuality, PersistedSourceClocks, type SourceClocks } from './source-quality';
import type { SourceQualityData } from '../dashboard/src/summary-data';
import type { CollectionBatch, StudyObservation } from './observation-types';

const NOW = Date.parse('2026-09-08T07:15:00Z');
const iso = (offset:number) => new Date(NOW+offset).toISOString();
function batch(source:'sse'|'lepass',receipt:number,provider:number|null):CollectionBatch {
  const observation:StudyObservation={source,observation_id:`${source}:${receipt}`,vehicle_id:`${source}:1`,provider_vehicle_id:'1',route_id:'12',trip_id:null,observed_at:provider,received_at:receipt/1000,lat:29.9,lon:-90.1,speed_mph:null,off_route:false,location_source:'provider_gps',timestamp_precision_seconds:source==='sse'?60:.001,direction_id:'0',pattern_id:null,mapping_confidence:'verified'};
  return {schema_version:1,batch_id:observation.observation_id,source,received_at:new Date(receipt).toISOString(),observations:[{observation,raw:{}}]};
}
const saved:SourceQualityData={status:'ready',sources:[
  {id:'sse',label:'RTA relay',status:'ready',last_received_at:iso(-900000),last_provider_at:iso(-960000),observations:123,from:iso(-86400000),to:iso(-900000)},
  {id:'lepass',label:'LePass',status:'ready',last_received_at:iso(-900000),last_provider_at:iso(-910000),observations:45,from:iso(-86400000),to:iso(-900000)}]};
const clocks:SourceClocks={sse:{received_at:NOW-10000,provider_at:NOW-60000},lepass:{received_at:NOW-5000,provider_at:NOW-8000}};
const options={clocks,paused:false,stale_ms:300000,now:NOW,lepass:{status:'collecting' as const,reason:null}};

test('source clocks advance only after successful durable append and never use commit time as receipt time',async()=>{
  const clock=new PersistedSourceClocks();let release!:()=>void;
  const waiting=new Promise<void>(resolve=>{release=resolve;});
  const write=clock.persist(batch('sse',NOW-120000,(NOW-180000)/1000),()=>waiting);
  assert.deepEqual(clock.values.sse,{received_at:0,provider_at:0});
  release();await write;
  assert.deepEqual(clock.values.sse,{received_at:NOW-120000,provider_at:NOW-180000});
  await assert.rejects(clock.persist(batch('sse',NOW,NOW/1000),async()=>{throw new Error('Disk write failed');}));
  assert.deepEqual(clock.values.sse,{received_at:NOW-120000,provider_at:NOW-180000});
});

test('replayed or missing provider samples do not manufacture GPS freshness and sources remain separate',async()=>{
  const clock=new PersistedSourceClocks(),append=async()=>{};
  await clock.persist(batch('sse',NOW-120000,(NOW-180000)/1000),append);
  await clock.persist(batch('sse',NOW-60000,(NOW-240000)/1000),append);
  await clock.persist(batch('sse',NOW,null),append);
  assert.deepEqual(clock.values.sse,{received_at:NOW,provider_at:NOW-180000});
  assert.deepEqual(clock.values.lepass,{received_at:0,provider_at:0});
  await clock.persist(batch('lepass',NOW-5000,(NOW-8000)/1000),append);
  assert.deepEqual(clock.values.lepass,{received_at:NOW-5000,provider_at:NOW-8000});
  assert.equal(clock.values.sse.provider_at,NOW-180000);
});

test('fresh receipts cannot hide mapping revalidation and live clocks do not change snapshot inventory',()=>{
  const before=JSON.stringify(saved);
  const result=mergeSourceQuality(saved,{...options,lepass:{status:'degraded',reason:'mapping_revision_changed'}});
  assert.equal(result.status,'degraded');assert.equal(result.sources[0].status,'ready');
  const source=result.sources[1];assert.equal(source.status,'degraded');assert.match(source.message!,/requires.*revalidated/);assert.match(source.message!,/Raw responses are retained/);
  assert.equal(source.last_received_at,iso(-5000));assert.equal(source.last_provider_at,iso(-8000));
  assert.equal(source.observations,45);assert.equal(source.from,saved.sources[1].from);assert.equal(source.to,saved.sources[1].to);
  assert.equal(JSON.stringify(saved),before);
  const recovered=mergeSourceQuality(result,options);assert.equal(recovered.sources[1].status,'ready');assert.equal(recovered.sources[1].message,undefined);
});

test('stale sources, partial failures and paused collection remain distinct from current healthy receipts',()=>{
  const stale=mergeSourceQuality(saved,{...options,clocks:{...clocks,sse:{received_at:0,provider_at:0}}});
  assert.equal(stale.sources[0].status,'degraded');assert.equal(stale.sources[1].status,'ready');
  const partial=mergeSourceQuality(saved,{...options,lepass:{status:'degraded',reason:'some_queries_failing'}});
  assert.match(partial.sources[1].message!,/queries are failing/);
  const mappingAlso=mergeSourceQuality(saved,{...options,lepass:{status:'degraded',reason:'some_queries_failing',queries:[{lastError:'mapping_revision_changed'}]}});
  assert.match(mappingAlso.sources[1].message!,/revalidated/);
  const unknown=mergeSourceQuality(saved,{...options,lepass:{status:'degraded',reason:'private credential detail'}});
  assert.doesNotMatch(JSON.stringify(unknown),/private credential detail/);
  const paused=mergeSourceQuality(saved,{...options,paused:true});assert.ok(paused.sources.every(s=>s.status==='degraded'&&s.message?.includes('paused')));
  const replay=mergeSourceQuality(saved,{...options,clocks:{sse:{received_at:NOW-999999,provider_at:NOW-999999},lepass:{received_at:0,provider_at:0}}});
  assert.equal(replay.sources[0].last_received_at,saved.sources[0].last_received_at);assert.equal(replay.sources[0].last_provider_at,saved.sources[0].last_provider_at);
});
