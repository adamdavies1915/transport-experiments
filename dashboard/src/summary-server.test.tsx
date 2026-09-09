import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,readFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {Socket} from 'node:net';
import {IncomingMessage,ServerResponse} from 'node:http';
import {gunzipSync} from 'node:zlib';
import {renderToStaticMarkup} from 'react-dom/server';
import SourceQuality from './SourceQuality';
import {SummaryStore,parseSummary} from '../server/summary-store';
import {createSummaryApp} from '../server/summary-server';
import type {TransitSummaryEnvelope} from './summary-data';
import {rowData,rowCell,signalData,signalCell} from './study-fixtures';
import {buildLegacySummary} from './legacy-summary';
const envelope:TransitSummaryEnvelope={schema_version:1,generated_at:'2026-09-03T12:00:00Z',source_quality:{status:'ready',sources:[{id:'sse',label:'Independent SSE',status:'ready'}]},row_study:rowData([rowCell(),rowCell({source:'lepass'})]),signal_study:signalData([signalCell(),signalCell({source:'lepass'})]),legacy:{otp:{status:'not_ready',days:[]}}};
test('private refresh is durable and failure or invalid data preserves the last good summary',async()=>{
  const directory=await mkdtemp(join(tmpdir(),'dashboard-summary-'));let calls=0;let mode='good';
  const store=new SummaryStore({url:'http://collector/internal/summary',token:'test-server-only-secret',cacheFile:join(directory,'summary.json'),now:()=>Date.parse(envelope.generated_at),fetcher:async(_url,options)=>{calls++;assert.equal((options?.headers as Record<string,string>).Authorization,'Bearer test-server-only-secret');return mode==='error'?new Response('Internal exception with private details',{status:500}):Response.json(mode==='invalid'?{schema_version:99}:envelope);}});
  try{
    await Promise.all([store.refresh(),store.refresh()]);assert.equal(calls,1);assert.equal(store.status.stale,false);
    const saved=await readFile(join(directory,'summary.json'),'utf8');assert.doesNotMatch(saved,/test-server-only-secret/);
    mode='error';await store.refresh();assert.deepEqual(store.snapshot,envelope);assert.equal(store.status.stale,true);assert.doesNotMatch(store.status.refresh_error!,/Internal exception/);
    mode='invalid';await store.refresh();assert.deepEqual(store.snapshot,envelope);
    const restarted=new SummaryStore({cacheFile:join(directory,'summary.json')});await restarted.load();assert.deepEqual(restarted.snapshot,envelope);assert.equal(restarted.status.origin,'disk');
    assert.equal('token' in parseSummary({...envelope,token:'must-not-publish'}),false);
  }finally{await rm(directory,{recursive:true,force:true});}
});
test('public routes use memory only and study/OTP availability is independent',async()=>{
  let networkCalls=0;const store=new SummaryStore({url:'http://unused',token:'secret',cacheFile:'/tmp/unused-dashboard-summary.json',fetcher:async()=>{networkCalls++;throw new Error('Must not fetch on request');}});
  // A fixture view of the store exercises the public app without contacting a collector.
  const fixture={get snapshot(){return envelope;},get status(){return store.status;}};
  const app=createSummaryApp(fixture);
  const request=(url:string)=>new Promise<{status:number;json:()=>Promise<Record<string,unknown>>}>(resolve=>{
    const req=new IncomingMessage(new Socket());req.url=url;req.method='GET';req.headers={};
    const res=new ServerResponse(req);
    res.end=((chunk:unknown)=>{resolve({status:res.statusCode,json:async()=>JSON.parse(String(chunk))});return res;}) as typeof res.end;
    app(req,res);
  });
  {
    const row=await request(`/api/row-study`).then(response=>response.json());const rowCells=row.cells as Array<{source:string}>;assert.equal(rowCells.length,1);assert.equal(rowCells[0].source,'sse');
    const signal=await request(`/api/signal-study?source=lepass`).then(response=>response.json());const signalCells=signal.cells as Array<{source:string}>;assert.equal(signalCells.length,1);assert.equal(signalCells[0].source,'lepass');
    assert.equal((await request(`/api/row-study?source=all`)).status,400);assert.equal((await request(`/api/row-study?from=2026-08-01`)).status,400);
    assert.equal((await request(`/api/summary`)).status,503);assert.equal((await request(`/api/otp`)).status,200);assert.equal((await request(`/api/health`)).status,200);
    assert.equal(networkCalls,0);
  }
});
test('legacy local-query failures do not suppress independent OTP results',async()=>{
  const result=await buildLegacySummary(async sql=>{if(sql.includes('otp_day_status'))return [];throw new Error('database password should never escape');},'local');
  assert.ok(result.errors?.summary);assert.doesNotMatch(JSON.stringify(result),/password/);
});
test('large saved JSON negotiates lossless gzip while health and clients declining gzip stay plain',async()=>{
  const fixture={snapshot:{...envelope,row_study:{...rowData(),limitations:['A repeated public method note. '.repeat(1000)]}},status:{generated_at:envelope.generated_at,received_at:null,origin:'disk' as const,stale:false,refresh_error:null}};
  const app=createSummaryApp(fixture);
  const request=(url:string,encoding?:string)=>new Promise<{body:Buffer;encoding:unknown;vary:unknown}>(resolve=>{
    const req=new IncomingMessage(new Socket());req.url=url;req.method='GET';req.headers=encoding?{'accept-encoding':encoding}:{};
    const res=new ServerResponse(req);
    res.end=((chunk:unknown)=>{resolve({body:Buffer.isBuffer(chunk)?chunk:Buffer.from(String(chunk)),encoding:res.getHeader('Content-Encoding'),vary:res.getHeader('Vary')});return res;}) as typeof res.end;
    app(req,res);
  });
  const plain=await request('/api/row-study');assert.equal(plain.encoding,undefined);
  const compressed=await request('/api/row-study','br, gzip');
  assert.equal(compressed.encoding,'gzip');assert.equal(compressed.vary,'Accept-Encoding');
  assert.deepEqual(gunzipSync(compressed.body),plain.body);assert.ok(compressed.body.length<plain.body.length/5);
  const declined=await request('/api/row-study','gzip;q=0, identity');assert.equal(declined.encoding,undefined);assert.deepEqual(declined.body,plain.body);
  const health=await request('/api/health','gzip');assert.equal(health.encoding,undefined);assert.equal(JSON.parse(health.body.toString()).status,'ok');
});
test('source coverage keeps live clocks separate from the saved analysis inventory',()=>{
  const html=renderToStaticMarkup(<SourceQuality data={{status:'degraded',sources:[{id:'lepass',label:'LePass',status:'degraded',observations:45,from:'2026-09-01T00:00:00Z',to:'2026-09-08T07:00:00Z',last_received_at:'2026-09-08T07:15:00Z',last_provider_at:'2026-09-08T07:14:58Z',message:'Raw responses are retained; mappings require revalidation.'}]}}/>);
  assert.match(html,/Data coverage and clocks/);assert.match(html,/Latest provider timestamp/);assert.match(html,/Saved analysis snapshot: 45 observation receipts/);
  assert.match(html,/not live counters or the sample size of the selected study/);assert.match(html,/Raw responses are retained/);
  assert.doesNotMatch(html,/<details open/);
});
