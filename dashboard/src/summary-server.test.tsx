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
import {SummaryStore,parseSummary,summaryStaleMs} from '../server/summary-store';
import {createSummaryApp} from '../server/summary-server';
import type {SummaryProcessing,TransitSummaryEnvelope} from './summary-data';
import {rowData,rowCell,signalData,signalCell} from './study-fixtures';
import {buildLegacySummary} from './legacy-summary';
const envelope:TransitSummaryEnvelope={schema_version:1,generated_at:'2026-09-03T12:00:00Z',source_quality:{status:'ready',sources:[{id:'sse',label:'Independent SSE',status:'ready'}]},row_study:rowData([rowCell(),rowCell({source:'lepass'})]),signal_study:signalData([signalCell(),signalCell({source:'lepass'})]),legacy:{otp:{status:'not_ready',days:[]}}};
const processing:SummaryProcessing={mode:'daily',job_id:'daily-2026-09-02',service_date:'2026-09-02',input_cutoff:'2026-09-03T11:00:00Z',completed_at:'2026-09-03T13:00:00Z',worker_id:'desktop-worker',analysis_revision:'revision-1',manifest_sha256:'a'.repeat(64)};
test('daily provenance validates public fields and omits extra coordinator data',()=>{
  const parsed=parseSummary({...envelope,processing:{...processing,token:'private-token',local_path:'/private/coordinator',fence:3}});
  assert.deepEqual(parsed.processing,processing);assert.doesNotMatch(JSON.stringify(parsed),/private-token|local_path|fence/);
  assert.equal(parseSummary(envelope).processing,undefined);
  for(const patch of [{mode:'hourly'},{job_id:''},{worker_id:'private\nheader'},{analysis_revision:'x'.repeat(201)},{service_date:'2026-02-30'},{input_cutoff:'not a timestamp'},{completed_at:'2026-02-30T13:00:00Z'},{manifest_sha256:'wrong'}]){
    assert.throws(()=>parseSummary({...envelope,processing:{...processing,...patch}}),/processing metadata/);
  }
  assert.throws(()=>parseSummary({...envelope,processing:null}),/processing metadata/);
});
test('analysis freshness configuration retains the default and rejects invalid intervals',()=>{
  assert.equal(summaryStaleMs(undefined),35*60000);
  assert.equal(summaryStaleMs('129600000'),36*3600000);
  for(const value of ['', '0', '-1', 'NaN', 'Infinity', '0.5', '9007199254740992'])assert.throws(()=>summaryStaleMs(value),/positive integer/);
  assert.throws(()=>new SummaryStore({cacheFile:'/tmp/unused-dashboard-summary.json',staleMs:NaN}),/positive integer/);
});
test('daily analysis stays fresh for the configured window while live clocks remain separate',async()=>{
  const directory=await mkdtemp(join(tmpdir(),'dashboard-daily-summary-'));
  const analysisAt=Date.parse(envelope.generated_at);let now=analysisAt+24*3600000;
  const receiptAt=new Date(now-1000).toISOString(),providerAt=new Date(now-60000).toISOString();
  const daily:TransitSummaryEnvelope={...envelope,processing,source_quality:{status:'ready',sources:[{id:'sse',label:'Independent SSE',status:'ready',last_received_at:receiptAt,last_provider_at:providerAt,observations:45,from:'2026-09-02T00:00:00Z',to:processing.input_cutoff}]}};
  const cacheFile=join(directory,'summary.json');
  const store=new SummaryStore({url:'http://collector/internal/summary',token:'private-token',cacheFile,staleMs:summaryStaleMs('129600000'),now:()=>now,fetcher:async()=>Response.json(daily)});
  try{
    await store.refresh();assert.equal(store.status.stale,false);assert.deepEqual(store.status.processing,processing);
    const defaultWindow=new SummaryStore({cacheFile,now:()=>now});await defaultWindow.load();assert.equal(defaultWindow.status.stale,true);
    const restarted=new SummaryStore({cacheFile,staleMs:36*3600000,now:()=>now});await restarted.load();assert.equal(restarted.status.stale,false);assert.deepEqual(restarted.status.processing,processing);
    const app=createSummaryApp(store);
    const publicData=await new Promise<Record<string,unknown>>(resolve=>{
      const req=new IncomingMessage(new Socket());req.url='/api/source-quality';req.method='GET';req.headers={};const res=new ServerResponse(req);
      res.end=((chunk:unknown)=>{resolve(JSON.parse(String(chunk)));return res;}) as typeof res.end;app(req,res);
    });
    assert.deepEqual(publicData.sources,daily.source_quality!.sources);assert.deepEqual(publicData.snapshot,store.status);
    const html=renderToStaticMarkup(<SourceQuality data={{...daily.source_quality!,snapshot:store.status}}/>);
    assert.match(html,/last collection/);assert.match(html,/Last analysis/);assert.match(html,/Daily processing · service date 2026-09-02/);
    assert.match(html,/dateTime="2026-09-03T12:00:00Z"/);assert.match(html,/Inputs collected through/);assert.match(html,/Result accepted/);
    assert.match(html,/Saved analysis snapshot: 45 observation receipts/);assert.doesNotMatch(html,/older than expected|refresh is unavailable|private-token|desktop-worker/);
    now=analysisAt+36*3600000;assert.equal(store.status.stale,false);
    now++;assert.equal(store.status.stale,true);
    // Freshly fetching the same analysis cannot reset its age or degrade source health.
    await store.refresh();assert.equal(store.status.stale,true);assert.equal(store.snapshot?.source_quality?.sources[0].status,'ready');
    assert.equal(store.snapshot?.source_quality?.sources[0].last_received_at,receiptAt);
    const staleHtml=renderToStaticMarkup(<SourceQuality data={{...daily.source_quality!,snapshot:store.status}}/>);
    assert.match(staleHtml,/older than expected/);assert.match(staleHtml,/Independent SSE<\/strong><p[^>]*>ready · last collection/);
    assert.ok(staleHtml.indexOf('older than expected')<staleHtml.indexOf('<details'));
  }finally{await rm(directory,{recursive:true,force:true});}
});
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
    const row=await request(`/api/row-study`).then(response=>response.json());const rowCells=row.cells as Array<{source:string}>;assert.equal(rowCells.length,2);assert.deepEqual(rowCells.map(cell=>cell.source).sort(),['lepass','sse']);
    const comparisons=row.comparisons as Array<{source:string;matched_dates:number}>;
    assert.equal(comparisons.length,2);assert.ok(comparisons.every(comparison=>comparison.matched_dates===0));
    const sse=await request(`/api/row-study?source=sse`).then(response=>response.json());assert.deepEqual((sse.cells as Array<{source:string}>).map(cell=>cell.source),['sse']);
    const signal=await request(`/api/signal-study?source=lepass`).then(response=>response.json());const signalCells=signal.cells as Array<{source:string}>;assert.equal(signalCells.length,1);assert.equal(signalCells[0].source,'lepass');
    assert.equal((await request(`/api/row-study?source=all`)).status,400);assert.equal((await request(`/api/row-study?from=2026-08-01`)).status,400);
    assert.equal((await request(`/api/summary`)).status,503);assert.equal((await request(`/api/otp`)).status,200);assert.equal((await request(`/api/health`)).status,200);
    assert.equal((await request(`/api/readiness`)).status,503);
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
test('only successful studies cache briefly and ETags revalidate gzip and plain responses with current freshness',async()=>{
  const fixture={snapshot:{...envelope,row_study:{...envelope.row_study!,limitations:['A repeated public method note. '.repeat(1000)]}},status:{generated_at:envelope.generated_at,received_at:null,origin:'disk' as const,stale:false,refresh_error:null as string|null}};
  const app=createSummaryApp(fixture);
  const request=(url:string,headers:Record<string,string>={})=>new Promise<{status:number;body:Buffer;cache:unknown;etag:unknown;encoding:unknown;vary:unknown}>(resolve=>{
    const req=new IncomingMessage(new Socket());req.url=url;req.method='GET';req.headers=headers;
    const res=new ServerResponse(req);
    res.end=((chunk:unknown)=>{resolve({status:res.statusCode,body:chunk==null?Buffer.alloc(0):Buffer.isBuffer(chunk)?chunk:Buffer.from(String(chunk)),cache:res.getHeader('Cache-Control'),etag:res.getHeader('ETag'),encoding:res.getHeader('Content-Encoding'),vary:res.getHeader('Vary')});return res;}) as typeof res.end;
    app(req,res);
  });
  for(const url of ['/api/health','/api/source-quality','/api/row-study?source=all','/api/signal-study?hour_from=24','/api/summary']){
    const response=await request(url);assert.equal(response.cache,'no-store',url);
  }
  const signal=await request('/api/signal-study?mode=bus');assert.equal(signal.status,200);assert.equal(signal.cache,'private, max-age=60');
  for(const headers of [{},{'accept-encoding':'gzip'}] as Record<string,string>[]){
    const first=await request('/api/row-study?mode=streetcar',headers);
    assert.equal(first.status,200);assert.equal(first.cache,'private, max-age=60');assert.equal(typeof first.etag,'string');assert.equal(first.vary,'Accept-Encoding');
    const unchanged=await request('/api/row-study?mode=streetcar',{...headers,'if-none-match':String(first.etag)});
    assert.equal(unchanged.status,304);assert.equal(unchanged.body.length,0);assert.equal(unchanged.cache,'private, max-age=60');
    fixture.status.stale=true;fixture.status.refresh_error='The next update is pending.';
    const changed=await request('/api/row-study?mode=streetcar',{...headers,'if-none-match':String(first.etag)});
    assert.equal(changed.status,200);assert.notEqual(changed.etag,first.etag);
    const body=JSON.parse((changed.encoding==='gzip'?gunzipSync(changed.body):changed.body).toString());
    assert.equal(body.snapshot.stale,true);assert.equal(body.snapshot.generated_at,envelope.generated_at);assert.equal(body.snapshot.received_at,null);
    fixture.status.stale=false;fixture.status.refresh_error=null;
  }
});
test('source coverage keeps live clocks separate from the saved analysis inventory',()=>{
  const html=renderToStaticMarkup(<SourceQuality data={{status:'degraded',sources:[{id:'lepass',label:'LePass',status:'degraded',observations:45,from:'2026-09-01T00:00:00Z',to:'2026-09-08T07:00:00Z',last_received_at:'2026-09-08T07:15:00Z',last_provider_at:'2026-09-08T07:14:58Z',message:'Raw responses are retained; mappings require revalidation.'}]}}/>);
  assert.match(html,/Data coverage and collection details/);assert.match(html,/Latest provider timestamp/);assert.match(html,/Saved analysis snapshot: 45 observation receipts/);
  assert.match(html,/not live counters or the sample size of the selected study/);assert.match(html,/Raw responses are retained/);
  assert.doesNotMatch(html,/<details open/);
});
