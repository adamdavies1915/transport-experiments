import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { gzipSync } from 'node:zlib';
import { DuckDBInstance } from '@duckdb/node-api';
import { CaptureExchange, CAPTURE_LIMITS } from './capture-exchange';
import { LocalJournal } from './local-journal';
import { initializeCaptureCloud, uploadCapture, cloudCaptureManifest, MotherDuckCaptureReader, captureStorageGuard } from './motherduck-capture';
import { ingestManifest } from './daily-processing';
import { openLocalStore, query } from './local-store';
import type { CollectionBatch } from './observation-types';

test('server bytes travel through MotherDuck, import idempotently, and reject corrupt downloads',async()=>{
  const root=await mkdtemp(join(tmpdir(),'md-capture-')),server=join(root,'server'),pc=join(root,'pc');
  const journal=new LocalJournal(server),exchange=new CaptureExchange(server),db=await DuckDBInstance.create(':memory:'),c=await db.connect();
  try {
    await c.run("ATTACH ':memory:' AS my_db");await initializeCaptureCloud(c,'my_db');
    await journal.init();await exchange.init();
    for(const [i,source] of ['sse','lepass'].entries()) {
      const batch:CollectionBatch={schema_version:1,batch_id:'batch-'+i,source:source as 'sse'|'lepass',received_at:'2026-09-22T12:00:00Z',observations:[],predictions:[{original_clock:'retained',source}]};
      await writeFile(join(journal.pending,i+'.json.gz'),gzipSync(JSON.stringify(batch)));
      await exchange.seal(journal);
    }
    const manifest=await exchange.manifest();
    const reader=async(kind:string,d:{id:string})=>readFile(join(server,'capture-exchange',kind==='bundle'?'bundles':'schedules',d.id+(kind==='bundle'?'.json.gz':'.zip')));
    const partial=await uploadCapture(c,'my_db',manifest,reader,manifest.bundles[0].bytes);
    assert.equal(partial.manifest?.bundle_count,1);
    const complete=await uploadCapture(c,'my_db',manifest,reader);
    assert.equal(complete.manifest?.bundle_count,2);assert.equal(complete.uploadedAssets,1);
    const retry=await uploadCapture(c,'my_db',manifest,async()=>{throw Error('Already committed assets must not need server bytes');});
    assert.equal(retry.uploadedAssets,0);
    const published=await cloudCaptureManifest(c,'my_db',manifest.capture_id);assert.ok(published);
    const downloader=new MotherDuckCaptureReader(c,'my_db',manifest.capture_id);
    await ingestManifest(pc,published,downloader,new AbortController().signal,()=>{});
    await ingestManifest(pc,published,downloader,new AbortController().signal,()=>{});
    const store=await openLocalStore(pc);
    try { assert.deepEqual(await query(store.c,'SELECT source,COUNT(*)::INTEGER AS n FROM collection_batches GROUP BY source ORDER BY source'),[{source:'lepass',n:1},{source:'sse',n:1}]); }
    finally {store.c.closeSync();store.db.closeSync();}
    await c.run("UPDATE my_db.transit_capture_assets SET payload='broken'::BLOB WHERE id='000000000001'");
    await assert.rejects(downloader.download('/bundles/000000000001',join(root,'corrupt.gz'),manifest.bundles[0],CAPTURE_LIMITS.bundle_bytes),/checksum/);
    await assert.rejects(uploadCapture(c,'my_db',partial.manifest!,reader),/regress|drops|changes/);
  } finally {c.closeSync();db.closeSync();await rm(root,{recursive:true,force:true});}
});

test('failed asset upload cannot advertise an incomplete MotherDuck manifest',async()=>{
  const root=await mkdtemp(join(tmpdir(),'md-capture-failure-')),db=await DuckDBInstance.create(':memory:'),c=await db.connect();
  try {
    await c.run("ATTACH ':memory:' AS my_db");await initializeCaptureCloud(c,'my_db');
    const journal=new LocalJournal(root),exchange=new CaptureExchange(root);await journal.init();await exchange.init();
    await writeFile(join(journal.pending,'frame.json.gz'),gzipSync(JSON.stringify({schema_version:1,batch_id:'one',source:'lepass',received_at:'2026-09-22T12:00:00Z',observations:[]})));
    await exchange.seal(journal);const manifest=await exchange.manifest();
    await assert.rejects(uploadCapture(c,'my_db',manifest,async()=>{throw Error('quota or connection failure');}),/quota/);
    assert.equal(await cloudCaptureManifest(c,'my_db',manifest.capture_id),null);
    assert.ok(await readFile(await exchange.bundlePath(manifest.bundles[0].id)));
    await assert.rejects(uploadCapture(c,'my_db',manifest,async()=>Buffer.from('bad')),/checksum/);
    assert.equal(await cloudCaptureManifest(c,'my_db',manifest.capture_id),null);
  } finally {c.closeSync();db.closeSync();await rm(root,{recursive:true,force:true});}
});

test('server live-storage guard rejects stale, unknown and over-budget accounting',async()=>{
  const db=await DuckDBInstance.create(':memory:'),c=await db.connect();
  try {
    await c.run('CREATE SCHEMA md_information_schema; CREATE TABLE md_information_schema.storage_info(active_bytes BIGINT,computed_ts TIMESTAMP)');
    await assert.rejects(captureStorageGuard(c),/unavailable/);
    await c.run('INSERT INTO md_information_schema.storage_info VALUES (1000,now())');
    assert.equal(await captureStorageGuard(c),1000);
    await c.run('UPDATE md_information_schema.storage_info SET active_bytes=8000000000');
    await assert.rejects(captureStorageGuard(c),/ceiling/);
    await c.run("UPDATE md_information_schema.storage_info SET active_bytes=1000,computed_ts=now()-INTERVAL 7 HOURS");
    await assert.rejects(captureStorageGuard(c),/stale/);
  } finally {c.closeSync();db.closeSync();}
});
