import assert from 'node:assert/strict';
import test from 'node:test';
import { DuckDBInstance } from '@duckdb/node-api';
import { prepareDerivedStage, replaceDerivedDay } from './derived-replacement';
import { initializeLegacyBackfillState, pendingLegacyStudyDays, finishLegacyStudyDay, failedLegacyStudyDays } from './local-backfill-state';

test('staging replaces reused study IDs, removes obsolete keys and leaves other dates unchanged', async () => {
  const db=await DuckDBInstance.create(':memory:'),c=await db.connect();
  try {
    await c.run('CREATE TABLE study_events(id VARCHAR PRIMARY KEY,date DATE,body JSON)');
    await c.run(`INSERT INTO study_events VALUES ('keep','2026-09-08','{"n":1}'),('obsolete','2026-09-08','{"n":2}'),('other','2026-09-07','{"n":3}')`);
    for(let run=0;run<2;run++) {
      await c.run('BEGIN');
      try {
        const stage=await prepareDerivedStage(c,'study_events');
        await c.run(`INSERT INTO ${stage} VALUES ('keep','2026-09-08','{"n":4}'),('new','2026-09-08','{"n":5}')`);
        await replaceDerivedDay(c,{table:'study_events',stage,date_column:'date',date:'2026-09-08',keys:['id']});
        await c.run('COMMIT');
      }catch(e){await c.run('ROLLBACK');throw e;}
    }
    assert.deepEqual((await c.runAndReadAll('SELECT id,body::VARCHAR AS body FROM study_events ORDER BY id')).getRowObjectsJS(),
      [{id:'keep',body:'{"n":4}'},{id:'new',body:'{"n":5}'},{id:'other',body:'{"n":3}'}]);
  } finally {c.closeSync();db.closeSync();}
});
test('duplicate, null, wrong-date and cross-date identities fail without silently dropping or moving evidence',async()=>{
  const db=await DuckDBInstance.create(':memory:'),c=await db.connect();
  try{
    await c.run('CREATE TABLE t(id VARCHAR PRIMARY KEY,date DATE,value INTEGER)');
    await c.run("INSERT INTO t VALUES ('same','2026-09-08',1),('other','2026-09-07',2)");
    const before=(await c.runAndReadAll('SELECT * FROM t ORDER BY id')).getRowObjectsJS();
    for(const values of ["('same','2026-09-08',3),('same','2026-09-08',3)","(NULL,'2026-09-08',3)","('same','2026-09-07',3)","('other','2026-09-08',3)"]){
      await c.run('BEGIN');
      try{
        const stage=await prepareDerivedStage(c,'t');await c.run(`INSERT INTO ${stage} VALUES ${values}`);
        await assert.rejects(replaceDerivedDay(c,{table:'t',stage,date_column:'date',date:'2026-09-08',keys:['id']}));
      }finally{await c.run('ROLLBACK');}
      assert.deepEqual((await c.runAndReadAll('SELECT * FROM t ORDER BY id')).getRowObjectsJS(),before);
    }
  }finally{c.closeSync();db.closeSync();}
});
test('legacy backfill completion is independent, resumable, bounded on failures and invalidated by new source evidence',async()=>{
  const db=await DuckDBInstance.create(':memory:'),c=await db.connect();
  try{
    await c.run('CREATE TABLE study_dates(date DATE,source_revision VARCHAR)');
    await c.run("INSERT INTO study_dates VALUES ('2026-09-08','r1'),('2026-09-07','r1')");
    await initializeLegacyBackfillState(c);assert.equal((await pendingLegacyStudyDays(c,'m')).length,2);
    await finishLegacyStudyDay(c,'2026-09-08','r1','m');
    await finishLegacyStudyDay(c,'2026-09-07','r1','m','retryable failure');
    await initializeLegacyBackfillState(c);assert.deepEqual((await pendingLegacyStudyDays(c,'m')).map(d=>d.date),['2026-09-07']);
    await finishLegacyStudyDay(c,'2026-09-07','r1','m','retryable failure');await finishLegacyStudyDay(c,'2026-09-07','r1','m','retryable failure');
    assert.equal((await pendingLegacyStudyDays(c,'m')).length,0);assert.deepEqual(await failedLegacyStudyDays(c,'m'),['2026-09-07']);
    await c.run("UPDATE study_dates SET source_revision='r2' WHERE date='2026-09-07'");
    assert.equal((await pendingLegacyStudyDays(c,'m')).length,1);
    await finishLegacyStudyDay(c,'2026-09-07','r2','m');assert.equal((await pendingLegacyStudyDays(c,'m')).length,0);
    assert.deepEqual(await failedLegacyStudyDays(c,'m'),[]);
  }finally{c.closeSync();db.closeSync();}
});
