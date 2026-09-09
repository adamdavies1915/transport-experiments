import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { collectCompactStudyDays, compactStudyPublication, serializeBoundedStudyPublication, writeBoundedStudyPublication, type PublicationDay } from './local-publication';
import { rowCell, rowData, signalCell, signalData } from '../dashboard/src/study-fixtures';
import { rowStudyFromCells, signalStudyFromCells } from '../dashboard/src/transit-study-filter';
import type { TransitSummaryEnvelope } from '../dashboard/src/summary-data';

const dates=['2026-09-01','2026-09-02','2026-09-03'];
function daily(date:string):PublicationDay {
  return {row_cells:Array.from({length:20},(_,i)=>rowCell({date,path_id:`${date}:${i}`,source:i%2?'sse':'lepass',row_class:i%3?'unknown':'reserved',passages:i+1,run_ids:Array.from({length:30},(_,j)=>`identity:${i}:${j}`)})),
    signal_cells:[signalCell({date,site_id:`light:${date}`,encounters:13,evaluable_encounters:0,wait_seconds:0,wait_events:0,detected_wait_encounters:0})],
    quality:[{date,source:'sse',raw_observations:100,usable_observations:50,completed_passages:210,complete_encounters:13,excluded:{}}]};
}
function snapshot():TransitSummaryEnvelope {
  const days=dates.map(daily),quality=days.flatMap(d=>d.quality);
  const compact=compactStudyPublication({...rowData(days.flatMap(d=>d.row_cells)),quality},{...signalData(days.flatMap(d=>d.signal_cells)),quality});
  return {schema_version:1,generated_at:'2026-09-03T20:00:00Z',row_study:rowStudyFromCells(compact.row,{}),signal_study:signalStudyFromCells(compact.signal,{}),legacy:{errors:{note:'Retained legacy metrics with multibyte text: é 路 🚋'}}};
}
test('sequential daily compaction equals full-history compaction and releases identity lists before retention',async()=>{
  const calls:string[]=[],full=dates.map(daily);
  const result=await collectCompactStudyDays([...dates].reverse(),async date=>{calls.push(date);return daily(date);},rowData(),signalData());
  assert.deepEqual(calls,[...dates].reverse());
  const actual=compactStudyPublication({...rowData(result.row_cells),coverage_cells:result.coverage_cells,quality:result.quality},{...signalData(result.signal_cells),quality:result.quality});
  const expected=compactStudyPublication({...rowData(full.flatMap(d=>d.row_cells)),quality:full.flatMap(d=>d.quality)},{...signalData(full.flatMap(d=>d.signal_cells)),quality:full.flatMap(d=>d.quality)});
  assert.deepEqual(actual,expected);
  assert.ok(result.row_cells.every(r=>r.run_ids===undefined&&r.window_ids===undefined));
  assert.ok(result.signal_cells.every(s=>s.run_ids===undefined));
});
test('compact accumulation stops at the first older date exceeding its budget without loading the rest',async()=>{
  const first=await collectCompactStudyDays([dates[2]],async date=>daily(date),rowData(),signalData());
  const cost=Buffer.byteLength(JSON.stringify({row_cells:first.row_cells,coverage_cells:first.coverage_cells,signal_cells:first.signal_cells,quality:first.quality}));
  const calls:string[]=[];
  const result=await collectCompactStudyDays(dates,async date=>{calls.push(date);return daily(date);},rowData(),signalData(),cost+1);
  assert.deepEqual(calls,[dates[2],dates[1]]);assert.deepEqual(result.retained_dates,[dates[2]]);
  assert.ok(result.row_cells.every(c=>c.date===dates[2]));
});
test('under-budget serialization is byte-for-byte unchanged, including multibyte UTF-8',()=>{
  const data=snapshot(),before=JSON.stringify(data),result=serializeBoundedStudyPublication(data);
  assert.equal(result.body,before);assert.equal(result.bytes,Buffer.byteLength(before));assert.equal(result.omitted_dates,0);
  assert.equal(JSON.stringify(data),before);
});
test('byte and date ceilings keep whole newest dates with exact filtered coverage and insufficient signal denominators',()=>{
  const data=snapshot(),before=JSON.stringify(data);
  const latest=serializeBoundedStudyPublication(data,{max_dates:1});
  const bounded=serializeBoundedStudyPublication(data,{max_bytes:latest.bytes});
  assert.deepEqual(bounded.retained_dates,[dates[2]]);assert.equal(bounded.omitted_dates,2);
  const parsed=JSON.parse(bounded.body) as TransitSummaryEnvelope;
  assert.equal(parsed.row_study!.from,dates[2]);assert.equal(parsed.row_study!.to,dates[2]);
  assert.ok(parsed.row_study!.limitations.some(s=>s.includes('2 older dates')&&s.includes(dates[0])));
  assert.deepEqual(parsed.legacy,data.legacy);
  for(const filters of [{},{source:'sse' as const},{source:'lepass' as const},{mode:'streetcar' as const},{hour_from:7,hour_to:9},{direction_id:'1'}]) {
    const expected=rowStudyFromCells(data.row_study!,{...filters,from:dates[2],to:dates[2]});
    assert.deepEqual(rowStudyFromCells(parsed.row_study!,filters).coverage,expected.coverage);
  }
  assert.equal(parsed.signal_study!.cells.reduce((n,s)=>n+s.encounters,0),13);
  assert.equal(parsed.signal_study!.signals[0].detected_wait_seconds_per_encounter,null);
  assert.deepEqual(parsed.row_study!.quality.map(q=>q.date),[dates[2]]);
  assert.equal(JSON.stringify(data),before);
});
test('stream-pruned and zero-event dates remain explicit in the published window',()=>{
  const data=snapshot();
  const lastOnly={...data,row_study:rowStudyFromCells(data.row_study!,{from:dates[2]}),signal_study:signalStudyFromCells(data.signal_study!,{from:dates[2]})};
  const result=serializeBoundedStudyPublication(lastOnly,{available_dates:[...dates,'2026-09-04'],included_dates:[dates[2],'2026-09-04']});
  assert.equal(result.omitted_dates,2);assert.deepEqual(result.retained_dates,[dates[2],'2026-09-04']);
  assert.equal(JSON.parse(result.body).row_study.to,'2026-09-04');
});
test('oversized newest date or fixed envelope fails before changing the previous snapshot',async()=>{
  const directory=await mkdtemp(join(tmpdir(),'study-publication-')),path=join(directory,'summary.json');
  try {
    await writeFile(path,'previous valid snapshot');
    const data=snapshot(),latest=serializeBoundedStudyPublication(data,{max_dates:1});
    await assert.rejects(writeBoundedStudyPublication(path,data,{max_bytes:latest.bytes-1}),/previous snapshot retained/);
    assert.equal(await readFile(path,'utf8'),'previous valid snapshot');
    await assert.rejects(writeBoundedStudyPublication(path,{schema_version:1,generated_at:'now',legacy:data.legacy},{max_bytes:1}),/no study dates/);
    assert.equal(await readFile(path,'utf8'),'previous valid snapshot');
    await writeBoundedStudyPublication(path,data);
    assert.equal(await readFile(path,'utf8'),JSON.stringify(data));
  } finally {await rm(directory,{recursive:true,force:true});}
});
