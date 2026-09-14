import {test} from 'node:test';
import assert from 'node:assert/strict';
import {renderToStaticMarkup} from 'react-dom/server';
import {rowStudyFromCells,signalStudyFromCells,serviceDateBootstrap} from './transit-study-filter';
import {rowCell,rowData,signalCell,signalData} from './study-fixtures';
import StudyPanel from './StudyPanel';
import {compactStudyPublication} from '../../src/local-publication';
import type {StudyFilters} from '../../src/transit-study-types';

const pairs=['2026-09-01','2026-09-02','2026-09-03','2026-09-04','2026-09-07','2026-09-08','2026-09-09'].flatMap((date,index)=>[
  rowCell({date,passages:index===0?100:10,distance_meters:index===0?20000:2000,duration_seconds:index===0?2000:200,duration_lower_seconds:0,duration_upper_seconds:index===0?4000:400}),
  rowCell({date,row_class:'shared',duration_seconds:index===0?600:400,duration_lower_seconds:0,duration_upper_seconds:index===0?800:600})]);
test('ROW compares same-date matched strata with equal date weights and never pools sources',()=>{
  const data=rowData([...pairs,rowCell({source:'lepass',duration_seconds:999999}),rowCell({date:'2026-09-10',duration_seconds:999999}),rowCell({row_class:'unknown',passages:17})]);
  const result=rowStudyFromCells(data,{source:'sse'}),row=result.comparisons[0];
  assert.equal(result.comparisons.length,1);assert.equal(row.matched_dates,7);assert.equal(row.reserved_passages,160);
  assert.equal(row.reserved_seconds_per_km,100);assert.ok(Math.abs(row.shared_seconds_per_km!-1500/7)<1e-10);
  assert.ok(Math.abs(row.shared_extra_seconds_per_km!-800/7)<1e-10);assert.equal(row.shared_extra_lower_seconds_per_km,-200);
  assert.equal(result.coverage.unknown_passages,17);
  const sparse=rowStudyFromCells(data,{source:'sse',to:'2026-09-02'});assert.equal(sparse.comparisons[0].status,'insufficient_data');assert.equal(sparse.comparisons[0].shared_extra_seconds_per_km,null);
  assert.equal(rowStudyFromCells(data,{source:'lepass'}).comparisons[0].status,'insufficient_data');
});
test('compacted unknown ROW coverage preserves every public filter and never enters comparisons',()=>{
  const unknown = [
    rowCell({row_class:'unknown',passages:11}),
    rowCell({row_class:'unknown',path_id:'other-path',context:'both',passages:13}),
    rowCell({row_class:'unknown',source:'lepass',passages:17}),
    rowCell({row_class:'unknown',mode:'bus',route_id:'51',passages:19}),
    rowCell({row_class:'unknown',direction_id:'1',hour:17,time_band:4,passages:23}),
    rowCell({row_class:'unknown',date:'2026-09-06',day_type:'weekend',passages:29}),
  ];
  const full=rowData([...pairs,...unknown]);
  const compact=compactStudyPublication(full,signalData()).row;
  assert.equal(compact.coverage_cells?.length,5);
  assert.ok(compact.cells.every(cell=>cell.row_class!=='unknown'));
  const filters:StudyFilters[]=[{}, {source:'sse'}, {source:'lepass'}, {mode:'bus',route_id:'51'},
    {direction_id:'1'}, {day_type:'weekend'}, {hour_from:9,hour_to:18},
    {from:'2026-09-02',to:'2026-09-07'}, {source:'sse',mode:'streetcar',route_id:'12',direction_id:'0',day_type:'weekday',hour_from:8,hour_to:8}];
  for(const filter of filters){
    const expected=rowStudyFromCells(full,filter),actual=rowStudyFromCells(compact,filter);
    assert.deepEqual(actual.coverage,expected.coverage,JSON.stringify(filter));
    assert.deepEqual(actual.comparisons,expected.comparisons,JSON.stringify(filter));
    assert.equal(actual.status,expected.status);
  }
  const mixed={...compact,cells:[...compact.cells,rowCell({row_class:'unknown',passages:31})]};
  assert.equal(rowStudyFromCells(mixed,{}).coverage.unknown_passages,compact.coverage.unknown_passages+31);
});
test('signal probabilities include unsampled complete encounters; mean is per wait event; overlap has no priority scenario',()=>{
  const result=signalStudyFromCells(signalData([signalCell(),signalCell({source:'lepass',wait_seconds:9999}),signalCell({context:'both'})]),{source:'sse'});
  assert.equal(result.signals.length,2);const isolated=result.signals.find(row=>row.context==='signal_only')!;
  assert.equal(isolated.detected_wait_probability,0.2);assert.equal(isolated.mean_detected_wait_seconds,30);assert.equal(isolated.detected_wait_seconds_per_encounter,9);
  assert.equal(isolated.evaluable_encounters,6);assert.deepEqual(isolated.recovery_seconds_per_encounter.map(row=>row.seconds),[2.25,4.5,6.75]);
  assert.ok(result.signals.find(row=>row.context==='both')!.recovery_seconds_per_encounter.every(row=>row.seconds===null));
  const noWait=signalStudyFromCells(signalData([signalCell({wait_events:0,wait_seconds:0,detected_wait_encounters:0})]),{}).signals[0];assert.equal(noWait.mean_detected_wait_seconds,null);assert.equal(noWait.detected_wait_seconds_per_encounter,0);
});
test('new panels expose evidence gaps without fabricating a zero-cost result',()=>{
  const row=renderToStaticMarkup(<StudyPanel kind="row" data={rowData([rowCell({row_class:'unknown'})])}/>);
  assert.match(row,/Collecting both feeds roadway comparisons/);assert.match(row,/Unknown roadway/);assert.doesNotMatch(row,/Shared minus reserved|0\.0 min/);
  const signal=renderToStaticMarkup(<StudyPanel kind="signals" data={signalData([signalCell({context:'both'})])}/>);
  assert.match(signal,/4 encounters had insufficient sampling/);assert.match(signal,/Boarding and signal delay cannot be separated/);assert.doesNotMatch(signal,/25% recovered/);
});
test('ROW detail shows timing range and does not mix direction or interpret a difference as causal',()=>{
  const html=renderToStaticMarkup(<StudyPanel kind="row" data={rowData(pairs)}/>);
  assert.match(html,/Time to travel one kilometre/);assert.match(html,/08:00–11:59/);assert.match(html,/Timing range for the difference/);assert.match(html,/does not isolate the causal effect/);
  const filtered=renderToStaticMarkup(<StudyPanel kind="row" data={rowData(pairs)} initialFilters={{source:'lepass'}}/>);assert.match(filtered,/Collecting Le Pass roadway comparisons/);assert.doesNotMatch(filtered,/Shared minus reserved/);
});

test('an entirely unevaluable signal sample cannot appear as measured zero delay',()=>{
  const data=signalData([signalCell({evaluable_encounters:0,detected_wait_encounters:0,wait_events:0,wait_seconds:0})]);
  assert.equal(signalStudyFromCells(data,{}).status,'collecting');
  const html=renderToStaticMarkup(<StudyPanel kind="signals" data={data}/>);
  assert.match(html,/No encounter had enough sampling/);
  assert.doesNotMatch(html,/25% recovered|0\.0 min/);
});


test('service-date bootstrap measures day variation independently of GPS clock bounds',()=>{
  const days=Array.from({length:7},(_,i)=>({numerator:i*10,denominator:1}));
  const ci=serviceDateBootstrap(days)!;
  assert.ok(ci[0]<30&&ci[1]>30); assert.deepEqual(serviceDateBootstrap(days),ci);
  assert.equal(serviceDateBootstrap(days.slice(1)),null);
  const result=rowStudyFromCells(rowData(pairs),{}).comparisons[0];
  assert.ok(result.shared_extra_ci_lower_seconds_per_km!>0);
  assert.ok(result.shared_extra_lower_seconds_per_km!<0);
});
test('signal readiness requires thirty evaluable encounters across seven dates per directional site',()=>{
  const days=Array.from({length:7},(_,i)=>signalCell({date:`2026-09-${String(i+1).padStart(2,'0')}`,wait_seconds:i*30}));
  const ready=signalStudyFromCells(signalData(days),{});
  assert.equal(ready.status,'ready');assert.equal(ready.signals[0].evaluable_dates,7);
  assert.ok(ready.signals[0].detected_wait_seconds_per_encounter_ci_upper!>ready.signals[0].detected_wait_seconds_per_encounter_ci_lower!);
  assert.equal(signalStudyFromCells(signalData(days.slice(1)),{}).status,'collecting');
  assert.equal(signalStudyFromCells(signalData(days.map(d=>({...d,evaluable_encounters:4}))),{}).status,'collecting');
  assert.equal(signalStudyFromCells(signalData(days.map(d=>({...d,evaluable_encounters:0,wait_events:0,wait_seconds:0,detected_wait_encounters:0}))),{}).signals[0].detected_wait_seconds_per_encounter,null);
});

test('signal figures appear provisionally while priority scenarios and service-date intervals wait for readiness',()=>{
  const sparse=renderToStaticMarkup(<StudyPanel kind="signals" data={signalData([signalCell()])}/>);
  const lead=sparse.match(/aria-label="Selected signal evidence"([\s\S]*?)<details/)?.[1]??'';
  assert.match(lead,/Preliminary observations/);assert.match(lead,/Detected wait per encounter/);assert.match(lead,/>9 sec<\/p>/);
  assert.match(lead,/Mean per detected wait/);assert.match(lead,/>30 sec<\/p>/);
  assert.match(lead,/2 of 10 complete encounters had a detected wait \(20%\)/);
  assert.match(lead,/6 of 10 encounters had enough sampling/);assert.match(lead,/Observed 2026-09-01 to 2026-09-01/);
  assert.match(sparse,/<td[^>]*>9 sec<span[^>]*>Preliminary<\/span>/);
  assert.doesNotMatch(sparse,/25% recovered|95% interval across service dates/);
  const cells=Array.from({length:7},(_,i)=>signalCell({date:`2026-09-${String(i+1).padStart(2,'0')}`,wait_seconds:i*30}));
  const ready=renderToStaticMarkup(<StudyPanel kind="signals" data={signalData(cells)}/>);
  assert.match(ready,/25% recovered/);assert.match(ready,/95% interval across service dates/);assert.doesNotMatch(ready,/Collecting a headline sample/);
  assert.match(ready,/Illustrative seconds saved per encounter/);assert.doesNotMatch(ready,/\d(?:\.\d+)? min(?:<|[ .,])/);
  const subsecond=renderToStaticMarkup(<StudyPanel kind="signals" data={signalData([signalCell({wait_seconds:3.28})])}/>);
  assert.match(subsecond,/>0\.3 sec<\/p>/);assert.match(subsecond,/>1 sec<\/p>/);
  assert.match(subsecond,/<td[^>]*>0\.3 sec<span[^>]*>Preliminary<\/span>/);
  const repeatedSubsecond=renderToStaticMarkup(<StudyPanel kind="signals" data={signalData(cells.map(cell=>({...cell,wait_seconds:3.28})))}/>);
  assert.match(repeatedSubsecond,/95% interval across service dates: 0\.3 sec to 0\.3 sec/);
  assert.match(repeatedSubsecond,/25% recovered<\/p><p[^>]*>0\.1 sec<\/p>/);
  assert.doesNotMatch(repeatedSubsecond,/\d(?:\.\d+)? min(?:<|[ .,])/);
});


test('both-feed roadway view selects a whole supported comparison and keeps coverage within its source',()=>{
  const lepass=Array.from({length:8},(_,i)=>`2026-09-${String(i+1).padStart(2,'0')}`).flatMap(date=>[
    rowCell({date,source:'lepass'}),rowCell({date,source:'lepass',row_class:'shared',duration_seconds:400}),
  ]);
  const html=renderToStaticMarkup(<StudyPanel kind="row" data={rowData([
    ...pairs,...lepass,rowCell({row_class:'unknown',passages:10000}),rowCell({source:'lepass',row_class:'unknown',passages:37}),
  ])}/>);
  const lead=html.match(/aria-label="Selected roadway evidence"([\s\S]*?)<details/)?.[1]??'';
  const coverage=html.match(/aria-label="Roadway observation coverage"([\s\S]*?)<\/details>/)?.[1]??'';
  assert.match(lead,/Le Pass observations/);
  assert.match(coverage,/Le Pass coverage/);
  assert.match(coverage,/>197<\/p>/);assert.match(coverage,/>37<\/p>/);
  assert.doesNotMatch(coverage,/10,000|10,230|10,427/);
  assert.match(html,/Explore 1 matched comparisons/);
  assert.equal((html.match(/<tbody><tr/g)??[]).length,1);
  assert.match(html,/Both feeds · one feed per result/);
  assert.match(html,/aria-expanded="false"[^>]*>Advanced filters/);
  assert.doesNotMatch(html,/aria-label="Observation source"/);
});

test('both-feed signal view leads with supported evidence rather than the largest observed delay',()=>{
  const sse=Array.from({length:7},(_,i)=>signalCell({date:`2026-09-${String(i+1).padStart(2,'0')}`,wait_seconds:10000}));
  const lepass=Array.from({length:8},(_,i)=>signalCell({date:`2026-09-${String(i+1).padStart(2,'0')}`,source:'lepass',wait_seconds:120}));
  const html=renderToStaticMarkup(<StudyPanel kind="signals" data={signalData([...sse,...lepass])}/>);
  const lead=html.match(/aria-label="Selected signal evidence"([\s\S]*?)<details/)?.[1]??'';
  assert.match(lead,/Le Pass observations/);assert.match(lead,/>12 sec<\/p>/);
  assert.match(lead,/48 of 80 encounters/);assert.doesNotMatch(lead,/150 encounters|1,000 sec/);
  assert.match(html,/Explore 1 site results/);
  assert.equal((html.match(/<tbody><tr/g)??[]).length,1);
  const explicit=renderToStaticMarkup(<StudyPanel kind="signals" data={signalData([...sse,...lepass])} initialFilters={{source:'sse'}}/>);
  assert.match(explicit,/SSE observations/);assert.match(explicit,/>1,000 sec<\/p>/);
  assert.doesNotMatch(explicit,/Le Pass observations/);
});

test('two sparse feeds cannot make a ready signal estimate or erase stop overlap',()=>{
  const cells=(['sse','lepass'] as const).flatMap(source=>Array.from({length:4},(_,i)=>signalCell({source,date:`2026-09-${String(i+1+(source==='lepass'?4:0)).padStart(2,'0')}`,context:'both',evaluable_encounters:10})));
  const html=renderToStaticMarkup(<StudyPanel kind="signals" data={signalData(cells)}/>);
  assert.match(html,/Preliminary observations/);assert.match(html,/40 of 40 encounters had enough sampling to evaluate waiting, across 4 dates/);
  assert.match(html,/Observed 2026-09-01 to 2026-09-04/);assert.doesNotMatch(html,/Observed 2026-09-01 to 2026-09-08|across 8 dates/);
  assert.match(html,/Explore 1 site results/);
  assert.match(html,/Boarding and signal delay cannot be separated/);
  assert.doesNotMatch(html,/25% recovered|What could signal priority save here/);
});

test('preliminary roadway detail exposes real paired figures with visible dates and timing uncertainty',()=>{
  const html=renderToStaticMarkup(<StudyPanel kind="row" data={rowData([
    ...pairs.slice(0,4),rowCell({date:'2026-09-10',passages:10000,duration_seconds:999999}),
    rowCell({source:'lepass',passages:10000}),rowCell({source:'lepass',row_class:'shared',passages:10000,duration_seconds:999999}),
  ])}/>);
  const lead=html.match(/aria-label="Selected roadway evidence"([\s\S]*?)<details/)?.[1]??'';
  assert.match(lead,/Preliminary observations/);assert.match(lead,/SSE observations/);
  assert.match(lead,/>1\.7 min<\/p>/);assert.match(lead,/>4\.2 min<\/p>/);assert.match(lead,/>2\.5 min<\/p>/);
  assert.match(lead,/110 reserved and 20 shared passages on 2 matched dates/);
  assert.match(lead,/2026-09-01 to 2026-09-02/);assert.doesNotMatch(lead,/Le Pass|10,000|2026-09-10/);
  assert.match(lead,/Timing uncertainty spans zero/);assert.match(lead,/cannot yet establish which roadway class is quicker/);
  assert.match(html,/<td[^>]*>2\.5 min<span[^>]*>Preliminary<\/span>/);
  assert.match(html,/Timing range for the difference/);assert.doesNotMatch(html,/95% interval across service dates/);
});

test('unpaired roadway dates or complementary feeds cannot produce a displayed roadway figure',()=>{
  for(const cells of [
    [rowCell(),rowCell({row_class:'shared',date:'2026-09-02'})],
    [rowCell(),rowCell({row_class:'shared',source:'lepass'})],
  ]) {
    const html=renderToStaticMarkup(<StudyPanel kind="row" data={rowData(cells)}/>);
    assert.match(html,/No comparable shared observation dates yet/);assert.match(html,/No matched dates/);
    assert.doesNotMatch(html,/Shared minus reserved|\d\.\d min|Preliminary observations|95% interval across service dates/);
  }
});

test('no detected signal waits display no detection rather than a fictional zero wait duration',()=>{
  const html=renderToStaticMarkup(<StudyPanel kind="signals" data={signalData([
    signalCell({wait_seconds:0,wait_events:0,detected_wait_encounters:0}),
  ])}/>);
  const lead=html.match(/aria-label="Selected signal evidence"([\s\S]*?)<details/)?.[1]??'';
  assert.match(lead,/No waits detected/);assert.match(lead,/0 of 10 complete encounters had a detected wait \(0%\)/);
  assert.match(lead,/6 of 10 encounters had enough sampling/);assert.match(lead,/Short waits can be missed/);
  assert.match(html,/<td[^>]*>No waits detected<span[^>]*>Preliminary<\/span>/);
  assert.doesNotMatch(html,/Mean per detected wait|0\.0 min|25% recovered|95% interval across service dates/);
});


test('signal exploration starts with ready evidence away from stops and retains overlapping sites for review',()=>{
  const isolated=Array.from({length:7},(_,i)=>signalCell({date:`2026-09-${String(i+1).padStart(2,'0')}`,wait_seconds:120}));
  const overlap=Array.from({length:8},(_,i)=>signalCell({date:`2026-09-${String(i+1).padStart(2,'0')}`,context:'both',wait_seconds:10000}));
  const html=renderToStaticMarkup(<StudyPanel kind="signals" data={signalData([...isolated,...overlap])}/>);
  const lead=html.match(/aria-label="Selected signal evidence"([\s\S]*?)<details/)?.[1]??'';
  assert.match(html,/Best-supported result away from passenger stops/);
  assert.match(lead,/>12 sec<\/p>/);
  assert.doesNotMatch(lead,/passenger stop overlaps/);
  assert.match(html,/Explore 2 site results/);assert.match(html,/Signal \+ passenger stop/);
  const onlyOverlap=renderToStaticMarkup(<StudyPanel kind="signals" data={signalData(overlap)}/>);
  assert.match(onlyOverlap,/Boarding and signal delay cannot be separated/);
  assert.doesNotMatch(onlyOverlap,/25% recovered|What could signal priority save here/);
});
