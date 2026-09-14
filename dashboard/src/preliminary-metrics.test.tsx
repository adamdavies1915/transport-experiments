import {test} from 'node:test';
import assert from 'node:assert/strict';
import {compareRowCells,summarizeSignalCells} from './transit-study-filter';
import {rowCell,signalCell} from './study-fixtures';
import type {StudyRowCell, StudyRowComparison} from '../../src/transit-study-types';

function assertNotReady(row:StudyRowComparison) {
  assert.equal(row.status,'insufficient_data');
  for(const field of ['reserved_seconds_per_km','shared_seconds_per_km','reserved_speed_mph','shared_speed_mph',
    'shared_extra_seconds_per_km','shared_extra_lower_seconds_per_km','shared_extra_upper_seconds_per_km',
    'shared_extra_ci_lower_seconds_per_km','shared_extra_ci_upper_seconds_per_km'] as const) assert.equal(row[field],null,field);
}

test('four matched dates expose descriptive rates with equal date weighting and preserve readiness',()=>{
  const cells=Array.from({length:4},(_,i)=>{
    const date=`2026-09-0${i+1}`,reservedKm=i===0?100:1,reservedRate=(i+1)*100,sharedRate=(5-i)*100;
    return [
      rowCell({date,passages:i===0?100:10,distance_meters:reservedKm*1000,duration_seconds:reservedRate*reservedKm,
        duration_lower_seconds:(reservedRate-20)*reservedKm,duration_upper_seconds:(reservedRate+20)*reservedKm}),
      rowCell({date,row_class:'shared',distance_meters:1000,duration_seconds:sharedRate,
        duration_lower_seconds:sharedRate-30,duration_upper_seconds:sharedRate+30}),
    ];
  }).flat();
  // This unpaired date must not influence the descriptive figure or its sample count.
  cells.push(rowCell({date:'2026-09-10',passages:10000,duration_seconds:1000000}));
  const [row]=compareRowCells(cells.reverse());
  assert.equal(row.matched_dates,4);
  assert.equal(row.reserved_passages,130);assert.equal(row.shared_passages,40);
  assert.deepEqual(row.observed,{dates:['2026-09-01','2026-09-02','2026-09-03','2026-09-04'],reserved_seconds_per_km:250,shared_seconds_per_km:350,shared_extra_seconds_per_km:100,
    shared_extra_lower_seconds_per_km:50,shared_extra_upper_seconds_per_km:150});
  assertNotReady(row);
});

test('unmatched service dates and one-class samples have no descriptive comparison',()=>{
  for(const cells of [[rowCell()], [rowCell(),rowCell({date:'2026-09-02',row_class:'shared'})]]) {
    const [row]=compareRowCells(cells);
    assert.equal(row.matched_dates,0);assert.equal(row.observed,undefined);assert.ok(!Object.hasOwn(row,'observed'));
    assertNotReady(row);
  }
  assert.deepEqual(compareRowCells([rowCell({row_class:'unknown'})]),[]);
  assert.equal(compareRowCells([rowCell(),rowCell({row_class:'shared',distance_meters:0})])[0].observed,undefined);
});

test('descriptive comparison cannot match a roadway class from another feed or incompatible stratum',()=>{
  const incompatible:Partial<StudyRowCell>[]=[
    {source:'lepass'}, {mode:'bus'}, {route_id:'47'}, {direction_id:'1'}, {day_type:'weekend'},
    {time_band:3}, {context:'signal_only'}, {signal_count:1}, {stop_count:1},
  ];
  for(const patch of incompatible) {
    const rows=compareRowCells([rowCell(),rowCell({row_class:'shared',...patch})]);
    assert.equal(rows.length,2,JSON.stringify(patch));
    for(const row of rows) {assert.equal(row.observed,undefined,JSON.stringify(patch));assertNotReady(row);}
  }
});

test('ready descriptive rates agree exactly with existing stable metrics and timing bounds',()=>{
  const cells=Array.from({length:7},(_,i)=>[
    rowCell({date:`2026-09-0${i+1}`,duration_seconds:200+i*10}),
    rowCell({date:`2026-09-0${i+1}`,row_class:'shared',duration_seconds:400+i*30,duration_lower_seconds:300,duration_upper_seconds:700}),
  ]).flat();
  const [row]=compareRowCells(cells);
  assert.equal(row.status,'ready');assert.ok(row.observed);
  for(const field of Object.keys(row.observed).filter(key=>key!=='dates') as Array<Exclude<keyof NonNullable<StudyRowComparison['observed']>,'dates'>>) {
    assert.equal(row.observed[field],row[field],field);
  }
  assert.equal(row.reserved_seconds_per_km,115);assert.equal(row.shared_seconds_per_km,245);
  assert.equal(row.shared_extra_seconds_per_km,130);
  assert.notEqual(row.shared_extra_ci_lower_seconds_per_km,null);
  assert.notEqual(row.shared_extra_ci_upper_seconds_per_km,null);
});

test('seven dates with fewer than thirty passages retain descriptive rates but no stable metrics or confidence interval',()=>{
  const cells=Array.from({length:7},(_,i)=>[
    rowCell({date:`2026-09-0${i+1}`,passages:1}),
    rowCell({date:`2026-09-0${i+1}`,passages:1,row_class:'shared',duration_seconds:400}),
  ]).flat();
  const [row]=compareRowCells(cells);
  assert.equal(row.matched_dates,7);assert.ok(row.observed);assertNotReady(row);
});

test('preliminary rates retain negative differences and timing uncertainty across zero',()=>{
  const [negative]=compareRowCells([
    rowCell({distance_meters:1000,duration_seconds:300,duration_lower_seconds:280,duration_upper_seconds:320}),
    rowCell({row_class:'shared',distance_meters:1000,duration_seconds:100,duration_lower_seconds:70,duration_upper_seconds:130}),
  ]);
  assert.deepEqual(negative.observed,{dates:['2026-09-01'],reserved_seconds_per_km:300,shared_seconds_per_km:100,shared_extra_seconds_per_km:-200,
    shared_extra_lower_seconds_per_km:-250,shared_extra_upper_seconds_per_km:-150});
  const [uncertain]=compareRowCells([
    rowCell({distance_meters:1000,duration_seconds:100,duration_lower_seconds:0,duration_upper_seconds:200}),
    rowCell({row_class:'shared',distance_meters:1000,duration_seconds:120,duration_lower_seconds:0,duration_upper_seconds:240}),
  ]);
  assert.equal(uncertain.observed?.shared_extra_seconds_per_km,20);
  assert.equal(uncertain.observed?.shared_extra_lower_seconds_per_km,-200);
  assert.equal(uncertain.observed?.shared_extra_upper_seconds_per_km,240);
  assertNotReady(negative);assertNotReady(uncertain);
});

test('signal date provenance follows each actual source and site sample',()=>{
  const summaries=summarizeSignalCells([
    signalCell({date:'2026-09-11'}),signalCell({date:'2026-09-08'}),signalCell({date:'2026-09-10'}),
    signalCell({date:'2026-08-01',source:'lepass'}),signalCell({date:'2026-09-13',site_id:'other-light'}),
  ]);
  const selected=summaries.find(row=>row.source==='sse'&&row.site_id==='light')!;
  assert.equal(selected.observed_from,'2026-09-08');assert.equal(selected.observed_to,'2026-09-11');
  assert.equal(selected.observed_dates,3);assert.equal(selected.status,'insufficient_data');
  const otherSource=summaries.find(row=>row.source==='lepass')!;
  assert.equal(otherSource.observed_from,'2026-08-01');assert.equal(otherSource.observed_to,'2026-08-01');
  assert.equal(summaries.find(row=>row.site_id==='other-light')!.observed_from,'2026-09-13');
  assert.deepEqual(summarizeSignalCells([]),[]);
});
