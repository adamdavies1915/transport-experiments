import {test} from 'node:test';
import assert from 'node:assert/strict';
import {priorityProfiles,priorityFilters,type PriorityStratum} from '../dashboard/src/priority-query';
import {priorityScenario} from '../dashboard/src/priority-data';
import type {StreetcarNetwork} from '../dashboard/src/streetcar-data';
const network:StreetcarNetwork={version:'fixture',generated_at:'today',schedule_hash:'test',sources:[],sites:[],mapillary_status:'test',
  corridors:[{id:'st_charles',name:'St Charles',routes:['12']}],paths:[
    {id:'a',corridor:'st_charles',route:'12',direction:'0',headsign:'North',points:[{lat:30,lon:-90},{lat:30.01,lon:-90}],stop_ids:[]},
    {id:'b',corridor:'st_charles',route:'12',direction:'1',headsign:'South',points:[{lat:30.01,lon:-90},{lat:30,lon:-90}],stop_ids:[]},
  ]};
function row(offset:number,category:PriorityStratum['category'],mean:number,p20:number,passages=30):PriorityStratum {
 return {path_id:'a',window_id:'a:'+offset,route:'12',direction:'0',from_meters:offset,to_meters:offset+200,
  category,signal_ids:category==='signal_only'||category==='both'?['s1','s2']:[],stop_ids:category==='both'?['stop']:[],
  days:['2026-09-01','2026-09-02','2026-09-03'],passages,mean_seconds:mean,p10:p20*.8,p20,p30:p20*1.2};
}
test('priority compares the same window within contexts and counts covered geography once',()=>{
 const profiles=priorityProfiles(network,'st_charles',[
  row(200,'signal_only',90,30),row(200,'signal_only',60,20,60),
  row(400,'both',100,50,40),row(600,'neither',80,80),row(800,'stop_only',100,50,3),
 ]);
 const p=profiles[0];
 assert.equal(p.eligible_windows,3);assert.equal(p.total_windows_observed,4);assert.equal(p.covered_meters,600);
 assert.equal(p.observed_seconds,250);assert.ok(Math.abs(p.signal_only_extra_seconds!-140/3)<1e-8);
 assert.equal(p.mixed_extra_seconds,50);assert.equal(p.other_extra_seconds,0);
 assert.ok(p.coverage_pct>53&&p.coverage_pct<55);
 assert.equal(profiles[1].observed_seconds,null,'opposite direction is never borrowed');
 assert.equal(p.windows[3].extra_seconds,null,'sparse strata do not acquire a baseline');
 const scenario=priorityScenario(p,50,50,true);
 assert.ok(Math.abs(scenario.saved_seconds!-145/6)<1e-8);
 assert.equal(priorityScenario(p,0,100,true).saved_seconds,0);
 assert.ok(priorityScenario(p,100,100,false).saved_seconds!<priorityScenario(p,100,100,true).saved_seconds!);
 assert.deepEqual(priorityScenario(profiles[1],50,50,true),{saved_seconds:null,after_seconds:null,reduction_pct:null});
});
test('faster benchmark is not borrowed across days or contexts with insufficient evidence',()=>{
 const singleDay=row(200,'signal_only',90,30,100);singleDay.days=['2026-09-01'];
 const p=priorityProfiles(network,'st_charles',[singleDay])[0];
 assert.equal(p.observed_seconds,null);assert.equal(p.coverage_pct,0);
 assert.equal(priorityProfiles(network,'st_charles',[])[0].observed_seconds,null);
 for(const input of [{hour_from:-1},{hour_to:24},{hour_from:20,hour_to:5},{day_type:'holiday'},{hour_from:'1 OR 1=1'}])assert.throws(()=>priorityFilters(input));
 assert.deepEqual(priorityFilters({}),{day_type:'all',hour_from:0,hour_to:23});
});
