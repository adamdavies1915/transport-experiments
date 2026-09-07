import {test} from 'node:test';
import assert from 'node:assert/strict';
import {DuckDBInstance} from '@duckdb/node-api';
import {calculateStreetcarDay,calculateStreetcarWaitDay,initializeStreetcars} from './streetcar-worker';
import {captureStreetcarSnapshots,initializeStreetcarSnapshots,persistStreetcarSnapshots} from './streetcar-snapshots';
import {METHOD} from './streetcar-analysis';
import {decodeStreetcarBins,decodeStreetcarSiteBins,streetcarQueries,streetcarRange} from '../dashboard/src/streetcar-query';
import type {StreetcarNetwork} from '../dashboard/src/streetcar-data';
import {priorityStatsSql,decodePriorityStats,priorityFilters} from '../dashboard/src/priority-query';
const network:StreetcarNetwork={version:'fixture',generated_at:'2026-09-07',schedule_hash:'fixture',
  corridors:[{id:'st_charles',name:'St. Charles',routes:['12']},{id:'canal',name:'Canal',routes:['47','48']},{id:'rampart',name:'Rampart',routes:['46']}],
  paths:[{id:'north',corridor:'st_charles',route:'12',direction:'0',headsign:'North',points:[{lat:30,lon:-90},{lat:30.01,lon:-90}],stop_ids:[]}],
  sites:[{id:'signal',corridor:'st_charles',kind:'signal',name:'Signal',routes:['12'],source_ids:['osm:node:1'],verification:'osm_unverified',lat:30.0045,lon:-90}],sources:[],mapillary_status:'Fixture'};
test('streetcar persistence is idempotent and actual API SQL preserves metrics, bounds, and sources',async()=>{
 const db=await DuckDBInstance.create(':memory:');const c=await db.connect();
 try {
  await initializeStreetcars(c,network);
  await c.run(`CREATE TABLE transit_data AS SELECT 'v1' AS vid,'12' AS route,'trip' AS trip_id,
   NULL::VARCHAR AS gtfs_trip_id,30.003 AS lat,-90.0 AS lon,0 AS heading,false AS is_off_route,
   NULL::TIMESTAMPTZ AS observed_at,TIMESTAMP '2026-09-06 10:00:00' AS timestamp,'North' AS destination
   UNION ALL SELECT 'v1','12','trip',NULL,30.004,-90,0,false,NULL,TIMESTAMP '2026-09-06 10:01:00','North'
   UNION ALL SELECT 'v1','12','trip',NULL,30.005,-90,0,false,NULL,TIMESTAMP '2026-09-06 10:02:00','North'
   UNION ALL SELECT 'v1','12','trip',NULL,30.006,-90,0,false,NULL,TIMESTAMP '2026-09-06 10:03:00','North'`);
  await c.run("INSERT INTO streetcar_backfill_days VALUES ('2026-09-06',NULL,NULL)");
  await calculateStreetcarDay(c,network,'2026-09-06');
  await calculateStreetcarDay(c,network,'2026-09-06');
  const catalog=String((await c.runAndReadAll('SELECT current_database() AS name')).getRowObjectsJson()[0].name);
  const sql=streetcarQueries(catalog,network.version,'st_charles','2026-09-06','2026-09-06',METHOD.name);
  const bins=decodeStreetcarBins((await c.runAndReadAll(sql.bins)).getRowObjectsJson());
  assert.ok(bins.length>0);assert.ok(bins.some(b=>b.category==='signal_only'));
  assert.deepEqual(bins[0].vehicle_ids,['v1']);
  assert.equal(bins[0].day_type,'weekend');
  assert.ok(bins[0].duration_seconds>0);
  if(METHOD.window_meters) {
   assert.equal(bins[0].distance_meters,bins[0].intervals*METHOD.window_meters);
   assert.ok(bins[0].duration_lower_seconds!<=bins[0].duration_seconds);
   assert.ok(bins[0].duration_upper_seconds!>=bins[0].duration_seconds);
  }
  const siteBins=decodeStreetcarSiteBins((await c.runAndReadAll(sql.site_bins)).getRowObjectsJson());
  assert.equal(siteBins[0].site_id,'signal');
  const priorityRows=decodePriorityStats((await c.runAndReadAll(priorityStatsSql(catalog,network.version,'st_charles','2026-09-06','2026-09-06',priorityFilters({})))).getRowObjectsJson());
  assert.ok(priorityRows.length>0);
  assert.equal(priorityRows.reduce((sum,p)=>sum+p.passages,0),bins.reduce((sum,b)=>sum+b.intervals,0));
  assert.deepEqual(priorityRows[0].days,['2026-09-06']);
  assert.ok(priorityRows[0].p10<=priorityRows[0].p20 && priorityRows[0].p20<=priorityRows[0].p30);
  assert.equal((await c.runAndReadAll('SELECT COUNT(*) AS n FROM transit_data')).getRowObjectsJson()[0].n,'4');
  assert.equal((await c.runAndReadAll('SELECT applied_version FROM streetcar_backfill_days')).getRowObjectsJson()[0].applied_version,network.version+':'+METHOD.name);
 }finally{c.closeSync();db.closeSync();}
});
test('streetcar request dates are bounded and invalid dates do not become queries',()=>{
 assert.deepEqual(streetcarRange({},'2026-08-01','2026-09-07'),{from:'2026-09-01',to:'2026-09-07'});
 assert.deepEqual(streetcarRange({},'2026-09-05','2026-09-07'),{from:'2026-09-05',to:'2026-09-07'});
 for(const input of [{to:'bad'},{from:'2026-02-30'},{from:'2026-09-08',to:'2026-09-07'},{from:'2025-01-01'}])assert.throws(()=>streetcarRange(input,'2026-08-01','2026-09-07'));
});
test('API timing ranges include minute timestamp precision for every completed passage',()=>{
 const rows=[
  {intervals:2,vehicle_ids:'["v1"]',duration_lower_seconds:150,duration_upper_seconds:260},
  {intervals:3,vehicle_ids:'[]',duration_lower_seconds:30,duration_upper_seconds:90},
  {intervals:1,vehicle_ids:'[]',duration_lower_seconds:null,duration_upper_seconds:null},
 ];
 const bins=decodeStreetcarBins(rows,60);
 assert.deepEqual(bins.map(b=>[b.duration_lower_seconds,b.duration_upper_seconds]),[[30,380],[0,270],[null,null]]);
 assert.deepEqual(decodeStreetcarSiteBins(rows,60),bins);
 assert.equal(decodeStreetcarBins(rows,0)[0].duration_lower_seconds,150);
});
test('retained receipts produce one idempotent dated candidate wait through the worker SQL',async()=>{
 const db=await DuckDBInstance.create(':memory:');const c=await db.connect();
 try {
  await initializeStreetcars(c,network);await initializeStreetcarSnapshots(c);
  const positions=[30.0040,30.0045,30.0045,30.0045,30.0050];
  const snapshots=positions.flatMap((lat,i)=>captureStreetcarSnapshots([
   {vid:'v1',rt:'12',tatripid:'trip',tmstmp:'2026-09-06T10:00:00-05:00',lat:String(lat),lon:'-90',spd:0,or:false},
  ],{received_at:new Date(Date.parse('2026-09-06T15:00:00Z')+i*10000).toISOString(),source_url:'https://example.test/sse',receipt_id:'r'+i}));
  await persistStreetcarSnapshots(c,snapshots);
  await calculateStreetcarWaitDay(c,network,'2026-09-06');
  await calculateStreetcarWaitDay(c,network,'2026-09-06');
  const events=(await c.runAndReadAll('SELECT date::VARCHAR AS day,site_id,context,duration_seconds FROM streetcar_waits')).getRowObjectsJson();
  assert.deepEqual(events,[{day:'2026-09-06',site_id:'signal',context:'signal_only',duration_seconds:20}]);
  const coverage=(await c.runAndReadAll("SELECT snapshots FROM streetcar_wait_quality WHERE corridor='st_charles'")).getRowObjectsJson();
  assert.equal(coverage[0].snapshots,5);
 }finally{c.closeSync();db.closeSync();}
});
