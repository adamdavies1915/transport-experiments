import {PRIORITY_METHOD, type PriorityProfile, type PriorityWindow} from './priority-data';
import type {StreetcarNetwork} from './streetcar-data';
import {streetcarCatalog,streetcarQuote as q} from './streetcar-query';

export interface PriorityFilters { day_type: 'all'|'weekday'|'weekend'; hour_from:number; hour_to:number }
export function priorityFilters(input:Record<string,unknown>):PriorityFilters {
  const day=String(input.day_type??'all');
  const from=Number(input.hour_from??0),to=Number(input.hour_to??23);
  if(!['all','weekday','weekend'].includes(day)||!Number.isInteger(from)||!Number.isInteger(to)||from<0||to>23||from>to)
    throw new Error('Choose valid local hours (0–23) and weekday/weekend filters.');
  return {day_type:day as PriorityFilters['day_type'],hour_from:from,hour_to:to};
}
export function priorityStatsSql(database:string,version:string,corridor:string,from:string,to:string,filters:PriorityFilters) {
  return `SELECT path_id,window_id,route,direction,from_meters,to_meters,category,signal_ids,stop_ids,
    day_type,FLOOR(hour/${PRIORITY_METHOD.time_band_hours}) AS time_band,
    COUNT(*)::INTEGER AS passages,to_json(list(DISTINCT date::VARCHAR)) AS days,
    AVG(duration_seconds) AS mean_seconds,
    quantile_cont(duration_seconds,0.1) AS p10,quantile_cont(duration_seconds,0.2) AS p20,
    quantile_cont(duration_seconds,0.3) AS p30
    FROM ${streetcarCatalog(database)}.streetcar_passages
    WHERE network_version=${q(version)} AND method=${q(PRIORITY_METHOD.name)} AND corridor=${q(corridor)}
      AND date BETWEEN ${q(from)}::DATE AND ${q(to)}::DATE
      AND hour BETWEEN ${filters.hour_from} AND ${filters.hour_to}
      ${filters.day_type==='all'?'':`AND day_type=${q(filters.day_type)}`}
    GROUP BY ALL ORDER BY path_id,from_meters,day_type,time_band`;
}
export interface PriorityStratum {
  path_id:string;window_id:string;route:string;direction:string;from_meters:number;to_meters:number;
  category:PriorityWindow['category'];signal_ids:string[];stop_ids:string[];days:string[];
  passages:number;mean_seconds:number;p10:number;p20:number;p30:number;
}
export function decodePriorityStats(rows:Record<string,unknown>[]):PriorityStratum[] {
  return rows.map(row=>({...row,signal_ids:JSON.parse(String(row.signal_ids)),stop_ids:JSON.parse(String(row.stop_ids)),days:JSON.parse(String(row.days))})) as unknown as PriorityStratum[];
}
function length(points:Array<{lat:number;lon:number}>) {
  let result=0;
  for(let i=1;i<points.length;i++) {
    const a=points[i-1],b=points[i],r=Math.PI/180;
    result+=6371000*Math.hypot((b.lat-a.lat)*r,(b.lon-a.lon)*r*Math.cos((a.lat+b.lat)/2*r));
  }
  return result;
}
export function priorityProfiles(network:StreetcarNetwork,corridor:string,stats:PriorityStratum[]):PriorityProfile[] {
  const names=new Map(network.sites.map(s=>[s.id,s.name]));
  return network.paths.filter(p=>p.corridor===corridor).map(path=>{
    const groups=new Map<string,PriorityStratum[]>();
    for(const row of stats.filter(s=>s.path_id===path.id)) {
      const group=groups.get(row.window_id)??[];group.push(row);groups.set(row.window_id,group);
    }
    const windows:PriorityWindow[]=[...groups.values()].map(group=>{
      const first=group[0];
      const eligible=group.filter(s=>s.passages>=PRIORITY_METHOD.min_passages&&s.days.length>=PRIORITY_METHOD.min_days);
      const selected=eligible.length?eligible:group;
      const n=selected.reduce((sum,s)=>sum+s.passages,0);
      const mean=(key:'mean_seconds'|'p10'|'p20'|'p30')=>selected.reduce((sum,s)=>sum+s[key]*s.passages,0)/n;
      const excess=(key:'p10'|'p20'|'p30')=>eligible.reduce((sum,s)=>sum+Math.max(0,s.mean_seconds-s[key])*s.passages,0)/n;
      const siteNames=[...new Set([...first.signal_ids,...first.stop_ids].map(id=>names.get(id)).filter(Boolean))];
      return {id:first.window_id,path_id:path.id,route:path.route,direction:path.direction,
        from_meters:first.from_meters,to_meters:first.to_meters,category:first.category,
        signal_ids:first.signal_ids,stop_ids:first.stop_ids,name:siteNames.join(' / ')||`Track ${Math.round(first.from_meters)}–${Math.round(first.to_meters)} m`,
        passages:n,days:new Set(selected.flatMap(s=>s.days)).size,eligible:eligible.length>0,
        observed_seconds:mean('mean_seconds'),baseline_seconds:eligible.length?mean('p20'):null,
        extra_seconds:eligible.length?excess('p20'):null,extra_p10_seconds:eligible.length?excess('p10'):null,extra_p30_seconds:eligible.length?excess('p30'):null};
    }).sort((a,b)=>a.from_meters-b.from_meters);
    const matched=windows.filter(w=>w.eligible);
    const sum=(fn:(w:PriorityWindow)=>number)=>matched.length?matched.reduce((n,w)=>n+fn(w),0):null;
    const covered=matched.reduce((n,w)=>n+w.to_meters-w.from_meters,0),meters=length(path.points);
    return {path_id:path.id,route:path.route,direction:path.direction,headsign:path.headsign,
      route_meters:meters,covered_meters:covered,coverage_pct:meters?100*covered/meters:0,
      eligible_windows:matched.length,total_windows_observed:windows.length,passages:matched.reduce((n,w)=>n+w.passages,0),
      observed_seconds:sum(w=>w.observed_seconds),baseline_seconds:sum(w=>w.baseline_seconds!),
      signal_only_extra_seconds:sum(w=>w.category==='signal_only'?w.extra_seconds!:0),
      mixed_extra_seconds:sum(w=>w.category==='both'?w.extra_seconds!:0),
      other_extra_seconds:sum(w=>['stop_only','neither'].includes(w.category)?w.extra_seconds!:0),windows};
  });
}
