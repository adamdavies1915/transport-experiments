import type { StreetcarBin, StreetcarData, StreetcarNetwork, StreetcarQuality, StreetcarSiteBin } from './streetcar-data';
export const streetcarQuote = (v: string) => `'${v.replace(/'/g, "''")}'`;
export function streetcarCatalog(database: string) { return `"${database.replace(/"/g,'""')}"`; }
export function streetcarRange(input: { from?: string; to?: string }, availableFrom: string, availableTo: string) {
  const to=input.to??availableTo;
  const valid=(s:string)=>/^\d{4}-\d{2}-\d{2}$/.test(s)&&Number.isFinite(Date.parse(s+'T00:00:00Z'))&&new Date(s+'T00:00:00Z').toISOString().slice(0,10)===s;
  if(!valid(to))throw new Error('Choose a valid date range of at most 93 days.');
  const from=input.from??[availableFrom,new Date(Date.parse(to+'T00:00:00Z')-6*86400000).toISOString().slice(0,10)].sort().at(-1)!;
  if(!valid(from)||!valid(to)||from>to||Date.parse(to)-Date.parse(from)>92*86400000)throw new Error('Choose a valid date range of at most 93 days.');
  return {from,to};
}
export function streetcarQueries(database:string,version:string,corridor:string,from:string,to:string,method?:string) {
  const catalog=streetcarCatalog(database);
  const where=`network_version=${streetcarQuote(version)} ${method?`AND method=${streetcarQuote(method)}`:''} AND corridor=${streetcarQuote(corridor)} AND date BETWEEN ${streetcarQuote(from)}::DATE AND ${streetcarQuote(to)}::DATE`;
  const columns=`date::VARCHAR AS date,corridor,route,direction,hour,day_type,category,intervals,duration_seconds,distance_meters,slow_seconds,vehicle_ids,duration_lower_seconds,duration_upper_seconds`;
  return {
    bins:`SELECT ${columns} FROM ${catalog}.streetcar_bins WHERE ${where} ORDER BY date,route,direction,hour,category`,
    site_bins:`SELECT site_id,${columns} FROM ${catalog}.streetcar_site_bins WHERE ${where} ORDER BY date,site_id,route,direction,hour,category`,
    quality:`SELECT date::VARCHAR AS date,corridor,raw_points,candidate_intervals,accepted_intervals,excluded,updated_at::VARCHAR AS updated_at FROM ${catalog}.streetcar_quality WHERE ${where} ORDER BY date,corridor`,
  };
}
export function decodeStreetcarBins(rows: Record<string,unknown>[], timestampQuantizationSeconds=0): StreetcarBin[] {
  return rows.map(r=>({...r,vehicle_ids:JSON.parse(String(r.vehicle_ids)),
    // Duration differences inherit up to one source-resolution unit at each
    // end in opposite directions. Aggregate padding is conservatively loose.
    duration_lower_seconds:r.duration_lower_seconds==null?null:Math.max(0,Number(r.duration_lower_seconds)-Number(r.intervals)*timestampQuantizationSeconds),
    duration_upper_seconds:r.duration_upper_seconds==null?null:Number(r.duration_upper_seconds)+Number(r.intervals)*timestampQuantizationSeconds,
  })) as unknown as StreetcarBin[];
}
export function decodeStreetcarSiteBins(rows:Record<string,unknown>[],timestampQuantizationSeconds=0):StreetcarSiteBin[] {
  return decodeStreetcarBins(rows,timestampQuantizationSeconds) as StreetcarSiteBin[];
}
export function decodeStreetcarQuality(rows:Record<string,unknown>[]):StreetcarQuality[] {
  return rows.map(r=>({...r,excluded:JSON.parse(String(r.excluded))})) as unknown as StreetcarQuality[];
}
export function emptyStreetcarData(network?: StreetcarNetwork):StreetcarData {
  return { status:'not_ready', network:network??{version:'',generated_at:'',schedule_hash:'',corridors:[
    {id:'st_charles',name:'St. Charles',routes:['12']},{id:'canal',name:'Canal',routes:['47','48']},{id:'rampart',name:'Rampart–Loyola',routes:['46']}
  ],paths:[],sites:[],sources:[],mapillary_status:'Location catalog has not been loaded yet.'},
  bins:[],site_bins:[],quality:[],updated_at:null,
  method:{name:'streetcar-analysis-pending',max_gap_seconds:90,feature_radius_meters:40,track_tolerance_meters:35,terminal_radius_meters:60,slow_mph:3,limitations:[]}};
}
