import type { RowStudyData, SignalStudyData, StudyDimensions, StudyFilters, StudyRowCell, StudyRowComparison, StudySignalCell, StudySignalSummary } from '../../src/transit-study-types';

export function matchesStudyFilters(cell: Omit<StudyDimensions, 'path_id'>, filters: StudyFilters): boolean {
  return (!filters.from || cell.date >= filters.from) && (!filters.to || cell.date <= filters.to)
    && (!filters.source || cell.source === filters.source) && (!filters.mode || cell.mode === filters.mode)
    && (!filters.route_id || cell.route_id === filters.route_id) && (!filters.direction_id || cell.direction_id === filters.direction_id)
    && (!filters.day_type || cell.day_type === filters.day_type)
    && (filters.hour_from == null || cell.hour >= filters.hour_from) && (filters.hour_to == null || cell.hour <= filters.hour_to);
}
export const ROW_MIN_PASSAGES = 30;
export const ROW_MIN_DATES = 7;
export const SIGNAL_MIN_ENCOUNTERS = 30;
export const SIGNAL_MIN_DATES = 7;
export const BOOTSTRAP_REPLICATES = 2000;
const sum = <T>(rows: T[], value: (row:T)=>number) => rows.reduce((total,row)=>total+value(row),0);
const mean = (values:number[]) => values.reduce((a,b)=>a+b,0)/values.length;
/** Resample whole sorted service dates, retaining all correlated encounters on each date.
 * A fixed seed makes offline publication and filtered browser results reproducible.
 * Percentile intervals describe variation across sampled days, not causal or clock uncertainty. */
export function serviceDateBootstrap(days: Array<{numerator:number;denominator:number}>): [number,number] | null {
  if (days.length < 7) return null;
  let state = 0x51a7c0de;
  const random = () => { state = (Math.imul(1664525,state) + 1013904223) >>> 0; return state / 4294967296; };
  const values:number[]=[];
  for(let b=0;b<BOOTSTRAP_REPLICATES;b++) {
    let numerator=0,denominator=0;
    for(let j=0;j<days.length;j++) { const day=days[Math.floor(random()*days.length)]; numerator+=day.numerator;denominator+=day.denominator; }
    if(denominator>0) values.push(numerator/denominator);
  }
  values.sort((a,b)=>a-b);
  const quantile=(q:number) => { const at=(values.length-1)*q,lo=Math.floor(at); return values[lo]+(values[Math.ceil(at)]-values[lo])*(at-lo); };
  return values.length ? [quantile(0.025),quantile(0.975)] : null;
}
export function compareRowCells(cells: StudyRowCell[]): StudyRowComparison[] {
  const groups = new Map<string,StudyRowCell[]>();
  for (const cell of cells) {
    if (cell.row_class==='unknown' || cell.distance_meters<=0) continue;
    const key=JSON.stringify([cell.source,cell.mode,cell.route_id,cell.direction_id,cell.day_type,cell.time_band,cell.context,cell.signal_count,cell.stop_count]);
    const group=groups.get(key)??[];group.push(cell);groups.set(key,group);
  }
  const comparisons:StudyRowComparison[]=[];
  for(const [id,group] of [...groups].sort(([a],[b])=>a.localeCompare(b))) {
    const first=group[0];
    const dates=[...new Set(group.map(cell=>cell.date))].sort();
    const matched=dates.map(date=>({reserved:group.filter(cell=>cell.date===date&&cell.row_class==='reserved'),shared:group.filter(cell=>cell.date===date&&cell.row_class==='shared')})).filter(day=>day.reserved.length&&day.shared.length);
    const reserved_passages=sum(matched,day=>sum(day.reserved,cell=>cell.passages)),shared_passages=sum(matched,day=>sum(day.shared,cell=>cell.passages));
    const ready=matched.length>=ROW_MIN_DATES&&reserved_passages>=ROW_MIN_PASSAGES&&shared_passages>=ROW_MIN_PASSAGES;
    const rate=(rows:StudyRowCell[],field:'duration_seconds'|'duration_lower_seconds'|'duration_upper_seconds')=>1000*sum(rows,cell=>cell[field])/sum(rows,cell=>cell.distance_meters);
    const reserved=ready?mean(matched.map(day=>rate(day.reserved,'duration_seconds'))):null;
    const shared=ready?mean(matched.map(day=>rate(day.shared,'duration_seconds'))):null;
    const ci=ready?serviceDateBootstrap(matched.map(day=>({numerator:rate(day.shared,'duration_seconds')-rate(day.reserved,'duration_seconds'),denominator:1}))):null;
    comparisons.push({id,source:first.source,mode:first.mode,route_id:first.route_id,direction_id:first.direction_id,day_type:first.day_type,time_band:first.time_band,context:first.context,signal_count:first.signal_count,stop_count:first.stop_count,
      status:ready?'ready':'insufficient_data',reserved_passages,shared_passages,matched_dates:matched.length,
      reserved_seconds_per_km:reserved,shared_seconds_per_km:shared,
      reserved_speed_mph:reserved!=null&&reserved>0?2236.9362920544/reserved:null,shared_speed_mph:shared!=null&&shared>0?2236.9362920544/shared:null,
      shared_extra_seconds_per_km:shared!=null&&reserved!=null?shared-reserved:null,
      shared_extra_lower_seconds_per_km:ready?mean(matched.map(day=>rate(day.shared,'duration_lower_seconds')-rate(day.reserved,'duration_upper_seconds'))):null,
      shared_extra_upper_seconds_per_km:ready?mean(matched.map(day=>rate(day.shared,'duration_upper_seconds')-rate(day.reserved,'duration_lower_seconds'))):null,
      shared_extra_ci_lower_seconds_per_km:ci?.[0]??null,shared_extra_ci_upper_seconds_per_km:ci?.[1]??null});
  }
  return comparisons;
}
export function rowStudyFromCells(data: RowStudyData, filters: StudyFilters): RowStudyData {
  const cells = data.cells.filter(cell=>matchesStudyFilters(cell,filters)),comparisons=compareRowCells(cells);
  const coverage_cells = data.coverage_cells?.filter(cell=>matchesStudyFilters(cell,filters));
  const compactUnknown = sum(coverage_cells??[],cell=>cell.passages);
  return {...data,from:filters.from??data.from,to:filters.to??data.to,cells,coverage_cells,comparisons,
    status:comparisons.some(row=>row.status==='ready')?'ready':'collecting',
    quality:data.quality.filter(row=>(!filters.from||row.date>=filters.from)&&(!filters.to||row.date<=filters.to)&&(!filters.source||row.source===filters.source)),
    coverage:{passages:sum(cells,cell=>cell.passages)+compactUnknown,classified_passages:sum(cells.filter(cell=>cell.row_class!=='unknown'),cell=>cell.passages),unknown_passages:sum(cells.filter(cell=>cell.row_class==='unknown'),cell=>cell.passages)+compactUnknown}};
}
export function summarizeSignalCells(cells: StudySignalCell[]): StudySignalSummary[] {
  const groups=new Map<string,typeof cells>();
  for(const cell of cells){const key=JSON.stringify([cell.source,cell.mode,cell.route_id,cell.direction_id,cell.site_id,cell.context]);const rows=groups.get(key)??[];rows.push(cell);groups.set(key,rows);}
  return [...groups].sort(([a],[b])=>a.localeCompare(b)).map(([,rows])=>{
    const first=rows[0],encounters=sum(rows,row=>row.encounters),evaluable_encounters=sum(rows,row=>row.evaluable_encounters),detected_wait_encounters=sum(rows,row=>row.detected_wait_encounters),wait_events=sum(rows,row=>row.wait_events),wait_seconds=sum(rows,row=>row.wait_seconds);
    const dates=[...new Set(rows.map(row=>row.date))].sort();
    const evaluable_dates=new Set(rows.filter(row=>row.evaluable_encounters>0).map(row=>row.date)).size;
    const ready=evaluable_encounters>=SIGNAL_MIN_ENCOUNTERS&&evaluable_dates>=SIGNAL_MIN_DATES;
    // A wholly unevaluable sample cannot be presented as measured zero delay.
    const perEncounter=encounters>0&&evaluable_encounters>0?wait_seconds/encounters:null;
    const ci=ready?serviceDateBootstrap(dates.map(date=>({numerator:sum(rows.filter(row=>row.date===date),row=>row.wait_seconds),denominator:sum(rows.filter(row=>row.date===date),row=>row.encounters)}))):null;
    return{source:first.source,mode:first.mode,route_id:first.route_id,direction_id:first.direction_id,site_id:first.site_id,context:first.context,encounters,evaluable_encounters,detected_wait_encounters,wait_events,wait_seconds,
      status:ready?'ready' as const:'insufficient_data' as const,observed_dates:dates.length,evaluable_dates,
      detected_wait_probability:encounters>0&&evaluable_encounters>0?detected_wait_encounters/encounters:null,mean_detected_wait_seconds:wait_events>0?wait_seconds/wait_events:null,detected_wait_seconds_per_encounter:perEncounter,
      detected_wait_seconds_per_encounter_ci_lower:ci?.[0]??null,detected_wait_seconds_per_encounter_ci_upper:ci?.[1]??null,
      recovery_seconds_per_encounter:([25,50,75] as const).map(percent=>({percent,seconds:first.context==='signal_only'&&perEncounter!=null?percent/100*perEncounter:null}))};
  });
}
export function signalStudyFromCells(data:SignalStudyData,filters:StudyFilters):SignalStudyData {
  const cells=data.cells.filter(cell=>matchesStudyFilters(cell,filters)),signals=summarizeSignalCells(cells);
  return{...data,from:filters.from??data.from,to:filters.to??data.to,cells,signals,status:signals.some(signal=>signal.status==='ready')?'ready':'collecting',
    quality:data.quality.filter(row=>(!filters.from||row.date>=filters.from)&&(!filters.to||row.date<=filters.to)&&(!filters.source||row.source===filters.source))};
}
