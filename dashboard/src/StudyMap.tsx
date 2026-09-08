import { useState } from 'react';
import type { StudyCatalog, StudyFilters, StudyPoint } from '../../src/transit-study-types';

const colors={reserved:'#34d399',shared:'#fb923c',unknown:'#64748b'};
const distance=(a:StudyPoint,b:StudyPoint)=>Math.hypot((b.lat-a.lat)*111195,(b.lon-a.lon)*111195*Math.cos((a.lat+b.lat)*Math.PI/360));
function sectionPoints(points:StudyPoint[],from:number,to:number):StudyPoint[]{
  const result:StudyPoint[]=[];let traveled=0;
  for(let i=1;i<points.length;i++){
    const a=points[i-1],b=points[i],length=distance(a,b),end=traveled+length;
    if(length>0&&end>=from&&traveled<=to){for(const value of [Math.max(from,traveled),Math.min(to,end)]){const fraction=(value-traveled)/length;result.push({lat:a.lat+(b.lat-a.lat)*fraction,lon:a.lon+(b.lon-a.lon)*fraction});}}
    traveled=end;if(traveled>to)break;
  }
  return result;
}
export default function StudyMap({network,filters,kind,selectedSite,onSelectSite}:{network:StudyCatalog;filters:StudyFilters;kind:'row'|'signals';selectedSite?:string;onSelectSite?:(id:string)=>void}){
  const [selectedSection,setSelectedSection]=useState('');
  const paths=network.paths.filter(path=>(!filters.mode||path.mode===filters.mode)&&(!filters.route_id||path.route_id===filters.route_id)&&(!filters.direction_id||path.direction_id===filters.direction_id));
  const pathIds=new Set(paths.map(path=>path.id)),routes=new Set(paths.map(path=>path.route_id));
  const sites=network.sites.filter(site=>site.route_ids.some(route=>routes.has(route))&&(!site.path_ids?.length||site.path_ids.some(id=>pathIds.has(id))));
  const points=paths.flatMap(path=>path.points);
  if(!points.length)return <p className="rounded-xl bg-slate-900 p-6 text-sm text-slate-300">Route geometry has not been published for this selection yet.</p>;
  let minLat=90,maxLat=-90,minLon=180,maxLon=-180;
  for(const point of points){minLat=Math.min(minLat,point.lat);maxLat=Math.max(maxLat,point.lat);minLon=Math.min(minLon,point.lon);maxLon=Math.max(maxLon,point.lon);}
  const cosine=Math.cos((minLat+maxLat)*Math.PI/360),width=Math.max((maxLon-minLon)*cosine,0.001),height=Math.max(maxLat-minLat,0.001),scale=Math.min(920/width,430/height);
  const project=(point:StudyPoint)=>({x:500+((point.lon-(minLon+maxLon)/2)*cosine)*scale,y:250-(point.lat-(minLat+maxLat)/2)*scale});
  const line=(coords:StudyPoint[])=>coords.map(point=>{const p=project(point);return`${p.x.toFixed(1)},${p.y.toFixed(1)}`;}).join(' ');
  const section=network.row_sections.find(item=>item.id===selectedSection&&pathIds.has(item.path_id));
  const selected=network.sites.find(site=>site.id===selectedSite);
  return <div>
    <svg role="img" aria-label={kind==='row'?'Route map with reviewed reserved and shared roadway sections':'Route map with traffic signals and passenger stops'} viewBox="0 0 1000 500" className="w-full rounded-xl bg-slate-950 border border-slate-700 min-h-48">
      <title>{kind==='row'?'Reviewed roadway classifications':'Signals and passenger stops'}</title>
      {paths.map(path=><polyline key={path.id} points={line(path.points)} fill="none" stroke={colors.unknown} strokeWidth={kind==='row'?5:3} strokeLinecap="round"><title>{`${path.name} · Route ${path.route_id} · Direction ${path.direction_id}`}</title></polyline>)}
      {kind==='row'&&network.row_sections.filter(item=>pathIds.has(item.path_id)).map(item=>{const path=paths.find(path=>path.id===item.path_id)!;const reviewed=item.reviewed_at!=null&&item.evidence_urls.length>0&&item.valid_from!=null&&item.row_class!=='unknown'&&(!filters.from||item.valid_from<=filters.from)&&(!item.valid_to||!filters.to||item.valid_to>=filters.to);return <polyline key={item.id} points={line(sectionPoints(path.points,item.from_meters,item.to_meters))} fill="none" stroke={colors[reviewed?item.row_class:'unknown']} strokeWidth={item.id===selectedSection?10:6} strokeLinecap="round" role="button" tabIndex={0} aria-label={`${item.row_class} section: ${item.notes}`} onClick={()=>setSelectedSection(item.id)} onKeyDown={event=>{if(event.key==='Enter'||event.key===' '){event.preventDefault();setSelectedSection(item.id);}}} className="cursor-pointer focus:outline-none focus:stroke-white"><title>{`${item.row_class} · ${item.notes}`}</title></polyline>;})}
      {kind==='signals'&&sites.map(site=>{const p=project(site),active=site.id===selectedSite;return <g key={site.id} transform={`translate(${p.x},${p.y})`} role="button" tabIndex={0} aria-label={`${site.name}: ${site.kind==='signal'?'traffic signal':site.kind==='stop'?'passenger stop':'rail signal, function unverified'}`} onClick={()=>onSelectSite?.(site.id)} onKeyDown={event=>{if(event.key==='Enter'||event.key===' '){event.preventDefault();onSelectSite?.(site.id);}}} className="cursor-pointer focus:outline-none focus:stroke-white">
        {active&&<circle r={15} fill="none" stroke="#fff" strokeWidth={2}/>}
        {site.kind==='stop'?<circle r={4} fill="#67e8f9" stroke="#0f172a" strokeWidth={1}/>:site.kind==='signal'?<path d="M 0,-7 L 6,5 L -6,5 Z" fill="#fbbf24" stroke="#0f172a" strokeWidth={1}/>:<rect x={-4} y={-4} width={8} height={8} fill="#a78bfa"/>}<title>{site.name}</title>
      </g>;})}
    </svg>
    <div className="flex flex-wrap gap-x-5 gap-y-2 text-xs mt-3 text-slate-300">
      {kind==='row'?<><span className="text-emerald-300">━ Reserved right of way</span><span className="text-orange-300">━ Shared roadway</span><span>━ Unreviewed / unknown</span></>:<><span className="text-amber-300">▲ Traffic signal</span><span className="text-cyan-300">● Passenger stop</span><span className="text-purple-300">■ Rail signal · function unverified</span></>}
    </div>
    {kind==='row'&&section&&<div className="mt-4 rounded-lg bg-slate-900 p-4 text-sm"><strong className="capitalize">{section.row_class} roadway</strong><p className="text-slate-300 mt-1">{section.notes}</p><p className="text-xs text-slate-400 mt-2">Reviewed: {section.reviewed_at??'Not reviewed'}. Applies from {section.valid_from??'unknown'} through {section.valid_to??'open ended'}.</p>{section.evidence_urls.filter(url=>/^https?:\/\//.test(url)).map((url,index)=><a key={url} href={url} target="_blank" rel="noreferrer" className="inline-block text-blue-300 underline mr-4 mt-2">Evidence {index+1}</a>)}</div>}
    {kind==='signals'&&selected&&<div className="mt-3 text-sm"><strong>{selected.name}</strong><p className="text-slate-300 mt-1">{selected.kind==='rail_signal'?'Rail signal — function unverified; excluded from traffic-signal estimates.':selected.verification==='mapillary'?'Mapillary detections corroborate this OSM location; this does not establish which vehicles the signal controls.':selected.verification==='reviewed_control'?'Signal control reviewed.':selected.verification==='gtfs'?'Passenger stop from the official GTFS schedule.':'OSM signal candidate; control is unverified.'}</p>{selected.source_ids.filter(id=>id.startsWith('mapillary/image/')).map(id=><a className="block text-blue-300 underline mt-2" key={id} href={`https://www.mapillary.com/app/?pKey=${encodeURIComponent(id.slice(16))}`} target="_blank" rel="noreferrer">View reviewed Mapillary imagery</a>)}</div>}
    <p className="text-xs text-slate-400 mt-3">Geometry and stop locations: RTA GTFS. Signal locations: <a className="underline" href="https://www.openstreetmap.org/copyright">© OpenStreetMap contributors</a>. Select a {kind==='row'?'section':'site'} for details.</p>
  </div>;
}
