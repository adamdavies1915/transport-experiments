import { useEffect, useState } from 'react';
import type { SourceQualityData } from './summary-data';
export default function SourceQuality(){
  const [data,setData]=useState<SourceQualityData|null>(null),[failed,setFailed]=useState(false);
  useEffect(()=>{const controller=new AbortController();fetch('/api/source-quality',{signal:controller.signal}).then(async response=>{if(!response.ok)throw new Error('Unavailable');return response.json() as Promise<SourceQualityData>;}).then(value=>{if(!controller.signal.aborted){setData(value);setFailed(false);}}).catch(()=>{if(!controller.signal.aborted)setFailed(true);});return()=>controller.abort();},[]);
  return <aside aria-label="Observation source coverage" className="mb-6 rounded-lg border border-slate-700 px-4 py-3 text-xs text-slate-300">
    <div className="flex flex-wrap gap-x-6 gap-y-2">{data?.sources.length?data.sources.map(source=><p key={source.id}><strong className="text-slate-100">{source.label}</strong> · {source.status}{source.last_received_at?` · last received ${new Date(source.last_received_at).toLocaleString()}`:''}{source.message&&<span className="block text-slate-400 mt-1 max-w-lg">{source.message}</span>}</p>):<p>{failed?'Source coverage is temporarily unavailable.':'Waiting for source coverage from the collector.'}</p>}</div>
    {data?.snapshot?.stale&&<p className="mt-2 text-amber-200" role="status">{data.snapshot.generated_at?`Saved summary from ${new Date(data.snapshot.generated_at).toLocaleString()}. A fresh summary is pending.`:'The first summary is still being prepared.'}</p>}
    {data?.snapshot?.generated_at&&!data.snapshot.stale&&<p className="mt-2 text-slate-400">Summary updated {new Date(data.snapshot.generated_at).toLocaleString()}. SSE and Le Pass observations are analyzed separately.</p>}
  </aside>;
}
