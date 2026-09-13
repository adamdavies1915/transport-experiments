import { useEffect, useState } from 'react';
import type { SourceQualityData } from './summary-data';

const timestamp = (value: string) => new Date(value).toLocaleString('en-US', { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit', timeZone: 'America/Chicago', timeZoneName: 'short' });

export default function SourceQuality({ data: supplied }: { data?: SourceQualityData } = {}) {
  const [saved, setData] = useState<SourceQualityData | null>(null);
  const [failed, setFailed] = useState(false);
  const [attempt, setAttempt] = useState(0);
  useEffect(() => {
    if (supplied) return;
    const controller = new AbortController();
    fetch('/api/source-quality', { signal: controller.signal }).then(async response => {
      if (!response.ok) throw new Error('Unavailable');
      return response.json() as Promise<SourceQualityData>;
    }).then(value => { if (!controller.signal.aborted) { setData(value); setFailed(false); } })
      .catch(() => { if (!controller.signal.aborted) setFailed(true); });
    return () => controller.abort();
  }, [supplied, attempt]);
  const data = supplied ?? saved;
  const snapshot = data?.snapshot;
  const processing = snapshot?.processing;
  return <aside aria-label="Observation source coverage" className="rounded-xl border border-slate-700/70 px-4 py-4 sm:px-5 text-xs text-slate-300">
    {snapshot?.stale && <p className="mb-3 text-amber-200" role="status">{!snapshot.generated_at ? 'The first analysis is still being prepared.' : snapshot.refresh_error ? 'Summary refresh is unavailable; showing the saved analysis.' : 'The saved analysis is older than expected; the next result is pending.'}</p>}
    <div className="flex flex-wrap items-center gap-x-5 gap-y-2"><strong className="text-slate-300 font-medium">Collection & analysis</strong>{snapshot?.generated_at && <p className="text-slate-400">Last analysis <time dateTime={snapshot.generated_at}>{timestamp(snapshot.generated_at)}</time>{processing ? ` · Daily processing · service date ${processing.service_date}` : ''}</p>}</div>
    {!data?.sources.length && <p className="mt-2 text-slate-400">{failed ? <>Source coverage is temporarily unavailable. <button type="button" className="text-blue-300 underline" onClick={() => setAttempt(value => value + 1)}>Retry source coverage</button></> : 'Waiting for source coverage from the collector.'}</p>}
    {!!data?.sources.length && <details className="mt-3">
      <summary className="story-disclosure">Data coverage and collection details</summary>
      <p className="mt-3 text-slate-400">Receipt and provider timestamps come from durably saved batches. Counts and date ranges belong to the saved analysis snapshot and update when it is published; they are not live counters or the sample size of the selected study.</p>
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-5 mt-4">{data.sources.map(source => <div key={source.id} className="border-l border-slate-600 pl-3"><strong className="text-slate-100">{source.label}</strong><p className="mt-1">{source.status}{source.last_received_at ? ` · last collection ${timestamp(source.last_received_at)}` : ''}</p>{source.message && <p className="text-slate-400 mt-2">{source.message}</p>}<p className="mt-2 text-slate-400">Latest provider timestamp: {source.last_provider_at ? timestamp(source.last_provider_at) : 'Not supplied'}</p><p className="mt-2 text-slate-400">Saved analysis snapshot: {source.observations == null ? 'Count not available' : `${source.observations.toLocaleString()} observation receipts`}{source.from && source.to ? ` · received ${timestamp(source.from)} through ${timestamp(source.to)}` : ''}.</p></div>)}</div>
      <p className="mt-4 text-slate-400">A repeated provider timestamp can arrive in a new receipt. Receipt time is not GPS time. Both feeds inform the studies; overlapping observations are not added together.</p>
    </details>}
    {processing && <details className="mt-3"><summary className="story-disclosure">Daily analysis details</summary><p className="mt-3 text-slate-400">Inputs collected through <time dateTime={processing.input_cutoff}>{timestamp(processing.input_cutoff)}</time>. Result accepted <time dateTime={processing.completed_at}>{timestamp(processing.completed_at)}</time>. Newer collection receipts are not included in this analysis.</p></details>}
  </aside>;
}
