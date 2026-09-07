import { useEffect, useState } from 'react';
import StreetcarPanel, { NetworkMap } from './StreetcarPanel';
import { priorityScenario, type PriorityData } from './priority-data';
import type { CorridorId, ExposureCategory } from './streetcar-data';

const corridors = [{ id: 'st_charles', name: 'St. Charles' }, { id: 'canal', name: 'Canal' }, { id: 'rampart', name: 'Rampart' }] as const;
const context: Record<ExposureCategory, { name: string; color: string }> = {
  signal_only: { name: 'Signal only', color: 'text-amber-300' },
  both: { name: 'Signal + passenger stop', color: 'text-purple-300' },
  stop_only: { name: 'Passenger stop only', color: 'text-cyan-300' },
  neither: { name: 'Neither nearby', color: 'text-slate-300' },
};
const minutes = (seconds: number | null | undefined) => seconds == null ? 'Unavailable' : `${(seconds / 60).toFixed(1)} min`;
const seconds = (value: number | null | undefined) => value == null ? 'Unavailable' : `${Math.round(value)} s`;
const count = (value: number) => value.toLocaleString();
const control = 'block w-full mt-1 bg-slate-900 border border-slate-600 rounded-lg px-3 py-2.5 text-sm text-slate-100';
type Filters = { corridor: CorridorId; from: string; to: string; dayType: string; hourFrom: number; hourTo: number };
type Scenario = { signalShare: number; recovery: number; includeOverlap: boolean };
type Props = { data?: PriorityData; initialFilters?: Partial<Filters>; initialProfileId?: string; initialScenario?: Partial<Scenario> };

function CandidateWaits({ data, corridorName }: { data: PriorityData; corridorName: string }) {
  const sites = [...data.waits.sites].sort((a, b) => b.total_seconds - a.total_seconds).slice(0, 5);
  return <section className="rounded-xl border border-slate-700 bg-slate-800 p-5 sm:p-6 mb-6" aria-labelledby="candidate-waits-heading">
    <h3 id="candidate-waits-heading" className="text-lg font-semibold">Candidate stationary waits near signals</h3>
    <p className="text-sm text-slate-300 mt-3">Working assumption: repeated stationary positions with reported zero speed near a signal are a candidate signal delay. A passenger stop beside that signal remains a mixed case; nearby does not prove the light was red.</p>
    <p className="text-sm text-amber-200 mt-3">All recorded vehicles on {corridorName}, across all directions in the selected dates and hours. These totals and site means are not specific to the one-way route selected above.</p>
    {data.waits.events === 0 ? <p className="text-sm text-slate-300 mt-4">No qualifying stationary episodes are available yet. Missing evidence is not zero signal delay.</p> : <>
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 mt-4">
        <div><p className="text-sm text-amber-300">Signal only</p><p className="text-2xl mt-1">{minutes(data.waits.signal_only_seconds)}</p></div>
        <div><p className="text-sm text-purple-300">Signal + passenger stop</p><p className="text-2xl mt-1">{minutes(data.waits.mixed_seconds)}</p></div>
        <div><p className="text-sm text-cyan-300">Passenger stop only</p><p className="text-2xl mt-1">{minutes(data.waits.stop_only_seconds)}</p></div>
      </div>
      {sites.length > 0 && <div className="overflow-x-auto mt-4"><table className="w-full text-left text-sm"><caption className="text-left text-slate-300 mb-2">Five sites with the most recorded stationary time</caption><thead><tr className="border-b border-slate-600"><th className="py-2 pr-4 font-medium">Site / context</th><th className="py-2 pr-4 font-medium">Mean per detected wait</th><th className="py-2 font-medium">Detected waits</th></tr></thead><tbody>
        {sites.map(site => <tr key={`${site.site_id}:${site.context}`} className="border-b border-slate-700"><td className="py-2 pr-4">{site.name}<p className={`text-xs mt-1 ${context[site.context].color}`}>{context[site.context].name}</p></td><td className="py-2 pr-4 whitespace-nowrap">{seconds(site.mean_seconds)}</td><td className="py-2">{count(site.events)}</td></tr>)}
      </tbody></table></div>}
    </>}
    <p className="text-xs text-slate-400 mt-4">Totals across recorded vehicles, not minutes per journey. {count(data.waits.events)} episodes from {count(data.waits.snapshots)} snapshots across whole selected dates{data.waits.from && data.waits.to ? ` (${data.waits.from} to ${data.waits.to})` : ''}. Durations use collector receipt times; more frequent SSE snapshots preserve position changes but do not improve the provider’s minute-precision GPS timestamps.</p>
  </section>;
}

export default function PriorityPanel({ data: suppliedData, initialFilters, initialProfileId, initialScenario }: Props) {
  const [filters, setFilters] = useState<Filters>({ corridor: 'st_charles', from: '', to: '', dayType: 'all', hourFrom: 0, hourTo: 23, ...initialFilters });
  const [scenario, setScenario] = useState<Scenario>({ signalShare: 50, recovery: 50, includeOverlap: true, ...initialScenario });
  const [selectedProfile, setSelectedProfile] = useState(initialProfileId || '');
  const [selectedSite, setSelectedSite] = useState('');
  const [showFilters, setShowFilters] = useState(false);
  const [showMap, setShowMap] = useState(false);
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [showAllWindows, setShowAllWindows] = useState(false);
  const [retry, setRetry] = useState(0);
  const [remote, setRemote] = useState<{ key: string; data?: PriorityData; error?: string }>({ key: '' });
  const parameters = new URLSearchParams({ corridor: filters.corridor, day_type: filters.dayType, hour_from: String(filters.hourFrom), hour_to: String(filters.hourTo) });
  if (filters.from) parameters.set('from', filters.from);
  if (filters.to) parameters.set('to', filters.to);
  const url = `/api/streetcar-priority?${parameters}`;
  const requestKey = `${url}#${retry}`;
  useEffect(() => {
    if (suppliedData) return;
    const controller = new AbortController();
    fetch(url, { signal: controller.signal }).then(async response => {
      if (!response.ok) {
        const body = await response.json().catch(() => null) as { error?: string } | null;
        throw new Error(body?.error || `Signal-priority data could not be loaded (${response.status}).`);
      }
      return response.json() as Promise<PriorityData>;
    }).then(data => { if (!controller.signal.aborted) setRemote({ key: requestKey, data }); })
      .catch((error: Error) => { if (!controller.signal.aborted) setRemote(previous => ({ key: requestKey, data: previous.data, error: error.message })); });
    return () => controller.abort();
  }, [url, requestKey, suppliedData]);
  const data = suppliedData ?? remote.data;
  const loading = !suppliedData && requestKey !== remote.key;
  const error = suppliedData ? undefined : remote.error;
  const from = filters.from || data?.selected_from || '';
  const to = filters.to || data?.selected_to || '';
  const profiles = data?.profiles ?? [];
  const profile = profiles.find(item => item.path_id === selectedProfile) ?? profiles.find(item => item.covered_meters > 0) ?? profiles[0];
  const estimate = profile ? priorityScenario(profile, scenario.signalShare, scenario.recovery, scenario.includeOverlap) : null;
  const candidate = profile?.signal_only_extra_seconds == null || profile?.mixed_extra_seconds == null ? null : profile.signal_only_extra_seconds + (scenario.includeOverlap ? profile.mixed_extra_seconds : 0);
  const windows = profile?.windows.filter(window => window.eligible && window.extra_seconds != null && window.signal_ids.length > 0)
    .sort((a, b) => (b.extra_seconds ?? 0) - (a.extra_seconds ?? 0)) ?? [];
  const visibleWindows = showAllWindows ? windows : windows.slice(0, 8);
  const site = data?.network.sites.find(item => item.id === selectedSite);
  const profileNetwork = data && profile ? { ...data.network, paths: data.network.paths.filter(path => path.id === profile.path_id) } : data?.network;
  const mixedSites = new Set(profile?.windows.filter(window => window.category === 'both').flatMap(window => [...window.signal_ids, ...window.stop_ids]) ?? []);
  const observed = profile?.observed_seconds;
  const afterWidth = observed != null && observed > 0 && estimate?.after_seconds != null ? 100 * estimate.after_seconds / observed : 0;

  return <section aria-labelledby="priority-heading">
    <div className="mb-6 max-w-3xl">
      <h2 id="priority-heading" className="text-2xl sm:text-3xl font-semibold tracking-tight">What could signal priority save?</h2>
      <p className="mt-3 text-base text-slate-300 leading-relaxed">Compare our observed streetcar travel times with faster passages over the same track. Then explore how much time better signals might recover.</p>
    </div>
    <div className="flex flex-wrap gap-2 mb-5" aria-label="Choose a streetcar corridor">
      {corridors.map(corridor => <button key={corridor.id} type="button" aria-pressed={filters.corridor === corridor.id}
        className={`rounded-full border px-5 py-2.5 text-sm font-medium ${filters.corridor === corridor.id ? 'bg-amber-300 border-amber-300 text-slate-950' : 'bg-slate-800 border-slate-600 text-slate-200 hover:bg-slate-700'}`}
        onClick={() => { setFilters(previous => ({ ...previous, corridor: corridor.id })); setSelectedProfile(''); setSelectedSite(''); }}>{corridor.name}</button>)}
    </div>
    {data && <div className="rounded-xl bg-slate-800 border border-slate-700 p-4 sm:p-5 mb-5">
      <div className="flex flex-wrap gap-4 items-end justify-between">
        <label className="text-sm text-slate-300 flex-1 min-w-52 max-w-xl">One-way route and direction<select value={profile?.path_id || ''} className={control}
          onChange={event => { setSelectedProfile(event.target.value); setSelectedSite(''); }}>
          {profiles.length === 0 && <option value="">No route profile available</option>}
          {profiles.map(item => <option key={item.path_id} value={item.path_id}>Route {item.route} · {item.headsign || `Direction ${item.direction}`}</option>)}
        </select></label>
        <div className="text-sm"><p className="text-slate-300">{from && to ? `${from} to ${to}` : 'Available observations'}</p>
          <button type="button" className="text-blue-300 underline mt-1" aria-expanded={showFilters} onClick={() => setShowFilters(value => !value)}>{showFilters ? 'Hide date and time filters' : 'Change dates and times'}</button></div>
      </div>
      {showFilters && <div className="grid grid-cols-2 md:grid-cols-5 gap-3 mt-4 pt-4 border-t border-slate-700">
        <label className="text-xs text-slate-300">From<input type="date" className={control} value={from} min={data.available_from || undefined} max={to || undefined} onChange={event => setFilters(previous => ({ ...previous, from: event.target.value, to }))} /></label>
        <label className="text-xs text-slate-300">Through<input type="date" className={control} value={to} min={from || undefined} max={data.available_to || undefined} onChange={event => setFilters(previous => ({ ...previous, from, to: event.target.value }))} /></label>
        <label className="text-xs text-slate-300">Days<select className={control} value={filters.dayType} onChange={event => setFilters(previous => ({ ...previous, dayType: event.target.value }))}><option value="all">All days</option><option value="weekday">Weekdays</option><option value="weekend">Weekends</option></select></label>
        <label className="text-xs text-slate-300">From hour<select className={control} value={filters.hourFrom} onChange={event => setFilters(previous => ({ ...previous, hourFrom: Number(event.target.value), hourTo: Math.max(previous.hourTo, Number(event.target.value)) }))}>{Array.from({ length: 24 }, (_, hour) => <option key={hour} value={hour}>{String(hour).padStart(2, '0')}:00</option>)}</select></label>
        <label className="text-xs text-slate-300">Through hour<select className={control} value={filters.hourTo} onChange={event => setFilters(previous => ({ ...previous, hourTo: Number(event.target.value), hourFrom: Math.min(previous.hourFrom, Number(event.target.value)) }))}>{Array.from({ length: 24 }, (_, hour) => <option key={hour} value={hour}>{String(hour).padStart(2, '0')}:59</option>)}</select></label>
        <p className="col-span-full text-xs text-slate-400">New Orleans local time. Weekday holidays count as weekdays. Up to 90 days per request.</p>
      </div>}
    </div>}
    {loading && <p className="rounded-xl bg-slate-800 p-10 text-center text-slate-300" role="status">Loading signal-priority estimates…</p>}
    {!loading && error && <div className="rounded-xl border border-amber-700 bg-slate-800 p-5" role="alert"><p className="text-amber-200">{error}</p><button type="button" className="text-blue-300 underline mt-3" onClick={() => setRetry(value => value + 1)}>Retry signal-priority data</button></div>}
    {!loading && !error && data && <>
      {data.processed_days!=null && data.selected_days!=null && data.processed_days<data.selected_days && <p className="rounded-lg border border-amber-600 bg-slate-800 p-4 mb-5 text-sm text-amber-200" role="status">Processed history covers {data.processed_days} of {data.selected_days} selected days. Estimates use the completed days available so far.</p>}
      <div className="rounded-xl border border-slate-600 bg-slate-800 p-5 sm:p-6 mb-6">
        <div className="flex flex-wrap items-start justify-between gap-3 mb-5">
          <div><h3 className="text-lg font-semibold">Covered portion of a one-way journey</h3>
            <p className="text-sm text-slate-300 mt-1">{profile ? `Route ${profile.route} · ${profile.headsign || `Direction ${profile.direction}`}` : 'Choose a route profile when data is available.'}</p></div>
          <div className="rounded-lg bg-slate-950 px-4 py-2 text-sm"><strong className={profile && profile.coverage_pct < 50 ? 'text-amber-300' : 'text-slate-100'}>{profile ? `${profile.coverage_pct.toFixed(0)}% of the route covered` : 'Coverage unavailable'}</strong>
            {profile && <p className="text-xs text-slate-400 mt-1">{(profile.covered_meters / 1000).toFixed(1)} of {(profile.route_meters / 1000).toFixed(1)} km · {profile.eligible_windows} track windows</p>}</div>
        </div>
        <p className="text-sm text-slate-300 mb-5">These approximate times add one passage through each covered track window. Gaps and terminals are omitted; this is not a complete end-to-end journey time.</p>
        {data.status === 'not_ready' || observed == null ? <p className="text-amber-200 py-3">Not enough repeated passages to estimate travel time for this selection. Each window needs at least {data.method.min_passages} passages across {data.method.min_days} days in a comparable time band.</p> : <>
          <div className="grid grid-cols-2 lg:grid-cols-3 gap-4">
            <div className="rounded-lg bg-slate-900 p-4"><p className="text-sm text-slate-300">Observed travel time</p><p className="text-3xl sm:text-4xl font-semibold mt-2">{minutes(observed)}</p></div>
            <div className="rounded-lg bg-slate-900 p-4"><p className="text-sm text-slate-300">With your priority assumptions</p><p className="text-3xl sm:text-4xl font-semibold mt-2">{minutes(estimate?.after_seconds)}</p></div>
            <div className="col-span-2 lg:col-span-1 rounded-lg border border-amber-300/60 bg-amber-300/10 p-4"><p className="text-sm text-amber-200">Illustrative time saved</p><p className="text-3xl sm:text-4xl font-semibold text-amber-300 mt-2">{minutes(estimate?.saved_seconds)}</p><p className="text-xs text-amber-200 mt-2">{estimate?.reduction_pct == null ? 'Unavailable' : `${estimate.reduction_pct.toFixed(1)}% of the covered travel time`}</p></div>
          </div>
          {estimate?.after_seconds != null && <div className="mt-5 space-y-3" aria-label="Observed and illustrative travel-time comparison">
            <div className="flex gap-3 items-center"><span className="text-xs text-slate-400 w-20 shrink-0">Observed</span><div className="h-3 rounded-full bg-slate-500 flex-1" /></div>
            <div className="flex gap-3 items-center"><span className="text-xs text-slate-400 w-20 shrink-0">Scenario</span><div className="h-3 rounded-full bg-slate-950 flex-1"><div className="h-3 rounded-full bg-amber-300 transition-all" style={{ width: `${afterWidth}%` }} /></div></div>
          </div>}
        </>}
      </div>
      <div className="rounded-xl border border-slate-700 bg-slate-800 p-5 sm:p-6 mb-6">
        <h3 className="text-lg font-semibold">Set the priority assumptions</h3>
        {candidate == null ? <p className="text-sm text-slate-300 mt-2">Not enough repeated observations to quantify extra time near signals in this selection.</p> : <p className="text-sm text-slate-300 mt-2 leading-relaxed"><strong className="text-slate-100">{minutes(candidate)} of extra time near signals</strong> compared with faster passages over those same track windows. Some of this can be boarding, traffic queues, or driving variation. The actual signal-related delay has not been isolated.</p>}
        <p className="text-xs text-amber-200 mt-2">The starting 50% / 50% values are illustrative assumptions, not measured effects.</p>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mt-5">
          <label className="text-sm text-slate-200">How much of the extra time is caused by signals?<span className="block text-2xl font-semibold text-amber-300 mt-2">{scenario.signalShare}%</span>
            <input className="w-full accent-amber-300 mt-3" type="range" min={0} max={100} step={5} value={scenario.signalShare} onChange={event => setScenario(previous => ({ ...previous, signalShare: Number(event.target.value) }))} />
            <span className="flex justify-between text-xs text-slate-400"><span>None</span><span>All extra time</span></span></label>
          <label className="text-sm text-slate-200">How much of that signal delay would priority remove?<span className="block text-2xl font-semibold text-amber-300 mt-2">{scenario.recovery}%</span>
            <input className="w-full accent-amber-300 mt-3" type="range" min={0} max={100} step={5} value={scenario.recovery} onChange={event => setScenario(previous => ({ ...previous, recovery: Number(event.target.value) }))} />
            <span className="flex justify-between text-xs text-slate-400"><span>No recovery</span><span>Full recovery</span></span></label>
        </div>
        <label className="flex items-start gap-3 text-sm mt-5"><input type="checkbox" className="accent-amber-300 mt-1" checked={scenario.includeOverlap} onChange={event => setScenario(previous => ({ ...previous, includeOverlap: event.target.checked }))} /><span>Include signals beside passenger stops<span className="block text-xs text-slate-400 mt-1">{minutes(profile?.mixed_extra_seconds)} of mixed extra time; boarding and signal effects cannot be separated here.</span></span></label>
        <p className="text-xs text-slate-400 mt-4">Scenario saving = selected extra time × {scenario.signalShare}% signal-related share × {scenario.recovery}% recovered by priority. No saving is extrapolated onto unobserved parts of the route.</p>
      </div>
      <CandidateWaits data={data} corridorName={corridors.find(corridor => corridor.id === filters.corridor)?.name || filters.corridor} />
      <div className="rounded-xl border border-slate-700 bg-slate-800 p-5 sm:p-6 mb-6">
        <div className="flex flex-wrap justify-between gap-3 items-center"><h3 className="text-lg font-semibold">Where the extra time adds up</h3><button type="button" className="text-sm text-blue-300 underline" aria-expanded={showMap} onClick={() => setShowMap(value => !value)}>{showMap ? 'Hide map' : 'Show stops and signals on the map'}</button></div>
        <p className="text-sm text-slate-300 mt-2 mb-4">Distinct track windows near signals, ranked by extra travel time. A signal beside a passenger stop remains a mixed case.</p>
        {showMap && profileNetwork && profile && <div className="mb-5"><NetworkMap key={profile.path_id} network={profileNetwork} corridor={filters.corridor} direction={profile.direction} selectedSite={selectedSite} mixedSites={mixedSites} onSelect={setSelectedSite} />
          {site && <div className="mt-3 rounded-lg bg-slate-900 p-4 text-sm"><p className="font-medium">{site.name}</p><p className="text-slate-400 mt-1">{site.kind === 'stop' ? 'Passenger stop' : site.kind === 'rail_signal' ? 'Rail signal; function unverified and excluded from comparisons' : 'Road signal'} · {site.verification === 'gtfs' ? 'GTFS location' : site.verification === 'mapillary' ? 'Mapillary detection corroborates location' : 'OSM location, imagery unverified'}</p>
            {site.source_ids.filter(id => /^mapillary\/image\/\d+$/.test(id)).map(id => <a key={id} className="block text-blue-300 underline mt-2" href={`https://www.mapillary.com/app/?pKey=${encodeURIComponent(id.split('/')[2])}`} target="_blank" rel="noreferrer">View reviewed Mapillary imagery</a>)}</div>}
        </div>}
        {visibleWindows.length === 0 ? <p className="rounded-lg bg-slate-900 p-4 text-slate-400">No signal-adjacent windows have enough repeat observations for this selection.</p> : <div className="overflow-x-auto"><table className="w-full text-sm text-left"><thead><tr className="border-b border-slate-600"><th className="py-3 pr-4 font-medium text-slate-300">Track window</th><th className="py-3 pr-4 font-medium text-slate-300">Nearby</th><th className="py-3 pr-4 font-medium text-slate-300">Extra per passage</th><th className="py-3 font-medium text-slate-300">Evidence</th></tr></thead>
          <tbody>{visibleWindows.map(window => {
            const sensitivity = window.extra_p10_seconds == null || window.extra_p30_seconds == null ? 'Benchmark sensitivity unavailable.' : `Alternative P10 / P30 faster-passage benchmarks give ${seconds(Math.min(window.extra_p10_seconds, window.extra_p30_seconds))} to ${seconds(Math.max(window.extra_p10_seconds, window.extra_p30_seconds))} of extra time.`;
            return <tr key={window.id} className="border-b border-slate-700"><td className="py-3 pr-4"><button type="button" className="text-left text-blue-300 underline" onClick={() => { setShowMap(true); setSelectedSite(window.signal_ids[0] || window.stop_ids[0] || ''); }}>{window.name}</button><p className="text-xs text-slate-400 mt-1">{Math.round(window.to_meters - window.from_meters)} m of track</p></td><td className={`py-3 pr-4 ${context[window.category].color}`}>{context[window.category].name}</td><td className="py-3 pr-4 font-medium whitespace-nowrap"><span tabIndex={0} title={sensitivity} aria-label={`Extra time ${seconds(window.extra_seconds)}. ${sensitivity}`}>{seconds(window.extra_seconds)}</span></td><td className="py-3 text-slate-400 whitespace-nowrap">{count(window.passages)} passages<br /><span className="text-xs">{window.days} days</span></td></tr>;
          })}</tbody>
        </table></div>}
        {windows.length > 8 && <button type="button" className="mt-4 text-sm text-blue-300 underline" onClick={() => setShowAllWindows(value => !value)}>{showAllWindows ? 'Show first 8 windows' : `Show all ${windows.length} windows`}</button>}
        <p className="text-xs text-slate-400 mt-4">Each track window counts once. Nearby signal and stop markers can refer to the same window; their times are not added again.</p>
      </div>
      <details className="text-sm text-slate-300 mb-6"><summary className="cursor-pointer font-medium">How this estimate works and what it cannot tell us</summary>
        <p className="mt-3">The faster-passage benchmark is the {data.method.baseline_percentile}th percentile for the same track window, direction, weekday/weekend group, and {data.method.time_band_hours}-hour band. A window needs at least {data.method.min_passages} passages across {data.method.min_days} days. It is a comparison with our own observations, not a measurement of travel under a green signal.</p>
        <ul className="list-disc pl-5 mt-3 space-y-2">{data.method.limitations.map(limitation => <li key={limitation}>{limitation}</li>)}</ul>
        <p className="mt-3">Mapped signals © <a href="https://www.openstreetmap.org/copyright" className="text-blue-300 underline">OpenStreetMap contributors</a>. {data.network.mapillary_status} A detected light does not prove it controls the streetcar movement.</p>
        <p className="mt-3 text-xs text-slate-400">{data.updated_at ? `Calculated ${data.updated_at}. ` : ''}Model: {data.method.name}.</p>
      </details>
    </>}
    <div className="border-t border-slate-700 pt-5 mt-6"><button type="button" className="text-sm text-blue-300 underline" aria-expanded={showAdvanced} onClick={() => setShowAdvanced(value => !value)}>{showAdvanced ? 'Hide advanced speed and timing diagnostics' : 'Show advanced speed and timing diagnostics'}</button>
      {showAdvanced && <div className="mt-5"><StreetcarPanel initialFilters={{ corridor: filters.corridor, from, to, dayType: filters.dayType, hourFrom: filters.hourFrom, hourTo: filters.hourTo, direction: profile?.direction || 'all' }} /></div>}
    </div>
  </section>;
}
