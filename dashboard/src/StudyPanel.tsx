import { useMemo, useState } from 'react';
import type { RowStudyData, SignalStudyData, StudyContext, StudyFilters, StudyRowComparison, StudySignalSummary, StudySource } from '../../src/transit-study-types';
import type { SummarySnapshotStatus } from './summary-data';
import { rowStudyFromCells, signalStudyFromCells } from './transit-study-filter';
import { rankRowEvidence, rankSignalEvidence, selectRowEvidence, selectSignalEvidence } from './study-evidence';
import StudyMap from './StudyMap';
import { useCachedStudy } from './hooks/useCachedStudy';

type Data = (RowStudyData | SignalStudyData) & { available_from?: string | null; available_to?: string | null; filters?: StudyFilters; snapshot?: SummarySnapshotStatus };
const input = 'mt-1 block w-full rounded-lg border border-slate-600 bg-slate-900 px-3 py-2.5 text-sm text-slate-100';
const context: Record<StudyContext, string> = { signal_only: 'Signal only', both: 'Signal + passenger stop', stop_only: 'Passenger stop only', neither: 'No signal or passenger stop nearby' };
const sourceLabel = (source: StudySource) => source === 'lepass' ? 'Le Pass' : 'SSE';
const minutes = (seconds: number | null) => seconds == null ? 'Not available' : `${(seconds / 60).toFixed(seconds !== 0 && Math.abs(seconds) < 3 ? 2 : 1)} min`;
const seconds = (value: number | null) => value == null ? 'Not available' : `${value === 0 ? '0' : Math.abs(value) < 1 ? value.toFixed(1) : Math.round(value).toLocaleString()} sec`;
const percent = (value: number | null) => value == null ? 'Not available' : value > 0 && value < 0.01 ? '<1%' : `${(value * 100).toFixed(0)}%`;
const count = (value: number) => value.toLocaleString();
const comparisonKey = (row: StudyRowComparison) => `${row.source}:${row.id}`;
const signalKey = (row: StudySignalSummary) => `${row.source}:${row.mode}:${row.site_id}:${row.route_id}:${row.direction_id}:${row.context}`;

function ComparisonDetail({ row }: { row: StudyRowComparison }) {
  const ready = row.status === 'ready';
  const observed = ready ? row : row.observed;
  const timingUncertain = observed && observed.shared_extra_lower_seconds_per_km != null && observed.shared_extra_upper_seconds_per_km != null
    && observed.shared_extra_lower_seconds_per_km <= 0 && observed.shared_extra_upper_seconds_per_km >= 0;
  return <div className="rounded-xl border border-slate-600 bg-slate-800 p-5 sm:p-6" aria-label="Selected roadway evidence">
    <p className="text-sm text-slate-300">Route {row.route_id} · Direction {row.direction_id} · {sourceLabel(row.source)} observations</p>
    <h3 className="font-semibold text-xl mt-2">Time to travel one kilometre</h3>
    {!ready && observed && <p className="text-sm text-amber-200 mt-3"><strong>Preliminary observations</strong> · Below the repeated-observation threshold.</p>}
    {observed ? <>
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-5 mt-5">
        <div><p className="text-sm text-emerald-300">Reserved right of way</p><p className="text-3xl font-semibold mt-2">{minutes(observed.reserved_seconds_per_km)}</p></div>
        <div><p className="text-sm text-orange-300">Shared roadway</p><p className="text-3xl font-semibold mt-2">{minutes(observed.shared_seconds_per_km)}</p></div>
        <div><p className="text-sm text-slate-300">Shared minus reserved</p><p className="text-3xl font-semibold mt-2">{minutes(observed.shared_extra_seconds_per_km)}</p><p className="text-xs text-slate-400 mt-2">per km · negative means less recorded time</p></div>
      </div>
      <p className="text-sm text-slate-300 mt-4">{count(row.reserved_passages)} reserved and {count(row.shared_passages)} shared passages on {row.matched_dates} matched dates.{row.observed?.dates.length ? ` ${row.observed.dates[0]} to ${row.observed.dates[row.observed.dates.length - 1]}.` : ''}</p>
      {timingUncertain && <p className="text-sm text-amber-200 mt-3">Timing uncertainty spans zero: these observations cannot yet establish which roadway class is quicker.</p>}
      {!ready && <p className="text-xs text-slate-400 mt-3">We require 30 completed passages in each roadway class across at least 7 matched dates before publishing a day-to-day interval.</p>}
    </> : <p className="text-amber-200 mt-4" role="status">No comparable shared observation dates yet. A comparison needs reserved and shared passages from the same feed on the same dates and under matched conditions.</p>}
    <p className="text-sm text-slate-300 mt-4">This compares different places under matched conditions; it does not isolate the causal effect of roadway design.</p>
    <details className="mt-5 border-t border-slate-700 pt-4 text-sm">
      <summary className="cursor-pointer text-blue-300">Sample and uncertainty</summary>
      <p className="mt-3 text-slate-300">{row.day_type}s · {String(row.time_band * 4).padStart(2, '0')}:00–{String(row.time_band * 4 + 3).padStart(2, '0')}:59 · {context[row.context]}. {row.signal_count} mapped signals and {row.stop_count} passenger stops per window.</p>
      {row.observed?.dates.length ? <p className="mt-3 text-slate-300">Matched service dates: {row.observed.dates.join(', ')}.</p> : null}
      {observed && <p className="text-slate-300 mt-3">Timing range for the difference: {minutes(observed.shared_extra_lower_seconds_per_km)} to {minutes(observed.shared_extra_upper_seconds_per_km)} per km. Sampling and timestamp precision are included; GPS error is not. This is not a confidence interval.</p>}
      {ready && <p className="text-slate-300 mt-3">95% interval across service dates: {minutes(row.shared_extra_ci_lower_seconds_per_km)} to {minutes(row.shared_extra_ci_upper_seconds_per_km)} per km. This measures day-to-day variation separately from the clock range above.</p>}
    </details>
  </div>;
}

function SignalDetail({ row, name }: { row: StudySignalSummary; name: string }) {
  const ready = row.status === 'ready';
  const sampled = row.evaluable_encounters > 0 && row.detected_wait_seconds_per_encounter != null;
  const detected = row.detected_wait_encounters > 0 && row.wait_events > 0;
  return <div className="rounded-xl border border-slate-600 bg-slate-800 p-5 sm:p-6" aria-label="Selected signal evidence">
    <p className="text-sm text-slate-300">Route {row.route_id} · Direction {row.direction_id} · {sourceLabel(row.source)} observations</p>
    <h3 className="text-xl font-semibold mt-2">{name}</h3>
    {!ready && sampled && <p className="text-sm text-amber-200 mt-3"><strong>Preliminary observations</strong> · Below the repeated-observation threshold.</p>}
    {sampled && <div className="mt-5">
      {detected ? <div className="grid grid-cols-1 sm:grid-cols-2 gap-5">
        <div><p className="text-sm text-slate-300">Detected wait per encounter</p><p className="text-4xl font-semibold mt-2">{seconds(row.detected_wait_seconds_per_encounter)}</p></div>
        <div><p className="text-sm text-slate-300">Mean per detected wait</p><p className="text-3xl font-semibold mt-2">{seconds(row.mean_detected_wait_seconds)}</p></div>
      </div> : <p className="text-2xl font-semibold">No waits detected</p>}
      <p className="text-sm text-slate-300 mt-3">{count(row.detected_wait_encounters)} of {count(row.encounters)} complete encounters had a detected wait ({percent(row.detected_wait_probability)}). {count(row.wait_events)} detected waits.</p>
      <p className="text-xs text-slate-400 mt-2">{count(row.evaluable_encounters)} of {count(row.encounters)} encounters had enough sampling to evaluate waiting, across {row.evaluable_dates} dates.{row.observed_from && row.observed_to ? ` Observed ${row.observed_from} to ${row.observed_to}.` : ''}</p>
      <p className="text-xs text-slate-400 mt-2">{count(row.encounters - row.evaluable_encounters)} encounters had insufficient sampling; they remain in the per-encounter denominator. Short waits can be missed.</p>
      {!ready && <p className="text-xs text-slate-400 mt-3">We require at least 30 evaluable encounters across 7 dates before publishing a day-to-day interval.</p>}
    </div>}
    <p className="text-sm text-slate-300 mt-4">Stationary time near a signal is a candidate wait. It does not establish that a red light caused it.</p>
    {row.context === 'both' && <p className="mt-4 rounded-lg bg-purple-300/10 p-3 text-sm text-purple-200">A passenger stop overlaps this signal. Boarding and signal delay cannot be separated here, so a signal-priority saving is not calculated.</p>}
    {row.evaluable_encounters === 0 ? <p className="mt-4 text-sm text-amber-200">No encounter had enough sampling to evaluate waiting. More observations are needed before showing a priority scenario.</p> : !ready && row.context !== 'both' ? <p className="mt-4 text-sm text-slate-300">Priority scenarios remain unavailable until the headline sample threshold is met.</p> : null}
    <details className="mt-5 border-t border-slate-700 pt-4 text-sm">
      <summary className="cursor-pointer text-blue-300">Sample and uncertainty</summary>
      <p className="text-slate-300 mt-3">{count(row.encounters)} complete encounters · {count(row.detected_wait_encounters)} with a detected wait · {count(row.wait_events)} detected waits. {count(row.encounters - row.evaluable_encounters)} encounters had insufficient sampling; they remain in the per-encounter denominator.</p>
      {ready && sampled && <p className="text-slate-300 mt-3">95% interval across service dates: {seconds(row.detected_wait_seconds_per_encounter_ci_lower)} to {seconds(row.detected_wait_seconds_per_encounter_ci_upper)} per encounter. Clock uncertainty is separate.</p>}
    </details>
    {ready && row.evaluable_encounters > 0 && row.context !== 'both' && <details className="mt-4 border-t border-slate-700 pt-4">
      <summary className="cursor-pointer text-sm text-blue-300">What could signal priority save here?</summary>
      <p className="text-sm text-slate-300 mt-3">If priority recovered some detected waiting time:</p>
      <div className="grid grid-cols-3 gap-3 mt-3">{row.recovery_seconds_per_encounter.map(scenario => <div key={scenario.percent}><p className="text-xs text-slate-300">{scenario.percent}% recovered</p><p className="text-xl font-semibold text-amber-300 mt-1">{seconds(scenario.seconds)}</p></div>)}</div>
      <p className="text-xs text-slate-400 mt-3">Illustrative seconds saved per encounter. These assumptions are not measured priority effects and do not prove signal control. Site figures are not summed into an end-to-end journey.</p>
    </details>}
  </div>;
}

export default function StudyPanel({ kind, data: supplied, initialFilters }: { kind: 'row' | 'signals'; data?: Data; initialFilters?: StudyFilters }) {
  const [filters, setFilters] = useState<StudyFilters>({ mode: 'streetcar', hour_from: 0, hour_to: 23, ...initialFilters });
  const [contextFilter, setContextFilter] = useState<StudyContext | ''>('');
  const [selected, setSelected] = useState(''), [siteId, setSiteId] = useState(''), [moreFilters, setMoreFilters] = useState(false), [showAllRows, setShowAllRows] = useState(false);
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(filters)) if (value != null && value !== '') params.set(key, String(value));
  const url = `/api/${kind === 'row' ? 'row' : 'signal'}-study?${params}`;
  const { data: raw, loading, error, refreshing, retry } = useCachedStudy<Data>(url, supplied);
  const data: Data | undefined = useMemo(() => {
    if (supplied) return 'comparisons' in supplied ? rowStudyFromCells(supplied, filters) : signalStudyFromCells(supplied, filters);
    // Older saved HTTP responses predate descriptive comparison fields. Keep
    // the shared request cache, deriving these rates once per response/filter.
    if (raw && 'comparisons' in raw && raw.comparisons.some(row => row.matched_dates > 0 && !row.observed)) return rowStudyFromCells(raw, filters);
    return raw;
  }, [supplied, raw, filters]);
  const from = filters.from ?? raw?.from ?? '', to = filters.to ?? raw?.to ?? '';
  const routes = [...new Set((raw?.network.paths ?? []).filter(path => path.mode === filters.mode).map(path => path.route_id))].sort((a, b) => Number(a) - Number(b) || a.localeCompare(b));
  const directions = [...new Set((raw?.network.paths ?? []).filter(path => path.mode === filters.mode && (!filters.route_id || path.route_id === filters.route_id)).map(path => path.direction_id))].sort();
  const comparisons = useMemo(() => {
    const rows = data && 'comparisons' in data ? data.comparisons.filter(row => !contextFilter || row.context === contextFilter) : [];
    return filters.source ? [...rows].sort(rankRowEvidence) : selectRowEvidence(rows);
  }, [data, contextFilter, filters.source]);
  const signals = useMemo(() => {
    const rows = data && 'signals' in data ? data.signals.filter(row => !contextFilter || row.context === contextFilter) : [];
    return filters.source ? [...rows].sort(rankSignalEvidence) : selectSignalEvidence(rows);
  }, [data, contextFilter, filters.source]);
  const comparison = comparisons.find(row => comparisonKey(row) === selected) ?? comparisons[0];
  const defaultSignal = signals.find(row => row.context === 'signal_only' && row.status === 'ready') ?? signals[0];
  const signal = selected ? signals.find(row => signalKey(row) === selected) ?? defaultSignal : siteId ? signals.find(row => row.site_id === siteId) : defaultSignal;
  const coverage = useMemo(() => {
    if (!raw || !('comparisons' in raw)) return undefined;
    const sources: StudySource[] = comparison ? [comparison.source] : filters.source ? [filters.source] : ['sse', 'lepass'];
    return sources.map(source => ({ source, ...rowStudyFromCells(raw, { ...filters, source }).coverage }))
      .sort((a, b) => b.classified_passages - a.classified_passages || b.passages - a.passages)[0];
  }, [raw, filters, comparison]);
  const name = (id: string) => data?.network.sites.find(site => site.id === id)?.name ?? id;
  const change = (patch: Partial<StudyFilters>) => { setFilters(previous => ({ ...previous, ...patch })); setSelected(''); setSiteId(''); setShowAllRows(false); };
  const sourceName = filters.source ? sourceLabel(filters.source) : 'both feeds';
  const resultCount = kind === 'row' ? comparisons.length : signals.length;
  return <section aria-labelledby="study-heading">
    <div className="max-w-3xl mb-6">
      <h2 id="study-heading" className="text-2xl sm:text-3xl font-semibold">{kind === 'row' ? 'How much time does shared roadway add?' : 'Where do vehicles wait near signals?'}</h2>
      <p className="mt-3 text-slate-300 leading-relaxed">{kind === 'row' ? 'Compare shared streets with reserved track under similar operating conditions.' : 'Find recurring waits near traffic lights, then explore what priority might recover.'}</p>
    </div>
    <div className="rounded-xl border border-slate-700 bg-slate-800 p-4 sm:p-5 mb-5">
      <div className="grid grid-cols-2 gap-3">
        <label className="text-xs text-slate-300">Service<select className={input} aria-label="Service" value={filters.mode} onChange={event => change({ mode: event.target.value as StudyFilters['mode'], route_id: undefined, direction_id: undefined })}><option value="streetcar">Streetcars</option><option value="bus">Buses</option></select></label>
        <label className="text-xs text-slate-300">Route<select className={input} aria-label="Route" value={filters.route_id ?? ''} onChange={event => change({ route_id: event.target.value || undefined, direction_id: undefined })}><option value="">All routes</option>{routes.map(route => <option key={route} value={route}>Route {route}</option>)}</select></label>
      </div>
      <div className="flex flex-wrap items-center justify-between gap-3 mt-4"><p className="text-xs text-slate-400">{from && to ? `${from} to ${to}` : 'Latest available observations'} · {filters.source ? sourceName : 'Both feeds · one feed per result'}</p><button type="button" className="text-sm text-blue-300 underline" aria-expanded={moreFilters} onClick={() => setMoreFilters(value => !value)}>{moreFilters ? 'Hide advanced filters' : 'Advanced filters'}</button></div>
      {moreFilters && <div className="grid grid-cols-2 sm:grid-cols-3 gap-3 mt-4 border-t border-slate-700 pt-4">
        <label className="text-xs text-slate-300">Observation source<select className={input} aria-label="Observation source" value={filters.source ?? ''} onChange={event => change({ source: event.target.value as StudyFilters['source'] || undefined })}><option value="">Both feeds</option><option value="sse">SSE</option><option value="lepass">Le Pass</option></select></label>
        <label className="text-xs text-slate-300">Direction<select className={input} aria-label="Direction" value={filters.direction_id ?? ''} onChange={event => change({ direction_id: event.target.value || undefined })}><option value="">Both · separate results</option>{directions.map(direction => <option key={direction} value={direction}>Direction {direction}</option>)}</select></label>
        <label className="text-xs text-slate-300">Nearby stops and signals<select className={input} aria-label="Nearby stops and signals" value={contextFilter} onChange={event => { setContextFilter(event.target.value as StudyContext | ''); setSelected(''); setSiteId(''); }}><option value="">All contexts · separate results</option>{Object.entries(context).filter(([value]) => kind === 'row' || value === 'signal_only' || value === 'both').map(([value, label]) => <option key={value} value={value}>{label}</option>)}</select></label>
        <label className="text-xs text-slate-300">From<input className={input} type="date" value={from} min={raw?.available_from ?? undefined} max={to || undefined} onChange={event => change({ from: event.target.value || undefined, to: to || undefined })} /></label>
        <label className="text-xs text-slate-300">Through<input className={input} type="date" value={to} min={from || undefined} max={raw?.available_to ?? undefined} onChange={event => change({ from: from || undefined, to: event.target.value || undefined })} /></label>
        <label className="text-xs text-slate-300">Days<select className={input} value={filters.day_type ?? 'all'} onChange={event => change({ day_type: event.target.value === 'all' ? undefined : event.target.value as StudyFilters['day_type'] })}><option value="all">All days</option><option value="weekday">Weekdays</option><option value="weekend">Weekends</option></select></label>
        <label className="text-xs text-slate-300">From hour<select className={input} value={filters.hour_from} onChange={event => change({ hour_from: Number(event.target.value), hour_to: Math.max(filters.hour_to ?? 23, Number(event.target.value)) })}>{Array.from({ length: 24 }, (_, hour) => <option key={hour} value={hour}>{String(hour).padStart(2, '0')}:00</option>)}</select></label>
        <label className="text-xs text-slate-300">Through hour<select className={input} value={filters.hour_to} onChange={event => change({ hour_to: Number(event.target.value), hour_from: Math.min(filters.hour_from ?? 0, Number(event.target.value)) })}>{Array.from({ length: 24 }, (_, hour) => <option key={hour} value={hour}>{String(hour).padStart(2, '0')}:59</option>)}</select></label>
        <div className="col-span-full flex flex-wrap gap-4 text-xs"><button className="text-blue-300 underline" type="button" onClick={() => { change({ source: undefined, direction_id: undefined, from: undefined, to: undefined, day_type: undefined, hour_from: 0, hour_to: 23 }); setContextFilter(''); }}>Reset advanced filters</button><span className="text-slate-400">New Orleans time. Weekday holidays count as weekdays.</span></div>
      </div>}
    </div>
    {loading && <p className="rounded-xl bg-slate-800 p-10 text-center text-slate-300" role="status">Loading {kind === 'row' ? 'roadway' : 'signal'} observations…</p>}
    {!loading && error && <div className="rounded-xl border border-amber-700 bg-slate-800 p-5 mb-4" role="alert"><p>{data ? 'Refresh unavailable; showing saved observations for this selection.' : error}</p><button type="button" className="text-blue-300 underline mt-3" onClick={retry}>Retry this study</button></div>}
    {refreshing && <p className="text-xs text-slate-400 mb-3" role="status">Updating observations…</p>}
    {!loading && data && <>
      {data.snapshot?.stale && <p className="text-sm text-amber-200 mb-4">Showing saved observations; the next collector summary is pending.</p>}
      {resultCount > 0 && <p className="text-xs text-slate-400 mb-3">{selected || siteId ? 'Selected result' : kind === 'signals' && signal?.context === 'signal_only' && signal.status === 'ready' ? 'Best-supported result away from passenger stops' : 'Best-supported result'} · {filters.source ? `${sourceName} observations` : 'Selected from both feeds by date coverage and sample size'}</p>}
      {kind === 'row' && comparison ? <ComparisonDetail row={comparison} /> : kind === 'signals' && signal ? <SignalDetail row={signal} name={name(signal.site_id)} /> : <div className="rounded-xl border border-slate-600 bg-slate-800 p-6" role="status"><h3 className="text-lg font-semibold">Collecting {sourceName} {kind === 'row' ? 'roadway comparisons' : 'signal encounters'}</h3><p className="text-slate-300 mt-2">{kind === 'row' ? 'A comparison needs reviewed roadway classifications and repeated passages in both classes on the same dates.' : 'No complete signal encounters are available for this selection yet. This does not mean there was no signal delay.'}</p></div>}
      {resultCount > 0 && <details className="mt-5 rounded-xl border border-slate-700 bg-slate-800 p-4 sm:p-5">
        <summary className="cursor-pointer font-medium">Explore {resultCount} {kind === 'row' ? 'matched comparisons' : 'site results'}</summary>
        <p className="text-sm text-slate-300 my-4">Select a result below. Routes, directions and stop contexts stay separate.{!filters.source && ' Each result uses one feed for the full date range; overlapping feeds are not added together.'}</p>
        <div className="overflow-x-auto"><table className="w-full text-left text-sm"><thead><tr className="border-b border-slate-600"><th className="p-2 pl-0">{kind === 'row' ? 'Route / matched context' : 'Site / route / context'}</th><th className="p-2">{kind === 'row' ? 'Shared extra / km' : 'Detected wait / encounter'}</th><th className="p-2">{kind === 'row' ? 'Matched dates' : 'Encounters'}</th></tr></thead><tbody>
          {kind === 'row' ? (showAllRows ? comparisons : comparisons.slice(0, 12)).map(row => <tr key={comparisonKey(row)} className={comparison && comparisonKey(comparison) === comparisonKey(row) ? 'bg-slate-700/50' : 'border-b border-slate-700'}><td className="p-2 pl-0"><button type="button" className="text-left text-blue-300 underline" onClick={() => setSelected(comparisonKey(row))}>Route {row.route_id} · Direction {row.direction_id}</button><p className="text-xs text-slate-400 mt-1">{sourceLabel(row.source)} · {context[row.context]} · {row.signal_count} signals / {row.stop_count} stops · {row.day_type} · {row.time_band * 4}:00–{row.time_band * 4 + 3}:59</p></td><td className="p-2 whitespace-nowrap">{row.status === 'ready' ? minutes(row.shared_extra_seconds_per_km) : row.observed ? <>{minutes(row.observed.shared_extra_seconds_per_km)}<span className="block text-xs text-amber-200">Preliminary</span></> : 'No matched dates'}</td><td className="p-2">{row.matched_dates}</td></tr>) : (showAllRows ? signals : signals.slice(0, 12)).map(row => <tr key={signalKey(row)} className={signal && signalKey(signal) === signalKey(row) ? 'bg-slate-700/50' : 'border-b border-slate-700'}><td className="p-2 pl-0"><button type="button" className="text-left text-blue-300 underline" onClick={() => { setSelected(signalKey(row)); setSiteId(row.site_id); }}>{name(row.site_id)}</button><p className="text-xs text-slate-400 mt-1">{sourceLabel(row.source)} · Route {row.route_id} · Direction {row.direction_id} · {context[row.context]}</p></td><td className="p-2 whitespace-nowrap">{row.evaluable_encounters > 0 && row.detected_wait_seconds_per_encounter != null ? <>{row.detected_wait_encounters > 0 ? seconds(row.detected_wait_seconds_per_encounter) : 'No waits detected'}{row.status !== 'ready' && <span className="block text-xs text-amber-200">Preliminary</span>}</> : 'Insufficient sampling'}</td><td className="p-2">{count(row.encounters)}</td></tr>)}
        </tbody></table></div>
        {resultCount > 12 && <button type="button" className="mt-4 text-sm text-blue-300 underline" aria-expanded={showAllRows} onClick={() => setShowAllRows(value => !value)}>{showAllRows ? 'Show fewer results' : `Show all ${resultCount} results`}</button>}
      </details>}
      <details className="mt-5 rounded-xl border border-slate-700 bg-slate-800 p-4 sm:p-5"><summary className="cursor-pointer font-medium">{kind === 'row' ? 'See roadway coverage on the map' : 'See signals and passenger stops on the map'}</summary><div className="mt-4"><StudyMap network={data.network} kind={kind} filters={{ ...filters, from: from || undefined, to: to || undefined }} selectedSite={siteId || signal?.site_id} onSelectSite={id => { setSiteId(id); setSelected(''); }} /></div></details>
      <details className="mt-5 rounded-xl border border-slate-700 bg-slate-800 p-5 text-sm">
        <summary className="cursor-pointer font-medium">Coverage, sources and method</summary>
        {coverage && <div className="mt-4" aria-label="Roadway observation coverage"><p className="text-xs text-slate-400 mb-3">{sourceLabel(coverage.source)} coverage for the selected service, route, direction, dates and hours. This counts one feed only.</p><div className="grid grid-cols-3 gap-3"><div><p className="text-xs text-slate-300">Completed passages</p><p className="text-xl font-semibold mt-1">{count(coverage.passages)}</p></div><div><p className="text-xs text-slate-300">Reviewed ROW coverage</p><p className="text-xl font-semibold mt-1">{coverage.passages ? percent(coverage.classified_passages / coverage.passages) : 'No sample'}</p></div><div><p className="text-xs text-slate-300">Unknown roadway</p><p className="text-xl font-semibold mt-1">{count(coverage.unknown_passages)}</p></div></div></div>}
        <p className="text-slate-300 mt-4">{kind === 'row' ? 'Completed 200 m passages are compared within the same source, route, direction, day type, four-hour band and signal/stop context. Each matched date has equal weight. Unreviewed roadway is excluded from comparisons.' : 'Detected stationary time uses the source’s documented clock. No detection is not proof of no wait; short waits can be missed. Complete encounters with insufficient sampling remain in the denominator.'}</p>
        <p className="text-slate-300 mt-3">Both feeds contribute evidence. For each equivalent comparison or directional site, the default view uses the feed with the strongest readiness, date coverage and sample size across the full selected period. It never chooses by the size of the delay or adds overlapping observations together.</p>
        <p className="text-slate-300 mt-3">SSE receipt times preserve collection cadence; provider position timestamps can have minute precision. Le Pass may share the same upstream vehicle feed. Neither source provides historical traffic-light phases or proves signal causality.</p>
        {(['sse', 'lepass'] as const).filter(source => !filters.source || filters.source === source).map(source => { const quality = data.quality.filter(row => row.source === source); return quality.length > 0 && <p key={source} className="text-slate-400 mt-3">{sourceLabel(source)} quality in selected dates (all routes and hours): {count(quality.reduce((n, row) => n + row.raw_observations, 0))} raw observations, {count(quality.reduce((n, row) => n + row.usable_observations, 0))} usable.</p>; })}
        <p className="text-slate-400 mt-3">Method: {data.method}. Network: {data.network.version}.</p>
        {[...data.limitations, ...data.network.limitations].filter((value, index, all) => all.indexOf(value) === index).map(value => <p key={value} className="text-slate-400 mt-2">{value}</p>)}
      </details>
    </>}
  </section>;
}
