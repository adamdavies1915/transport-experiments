import { useId, useMemo, type ReactNode } from 'react';
import type { RowStudyData, SignalStudyData, StudyMode, StudyRowComparison, StudySignalSummary } from '../../src/transit-study-types';
import type { SummarySnapshotStatus } from './summary-data';
import { rowStudyFromCells, signalStudyFromCells, ROW_MIN_DATES, ROW_MIN_PASSAGES, SIGNAL_MIN_DATES, SIGNAL_MIN_ENCOUNTERS } from './transit-study-filter';
import { selectRowEvidence, selectSignalEvidence, studySourceName } from './study-evidence';
import { useCachedStudy, type CachedStudyResult } from './hooks/useCachedStudy';

type SnapshotStudy<T> = T & { snapshot?: SummarySnapshotStatus };
type StudyResult<T> = CachedStudyResult<SnapshotStudy<T>>;

function useStudy<T>(url: string, supplied?: SnapshotStudy<T>): StudyResult<T> {
  return useCachedStudy<SnapshotStudy<T>>(url, supplied);
}

const count = (value: number) => value.toLocaleString();
const seconds = (value: number) => value === 0 ? '0' : Math.abs(value) < 1 ? value.toFixed(1) : Math.round(value).toLocaleString();
const minutes = (value: number) => (value / 60).toFixed(1);
const date = (value: string) => new Date(`${value.slice(0, 10)}T12:00:00Z`).toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'America/Chicago' });
const period = (data: RowStudyData | SignalStudyData) => data.from && data.to ? `${date(data.from)}–${date(data.to)}` : 'Available observation dates';

function Status({ ready, children }: { ready: boolean; children?: ReactNode }) {
  return <span className={`story-status ${ready ? 'story-status-ready' : 'story-status-collecting'}`}><span aria-hidden="true" className="story-status-dot" />{children ?? (ready ? 'Early finding' : 'Building the evidence')}</span>;
}

function StudyLoadState({ result, label }: { result: StudyResult<unknown>; label: string }) {
  if (result.loading) return <p className="story-loading" role="status">Loading {label} observations…</p>;
  if (result.error && result.data) return <p className="mb-4 text-sm text-amber-200" role="alert">Refresh unavailable; showing saved observations. <button type="button" onClick={result.retry} className="underline">Retry {label} observations</button></p>;
  if (result.error) return <div className="story-loading" role="alert"><p>{label[0].toUpperCase() + label.slice(1)} observations are temporarily unavailable.</p><button type="button" onClick={result.retry} className="story-link mt-3">Retry {label} observations <span aria-hidden="true">↻</span></button></div>;
  if (result.refreshing) return <p className="mb-3 text-xs text-slate-400" role="status">Updating {label} observations…</p>;
  return null;
}

function StaleNotice({ data }: { data?: { snapshot?: SummarySnapshotStatus } }) {
  if (!data?.snapshot?.stale) return null;
  return <p role="status" className="rounded-lg border border-amber-300/30 bg-amber-300/5 p-3 text-sm text-amber-200 mb-5">This is a saved analysis. The next update is pending.</p>;
}

function EvidenceProgress({ days, minimumDays, sample, minimumSample, sampleLabel }: { days: number; minimumDays: number; sample: number; minimumSample: number; sampleLabel: string }) {
  const progressId = useId();
  return <div className="evidence-progress mt-5">
    <div className="flex flex-wrap justify-between gap-2 text-sm"><label htmlFor={progressId} className="text-slate-300">Comparable observation days</label><span className="font-medium text-slate-100">{days} <span className="text-slate-400">/ {minimumDays} needed</span></span></div>
    <progress id={progressId} className="story-progress mt-2" value={Math.min(days, minimumDays)} max={minimumDays}>{days} of {minimumDays} days</progress>
    <p className="mt-2 text-xs text-slate-400">{count(sample)} {sampleLabel} · at least {minimumSample} needed{sample >= minimumSample ? ' ✓' : ''}</p>
  </div>;
}

function RowFinding({ row, data }: { row?: StudyRowComparison; data: RowStudyData }) {
  const coverage = useMemo(() => row ? rowStudyFromCells(data, { source: row.source, mode: row.mode,
    route_id: row.route_id, direction_id: row.direction_id, day_type: row.day_type,
    hour_from: row.time_band * 4, hour_to: row.time_band * 4 + 3 }).coverage : undefined, [data, row]);
  const path = row && data.network.paths.find(path => path.route_id === row.route_id && path.direction_id === row.direction_id && path.mode === row.mode);
  const observed = row?.observed;
  const ready = row?.status === 'ready';
  const unresolved = observed && observed.shared_extra_lower_seconds_per_km <= 0 && observed.shared_extra_upper_seconds_per_km >= 0;
  const observedPeriod = observed?.dates.length ? `${date(observed.dates[0])}–${date(observed.dates.at(-1)!)}` : undefined;
  return <div className="story-finding">
    <Status ready={ready}>{observed ? ready ? 'Observed figures' : 'Preliminary figures' : 'Building the evidence'}</Status>
    {coverage && <p className="mt-3 text-xs text-slate-400">Roadway review coverage: {count(coverage.classified_passages)} of {count(coverage.passages)} completed passages in this source, route, direction and time selection have a reviewed roadway class. The comparison below uses only matched dates and stop/signal conditions.</p>}
    {row && observed ? <>
      <h4 className="mt-4 text-xl sm:text-2xl font-semibold tracking-tight">Observed time to travel one kilometre</h4>
      <p className="mt-2 text-sm text-slate-300">{path ? `Route ${row.route_id} · ${path.name}` : `Route ${row.route_id}`} · {row.day_type === 'weekday' ? 'Weekdays' : 'Weekends'}, {String(row.time_band * 4).padStart(2, '0')}:00–{String(row.time_band * 4 + 3).padStart(2, '0')}:59</p>
      <div className="grid grid-cols-2 gap-5 mt-6">
        <div className="border-t-2 border-emerald-300 pt-3"><p className="text-sm text-emerald-200">Reserved track</p><p className="story-number mt-2">{minutes(observed.reserved_seconds_per_km)} <span>min / km</span></p></div>
        <div className="border-t-2 border-orange-300 pt-3"><p className="text-sm text-orange-200">Shared roadway</p><p className="story-number mt-2">{minutes(observed.shared_seconds_per_km)} <span>min / km</span></p></div>
      </div>
      <p className="mt-4 text-sm text-slate-300">{count(row.reserved_passages)} reserved and {count(row.shared_passages)} shared passages · {row.matched_dates} matched dates · {observedPeriod}</p>
      <div className="mt-4 rounded-lg border border-amber-300/25 bg-amber-300/5 p-3 text-sm">
        <p className="text-amber-100">{unresolved ? 'Timing uncertainty is too wide to tell which roadway is quicker.' : 'These are observed times for this sample, not a measured effect of changing the roadway.'}</p>
        <p className="mt-2 text-slate-300">Recorded difference, shared minus reserved: {minutes(observed.shared_extra_seconds_per_km)} min/km. Timing range: {minutes(observed.shared_extra_lower_seconds_per_km)} to {minutes(observed.shared_extra_upper_seconds_per_km)} min/km.</p>
      </div>
      {!ready && <p className="mt-3 text-sm text-slate-400">Preliminary sample. We keep collecting toward {ROW_MIN_DATES} matched dates and {ROW_MIN_PASSAGES} passages in each class.</p>}
      <details className="mt-5 text-sm"><summary className="story-disclosure">How certain is this comparison?</summary><p className="mt-3 text-slate-300">Each matched date has equal weight. Nearby signals and passenger stops are matched, but roadway locations differ. This does not measure what changing their roadway would save.</p><p className="mt-3 text-slate-300">{row.shared_extra_ci_lower_seconds_per_km == null ? 'A day-to-day confidence interval is not available for this sample. The timing range above describes timestamp uncertainty, not statistical confidence.' : `Day-to-day 95% interval: ${seconds(row.shared_extra_ci_lower_seconds_per_km)} to ${seconds(row.shared_extra_ci_upper_seconds_per_km!)} seconds per kilometre. This is separate from the timing range above.`}</p><EvidenceProgress days={row.matched_dates} minimumDays={ROW_MIN_DATES} sample={Math.min(row.reserved_passages, row.shared_passages)} minimumSample={ROW_MIN_PASSAGES} sampleLabel="passages in the smaller roadway sample" /></details>
    </> : <>
      <h4 className="mt-4 text-xl sm:text-2xl font-semibold tracking-tight">No matched roadway sample yet.</h4>
      <p className="mt-3 text-sm leading-relaxed text-slate-300">We need passages on both reserved and shared roadway on the same dates before showing a comparison. Unknown roadway sections are excluded until reviewed.</p>
    </>}
    <p className="story-provenance">{row ? `${studySourceName(row.source)} observations · ` : ''}{observedPeriod ?? `Selected window: ${period(data)}`} · One matched comparison, not a network average.</p>
  </div>;
}

function RoadwayStory({ data: supplied }: { data?: SnapshotStudy<RowStudyData> }) {
  const result = useStudy<RowStudyData>('/api/row-study?mode=streetcar', supplied);
  const row = useMemo(() => result.data ? selectRowEvidence(supplied || result.data.comparisons.some(row => row.matched_dates > 0 && !row.observed) ? rowStudyFromCells(result.data, { mode: 'streetcar' }).comparisons : result.data.comparisons)[0] : undefined, [result.data, supplied]);
  return <section className="story-chapter" aria-labelledby="story-row-heading">
    <div className="story-chapter-intro">
      <span className="story-chapter-number" aria-hidden="true">01</span>
      <p className="story-eyebrow">The roadway</p>
      <h3 id="story-row-heading">How much does sharing the road cost streetcars?</h3>
      <p className="story-chapter-description">Some streetcars have their own track. Others share space with traffic. We compare completed stretches of travel to see how their times differ.</p>
      <a href="#row" className="story-link mt-5">Explore the roadway evidence <span aria-hidden="true">→</span></a>
    </div>
    <div className="min-w-0"><StaleNotice data={result.data} /><StudyLoadState result={result} label="roadway" />{!result.loading && result.data && <RowFinding row={row} data={result.data} />}</div>
  </section>;
}

function signalPeriod(row: StudySignalSummary, data: SignalStudyData): string {
  // Older browser-cached responses may lack the new date provenance fields.
  const dates = row.observed_from && row.observed_to ? [row.observed_from, row.observed_to]
    : data.cells.filter(cell => cell.source === row.source && cell.mode === row.mode && cell.route_id === row.route_id && cell.direction_id === row.direction_id && cell.site_id === row.site_id && cell.context === row.context).map(cell => cell.date).sort();
  return dates.length ? `${date(dates[0])}–${date(dates.at(-1)!)}` : `Selected window: ${period(data)}`;
}

function SignalFinding({ row, data, mode }: { row?: StudySignalSummary; data: SignalStudyData; mode: StudyMode }) {
  const measured = row != null && row.detected_wait_seconds_per_encounter != null && row.evaluable_encounters > 0;
  const ready = row?.status === 'ready';
  const name = row && (data.network.sites.find(site => site.id === row.site_id)?.name ?? row.site_id);
  const path = row && data.network.paths.find(path => path.route_id === row.route_id && path.direction_id === row.direction_id && path.mode === row.mode);
  return <div className="story-finding">
    <div className="flex flex-wrap items-center justify-between gap-3"><h4 className="text-lg font-semibold">{mode === 'streetcar' ? 'Streetcars' : 'Buses'}</h4><Status ready={ready}>{measured ? ready ? 'Observed figures' : 'Preliminary figures' : 'Building the evidence'}</Status></div>
    {row && measured ? <>
      {row.mean_detected_wait_seconds != null ? <>
        <p className="story-number mt-6">{seconds(row.mean_detected_wait_seconds)} <span>sec / detected wait</span></p>
        <p className="mt-2 text-sm text-slate-300">Average duration when a wait was detected.</p>
        <p className="mt-3 text-slate-200"><strong>{seconds(row.detected_wait_seconds_per_encounter!)} sec / pass</strong> averaged across all complete passes.</p>
      </> : <>
        <p className="story-number mt-6">0 <span>waits detected</span></p>
        <p className="mt-2 text-sm text-slate-300">In {count(row.encounters)} complete passes at this signal. Short or unsampled waits can still be missed.</p>
      </>}
      <h5 className="font-medium text-slate-100 mt-5">{name}</h5>
      <p className="text-sm text-slate-400 mt-1">{path ? `Route ${row.route_id} · ${path.name}` : `Route ${row.route_id}`}</p>
      <p className="mt-4 text-sm leading-relaxed text-slate-300">A wait was detected on {count(row.detected_wait_encounters)} of {count(row.encounters)} complete passes, across {row.evaluable_dates} days with usable waiting observations.</p>
      <p className="mt-2 text-xs text-slate-400">{count(row.evaluable_encounters)} of {count(row.encounters)} passes had enough sampling to evaluate waiting · {signalPeriod(row, data)}.</p>
      {!ready && <p className="mt-3 text-sm text-amber-100/90">Preliminary: fewer than {SIGNAL_MIN_DATES} observation days or {SIGNAL_MIN_ENCOUNTERS} evaluable passes.</p>}
      <p className="mt-3 text-sm leading-relaxed text-amber-100/90">This is observed waiting near a light. We cannot yet say how much priority would recover.</p>
      <details className="mt-5 text-sm"><summary className="story-disclosure">What this estimate includes</summary><p className="mt-3 text-slate-300">{count(row.evaluable_encounters)} of {count(row.encounters)} complete passes had enough sampling to evaluate waiting. The per-pass average includes every complete pass, so missed waits can make it too low.</p><p className="mt-3 text-slate-300">No mapped passenger stop overlaps this site. The observations do not establish that the light controlled the vehicle; traffic queues and other causes remain possible.</p><p className="mt-3 text-slate-300">Day-to-day 95% interval: {row.detected_wait_seconds_per_encounter_ci_lower == null ? 'unavailable' : `${seconds(row.detected_wait_seconds_per_encounter_ci_lower)} to ${seconds(row.detected_wait_seconds_per_encounter_ci_upper!)} seconds per pass`}. This interval does not include clock uncertainty.</p></details>
    </> : <>
      <h5 className="mt-5 text-lg font-medium">No measurable waiting sample yet.</h5>
      <p className="mt-3 text-sm leading-relaxed text-slate-300">{row ? `${count(row.encounters)} complete passes were observed, but none had enough sampling to evaluate waiting.` : 'No isolated signal has a usable sample yet.'} This does not mean there were no waits.</p>
    </>}
    <p className="story-provenance">{row ? `${studySourceName(row.source)} observations · ${signalPeriod(row, data)} · One site and direction.` : `Selected window: ${period(data)} · Passenger-stop overlaps are kept separate.`}</p>
  </div>;
}

function MoreSignalFigures({ rows, data }: { rows: StudySignalSummary[]; data: SignalStudyData }) {
  if (!rows.length) return null;
  return <div className="story-finding mt-4">
    <h5 className="text-sm font-semibold">More sampled signals</h5>
    <p className="mt-2 text-xs text-slate-400">Ordered by sample support. Each location keeps its own figures.</p>
    <ul className="mt-4 space-y-4">{rows.map(row => <li key={`${row.source}:${row.route_id}:${row.direction_id}:${row.site_id}`} className="border-t border-slate-600/60 pt-3">
      <div className="flex flex-wrap justify-between gap-2"><p className="text-sm font-medium">{data.network.sites.find(site => site.id === row.site_id)?.name ?? row.site_id}</p><p className="text-sm text-amber-100">{row.evaluable_encounters === 0 ? 'Not measurable' : row.mean_detected_wait_seconds == null ? 'No wait detected' : `${seconds(row.mean_detected_wait_seconds)} sec / detected wait`}</p></div>
      <p className="mt-1 text-xs text-slate-400">Route {row.route_id} · Direction {row.direction_id} · {studySourceName(row.source)} · {row.status === 'ready' ? 'Observed figures' : 'Preliminary'}</p>
      <p className="mt-2 text-xs text-slate-300">{count(row.detected_wait_encounters)} of {count(row.encounters)} passes with a detected wait · {count(row.evaluable_encounters)} evaluable · {row.evaluable_dates} days.</p>
      {row.evaluable_encounters > 0 && row.mean_detected_wait_seconds != null && <p className="mt-1 text-xs text-slate-300">{seconds(row.detected_wait_seconds_per_encounter!)} sec / pass averaged across all complete passes.</p>}
      <p className="mt-1 text-xs text-slate-400">{signalPeriod(row, data)}</p>
    </li>)}</ul>
    <p className="mt-4 text-xs text-slate-400">Detected stationary time near mapped lights; it does not establish red-light causation or recoverable savings.</p>
  </div>;
}

function SignalStory({ mode, data: supplied }: { mode: StudyMode; data?: SnapshotStudy<SignalStudyData> }) {
  const result = useStudy<SignalStudyData>(`/api/signal-study?mode=${mode}`, supplied);
  const rows = useMemo(() => result.data ? selectSignalEvidence((supplied ? signalStudyFromCells(result.data, { mode }).signals : result.data.signals).filter(row => row.context === 'signal_only')) : [], [mode, result.data, supplied]);
  return <div className="min-w-0"><StaleNotice data={result.data} /><StudyLoadState result={result} label={mode === 'streetcar' ? 'streetcar signal' : 'bus signal'} />{!result.loading && result.data && <><SignalFinding row={rows[0]} data={result.data} mode={mode} /><MoreSignalFigures rows={rows.slice(1, 3)} data={result.data} /></>}</div>;
}

export default function StoryPage({ rowData, signalData }: { rowData?: SnapshotStudy<RowStudyData>; signalData?: SnapshotStudy<SignalStudyData> }) {
  return <div className="transit-story">
    <section className="story-hero" aria-labelledby="story-heading">
      <p className="story-eyebrow">Following the journey</p>
      <h2 id="story-heading">Where does transit<br className="hidden sm:block" /> <span>lose time?</span></h2>
      <p className="story-deck">A faster trip starts with knowing where the time goes. We’re following New Orleans streetcars and buses to answer two questions.</p>
      <div className="story-question-index" aria-label="Study questions"><p><span aria-hidden="true">01</span> Sharing the roadway</p><p><span aria-hidden="true">02</span> Waiting at traffic lights</p></div>
    </section>
    <RoadwayStory data={rowData} />
    <section className="story-chapter story-chapter-signals" aria-labelledby="story-signals-heading">
      <div className="story-chapter-intro">
        <span className="story-chapter-number" aria-hidden="true">02</span>
        <p className="story-eyebrow">The intersections</p>
        <h3 id="story-signals-heading">What happens when transit reaches a traffic light?</h3>
        <p className="story-chapter-description">Repeated stationary positions help us find waits near signals. We keep passenger stops separate, so boarding does not become a claim about traffic lights.</p>
        <a href="#signals" className="story-link mt-5">Explore the signal evidence <span aria-hidden="true">→</span></a>
      </div>
      <div className="story-signal-grid grid grid-cols-1 sm:grid-cols-2 gap-4"><SignalStory mode="streetcar" data={signalData} /><SignalStory mode="bus" data={signalData} /></div>
    </section>
    <section aria-labelledby="story-sources-heading" className="story-method-card">
      <div><p className="story-eyebrow">One view of the evidence</p><h3 id="story-sources-heading" className="text-xl sm:text-2xl font-semibold tracking-tight mt-2">Both feeds inform the story.</h3><p className="mt-3 leading-relaxed text-sm text-slate-300 max-w-2xl">We collect both the live vehicle feed and Le Pass. For each comparison or signal, we use the source with the strongest sample. You can explore both sources in the detailed studies.</p></div>
      <details className="text-sm"><summary className="story-disclosure">How the sources work together</summary><p className="mt-3 text-slate-300 leading-relaxed">The feeds may describe the same vehicles, with different timestamps and coverage. We keep their observations separate and choose one complete estimate per result using readiness, observation days and sample coverage. We do not add their sample counts or select the largest delay.</p><p className="mt-3 text-slate-400 leading-relaxed">Every headline describes a particular route, direction or signal. These examples are not summed into a journey or a citywide estimate.</p></details>
    </section>
    <div className="mt-6 flex flex-wrap gap-x-4 gap-y-2 text-sm text-slate-400"><p>Interested in the timetable?</p><a className="story-link" href="#otp">See how often service runs on time <span aria-hidden="true">→</span></a></div>
  </div>;
}
