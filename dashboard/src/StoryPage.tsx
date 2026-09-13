import { useEffect, useId, useMemo, useState, type ReactNode } from 'react';
import type { RowStudyData, SignalStudyData, StudyMode, StudyRowComparison, StudySignalSummary } from '../../src/transit-study-types';
import type { SummarySnapshotStatus } from './summary-data';
import { rowStudyFromCells, signalStudyFromCells, ROW_MIN_DATES, ROW_MIN_PASSAGES, SIGNAL_MIN_DATES, SIGNAL_MIN_ENCOUNTERS } from './transit-study-filter';
import { selectRowEvidence, selectSignalEvidence, studySourceName } from './study-evidence';

type SnapshotStudy<T> = T & { snapshot?: SummarySnapshotStatus };
type StudyResult<T> = { data?: SnapshotStudy<T>; error?: string; loading: boolean; retry: () => void };

function useStudy<T>(url: string, supplied?: SnapshotStudy<T>): StudyResult<T> {
  const [attempt, setAttempt] = useState(0);
  const [result, setResult] = useState<{ attempt: number; data?: SnapshotStudy<T>; error?: string }>({ attempt: -1 });
  useEffect(() => {
    if (supplied) return;
    const controller = new AbortController();
    fetch(url, { signal: controller.signal }).then(async response => {
      if (!response.ok) throw new Error('The saved observations could not be loaded.');
      return response.json() as Promise<SnapshotStudy<T>>;
    }).then(data => { if (!controller.signal.aborted) setResult({ attempt, data }); })
      .catch(() => { if (!controller.signal.aborted) setResult({ attempt, error: 'The saved observations could not be loaded.' }); });
    return () => controller.abort();
  }, [attempt, supplied, url]);
  return supplied ? { data: supplied, loading: false, retry: () => {} } : { ...result, loading: result.attempt !== attempt, retry: () => setAttempt(value => value + 1) };
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
  if (result.error) return <div className="story-loading" role="alert"><p>{label[0].toUpperCase() + label.slice(1)} observations are temporarily unavailable.</p><button type="button" onClick={result.retry} className="story-link mt-3">Retry {label} observations <span aria-hidden="true">↻</span></button></div>;
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
  const path = row && data.network.paths.find(path => path.route_id === row.route_id && path.direction_id === row.direction_id && path.mode === row.mode);
  const ready = row?.status === 'ready' && row.shared_seconds_per_km != null && row.reserved_seconds_per_km != null && row.shared_extra_seconds_per_km != null;
  return <div className="story-finding">
    <Status ready={!!ready} />
    {ready ? <>
      <h4 className="mt-4 text-xl sm:text-2xl font-semibold tracking-tight">Two kinds of roadway. A measured comparison.</h4>
      <p className="mt-2 text-sm text-slate-300">{path ? `Route ${row.route_id} · ${path.name}` : `Route ${row.route_id}`} · time to travel one kilometre</p>
      <div className="grid grid-cols-2 gap-5 mt-6">
        <div className="border-t-2 border-emerald-300 pt-3"><p className="text-sm text-emerald-200">Reserved track</p><p className="story-number mt-2">{minutes(row.reserved_seconds_per_km!)} <span>min</span></p></div>
        <div className="border-t-2 border-orange-300 pt-3"><p className="text-sm text-orange-200">Shared roadway</p><p className="story-number mt-2">{minutes(row.shared_seconds_per_km!)} <span>min</span></p></div>
      </div>
      <p className="mt-5 text-slate-200">Shared roadway took <strong>{seconds(Math.abs(row.shared_extra_seconds_per_km!))} seconds {row.shared_extra_seconds_per_km! < 0 ? 'less' : 'more'}</strong> per kilometre in this comparison.</p>
      <p className="mt-3 text-sm text-slate-400">This compares different places under matched conditions. It does not measure what changing their roadway would save.</p>
      <details className="mt-5 text-sm"><summary className="story-disclosure">How certain is this comparison?</summary><p className="mt-3 text-slate-300">{row.matched_dates} matching dates; {count(row.reserved_passages)} reserved and {count(row.shared_passages)} shared passages. {row.day_type === 'weekday' ? 'Weekdays' : 'Weekends'}, {String(row.time_band * 4).padStart(2, '0')}:00–{String(row.time_band * 4 + 3).padStart(2, '0')}:59, New Orleans time. Nearby signals and passenger stops are matched.</p><p className="mt-3 text-slate-300">Day-to-day 95% interval: {row.shared_extra_ci_lower_seconds_per_km == null ? 'unavailable' : `${seconds(row.shared_extra_ci_lower_seconds_per_km)} to ${seconds(row.shared_extra_ci_upper_seconds_per_km!)} seconds per kilometre`}. Timestamp range: {row.shared_extra_lower_seconds_per_km == null ? 'unavailable' : `${seconds(row.shared_extra_lower_seconds_per_km)} to ${seconds(row.shared_extra_upper_seconds_per_km!)} seconds per kilometre`}. A range crossing zero leaves the direction uncertain.</p></details>
    </> : <>
      <h4 className="mt-4 text-xl sm:text-2xl font-semibold tracking-tight">We cannot put a reliable number on it yet.</h4>
      <p className="mt-3 text-sm leading-relaxed text-slate-300">A fair comparison needs repeated trips on reserved and shared roadway, on the same days and under similar conditions. Our best-supported comparison still needs more evidence.</p>
      {row ? <><EvidenceProgress days={row.matched_dates} minimumDays={ROW_MIN_DATES} sample={Math.min(row.reserved_passages, row.shared_passages)} minimumSample={ROW_MIN_PASSAGES} sampleLabel="passages in the smaller roadway sample" /><p className="mt-4 text-xs text-slate-400">{path ? `Route ${row.route_id} · ${path.name}` : `Route ${row.route_id}`} · {count(row.reserved_passages)} reserved and {count(row.shared_passages)} shared passages on matching dates.</p></> : <p className="mt-5 border-l-2 border-amber-300 pl-4 text-sm text-amber-200">No matched roadway comparison is available yet. Unknown roadway sections are excluded until reviewed.</p>}
    </>}
    <p className="story-provenance">{row ? `${studySourceName(row.source)} observations · ` : ''}{period(data)} · {ready ? 'One matched comparison, not a network average.' : 'Unreviewed roadway is excluded from comparisons.'}</p>
  </div>;
}

function RoadwayStory({ data: supplied }: { data?: SnapshotStudy<RowStudyData> }) {
  const result = useStudy<RowStudyData>('/api/row-study?mode=streetcar', supplied);
  const row = useMemo(() => result.data ? selectRowEvidence(rowStudyFromCells(result.data, { mode: 'streetcar' }).comparisons)[0] : undefined, [result.data]);
  return <section className="story-chapter" aria-labelledby="story-row-heading">
    <div className="story-chapter-intro">
      <span className="story-chapter-number" aria-hidden="true">01</span>
      <p className="story-eyebrow">The roadway</p>
      <h3 id="story-row-heading">How much does sharing the road cost streetcars?</h3>
      <p className="story-chapter-description">Some streetcars have their own track. Others share space with traffic. We compare completed stretches of travel to see how their times differ.</p>
      <a href="#row" className="story-link mt-5">Explore the roadway evidence <span aria-hidden="true">→</span></a>
    </div>
    <div className="min-w-0"><StaleNotice data={result.data} /><StudyLoadState result={result} label="roadway" />{!result.loading && !result.error && result.data && <RowFinding row={row} data={result.data} />}</div>
  </section>;
}

function SignalFinding({ row, data, mode }: { row?: StudySignalSummary; data: SignalStudyData; mode: StudyMode }) {
  const ready = row?.status === 'ready' && row.detected_wait_seconds_per_encounter != null && row.evaluable_encounters > 0;
  const name = row && (data.network.sites.find(site => site.id === row.site_id)?.name ?? row.site_id);
  const path = row && data.network.paths.find(path => path.route_id === row.route_id && path.direction_id === row.direction_id && path.mode === row.mode);
  return <div className="story-finding h-full">
    <div className="flex flex-wrap items-center justify-between gap-3"><h4 className="text-lg font-semibold">{mode === 'streetcar' ? 'Streetcars' : 'Buses'}</h4><Status ready={!!ready} /></div>
    {ready ? <>
      <p className="story-number mt-6">{seconds(row.detected_wait_seconds_per_encounter!)} <span>sec / pass</span></p>
      <p className="mt-2 text-sm text-slate-300">of detected stationary time near one signal</p>
      <h5 className="font-medium text-slate-100 mt-5">{name}</h5>
      <p className="text-sm text-slate-400 mt-1">{path ? `Route ${row.route_id} · ${path.name}` : `Route ${row.route_id}`}</p>
      <p className="mt-4 text-sm leading-relaxed text-slate-300">A wait was detected on {count(row.detected_wait_encounters)} of {count(row.encounters)} complete passes, across {row.evaluable_dates} days with usable waiting observations.</p>
      <p className="mt-3 text-sm leading-relaxed text-amber-100/90">This is observed waiting near a light. We cannot yet say how much priority would recover.</p>
      <details className="mt-5 text-sm"><summary className="story-disclosure">What this estimate includes</summary><p className="mt-3 text-slate-300">{count(row.evaluable_encounters)} of {count(row.encounters)} complete passes had enough sampling to evaluate waiting. The average includes every complete pass, so missed waits can make it too low.</p><p className="mt-3 text-slate-300">No mapped passenger stop overlaps this site. The observations do not establish that the light controlled the vehicle; traffic queues and other causes remain possible.</p><p className="mt-3 text-slate-300">Day-to-day 95% interval: {row.detected_wait_seconds_per_encounter_ci_lower == null ? 'unavailable' : `${seconds(row.detected_wait_seconds_per_encounter_ci_lower)} to ${seconds(row.detected_wait_seconds_per_encounter_ci_upper!)} seconds per pass`}. This interval does not include clock uncertainty.</p></details>
    </> : <>
      <h5 className="mt-5 text-lg font-medium">More days before a headline estimate.</h5>
      <p className="mt-3 text-sm leading-relaxed text-slate-300">We need enough well-observed passes at the same signal to distinguish a pattern from a few isolated waits.</p>
      {row ? <><EvidenceProgress days={row.evaluable_dates} minimumDays={SIGNAL_MIN_DATES} sample={row.evaluable_encounters} minimumSample={SIGNAL_MIN_ENCOUNTERS} sampleLabel={`${mode} passes with usable waiting observations`} /><p className="mt-4 text-xs text-slate-400">{name} · {path ? `Route ${row.route_id} · ${path.name}` : `Route ${row.route_id}`}</p></> : <p className="mt-5 text-sm text-amber-200">No isolated signal has a usable sample yet. This does not mean there were no waits.</p>}
    </>}
    <p className="story-provenance">{row ? `${studySourceName(row.source)} observations · ` : ''}{period(data)} · {row ? 'One site and direction.' : 'Passenger-stop overlaps are kept separate.'}</p>
  </div>;
}

function SignalStory({ mode, data: supplied }: { mode: StudyMode; data?: SnapshotStudy<SignalStudyData> }) {
  const result = useStudy<SignalStudyData>(`/api/signal-study?mode=${mode}`, supplied);
  const row = useMemo(() => result.data ? selectSignalEvidence(signalStudyFromCells(result.data, { mode }).signals.filter(row => row.context === 'signal_only'))[0] : undefined, [mode, result.data]);
  return <div className="min-w-0"><StaleNotice data={result.data} /><StudyLoadState result={result} label={mode === 'streetcar' ? 'streetcar signal' : 'bus signal'} />{!result.loading && !result.error && result.data && <SignalFinding row={row} data={result.data} mode={mode} />}</div>;
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
