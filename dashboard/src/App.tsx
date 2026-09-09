import OtpPanel from './OtpPanel';
import PriorityPanel from './PriorityPanel';
import StudyPanel from './StudyPanel';
import SourceQuality from './SourceQuality';
import { useTransitData } from './hooks/useTransitData';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer, Cell, LineChart, Line
} from 'recharts';
import { useEffect, useState, type ReactNode } from 'react';
import type { OtpData } from './otp-data';

const COLORS = {
  dedicated: '#22c55e',
  mixed: '#ef4444',
  primary: '#3b82f6',
  secondary: '#8b5cf6'
};

type StatColor = 'blue' | 'green' | 'red' | 'purple';

interface StatCardProps {
  title: string;
  value: ReactNode;
  subtitle?: ReactNode;
  color?: StatColor;
}

function StatCard({ title, value, subtitle, color = 'blue' }: StatCardProps) {
  const colorClasses: Record<StatColor, string> = {
    blue: 'border-blue-500 bg-blue-500/10',
    green: 'border-green-500 bg-green-500/10',
    red: 'border-red-500 bg-red-500/10',
    purple: 'border-purple-500 bg-purple-500/10'
  };

  return (
    <div className={`rounded-lg border-l-4 p-4 ${colorClasses[color]}`}>
      <h3 className="text-sm text-slate-400 uppercase tracking-wide">{title}</h3>
      <p className="text-3xl font-bold mt-1">{value}</p>
      {subtitle && <p className="text-sm text-slate-400 mt-1">{subtitle}</p>}
    </div>
  );
}

function OverviewPage({ onRetry }: { onRetry: () => void }) {
  const { data, loading, error } = useTransitData();

  if (loading) {
    return (
      <div className="flex items-center justify-center rounded-xl bg-slate-800 p-12" role="status">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-500 mx-auto"></div>
          <p className="mt-4 text-slate-300">Loading the transit overview…</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="rounded-xl bg-slate-800 p-6" role="alert">
        <div>
          <h2 className="text-amber-300 font-semibold text-lg">The overview is unavailable</h2>
          <p className="mt-2 text-slate-300">{error}</p>
          <button type="button" className="mt-4 text-blue-300 underline" onClick={onRetry}>Retry overview</button>
        </div>
      </div>
    );
  }

  if (!data) return null;

  const dedicatedData = data.segmentType.find(s => s.segment_type === 'dedicated_row');
  const mixedData = data.segmentType.find(s => s.segment_type === 'mixed_traffic');

  return (
    <section aria-labelledby="overview-heading">
        <div className="mb-8">
          <h2 id="overview-heading" className="text-2xl font-semibold">Transit overview</h2>
          <p className="text-slate-300 mt-2">
            Historical collection coverage and reported vehicle-speed readings. Use the ROW study for matched completed-passage comparisons.
          </p>
        </div>

        {/* Summary Stats */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-8">
          <StatCard
            title="Total Records"
            value={Number(data.summary.total_records).toLocaleString()}
            subtitle={`${data.summary.total_routes} routes tracked`}
            color="blue"
          />
          <StatCard
            title="Dedicated ROW Speed"
            value={`${dedicatedData?.avg_speed || 0} mph`}
            subtitle="Historical reported-speed average"
            color="green"
          />
          <StatCard
            title="Mixed Traffic Speed"
            value={`${mixedData?.avg_speed || 0} mph`}
            subtitle={`${mixedData?.delay_pct || 0}% of readings flagged delayed`}
            color="red"
          />
          <StatCard
            title="Data Range"
            value={(() => {
              const hours = Math.round((new Date(data.summary.last_record).getTime() - new Date(data.summary.first_record).getTime()) / 3600000);
              return hours >= 48 ? `${Math.round(hours / 24)} days` : `${hours}h`;
            })()}
            subtitle={`${new Date(data.summary.first_record).toLocaleDateString()} - ${new Date(data.summary.last_record).toLocaleDateString()}`}
            color="purple"
          />
        </div>

        {/* Main Charts Row */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
          {/* ROW vs Mixed Traffic */}
          <div className="bg-slate-800 rounded-lg p-6">
            <h2 className="text-xl font-semibold mb-4">Historical reported speeds and delay flags</h2>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={data.segmentType} layout="vertical">
                <CartesianGrid strokeDasharray="3 3" stroke="#475569" />
                <XAxis type="number" stroke="#94a3b8" />
                <YAxis
                  type="category"
                  dataKey="segment_type"
                  stroke="#94a3b8"
                  tickFormatter={(v) => v === 'dedicated_row' ? 'Dedicated ROW' : 'Mixed Traffic'}
                  width={100}
                />
                <Tooltip
                  contentStyle={{ backgroundColor: '#1e293b', border: 'none' }}
                  formatter={(value, name) => [
                    name === 'avg_speed' ? `${value} mph` : `${value}%`,
                    name === 'avg_speed' ? 'Avg Speed' : 'Delay %'
                  ]}
                />
                <Legend />
                <Bar dataKey="avg_speed" name="Avg Speed (mph)" fill={COLORS.primary} />
                <Bar dataKey="delay_pct" name="Delay %" fill={COLORS.secondary} />
              </BarChart>
            </ResponsiveContainer>
          </div>

          {/* Speed by Segment */}
          <div className="bg-slate-800 rounded-lg p-6">
            <h2 className="text-xl font-semibold mb-4">Speed by Segment</h2>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={data.segments}>
                <CartesianGrid strokeDasharray="3 3" stroke="#475569" />
                <XAxis
                  dataKey="segment_name"
                  stroke="#94a3b8"
                  tick={{ fontSize: 10 }}
                  angle={-45}
                  textAnchor="end"
                  height={80}
                />
                <YAxis stroke="#94a3b8" />
                <Tooltip
                  contentStyle={{ backgroundColor: '#1e293b', border: 'none' }}
                  formatter={(value) => [`${value} mph`, 'Speed']}
                />
                <Bar dataKey="avg_speed" fill={COLORS.primary} name="Avg Speed">
                  {data.segments.map((entry, index) => (
                    <Cell
                      key={`cell-${index}`}
                      fill={entry.segment_type === 'dedicated_row' ? COLORS.dedicated : COLORS.mixed}
                    />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
            <div className="flex justify-center gap-6 mt-2 text-sm">
              <span className="flex items-center gap-2">
                <span className="w-3 h-3 rounded" style={{ backgroundColor: COLORS.dedicated }}></span>
                Dedicated ROW
              </span>
              <span className="flex items-center gap-2">
                <span className="w-3 h-3 rounded" style={{ backgroundColor: COLORS.mixed }}></span>
                Mixed Traffic
              </span>
            </div>
          </div>
        </div>

        {/* Timeline Charts */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
          {/* Daily ROW vs Mixed Speed */}
          {data.dailySegments && data.dailySegments.length > 0 && (
            <div className="bg-slate-800 rounded-lg p-6">
              <h2 className="text-xl font-semibold mb-4">📅 Streetcar Speed: ROW vs Mixed Over Time</h2>
              <ResponsiveContainer width="100%" height={300}>
                <LineChart data={
                  // Pivot data by date
                  [...new Set(data.dailySegments.map(d => d.date))].map(date => {
                    const row = data.dailySegments.find(d => d.date === date && d.segment_type === 'dedicated_row');
                    const mixed = data.dailySegments.find(d => d.date === date && d.segment_type === 'mixed_traffic');
                    return { date, dedicated_speed: row?.avg_speed, mixed_speed: mixed?.avg_speed };
                  })
                }>
                  <CartesianGrid strokeDasharray="3 3" stroke="#475569" />
                  <XAxis
                    dataKey="date"
                    stroke="#94a3b8"
                    tick={{ fontSize: 10 }}
                    tickFormatter={(d) => new Date(d).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}
                  />
                  <YAxis stroke="#94a3b8" />
                  <Tooltip
                    contentStyle={{ backgroundColor: '#1e293b', border: 'none' }}
                    labelFormatter={(d) => new Date(d).toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' })}
                    formatter={(value, name) => [`${value} mph`, name === 'dedicated_speed' ? 'Dedicated ROW' : 'Mixed Traffic']}
                  />
                  <Legend formatter={(value) => value === 'dedicated_speed' ? 'Dedicated ROW' : 'Mixed Traffic'} />
                  <Line type="monotone" dataKey="dedicated_speed" stroke={COLORS.dedicated} strokeWidth={2} dot={false} />
                  <Line type="monotone" dataKey="mixed_speed" stroke={COLORS.mixed} strokeWidth={2} dot={false} />
                </LineChart>
              </ResponsiveContainer>
            </div>
          )}
        </div>

    </section>
  );
}

function OverviewView() {
  const [attempt, setAttempt] = useState(0);
  return <OverviewPage key={attempt} onRetry={() => setAttempt(value => value + 1)} />;
}

function OtpView() {
  const [attempt, setAttempt] = useState(0);
  const [result, setResult] = useState<{ attempt: number; data?: OtpData; error?: string }>({ attempt: -1 });
  useEffect(() => {
    const controller = new AbortController();
    fetch('/api/otp', { signal: controller.signal }).then(async response => {
      if (!response.ok) throw new Error(`On-time performance request failed (${response.status}).`);
      return response.json() as Promise<OtpData>;
    }).then(data => { if (!controller.signal.aborted) setResult({ attempt, data }); })
      .catch((error: Error) => { if (!controller.signal.aborted) setResult({ attempt, error: error.message }); });
    return () => controller.abort();
  }, [attempt]);
  if (result.attempt !== attempt) return <p className="rounded-xl bg-slate-800 p-12 text-center text-slate-300" role="status">Loading on-time performance…</p>;
  if (result.error) return <div className="rounded-xl bg-slate-800 p-6" role="alert"><h2 className="text-lg text-amber-300 font-semibold">On-time performance is unavailable</h2><p className="mt-2 text-slate-300">{result.error}</p><button className="mt-4 text-blue-300 underline" type="button" onClick={() => setAttempt(value => value + 1)}>Retry on-time performance</button></div>;
  return result.data ? <OtpPanel data={result.data} /> : null;
}

function SignalsView() {
  const [historical, setHistorical] = useState(false);
  return <><StudyPanel kind="signals" /><div className="mt-8 border-t border-slate-700 pt-5"><button type="button" className="text-sm text-blue-300 underline" aria-expanded={historical} onClick={() => setHistorical(value => !value)}>{historical ? 'Hide' : 'Open'} earlier streetcar priority scenarios</button>{historical && <div className="mt-6"><PriorityPanel /></div>}</div></>;
}

const views = [
  { id: 'row', label: 'Roadway time', detail: 'Reserved vs shared roadway' },
  { id: 'signals', label: 'Signals', detail: 'Detected waits and priority' },
  { id: 'overview', label: 'Overview', detail: 'Network and data coverage' },
  { id: 'otp', label: 'On-time performance', detail: 'Service against the schedule' },
] as const;
type ViewId = typeof views[number]['id'];
function viewFromHash(): ViewId {
  const hash = typeof window === 'undefined' ? '' : window.location.hash.slice(1);
  return hash === 'signal-priority' || hash === 'priority' ? 'signals' : views.find(view => view.id === hash)?.id ?? 'row';
}

function App({ initialView }: { initialView?: ViewId }) {
  const [view, setView] = useState<ViewId>(initialView ?? viewFromHash);
  useEffect(() => {
    const handleHashChange = () => setView(viewFromHash());
    window.addEventListener('hashchange', handleHashChange);
    return () => window.removeEventListener('hashchange', handleHashChange);
  }, []);
  useEffect(() => { window.scrollTo(0, 0); }, [view]);
  return <div className="min-h-screen px-4 py-6 sm:px-6 sm:py-8">
    <div className="max-w-7xl mx-auto">
      <header className="mb-6 sm:mb-8">
        <p className="text-xs uppercase tracking-widest text-amber-300 mb-2">Independent transit data</p>
        <h1 className="text-2xl sm:text-3xl font-semibold tracking-tight">NOLA transit performance</h1>
        <p className="text-slate-300 mt-2 max-w-2xl">Measure roadway travel time and candidate signal waits using our observations, with Le Pass as a separate comparison source.</p>
      </header>
      <nav aria-label="Dashboard views" className="sticky top-0 z-20 -mx-4 px-4 pt-2 pb-4 bg-slate-900/95 backdrop-blur sm:rounded-xl mb-5">
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
          {views.map(item => <a key={item.id} href={`#${item.id}`} aria-current={view === item.id ? 'page' : undefined}
            className={`rounded-lg border px-3 py-3 sm:px-4 transition-colors focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-amber-300 ${view === item.id ? 'border-amber-300 bg-amber-300 text-slate-950' : 'border-slate-600 bg-slate-800 text-slate-200 hover:bg-slate-700'}`}
            onClick={() => setView(item.id)}>
            <span className="block text-sm sm:text-base font-semibold">{item.label}</span>
            <span className={`hidden sm:block text-xs mt-1 ${view === item.id ? 'text-slate-800' : 'text-slate-400'}`}>{item.detail}</span>
          </a>)}
        </div>
      </nav>
      <SourceQuality />
      <main id="dashboard-content" aria-label={views.find(item => item.id === view)?.label}>
        {view === 'row' && <StudyPanel key="row" kind="row" />}
        {view === 'signals' && <SignalsView />}
        {view === 'overview' && <OverviewView />}
        {view === 'otp' && <OtpView />}
      </main>
      <footer className="border-t border-slate-700 mt-10 pt-5 text-sm text-slate-400 flex flex-wrap gap-x-6 gap-y-2 justify-between">
        <p>Independent observations. Sources are analyzed separately; missing data is not zero delay.</p>
        <a href="https://github.com/adamdavies1915/transport-experiments" className="text-blue-300 hover:underline">Source and methods</a>
      </footer>
    </div>
  </div>;
}

export default App;
