import OtpPanel from './OtpPanel';
import StreetcarPanel from './StreetcarPanel';
import { useTransitData } from './hooks/useTransitData';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer, Cell, LineChart, Line
} from 'recharts';
import type { ReactNode } from 'react';

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

function App() {
  const { data, loading, error } = useTransitData();

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-500 mx-auto"></div>
          <p className="mt-4 text-slate-400">Loading transit data...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="bg-red-500/10 border border-red-500 rounded-lg p-6 max-w-md">
          <h2 className="text-red-500 font-bold text-lg">Error</h2>
          <p className="mt-2 text-slate-300">{error}</p>
        </div>
      </div>
    );
  }

  if (!data) return null;

  const dedicatedData = data.segmentType.find(s => s.segment_type === 'dedicated_row');
  const mixedData = data.segmentType.find(s => s.segment_type === 'mixed_traffic');
  const speedDiff = ((dedicatedData?.avg_speed || 0) / (mixedData?.avg_speed || 1)).toFixed(1);

  return (
    <div className="min-h-screen p-6">
      <div className="max-w-7xl mx-auto">
        {/* Header */}
        <div className="mb-8">
          <h1 className="text-3xl font-bold">NOLA Transit Dashboard</h1>
          <p className="text-slate-400 mt-1">
            Independent transit performance data
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
            subtitle={`${speedDiff}x faster than mixed`}
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
            <h2 className="text-xl font-semibold mb-4">Streetcar: ROW vs Mixed Traffic</h2>
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

        <OtpPanel data={data.otp} />
        <StreetcarPanel />

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

        {/* Footer */}
        <div className="text-center text-slate-500 text-sm">
          <p>Data collected independently from NOLA RTA real-time feed</p>
          <p className="mt-1">
            <a href="https://github.com/adamdavies1915/transport-experiments" className="text-blue-400 hover:underline">
              View Source
            </a>
          </p>
        </div>
      </div>
    </div>
  );
}

export default App;
