import { useTransitData } from './hooks/useTransitData';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer, Cell
} from 'recharts';

const COLORS = {
  dedicated: '#22c55e',
  mixed: '#ef4444',
  primary: '#3b82f6',
  secondary: '#8b5cf6'
};

function StatCard({ title, value, subtitle, color = 'blue' }) {
  const colorClasses = {
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

  const dedicatedData = data.segmentType.find(s => s.segment_type === 'dedicated_row') || {};
  const mixedData = data.segmentType.find(s => s.segment_type === 'mixed_traffic') || {};
  const speedDiff = ((dedicatedData.avg_speed || 0) / (mixedData.avg_speed || 1)).toFixed(1);

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
            value={`${dedicatedData.avg_speed || 0} mph`}
            subtitle={`${speedDiff}x faster than mixed`}
            color="green"
          />
          <StatCard
            title="Mixed Traffic Speed"
            value={`${mixedData.avg_speed || 0} mph`}
            subtitle={`${mixedData.delay_pct || 0}% delayed`}
            color="red"
          />
          <StatCard
            title="Data Range"
            value={`${Math.round((new Date(data.summary.last_record) - new Date(data.summary.first_record)) / 3600000)}h`}
            subtitle="of collection"
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

        {/* Route Performance Table */}
        <div className="bg-slate-800 rounded-lg p-6 mb-8">
          <h2 className="text-xl font-semibold mb-4">On-Time Performance by Route</h2>
          <div className="overflow-x-auto">
            <table className="w-full text-left">
              <thead>
                <tr className="border-b border-slate-700">
                  <th className="pb-3 text-slate-400 font-medium">Route</th>
                  <th className="pb-3 text-slate-400 font-medium text-right">Readings</th>
                  <th className="pb-3 text-slate-400 font-medium text-right">On-Time %</th>
                  <th className="pb-3 text-slate-400 font-medium text-right">Avg Speed</th>
                  <th className="pb-3 text-slate-400 font-medium">Performance</th>
                </tr>
              </thead>
              <tbody>
                {data.routes.map((route, i) => (
                  <tr key={i} className="border-b border-slate-700/50">
                    <td className="py-3 font-medium">{route.route}</td>
                    <td className="py-3 text-right text-slate-400">{Number(route.readings).toLocaleString()}</td>
                    <td className="py-3 text-right">
                      <span className={route.on_time_pct >= 90 ? 'text-green-400' : route.on_time_pct >= 80 ? 'text-yellow-400' : 'text-red-400'}>
                        {route.on_time_pct}%
                      </span>
                    </td>
                    <td className="py-3 text-right">{route.avg_speed} mph</td>
                    <td className="py-3">
                      <div className="w-full bg-slate-700 rounded-full h-2">
                        <div
                          className={`h-2 rounded-full ${route.on_time_pct >= 90 ? 'bg-green-500' : route.on_time_pct >= 80 ? 'bg-yellow-500' : 'bg-red-500'}`}
                          style={{ width: `${route.on_time_pct}%` }}
                        ></div>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
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
