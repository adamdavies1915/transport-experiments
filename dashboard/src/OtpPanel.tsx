import { useState } from 'react';
import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { observedIdsOnly, percentage, totals, type OtpData } from './otp-data';

const pct = (value: number | null) => value === null ? 'Unavailable' : `${value.toFixed(1)}%`;
// Published benchmarks stay separate from our observations and calculation.
const reference: Record<string, { march: number; april: number }> = {
  '11': { march: 85.1, april: 80.5 }, '3': { march: 78.4, april: 78.0 },
  '57': { march: 70.4, april: 73.3 }, '9': { march: 73.8, april: 72.2 },
  '66': { march: 62.4, april: 68.7 },
};
const referenceUrl = 'https://norta.legistar.com/View.ashx?GUID=91C6E35C-D2F7-4736-8CFC-3D154B5DFBE4&ID=1365378&M=PA';

export default function OtpPanel({ data }: { data: OtpData }) {
  const months = [...new Set(data.days.map(d => d.date.slice(0, 7)))].sort().reverse();
  const [selectedMonth, setSelectedMonth] = useState('');
  const [selectedRoute, setSelectedRoute] = useState('all');
  const [matching, setMatching] = useState('all');
  const month = selectedMonth || months[0] || '';
  const monthly = data.days.filter(d => d.date.startsWith(month)).map(d => matching === 'observed' ? observedIdsOnly(d) : d);
  const routes = [...new Set(monthly.map(d => d.route))].sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));
  const route = routes.includes(selectedRoute) ? selectedRoute : 'all';
  const selected = monthly.filter(d => route === 'all' || d.route === route);
  const summary = totals(selected);
  const calendarDates = month ? Array.from({ length: new Date(Number(month.slice(0, 4)), Number(month.slice(5)), 0).getDate() },
    (_, i) => `${month}-${String(i + 1).padStart(2, '0')}`) : [];
  const daily = calendarDates.map(date => ({
    date, ...totals(selected.filter(d => d.date === date)),
  }));
  const routeRows = routes.filter(r => route === 'all' || route === r).map(route => ({
    route, ...totals(monthly.filter(d => d.route === route)),
  }));
  const latest = selected.map(d => d.updated_at).sort().at(-1);
  return (
    <section className="bg-slate-800 rounded-lg p-6 mb-8" aria-labelledby="otp-heading">
      <h2 id="otp-heading" className="text-xl font-semibold mb-2">Schedule-based On-Time Performance</h2>
      <p className="text-sm text-slate-300 mb-4">
        Independent estimate from our vehicle observations at scheduled timepoints.
        On time: 1 minute early through 5 minutes late. Departures are measured at intermediate
        stops; arrivals at the final stop. Each classified stop event counts once.
        Reported OTP uses direct trip IDs, observed ID mappings, and historical trip-order reconstruction.
      </p>
      {data.status === 'not_ready' ? (
        <p className="text-amber-300">Schedule-based OTP is not available yet. Waiting for a valid schedule and matched observations. Missing data is not counted as on time.</p>
      ) : (
        <>
          <div className="flex flex-wrap gap-4 mb-4">
            <label>Month <select className="bg-slate-900 rounded p-2 ml-2" value={month} onChange={e => setSelectedMonth(e.target.value)}>
              {months.map(m => <option key={m}>{m}</option>)}
            </select></label>
            <label>Trip matching <select className="bg-slate-900 rounded p-2 ml-2" value={matching} onChange={e => setMatching(e.target.value)}>
              <option value="all">Include inferred historical trips</option>
              <option value="observed">Observed trip IDs only</option>
            </select></label>
            <label>Route <select className="bg-slate-900 rounded p-2 ml-2" value={route} onChange={e => setSelectedRoute(e.target.value)}>
              <option value="all">All bus and streetcar routes</option>
              {routes.map(r => <option key={r}>{r}</option>)}
            </select></label>
          </div>
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-4">
            <div><p className="text-slate-400">On time</p><p className="text-2xl">{pct(summary.on_time_pct)}</p></div>
            <div><p className="text-slate-400">Early / late</p><p className="text-xl">{pct(percentage(summary.early, summary.classified))} / {pct(percentage(summary.late, summary.classified))}</p></div>
            <div><p className="text-slate-400">Classified events</p><p className="text-2xl">{summary.classified.toLocaleString()}</p></div>
            <div><p className="text-slate-400">Schedule coverage</p><p className="text-2xl">{pct(summary.coverage_pct)}</p></div>
          </div>
          <p className="text-sm text-slate-300 mb-4">
            Schedules available for {new Set(selected.map(d => d.date)).size} service dates this month.
            {' '}
            {summary.classified.toLocaleString()} of {summary.scheduled.toLocaleString()} scheduled timepoints measured with a clear outcome;
            {' '}{summary.uncertain.toLocaleString()} observations straddle a timing boundary.
            {' '}{summary.crosswalk_events.toLocaleString()} stop events were reconstructed using observed ID mappings.
            {' '}{summary.sequence_events.toLocaleString()} stop events use inferred historical trip IDs from recurring complete block sequences.
            {' '}{summary.matched_trips.toLocaleString()} of {summary.observed_trips.toLocaleString()} observed trip groups matched;
            {' '}{summary.block_matched_trips.toLocaleString()} use inferred block matches and are excluded from reported OTP.
            Missing, ambiguous, and unobserved events are excluded from OTP, so incomplete coverage can bias this estimate.
          </p>
          {summary.sequence_events > 0 && <p className="text-amber-300 mb-4">Historical inference included: trip order must agree on at least two complete service dates. Current-data checks can validate matching, but cannot prove every historical assignment. Select observed trip IDs only to exclude this inference.</p>}
          {summary.classified === 0 && <p className="text-amber-300 mb-4">No reliably classified stop events for this selection. OTP remains unavailable.</p>}
          {summary.classified > 0 && (summary.coverage_pct ?? 0) < 50 && <p className="text-amber-300 mb-4">Low coverage: this sample may not represent the full service.</p>}
          <ResponsiveContainer width="100%" height={260}>
            <LineChart data={daily}>
              <CartesianGrid strokeDasharray="3 3" stroke="#475569" />
              <XAxis dataKey="date" stroke="#94a3b8" tickFormatter={d => String(d).slice(5)} />
              <YAxis stroke="#94a3b8" domain={[0, 100]} unit="%" />
              <Tooltip contentStyle={{ backgroundColor: '#1e293b', border: 'none' }} formatter={value => [value == null ? 'Unavailable' : `${Number(value).toFixed(1)}%`, 'On time']} />
              <Line type="linear" dataKey="on_time_pct" stroke="#3b82f6" dot connectNulls={false} />
            </LineChart>
          </ResponsiveContainer>
          <div className="overflow-x-auto mt-4">
            <table className="w-full text-left">
              <caption className="text-left text-lg mb-3">On-Time Performance by Route — {month}</caption>
              <thead><tr className="border-b border-slate-600">
                {['Route', 'Classified events', 'Our OTP', 'Early', 'Late', 'Coverage', 'RTA OTP*', 'Difference*'].map(label => <th key={label} className="p-2 font-medium">{label}</th>)}
              </tr></thead>
              <tbody>{routeRows.map(r => {
                const official = month === '2026-03' ? reference[r.route]?.march : month === '2026-04' ? reference[r.route]?.april : undefined;
                const difference = official !== undefined && r.on_time_pct !== null ? r.on_time_pct - official : null;
                return <tr key={r.route} className="border-b border-slate-700">
                  <td className="p-2">{r.route}</td><td className="p-2">{r.classified.toLocaleString()}</td>
                  <td className="p-2">{pct(r.on_time_pct)}</td>
                  <td className="p-2">{pct(percentage(r.early, r.classified))}</td>
                  <td className="p-2">{pct(percentage(r.late, r.classified))}</td>
                  <td className="p-2">{pct(r.coverage_pct)}</td>
                  <td className="p-2">{official === undefined ? '—' : `${official.toFixed(1)}%`}</td>
                  <td className="p-2">{difference === null ? '—' : `${difference > 0 ? '+' : ''}${difference.toFixed(1)} pp`}</td>
                </tr>;
              })}</tbody>
            </table>
          </div>
          {latest && <p className="text-xs text-slate-400 mt-3">Calculated {latest}. Current-day coverage is partial; dates use New Orleans service days.</p>}
        </>
      )}
      <details className="mt-4 text-sm text-slate-300">
        <summary className="cursor-pointer">Method and RTA comparison</summary>
        <p className="mt-2">OTP = on-time events ÷ (early + on-time + late events). GPS visits within 35 metres of a stop bracket a departure or arrival, with at most 120 seconds between observations. If the bracket crosses the −1 or +5 minute boundary, the outcome is uncertain. Repeated or nearby stops and conflicting trip assignments remain unmeasured.</p>
        <p className="mt-2">Reported OTP requires a direct schedule trip ID or a conflict-free tatripid-to-tripid pair observed in the feed and tied to the same schedule archive. Reconstructed history assumes that pair remained stable during that schedule; route, available block and destination, and active service are checked. For historical diagnostics, a unique route, block and destination match must fit the active schedule within a 90-minute allowance. These inferred events are stored separately and excluded from reported OTP because that allowance can bias the sample. RTA may use different observations and exclusions.</p>
        <p className="mt-2">* RTA figures are separate benchmarks for the same route and month, never inputs to our OTP calculation. The 1-minute-early / 5-minutes-late window follows the historically documented RTA definition; equivalence to its current internal reporting has not been verified.</p>
        <p className="mt-2"><a className="text-blue-400 underline" href={referenceUrl}>RTA report, pages 26–27 (March/April 2026)</a>{' · '}<a className="text-blue-400 underline" href="https://rideneworleans.org/wp/wp-content/uploads/2021/10/2021-RIDE-State-of-Transit-.pdf">Documented RTA timing window</a></p>
      </details>
    </section>
  );
}
