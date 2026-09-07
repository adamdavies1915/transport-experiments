import { useEffect, useState } from 'react';
import { Bar, BarChart, CartesianGrid, Cell, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import type { CorridorId, ExposureCategory, StreetcarBin, StreetcarData, StreetcarNetwork, StreetcarSite } from './streetcar-data';

const categories: Array<{ id: ExposureCategory; label: string; color: string; description: string }> = [
  { id: 'signal_only', label: 'Signal only', color: '#fbbf24', description: 'Near a signal, away from passenger stops' },
  { id: 'stop_only', label: 'Stop only', color: '#22d3ee', description: 'Near a passenger stop, away from signals' },
  { id: 'both', label: 'Stop + signal', color: '#c084fc', description: 'Both features nearby; their effects cannot be separated' },
  { id: 'neither', label: 'Neither nearby', color: '#94a3b8', description: 'Outside the stop and signal areas' },
];
const number = (value: number) => value.toLocaleString(undefined, { maximumFractionDigits: 0 });
const speed = (value: number | null) => value === null ? 'Unavailable' : `${value.toFixed(1)} mph`;
const timeRange = (lower: number | null, upper: number | null) => lower === null || upper === null ? 'Unavailable' : `${lower.toFixed(1)}–${upper.toFixed(1)} s`;
const hourLabel = (hour: number) => `${String(hour).padStart(2, '0')}:00`;
const inputClass = 'bg-slate-900 border border-slate-600 rounded px-3 py-2 text-sm text-slate-100';

function summarize(rows: StreetcarBin[]) {
  const summary = rows.reduce((acc, row) => {
    acc.intervals += row.intervals;
    acc.seconds += row.duration_seconds;
    acc.meters += row.distance_meters;
    const lower = row.duration_lower_seconds;
    const upper = row.duration_upper_seconds;
    if (lower == null || upper == null || !Number.isFinite(lower) || !Number.isFinite(upper) || lower < 0 || upper < lower) {
      acc.completeBounds = false;
    } else {
      acc.lowerSeconds += lower;
      acc.upperSeconds += upper;
    }
    row.vehicle_ids.forEach(id => acc.vehicles.add(id));
    return acc;
  }, { intervals: 0, seconds: 0, meters: 0, lowerSeconds: 0, upperSeconds: 0, completeBounds: true, vehicles: new Set<string>() });
  const hasBounds = summary.intervals > 0 && summary.completeBounds;
  return { ...summary, mph: summary.seconds > 0 ? summary.meters / summary.seconds * 2.2369362921 : null,
    lowerMean: hasBounds ? summary.lowerSeconds / summary.intervals : null,
    upperMean: hasBounds ? summary.upperSeconds / summary.intervals : null,
    speedLower: hasBounds && summary.upperSeconds > 0 ? summary.meters / summary.upperSeconds * 2.2369362921 : null,
    speedUpper: hasBounds && summary.lowerSeconds > 0 ? summary.meters / summary.lowerSeconds * 2.2369362921 : null,
  };
}

function speedRange(summary: ReturnType<typeof summarize>) {
  if (summary.speedLower === null) return 'Unavailable';
  return summary.speedUpper === null ? `${summary.speedLower.toFixed(1)} mph to an unbounded upper speed` : `${summary.speedLower.toFixed(1)}–${summary.speedUpper.toFixed(1)} mph`;
}

type Filters = { corridor: CorridorId; from: string; to: string; direction: string; dayType: string; hourFrom: number; hourTo: number };
type Props = { data?: StreetcarData; initialFilters?: Partial<Filters>; initialSelectedSite?: string };

export function NetworkMap({ network, corridor, direction, selectedSite, mixedSites, onSelect }: {
  network: StreetcarNetwork; corridor: CorridorId; direction: string; selectedSite: string;
  mixedSites: Set<string>;
  onSelect: (id: string) => void;
}) {
  const [zoom, setZoom] = useState(1);
  const paths = network.paths.filter(path => path.corridor === corridor && (direction === 'all' || path.direction === direction));
  const routes = new Set(paths.map(path => path.route));
  const sites = network.sites.filter(site => site.corridor === corridor && (routes.size === 0 || site.routes.some(route => routes.has(route))));
  const points = paths.flatMap(path => path.points);
  const bounds = points.length ? points : sites;
  const minLon = Math.min(...bounds.map(point => point.lon));
  const maxLon = Math.max(...bounds.map(point => point.lon));
  const minLat = Math.min(...bounds.map(point => point.lat));
  const maxLat = Math.max(...bounds.map(point => point.lat));
  const longitudeScale = Math.cos((minLat + maxLat) / 2 * Math.PI / 180);
  const width = Math.max((maxLon - minLon) * longitudeScale, 0.0001);
  const height = Math.max(maxLat - minLat, 0.0001);
  const scale = Math.min(920 / width, 410 / height);
  const project = (point: { lat: number; lon: number }) => ({
    x: 500 + (point.lon - (minLon + maxLon) / 2) * longitudeScale * scale,
    y: 240 - (point.lat - (minLat + maxLat) / 2) * scale,
  });
  const selected = sites.find(site => site.id === selectedSite);
  const focus = selected ? project(selected) : { x: 500, y: 240 };
  const viewWidth = 1000 / zoom;
  const viewHeight = 480 / zoom;
  const originX = Math.max(0, Math.min(1000 - viewWidth, focus.x - viewWidth / 2));
  const originY = Math.max(0, Math.min(480 - viewHeight, focus.y - viewHeight / 2));
  return <div className="rounded-lg border border-slate-600 bg-slate-950 overflow-hidden">
    <div className="flex flex-wrap gap-3 items-center justify-between px-4 pt-3">
      <div className="text-sm text-slate-300 flex flex-wrap gap-4">
        <span><span className="text-amber-300">◆</span> Road signal</span>
        <span><span className="text-cyan-300">●</span> Passenger stop</span>
        <span><span className="text-orange-300">■</span> Rail signal — function unverified</span>
        <span><span className="text-purple-400">◯</span> Stop + signal passages</span>
      </div>
      <div className="flex gap-2 text-sm">
        <button type="button" className="rounded border border-slate-600 px-3 py-1 disabled:opacity-40" aria-label="Zoom out of streetcar map" disabled={zoom <= 1} onClick={() => setZoom(value => Math.max(1, value / 1.5))}>−</button>
        <button type="button" className="rounded border border-slate-600 px-3 py-1 disabled:opacity-40" aria-label="Zoom into streetcar map" disabled={zoom >= 5} onClick={() => setZoom(value => Math.min(5, value * 1.5))}>+</button>
        <button type="button" className="rounded border border-slate-600 px-3 py-1" onClick={() => { setZoom(1); onSelect(''); }}>Reset map</button>
      </div>
    </div>
    {bounds.length === 0 ? <p className="p-8 text-slate-400">No mapped geometry for this selection.</p> : <svg
      className="w-full max-h-[440px]" viewBox={`${originX} ${originY} ${viewWidth} ${viewHeight}`}
      role="group" aria-label="Streetcar tracks, passenger stops, and mapped traffic signals. Select a site to inspect its observations.">
      <title>Streetcar tracks and nearby passenger stops and signals</title>
      {paths.map(path => <polyline key={path.id} points={path.points.map(point => { const p = project(point); return `${p.x},${p.y}`; }).join(' ')} fill="none" stroke="#64748b" strokeWidth={3 / zoom} opacity={0.6}><title>{`Route ${path.route}: ${path.headsign}`}</title></polyline>)}
      {sites.map(site => {
        const point = project(site);
        const size = (site.id === selectedSite ? 7 : 4) / Math.sqrt(zoom);
        const label = `${site.name}: ${site.kind === 'stop' ? 'passenger stop' : site.kind === 'rail_signal' ? 'rail signal — function unverified' : 'road signal'}`;
        return <g key={site.id} transform={`translate(${point.x},${point.y})`} role="button" tabIndex={0}
          aria-label={`Inspect ${label}`} aria-pressed={site.id === selectedSite} className="cursor-pointer outline-none focus:stroke-white"
          onClick={() => onSelect(site.id)} onKeyDown={event => { if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); onSelect(site.id); } }}>
          <title>{label}</title>
          <circle r={11 / Math.sqrt(zoom)} fill="transparent" />
          {mixedSites.has(site.id) && <circle r={size + 3 / Math.sqrt(zoom)} fill="none" stroke="#c084fc" strokeWidth={1.5 / Math.sqrt(zoom)} />}
          {site.id === selectedSite && <circle r={size + 5 / zoom} fill="#fff" opacity={0.2} stroke="#fff" strokeWidth={1 / zoom} />}
          {site.kind === 'stop' ? <circle r={size} fill="#22d3ee" stroke="#082f49" strokeWidth={1 / zoom} /> :
            <rect x={-size} y={-size} width={size * 2} height={size * 2} transform={site.kind === 'signal' ? 'rotate(45)' : undefined} fill={site.kind === 'signal' ? '#fbbf24' : '#fb923c'} stroke="#451a03" strokeWidth={1 / zoom} />}
        </g>;
      })}
    </svg>}
    <p className="px-4 pb-3 text-xs text-slate-400">Select a marker, then zoom to inspect nearby sites. Geometry: GTFS tracks and stops · signals © <a href="https://www.openstreetmap.org/copyright" className="underline">OpenStreetMap contributors</a>. Signal presence is mapped; signal phases are unknown.</p>
  </div>;
}

export default function StreetcarPanel({ data: suppliedData, initialFilters, initialSelectedSite }: Props) {
  const [filters, setFilters] = useState<Filters>({ corridor: 'st_charles', from: '', to: '', direction: 'all', dayType: 'all', hourFrom: 0, hourTo: 23, ...initialFilters });
  const [selectedSite, setSelectedSite] = useState(initialSelectedSite || '');
  const [siteSearch, setSiteSearch] = useState('');
  const [siteSort, setSiteSort] = useState('samples');
  const [showAllSites, setShowAllSites] = useState(false);
  const [retry, setRetry] = useState(0);
  const [remote, setRemote] = useState<{ key: string; data?: StreetcarData; error?: string }>({ key: '' });
  const query = new URLSearchParams({ corridor: filters.corridor });
  if (filters.from) query.set('from', filters.from);
  if (filters.to) query.set('to', filters.to);
  const url = `/api/streetcars?${query}`;
  const requestKey = `${url}#${retry}`;
  useEffect(() => {
    if (suppliedData) return;
    const controller = new AbortController();
    fetch(url, { signal: controller.signal }).then(async response => {
      if (!response.ok) {
        const body = await response.json().catch(() => null) as { error?: string } | null;
        throw new Error(body?.error || `Streetcar analysis request failed (${response.status}).`);
      }
      return response.json() as Promise<StreetcarData>;
    }).then(data => { if (!controller.signal.aborted) setRemote({ key: requestKey, data }); })
      .catch((error: Error) => { if (!controller.signal.aborted) setRemote(previous => ({ key: requestKey, data: previous.data, error: error.message })); });
    return () => controller.abort();
  }, [url, requestKey, suppliedData]);
  const data = suppliedData ?? remote.data;
  const loading = !suppliedData && remote.key !== requestKey;
  const error = suppliedData ? undefined : remote.error;
  const dates = data ? [...new Set([...data.bins.map(row => row.date), ...data.quality.map(row => row.date)])].sort() : [];
  const from = filters.from || data?.selected_from || dates[0] || '';
  const to = filters.to || data?.selected_to || dates.at(-1) || '';
  const availableFrom = data?.available_from || dates[0] || '';
  const availableTo = data?.available_to || dates.at(-1) || '';
  const recentLimit = availableTo ? new Date(new Date(`${availableTo}T12:00:00Z`).getTime() - 89 * 86400000).toISOString().slice(0, 10) : '';
  const expandedFrom = availableFrom && recentLimit ? (availableFrom > recentLimit ? availableFrom : recentLimit) : availableFrom;
  const directions = data ? [...new Set(data.network.paths.filter(path => path.corridor === filters.corridor).map(path => path.direction))].sort() : [];
  const direction = directions.includes(filters.direction) ? filters.direction : 'all';
  const accepts = (row: StreetcarBin) => row.corridor === filters.corridor && (!from || row.date >= from) && (!to || row.date <= to)
    && (direction === 'all' || row.direction === direction) && (filters.dayType === 'all' || row.day_type === filters.dayType)
    && row.hour >= filters.hourFrom && row.hour <= filters.hourTo;
  const rows = data?.bins.filter(accepts) ?? [];
  const summary = summarize(rows);
  const windowMeters = data?.method.window_meters;
  const timestampPrecision = data?.method.timestamp_quantization_seconds;
  const byCategory = categories.map(category => ({ ...category, ...summarize(rows.filter(row => row.category === category.id)) }));
  const signal = byCategory[0];
  const neither = byCategory[3];
  const comparableBounds = signal.lowerMean !== null && signal.upperMean !== null && neither.lowerMean !== null && neither.upperMean !== null;
  const boundsOverlap = comparableBounds && signal.lowerMean! <= neither.upperMean! && neither.lowerMean! <= signal.upperMean!;
  const selectedBins = data?.site_bins.filter(accepts) ?? [];
  const groupedSites = new Map<string, typeof selectedBins>();
  selectedBins.forEach(row => { const previous = groupedSites.get(row.site_id); if (previous) previous.push(row); else groupedSites.set(row.site_id, [row]); });
  const siteRows = data?.network.sites.filter(site => site.corridor === filters.corridor).map(site => {
    const bins = groupedSites.get(site.id) ?? [];
    return { site, ...summarize(bins), contexts: categories.filter(category => bins.some(row => row.category === category.id)) };
  }) ?? [];
  const matchingSites = siteRows.filter(row => row.site.name.toLowerCase().includes(siteSearch.toLowerCase())).sort((a, b) => siteSort === 'time' ? (b.intervals ? b.seconds / b.intervals : -1) - (a.intervals ? a.seconds / a.intervals : -1) : siteSort === 'name' ? a.site.name.localeCompare(b.site.name) : b.intervals - a.intervals);
  const visibleSites = showAllSites ? matchingSites : matchingSites.slice(0, 15);
  const selected = siteRows.find(row => row.site.id === selectedSite);
  const reviewedImages = [...new Set(selected?.site.source_ids.filter(id => /^mapillary\/image\/\d+$/.test(id)).map(id => id.split('/')[2]) ?? [])];
  const quality = data?.quality.filter(row => row.corridor === filters.corridor && (!from || row.date >= from) && (!to || row.date <= to)) ?? [];
  const exclusionCounts: Record<string, number> = {};
  quality.forEach(row => Object.entries(row.excluded).forEach(([reason, count]) => { exclusionCounts[reason] = (exclusionCounts[reason] ?? 0) + count; }));
  const excluded = Object.entries(exclusionCounts).sort((a, b) => b[1] - a[1]);
  const selectSite = (id: string) => setSelectedSite(id);
  const siteType = (site: StreetcarSite) => site.kind === 'stop' ? 'Passenger stop' : site.kind === 'rail_signal' ? 'Rail signal — function unverified' : 'Road signal';

  return <section className="bg-slate-800 rounded-lg p-6 mb-8" aria-labelledby="streetcar-heading">
    <div className="flex flex-wrap justify-between items-start gap-3 mb-3">
      <div><p className="text-xs tracking-widest uppercase text-amber-300 mb-1">Streetcar corridor study</p>
        <h2 id="streetcar-heading" className="text-xl font-semibold">Streetcar speed &amp; traffic signals</h2></div>
      {data?.updated_at && <p className="text-xs text-slate-400">Updated {data.updated_at}</p>}
    </div>
    <p className="text-sm text-slate-300 mb-5">Compare travel through equal-length track windows near signals, passenger stops, and places with neither. Only completed passages count. Keep the corridor, direction, and time window consistent when exploring the differences.</p>
    <div className="flex flex-wrap gap-2 mb-4" aria-label="Streetcar corridor">
      {([{ id: 'st_charles', name: 'St. Charles' }, { id: 'canal', name: 'Canal' }, { id: 'rampart', name: 'Rampart' }] as const).map(corridor => <button key={corridor.id} type="button" aria-pressed={filters.corridor === corridor.id}
        className={`rounded-full px-4 py-2 text-sm border ${filters.corridor === corridor.id ? 'bg-amber-300 text-slate-950 border-amber-300 font-semibold' : 'border-slate-500 text-slate-200 hover:bg-slate-700'}`}
        onClick={() => { setFilters(previous => ({ ...previous, corridor: corridor.id, direction: 'all' })); setSelectedSite(''); }}>{corridor.name}</button>)}
    </div>
    {data && <div className="flex flex-wrap items-end gap-3 mb-5">
      <label className="text-xs text-slate-300">From<input type="date" className={`${inputClass} block mt-1`} aria-label="Streetcar start date" value={from} min={availableFrom || undefined} max={to || undefined} onChange={event => setFilters(previous => ({ ...previous, from: event.target.value, to }))} /></label>
      <label className="text-xs text-slate-300">Through<input type="date" className={`${inputClass} block mt-1`} aria-label="Streetcar end date" value={to} min={from || undefined} max={availableTo || undefined} onChange={event => setFilters(previous => ({ ...previous, from, to: event.target.value }))} /></label>
      <label className="text-xs text-slate-300">Direction<select className={`${inputClass} block mt-1 max-w-64`} value={direction} onChange={event => setFilters(previous => ({ ...previous, direction: event.target.value }))}>
        <option value="all">All directions</option>{directions.map(value => <option key={value} value={value}>Direction {value} · {[...new Set(data.network.paths.filter(path => path.corridor === filters.corridor && path.direction === value).map(path => path.headsign))].join(' / ')}</option>)}</select></label>
      <label className="text-xs text-slate-300">Days<select className={`${inputClass} block mt-1`} value={filters.dayType} onChange={event => setFilters(previous => ({ ...previous, dayType: event.target.value }))}>
        <option value="all">All days</option><option value="weekday">Weekdays</option><option value="weekend">Weekends</option></select></label>
      <label className="text-xs text-slate-300">From hour<select className={`${inputClass} block mt-1`} value={filters.hourFrom} onChange={event => setFilters(previous => ({ ...previous, hourFrom: Number(event.target.value), hourTo: Math.max(previous.hourTo, Number(event.target.value)) }))}>
        {Array.from({ length: 24 }, (_, hour) => <option key={hour} value={hour}>{hourLabel(hour)}</option>)}</select></label>
      <label className="text-xs text-slate-300">Through hour<select className={`${inputClass} block mt-1`} value={filters.hourTo} onChange={event => setFilters(previous => ({ ...previous, hourTo: Number(event.target.value), hourFrom: Math.min(previous.hourFrom, Number(event.target.value)) }))}>
        {Array.from({ length: 24 }, (_, hour) => <option key={hour} value={hour}>{hourLabel(hour)}–{String(hour).padStart(2, '0')}:59</option>)}</select></label>
      {availableFrom && availableTo && <button type="button" className="text-sm text-blue-300 underline py-2" onClick={() => setFilters(previous => ({ ...previous, from: expandedFrom, to: availableTo }))}>{expandedFrom === availableFrom ? 'All available dates' : 'Latest 90 days'}</button>}
    </div>}
    {loading && <p className="py-6 text-slate-300" role="status">Loading streetcar observations and mapped signals…</p>}
    {!loading && error && <div className="rounded border border-amber-700 p-4 text-amber-200" role="alert"><p>{error}</p><button className="underline mt-2" type="button" onClick={() => setRetry(value => value + 1)}>Retry streetcar analysis</button></div>}
    {!loading && !error && data && <>
      <p className="text-xs text-slate-400 mb-4">Dates and hours use New Orleans local time. Weekdays include weekday holidays. {availableFrom && availableTo ? `Historical data: ${availableFrom} through ${availableTo}. Select up to 90 days at a time.` : ''}</p>
      {data.status === 'not_ready' && <p className="text-amber-300 mb-4">Streetcar observations are being prepared. Mapped locations are available below; travel times remain unavailable until completed passages have been processed.</p>}
      <p className="text-sm text-slate-300 mb-3">Mean travel-time bounds{windowMeters ? ` per ${windowMeters} m passage` : ''}. These ranges include report spacing{timestampPrecision ? ` and ${timestampPrecision}-second timestamp precision` : ''}, not GPS position error. They are not statistical confidence intervals or estimates of delay caused by signals.</p>
      <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-3 mb-5">
        {byCategory.map(category => <div key={category.id} className="rounded-lg border border-slate-600 bg-slate-900 p-4" style={{ borderTopColor: category.color, borderTopWidth: 3 }}>
          <h3 className="font-medium" style={{ color: category.color }}>{category.label}</h3>
          <p className="text-xs text-slate-400 min-h-9 mt-1">{category.description}</p>
          <p className="text-2xl mt-2 font-semibold">{timeRange(category.lowerMean, category.upperMean)}</p>
          <p className="text-sm text-slate-300 mt-1">Estimated speed: {speed(category.mph)}</p>
          <p className="text-xs text-slate-400 mt-1">Speed bounds: {speedRange(category)}</p>
          <p className="text-xs text-slate-400 mt-2">{number(category.intervals)} completed passages · {number(category.vehicles.size)} vehicles</p>
        </div>)}
      </div>
      {summary.intervals === 0 ? <p className="text-amber-300 mb-5">No completed passages for this selection. Incomplete observations are excluded; missing data is not a measured zero speed.</p> : <div className="grid grid-cols-1 lg:grid-cols-2 gap-5 mb-6">
        <div className="min-w-0">
          <h3 className="text-sm text-slate-300 mb-2">Estimated average speed through equal-length windows</h3>
          <ResponsiveContainer width="100%" height={210}><BarChart data={byCategory} layout="vertical" margin={{ left: 12, right: 20 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#334155" horizontal={false} />
            <XAxis type="number" stroke="#94a3b8" domain={[0, 'auto']} unit=" mph" tick={{ fontSize: 11 }} />
            <YAxis dataKey="label" type="category" stroke="#94a3b8" width={100} tick={{ fontSize: 11 }} />
            <Tooltip cursor={{ fill: '#334155' }} contentStyle={{ backgroundColor: '#0f172a', borderColor: '#475569', color: '#f8fafc' }} formatter={value => [speed(value == null ? null : Number(value)), 'Estimated average speed']} />
            <Bar dataKey="mph" name="Estimated average speed" radius={[0, 4, 4, 0]}>{byCategory.map(category => <Cell key={category.id} fill={category.color} />)}</Bar>
          </BarChart></ResponsiveContainer>
        </div>
        <div className="rounded-lg bg-slate-900 p-5 text-sm text-slate-300">
          <h3 className="font-semibold text-slate-100 mb-2">What this comparison shows</h3>
          {!comparableBounds ? <p>A descriptive comparison needs completed passages with timing bounds in both signal-only windows and windows with neither feature. Neither category is assumed to represent free-flow travel.</p> : <>
            <p>Signal only: <strong className="text-slate-100">{timeRange(signal.lowerMean, signal.upperMean)}</strong>. Neither nearby: <strong className="text-slate-100">{timeRange(neither.lowerMean, neither.upperMean)}</strong>{windowMeters ? ` per ${windowMeters} m passage` : ''}.</p>
            <p className="mt-3">{boundsOverlap ? 'The timing bounds overlap, so they do not resolve a travel-time difference between these groups.' : 'These observed timing ranges differ, but do not establish delay caused by traffic signals.'}</p>
          </>}
          <p className="mt-3">This is an association between location and travel time. Traffic, track layout, turning movements, and service patterns also differ between locations. Stop + signal passages cannot isolate a traffic-light effect.</p>
          <p className="mt-3 text-slate-400">Estimated speed = total completed distance ÷ estimated passage time. Entry and exit times are interpolated between GPS reports. The timing ranges include report spacing{timestampPrecision ? ' and timestamp truncation' : ''}; they do not account for GPS position error.</p>
        </div>
      </div>}
      <h3 className="font-medium mb-3">Explore the corridor</h3>
      <NetworkMap key={filters.corridor} network={data.network} corridor={filters.corridor} direction={direction} selectedSite={selectedSite} mixedSites={new Set(selectedBins.filter(row => row.category === 'both').map(row => row.site_id))} onSelect={selectSite} />
      {selected && <div className="border border-slate-500 rounded-lg bg-slate-900 p-4 mt-3" aria-live="polite">
        <div className="flex justify-between gap-3"><h4 className="font-semibold">{selected.site.name}</h4><button type="button" className="text-sm underline text-slate-400" onClick={() => setSelectedSite('')}>Clear selection</button></div>
        <p className="text-xs text-slate-400 mt-1">{siteType(selected.site)} · {selected.site.verification === 'gtfs' ? 'GTFS location' : selected.site.verification === 'mapillary' ? reviewedImages.length > 0 ? 'Mapillary location corroborated; imagery reviewed' : 'Mapillary detection corroborated; imagery unreviewed' : 'OSM location; imagery unverified'} · {selected.site.lat.toFixed(5)}, {selected.site.lon.toFixed(5)}</p>
        {reviewedImages.length > 0 && <div className="flex flex-wrap gap-3 text-sm mt-2">{reviewedImages.map((id, index) => <a key={id} className="text-blue-300 underline" href={`https://www.mapillary.com/app/?pKey=${encodeURIComponent(id)}`} target="_blank" rel="noreferrer">View reviewed Mapillary imagery{reviewedImages.length > 1 ? ` (${index + 1})` : ''}</a>)}</div>}
        {selected.site.kind === 'rail_signal' && <p className="text-sm text-orange-300 mt-3">Passages near this rail-tagged signal are excluded from headline comparisons until its function is verified. It may control streetcar traffic or a track switch.</p>}
        <p className="mt-3 text-sm">Mean travel-time bounds: {timeRange(selected.lowerMean, selected.upperMean)}{windowMeters ? ` per ${windowMeters} m` : ''} · {number(selected.intervals)} completed passages</p>
        <p className="mt-1 text-sm text-slate-300">Estimated speed: {speed(selected.mph)} · Speed bounds: {speedRange(selected)}</p>
        <div className="flex flex-wrap gap-x-5 gap-y-2 mt-3 text-xs">{categories.map(category => {
          const categorySummary = summarize((groupedSites.get(selected.site.id) ?? []).filter(row => row.category === category.id));
          return <span key={category.id}><span style={{ color: category.color }}>{category.label}</span>: {timeRange(categorySummary.lowerMean, categorySummary.upperMean)} ({number(categorySummary.intervals)} passages)</span>;
        })}</div>
        {selected.contexts.some(category => category.id === 'both') && <p className="text-sm text-purple-300 mt-3">Some passages also lie near a passenger stop and a signal. Their travel time cannot be assigned to the signal alone.</p>}
      </div>}
      <div className="flex flex-wrap items-center justify-between gap-3 mt-6 mb-3"><h3 className="font-medium">Observations by site</h3><div className="flex flex-wrap gap-2">
        <input className={inputClass} type="search" placeholder="Find a stop or signal" aria-label="Find a streetcar site" value={siteSearch} onChange={event => { setSiteSearch(event.target.value); setShowAllSites(false); }} />
        <select className={inputClass} aria-label="Sort streetcar sites" value={siteSort} onChange={event => setSiteSort(event.target.value)}><option value="samples">Most completed passages</option><option value="time">Longest estimated passage</option><option value="name">Site name</option></select>
      </div></div>
      <p className="text-xs text-slate-400 mb-3">Select a site to see its separate stop/signal contexts. Sites can share completed passages; site counts must not be added together. Small samples can be unrepresentative.</p>
      <div className="overflow-x-auto"><table className="w-full text-left text-sm"><thead><tr className="border-b border-slate-600">
        {['Site', 'Type / window context', 'Mean travel-time bounds', 'Estimated speed', 'Passages'].map(label => <th key={label} className="p-2 font-medium text-slate-300">{label}</th>)}
      </tr></thead><tbody>{visibleSites.map(row => <tr key={row.site.id} className={`border-b border-slate-700 ${selectedSite === row.site.id ? 'bg-slate-700' : ''}`}>
        <td className="p-2"><button type="button" onClick={() => selectSite(row.site.id)} className="text-blue-300 underline text-left">{row.site.name}</button></td>
        <td className="p-2"><span>{siteType(row.site)}</span><div className="text-xs flex flex-wrap gap-x-2">{row.contexts.map(category => <span key={category.id} style={{ color: category.color }}>{category.label}</span>)}</div></td>
        <td className="p-2 whitespace-nowrap">{timeRange(row.lowerMean, row.upperMean)}</td><td className="p-2 whitespace-nowrap">{speed(row.mph)}</td><td className="p-2">{number(row.intervals)}</td>
      </tr>)}</tbody></table></div>
      {matchingSites.length === 0 && <p className="text-sm text-slate-400 py-4">No mapped sites match this search.</p>}
      {matchingSites.length > 15 && <button className="text-sm text-blue-300 underline mt-3" type="button" onClick={() => setShowAllSites(value => !value)}>{showAllSites ? 'Show first 15 sites' : `Show all ${matchingSites.length} sites`}</button>}
      <details className="mt-6 text-sm text-slate-300"><summary className="cursor-pointer font-medium">Data quality, sources, and measurement limits</summary>
        <p className="mt-3">{number(summary.intervals)} completed passages from {number(summary.vehicles.size)} vehicles in the selected direction and time window. {number(quality.reduce((total, row) => total + row.raw_points, 0))} raw points and {number(quality.reduce((total, row) => total + row.candidate_intervals, 0))} candidate GPS intervals were reviewed across the selected corridor and dates; {number(quality.reduce((total, row) => total + row.accepted_intervals, 0))} GPS intervals were accepted before assembling passages. GPS intervals and completed passages are different counts. Exclusions below cover whole dates, before the direction, day-type, and hour filters.</p>
        {excluded.length > 0 && <ul className="list-disc pl-5 mt-2">{excluded.map(([reason, count]) => <li key={reason}>{reason.replaceAll('_', ' ')}: {number(count)}</li>)}</ul>}
        <p className="mt-3">{timestampPrecision ? `Provider timestamps have ${timestampPrecision === 60 ? 'minute precision' : `${timestampPrecision}-second precision`} even when relay polls are more frequent. ` : ''}Vehicle positions and timestamps cannot identify the exact red-light wait or distinguish all passenger dwell from traffic delay. No historical signal-phase data is available. Track windows{windowMeters ? ` of ${windowMeters} metres` : ''} are fixed and do not overlap along a path. A window is classified by nearby mapped features before measuring speed, so fast movement cannot change its category. Features within {data.method.feature_radius_meters} metres are considered; GPS intervals over {data.method.max_gap_seconds} seconds apart and points too far from the track are excluded. Only complete entry-to-exit passages count, and each belongs to exactly one of the four categories.</p>
        <p className="mt-3">Timing ranges use the earliest and latest entry and exit allowed by the surrounding reports.{timestampPrecision ? ` The ranges allow an additional ${timestampPrecision} seconds per passage on either side for timestamp truncation, with durations never below zero.` : ''} Main cards average these lower and upper durations per completed passage. The ranges cover sampling{timestampPrecision ? ' and timestamp precision' : ''}, but do not bound GPS position error, track-assignment error, or delay caused by a signal. They are not confidence intervals. When a whole window fits between two reports, the lower duration may be zero and no finite upper speed can be inferred. Incomplete passages, including vehicles still waiting without an observed exit, are excluded; completion filtering can underrepresent long waits.</p>
        {data.method.limitations.length > 0 && <ul className="list-disc pl-5 mt-2">{data.method.limitations.map(limitation => <li key={limitation}>{limitation}</li>)}</ul>}
        <p className="mt-3"><strong>Mapillary check:</strong> {data.network.mapillary_status}</p>
        <p className="mt-2">Mapillary detection corroboration means a nearby vehicle traffic-light detection supports the mapped location. Detection corroboration alone is not a human imagery review or proof that the signal controls a streetcar movement. Sites with an actual imagery review offer a link to the reviewed image.</p>
        <p className="mt-2">Rail-tagged signals remain visible for inspection, but nearby passages are excluded from headline comparisons until their function is verified. These signals may control streetcar traffic or track switches.</p>
        <p className="mt-2">Signal locations come from the current map. Historical streetcar observations do not prove a mapped signal existed or was operating in the same way on every past date.</p>
        <ul className="list-disc pl-5 mt-3">{data.network.sources.map(source => <li key={source.url}><a href={source.url} className="text-blue-300 underline">{source.name}</a> — {source.attribution}. Retrieved {source.fetched_at}.</li>)}</ul>
      </details>
    </>}
  </section>;
}
