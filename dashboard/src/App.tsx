import OtpPanel from './OtpPanel';
import PriorityPanel from './PriorityPanel';
import StudyPanel from './StudyPanel';
import SourceQuality from './SourceQuality';
import StoryPage from './StoryPage';
import { useEffect, useState } from 'react';
import type { OtpData } from './otp-data';

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
  { id: 'overview', label: 'The story' },
  { id: 'row', label: 'Roadway time' },
  { id: 'signals', label: 'Traffic lights' },
] as const;
type ViewId = typeof views[number]['id'] | 'otp';
function viewFromHash(): ViewId {
  const hash = typeof window === 'undefined' ? '' : window.location.hash.slice(1);
  if (hash === 'signal-priority' || hash === 'priority') return 'signals';
  if (hash === 'otp') return 'otp';
  return views.find(view => view.id === hash)?.id ?? 'overview';
}

function App({ initialView }: { initialView?: ViewId }) {
  const [view, setView] = useState<ViewId>(initialView ?? viewFromHash);
  useEffect(() => {
    const handleHashChange = () => { if (window.location.hash !== '#dashboard-content') setView(viewFromHash()); };
    window.addEventListener('hashchange', handleHashChange);
    return () => window.removeEventListener('hashchange', handleHashChange);
  }, []);
  useEffect(() => { window.scrollTo(0, 0); }, [view]);
  return <div className="dashboard-shell min-h-screen px-4 pb-6 pt-5 sm:px-6 sm:pb-8 sm:pt-8">
    <a href="#dashboard-content" className="skip-link">Skip to content</a>
    <div className="max-w-6xl mx-auto">
      <header className="flex flex-wrap items-center justify-between gap-x-6 gap-y-3 mb-6 sm:mb-8">
        <a href="#overview" onClick={() => setView('overview')} className="brand-link flex items-center gap-3" aria-label="NOLA transit performance — the story">
          <span aria-hidden="true" className="brand-mark">N</span>
          <div><h1 className="text-base font-semibold tracking-tight">NOLA transit performance</h1><p className="mt-0.5 text-xs text-slate-400">An independent study of faster transit</p></div>
        </a>
        <a href="#otp" aria-current={view === 'otp' ? 'page' : undefined} onClick={() => setView('otp')} className={`text-sm underline-offset-4 hover:underline ${view === 'otp' ? 'text-amber-300' : 'text-slate-300'}`}>On-time performance <span aria-hidden="true">↗</span></a>
      </header>
      <nav aria-label="Dashboard views" className="story-nav sticky top-0 z-20 mb-8 sm:mb-10">
        <div className="flex gap-1 sm:gap-2">
          {views.map(item => <a key={item.id} href={`#${item.id}`} aria-current={view === item.id ? 'page' : undefined}
            className={`min-w-0 flex-1 rounded-lg px-2 py-3 text-center text-sm font-medium transition-colors sm:flex-none sm:px-6 ${view === item.id ? 'bg-slate-100 text-slate-950' : 'text-slate-300 hover:bg-slate-700/60 hover:text-white'}`}
            onClick={() => setView(item.id)}>{item.label}</a>)}
        </div>
      </nav>
      <main id="dashboard-content" tabIndex={-1} aria-label={view === 'otp' ? 'On-time performance' : views.find(item => item.id === view)?.label}>
        {view === 'overview' && <StoryPage />}
        {view === 'row' && <StudyPanel key="row" kind="row" />}
        {view === 'signals' && <SignalsView />}
        {view === 'otp' && <OtpView />}
      </main>
      <div className="mt-10 sm:mt-14"><SourceQuality /></div>
      <footer className="border-t border-slate-700/70 mt-6 pt-5 text-xs text-slate-400 flex flex-wrap gap-x-6 gap-y-3 justify-between">
        <p>Independent observations of New Orleans transit. Missing observations do not mean no delay.</p>
        <a href="https://github.com/adamdavies1915/transport-experiments" className="text-slate-300 hover:underline">Source and methods <span aria-hidden="true">↗</span></a>
      </footer>
    </div>
  </div>;
}

export default App;
