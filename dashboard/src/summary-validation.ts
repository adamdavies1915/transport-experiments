import type { SummaryProcessing, TransitSummaryEnvelope } from './summary-data';

const object = (value: unknown): value is Record<string, unknown> => value != null && typeof value === 'object' && !Array.isArray(value);
function parseProcessing(value: unknown): SummaryProcessing {
  if (!object(value) || value.mode !== 'daily') throw new Error('Invalid summary processing metadata');
  const identifier = (key: string) => typeof value[key] === 'string' && value[key].trim().length > 0 && value[key].length <= 200 && !Array.from(value[key]).some(character => character.charCodeAt(0) < 32 || character.charCodeAt(0) === 127);
  const timestamp = (key: string) => typeof value[key] === 'string' && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$/.test(value[key]) && Number.isFinite(Date.parse(value[key])) && new Date(value[key]).toISOString().slice(0, 19) === value[key].slice(0, 19);
  if (!['job_id', 'worker_id', 'analysis_revision'].every(identifier) || !['input_cutoff', 'completed_at'].every(timestamp) ||
      typeof value.service_date !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value.service_date) || !Number.isFinite(Date.parse(value.service_date)) || new Date(value.service_date).toISOString().slice(0, 10) !== value.service_date ||
      typeof value.manifest_sha256 !== 'string' || !/^[a-fA-F0-9]{64}$/.test(value.manifest_sha256)) throw new Error('Invalid summary processing metadata');
  // Coordinator credentials, paths and additional private provenance are never forwarded.
  return { mode: 'daily', job_id: value.job_id as string, service_date: value.service_date, input_cutoff: value.input_cutoff as string,
    completed_at: value.completed_at as string, worker_id: value.worker_id as string, analysis_revision: value.analysis_revision as string, manifest_sha256: value.manifest_sha256 };
}
/** Accept only the public envelope; tokens and collector configuration never belong here. */
export function parseSummary(value: unknown): TransitSummaryEnvelope {
  if (!object(value) || value.schema_version !== 1 || typeof value.generated_at !== 'string' || !Number.isFinite(Date.parse(value.generated_at))) throw new Error('Invalid summary envelope');
  for (const key of ['source_quality', 'row_study', 'signal_study', 'legacy']) if (value[key] != null && !object(value[key])) throw new Error('Invalid summary section');
  for (const key of ['row_study', 'signal_study']) {
    const study = value[key];
    if (object(study) && (!Array.isArray(study.cells) || !object(study.network) || !Array.isArray(study.network.paths) || !Array.isArray(study.network.sites) || !Array.isArray(study.network.row_sections) || !Array.isArray(study.network.limitations) || !Array.isArray(study.limitations) || !Array.isArray(study.quality))) throw new Error('Invalid study section');
    if (object(study) && study.coverage_cells != null && !Array.isArray(study.coverage_cells)) throw new Error('Invalid study coverage');
  }
  if (object(value.source_quality) && !Array.isArray(value.source_quality.sources)) throw new Error('Invalid source quality');
  return { schema_version: 1, generated_at: value.generated_at,
    ...(value.processing === undefined ? {} : { processing: parseProcessing(value.processing) }),
    source_quality: value.source_quality as TransitSummaryEnvelope['source_quality'],
    row_study: value.row_study as TransitSummaryEnvelope['row_study'],
    signal_study: value.signal_study as TransitSummaryEnvelope['signal_study'],
    legacy: value.legacy as TransitSummaryEnvelope['legacy'] };
}
