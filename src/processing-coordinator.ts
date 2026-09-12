import { createHash, randomUUID, timingSafeEqual } from 'node:crypto';
import { mkdir, open, readFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { isDeepStrictEqual } from 'node:util';
import { Temporal } from '@js-temporal/polyfill';
import { atomicFile } from './local-journal';
import { validateCaptureManifest, type CaptureManifest } from './capture-exchange';
import { parseSummary } from '../dashboard/src/summary-validation';

export const PROCESSING_SUMMARY_MAX_BYTES = 60 * 1024 * 1024;
const TIME_ZONE = 'America/Chicago';
const queues = new Map<string, Promise<void>>();
const hash = (body: string | Uint8Array) => createHash('sha256').update(body).digest('hex');
const iso = (at: number) => new Date(at).toISOString();
const clone = <T>(value: T): T => structuredClone(value);
const validId = (value: string) => typeof value === 'string' && value.trim().length > 0 && value.length <= 200 && !/[\x00-\x1f\x7f]/.test(value);
const validDate = (value: string) => {
  try { return /^\d{4}-\d{2}-\d{2}$/.test(value) && Temporal.PlainDate.from(value).toString() === value; } catch { return false; }
};
const sameToken = (a: string, b: string) => typeof b === 'string' && Buffer.byteLength(a) === Buffer.byteLength(b) && timingSafeEqual(Buffer.from(a), Buffer.from(b));

export interface ProcessingCoordinatorOptions {
  data_dir: string;
  baseline_id: string;
  analysis_revision: string;
  first_service_date?: string;
  due_hour?: number;
  due_minute?: number;
  lease_seconds?: number;
  /** Epoch milliseconds; injected to test the Chicago schedule and expiration. */
  now?: () => number;
  /** Atomic durable writer injection for crash-recovery tests. */
  write_atomic?: typeof atomicFile;
}
export interface ProcessingLeaseReference { job_id: string; lease_token: string; fence: number }
export interface ProcessingLease extends ProcessingLeaseReference {
  service_date: string;
  covers_from: string;
  baseline_id: string;
  analysis_revision: string;
  manifest: CaptureManifest;
  manifest_sha256: string;
  input_cutoff: string;
  worker_id: string;
  lease_seconds: number;
  expires_at: string;
}
export interface ProcessingCompletionRequest extends ProcessingLeaseReference {
  manifest_sha256: string;
  summary: string | Uint8Array;
}
export interface ProcessingCompletion {
  job_id: string;
  service_date: string;
  completed_at: string;
  summary_sha256: string;
  replayed: boolean;
}
interface JobInput {
  job_id: string; service_date: string; covers_from: string;
  baseline_id: string; analysis_revision: string;
  manifest: CaptureManifest; manifest_sha256: string; input_cutoff: string;
}
interface Lease { worker_id: string; token: string; fence: number; expires_at: string }
interface Publication {
  completed_at: string; request_sha256: string; summary_sha256: string;
  filename: string; worker_id: string; token: string; fence: number;
}
interface StoredJob {
  job_id: string; service_date: string; analysis_revision: string;
  status: 'pending' | 'leased' | 'publishing' | 'completed' | 'superseded';
  lease?: Lease; publication?: Publication;
}
interface CoordinatorState {
  schema_version: 1; baseline_id: string; analysis_revision: string;
  first_service_date: string; capture_id: string | null; fence: number;
  jobs: StoredJob[];
}

/** Return the latest closed day whose next-day Chicago processing time is due.
 * Calendar arithmetic, rather than subtracting 24 hours, handles DST changes. */
export function latestProcessingDate(now: number, hour = 6, minute = 0): string {
  const local = Temporal.Instant.fromEpochMilliseconds(now).toZonedDateTimeISO(TIME_ZONE);
  const due = local.hour * 60 + local.minute >= hour * 60 + minute;
  return local.toPlainDate().subtract({ days: due ? 1 : 2 }).toString();
}

/** Reject a cumulative generation that drops or rewrites accepted evidence.
 * Capture validation additionally requires contiguous sequences from one. */
export function assertCumulativeManifest(previous: CaptureManifest, next: CaptureManifest): void {
  if (previous.capture_id !== next.capture_id || next.latest_sequence < previous.latest_sequence)
    throw new Error('Input manifest regresses the accepted capture history');
  for (let i = 0; i < previous.bundles.length; i++) {
    if (!isDeepStrictEqual(previous.bundles[i], next.bundles[i]))
      throw new Error('Input manifest drops or changes an accepted bundle');
  }
  const schedules = new Map(next.schedules.map(s => [s.id, s]));
  for (const schedule of previous.schedules) {
    if (!isDeepStrictEqual(schedule, schedules.get(schedule.id)))
      throw new Error('Input manifest drops or changes an accepted schedule');
  }
}

/** One server process owns DATA_DIR. All coordinator instances in that process
 * serialize state changes; desktop/Mac workers interact only through its API.
 * Do not run overlapping coordinator server processes on the same directory.
 * Raw capture bundles and immutable job inputs are never deleted here. */
export class ProcessingCoordinator {
  private readonly directory: string;
  private readonly statePath: string;
  private readonly summaryPath: string;
  private readonly clock: () => number;
  private readonly write: typeof atomicFile;
  private readonly hour: number;
  private readonly minute: number;
  private readonly leaseSeconds: number;
  constructor(private readonly options: ProcessingCoordinatorOptions) {
    if (!validId(options.baseline_id) || !validId(options.analysis_revision)) throw new Error('Processing baseline and analysis revision are required');
    if (options.first_service_date !== undefined && !validDate(options.first_service_date)) throw new Error('Invalid first processing date');
    this.hour = options.due_hour ?? 6; this.minute = options.due_minute ?? 0; this.leaseSeconds = options.lease_seconds ?? 900;
    if (!Number.isInteger(this.hour) || this.hour < 0 || this.hour > 23 || !Number.isInteger(this.minute) || this.minute < 0 || this.minute > 59 ||
      !Number.isSafeInteger(this.leaseSeconds) || this.leaseSeconds <= 0) throw new Error('Invalid processing schedule or lease duration');
    this.directory = resolve(options.data_dir, 'processing');
    this.statePath = join(this.directory, 'coordinator.json'); this.summaryPath = resolve(options.data_dir, 'summary.json');
    this.clock = options.now ?? Date.now; this.write = options.write_atomic ?? atomicFile;
  }
  private now() {
    const now = this.clock();
    if (!Number.isSafeInteger(now) || !Number.isFinite(now)) throw new Error('Invalid coordinator clock');
    return now;
  }
  private exclusive<T>(operation: () => Promise<T>): Promise<T> {
    const previous = queues.get(this.directory) ?? Promise.resolve();
    const result = previous.then(operation), settled = result.then(() => {}, () => {});
    queues.set(this.directory, settled);
    void settled.then(() => { if (queues.get(this.directory) === settled) queues.delete(this.directory); });
    return result;
  }
  private async save(state: CoordinatorState) { await this.write(this.statePath, JSON.stringify(state)); }
  private jobPath(id: string) {
    if (!/^[a-f0-9-]{36}$/.test(id)) throw new Error('Invalid stored processing job identifier');
    return join(this.directory, 'jobs', `${id}.json`);
  }
  private async input(job: StoredJob): Promise<JobInput> {
    const input = JSON.parse(await readFile(this.jobPath(job.job_id), 'utf8')) as JobInput;
    const manifest = validateCaptureManifest(input.manifest);
    if (input.job_id !== job.job_id || input.service_date !== job.service_date || input.analysis_revision !== job.analysis_revision ||
      input.baseline_id !== this.options.baseline_id || input.manifest_sha256 !== manifest.manifest_sha256)
      throw new Error('Stored processing input does not match its immutable job');
    return { ...input, manifest };
  }
  private async load(): Promise<CoordinatorState> {
    await mkdir(join(this.directory, 'jobs'), { recursive: true, mode: 0o700 });
    await mkdir(join(this.directory, 'publications'), { recursive: true, mode: 0o700 });
    let state: CoordinatorState;
    try { state = JSON.parse(await readFile(this.statePath, 'utf8')) as CoordinatorState; }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
      // Persist the processing directory entry as well as the state file.
      const parent = await open(resolve(this.options.data_dir), 'r');
      try { await parent.sync(); } finally { await parent.close(); }
      state = { schema_version: 1, baseline_id: this.options.baseline_id, analysis_revision: this.options.analysis_revision,
        first_service_date: this.options.first_service_date ?? latestProcessingDate(this.now(), this.hour, this.minute),
        capture_id: null, fence: 0, jobs: [] };
      await this.save(state);
    }
    if (state.schema_version !== 1 || state.baseline_id !== this.options.baseline_id || !validDate(state.first_service_date) ||
      !Number.isSafeInteger(state.fence) || state.fence < 0 || !Array.isArray(state.jobs)) throw new Error('Invalid or different processing baseline state');
    // A publishing intent was accepted while its lease was valid. Recover that
    // committed decision before issuing any new lease, including after a crash.
    for (const job of state.jobs.filter(j => j.status === 'publishing')) await this.finishPublication(state, job);
    if (state.analysis_revision !== this.options.analysis_revision) {
      for (const job of state.jobs) if (job.status === 'pending' || job.status === 'leased') { job.status = 'superseded'; delete job.lease; }
      state.analysis_revision = this.options.analysis_revision; await this.save(state);
    }
    return state;
  }
  private active(state: CoordinatorState, reference: ProcessingLeaseReference): StoredJob {
    const job = state.jobs.find(j => j.job_id === reference.job_id), lease = job?.lease;
    if (!job || job.status !== 'leased' || !lease || lease.fence !== reference.fence || !sameToken(lease.token, reference.lease_token) ||
      Date.parse(lease.expires_at) <= this.now() || job.analysis_revision !== this.options.analysis_revision)
      throw new Error('Processing lease is expired, fenced or invalid');
    return job;
  }
  private async leaseResult(job: StoredJob): Promise<ProcessingLease> {
    const input = await this.input(job), lease = job.lease!;
    return clone({ ...input, worker_id: lease.worker_id, lease_token: lease.token, fence: lease.fence,
      lease_seconds: this.leaseSeconds, expires_at: lease.expires_at });
  }
  async claim(workerId: string, baselineId: string, revision: string, manifest: CaptureManifest): Promise<ProcessingLease | null> {
    if (!validId(workerId)) throw new Error('Invalid processing worker identifier');
    if (baselineId !== this.options.baseline_id || revision !== this.options.analysis_revision)
      throw new Error('Worker baseline or analysis revision does not match the server');
    return this.exclusive(async () => {
      const state = await this.load();
      let job = state.jobs.find(j => j.status === 'pending' || j.status === 'leased');
      if (job?.status === 'leased' && Date.parse(job.lease!.expires_at) > this.now()) return null;
      if (!job) {
        const serviceDate = latestProcessingDate(this.now(), this.hour, this.minute);
        if (serviceDate < state.first_service_date || state.jobs.some(j => j.status === 'completed' && j.service_date >= serviceDate && j.analysis_revision === this.options.analysis_revision)) return null;
        const pinned = validateCaptureManifest(clone(manifest));
        if (state.capture_id && state.capture_id !== pinned.capture_id) throw new Error('Capture identity differs from the accepted processing history');
        // Even an unfinished job pins immutable capture history. Changing the
        // analysis revision cannot authorize dropping its retained raw bundles.
        const previous = state.jobs.at(-1);
        if (previous) assertCumulativeManifest((await this.input(previous)).manifest, pinned);
        const previousDate = [...state.jobs].reverse().find(j => j.status === 'completed' && j.analysis_revision === this.options.analysis_revision)?.service_date;
        const jobId = randomUUID();
        const input: JobInput = { job_id: jobId, service_date: serviceDate,
          covers_from: previousDate ? Temporal.PlainDate.from(previousDate).add({ days: 1 }).toString() : state.first_service_date,
          baseline_id: this.options.baseline_id, analysis_revision: this.options.analysis_revision, manifest: pinned,
          manifest_sha256: pinned.manifest_sha256, input_cutoff: iso(this.now()) };
        await this.write(this.jobPath(jobId), JSON.stringify(input));
        job = { job_id: jobId, service_date: serviceDate, analysis_revision: this.options.analysis_revision, status: 'pending' };
        state.jobs.push(job); state.capture_id = pinned.capture_id;
      }
      if (state.fence >= Number.MAX_SAFE_INTEGER) throw new Error('Processing fence exhausted');
      state.fence++;
      job.status = 'leased'; job.lease = { worker_id: workerId, token: randomUUID(), fence: state.fence,
        expires_at: iso(this.now() + this.leaseSeconds * 1000) };
      await this.save(state);
      return this.leaseResult(job);
    });
  }
  async renew(reference: ProcessingLeaseReference): Promise<ProcessingLease> {
    return this.exclusive(async () => {
      const state = await this.load(), job = this.active(state, reference);
      job.lease!.expires_at = iso(this.now() + this.leaseSeconds * 1000); await this.save(state);
      return this.leaseResult(job);
    });
  }
  async release(reference: ProcessingLeaseReference): Promise<void> {
    return this.exclusive(async () => {
      const state = await this.load(), job = this.active(state, reference);
      job.status = 'pending'; delete job.lease; await this.save(state);
    });
  }
  private result(job: StoredJob, replayed: boolean): ProcessingCompletion {
    return { job_id: job.job_id, service_date: job.service_date, completed_at: job.publication!.completed_at,
      summary_sha256: job.publication!.summary_sha256, replayed };
  }
  private async finishPublication(state: CoordinatorState, job: StoredJob): Promise<void> {
    const publication = job.publication;
    if (!publication || publication.filename !== `${job.job_id}-${publication.fence}.json`) throw new Error('Invalid processing publication intent');
    const body = await readFile(join(this.directory, 'publications', publication.filename));
    if (body.length > PROCESSING_SUMMARY_MAX_BYTES || hash(body) !== publication.summary_sha256) throw new Error('Committed processing publication is missing or changed');
    // Atomic replacement preserves the prior server summary on any write error.
    await this.write(this.summaryPath, body);
    job.status = 'completed'; delete job.lease; await this.save(state);
  }
  async complete(request: ProcessingCompletionRequest): Promise<ProcessingCompletion> {
    const raw = typeof request.summary === 'string' ? Buffer.from(request.summary) : Buffer.from(request.summary);
    if (raw.length > PROCESSING_SUMMARY_MAX_BYTES) throw new Error('Processing summary exceeds 60 MiB');
    const requestHash = hash(raw);
    return this.exclusive(async () => {
      const state = await this.load();
      const existing = state.jobs.find(j => j.job_id === request.job_id);
      if (existing?.status === 'completed') {
        const publication = existing.publication!, input = await this.input(existing);
        if (publication.fence !== request.fence || !sameToken(publication.token, request.lease_token) ||
          publication.request_sha256 !== requestHash || input.manifest_sha256 !== request.manifest_sha256)
          throw new Error('Completed processing job cannot be changed or replayed by another owner');
        return this.result(existing, true);
      }
      const job = this.active(state, request), input = await this.input(job);
      if (request.manifest_sha256 !== input.manifest_sha256) throw new Error('Processing completion does not match its fixed input manifest');
      const summary = parseSummary(JSON.parse(raw.toString('utf8')));
      if (!summary.row_study || !summary.signal_study || !summary.legacy) throw new Error('Processing completion must include both studies and historical summary');
      const generated = Date.parse(summary.generated_at), now = this.now();
      if (generated > now + 60_000) throw new Error('Processing summary generation time is in the future');
      const previous = [...state.jobs].reverse().find(j => j.status === 'completed');
      if (previous) {
        assertCumulativeManifest((await this.input(previous)).manifest, input.manifest);
        const previousBody = JSON.parse(await readFile(join(this.directory, 'publications', previous.publication!.filename), 'utf8')) as { generated_at: string };
        if (generated < Date.parse(previousBody.generated_at)) throw new Error('Processing summary generation time regresses the published history');
      }
      const completedAt = iso(now), lease = job.lease!;
      summary.processing = { mode: 'daily', job_id: job.job_id, service_date: job.service_date, input_cutoff: input.input_cutoff,
        completed_at: completedAt, worker_id: lease.worker_id, analysis_revision: job.analysis_revision, manifest_sha256: input.manifest_sha256 };
      const body = JSON.stringify(summary);
      if (Buffer.byteLength(body) > PROCESSING_SUMMARY_MAX_BYTES) throw new Error('Processing summary plus metadata exceeds 60 MiB');
      const filename = `${job.job_id}-${lease.fence}.json`;
      await this.write(join(this.directory, 'publications', filename), body);
      // Serialization/durable staging can take time. Recheck expiration directly
      // before accepting the publication intent; no stale owner can commit it.
      this.active(state, request);
      job.publication = { completed_at: completedAt, request_sha256: requestHash, summary_sha256: hash(body), filename,
        worker_id: lease.worker_id, token: lease.token, fence: lease.fence };
      job.status = 'publishing'; await this.save(state);
      await this.finishPublication(state, job);
      return this.result(job, false);
    });
  }
  async status() {
    return this.exclusive(async () => {
      const state = await this.load(), active = state.jobs.find(j => j.status === 'pending' || j.status === 'leased');
      const completed = [...state.jobs].reverse().find(j => j.status === 'completed');
      return { schema_version: 1 as const, baseline_id: this.options.baseline_id, analysis_revision: this.options.analysis_revision,
        first_service_date: state.first_service_date, time_zone: TIME_ZONE, due_hour: this.hour, due_minute: this.minute,
        lease_seconds: this.leaseSeconds, due_service_date: latestProcessingDate(this.now(), this.hour, this.minute),
        active: active ? { job_id: active.job_id, service_date: active.service_date, status: active.status,
          worker_id: active.lease?.worker_id ?? null, fence: active.lease?.fence ?? null, expires_at: active.lease?.expires_at ?? null } : null,
        last_completed: completed ? this.result(completed, false) : null };
    });
  }
}
