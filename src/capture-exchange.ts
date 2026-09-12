import { createHash, randomUUID } from 'node:crypto';
import { constants } from 'node:fs';
import { link, mkdir, open, readdir, realpath, unlink } from 'node:fs/promises';
import { basename, dirname, join, resolve } from 'node:path';
import { gzipSync, gunzipSync } from 'node:zlib';
import type { LocalJournal } from './local-journal';
import type { CollectionBatch } from './observation-types';

export const CAPTURE_LIMITS = {
  frames: 1000, frame_bytes: 8 * 1024 * 1024, frame_json_bytes: 8 * 1024 * 1024,
  bundle_bytes: 64 * 1024 * 1024, bundle_json_bytes: 64 * 1024 * 1024,
  decoded_frames_bytes: 64 * 1024 * 1024,
  schedule_bytes: 32 * 1024 * 1024,
} as const;
export interface CaptureFrame { name: string; sha256: string; bytes: number; data_base64: string }
export interface CaptureBundle {
  schema_version: 1; capture_id: string; id: string; sequence: number; frames: CaptureFrame[];
}
export interface CaptureBundleDescriptor {
  id: string; sequence: number; sha256: string; bytes: number; uncompressed_bytes: number;
  frames: number; first_received_at: string; last_received_at: string; sealed_at: string;
}
export interface CaptureScheduleDescriptor { id: string; sha256: string; bytes: number; source: string; first_seen_at: string }
export interface CaptureManifest {
  schema_version: 1; capture_id: string; latest_sequence: number; manifest_sha256: string;
  bundles: CaptureBundleDescriptor[]; schedules: CaptureScheduleDescriptor[];
  bundle_count: number; bundle_bytes: number; schedule_bytes: number;
}
interface Identity { schema_version: 1; capture_id: string; created_at: string }
interface SealIntent {
  schema_version: 1; capture_id: string; journal_directory: string;
  descriptor: CaptureBundleDescriptor; frames: Omit<CaptureFrame, 'data_base64'>[];
}
const HASH = /^[a-f0-9]{64}$/, ID = /^\d{12}$/;
const UUID = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/;
const digest = (data: string | Uint8Array) => createHash('sha256').update(data).digest('hex');
const sequenceId = (sequence: number) => String(sequence).padStart(12, '0');
const integer = (value: unknown, min: number, max = Number.MAX_SAFE_INTEGER): value is number => Number.isSafeInteger(value) && Number(value) >= min && Number(value) <= max;
const clock = (value: unknown): value is string => typeof value === 'string' && /^\d{4}-\d\d-\d\dT.*(?:Z|[+-]\d\d:\d\d)$/.test(value) && Number.isFinite(Date.parse(value));
const object = (value: unknown): value is Record<string, unknown> => !!value && typeof value === 'object' && !Array.isArray(value);
function canonical(value: unknown): string {
  if (Array.isArray(value)) return '[' + value.map(canonical).join(',') + ']';
  if (object(value)) return '{' + Object.keys(value).sort().map(key => JSON.stringify(key) + ':' + canonical(value[key])).join(',') + '}';
  return JSON.stringify(value);
}
export function captureManifestDigest(manifest: Omit<CaptureManifest, 'manifest_sha256'>): string { return digest(canonical(manifest)); }
function checkBundle(value: unknown): asserts value is CaptureBundleDescriptor {
  if (!object(value) || !integer(value.sequence, 1, 999_999_999_999) || value.id !== sequenceId(value.sequence) ||
    typeof value.sha256 !== 'string' || !HASH.test(value.sha256) || !integer(value.bytes, 1, CAPTURE_LIMITS.bundle_bytes) ||
    !integer(value.uncompressed_bytes, 1, CAPTURE_LIMITS.bundle_json_bytes) || !integer(value.frames, 1, CAPTURE_LIMITS.frames) ||
    !clock(value.first_received_at) || !clock(value.last_received_at) || !clock(value.sealed_at) ||
    Date.parse(value.first_received_at) > Date.parse(value.last_received_at)) throw new Error('Invalid capture bundle descriptor');
}
function checkSchedule(value: unknown): asserts value is CaptureScheduleDescriptor {
  if (!object(value) || typeof value.id !== 'string' || !HASH.test(value.id) || value.sha256 !== value.id ||
    !integer(value.bytes, 1, CAPTURE_LIMITS.schedule_bytes) || typeof value.source !== 'string' || !value.source.trim() ||
    value.source.length > 4096 || !clock(value.first_seen_at)) throw new Error('Invalid capture schedule descriptor');
}
export function validateCaptureManifest(value: unknown): CaptureManifest {
  if (!object(value) || value.schema_version !== 1 || typeof value.capture_id !== 'string' || !UUID.test(value.capture_id) ||
    !Array.isArray(value.bundles) || !Array.isArray(value.schedules)) throw new Error('Invalid capture manifest');
  value.bundles.forEach((bundle, index) => { checkBundle(bundle); if (bundle.sequence !== index + 1) throw new Error('Capture manifest has a sequence gap'); });
  value.schedules.forEach((schedule, index) => {
    checkSchedule(schedule);
    if (index && (value.schedules as CaptureScheduleDescriptor[])[index - 1].id >= schedule.id) throw new Error('Capture schedule IDs must be unique and sorted');
  });
  const bundles = value.bundles as CaptureBundleDescriptor[], schedules = value.schedules as CaptureScheduleDescriptor[];
  if (value.latest_sequence !== bundles.length || value.bundle_count !== bundles.length ||
    value.bundle_bytes !== bundles.reduce((sum, b) => sum + b.bytes, 0) || value.schedule_bytes !== schedules.reduce((sum, s) => sum + s.bytes, 0))
    throw new Error('Capture manifest totals do not match its descriptors');
  const { manifest_sha256, ...body } = value;
  if (manifest_sha256 !== captureManifestDigest(body as unknown as Omit<CaptureManifest, 'manifest_sha256'>)) throw new Error('Capture manifest checksum does not match');
  return value as unknown as CaptureManifest;
}
function frameName(value: unknown): asserts value is string {
  if (typeof value !== 'string' || value.length > 255 || !/^[A-Za-z0-9][A-Za-z0-9_.:-]*\.json\.gz$/.test(value) || basename(value) !== value || value.includes('..'))
    throw new Error('Invalid capture journal frame name');
}
async function readBounded(path: string, limit: number): Promise<Buffer> {
  const file = await open(path, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
  try {
    const before = await file.stat();
    if (!before.isFile() || before.size > limit) throw new Error('Capture file is not regular or exceeds its size limit');
    const bytes = Buffer.alloc(before.size + 1);
    let total = 0;
    while (total < bytes.length) {
      const { bytesRead } = await file.read(bytes, total, bytes.length - total, total);
      if (!bytesRead) break;
      total += bytesRead;
    }
    const after = await file.stat();
    if (total !== before.size || after.size !== before.size || after.mtimeMs !== before.mtimeMs) throw new Error('Capture file changed while being read');
    return bytes.subarray(0, total);
  } finally { await file.close(); }
}
async function optional(path: string, limit: number): Promise<Buffer | null> {
  try { return await readBounded(path, limit); } catch (error) { if ((error as NodeJS.ErrnoException).code === 'ENOENT') return null; throw error; }
}
async function syncDirectory(path: string): Promise<void> {
  const directory = await open(path, 'r');
  try { await directory.sync(); } finally { await directory.close(); }
}
/** Atomic no-overwrite commit: identical retries are accepted, conflicts fail. */
async function immutable(path: string, bytes: Uint8Array): Promise<void> {
  const previous = await optional(path, bytes.length);
  if (previous) {
    if (!previous.equals(bytes)) throw new Error('Conflicting immutable capture asset');
    // A process may have died between linking the file and syncing its directory.
    await syncDirectory(dirname(path)); return;
  }
  const temporary = path + '.' + randomUUID() + '.tmp';
  const file = await open(temporary, 'wx', 0o600);
  try { await file.writeFile(bytes); await file.sync(); } finally { await file.close(); }
  try {
    try { await link(temporary, path); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'EEXIST' || !(await readBounded(path, bytes.length)).equals(bytes)) throw error;
    }
    await syncDirectory(dirname(path));
  } finally { await unlink(temporary).catch(() => {}); }
}
const encode = (value: unknown) => Buffer.from(JSON.stringify(value) + '\n');
async function remove(path: string): Promise<void> {
  await unlink(path).catch(error => { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; });
  await syncDirectory(dirname(path));
}
function inspectFrame(bytes: Buffer): { batch: CollectionBatch; decoded_bytes: number } {
  if (!bytes.length || bytes.length > CAPTURE_LIMITS.frame_bytes) throw new Error('Capture journal frame exceeds its size limit');
  const decoded = gunzipSync(bytes, { maxOutputLength: CAPTURE_LIMITS.frame_json_bytes });
  const value: unknown = JSON.parse(decoded.toString('utf8'));
  if (!object(value) || value.schema_version !== 1 || typeof value.batch_id !== 'string' || !value.batch_id ||
    !['sse', 'lepass'].includes(String(value.source)) || !clock(value.received_at) || !Array.isArray(value.observations))
    throw new Error('Corrupt capture journal frame');
  return { batch: value as unknown as CollectionBatch, decoded_bytes: decoded.length };
}

function verifyCaptureBundle(bytes: Uint8Array, descriptor: CaptureBundleDescriptor, captureId: string): { bundle: CaptureBundle; batches: CollectionBatch[] } {
  checkBundle(descriptor);
  if (!UUID.test(captureId) || bytes.length > CAPTURE_LIMITS.bundle_bytes || bytes.length !== descriptor.bytes || digest(bytes) !== descriptor.sha256)
    throw new Error('Capture bundle checksum mismatch');
  const decoded = gunzipSync(bytes, { maxOutputLength: CAPTURE_LIMITS.bundle_json_bytes });
  if (decoded.length !== descriptor.uncompressed_bytes) throw new Error('Capture bundle decoded size mismatch');
  const bundle: CaptureBundle = JSON.parse(decoded.toString('utf8'));
  if (bundle.schema_version !== 1 || bundle.capture_id !== captureId || bundle.id !== descriptor.id || bundle.sequence !== descriptor.sequence ||
    !Array.isArray(bundle.frames) || bundle.frames.length !== descriptor.frames) throw new Error('Capture bundle identity mismatch');
  const clocks: string[] = [], names = new Set<string>(), batches: CollectionBatch[] = [];
  let totalDecoded = 0;
  for (const frame of bundle.frames) {
    frameName(frame.name);
    if (names.has(frame.name) || !integer(frame.bytes, 1, CAPTURE_LIMITS.frame_bytes) || !HASH.test(frame.sha256) ||
      typeof frame.data_base64 !== 'string' || frame.data_base64.length !== 4 * Math.ceil(frame.bytes / 3)) throw new Error('Capture frame manifest mismatch');
    names.add(frame.name);
    const raw = Buffer.from(frame.data_base64, 'base64');
    if (raw.length !== frame.bytes || raw.toString('base64') !== frame.data_base64 || digest(raw) !== frame.sha256) throw new Error('Capture frame checksum mismatch');
    const { batch, decoded_bytes } = inspectFrame(raw); totalDecoded += decoded_bytes;
    if (totalDecoded > CAPTURE_LIMITS.decoded_frames_bytes) throw new Error('Capture decoded frames exceed their aggregate size limit');
    batches.push(batch); clocks.push(batch.received_at);
  }
  clocks.sort((a, b) => Date.parse(a) - Date.parse(b));
  if (clocks[0] !== descriptor.first_received_at || clocks.at(-1) !== descriptor.last_received_at) throw new Error('Capture bundle receipt range mismatch');
  return { bundle, batches };
}
/** Validates the complete bundle and every nested frame before returning any
 * batches to an importer. All original provider/receipt clocks remain unchanged. */
export function decodeCaptureBundle(bytes: Uint8Array, descriptor: CaptureBundleDescriptor, captureId: string): CollectionBatch[] {
  return verifyCaptureBundle(bytes, descriptor, captureId).batches;
}

// One capture writer process per directory. Within it, HTTP/timer callers and
// multiple instances serialize through the same queue. Journal appends continue.
const queues = new Map<string, Promise<unknown>>();
export class CaptureExchange {
  readonly directory: string;
  private readonly bundles: string;
  private readonly schedules: string;
  private readonly pending: string;
  constructor(directory: string) {
    this.directory = resolve(directory, 'capture-exchange');
    this.bundles = join(this.directory, 'bundles'); this.schedules = join(this.directory, 'schedules');
    this.pending = join(this.directory, 'seal.pending.json');
  }
  private serial<T>(action: () => Promise<T>): Promise<T> {
    const next = (queues.get(this.directory) ?? Promise.resolve()).then(action, action);
    queues.set(this.directory, next.catch(() => {})); return next;
  }
  private async identity(): Promise<Identity> {
    await mkdir(this.bundles, { recursive: true, mode: 0o700 });
    await mkdir(this.schedules, { recursive: true, mode: 0o700 });
    // Persist the new directory entries, not just the files they later contain.
    await syncDirectory(this.directory); await syncDirectory(dirname(this.directory));
    const path = join(this.directory, 'identity.json'), saved = await optional(path, 4096);
    if (!saved) {
      if ((await readdir(this.bundles)).length || (await readdir(this.schedules)).length || await optional(this.pending, 1024 * 1024))
        throw new Error('Capture identity missing from an existing exchange');
      const identity: Identity = { schema_version: 1, capture_id: randomUUID(), created_at: new Date().toISOString() };
      await immutable(path, encode(identity)); return identity;
    }
    const identity: Identity = JSON.parse(saved.toString('utf8'));
    if (identity.schema_version !== 1 || !UUID.test(identity.capture_id) || !clock(identity.created_at)) throw new Error('Invalid capture identity');
    return identity;
  }
  private asset(id: string): string { if (!ID.test(id)) throw new Error('Invalid capture bundle ID'); return join(this.bundles, id + '.json.gz'); }
  private descriptor(id: string): string { this.asset(id); return join(this.bundles, id + '.manifest.json'); }
  private scheduleAsset(id: string): string { if (!HASH.test(id)) throw new Error('Invalid capture schedule ID'); return join(this.schedules, id + '.zip'); }
  private async intent(identity: Identity): Promise<SealIntent | null> {
    const bytes = await optional(this.pending, 1024 * 1024);
    if (!bytes) return null;
    const value: SealIntent = JSON.parse(bytes.toString('utf8')); checkBundle(value.descriptor);
    if (value.schema_version !== 1 || value.capture_id !== identity.capture_id || typeof value.journal_directory !== 'string' ||
      !Array.isArray(value.frames) || value.frames.length !== value.descriptor.frames) throw new Error('Invalid pending capture seal');
    const names = new Set<string>();
    for (const frame of value.frames) {
      frameName(frame.name);
      if (!integer(frame.bytes, 1, CAPTURE_LIMITS.frame_bytes) || !HASH.test(frame.sha256) || names.has(frame.name)) throw new Error('Invalid pending capture frame');
      names.add(frame.name);
    }
    return value;
  }
  private verifyBundle(bytes: Buffer, intent: SealIntent): CaptureBundle {
    const { bundle } = verifyCaptureBundle(bytes, intent.descriptor, intent.capture_id);
    for (const [index, frame] of bundle.frames.entries()) {
      const expected = intent.frames[index];
      if (frame.name !== expected.name || frame.bytes !== expected.bytes || frame.sha256 !== expected.sha256) throw new Error('Pending capture frame manifest mismatch');
    }
    return bundle;
  }
  private async commitExisting(intent: SealIntent): Promise<boolean> {
    const bytes = await optional(this.asset(intent.descriptor.id), CAPTURE_LIMITS.bundle_bytes);
    if (!bytes) {
      if (await optional(this.descriptor(intent.descriptor.id), 4096)) throw new Error('Committed capture bundle is missing');
      return false;
    }
    this.verifyBundle(bytes, intent);
    await immutable(this.descriptor(intent.descriptor.id), encode(intent.descriptor));
    return true;
  }
  private async recoverSchedules(): Promise<void> {
    for (const name of (await readdir(this.schedules)).filter(name => /^[a-f0-9]{64}\.pending\.json$/.test(name)).sort()) {
      const path = join(this.schedules, name), saved: unknown = JSON.parse((await readBounded(path, 8192)).toString('utf8'));
      checkSchedule(saved);
      if (name !== saved.id + '.pending.json') throw new Error('Capture schedule pending identity mismatch');
      const bytes = await optional(this.scheduleAsset(saved.id), CAPTURE_LIMITS.schedule_bytes);
      if (!bytes) continue;
      if (bytes.length !== saved.bytes || digest(bytes) !== saved.sha256) throw new Error('Capture schedule checksum mismatch');
      await immutable(join(this.schedules, saved.id + '.manifest.json'), encode(saved));
      await remove(path);
    }
  }
  private async initialize(): Promise<Identity> {
    const identity = await this.identity(), intent = await this.intent(identity);
    if (intent) await this.commitExisting(intent);
    await this.recoverSchedules(); return identity;
  }
  init(): Promise<void> { return this.serial(async () => { await this.initialize(); }); }
  private async listing(identity: Identity): Promise<CaptureManifest> {
    const bundles: CaptureBundleDescriptor[] = [], schedules: CaptureScheduleDescriptor[] = [];
    for (const name of (await readdir(this.bundles)).filter(name => /^\d{12}\.manifest\.json$/.test(name)).sort()) {
      const descriptor: unknown = JSON.parse((await readBounded(join(this.bundles, name), 4096)).toString('utf8'));
      checkBundle(descriptor); if (name !== descriptor.id + '.manifest.json') throw new Error('Capture bundle descriptor filename mismatch');
      bundles.push(descriptor);
    }
    for (const name of (await readdir(this.schedules)).filter(name => /^[a-f0-9]{64}\.manifest\.json$/.test(name)).sort()) {
      const descriptor: unknown = JSON.parse((await readBounded(join(this.schedules, name), 8192)).toString('utf8'));
      checkSchedule(descriptor); if (name !== descriptor.id + '.manifest.json') throw new Error('Capture schedule descriptor filename mismatch');
      schedules.push(descriptor);
    }
    const body = { schema_version: 1 as const, capture_id: identity.capture_id, latest_sequence: bundles.length, bundles, schedules,
      bundle_count: bundles.length, bundle_bytes: bundles.reduce((sum, b) => sum + b.bytes, 0), schedule_bytes: schedules.reduce((sum, s) => sum + s.bytes, 0) };
    return validateCaptureManifest({ ...body, manifest_sha256: captureManifestDigest(body) });
  }
  manifest(): Promise<CaptureManifest> { return this.serial(async () => this.listing(await this.initialize())); }
  private async cleanup(journal: LocalJournal, intent: SealIntent): Promise<void> {
    if (await realpath(journal.pending) !== intent.journal_directory) throw new Error('Pending capture seal belongs to a different journal');
    // Reverify the committed bundle and sidecar before deleting any loose frame.
    if (!await this.commitExisting(intent)) throw new Error('Capture bundle is not durably committed');
    for (const frame of intent.frames) {
      const path = join(journal.pending, frame.name), bytes = await optional(path, CAPTURE_LIMITS.frame_bytes);
      if (!bytes) continue;
      if (bytes.length !== frame.bytes || digest(bytes) !== frame.sha256) throw new Error('Journal frame changed after capture; it was retained');
      await journal.acknowledge(path);
    }
    await syncDirectory(journal.pending);
    await remove(this.pending);
  }
  seal(journal: LocalJournal, limit = 1000): Promise<CaptureBundleDescriptor | null> {
    return this.serial(async () => {
      if (!integer(limit, 1, CAPTURE_LIMITS.frames)) throw new Error('Capture seal limit must be between 1 and 1000');
      const identity = await this.initialize(), existing = await this.intent(identity), journalDirectory = await realpath(journal.pending);
      if (existing) {
        if (existing.journal_directory !== journalDirectory) throw new Error('Pending capture seal belongs to a different journal');
        if (!await this.commitExisting(existing)) {
          const frames: CaptureFrame[] = [];
          for (const frame of existing.frames) {
            const bytes = await readBounded(join(journal.pending, frame.name), CAPTURE_LIMITS.frame_bytes);
            if (bytes.length !== frame.bytes || digest(bytes) !== frame.sha256) throw new Error('Pending capture source frame changed');
            frames.push({ ...frame, data_base64: bytes.toString('base64') });
          }
          const bytes = gzipSync(JSON.stringify({ schema_version: 1, capture_id: identity.capture_id, id: existing.descriptor.id, sequence: existing.descriptor.sequence, frames }), { level: 6 });
          this.verifyBundle(bytes, existing);
          await immutable(this.asset(existing.descriptor.id), bytes);
        }
        await this.cleanup(journal, existing); return existing.descriptor;
      }
      const previous = await this.listing(identity), sequence = previous.latest_sequence + 1;
      if (sequence > 999_999_999_999) throw new Error('Capture sequence exhausted');
      const bundle: CaptureBundle = { schema_version: 1, capture_id: identity.capture_id, id: sequenceId(sequence), sequence, frames: [] };
      const clocks: string[] = []; let jsonBytes = Buffer.byteLength(JSON.stringify(bundle)), decodedBytes = 0;
      // Snapshot names once. Later arrivals stay loose for the next bundle.
      const paths = await journal.files(limit);
      if (paths.length > limit || new Set(paths).size !== paths.length) throw new Error('Invalid capture journal snapshot');
      for (const path of paths) {
        const name = basename(path); frameName(name);
        if (resolve(path) !== join(journalDirectory, name)) throw new Error('Journal frame escaped its directory');
        const bytes = await readBounded(path, CAPTURE_LIMITS.frame_bytes), { batch, decoded_bytes } = inspectFrame(bytes);
        const frame = { name, sha256: digest(bytes), bytes: bytes.length, data_base64: bytes.toString('base64') };
        const additional = Buffer.byteLength(JSON.stringify(frame)) + (bundle.frames.length ? 1 : 0);
        if (jsonBytes + additional > CAPTURE_LIMITS.bundle_json_bytes || decodedBytes + decoded_bytes > CAPTURE_LIMITS.decoded_frames_bytes) break;
        bundle.frames.push(frame); clocks.push(batch.received_at); jsonBytes += additional; decodedBytes += decoded_bytes;
      }
      if (!bundle.frames.length) return null;
      const plain = Buffer.from(JSON.stringify(bundle)), bytes = gzipSync(plain, { level: 6 });
      if (plain.length > CAPTURE_LIMITS.bundle_json_bytes || bytes.length > CAPTURE_LIMITS.bundle_bytes) throw new Error('Capture bundle exceeds its size limit');
      clocks.sort((a, b) => Date.parse(a) - Date.parse(b));
      const descriptor: CaptureBundleDescriptor = { id: bundle.id, sequence, sha256: digest(bytes), bytes: bytes.length,
        uncompressed_bytes: plain.length, frames: bundle.frames.length, first_received_at: clocks[0], last_received_at: clocks.at(-1)!, sealed_at: new Date().toISOString() };
      const intent: SealIntent = { schema_version: 1, capture_id: identity.capture_id, journal_directory: journalDirectory, descriptor,
        frames: bundle.frames.map(({ data_base64: _data, ...frame }) => frame) };
      await immutable(this.pending, encode(intent));
      await immutable(this.asset(descriptor.id), bytes);
      await this.cleanup(journal, intent); return descriptor;
    });
  }
  bundlePath(id: string): Promise<string> {
    return this.serial(async () => {
      const path = this.asset(id), manifest = await this.listing(await this.initialize());
      const descriptor = manifest.bundles.find(bundle => bundle.id === id);
      if (!descriptor) throw new Error('Unknown capture bundle');
      const bytes = await readBounded(path, CAPTURE_LIMITS.bundle_bytes);
      if (bytes.length !== descriptor.bytes || digest(bytes) !== descriptor.sha256) throw new Error('Capture bundle checksum mismatch');
      return path;
    });
  }
  recordSchedule(bytes: Uint8Array, source: string, firstSeenISO: string): Promise<CaptureScheduleDescriptor> {
    // Copy caller-owned bytes so an asynchronous mutation cannot alter the asset.
    if (!bytes.length || bytes.length > CAPTURE_LIMITS.schedule_bytes) return Promise.reject(new Error('Capture schedule exceeds its size limit'));
    const data = Buffer.from(bytes);
    return this.serial(async () => {
      await this.initialize();
      const descriptor: CaptureScheduleDescriptor = { id: digest(data), sha256: digest(data), bytes: data.length, source, first_seen_at: firstSeenISO };
      checkSchedule(descriptor);
      const path = join(this.schedules, descriptor.id + '.manifest.json'), pending = join(this.schedules, descriptor.id + '.pending.json');
      const earlier = await optional(path, 8192) ?? await optional(pending, 8192);
      const preserved: unknown = earlier ? JSON.parse(earlier.toString('utf8')) : descriptor; checkSchedule(preserved);
      if (preserved.id !== descriptor.id || preserved.bytes !== data.length) throw new Error('Capture schedule identity conflict');
      if (!earlier) await immutable(pending, encode(preserved));
      await immutable(this.scheduleAsset(descriptor.id), data);
      const verified = await readBounded(this.scheduleAsset(descriptor.id), CAPTURE_LIMITS.schedule_bytes);
      if (digest(verified) !== descriptor.sha256) throw new Error('Capture schedule checksum mismatch');
      await immutable(path, encode(preserved)); await remove(pending);
      return preserved;
    });
  }
  schedulePath(id: string): Promise<string> {
    return this.serial(async () => {
      const path = this.scheduleAsset(id), manifest = await this.listing(await this.initialize());
      const descriptor = manifest.schedules.find(schedule => schedule.id === id);
      if (!descriptor) throw new Error('Unknown capture schedule');
      const bytes = await readBounded(path, CAPTURE_LIMITS.schedule_bytes);
      if (bytes.length !== descriptor.bytes || digest(bytes) !== descriptor.sha256) throw new Error('Capture schedule checksum mismatch');
      return path;
    });
  }
}
