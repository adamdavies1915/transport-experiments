import { randomUUID, timingSafeEqual } from 'node:crypto';
import { mkdir, open, rename, unlink } from 'node:fs/promises';
import type { IncomingMessage, ServerResponse } from 'node:http';
import { join } from 'node:path';
import { promisify } from 'node:util';
import { gzip, gunzip } from 'node:zlib';
import { parseSummary } from '../dashboard/src/summary-validation';

export const SUMMARY_TRANSFER_MAX_BYTES = 60 * 1024 * 1024;
const compress = promisify(gzip), decompress = promisify(gunzip);
const SUMMARY_PATH = '/internal/summary';

class TransferError extends Error {
  constructor(readonly status: number, message: string) { super(message); }
}
function authorize(header: string | undefined, token: string) {
  if (!header?.startsWith('Bearer ') || Buffer.byteLength(header) !== Buffer.byteLength(token) + 7) return false;
  return timingSafeEqual(Buffer.from(header.slice(7)), Buffer.from(token));
}
function validateToken(token: string | undefined, name: string) {
  if (token !== undefined && (token.length < 32 || token.length > 1024 || /\s/.test(token))) throw new Error(`${name} must contain 32–1024 non-whitespace characters`);
}
async function readBoundedFile(path: string, maximum: number) {
  const file = await open(path, 'r');
  try {
    const info = await file.stat();
    if (!info.isFile() || info.size > maximum) throw new TransferError(413, 'Summary exceeds its size limit');
    const chunks: Buffer[] = []; let length = 0;
    while (true) {
      const buffer = Buffer.allocUnsafe(Math.min(64 * 1024, maximum - length + 1));
      const { bytesRead } = await file.read(buffer);
      if (!bytesRead) break;
      length += bytesRead;
      if (length > maximum) throw new TransferError(413, 'Summary exceeds its size limit');
      chunks.push(buffer.subarray(0, bytesRead));
    }
    return Buffer.concat(chunks, length);
  } finally { await file.close(); }
}
function publicSummary(bytes: Buffer, maximum: number) {
  if (bytes.length > maximum) throw new TransferError(413, 'Summary exceeds its size limit');
  try {
    const summary = parseSummary(JSON.parse(bytes.toString('utf8')));
    const encoded = Buffer.from(JSON.stringify(summary));
    if (encoded.length > maximum) throw new TransferError(413, 'Summary exceeds its size limit');
    // Do not retain the parsed object tree while writing or compressing a large
    // publication. Only its timestamp and bounded serialized bytes are needed.
    return { generatedAt: summary.generated_at, encoded };
  } catch (error) {
    if (error instanceof TransferError) throw error;
    throw new TransferError(400, 'Invalid public summary');
  }
}
async function receiveSummary(req: IncomingMessage, maximum: number) {
  const encoding = req.headers['content-encoding'] ?? 'identity';
  if (encoding !== 'identity' && encoding !== 'gzip') throw new TransferError(415, 'Unsupported summary encoding');
  if (req.headers['content-type']?.split(';')[0].trim().toLowerCase() !== 'application/json') throw new TransferError(415, 'A JSON summary is required');
  const declared = req.headers['content-length'];
  if (declared && (!/^\d+$/.test(declared) || Number(declared) > maximum)) throw new TransferError(413, 'Summary exceeds its size limit');
  const chunks: Buffer[] = []; let length = 0;
  req.setTimeout(30_000, () => req.destroy());
  try {
    for await (const chunk of req) {
      const bytes = Buffer.from(chunk); length += bytes.length;
      if (length > maximum) throw new TransferError(413, 'Summary exceeds its size limit');
      chunks.push(bytes);
    }
  } finally { req.setTimeout(0); }
  const bytes = Buffer.concat(chunks, length);
  if (encoding === 'identity') return publicSummary(bytes, maximum);
  try {
    // zlib enforces the decoded bound while inflating, including concatenated
    // gzip members. A small compressed request cannot allocate unlimited RAM.
    return publicSummary(await decompress(bytes, { maxOutputLength: maximum }), maximum);
  } catch (error) {
    if (error instanceof TransferError) throw error;
    if ((error as NodeJS.ErrnoException).code === 'ERR_BUFFER_TOO_LARGE') throw new TransferError(413, 'Summary exceeds its size limit');
    throw new TransferError(400, 'Invalid compressed summary');
  }
}
async function saveSummary(directory: string, bytes: Buffer) {
  await mkdir(directory, { recursive: true, mode: 0o700 });
  const temporary = join(directory, `.summary-${randomUUID()}.tmp`);
  try {
    const file = await open(temporary, 'wx', 0o600);
    try { await file.writeFile(bytes); await file.sync(); } finally { await file.close(); }
    await rename(temporary, join(directory, 'summary.json'));
    const parent = await open(directory, 'r');
    try { await parent.sync(); } finally { await parent.close(); }
  } finally { await unlink(temporary).catch(() => {}); }
}

/** Small authenticated publication service; owns only the saved public JSON. */
export function createSummaryTransferHandler(options: {
  directory: string; readToken?: string; publishToken?: string;
  /** Lower bounds are useful for integration tests; the production ceiling cannot be raised. */
  maxBytes?: number;
}) {
  validateToken(options.readToken, 'TRANSIT_SUMMARY_TOKEN');
  validateToken(options.publishToken, 'TRANSIT_SUMMARY_PUBLISH_TOKEN');
  if (options.readToken && options.publishToken === options.readToken) throw new Error('Summary read and publish tokens must be different');
  const maximum = options.maxBytes ?? SUMMARY_TRANSFER_MAX_BYTES;
  if (!Number.isSafeInteger(maximum) || maximum < 1 || maximum > SUMMARY_TRANSFER_MAX_BYTES) throw new Error('Invalid summary transfer size limit');
  let receiving = false;
  return async (req: IncomingMessage, res: ServerResponse): Promise<boolean> => {
    if (req.url !== SUMMARY_PATH) return false;
    const json = (status: number, value: unknown) => {
      res.statusCode = status; res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify(value));
    };
    // Rejected requests do not drain an arbitrary unauthenticated body into RAM.
    res.setHeader('Cache-Control', 'no-store');
    res.setHeader('Connection', 'close');
    if (req.method !== 'GET' && req.method !== 'POST') { res.setHeader('Allow', 'GET, POST'); json(405, { error: 'Unsupported summary method' }); return true; }
    const token = req.method === 'GET' ? options.readToken : options.publishToken;
    if (!token) { json(503, { error: 'Summary access is not configured' }); return true; }
    if (!authorize(req.headers.authorization, token)) { json(401, { error: 'Authentication required' }); return true; }
    if (req.method === 'GET') {
      try {
        const saved = publicSummary(await readBoundedFile(join(options.directory, 'summary.json'), maximum), maximum);
        const acceptsGzip = req.headers['accept-encoding']?.split(',').some(part => {
          const [name, ...parameters] = part.trim().split(';');
          const quality = parameters.find(value => value.trim().startsWith('q='))?.trim().slice(2);
          return name === 'gzip' && (quality === undefined || Number(quality) > 0);
        });
        const body = acceptsGzip ? await compress(saved.encoded) : saved.encoded;
        res.statusCode = 200; res.setHeader('Content-Type', 'application/json'); res.setHeader('Vary', 'Accept-Encoding');
        if (acceptsGzip) res.setHeader('Content-Encoding', 'gzip');
        res.setHeader('Content-Length', body.length); res.end(body);
      } catch { json(503, { error: 'No readable saved summary is available yet' }); }
      return true;
    }
    if (receiving) { json(429, { error: 'A summary publication is already running' }); return true; }
    receiving = true;
    try {
      const next = await receiveSummary(req, maximum);
      let previous: ReturnType<typeof publicSummary> | undefined;
      try { previous = publicSummary(await readBoundedFile(join(options.directory, 'summary.json'), maximum), maximum); }
      catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw new TransferError(503, 'Existing summary could not be verified'); }
      if (previous && Date.parse(next.generatedAt) < Date.parse(previous.generatedAt)) throw new TransferError(409, 'An older summary cannot replace the saved summary');
      await saveSummary(options.directory, next.encoded);
      json(200, { published: true, generated_at: next.generatedAt, bytes: next.encoded.length });
    } catch (error) {
      if (!res.destroyed && !res.headersSent) json(error instanceof TransferError ? error.status : 503,
        { error: error instanceof TransferError ? error.message : 'Summary publication failed; the previous summary remains available' });
    } finally { receiving = false; }
    return true;
  };
}

function publicationUrl(value: string) {
  const url = new URL(value);
  const loopback = ['localhost', '127.0.0.1', '[::1]'].includes(url.hostname);
  if ((url.protocol !== 'https:' && !(url.protocol === 'http:' && loopback)) || url.username || url.password || url.search || url.hash || url.pathname !== SUMMARY_PATH) throw new Error('Invalid summary publication URL');
  return url.toString();
}
/** Reads the completed local publication; errors never affect local analysis. */
export async function pushSummaryFile(options: {
  path: string; url: string; token: string; timeoutMs?: number; fetcher?: typeof fetch;
}): Promise<{ published: boolean; reason?: string; bytes?: number }> {
  try {
    validateToken(options.token, 'TRANSIT_SUMMARY_PUBLISH_TOKEN');
    const url = publicationUrl(options.url);
    const saved = publicSummary(await readBoundedFile(options.path, SUMMARY_TRANSFER_MAX_BYTES), SUMMARY_TRANSFER_MAX_BYTES);
    const bytes = await compress(saved.encoded);
    if (bytes.length > SUMMARY_TRANSFER_MAX_BYTES) throw new Error('Compressed summary exceeds its size limit');
    const response = await (options.fetcher ?? fetch)(url, { method: 'POST', redirect: 'error', signal: AbortSignal.timeout(options.timeoutMs ?? 30_000),
      headers: { Authorization: `Bearer ${options.token}`, 'Content-Type': 'application/json', 'Content-Encoding': 'gzip' }, body: new Uint8Array(bytes) });
    await response.body?.cancel();
    if (response.status !== 200) return { published: false, reason: `Summary server rejected publication (HTTP ${response.status})` };
    return { published: true, bytes: saved.encoded.length };
  } catch { return { published: false, reason: 'Summary upload failed; the completed local summary is retained' }; }
}

export async function publishSummaryIfConfigured(path: string, options: {
  env?: NodeJS.ProcessEnv; fetcher?: typeof fetch; log?: (message: string) => void;
} = {}) {
  const env = options.env ?? process.env;
  // Daily processing publishes only after the coordinator validates its lease.
  if (env.PROCESSING_JOB_ID || (!env.TRANSIT_SUMMARY_PUBLISH_URL && !env.TRANSIT_SUMMARY_PUBLISH_TOKEN)) return;
  const log = options.log ?? console.log;
  if (!env.TRANSIT_SUMMARY_PUBLISH_URL || !env.TRANSIT_SUMMARY_PUBLISH_TOKEN) {
    log('[Summary upload] Both publication URL and token are required; the local summary is retained'); return;
  }
  const result = await pushSummaryFile({ path, url: env.TRANSIT_SUMMARY_PUBLISH_URL, token: env.TRANSIT_SUMMARY_PUBLISH_TOKEN, fetcher: options.fetcher });
  log(result.published ? `[Summary upload] Published ${result.bytes} bytes` : `[Summary upload] ${result.reason}`);
}
