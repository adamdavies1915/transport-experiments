import { timingSafeEqual } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { stat } from 'node:fs/promises';
import type { IncomingMessage, ServerResponse } from 'node:http';
import type { CaptureExchange } from './capture-exchange';
import type { ProcessingCoordinator } from './processing-coordinator';
import { safeError } from './log-safety';

const ROOT = '/internal/processing';
function authorized(header: string | undefined, token: string) {
  const value = header?.startsWith('Bearer ') ? header.slice(7) : '';
  const a = Buffer.from(value), b = Buffer.from(token);
  return a.length === b.length && a.length > 0 && timingSafeEqual(a, b);
}
async function body(req: IncomingMessage, maximum: number): Promise<Record<string, unknown>> {
  let length = 0; const chunks: Buffer[] = [];
  for await (const chunk of req) {
    const bytes = Buffer.from(chunk); length += bytes.length;
    if (length > maximum) throw new Error('Processing request exceeds its size limit'); chunks.push(bytes);
  }
  const value: unknown = JSON.parse(Buffer.concat(chunks).toString('utf8'));
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Invalid processing request');
  return value as Record<string, unknown>;
}
function string(value: unknown): string { if (typeof value !== 'string' || !value.length || value.length > 256) throw new Error('Invalid processing request field'); return value; }
function reference(value: Record<string, unknown>) {
  if (!Number.isSafeInteger(value.fence) || Number(value.fence) < 1) throw new Error('Invalid processing fence');
  return { job_id: string(value.job_id), lease_token: string(value.lease_token), fence: Number(value.fence) };
}
export function processingRequestHandler(options: { exchange: CaptureExchange; coordinator: ProcessingCoordinator; token: string; seal: () => Promise<unknown> }) {
  if (options.token.length < 32) throw new Error('TRANSIT_PROCESSING_TOKEN must contain at least 32 characters');
  return async (req: IncomingMessage, res: ServerResponse): Promise<boolean> => {
    if (!req.url?.startsWith(ROOT + '/')) return false;
    const json = (status: number, value: unknown) => { res.statusCode = status; res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify(value)); };
    res.setHeader('Cache-Control', 'no-store');
    if (!authorized(req.headers.authorization, options.token)) { json(401, { error: 'Authentication required' }); return true; }
    try {
      if (req.method === 'GET' && req.url === ROOT + '/status') json(200, await options.coordinator.status());
      else if (req.method === 'GET' && req.url === ROOT + '/manifest') json(200, await options.exchange.manifest());
      else if (req.method === 'GET' && /^\/internal\/processing\/(bundles\/\d{12}|schedules\/[a-f0-9]{64})$/.test(req.url)) {
        const [, , , type, id] = req.url.split('/');
        const path = type === 'bundles' ? await options.exchange.bundlePath(id) : await options.exchange.schedulePath(id);
        const info = await stat(path); res.setHeader('Content-Type', 'application/octet-stream'); res.setHeader('Content-Length', info.size);
        const stream = createReadStream(path); stream.on('error', () => res.destroy()); res.on('close', () => stream.destroy()); stream.pipe(res);
      } else if (req.method === 'POST' && req.url === ROOT + '/claim') {
        const value = await body(req, 8192); await options.seal();
        json(200, await options.coordinator.claim(string(value.worker_id), string(value.baseline_id), string(value.analysis_revision), await options.exchange.manifest()));
      } else if (req.method === 'POST' && (req.url === ROOT + '/renew' || req.url === ROOT + '/release')) {
        const ref = reference(await body(req, 8192));
        if (req.url.endsWith('/renew')) json(200, await options.coordinator.renew(ref));
        else { await options.coordinator.release(ref); json(200, { released: true }); }
      } else if (req.method === 'POST' && req.url === ROOT + '/complete') {
        const value = await body(req, 64 * 1024 * 1024);
        if (!value.summary || typeof value.summary !== 'object' || Array.isArray(value.summary)) throw new Error('A public summary is required');
        json(200, await options.coordinator.complete({ ...reference(value), manifest_sha256: string(value.manifest_sha256), summary: JSON.stringify(value.summary) }));
      } else json(404, { error: 'Unknown processing endpoint' });
    } catch (error) { if (!res.headersSent) json(409, { error: safeError(error) }); else res.destroy(); }
    return true;
  };
}
