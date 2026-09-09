import { createCipheriv, createDecipheriv, randomBytes, randomUUID } from 'node:crypto';
import { mkdir, open, readFile, rename, unlink } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { decodeThrift, decodeThriftSequence, encodeThrift, field, numeric, stringId, struct, T, type ThriftStruct } from './lepass-thrift.js';

export interface LePassToken { issuedAt: number; expiresAt: number; token: string }
export interface LePassCredentials { version: 1; installationId: string; userKey: string; access: LePassToken; refresh: LePassToken }
export class LePassError extends Error {
  constructor(public code: string, public status: number | null = null, public retryAfterMs = 0) { super(code); this.name = 'LePassError'; }
}
export type LePassTransport = (endpoint: string, body: Buffer, headers: Record<string, string>, signal?: AbortSignal, method?: 'POST' | 'GET') => Promise<{ body: Buffer; receivedAt: number }>;

/** No bodies, token strings, or response errors are logged or exposed as health messages. */
export const fetchLePass: LePassTransport = async (endpoint, body, headers, signal, method = 'POST') => {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 20_000);
  const abort = () => controller.abort(); signal?.addEventListener('abort', abort, { once: true });
  if (signal?.aborted) controller.abort();
  try {
    const response = await fetch(endpoint, { method, body: method === 'POST' ? new Uint8Array(body).buffer : undefined, headers, signal: controller.signal, redirect: 'error' });
    if (!response.ok) {
      await response.body?.cancel();
      const after = response.headers.get('retry-after');
      const seconds = Number(after);
      const retryAfterMs = after ? (Number.isFinite(seconds) ? seconds * 1000 : Date.parse(after) - Date.now()) : 0;
      throw new LePassError(response.status === 401 || response.status === 403 ? 'authentication_rejected' : 'http_error', response.status, Math.max(0, retryAfterMs || 0));
    }
    const chunks: Buffer[] = []; let bytes = 0;
    const reader = response.body?.getReader();
    if (!reader) throw new LePassError('empty_response');
    for (;;) {
      const chunk = await reader.read(); if (chunk.done) break;
      bytes += chunk.value.byteLength;
      if (bytes > 8_000_000) { await reader.cancel(); throw new LePassError('response_too_large'); }
      chunks.push(Buffer.from(chunk.value));
    }
    return { body: Buffer.concat(chunks), receivedAt: Date.now() };
  } catch (error) {
    if (error instanceof LePassError) throw error;
    throw new LePassError(controller.signal.aborted ? 'request_aborted' : 'network_error');
  } finally { clearTimeout(timeout); signal?.removeEventListener('abort', abort); }
};

export class LePassCredentialStore {
  private key: Buffer;
  constructor(public readonly path: string, encryptionKey: string) {
    this.key = Buffer.from(encryptionKey, 'base64');
    if (this.key.length !== 32) throw new LePassError('encryption_key_must_be_32_bytes_base64');
  }
  async load<T>(): Promise<T | null> {
    let data: Buffer;
    try { data = await readFile(this.path); } catch (e) { if ((e as NodeJS.ErrnoException).code === 'ENOENT') return null; throw new LePassError('credential_read_failed'); }
    try {
      if (data.length < 33 || data.subarray(0, 4).toString() !== 'LPS1') throw new Error();
      const decipher = createDecipheriv('aes-256-gcm', this.key, data.subarray(4, 16));
      decipher.setAAD(Buffer.from('lepass-credentials-v1')); decipher.setAuthTag(data.subarray(16, 32));
      return JSON.parse(Buffer.concat([decipher.update(data.subarray(32)), decipher.final()]).toString()) as T;
    } catch { throw new LePassError('credential_decryption_failed'); }
  }
  async save(value: unknown): Promise<void> {
    await mkdir(dirname(this.path), { recursive: true, mode: 0o700 });
    const iv = randomBytes(12), cipher = createCipheriv('aes-256-gcm', this.key, iv);
    cipher.setAAD(Buffer.from('lepass-credentials-v1'));
    const content = Buffer.concat([cipher.update(JSON.stringify(value)), cipher.final()]);
    const encoded = Buffer.concat([Buffer.from('LPS1'), iv, cipher.getAuthTag(), content]);
    const temporary = `${this.path}.${randomUUID()}.tmp`;
    try {
      const handle = await open(temporary, 'wx', 0o600);
      try { await handle.writeFile(encoded); await handle.sync(); } finally { await handle.close(); }
      await rename(temporary, this.path);
      const directory = await open(dirname(this.path), 'r'); try { await directory.sync(); } finally { await directory.close(); }
    } catch { await unlink(temporary).catch(() => {}); throw new LePassError('credential_write_failed'); }
  }
}

function token(value: unknown): LePassToken {
  const s = struct(value as never), issuedAt = numeric(s?.[1]), expiresAt = numeric(s?.[2]), text = stringId(s?.[3]);
  if (!issuedAt || !expiresAt || expiresAt <= issuedAt || !text) throw new LePassError('invalid_token_response');
  return { issuedAt, expiresAt, token: text };
}
function pair(value: unknown): Pick<LePassCredentials, 'access' | 'refresh'> {
  const s = struct(value as never); return { access: token(s?.[1]), refresh: token(s?.[2]) };
}
export function tokenRefreshDue(value: LePassToken, now: number): boolean { return now < value.issuedAt || now >= value.issuedAt + (value.expiresAt - value.issuedAt) * .9; }

export function createGuestRequest(installationId: string, now: number, apiKey: string): Buffer {
  return encodeThrift([
    field(1, T.STRUCT, [field(1, T.I32, 29_953_527), field(2, T.I32, -90_070_130)]),
    field(2, T.I16, 1504),
    field(3, T.STRUCT, [field(1, T.STRING, 'en'), field(2, T.STRING, 'US'), field(3, T.STRING, '')]),
    field(4, T.STRING, 'Transit research collector'), field(5, T.STRING, 'Android 12'), field(6, T.I32, 2),
    field(7, T.STRUCT, [field(1, T.STRING, ''), field(2, T.STRING, ''), field(3, T.STRING, '')]),
    field(8, T.STRING, ''), field(9, T.STRING, ''), field(10, T.BOOL, true), field(11, T.I32, 4),
    field(12, T.I64, now), field(13, T.I32, 1), field(15, T.STRING, installationId), field(16, T.STRING, apiKey),
    field(19, T.STRING, installationId), field(21, T.STRING, 'com.norta.lepass'),
  ]);
}

export interface LePassAuthOptions { stateDir: string; encryptionKey: string; apiKey: string; clientVersion?: string; allowGuestBootstrap?: boolean; transport?: LePassTransport; now?: () => number; signal?: AbortSignal }
export class LePassAuth {
  private credentials: LePassCredentials | null = null;
  private pending: Promise<LePassCredentials> | null = null;
  readonly store: LePassCredentialStore;
  private transport: LePassTransport; private now: () => number;
  private metroRevision: string | null = null; private metroUpdatedAt = 0;
  get currentMetroRevision(): string | null { return this.metroRevision; }
  constructor(private options: LePassAuthOptions) {
    this.store = new LePassCredentialStore(join(options.stateDir, 'lepass-credentials.enc'), options.encryptionKey);
    this.transport = options.transport ?? fetchLePass; this.now = options.now ?? Date.now;
  }
  headers(credentials?: LePassCredentials, access = true): Record<string, string> {
    return { 'Content-Type': 'application/octet', Accept: 'application/octet', API_KEY: this.options.apiKey, CLIENT_VERSION: this.options.clientVersion ?? '5.197.0.1799', PHONE_TYPE: '2', 'Gtfs-Language': 'en',
      ...(credentials ? { USER_KEY: credentials.userKey } : {}), ...(credentials && access ? { 'Access-Token': credentials.access.token } : {}) };
  }
  async get(force = false): Promise<LePassCredentials> {
    if (this.pending) return this.pending;
    this.pending = this.acquire(force).finally(() => { this.pending = null; }); return this.pending;
  }
  private async acquire(force: boolean): Promise<LePassCredentials> {
    if (!this.credentials) {
      const saved = await this.store.load<LePassCredentials | { pending: true; installationId: string }>();
      if (saved && 'pending' in saved) throw new LePassError('guest_bootstrap_requires_recovery');
      if (saved) {
        if (saved.version !== 1 || !saved.userKey || !saved.access?.token || !saved.refresh?.token) throw new LePassError('invalid_saved_credentials');
        this.credentials = saved;
      }
    }
    if (!this.credentials) {
      if (!this.options.allowGuestBootstrap) throw new LePassError('guest_bootstrap_required');
      const installationId = randomUUID();
      // A lost response must not trigger a new guest account on every restart.
      await this.store.save({ pending: true, installationId });
      const result = await this.transport('https://app4.moovitapp.com/services-app/services/UserAuth/CreateUser', createGuestRequest(installationId, this.now(), this.options.apiKey), this.headers(), this.options.signal);
      const response = decodeThrift(result.body), user = struct(response[1]), userKey = stringId(user?.[1]);
      if (!userKey) throw new LePassError('invalid_guest_response');
      const authentication = struct(user?.[7]);
      const credentials: LePassCredentials = { version: 1, installationId, userKey, ...pair(authentication?.[1]) };
      await this.store.save(credentials); this.credentials = credentials;
    }
    if (force || tokenRefreshDue(this.credentials.access, this.now())) {
      if (this.credentials.refresh.expiresAt <= this.now()) throw new LePassError('refresh_token_expired');
      const response = await this.transport('https://app4.moovitapp.com/services-app/services/UserAuth/RefreshTokens', encodeThrift([field(1, T.STRING, this.credentials.refresh.token)]), this.headers(this.credentials, false), this.options.signal);
      const credentials = { ...this.credentials, ...pair(decodeThrift(response.body)[1]) };
      await this.store.save(credentials); this.credentials = credentials;
    }
    return this.credentials;
  }
  async request(path: 'V4/StopsArrivals' | 'V4/LineArrivals', body: Buffer): Promise<{ data: ThriftStruct; receivedAt: number }> {
    const response = await this.requestMany(path, body);
    if (response.data.length !== 1) throw new LePassError('unexpected_response_count');
    return { ...response, data: response.data[0] };
  }
  async requestMany(path: 'V4/StopsArrivals' | 'V4/LineArrivals', body: Buffer): Promise<{ data: ThriftStruct[]; receivedAt: number }> {
    const before = await this.get();
    await this.updateMetro();
    const run = async (credentials: LePassCredentials) => {
      const response = await this.transport(`https://app5.moovitapp.com/services-app/services/${path}`, body, { ...this.headers(credentials), 'Metro-Revision-Metro-Id': '1504', 'Metro-Revision-Number': this.metroRevision! }, this.options.signal);
      return { data: decodeThriftSequence(response.body), receivedAt: response.receivedAt };
    };
    try { return await run(before); }
    catch (error) {
      if (error instanceof LePassError && error.status === 412) { await this.updateMetro(true); return run(await this.get()); }
      if (!(error instanceof LePassError) || error.status !== 401) throw error;
      const credentials = this.credentials?.access.token !== before.access.token ? this.credentials! : await this.get(true);
      return run(credentials); // Exactly one authenticated replay, even if that replay is rejected.
    }
  }
  async updateMetro(force = false): Promise<ThriftStruct | null> {
    if (!force && this.metroRevision && this.now() - this.metroUpdatedAt < 6 * 60 * 60 * 1000) return null;
    const response = await this.transport('https://app5.moovitapp.com/services-app/services/V4/GetMetroData?metroAreaId=1504&metroRevisionNumber=0', Buffer.alloc(0), this.headers(await this.get()), this.options.signal, 'GET');
    const data = decodeThrift(response.body), revision = stringId(data[14]);
    if (numeric(data[1]) !== 1504 || !revision) throw new LePassError('invalid_metro_response');
    this.metroRevision = revision; this.metroUpdatedAt = this.now(); return data;
  }
}
