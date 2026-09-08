/** Bounded, unframed Thrift binary. Field numbers come from Le Pass 5.197.0.1799. */
export type ThriftValue = boolean | number | bigint | string | ThriftStruct | ThriftValue[];
export interface ThriftStruct { [field: number]: ThriftValue }
export interface ThriftField { id: number; type: number; value: ThriftValue; itemType?: number }
export const T = { BOOL: 2, BYTE: 3, DOUBLE: 4, I16: 6, I32: 8, I64: 10, STRING: 11, STRUCT: 12, MAP: 13, SET: 14, LIST: 15 } as const;

export function decodeThrift(data: Uint8Array): ThriftStruct {
  const result = decodeThriftSequence(data);
  if (result.length !== 1) throw new Error('Trailing Thrift response data');
  return result[0];
}

/** Multi-item app endpoints stream consecutive unframed structures. */
export function decodeThriftSequence(data: Uint8Array): ThriftStruct[] {
  const b = Buffer.from(data); let at = 0; let items = 0;
  if (b.length > 8_000_000) throw new Error('Thrift response exceeds size limit');
  function take(size: number): Buffer {
    if (size < 0 || at + size > b.length) throw new Error('Truncated Thrift response');
    const result = b.subarray(at, at + size); at += size; return result;
  }
  function count(n: number): number {
    items += n;
    if (n < 0 || n > 100_000 || items > 200_000) throw new Error('Thrift collection exceeds limit');
    return n;
  }
  function read(type: number, depth: number): ThriftValue {
    if (depth > 32) throw new Error('Thrift nesting exceeds limit');
    switch (type) {
      case T.BOOL: return take(1)[0] !== 0;
      case T.BYTE: return take(1).readInt8();
      case T.DOUBLE: return take(8).readDoubleBE();
      case T.I16: return take(2).readInt16BE();
      case T.I32: return take(4).readInt32BE();
      case T.I64: { const n = take(8).readBigInt64BE(); return n >= BigInt(Number.MIN_SAFE_INTEGER) && n <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(n) : n; }
      case T.STRING: return take(take(4).readInt32BE()).toString('utf8');
      case T.STRUCT: {
        const result: ThriftStruct = {};
        for (;;) {
          const kind = take(1)[0]; if (!kind) return result;
          count(1); const id = take(2).readInt16BE();
          if (Object.hasOwn(result, id)) throw new Error('Duplicate Thrift field');
          result[id] = read(kind, depth + 1);
        }
      }
      case T.LIST: case T.SET: {
        const kind = take(1)[0]; const n = count(take(4).readInt32BE());
        return Array.from({ length: n }, () => read(kind, depth + 1));
      }
      case T.MAP: {
        const key = take(1)[0], value = take(1)[0], n = count(take(4).readInt32BE());
        return Array.from({ length: n }, () => [read(key, depth + 1), read(value, depth + 1)]);
      }
      default: throw new Error('Unknown Thrift field type');
    }
  }
  if (!b.length) throw new Error('Truncated Thrift response');
  const result: ThriftStruct[] = [];
  while (at < b.length) {
    if (result.length >= 10_000) throw new Error('Thrift response sequence exceeds limit');
    result.push(read(T.STRUCT, 0) as ThriftStruct);
  }
  return result;
}

export function encodeThrift(fields: ThriftField[]): Buffer {
  const chunks: Buffer[] = [];
  function number(value: number | bigint, bytes: number): void {
    const b = Buffer.alloc(bytes);
    if (bytes === 8) b.writeBigInt64BE(BigInt(value));
    else b.writeIntBE(Number(value), 0, bytes);
    chunks.push(b);
  }
  function value(type: number, v: ThriftValue, itemType?: number): void {
    switch (type) {
      case T.BOOL: chunks.push(Buffer.from([v ? 1 : 0])); break;
      case T.BYTE: number(Number(v), 1); break;
      case T.I16: number(Number(v), 2); break;
      case T.I32: number(Number(v), 4); break;
      case T.I64: number(BigInt(v as number | bigint), 8); break;
      case T.STRING: { const b = Buffer.from(String(v)); number(b.length, 4); chunks.push(b); break; }
      case T.STRUCT: chunks.push(encodeThrift(v as unknown as ThriftField[])); break;
      case T.LIST: case T.SET: {
        if (!itemType || !Array.isArray(v)) throw new Error('Thrift list requires item type');
        number(itemType, 1); number(v.length, 4); for (const x of v) value(itemType, x); break;
      }
      default: throw new Error('Unsupported Thrift write type');
    }
  }
  for (const field of fields) { number(field.type, 1); number(field.id, 2); value(field.type, field.value, field.itemType); }
  chunks.push(Buffer.from([0])); return Buffer.concat(chunks);
}

export const field = (id: number, type: number, value: ThriftValue | ThriftField[], itemType?: number): ThriftField => ({ id, type, value: value as ThriftValue, itemType });
export function struct(value: ThriftValue | undefined): ThriftStruct | null { return value !== null && typeof value === 'object' && !Array.isArray(value) ? value : null; }
export function numeric(value: ThriftValue | undefined): number | null { return typeof value === 'number' && Number.isFinite(value) ? value : null; }
export function stringId(value: ThriftValue | undefined): string | null { return typeof value === 'string' || typeof value === 'number' || typeof value === 'bigint' ? String(value) : null; }
