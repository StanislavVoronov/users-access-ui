import { createHash } from 'node:crypto';
import { promises as fs } from 'node:fs';
import type { IncomingMessage, ServerResponse } from 'node:http';
import path from 'node:path';

export type CacheMode = 'remote' | 'offline' | 'auto';

export type PathFilter =
  | string
  | RegExp
  | ((pathname: string, request: IncomingMessage) => boolean);

export interface CacheRule {
  pathFilter: PathFilter;
  mode: CacheMode;
  /** Cache TTL in milliseconds. Used by auto mode. */
  ttl?: number;
}

export interface CacheMiddlewareOptions {
  cacheDir?: string;
  rules: CacheRule[];
  getUserId: (
    request: IncomingMessage,
  ) => string | undefined | Promise<string | undefined>;
}

interface CachedResponse {
  cachedAt: number;
  request: {
    method: string;
    url: string;
  };
  response: {
    status: number;
    headers: Record<string, string | string[]>;
    body: string; // base64
  };
}

type Next = (error?: unknown) => void;

const DEFAULT_CACHE_DIR = '.cache/api';

/**
 * Rsbuild/connect-compatible middleware for local API caching.
 *
 * Modes:
 * - remote: always continue to proxy/backend and record successful responses.
 * - offline: never call next(); serve the current user's cached response only.
 * - auto: continue to proxy/backend; if it returns an error response, use a
 *   non-expired cached response when available. Successful responses refresh cache.
 *
 * Intended for ordinary buffered API responses (JSON/text/small binary responses),
 * not SSE, streaming downloads, or WebSocket traffic.
 */
export function createCacheMiddleware(options: CacheMiddlewareOptions) {
  const cacheDir = options.cacheDir ?? DEFAULT_CACHE_DIR;

  return async (
    request: IncomingMessage,
    response: ServerResponse,
    next: Next,
  ): Promise<void> => {
    try {
      const url = new URL(
        request.url ?? '/',
        `http://${request.headers.host ?? 'localhost'}`,
      );

      const rule = options.rules.find(({ pathFilter }) =>
        matchesPathFilter(pathFilter, url.pathname, request),
      );

      if (!rule) {
        next();
        return;
      }

      const userId = await options.getUserId(request);

      // Never mix anonymous/unknown users into one shared cache.
      if (!userId) {
        if (rule.mode === 'offline') {
          sendJson(response, 401, { error: 'CACHE_USER_NOT_FOUND' });
          return;
        }

        next();
        return;
      }

      const cacheFile = getCacheFile(
        cacheDir,
        userId,
        createCacheKey(request),
      );

      if (rule.mode === 'offline') {
        const cached = await readCache(cacheFile);

        if (!cached) {
          sendJson(response, 503, { error: 'OFFLINE_CACHE_MISS' });
          return;
        }

        sendCachedResponse(response, cached, 'OFFLINE');
        return;
      }

      interceptResponse({
        request,
        response,
        cacheFile,
        rule,
      });

      next();
    } catch (error) {
      next(error);
    }
  };
}

function interceptResponse({
  request,
  response,
  cacheFile,
  rule,
}: {
  request: IncomingMessage;
  response: ServerResponse;
  cacheFile: string;
  rule: CacheRule;
}): void {
  const originalWrite = response.write.bind(response);
  const originalEnd = response.end.bind(response);
  const originalWriteHead = response.writeHead.bind(response);

  const chunks: Buffer[] = [];
  let capturedStatus = response.statusCode;
  let capturedHeaders: Record<string, string | string[]> = {};
  let finished = false;

  response.writeHead = ((...args: Parameters<ServerResponse['writeHead']>) => {
    capturedStatus = args[0];

    // Do not actually flush headers yet. auto mode must be able to replace
    // a failed proxy response with cached content.
    const headers = extractWriteHeadHeaders(args);
    if (headers) {
      capturedHeaders = {
        ...capturedHeaders,
        ...normalizeHeaders(headers),
      };
    }

    return response;
  }) as ServerResponse['writeHead'];

  response.write = ((
    chunk: unknown,
    encoding?: BufferEncoding | ((error?: Error | null) => void),
    callback?: (error?: Error | null) => void,
  ) => {
    if (chunk !== undefined && chunk !== null) {
      chunks.push(
        toBuffer(
          chunk,
          typeof encoding === 'string' ? encoding : undefined,
        ),
      );
    }

    const cb = typeof encoding === 'function' ? encoding : callback;
    cb?.(null);
    return true;
  }) as ServerResponse['write'];

  response.end = ((
    chunk?: unknown,
    encoding?: BufferEncoding | (() => void),
    callback?: () => void,
  ) => {
    if (finished) {
      return response;
    }

    finished = true;

    if (chunk !== undefined && chunk !== null) {
      chunks.push(
        toBuffer(
          chunk,
          typeof encoding === 'string' ? encoding : undefined,
        ),
      );
    }

    const cb = typeof encoding === 'function' ? encoding : callback;

    void finalizeInterceptedResponse({
      request,
      response,
      cacheFile,
      rule,
      status: capturedStatus || response.statusCode,
      headers: {
        ...capturedHeaders,
        ...normalizeHeaders(response.getHeaders()),
      },
      body: Buffer.concat(chunks),
      originalWriteHead,
      originalWrite,
      originalEnd,
      callback: cb,
    });

    return response;
  }) as ServerResponse['end'];
}

async function finalizeInterceptedResponse({
  request,
  response,
  cacheFile,
  rule,
  status,
  headers,
  body,
  originalWriteHead,
  originalWrite,
  originalEnd,
  callback,
}: {
  request: IncomingMessage;
  response: ServerResponse;
  cacheFile: string;
  rule: CacheRule;
  status: number;
  headers: Record<string, string | string[]>;
  body: Buffer;
  originalWriteHead: ServerResponse['writeHead'];
  originalWrite: ServerResponse['write'];
  originalEnd: ServerResponse['end'];
  callback?: () => void;
}): Promise<void> {
  if (isSuccessfulStatus(status)) {
    await writeCache(cacheFile, {
      cachedAt: Date.now(),
      request: {
        method: request.method ?? 'GET',
        url: request.url ?? '/',
      },
      response: {
        status,
        headers,
        body: body.toString('base64'),
      },
    }).catch(() => undefined);

    flushResponse({
      status,
      headers,
      body,
      originalWriteHead,
      originalWrite,
      originalEnd,
      callback,
      cacheStatus: 'MISS',
    });
    return;
  }

  if (rule.mode === 'auto') {
    const cached = await readCache(cacheFile);

    if (cached && isFresh(cached, rule.ttl)) {
      flushResponse({
        status: cached.response.status,
        headers: cached.response.headers,
        body: Buffer.from(cached.response.body, 'base64'),
        originalWriteHead,
        originalWrite,
        originalEnd,
        callback,
        cacheStatus: 'HIT',
        cacheAge: Date.now() - cached.cachedAt,
      });
      return;
    }
  }

  flushResponse({
    status,
    headers,
    body,
    originalWriteHead,
    originalWrite,
    originalEnd,
    callback,
    cacheStatus: 'MISS',
  });
}

function flushResponse({
  status,
  headers,
  body,
  originalWriteHead,
  originalWrite,
  originalEnd,
  callback,
  cacheStatus,
  cacheAge,
}: {
  status: number;
  headers: Record<string, string | string[]>;
  body: Buffer;
  originalWriteHead: ServerResponse['writeHead'];
  originalWrite: ServerResponse['write'];
  originalEnd: ServerResponse['end'];
  callback?: () => void;
  cacheStatus: 'HIT' | 'MISS';
  cacheAge?: number;
}): void {
  const finalHeaders = sanitizeResponseHeaders(headers);
  finalHeaders['x-dev-cache'] = cacheStatus;

  if (cacheAge !== undefined) {
    finalHeaders['x-dev-cache-age'] = String(Math.floor(cacheAge / 1000));
  }

  finalHeaders['content-length'] = String(body.byteLength);

  originalWriteHead(status, finalHeaders);

  if (body.byteLength > 0) {
    originalWrite(body);
  }

  originalEnd(callback);
}

function sendCachedResponse(
  response: ServerResponse,
  cached: CachedResponse,
  cacheStatus: 'OFFLINE' | 'HIT',
): void {
  const body = Buffer.from(cached.response.body, 'base64');
  const headers = sanitizeResponseHeaders(cached.response.headers);

  headers['x-dev-cache'] = cacheStatus;
  headers['x-dev-cache-age'] = String(
    Math.floor((Date.now() - cached.cachedAt) / 1000),
  );
  headers['content-length'] = String(body.byteLength);

  response.writeHead(cached.response.status, headers);
  response.end(body);
}

function matchesPathFilter(
  filter: PathFilter,
  pathname: string,
  request: IncomingMessage,
): boolean {
  if (typeof filter === 'function') {
    return filter(pathname, request);
  }

  if (filter instanceof RegExp) {
    filter.lastIndex = 0;
    return filter.test(pathname);
  }

  return pathname.startsWith(filter);
}

function createCacheKey(request: IncomingMessage): string {
  return createHash('sha256')
    .update(request.method ?? 'GET')
    .update('\n')
    .update(request.url ?? '/')
    .digest('hex');
}

function getCacheFile(
  cacheDir: string,
  userId: string,
  key: string,
): string {
  return path.join(cacheDir, sanitize(userId), `${key}.json`);
}

async function readCache(file: string): Promise<CachedResponse | undefined> {
  try {
    return JSON.parse(await fs.readFile(file, 'utf8')) as CachedResponse;
  } catch {
    return undefined;
  }
}

async function writeCache(file: string, value: CachedResponse): Promise<void> {
  await fs.mkdir(path.dirname(file), { recursive: true });

  const tempFile = `${file}.${process.pid}.tmp`;
  await fs.writeFile(tempFile, JSON.stringify(value), 'utf8');
  await fs.rename(tempFile, file);
}

function isFresh(cached: CachedResponse, ttl?: number): boolean {
  if (ttl === undefined) {
    return true;
  }

  return Date.now() - cached.cachedAt <= ttl;
}

function isSuccessfulStatus(status: number): boolean {
  return status >= 200 && status < 300;
}

function normalizeHeaders(
  headers:
    | Record<string, string | string[] | number | undefined>
    | ReturnType<ServerResponse['getHeaders']>,
): Record<string, string | string[]> {
  const result: Record<string, string | string[]> = {};

  for (const [name, value] of Object.entries(headers)) {
    if (value === undefined) {
      continue;
    }

    result[name.toLowerCase()] = Array.isArray(value)
      ? value.map(String)
      : String(value);
  }

  return result;
}

function sanitizeResponseHeaders(
  headers: Record<string, string | string[]>,
): Record<string, string | string[]> {
  const result = { ...headers };

  delete result['content-length'];
  delete result['transfer-encoding'];

  return result;
}

function extractWriteHeadHeaders(
  [
    _statusCode,
    statusMessageOrHeaders,
    headers,
  ]: Parameters<ServerResponse['writeHead']>,
): Record<string, string | string[] | number | undefined> | undefined {
  if (typeof statusMessageOrHeaders === 'string') {
    return headers as
      | Record<string, string | string[] | number | undefined>
      | undefined;
  }

  return statusMessageOrHeaders as
    | Record<string, string | string[] | number | undefined>
    | undefined;
}

function toBuffer(value: unknown, encoding?: BufferEncoding): Buffer {
  if (Buffer.isBuffer(value)) {
    return value;
  }

  if (value instanceof Uint8Array) {
    return Buffer.from(value);
  }
  return Buffer.from(String(value), encoding ?? 'utf8');
}

function sanitize(value: string): string {
  return value.replace(/[^a-zA-Z0-9._-]/g, '_');
}

function sendJson(
  response: ServerResponse,
  status: number,
  body: Record<string, unknown>,
): void {
  const content = Buffer.from(JSON.stringify(body));

  response.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': String(content.byteLength),
  });
  response.end(content);
}
