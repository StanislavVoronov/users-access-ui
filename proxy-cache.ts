import { createHash } from 'node:crypto';
import { promises as fs } from 'node:fs';
import type { IncomingMessage, ServerResponse } from 'node:http';
import path from 'node:path';

export type CacheMode = 'remote' | 'offline' | 'auto';

export interface ProxyCacheOptions {
  mode: CacheMode;
  /** Required for auto. Ignored by remote/offline. Milliseconds. */
  ttl?: number;
}

export interface ProxyEntry {
  pathFilter?: string | RegExp | ((pathname: string, req: IncomingMessage) => boolean);
  target?: string;
  cache?: ProxyCacheOptions;
  on?: Record<string, (...args: any[]) => any>;
  [key: string]: any;
}

export interface CreateProxyCacheOptions {
  cacheDir?: string;
  getUserId: (req: IncomingMessage) => string | undefined | Promise<string | undefined>;
}

type CachedResponse = {
  cachedAt: number;
  request: { method: string; url: string };
  response: {
    status: number;
    headers: Record<string, string | string[]>;
    body: string; // base64
  };
};

const DEFAULT_CACHE_DIR = '.cache/proxy';
const ctx = new WeakMap<IncomingMessage, Promise<RequestContext | undefined>>();

type RequestContext = {
  userId: string;
  key: string;
  file: string;
};

export function createProxyCache<T extends ProxyEntry[]>(
  proxies: T,
  options: CreateProxyCacheOptions,
) {
  validate(proxies);
  const cacheDir = options.cacheDir ?? DEFAULT_CACHE_DIR;

  const proxy = proxies
    .filter((entry) => entry.cache?.mode !== 'offline')
    .map((entry) => enhanceProxy(entry, options, cacheDir));

  const setup = ({ server }: any) => {
    server.middlewares.use(async (req: IncomingMessage, res: ServerResponse, next: (error?: unknown) => void) => {
      try {
        const pathname = getPathname(req);
        const entry = proxies.find((candidate) =>
          candidate.cache?.mode === 'offline' && matchPathFilter(candidate.pathFilter, pathname, req),
        );

        if (!entry?.cache) return next();

        const requestContext = await getRequestContext(req, options, cacheDir);
        if (!requestContext) return cacheMiss(res, 'USER_NOT_FOUND');

        const cached = await readCache(requestContext.file);
        if (!cached) return cacheMiss(res, 'CACHE_MISS');

        // Offline intentionally ignores TTL: it must remain usable without network.
        sendCached(res, cached, 'OFFLINE');
      } catch (error) {
        next(error);
      }
    });
  };

  return { proxy, setup };
}

function enhanceProxy(entry: ProxyEntry, options: CreateProxyCacheOptions, cacheDir: string): ProxyEntry {
  const { cache, on = {}, ...rest } = entry;
  if (!cache) return entry;

  const originalProxyRes = on.proxyRes;
  const originalError = on.error;

  return {
    ...rest,
    on: {
      ...on,

      proxyRes(proxyRes: IncomingMessage, req: IncomingMessage, res: ServerResponse) {
        originalProxyRes?.(proxyRes, req, res);
        void recordResponse(proxyRes, req, options, cacheDir);
      },

      error(error: unknown, req: IncomingMessage, res: ServerResponse, target: unknown) {
        if (cache.mode !== 'auto') {
          if (originalError) return originalError(error, req, res, target);
          return proxyError(res);
        }

        void (async () => {
          const requestContext = await getRequestContext(req, options, cacheDir);
          if (!requestContext) {
            if (originalError) return originalError(error, req, res, target);
            return proxyError(res);
          }

          const cached = await readCache(requestContext.file);
          const ttl = cache.ttl!;

          if (!cached || Date.now() - cached.cachedAt > ttl) {
            if (originalError) return originalError(error, req, res, target);
            return proxyError(res);
          }

          sendCached(res, cached, 'HIT');
        })().catch(() => {
          if (!res.headersSent) proxyError(res);
        });
      },
    },
  };
}

async function recordResponse(
  proxyRes: IncomingMessage,
  req: IncomingMessage,
  options: CreateProxyCacheOptions,
  cacheDir: string,
) {
  const status = proxyRes.statusCode ?? 500;
  if (status < 200 || status >= 300) return;

  const requestContext = await getRequestContext(req, options, cacheDir);
  if (!requestContext) return;

  const chunks: Buffer[] = [];
  proxyRes.on('data', (chunk) => chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)));
  proxyRes.on('end', () => {
    void writeCache(requestContext.file, {
      cachedAt: Date.now(),
      request: { method: req.method ?? 'GET', url: req.url ?? '/' },
      response: {
        status,
        headers: normalizeHeaders(proxyRes.headers),
        body: Buffer.concat(chunks).toString('base64'),
      },
    });
  });
}

async function getRequestContext(
  req: IncomingMessage,
  options: CreateProxyCacheOptions,
  cacheDir: string,
): Promise<RequestContext | undefined> {
  let pending = ctx.get(req);
  if (!pending) {
    pending = (async () => {
      const userId = await options.getUserId(req);
      if (!userId) return undefined;
      const key = createCacheKey(req);
      return {
        userId,
        key,
        file: path.join(cacheDir, sanitize(userId), `${key}.json`),
      };
    })();
    ctx.set(req, pending);
  }
  return pending;
}

function createCacheKey(req: IncomingMessage): string {
  return createHash('sha256')
    .update(req.method ?? 'GET')
    .update('\n')
    .update(req.url ?? '/')
    .digest('hex');
}

function matchPathFilter(filter: ProxyEntry['pathFilter'], pathname: string, req: IncomingMessage): boolean {
  if (!filter) return true;
  if (typeof filter === 'function') return filter(pathname, req);
  if (filter instanceof RegExp) {
    filter.lastIndex = 0;
    return filter.test(pathname);
  }
  return pathname.startsWith(filter);
}

function getPathname(req: IncomingMessage): string {
  return new URL(req.url ?? '/', `http://${req.headers.host ?? 'localhost'}`).pathname;
}

async function writeCache(file: string, value: CachedResponse) {
  await fs.mkdir(path.dirname(file), { recursive: true });
  const tmp = `${file}.${process.pid}.tmp`;
  await fs.writeFile(tmp, JSON.stringify(value), 'utf8');
  await fs.rename(tmp, file);
}

async function readCache(file: string): Promise<CachedResponse | undefined> {
  try {
    return JSON.parse(await fs.readFile(file, 'utf8')) as CachedResponse;
  } catch {
    return undefined;
  }
}

function sendCached(res: ServerResponse, cached: CachedResponse, state: 'HIT' | 'OFFLINE') {
  res.statusCode = cached.response.status;
  for (const [name, value] of Object.entries(cached.response.headers)) {
    const lower = name.toLowerCase();
    if (lower === 'content-length' || lower === 'transfer-encoding' || lower === 'content-encoding') continue;
    res.setHeader(name, value);
  }
  const body = Buffer.from(cached.response.body, 'base64');
  res.setHeader('content-length', body.length);
  res.setHeader('x-proxy-cache', state);
  res.setHeader('x-proxy-cache-age', Math.max(0, Math.floor((Date.now() - cached.cachedAt) / 1000)));
  res.end(body);
}

function cacheMiss(res: ServerResponse, reason: string) {
  res.statusCode = 503;
  res.setHeader('content-type', 'application/json; charset=utf-8');
  res.setHeader('x-proxy-cache', 'MISS');
  res.end(JSON.stringify({ error: 'PROXY_CACHE_MISS', reason }));
}

function proxyError(res: ServerResponse) {
  if (res.headersSent) return;
  res.statusCode = 502;
  res.setHeader('content-type', 'application/json; charset=utf-8');
  res.end(JSON.stringify({ error: 'PROXY_ERROR' }));
}

function normalizeHeaders(headers: IncomingMessage['headers']): Record<string, string | string[]> {
  const result: Record<string, string | string[]> = {};
  for (const [name, value] of Object.entries(headers)) {
    if (value !== undefined) result[name] = value;
  }
  return result;
}

function sanitize(value: string) {
  return value.replace(/[^a-zA-Z0-9._-]/g, '_');
}

function validate(proxies: ProxyEntry[]) {
  for (const proxy of proxies) {
    if (proxy.cache?.mode === 'auto' && (!proxy.cache.ttl || proxy.cache.ttl <= 0)) {
      throw new Error('proxy.cache.ttl must be > 0 when mode is "auto"');
    }
  }
}
