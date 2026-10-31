const ENDPOINT = "https://phl.carto.com/api/v2/sql";

export class PublicDataError extends Error {
  constructor(message, { status = 503, cause } = {}) {
    super(message, { cause });
    this.name = "PublicDataError";
    this.status = status;
    this.code = "PUBLIC_DATA_UNAVAILABLE";
  }
}

export function createCartoQuery({
  fetchImpl = globalThis.fetch,
  timeoutMs = 15000,
  ttlMs = 60000,
  maxEntries = 256,
  now = Date.now,
} = {}) {
  const cache = new Map();
  const pending = new Map();
  return async function query(sql) {
    const cached = cache.get(sql);
    if (cached && cached.expires > now()) return cached.rows;
    cache.delete(sql);
    if (pending.has(sql)) return pending.get(sql);
    const request = (async () => {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), timeoutMs);
      try {
        const url = new URL(ENDPOINT);
        url.searchParams.set("q", sql);
        const response = await fetchImpl(url, {
          headers: { Accept: "application/json" },
          signal: controller.signal,
        });
        if (!response.ok) throw new Error(`CARTO HTTP ${response.status}`);
        const data = await response.json();
        if (data.error || !Array.isArray(data.rows)) {
          throw new Error("Invalid CARTO result");
        }
        if (maxEntries > 0) {
          while (cache.size >= maxEntries) cache.delete(cache.keys().next().value);
          cache.set(sql, { rows: data.rows, expires: now() + ttlMs });
        }
        return data.rows;
      } catch (cause) {
        throw new PublicDataError(
          "Property data is temporarily unavailable. Please try again.",
          { status: controller.signal.aborted ? 504 : 503, cause },
        );
      } finally {
        clearTimeout(timer);
      }
    })();
    pending.set(sql, request);
    try {
      return await request;
    } finally {
      pending.delete(sql);
    }
  };
}

export const queryCarto = createCartoQuery();

