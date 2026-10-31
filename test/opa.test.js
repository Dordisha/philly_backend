import test from "node:test";
import assert from "node:assert/strict";
import express from "express";
import { createViolationsRouter } from "../src/routes/violations.js";
import { createOpaRouter } from "../src/routes/opa.js";
import { createCartoQuery, PublicDataError } from "../src/lib/carto.js";
import {
  normalizeAddress, propertyByOpaSql, exactAddressSql, suggestionsSql, toProperty,
} from "../src/lib/propertyData.js";

const row = {
  parcel_number: "363258000", location: "1539 S LAMBERT ST",
  owner_1: "DISHANI DORIAN", owner_2: null, market_value: 447100,
  sale_price: 395000, sale_date: "2020-03-17T04:00:00Z", zoning: "RSA5", zip_code: "19146",
};
async function withApi(query, run) {
  const app = express();
  app.use("/api/opa", createOpaRouter({ query }));
  const server = app.listen(0, "127.0.0.1");
  await new Promise((resolve) => server.once("listening", resolve));
  try {
    await run("http://127.0.0.1:" + server.address().port + "/api/opa");
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
}
async function get(base, path) {
  const response = await fetch(base + path);
  return { status: response.status, body: await response.json() };
}

test("normalizes the shipped City Hall address, ZIP and street designation", () => {
  assert.deepEqual(normalizeAddress("1400 John F Kennedy Boulevard, Philadelphia, PA 19107"), {
    address: "1400 JOHN F KENNEDY BLVD", zip: "19107",
  });
  assert.equal(normalizeAddress("12345 MARKET ST").address, "12345 MARKET ST");
  assert.equal(normalizeAddress("1539 S Lambert St Philadelphia PA").address, row.location);
  assert.equal(normalizeAddress("10 MAIN ST APT 2").address, "10 MAIN ST UNIT 2");
});
test("OPA validation and SQL literals reject injection or quote text safely", () => {
  assert.throws(() => propertyByOpaSql("363258000' OR TRUE--"));
  const sql = exactAddressSql("123 O'BRIEN ST");
  assert.ok(sql.includes("O''BRIEN"));
  assert.ok(sql.includes("LIMIT 2"));
  assert.equal(suggestionsSql("9999999999999999999 TEST"), null);
});
test("suggestions keep direction, exact house, range and explicit block fallback", () => {
  const exact = suggestionsSql("524 S MAR", 999);
  assert.ok(exact.includes("= 'S'"));
  assert.ok(exact.includes("= 524"));
  assert.ok(exact.includes("split_part(location"));
  assert.ok(exact.endsWith("LIMIT 25"));
  assert.ok(suggestionsSql("526 MARK", 10, { block: true }).includes("BETWEEN 500 AND 599"));
  assert.equal(suggestionsSql("MA"), null);
});
test("mapping preserves all fields the Flutter client consumes", () => {
  assert.deepEqual(toProperty(row), {
    opa: row.parcel_number, address: row.location, owner: row.owner_1,
    market_value: 447100, sale_price: 395000, sale_date: "2020-03-17",
    zoning: "RSA5", tax: { lookup_url: "https://tax-services.phila.gov/_/" },
  });
  assert.equal(toProperty({ ...row, market_value: null }).market_value, null);
});
test("search and legacy flattened detail maintain their existing response shapes", async () => {
  const calls = [];
  await withApi(async (sql) => { calls.push(sql); return [row]; }, async (base) => {
    for (const path of ["/search?opa=363258000", "/search?address=1539%20S%20Lambert%20St"]) {
      const result = await get(base, path);
      assert.equal(result.status, 200);
      assert.equal(result.body.result.opa, row.parcel_number);
      assert.equal(result.body.result.zoning, "RSA5");
    }
    const detail = await get(base, "?opa=363258000");
    assert.equal(detail.body.opa, row.parcel_number);
    assert.equal(detail.body.result, undefined);
  });
  assert.equal(calls.length, 3);
});
test("suggest accepts q/query aliases; failures are not successful empty lists", async () => {
  await withApi(async () => [row], async (base) => {
    const a = await get(base, "/suggest?q=1539");
    const b = await get(base, "/suggest?query=1539");
    assert.deepEqual(a, b);
    assert.equal(a.body.suggestions[0].opa, row.parcel_number);
  });
  await withApi(async () => { throw new PublicDataError("upstream failed"); }, async (base) => {
    for (const path of ["/suggest?q=1539", "/search?opa=363258000"]) {
      const result = await get(base, path);
      assert.equal(result.status, 503);
      assert.equal(result.body.ok, false);
      assert.equal(result.body.code, "PUBLIC_DATA_UNAVAILABLE");
    }
  });
});
test("nonexistent typed addresses never trigger legacy automatic substitution", async () => {
  await withApi(async () => [], async (base) => {
    const result = await get(base, "/search?address=999999%20FAKE%20ST");
    assert.equal(result.status, 404);
    assert.equal(result.body.code, "ADDRESS_NOT_FOUND");
    assert.deepEqual(result.body.suggestions, []);
  });
});
test("ambiguous parcels require selection and invalid OPA never queries upstream", async () => {
  let calls = 0;
  await withApi(async () => { calls++; return [row, { ...row, parcel_number: "123456789" }]; }, async (base) => {
    assert.equal((await get(base, "/search?opa=bad")).status, 400);
    assert.equal(calls, 0);
    const result = await get(base, "/search?address=1539%20S%20LAMBERT%20ST");
    assert.equal(result.status, 409);
    assert.equal(result.body.code, "ADDRESS_AMBIGUOUS");
  });
});
test("Carto client caches successful requests, deduplicates in-flight calls, expires entries", async () => {
  let calls = 0;
  let time = 100;
  const query = createCartoQuery({
    now: () => time, ttlMs: 10,
    fetchImpl: async () => {
      calls++;
      await new Promise((resolve) => setTimeout(resolve, 5));
      return { ok: true, json: async () => ({ rows: [row] }) };
    },
  });
  await Promise.all([query("SELECT"), query("SELECT")]);
  await query("SELECT");
  assert.equal(calls, 1);
  time += 11;
  await query("SELECT");
  assert.equal(calls, 2);
});
test("Carto JSON/HTTP failures are not cached as empty properties", async () => {
  for (const response of [
    { ok: false, status: 500 },
    { ok: true, json: async () => ({ error: ["missing table"] }) },
    { ok: true, json: async () => ({}) },
    { ok: true, json: async () => { throw new SyntaxError("HTML"); } },
  ]) {
    let calls = 0;
    const query = createCartoQuery({ fetchImpl: async () => { calls++; return response; } });
    await assert.rejects(query("SELECT"), PublicDataError);
    await assert.rejects(query("SELECT"), PublicDataError);
    assert.equal(calls, 2);
  }
});
test("Carto requests abort at the deadline", async () => {
  const query = createCartoQuery({
    timeoutMs: 10,
    fetchImpl: async (_url, { signal }) => new Promise((_resolve, reject) => {
      signal.addEventListener("abort", () => reject(new Error("aborted")), { once: true });
    }),
  });
  await assert.rejects(query("SELECT"), (e) => e instanceof PublicDataError && e.status === 504);
});
test("Carto cache has bounded retention", async () => {
  let calls = 0;
  const query = createCartoQuery({
    maxEntries: 1,
    fetchImpl: async () => { calls++; return { ok: true, json: async () => ({ rows: [] }) }; },
  });
  await query("A"); await query("B"); await query("A");
  assert.equal(calls, 3);
});



test("enforcement fails visibly on upstream errors and rejects invalid OPA", async () => {
  const app = express();
  let calls = 0;
  app.use('/api/violations', createViolationsRouter({query: async () => {
    calls++;
    throw new PublicDataError('upstream failed');
  }}));
  const server = app.listen(0, '127.0.0.1');
  await new Promise(resolve => server.once('listening', resolve));
  try {
    const base = 'http://127.0.0.1:' + server.address().port;
    assert.equal((await get(base, '/api/violations/summary?opa=bad')).status, 400);
    assert.equal(calls, 0);
    const result = await get(base, '/api/violations/summary?opa=363258000');
    assert.equal(result.status, 503);
    assert.equal(result.body.ok, false);
    assert.equal(result.body.risk_level, undefined);
  } finally { await new Promise(resolve => server.close(resolve)); }
});
