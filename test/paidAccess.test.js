import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import express from 'express';
import { resolvePaidAccess, requirePaidAccess } from '../src/services/paidAccess.js';
import { createRecordsRouter } from '../src/routes/records.js';
import { createCrimeRouter } from '../src/routes/crime.js';
import { createViolationsRouter } from '../src/routes/violations.js';
import { createComplaintsRouter } from '../src/routes/complaints.js';
import { createOpaRouter } from '../src/routes/opa.js';
import { ownerRouter } from '../src/routes/owners.js';

const token = 'a'.repeat(43); // Test fixture only, never provisioned.
const authorization = 'Bearer ' + token;
const now = Date.parse('2026-11-09T11:00:00Z');
const baseGrant = {
  token_sha256: createHash('sha256').update(token).digest('hex'),
  subject: 'fixture-account', tier: 'pro', source: 'complimentary',
  expires_at: '2026-12-01T00:00:00Z',
};
const property = {
  parcel_number: '363258000', location: '1539 S LAMBERT ST', zip_code: '19146',
  owner_1: 'FIXTURE OWNER', total_livable_area: 1400,
  building_code_new: '25', category_code: '1', latitude: 39.93, longitude: -75.17,
};
const routes = [
  ['/api/records/311', 'pro'], ['/api/records/violations', 'pro'],
  ['/api/violations/details', 'pro'], ['/api/complaints/details', 'pro'],
  ['/api/owners', 'pro'], ['/api/records/sales', 'ai'],
  ['/api/records/facts', 'ai'], ['/api/crime', 'ai'],
];

async function withApi(authorize, run) {
  let queries = 0;
  const query = async sql => {
    queries++;
    if (sql.includes('max(dispatch_date)')) return [{ latest_date: '2026-11-08' }];
    if (sql.includes('FROM incidents_part1_part2')) return [];
    if (sql.includes('FROM opa_properties_public')) return [property];
    return [];
  };
  const options = { query, ...(authorize ? { authorize } : {}) };
  const app = express();
  app.use('/api/records', createRecordsRouter(options));
  app.use('/api/crime', createCrimeRouter({ ...options, now: () => new Date(now) }));
  app.use('/api/violations', createViolationsRouter(options));
  app.use('/api/complaints', createComplaintsRouter({ ...options, query: async sql => ({ rows: await query(sql) }) }));
  app.use('/api/owners', ownerRouter({ ...options, search: async () => {
    queries++;
    return { rows: [property], next_cursor: null };
  }}));
  app.use('/api/opa', createOpaRouter({ query }));
  const server = app.listen(0, '127.0.0.1');
  await new Promise(resolve => server.once('listening', resolve));
  const get = async (path, headers = {}) => {
    const res = await fetch(`http://127.0.0.1:${server.address().port}${path}?opa=363258000&name=Fixture%20Owner&tier=ai&PRO_PREVIEW=true&INTERNAL_TESTING=true&offset=25`, { headers });
    return { status: res.status, cache: res.headers.get('cache-control'), body: await res.json() };
  };
  try { await run(get, () => queries); }
  finally { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); }
}

const resolve = grants => async (auth, tier) => resolvePaidAccess(auth, tier, { registry: JSON.stringify(grants()), now });

test('all paid routes deny unauthenticated/spoofed/preview requests before upstream I/O by default', async () => {
  await withApi(undefined, async (get, queries) => {
    for (const [path, tier] of routes) {
      for (const headers of [{}, { Authorization: 'Bearer forged', 'X-Pro': 'true', 'X-Tier': 'ai', 'X-Preview': 'true' }]) {
        const r = await get(path, headers);
        assert.equal(r.status, 403, path);
        assert.equal(r.body.code, tier + '_required', path);
        assert.equal(r.cache, 'no-store', path);
        assert.equal(r.body.rows, undefined, path);
        assert.equal(queries(), 0, path);
      }
      // Express also accepts case variants and trailing slashes: same gate.
      assert.equal((await get(path.toUpperCase() + '/')).status, 403, path);
    }
    assert.equal(queries(), 0);
  });
});

test('server Pro grants open Pro details only; AI grants include Pro without moving crime into Pro', async () => {
  for (const tier of ['pro', 'ai']) {
    await withApi(resolve(() => [{ ...baseGrant, tier }]), async (get, queries) => {
      for (const [path, required] of routes) {
        const before = queries();
        const allowed = required === 'pro' || tier === 'ai';
        const r = await get(path, { Authorization: authorization });
        assert.equal(r.status, allowed ? 200 : 403, `${tier}: ${path}`);
        assert.equal(r.cache, 'no-store', path);
        if (allowed) assert.ok(queries() > before, path);
        else assert.equal(queries(), before, path);
      }
    });
  }
});

test('expired, revoked, free and unverified grants deny every paid path', async () => {
  for (const change of [
    { expires_at: new Date(now).toISOString() }, { revoked: true },
    { tier: 'free' }, { subject: '' }, { source: 'subscription' },
  ]) {
    await withApi(resolve(() => [{ ...baseGrant, ...change }]), async (get, queries) => {
      for (const [path] of routes) assert.equal((await get(path, { Authorization: authorization })).status, 403, path);
      assert.equal(queries(), 0);
    });
  }
});

test('access is resolved again for later pages after server revocation', async () => {
  let grant = { ...baseGrant, tier: 'ai' };
  await withApi(resolve(() => [grant]), async (get, queries) => {
    for (const [path] of routes) assert.equal((await get(path, { Authorization: authorization })).status, 200, path);
    const before = queries();
    grant = { ...grant, revoked: true };
    for (const [path] of routes) assert.equal((await get(path, { Authorization: authorization })).status, 403, path);
    assert.equal(queries(), before);
  });
});

test('asynchronous denial, malformed approval and verifier failure never release paid data', async () => {
  for (const [authorize, status] of [
    [async () => false, 403], [async () => ({ tier: 'ai' }), 403],
    [async () => { throw Error('private verification details'); }, 503],
    [(auth, tier) => resolvePaidAccess(auth, tier, { registry: '{bad' }), 503],
  ]) {
    await withApi(authorize, async (get, queries) => {
      for (const [path] of routes) {
        const r = await get(path, { Authorization: authorization });
        assert.equal(r.status, status, path);
        assert.equal(r.cache, 'no-store', path);
        assert.equal(r.body.rows, undefined, path);
        assert.ok(!JSON.stringify(r.body).includes('private verification'));
      }
      assert.equal(queries(), 0);
    });
  }
});

test('Free exact-property lookup, ownership copy fields and summaries do not require paid access', async () => {
  await withApi(() => { throw Error('Free must not call authorization'); }, async get => {
    const lookup = await get('/api/opa/search');
    assert.equal(lookup.status, 200);
    assert.equal(lookup.body.result.opa, property.parcel_number);
    assert.equal(lookup.body.result.address, property.location);
    assert.match(lookup.body.result.owner, /FIXTURE OWNER/);
    for (const path of ['/api/violations/summary', '/api/complaints/summary']) {
      const r = await get(path);
      assert.equal(r.status, 200, path);
      assert.equal(r.body.opa, property.parcel_number, path);
      assert.equal(r.body.rows, undefined, path);
    }
  });
});

test('trusted subscription records obey tier boundaries; unknown required tiers deny', () => {
  const subscription = { ...baseGrant, source: 'subscription', verified_transaction_id: 'fixture-only', verified_at: '2026-11-08' };
  const options = { registry: JSON.stringify([subscription]), now };
  assert.equal(resolvePaidAccess(authorization, 'pro', options), true);
  assert.equal(resolvePaidAccess(authorization, 'ai', options), false);
  assert.equal(resolvePaidAccess(authorization, 'admin', options), false);
});


test('conflicting token records fail closed in either order on every paid route', async () => {
  const active = { ...baseGrant, tier: 'ai' };
  for (const conflict of [{ ...active, revoked: true }, { ...active, subject: 'other-account' }, active]) {
    for (const grants of [[active, conflict], [conflict, active]]) {
      await withApi(resolve(() => grants), async (get, queries) => {
        for (const [path] of routes) {
          const response = await get(path, { Authorization: authorization });
          assert.equal(response.status, 503, path);
          assert.equal(response.cache, 'no-store', path);
          assert.ok(!JSON.stringify(response.body).includes('account'));
        }
        assert.equal(queries(), 0);
      });
    }
  }
});

test('malformed revocation, identity and subscription timestamps cannot authorize data', async () => {
  for (const change of [
    { revoked: 'true' }, { revoked: 'false' }, { revoked: 0 }, { revoked: null },
    { subject: {} }, { subject: ['account'] }, { subject: '  ' },
    { expires_at: ['2030-01-01'] },
    { source: 'subscription', verified_transaction_id: {}, verified_at: '2026-11-08' },
    { source: 'subscription', verified_transaction_id: '  ', verified_at: '2026-11-08' },
    { source: 'subscription', verified_transaction_id: 'fixture', verified_at: ['2026-11-08'] },
    { source: 'subscription', verified_transaction_id: 'fixture', verified_at: new Date(now + 1).toISOString() },
  ]) {
    await withApi(resolve(() => [{ ...baseGrant, tier: 'ai', ...change }]), async (get, queries) => {
      for (const [path] of routes) assert.equal((await get(path, { Authorization: authorization })).status, 403, path);
      assert.equal(queries(), 0);
    });
  }
});

test('valid explicit unrevoked subscription and unrelated grants remain usable', () => {
  const grant = { ...baseGrant, revoked: false, source: 'subscription', verified_transaction_id: 'fixture', verified_at: new Date(now).toISOString() };
  const options = { registry: JSON.stringify([null, { ...baseGrant, token_sha256: 'b'.repeat(64), revoked: true }, grant]), now };
  assert.equal(resolvePaidAccess(authorization, 'pro', options), true);
  assert.equal(resolvePaidAccess(authorization, 'pro', { ...options, now: NaN }), false);
  assert.equal(resolvePaidAccess({ toString: () => authorization }, 'pro', options), false);
});


for (const tier of ['pro', 'ai']) {
  test(`${tier} stalled verification returns unavailable and ignores late approval/rejection`, async () => {
    for (const lateResult of ['approve', 'reject']) {
      let approve, reject, queries = 0, responses = 0;
      const decision = new Promise((yes, no) => { approve = yes; reject = no; });
      const headers = {};
      const res = {
        set(key, value) { headers[key] = value; },
        status(code) { this.statusCode = code; return this; },
        json(body) { this.body = body; responses++; return this; },
      };
      await requirePaidAccess(tier, () => decision, { timeoutMs: 10 })(
        { get: () => authorization }, res, () => { queries++; });
      assert.equal(res.statusCode, 503);
      assert.equal(headers['Cache-Control'], 'no-store');
      assert.deepEqual(res.body, { ok: false, message: 'Access verification unavailable. Please retry.' });
      assert.equal(queries, 0);
      if (lateResult === 'approve') approve(true);
      else reject(new Error('private verifier details'));
      await new Promise(resolve => setImmediate(resolve));
      assert.equal(queries, 0);
      assert.equal(responses, 1);
    }
  });
}

test('verification deadline cannot be disabled or extended by invalid configuration', () => {
  for (const timeoutMs of [0, -1, NaN, Infinity, '10', 10001, 1.5]) {
    assert.throws(() => requirePaidAccess('pro', () => true, { timeoutMs }), TypeError);
  }
});
