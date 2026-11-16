import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import { createViolationsRouter } from '../src/routes/violations.js';
const opa = '363258000';
const row = { opa_account_num: opa, casestatus: 'CLOSED', violationstatus: 'OPEN', total_count: 2, last_activity: '2026-01-01T00:00:00Z' };
async function api(query, run) {
  const app = express();
  app.use(createViolationsRouter({ query, authorize: () => assert.fail('summary must stay Free') }));
  const server = app.listen(0, '127.0.0.1');
  await new Promise(resolve => server.once('listening', resolve));
  const get = async (params = `opa=${opa}`) => {
    const res = await fetch(`http://127.0.0.1:${server.address().port}/summary?${params}`);
    return { status: res.status, body: await res.json(), cache: res.headers.get('cache-control') };
  };
  try { await run(get); } finally { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); }
}
test('Free violation summary preserves separate raw statuses without risk or active inference', async () => {
  await api(async sql => {
    assert.match(sql, /WHERE opa_account_num = '363258000'/);
    assert.match(sql, /GROUP BY opa_account_num, casestatus, violationstatus/);
    assert.doesNotMatch(sql, /casestatus <>|casestatus = 'CLOSED'/);
    return [row, { ...row, casestatus: null, violationstatus: 'COMPLIED', total_count: '1', last_activity: null }];
  }, async get => {
    const r = await get();
    assert.equal(r.status, 200); assert.equal(r.cache, 'no-store');
    assert.equal(r.body.opa, opa); assert.equal(r.body.summary_version, 2);
    assert.equal(r.body.total_count, 3);
    assert.equal(r.body.active_count, null); assert.equal(r.body.historical_count, null);
    assert.equal(r.body.risk_level, null); assert.equal(r.body.flags, undefined);
    assert.equal(r.body.status_mapping, 'unverified');
    assert.deepEqual(r.body.status_counts, [{ casestatus: 'CLOSED', violationstatus: 'OPEN', count: 2 }, { casestatus: null, violationstatus: 'COMPLIED', count: 1 }]);
    assert.equal(r.body.last_activity, '2026-01-01T00:00:00.000Z');
    assert.equal(r.body.source.table, 'violations');
    assert.ok(Number.isFinite(Date.parse(r.body.retrieved_at)));
  });
});
test('only valid empty grouped results return zero; outages never imply low risk', async () => {
  await api(async () => [], async get => {
    const r = await get(); assert.equal(r.status, 200);
    assert.equal(r.body.total_count, 0); assert.deepEqual(r.body.status_counts, []);
    assert.equal(r.body.last_activity, null); assert.equal(r.body.risk_level, null);
  });
  for (const status of [undefined, 500, 504]) {
    await api(async () => { throw Object.assign(new Error('private SQL'), { status }); }, async get => {
      const r = await get(); assert.equal(r.status, status === 504 ? 504 : 503);
      assert.equal(r.body.total_count, undefined); assert.equal(r.body.risk_level, undefined);
      assert.doesNotMatch(JSON.stringify(r.body), /private SQL/);
    });
  }
});
test('summary rejects malformed or mismatched groups instead of manufacturing counts', async () => {
  const badRows = [null, {}, [null], [{}], [row, row],
    ...['999999999', null].map(opa_account_num => [{ ...row, opa_account_num }]),
    ...[null, true, '', '1x', '1.5', -1, 0, 1.5, Number.MAX_SAFE_INTEGER + 1].map(total_count => [{ ...row, total_count }]),
    ...[undefined, {}, 1].map(casestatus => [{ ...row, casestatus }]),
    ...[undefined, {}, 1].map(violationstatus => [{ ...row, violationstatus }]),
    ...[undefined, '', 123, 'invalid'].map(last_activity => [{ ...row, last_activity }]),
    [{ ...row, total_count: Number.MAX_SAFE_INTEGER }, { ...row, casestatus: null }]];
  for (const rows of badRows) {
    await api(async () => rows, async get => {
      const r = await get(); assert.equal(r.status, 503, JSON.stringify(rows));
      assert.equal(r.body.total_count, undefined); assert.equal(r.body.risk_level, undefined);
    });
  }
});
test('summary rejects non-exact or repeated OPA parameters before querying', async () => {
  await api(() => assert.fail('no source call'), async get => {
    for (const value of ['', '123456', '1234567890', '363258000%27', '363258000&opa=363258001']) {
      assert.equal((await get(`opa=${value}`)).status, 400, value);
    }
  });
});
