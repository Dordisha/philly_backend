import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import { createViolationsRouter } from '../src/routes/violations.js';
const opa = '363258000';
const row = { cartodb_id: 3, opa_account_num: opa, casenumber: 'CASE', casestatus: null, violationstatus: 'COMPLIED' };
async function api(query, run, authorize = () => true) {
  const app = express();
  app.use(createViolationsRouter({ query, authorize }));
  const server = app.listen(0, '127.0.0.1');
  await new Promise(resolve => server.once('listening', resolve));
  const get = async (params = `opa=${opa}`) => {
    const res = await fetch(`http://127.0.0.1:${server.address().port}/details?${params}`);
    return { status: res.status, body: await res.json(), cache: res.headers.get('cache-control') };
  };
  try { await run(get); } finally { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); }
}
test('legacy L&I details preserve raw statuses and page exact parcel records', async () => {
  const sqls = [];
  await api(async sql => { sqls.push(sql); return sql.includes('OFFSET 1') ? [{ ...row, cartodb_id: 2 }] : [row, { ...row, cartodb_id: 2 }]; }, async get => {
    const first = await get(`opa=${opa}&limit=1`);
    assert.equal(first.status, 200);
    assert.deepEqual(first.body.rows, [row]);
    assert.equal(first.body.count, 1);
    assert.equal(first.body.has_more, true);
    assert.equal(first.body.next_offset, 1);
    assert.equal(first.body.source.table, 'violations');
    assert.ok(Number.isFinite(Date.parse(first.body.retrieved_at)));
    assert.equal(first.cache, 'no-store');
    const second = await get(`opa=${opa}&limit=1&offset=1`);
    assert.equal(second.body.rows[0].cartodb_id, 2);
    assert.equal(second.body.has_more, false);
    assert.equal(second.body.next_offset, null);
    assert.match(sqls[0], /ORDER BY violationdate DESC NULLS LAST, cartodb_id DESC/);
    assert.match(sqls[0], /LIMIT 2 OFFSET 0/);
    assert.match(sqls[1], /LIMIT 2 OFFSET 1/);
    assert.match(sqls[0], /WHERE opa_account_num = '363258000'/);
  });
});
test('legacy L&I rejects malformed pagination and OPA without source calls', async () => {
  await api(() => { assert.fail('must not query'); }, async get => {
    for (const param of ['limit=0', 'limit=501', 'limit=-1', 'limit=1x', 'limit=1.5', 'limit=', 'limit=1&limit=2', 'offset=-1', 'offset=1x', 'offset=', 'offset=1.5', 'offset=9007199254740991']) {
      assert.equal((await get(`opa=${opa}&${param}`)).status, 400, param);
    }
    for (const value of ['', '123456', '1234567890', '363258000%27', '363258000&opa=363258001']) {
      assert.equal((await get(`opa=${value}`)).status, 400, value);
    }
  });
});
test('legacy L&I rejects wrong, missing or duplicated row identity, including lookahead', async () => {
  for (const rows of [null, {}, [null], [{ ...row, opa_account_num: '999999999' }], [{ ...row, opa_account_num: null }], [{ ...row, cartodb_id: null }], [row, row], [row, { ...row, cartodb_id: 2, opa_account_num: '999999999' }], [row, { ...row, cartodb_id: 2 }, { ...row, cartodb_id: 1 }]]) {
    await api(async () => rows, async get => {
      const r = await get(`opa=${opa}&limit=1`);
      assert.equal(r.status, 503);
      assert.equal(r.body.rows, undefined);
      assert.equal(r.body.count, undefined);
    });
  }
});
test('legacy L&I empty results are distinct from upstream failures and timeouts', async () => {
  await api(async () => [], async get => {
    const r = await get(); assert.equal(r.status, 200);
    assert.deepEqual(r.body.rows, []); assert.equal(r.body.count, 0);
    assert.equal(r.body.has_more, false); assert.equal(r.body.next_offset, null);
    assert.equal(r.body.limit, 200);
  });
  for (const status of [undefined, 500, 504]) {
    await api(async () => { throw Object.assign(new Error('private source error'), { status }); }, async get => {
      const r = await get(); assert.equal(r.status, status === 504 ? 504 : 503);
      assert.equal(r.body.rows, undefined); assert.doesNotMatch(JSON.stringify(r.body), /private source/);
    });
  }
});
test('legacy L&I reauthorizes each page before querying the source', async () => {
  let allowed = true, calls = 0;
  await api(async () => { calls++; return [row]; }, async get => {
    assert.equal((await get()).status, 200);
    allowed = false;
    const r = await get(`opa=${opa}&offset=1`);
    assert.equal(r.status, 403); assert.equal(r.body.code, 'pro_required');
    assert.equal(calls, 1); assert.equal(r.cache, 'no-store');
  }, () => allowed);
});
