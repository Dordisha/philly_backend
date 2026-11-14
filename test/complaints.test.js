import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import { createComplaintsRouter } from '../src/routes/complaints.js';

async function withApi(query, run, authorize = async () => true) {
  const app = express();
  app.use('/api/complaints', createComplaintsRouter({query, authorize}));
  const server = app.listen(0, '127.0.0.1');
  await new Promise(resolve => server.once('listening', resolve));
  const get = async path => {
    const res = await fetch(`http://127.0.0.1:${server.address().port}/api/complaints/${path}`);
    return {status: res.status, body: await res.json()};
  };
  try { await run(get); }
  finally { server.closeAllConnections(); await new Promise(r => server.close(r)); }
}

test('complaint summary preserves unknown and case statuses without inventing open counts or risk', async () => {
  await withApi(async sql => {
    assert.match(sql, /GROUP BY complaintstatus, casestatus/);
    assert.match(sql, /opa_account_num = '363258000'/);
    return {rows: [
      {complaintstatus:'CLOSED', casestatus:'OPEN', total_count:'1', last_activity:'2026-01-01'},
      {complaintstatus:null, casestatus:null, total_count:'2', last_activity:'2026-02-01'},
      {complaintstatus:'UNRECOGNIZED', casestatus:'OTHER', total_count:'1', last_activity:null},
    ]};
  }, async get => {
    const {status,body} = await get('summary?opa=363258000');
    assert.equal(status,200); assert.equal(body.total_count,4);
    assert.equal(body.last_activity,'2026-02-01T00:00:00.000Z');
    assert.equal(body.open_count,null); assert.equal(body.risk_level,null);
    assert.equal(body.status_counts[0].casestatus,'OPEN');
    assert.equal(body.status_counts[1].complaintstatus,null);
    assert.equal(body.status_counts[2].complaintstatus,'UNRECOGNIZED');
  }, () => { throw Error('Summary must remain Free'); });
});

test('empty complaints is zero records with unknown classification; upstream failure is not zero', async () => {
  for (const [query, expected] of [
    [async () => ({rows:[]}),200],
    [async () => { throw Error('offline'); },500],
    [async () => ({}),500],
    [async () => ({rows:[{total_count:'invalid'}]}),500],
    [async () => ({rows:[{total_count:1,last_activity:'invalid'}]}),500],
  ]) await withApi(query, async get => {
    const r = await get('summary?opa=363258000');
    assert.equal(r.status,expected);
    if (expected===200) { assert.equal(r.body.total_count,0); assert.equal(r.body.risk_level,null); }
    else { assert.equal(r.body.ok,false); assert.equal(r.body.total_count,undefined); }
  });
});

test('details preserve raw status aliases and stable bounded pagination with exact parcel identity', async () => {
  await withApi(async sql => {
    assert.match(sql,/complaintstatus AS status/);
    assert.match(sql,/complaintstatus, casestatus/);
    assert.match(sql,/complaintcodename AS complaint/);
    assert.match(sql,/ORDER BY complaintdate DESC NULLS LAST, cartodb_id DESC/);
    assert.match(sql,/LIMIT 1\s+OFFSET 1/);
    return {rows:[{opa_account_num:'363258000',complaintstatus:null,casestatus:'OPEN',status:null}]};
  }, async get => {
    const r = await get('details?opa=363258000&limit=1&offset=1');
    assert.equal(r.status,200); assert.equal(r.body.rows[0].status,null);
    assert.equal(r.body.rows[0].casestatus,'OPEN');
  });
});

test('details reject malformed upstream and wrong-property records', async () => {
  for (const result of [{}, {rows:[{opa_account_num:'999999999'}]}, {rows:[{}]}]) {
    await withApi(async () => result, async get => {
      const r = await get('details?opa=363258000');
      assert.equal(r.status,500); assert.equal(r.body.rows,undefined);
    });
  }
});

test('invalid OPA and pagination never query upstream', async () => {
  await withApi(async () => { assert.fail('Unexpected upstream query'); }, async get => {
    for (const path of ['summary?opa=invalid','details?opa=invalid',
      'details?opa=363258000&limit=NaN','details?opa=363258000&offset=-1',
      'details?opa=363258000&limit=101','details?opa=363258000&offset=1x',
      'details?opa=363258000&offset=1.5']) assert.equal((await get(path)).status,400,path);
  });
});

test('details retain server authorization before any upstream read', async () => {
  await withApi(async () => { assert.fail('Unexpected upstream query'); }, async get => {
    assert.equal((await get('details?opa=363258000')).status,403);
  }, async () => false);
});
