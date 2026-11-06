import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import { ownerRouter } from '../src/routes/owners.js';

async function withServer(search, run) {
  const app = express();
  app.use('/api/owners', ownerRouter({ search }));
  const server = app.listen(0, '127.0.0.1');
  await new Promise(resolve => server.once('listening', resolve));
  try { await run(`http://127.0.0.1:${server.address().port}/api/owners`); }
  finally { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); }
}

test('owner search is free without authorization and retains bounded pagination', async () => {
  await withServer(async (name, options) => {
    assert.equal(name, 'TEST OWNER');
    assert.deepEqual(options, { limit: 25, after: '123456789' });
    return { rows: [{ parcel_number: '123456790' }], next_cursor: null };
  }, async url => {
    const response = await fetch(url + '?name=TEST%20OWNER&after=123456789&limit=999999');
    assert.equal(response.status, 200);
    assert.equal(response.headers.get('cache-control'), 'no-store');
    assert.equal((await response.json()).rows.length, 1);
  });
});

test('public owner lookup preserves validation and upstream failure responses', async () => {
  for (const [error, status] of [[new TypeError('invalid'), 400], [new Error('upstream'), 503]]) {
    await withServer(async () => { throw error; }, async url => {
      const response = await fetch(url + '?name=x');
      assert.equal(response.status, status);
      assert.equal((await response.json()).ok, false);
    });
  }
});
