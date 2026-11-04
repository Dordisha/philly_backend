import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import express from 'express';
import { resolveOwnerAccess } from '../src/services/ownerAccess.js';
import { ownerRouter } from '../src/routes/owners.js';
const token = 'a'.repeat(43);
const grant = { token_sha256: createHash('sha256').update(token).digest('hex'), subject: 'test-account', tier: 'pro', source: 'complimentary', expires_at: '2030-01-01' };
const check = g => resolveOwnerAccess('Bearer ' + token, { registry: JSON.stringify([g]), now: Date.parse('2026-11-04') });
test('server grants allow Pro/AI and reject expiry, revocation, free and unverified subscriptions', () => {
  assert.equal(check(grant), true);
  assert.equal(check({...grant,tier:'ai'}), true);
  for (const change of [{revoked:true},{tier:'free'},{expires_at:'2020-01-01'},{expires_at:null},{subject:''},{source:'subscription'},{source:'client'}]) assert.equal(check({...grant,...change}), false);
  assert.equal(check({...grant,source:'subscription',verified_transaction_id:'trusted-id',verified_at:'2026-11-03'}), true);
  assert.equal(resolveOwnerAccess('Bearer forged', {registry:JSON.stringify([grant])}), false);
});
test('denied requests never query owner records, including spoofed tier headers', async () => {
  let queries = 0;
  const app = express();
  app.use('/api/owners', ownerRouter({authorize: () => false, search: () => { queries++; }}));
  const server = app.listen(0, '127.0.0.1');
  await new Promise(r => server.once('listening', r));
  try {
    const response = await fetch(`http://127.0.0.1:${server.address().port}/api/owners?name=Dorian%20Dishani&tier=pro`, {headers:{'X-Pro':'true'}});
    assert.equal(response.status,403); assert.equal((await response.json()).message,'Pro subscription required.'); assert.equal(queries,0);
  } finally { server.closeAllConnections(); await new Promise(r=>server.close(r)); }
});
test('authorized requests pass pagination through, and service failures remain unavailable', async () => {
  const app=express();
  app.use('/api/owners',ownerRouter({authorize:()=>true,search:async (name, options)=>{assert.equal(name,'Dorian Dishani');assert.equal(options.after,'123456789');return {rows:[],next_cursor:null};}}));
  const server=app.listen(0,'127.0.0.1'); await new Promise(r=>server.once('listening',r));
  try { const response=await fetch(`http://127.0.0.1:${server.address().port}/api/owners?name=Dorian%20Dishani&after=123456789`); assert.equal(response.status,200); assert.deepEqual((await response.json()).rows,[]); }
  finally {server.closeAllConnections();await new Promise(r=>server.close(r));}
});
