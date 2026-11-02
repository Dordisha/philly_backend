import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import { mkdtempSync, rmSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { randomUUID, generateKeyPairSync, verify } from 'node:crypto';
import { notificationRouter } from '../src/routes/notifications.js';
import { PushStore } from '../src/services/pushStore.js';
import { appPolicy, compareVersions } from '../src/services/appPolicy.js';
import { providerToken } from '../src/services/apns.js';

test('required policy is opt-in and invalid minimum never locks users',()=>{
 assert.equal(appPolicy({}).required,false);
 assert.equal(appPolicy({IOS_REQUIRE_UPDATE:'true',IOS_MINIMUM_VERSION:'2.0',IOS_RELEASED_VERSION:'1.1'}).required,false);
 assert.equal(appPolicy({IOS_REQUIRE_UPDATE:'true',IOS_MINIMUM_VERSION:'1.1',IOS_RELEASED_VERSION:'1.2'}).required,true);
 assert.equal(appPolicy({IOS_REQUIRE_UPDATE:'true',IOS_MINIMUM_VERSION:'oops'}).required,false);
 assert.equal(compareVersions('1.10','1.9'),1);
});
test('APNs JWT is P-256 ES256 and uses the correct team/key',()=>{
 const {privateKey,publicKey}=generateKeyPairSync('ec',{namedCurve:'prime256v1'});
 const jwt=providerToken({APNS_KEY_ID:'ABCDEFGHIJ',APNS_TEAM_ID:'1234567890',APNS_PRIVATE_KEY:privateKey.export({type:'pkcs8',format:'pem'})},1000);
 const [h,b,s]=jwt.split('.');
 assert.deepEqual(JSON.parse(Buffer.from(h,'base64url')),{alg:'ES256',kid:'ABCDEFGHIJ'});
 assert.deepEqual(JSON.parse(Buffer.from(b,'base64url')),{iss:'1234567890',iat:1000});
 assert.equal(verify('sha256',Buffer.from(`${h}.${b}`),{key:publicKey,dsaEncoding:'ieee-p1363'},Buffer.from(s,'base64url')),true);
});
test('device ownership, encrypted persistence, opt-out, environment isolation, admin preview and idempotency',async()=>{
 const folder=mkdtempSync(join(tmpdir(),'philly-push-')),file=join(folder,'push.json');
 const store=new PushStore(file,'a'.repeat(64));
 const env={PUSH_REGISTRATION_ENABLED:'true',PUSH_SENDING_ENABLED:'true',PUSH_ADMIN_TOKEN:'admin'.repeat(10)};
 const sent=[];
 const app=express();app.use(express.json());app.use(notificationRouter({env,suppliedStore:store,send:async(t,e,c)=>{sent.push({t,e,c});return {status:200};}}));
 const server=app.listen(0,'127.0.0.1');await new Promise(r=>server.once('listening',r));
 const base=`http://127.0.0.1:${server.address().port}`;
 const request=async(path,method='GET',body,headers={})=>{const r=await fetch(base+path,{method,headers:{'Content-Type':'application/json',...headers},body:body?JSON.stringify(body):undefined});return {status:r.status,body:await r.json()};};
 const admin={Authorization:'Bearer '+env.PUSH_ADMIN_TOKEN};
 const id=randomUUID(),second=randomUUID(),secret='b'.repeat(64),token='c'.repeat(64);
 try{
  assert.equal((await request('/admin/announcements','POST',{title:'Hi',body:'Update',environment:'sandbox'})).status,401);
  const register={token,environment:'sandbox',version:'1.1.0'};
  assert.equal((await request('/devices/'+id,'PUT',register,{'X-Installation-Secret':secret})).status,200);
  assert.equal(readFileSync(file,'utf8').includes(token),false);
  assert.equal(new PushStore(file,'a'.repeat(64)).decrypt(store.data.devices[id].token),token);
  assert.equal((await request('/devices/'+id,'PUT',register,{'X-Installation-Secret':'d'.repeat(64)})).status,403);
  assert.equal((await request('/devices/'+second,'PUT',register,{'X-Installation-Secret':secret})).status,409);
  const prod=await request('/admin/announcements','POST',{title:'Release',body:'New version',environment:'production'},admin);
  assert.equal(prod.body.recipients,0);
  const draft=await request('/admin/announcements','POST',{title:'Release',body:'New version',environment:'sandbox'},admin);
  assert.equal(draft.body.recipients,1);assert.equal(sent.length,0);
  assert.equal((await request(`/admin/announcements/${draft.body.id}/send`,'POST',{},admin)).status,400);
  const done=await request(`/admin/announcements/${draft.body.id}/send`,'POST',{confirm:draft.body.id},admin);
  assert.equal(done.body.results.accepted,1);assert.equal(sent.length,1);
  assert.equal((await request(`/admin/announcements/${draft.body.id}/send`,'POST',{confirm:draft.body.id},admin)).status,409);
  const pending=await request('/admin/announcements','POST',{title:'Release',body:'New version',environment:'sandbox'},admin);
  await request('/devices/'+id,'DELETE',undefined,{'X-Installation-Secret':secret});
  const skipped=await request(`/admin/announcements/${pending.body.id}/send`,'POST',{confirm:pending.body.id},admin);
  assert.equal(skipped.body.results.skipped,1);assert.equal(sent.length,1);
 }finally{await new Promise(r=>server.close(r));rmSync(folder,{recursive:true,force:true});}
});

test('registration and sending are disabled by default; malformed registration and stale APNs tokens',async()=>{
 const folder=mkdtempSync(join(tmpdir(),'philly-push-')),store=new PushStore(join(folder,'push.json'),'a'.repeat(64));
 const env={PUSH_ADMIN_TOKEN:'z'.repeat(40)};
 const app=express();app.use(express.json());app.use(notificationRouter({env,suppliedStore:store,send:async()=>({status:410,reason:'Unregistered'})}));
 const server=app.listen(0,'127.0.0.1');await new Promise(r=>server.once('listening',r));
 const base=`http://127.0.0.1:${server.address().port}`;
 const req=async(path,method,body,headers={})=>{const r=await fetch(base+path,{method,headers:{'Content-Type':'application/json',...headers},body:JSON.stringify(body)});return {status:r.status,body:await r.json()};};
 const id=randomUUID(),secret='b'.repeat(64),admin={Authorization:'Bearer '+env.PUSH_ADMIN_TOKEN};
 try{
  assert.equal((await req('/devices/'+id,'PUT',{},{})).status,503);
  env.PUSH_REGISTRATION_ENABLED='true';
  assert.equal((await req('/devices/'+id,'PUT',{token:'bad'}, {'X-Installation-Secret':secret})).status,400);
  await req('/devices/'+id,'PUT',{token:'f'.repeat(64),environment:'sandbox',version:'1.1.0'},{'X-Installation-Secret':secret});
  const draft=await req('/admin/announcements','POST',{title:'Update',body:'Test only',environment:'sandbox'},admin);
  assert.equal((await req(`/admin/announcements/${draft.body.id}/send`,'POST',{confirm:draft.body.id},admin)).status,503);
  env.PUSH_SENDING_ENABLED='true';
  const sent=await req(`/admin/announcements/${draft.body.id}/send`,'POST',{confirm:draft.body.id},admin);
  assert.equal(sent.body.results.rejected,1);assert.equal(store.data.devices[id],undefined);
 }finally{await new Promise(r=>server.close(r));rmSync(folder,{recursive:true,force:true});}
});
