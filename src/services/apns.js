import http2 from 'node:http2';
import { createPrivateKey, sign } from 'node:crypto';
import { STORE_URL } from './appPolicy.js';
export function providerToken(env=process.env, now=Math.floor(Date.now()/1000)) {
  if (!/^[A-Z0-9]{10}$/.test(env.APNS_KEY_ID||'') || !/^[A-Z0-9]{10}$/.test(env.APNS_TEAM_ID||'')) throw new Error('APNs identity missing');
  const head=Buffer.from(JSON.stringify({alg:'ES256',kid:env.APNS_KEY_ID})).toString('base64url');
  const body=Buffer.from(JSON.stringify({iss:env.APNS_TEAM_ID,iat:now})).toString('base64url');
  const key=createPrivateKey((env.APNS_PRIVATE_KEY||'').replaceAll('\\n','\n'));
  if(key.asymmetricKeyType!=='ec' || key.asymmetricKeyDetails?.namedCurve!=='prime256v1') throw new Error('APNs requires P-256');
  return `${head}.${body}.`+sign('sha256',Buffer.from(`${head}.${body}`),{key,dsaEncoding:'ieee-p1363'}).toString('base64url');
}
export function sendAPNs(token, environment, campaign, env=process.env) {
  if(env.PUSH_SENDING_ENABLED!=='true') throw new Error('Sending disabled');
  if(!['sandbox','production'].includes(environment) || !/^[a-f0-9]{32,256}$/i.test(token)) throw new Error('Invalid device');
  const jwt=providerToken(env);
  const payload=JSON.stringify({aps:{alert:{title:campaign.title,body:campaign.body},sound:'default'},
    kind:'app_update',url:STORE_URL,campaignId:campaign.id});
  return new Promise((resolve,reject)=>{
    const host=environment==='sandbox'?'api.sandbox.push.apple.com':'api.push.apple.com';
    const client=http2.connect(`https://${host}`);
    let settled=false, status=0, response='';
    const finish=(error,value)=>{if(settled)return;settled=true;clearTimeout(timer);client.destroy();error?reject(error):resolve(value);};
    const timer=setTimeout(()=>finish(new Error('APNs timeout')),10000);
    client.on('error',()=>finish(new Error('APNs connection failed')));
    const req=client.request({':method':'POST',':path':`/3/device/${token}`,
      authorization:`bearer ${jwt}`,'apns-topic':'com.dorian.phillyscan',
      'apns-push-type':'alert','apns-priority':'10','apns-expiration':'0',
      'apns-collapse-id':campaign.id});
    req.on('response',h=>{status=h[':status'];});
    req.on('data',chunk=>{if(response.length<4096)response+=chunk;});
    req.on('error',()=>finish(new Error('APNs request failed')));
    req.on('end',()=>{let reason='';try{reason=JSON.parse(response).reason||'';}catch{}
      finish(null,{status,reason});});
    req.end(payload);
  });
}
