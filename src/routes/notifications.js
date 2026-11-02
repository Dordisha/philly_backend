import express from 'express';
import { randomUUID, timingSafeEqual } from 'node:crypto';
import { PushStore, digest } from '../services/pushStore.js';
import { sendAPNs, providerToken } from '../services/apns.js';
import { versionParts } from '../services/appPolicy.js';

const same=(a,b)=>typeof a==='string' && typeof b==='string' && a.length===b.length
  && timingSafeEqual(Buffer.from(a),Buffer.from(b));
const validSecret=v=>typeof v==='string' && /^[a-f0-9]{64}$/i.test(v);
const validId=v=>typeof v==='string' && /^[a-f0-9-]{36}$/i.test(v);
export function notificationRouter({env=process.env, suppliedStore, send=sendAPNs, clock=()=>Date.now()}={}) {
  const router=express.Router(), limits=new Map(); let store=suppliedStore;
  router.use((req,res,next)=>{
    if(env.PUSH_REGISTRATION_ENABLED!=='true')return res.status(503).json({error:'Notifications not configured'});
    try { store ||= new PushStore(env.PUSH_STORE_PATH,env.PUSH_STORE_KEY); }
    catch {return res.status(503).json({error:'Notifications storage unavailable'});}
    // Do not trust spoofable forwarded headers. Configure reverse-proxy limiting separately.
    const now=clock(), key=req.ip;
    for(const [k,v] of limits)if(v.until<now)limits.delete(k);
    const item=limits.get(key)||{count:0,until:now+60000};
    if(limits.size>=10000 || ++item.count>120)return res.status(429).json({error:'Try later'});
    limits.set(key,item);next();
  });
  const authenticateDevice=(req,res,next)=>{
    const id=req.params.id, secret=req.get('X-Installation-Secret');
    if(!validId(id)||!validSecret(secret))return res.status(400).json({error:'Invalid installation'});
    const device=store.data.devices[id];
    if(device && !same(device.secretHash,digest(secret)))return res.status(403).json({error:'Invalid installation'});
    req.secretHash=digest(secret);next();
  };
  router.put('/devices/:id',authenticateDevice,(req,res)=>{
    const {token,environment,version}=req.body||{};
    if(typeof token!=='string'||!/^[a-f0-9]{32,256}$/i.test(token)||!['sandbox','production'].includes(environment)||!versionParts(version))
      return res.status(400).json({error:'Invalid registration'});
    const id=req.params.id, tokenHash=digest(environment+token.toLowerCase());
    // A different installation cannot take over an existing token registration.
    if(Object.entries(store.data.devices).some(([k,d])=>k!==id&&d.tokenHash===tokenHash))return res.status(409).json({error:'Registration conflict'});
    if(!store.data.devices[id]&&Object.keys(store.data.devices).length>=10000)return res.status(503).json({error:'Registration capacity reached'});
    store.data.devices[id]={secretHash:req.secretHash,tokenHash,token:store.encrypt(token.toLowerCase()),environment,version,updatedAt:clock()};
    store.save();res.json({ok:true});
  });
  router.delete('/devices/:id',authenticateDevice,(req,res)=>{delete store.data.devices[req.params.id];store.save();res.json({ok:true});});
  router.use('/admin',(req,res,next)=>{
    const configured=env.PUSH_ADMIN_TOKEN;
    if(!configured||configured.length<32||!same(digest(req.get('Authorization')||''),digest('Bearer '+configured)))
      return res.status(401).json({error:'Unauthorized'});
    next();
  });
  const preview=c=>({id:c.id,title:c.title,body:c.body,environment:c.environment,status:c.status,
    recipients:Object.keys(c.recipients).length,results:Object.values(c.recipients).reduce((a,r)=>(a[r.state]=(a[r.state]||0)+1,a),{})});
  router.post('/admin/announcements',(req,res)=>{
    const {title,body,environment}=req.body||{};
    if(typeof title!=='string'||!title.trim()||title.length>80||typeof body!=='string'||!body.trim()||body.length>240||!['sandbox','production'].includes(environment))
      return res.status(400).json({error:'Use title (1–80), body (1–240), environment'});
    if(Object.keys(store.data.campaigns).length>=1000)return res.status(409).json({error:'Archive old announcement history before adding more'});
    const recipients={};
    for(const [id,d] of Object.entries(store.data.devices))if(d.environment===environment && d.updatedAt>clock()-90*86400000)
      recipients[id]={state:'pending',tokenHash:d.tokenHash};
    const c={id:randomUUID(),title:title.trim(),body:body.trim(),environment,status:'draft',createdAt:clock(),recipients};
    store.data.campaigns[c.id]=c;store.save();res.status(201).json(preview(c));
  });
  router.get('/admin/announcements/:id',(req,res)=>{
    const c=store.data.campaigns[req.params.id];if(!c)return res.sendStatus(404);res.json(preview(c));
  });
  router.post('/admin/announcements/:id/send',async(req,res)=>{
    const c=store.data.campaigns[req.params.id];if(!c)return res.sendStatus(404);
    if(env.PUSH_SENDING_ENABLED!=='true')return res.status(503).json({error:'Sending disabled'});
    if(req.body?.confirm!==c.id)return res.status(400).json({error:'Confirm the reviewed announcement ID'});
    if(c.status!=='draft')return res.status(409).json({error:'Announcement already attempted; inspect results',...preview(c)});
    if(clock()-c.createdAt>3600000)return res.status(409).json({error:'Draft expired; create a fresh preview'});
    if(Object.keys(c.recipients).length>200)return res.status(409).json({error:'Audience exceeds synchronous sender limit (200). Use a queued sender before enabling larger announcements.'});
    if(send===sendAPNs){try{providerToken(env);}catch{return res.status(503).json({error:'APNs credentials unavailable'});}}
    c.status='sending';store.save();
    for(const [id,r] of Object.entries(c.recipients)) {
      const d=store.data.devices[id];
      if(!d||d.tokenHash!==r.tokenHash){r.state='skipped';store.save();continue;}
      r.state='attempting';store.save(); // crash => unknown; never silently resend a campaign
      try {
        const result=await send(store.decrypt(d.token),d.environment,c,env);
        r.state=result.status===200?'accepted':'rejected';r.code=result.status;
        if(result.status===410||result.reason==='BadDeviceToken'||result.reason==='DeviceTokenNotForTopic') {
          if(store.data.devices[id]?.tokenHash===d.tokenHash)delete store.data.devices[id];
        }
      } catch {r.state='unknown';}
      store.save();
    }
    c.status='complete';store.save();res.json(preview(c));
  });
  router.use((err,req,res,next)=>{res.status(503).json({error:'Notifications operation unavailable'});});
  return router;
}
