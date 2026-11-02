// Run from a trusted admin machine. Secret comes from environment, never a URL or app.
import { readFileSync } from 'node:fs';
const [command,arg]=process.argv.slice(2);
const base=process.env.PHILLY_ADMIN_BASE_URL, token=process.env.PUSH_ADMIN_TOKEN;
if(!base||new URL(base).protocol!=='https:'||!token||token.length<32)throw new Error('Set HTTPS PHILLY_ADMIN_BASE_URL and PUSH_ADMIN_TOKEN');
const paths={draft:'/api/notifications/admin/announcements',preview:`/api/notifications/admin/announcements/${encodeURIComponent(arg||'')}`,
  send:`/api/notifications/admin/announcements/${encodeURIComponent(arg||'')}/send`};
if(!paths[command]||!arg)throw new Error('Usage: node tools/announcements.mjs draft announcement.json | preview ID | send ID');
if(command==='send'&&process.env.CONFIRM_ANNOUNCEMENT_ID!==arg)throw new Error('Preview first, then set CONFIRM_ANNOUNCEMENT_ID to the reviewed ID');
const body=command==='draft'?JSON.parse(readFileSync(arg,'utf8')):command==='send'?{confirm:arg}:undefined;
const r=await fetch(new URL(paths[command],base),{method:command==='preview'?'GET':'POST',
  headers:{Authorization:`Bearer ${token}`,'Content-Type':'application/json'},body:body?JSON.stringify(body):undefined,
  signal:AbortSignal.timeout(command==='send'?2200000:15000)});
console.log(JSON.stringify(await r.json(),null,2));if(!r.ok)process.exitCode=1;
