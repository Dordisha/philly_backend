import { Router } from 'express';
import { queryCarto } from '../lib/carto.js';
import { validOpa, literal } from '../lib/propertyData.js';
import { requirePaidAccess, resolvePaidAccess } from '../services/paidAccess.js';
const DAY = 86400000;
const iso = d => new Date(d).toISOString().slice(0,10);
export function summarizeCrime(rows, latest, now = new Date()) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(latest || '') || !Number.isFinite(Date.parse(latest))) throw new Error('Missing source date');
  const today = Date.parse(iso(now));
  const end = Math.min(Date.parse(latest) + DAY, today); // exclude today
  const start = end - 90 * DAY, previous = start - 90 * DAY;
  let current = 0, prior = 0;
  const categories = new Map(), months = new Map();
  const monthEnd = new Date(end); monthEnd.setUTCDate(1);
  for (let i = 12; i > 0; i--) {
    const d = new Date(monthEnd); d.setUTCMonth(d.getUTCMonth() - i);
    months.set(iso(d).slice(0,7), 0);
  }
  for (const r of rows) {
    const day = Date.parse(r.day), count = Number(r.count);
    if (!Number.isFinite(day) || !Number.isSafeInteger(count) || count < 0) throw new Error('Invalid aggregate');
    if (day >= end) continue;
    if (day >= start) { current += count; const key = r.category || 'Unclassified'; categories.set(key,(categories.get(key)||0)+count); }
    else if (day >= previous) prior += count;
    const month = iso(day).slice(0,7);
    if (months.has(month)) months.set(month, months.get(month)+count);
  }
  const stale = today - Date.parse(latest) > 14 * DAY;
  return { current_count: current, previous_count: prior,
    change_percent: !stale && prior > 0 ? Math.round((current-prior)/prior*1000)/10 : null,
    stale, latest_source_date: latest,
    current_period: {from:iso(start), through:iso(end-DAY)},
    previous_period: {from:iso(previous), through:iso(start-DAY)},
    categories:[...categories].map(([category,count])=>({category,count})).sort((a,b)=>b.count-a.count),
    months:[...months].map(([month,count])=>({month,count})) };
}
export function crimeSql(opa, radius, from, through) {
  return `SELECT c.dispatch_date AS day, c.text_general_code AS category, count(*)::integer AS count
    FROM incidents_part1_part2 c
    WHERE c.dispatch_date >= ${literal(from)} AND c.dispatch_date < ${literal(through)}
    AND c.the_geom IS NOT NULL
    AND ST_DWithin(c.the_geom, (SELECT the_geom FROM opa_properties_public WHERE parcel_number = ${literal(opa)} LIMIT 1), 0.02)
    AND ST_DWithin(c.the_geom::geography, (SELECT the_geom::geography FROM opa_properties_public WHERE parcel_number = ${literal(opa)} LIMIT 1), ${radius})
    GROUP BY c.dispatch_date,c.text_general_code ORDER BY c.dispatch_date`;
}
export function createCrimeRouter({query=queryCarto, now=()=>new Date(), authorize=resolvePaidAccess}={}) {
 const router=Router();
 router.get('/', requirePaidAccess('ai', authorize), async(req,res)=>{
  const opa=typeof req.query.opa==='string'?req.query.opa.trim():'';
  const radius=req.query.radius_m === undefined ? 402 : Number(req.query.radius_m);
  if(!validOpa(opa)||![402,805,1609].includes(radius)) return res.status(400).json({ok:false,message:'Valid OPA and supported radius required.'});
  try {
   const properties=await query(`SELECT location, ST_X(the_geom) AS longitude, ST_Y(the_geom) AS latitude FROM opa_properties_public WHERE parcel_number = ${literal(opa)} LIMIT 1`);
   if(!properties.length) return res.status(404).json({ok:false,message:'Property not found.'});
   const p=properties[0];
   if(typeof p.latitude!=='number'||typeof p.longitude!=='number'||p.latitude<39.8||p.latitude>40.2||p.longitude < -75.4||p.longitude > -74.9) return res.status(422).json({ok:false,message:'Property location unavailable.'});
   const at=now();
   const freshness=await query(`SELECT max(dispatch_date) AS latest_date FROM incidents_part1_part2 WHERE dispatch_date <= ${literal(iso(at))}`);
   const latest=freshness[0]?.latest_date;
   const empty=summarizeCrime([],latest,at);
   const end=new Date(Date.parse(empty.current_period.through)+DAY);
   const from=new Date(end);from.setUTCDate(1);from.setUTCMonth(from.getUTCMonth()-12);
   const rows=await query(crimeSql(opa,radius,iso(from),iso(end)));
   return res.json({ok:true,opa,address:p.location,radius_m:radius,retrieved_at:at.toISOString(),
    source:{label:'Philadelphia Police Department · OpenDataPhilly',url:'https://opendataphilly.org/datasets/crime-incidents/'},
    note:'Published incident records near this parcel, not incidents at the property. Locations are approximate; missing locations are excluded. Counts use dispatch dates and may change with reporting delays or revisions. No population-adjusted crime rate or safety score is inferred. A zero count means no matching published records, not no crime.',
    ...summarizeCrime(rows,latest,at)});
  } catch {return res.status(503).json({ok:false,message:'Crime records unavailable. Please retry.'});}
 });return router;
}
export default createCrimeRouter();
