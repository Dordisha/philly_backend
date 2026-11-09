import test from 'node:test';
import assert from 'node:assert/strict';
import { summarizeCrime,crimeSql,createCrimeRouter } from '../src/routes/crime.js';
import express from 'express';
test('crime windows are equal, exclusive of today, and zero baseline is not a percentage',()=>{
 const now=new Date('2026-11-02T15:00:00Z');
 const result=summarizeCrime([{day:'2026-11-01',category:'Theft',count:3},{day:'2026-08-03',category:'Theft',count:2},{day:'2026-11-02',category:'Theft',count:999}], '2026-11-02',now);
 assert.equal(result.current_count,3);assert.equal(result.previous_count,2);assert.equal(result.change_percent,50);
 assert.equal(result.current_period.from,'2026-08-04');assert.equal(result.months.length,12);
 assert.equal(summarizeCrime([], '2026-11-01',now).change_percent,null);
 assert.equal(summarizeCrime([], '2026-09-01',now).stale,true);
});
test('crime SQL uses exact parcel and meter distance',()=>{
 const sql=crimeSql('782273400',402,'2025-11-01','2026-11-02');
 assert.match(sql,/parcel_number = '782273400'/);assert.match(sql,/::geography/);assert.match(sql,/, 402\)/);
});
test('crime route validates inputs, handles missing location and upstream failures',async t=>{
 let mode='ok',calls=0;
 const app=express();app.use('/api/crime',createCrimeRouter({authorize:()=>true,now:()=>new Date('2026-11-02'),query:async sql=>{
 calls++;if(mode==='fail')throw Error();
 if(sql.startsWith('SELECT location'))return mode==='missing'?[]:mode==='nogeo'?[{latitude:null,longitude:null}]:[{location:'CITY HALL',latitude:39.95,longitude:-75.16}];
 if(sql.startsWith('SELECT max'))return [{latest_date:'2026-11-01'}];
 return [{day:'2026-11-01',category:'Theft',count:2}];
 }}));const server=app.listen(0);t.after(()=>server.close());await new Promise(r=>server.once('listening',r));
 const url=`http://127.0.0.1:${server.address().port}/api/crime`;
 assert.equal((await fetch(url+'?opa=bad')).status,400);assert.equal(calls,0);
 assert.equal((await fetch(url+'?opa=782273400&radius_m=99999')).status,400);
 const r=await (await fetch(url+'?opa=782273400')).json();assert.equal(r.current_count,2);assert.equal(r.radius_m,402);
 mode='missing';assert.equal((await fetch(url+'?opa=782273400')).status,404);
 mode='nogeo';assert.equal((await fetch(url+'?opa=782273400')).status,422);
 mode='fail';assert.equal((await fetch(url+'?opa=782273400')).status,503);
});
