import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import { createRecordsRouter, complaintsSql, nearbySalesSql } from '../src/routes/records.js';
const property = { parcel_number: '363258000', location: "10 O'BRIEN ST", zip_code: '19146', total_livable_area: 1400, category_code: '1', building_code_new: '25' };
async function api(query, run) {
  const app = express();
  app.use('/api/records', createRecordsRouter({ query, now: () => '2026-11-01T00:00:00Z' }));
  const server = app.listen(0, '127.0.0.1');
  await new Promise(r => server.once('listening', r));
  try { await run('http://127.0.0.1:' + server.address().port + '/api/records'); }
  finally { await new Promise(r => server.close(r)); }
}
async function get(base, path) { const r = await fetch(base + path); return { status: r.status, body: await r.json() }; }
test('records reject invalid identifiers without querying the city', async () => {
  await api(() => { throw Error('must not query'); }, async base => {
    assert.equal((await get(base, '/311?opa=1%27OR%20TRUE')).status, 400);
    assert.equal((await get(base, '/facts')).status, 400);
  });
});
test('311 uses exact published address and ZIP, not nearby/parcel assumptions', () => {
  const sql = complaintsSql(property, 25, 50);
  assert.ok(sql.includes("address = '10 O''BRIEN ST'"));
  assert.ok(sql.includes("zipcode = '19146'"));
  assert.ok(sql.includes('LIMIT 26 OFFSET 50'));
  assert.ok(!sql.includes('subject') && !sql.includes('media_url'));
});
test('311 pagination returns real source fields and explicit matching limits', async () => {
  const calls = [];
  await api(async sql => { calls.push(sql); return sql.includes('public_cases_fc') ? [{service_request_id: 1}, {service_request_id: 2}] : [property]; }, async base => {
    const r = await get(base, '/311?opa=363258000&limit=1&offset=1');
    assert.equal(r.status, 200);
    assert.deepEqual(r.body.rows, [{service_request_id: 1}]);
    assert.equal(r.body.has_more, true);
    assert.equal(r.body.source.table, 'public_cases_fc');
    assert.match(r.body.note, /no parcel ID/);
    assert.ok(calls[1].includes('LIMIT 2 OFFSET 1'));
  });
});
test('empty published records and source failures remain distinct', async () => {
  await api(async sql => sql.includes('public_cases_fc') ? [] : [property], async base => {
    const r = await get(base, '/311?opa=363258000');
    assert.equal(r.status, 200); assert.deepEqual(r.body.rows, []);
  });
  await api(async sql => { if (sql.includes('public_cases_fc')) throw Error('private upstream details'); return [property]; }, async base => {
    const r = await get(base, '/311?opa=363258000');
    assert.equal(r.status, 503); assert.equal(r.body.rows, undefined);
    assert.ok(!JSON.stringify(r.body).includes('private upstream'));
  });
});
test('sales show bounds and raw condition fields without estimating value', () => {
  const sql = nearbySalesSql('363258000', 25, 0);
  assert.match(sql, /800/); assert.match(sql, /24 months/);
  assert.match(sql, /interior_condition/); assert.match(sql, /0.75/);
  assert.ok(!sql.includes('AVG('));
});
test('L&I retains raw status, unknowns, and parcel matching', async () => {
  const row = {casenumber:'CASE-1',casestatus:null,violationstatus:'UNRESOLVED'};
  await api(async sql => sql.includes('FROM violations') ? [row] : [property], async base => {
    const r = await get(base, '/violations?opa=363258000');
    assert.deepEqual(r.body.rows, [row]);
    assert.equal(r.body.source.table, 'violations');
  });
});
test('unmatchable property fails clearly, never substitutes an adjacent property', async () => {
  await api(async () => [], async base => assert.equal((await get(base,'/facts?opa=363258000')).status,404));
  await api(async () => [{...property,total_livable_area:null}], async base => assert.equal((await get(base,'/sales?opa=363258000')).status,422));
});
test('facts preserve raw values and do not invent listing or seller scores', async () => {
  await api(async () => [property], async base => {
    const r = await get(base,'/facts?opa=363258000');
    assert.equal(r.body.rows[0].parcel_number, property.parcel_number);
    assert.equal(r.body.score, undefined); assert.equal(r.body.listed, undefined);
    assert.match(r.body.note,/Listing status is unknown/);
  });
});

test('selling signals use dated facts and explicit weak rules, never fabricated probabilities', async () => {
  const { propertySignals } = await import('../src/routes/records.js');
  const result = propertySignals({...property,sale_date:'2000-01-01',mailing_street:'20 OTHER ST'}, '2026-11-01T00:00:00Z');
  assert.equal(result.factors.length,2);
  assert.equal(result.label,'Weak evidence only');
  assert.equal(result.probability,undefined);
  assert.match(result.factors[0].evidence,/26 years/);
  assert.equal(propertySignals({...property,sale_date:null},'2026-11-01').label,'Insufficient evidence');
  assert.equal(propertySignals({...property,sale_date:'2030-01-01'},'2026-11-01').factors.length,0);
});
