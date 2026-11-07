import test from 'node:test';
import assert from 'node:assert/strict';
import { summarizeCrime } from '../src/routes/crime.js';
import { checkStaging } from '../scripts/check-staging.mjs';
function fake(change = () => {}) {
  return async (url, options) => {
    assert.equal(new URL(url).host, 'philly-scan-repair-test.onrender.com');
    assert.equal(options.method, 'GET');
    assert.equal(options.redirect, 'error');
    const u = new URL(url), section = u.pathname.split('/').at(-1);
    let status = u.searchParams.get('opa') === 'invalid' ? 400 : 200;
    let body = {ok: status === 200, commit: 'a'.repeat(40), result: {opa:'363258000'}, opa:'363258000', address:'1539 S LAMBERT ST', retrieved_at:'2026-11-02T00:00:00Z', source:{table: section === '311' ? 'public_cases_fc' : section === 'violations' ? 'violations' : 'opa_properties_public', url:'https://example.org'}, limit:1, offset:Number(u.searchParams.get('offset')), has_more:false, rows:[]};
    if (section === 'crime') Object.assign(body, summarizeCrime([], '2026-11-01', new Date('2026-11-02')), {radius_m:Number(u.searchParams.get('radius_m')), source:{url:'https://opendataphilly.org/datasets/crime-incidents/'}});
    const mutated = change({url:u, body, status});
    if (mutated) ({body, status} = mutated);
    return {status, json: async () => body};
  };
}
test('staging checker passes empty valid pages with exactly twelve read-only calls', async () => {
  let calls = 0;
  const fetcher = fake();
  const result = await checkStaging({fetchImpl: (...args) => { calls++; return fetcher(...args); }});
  assert.equal(calls, 12); assert.equal(result.passed, 12);
});
test('wrong parcel fails without suppressing independent checks', async () => {
  const result = await checkStaging({fetchImpl:fake(({body}) => {body.opa='999999999';})});
  assert.equal(result.failed, 8); assert.equal(result.passed, 4);
});
test('upstream unavailability is not a passing empty response', async () => {
  const result = await checkStaging({fetchImpl:fake(({body}) => ({body, status:503}))});
  assert.equal(result.unavailable, 12); assert.equal(result.passed, 0);
});
test('malformed JSON is reported as failure', async () => {
  const result = await checkStaging({fetchImpl: async () => ({status:200,json:async()=>{throw new SyntaxError('invalid JSON');}})});
  assert.equal(result.failed, 12);
});
test('timeout is unavailable and checker continues within request budget', async () => {
  const result = await checkStaging({fetchImpl: async () => {throw new DOMException('timed out','TimeoutError');}});
  assert.equal(result.unavailable, 12);
});
test('wrong-address complaint and repeated pagination fail', async () => {
  for (const wrongAddress of [true, false]) {
    const result = await checkStaging({fetchImpl:fake(({url,body}) => {
      if (url.pathname.endsWith('/311') && body.ok) body.rows=[{service_request_id:1,address:wrongAddress?'NEIGHBOR':body.address,zipcode:'19146',requested_datetime:'2026-01-01'}];
    })});
    assert.equal(result.failed, wrongAddress ? 2 : 1);
  }
});

test('crime checker rejects wrong identity, radius, counts and reporting periods', async () => {
  const changes = [
    b => { b.address = 'NEIGHBOR'; },
    b => { b.radius_m = 1; },
    b => { b.current_count = null; },
    b => { b.current_period.from = '2026-01-01'; },
    b => { b.months.pop(); },
    b => { b.categories = [{count:1}]; },
    b => { b.stale = true; b.change_percent = 10; },
  ];
  for (const change of changes) {
    const result = await checkStaging({fetchImpl:fake(({url, body}) => {
      if (url.pathname === '/api/crime') change(body);
    })});
    assert.equal(result.failed, 3);
    assert.equal(result.passed, 9);
  }
});
test('missing crime route and unavailable source cannot pass as zero', async () => {
  for (const status of [404, 503]) {
    const result = await checkStaging({fetchImpl:fake(({url, body, status:original}) => ({body, status:url.pathname === '/api/crime' ? status : original}))});
    assert.equal(result.passed, 9);
    assert.equal(status === 404 ? result.failed : result.unavailable, 3);
  }
});
