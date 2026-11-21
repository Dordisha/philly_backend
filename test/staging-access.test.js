import test from 'node:test';
import assert from 'node:assert/strict';
import { checkStagingAccess } from '../scripts/check-staging-access.mjs';

const sha = 'a'.repeat(40);
function fixture(change = () => {}) {
  let calls = 0;
  const fetchImpl = async (url, options) => {
    calls++;
    const u = new URL(url);
    assert.equal(u.origin, 'https://philly-scan-repair-test.onrender.com');
    assert.equal(options.method, 'GET');
    assert.equal(options.redirect, 'error');
    assert.ok(options.signal instanceof AbortSignal);
    assert.ok(!options.headers.Authorization || options.headers.Authorization === 'Bearer invalid-release-audit');
    const free = u.pathname !== '/api/records/signals';
    const ai = u.pathname === '/api/records/signals';
    const r = { status: free ? 200 : 403, cache: 'no-store', body: free ?
      { ok: true, commit: sha, opa: '363258000', result: { opa: '363258000' } } :
      { ok: false, code: ai ? 'ai_required' : 'pro_required', message: ai ? 'AI Premium access required.' : 'Pro subscription required.' } };
    if (u.pathname === '/api/owners') r.body.rows = [];
    if (u.pathname === '/api/violations/summary') Object.assign(r.body, {
      summary_version: 2, status_mapping: 'unverified', total_count: 2,
      status_counts: [{ casestatus: 'CLOSED', violationstatus: 'OPEN', count: 2 }],
      last_activity: '2026-01-01T00:00:00Z',
      active_count: null, historical_count: null, risk_level: null,
    });
    change(r, u, free);
    return { status: r.status, headers: new Headers({ 'cache-control': r.cache }), json: async () => r.body };
  };
  return { fetchImpl, calls: () => calls };
}

test('audit checks seven public record routes and one interpretation route twice plus controls without real credentials', async () => {
  const f = fixture();
  const r = await checkStagingAccess({ ...f, expectedCommit: sha });
  assert.equal(f.calls(), 20);
  assert.equal(r.passed, 20);
  assert.equal(r.source_match, true);
  assert.equal(r.failed + r.unavailable, 0);
  assert.equal(r.results.filter(x => x.name.startsWith('forged:')).length, 8);
});

test('public paid responses fail even with an empty body; missing route and outage never count as denial', async () => {
  for (const status of [200, 204, 401, 404, 429, 500, 503]) {
    const r = await checkStagingAccess(fixture((r, u, free) => { if (!free) { r.status = status; r.body = {}; } }));
    assert.equal(r.passed, 18);
    assert.equal(r.failed + r.unavailable, 2);
    assert.equal(r.unavailable, [429, 500, 503].includes(status) ? 2 : 0);
  }
});

test('a 403 requires the tier contract, no-store and no data leakage', async () => {
  for (const mutate of [
    r => { r.cache = 'public, max-age=300'; },
    r => { r.body.ok = true; },
    r => { r.body.code = 'unknown'; },
    r => { r.body.rows = [{ secret: 'private fixture' }]; },
    r => { r.body.message = 'private fixture'; },
  ]) {
    const r = await checkStagingAccess(fixture((r, u, free) => { if (!free) mutate(r); }));
    assert.equal(r.passed, 18);
    assert.equal(r.failed, 2);
    assert.ok(!JSON.stringify(r).includes('private fixture'));
  }
});

test('Free controls detect wrong identity, locked summaries and leaked details independently', async () => {
  const r = await checkStagingAccess(fixture((r, u) => {
    if (u.pathname.endsWith('/search')) r.body.result.opa = '999999999';
    if (u.pathname === '/api/complaints/summary') r.status = 403;
    if (u.pathname === '/api/violations/summary') r.body.rows = [];
  }));
  assert.equal(r.failed, 3);
  assert.equal(r.passed, 17);
});

test('hosted commit mismatch fails without suppressing route checks; omitted expected SHA is unverified', async () => {
  const r = await checkStagingAccess({ ...fixture(), expectedCommit: 'b'.repeat(40) });
  assert.equal(r.failed, 1);
  assert.equal(r.passed, 19);
  assert.equal(r.source_match, false);
  assert.equal((await checkStagingAccess(fixture())).source_match, null);
  await assert.rejects(checkStagingAccess({ expectedCommit: 'short' }), TypeError);
});

test('timeouts and network errors remain unavailable; malformed JSON fails without leaking exception text', async () => {
  for (const error of [new DOMException('private fixture', 'TimeoutError'), new TypeError('private fixture'), new SyntaxError('private fixture')]) {
    const r = await checkStagingAccess({ fetchImpl: async () => { throw error; } });
    assert.equal(r.passed, 0);
    assert.equal(error.name === 'SyntaxError' ? r.failed : r.unavailable, 20);
    assert.ok(!JSON.stringify(r).includes('private fixture'));
  }
});

test('one failed probe does not hide a later access bypass', async () => {
  const r = await checkStagingAccess(fixture((r, u) => {
    if (u.pathname === '/api/owners') r.status = 404;
    if (u.pathname === '/api/records/signals') r.status = 200;
  }));
  assert.equal(r.failed, 4);
  assert.equal(r.passed, 16);
});

test('healthy legacy and malformed L&I summaries fail release compatibility independently of source stamp', async () => {
  const mutations = [
    b => { delete b.summary_version; },
    b => { b.summary_version = 1; },
    b => { b.status_mapping = 'verified'; },
    b => { b.total_count = '2'; },
    b => { b.total_count = 3; },
    b => { b.status_counts = null; },
    b => { b.status_counts = [null]; },
    b => { delete b.status_counts[0].casestatus; },
    b => { b.status_counts[0].count = -1; },
    b => { b.status_counts.push({ ...b.status_counts[0] }); b.total_count = 4; },
    b => { b.status_counts[0].count = Number.MAX_SAFE_INTEGER + 1; },
    b => { delete b.last_activity; },
    b => { b.last_activity = 'invalid'; },
    b => { b.active_count = 2; },
    b => { b.historical_count = 0; },
    b => { b.risk_level = 'Low'; },
    b => { b.total_count = 0; b.status_counts = []; },
  ];
  for (const mutate of mutations) {
    const r = await checkStagingAccess({ ...fixture((r, u) => {
      if (u.pathname === '/api/violations/summary') mutate(r.body);
    }), expectedCommit: sha });
    assert.equal(r.source_match, true);
    assert.equal(r.failed, 1);
    assert.equal(r.passed, 19);
    assert.equal(r.results.find(x => x.name === 'free-violations').outcome, 'failed');
  }
});

test('valid zero and unknown raw L&I statuses remain compatible without risk inference', async () => {
  for (const zero of [true, false]) {
    const r = await checkStagingAccess(fixture((r, u) => {
      if (u.pathname !== '/api/violations/summary') return;
      r.body.last_activity = null;
      r.body.total_count = zero ? 0 : 2;
      r.body.status_counts = zero ? [] : [{ casestatus: null, violationstatus: 'UNREVIEWED', count: 2 }];
    }));
    assert.equal(r.passed, 20);
  }
});
