import assert from 'node:assert/strict';
import { pathToFileURL } from 'node:url';

const HOST = 'https://philly-scan-repair-test.onrender.com';
const OPA = '363258000';
const recordRoutes = [
  ['/api/records/311', null], ['/api/records/violations', null],
  ['/api/complaints/details', null], ['/api/violations/details', null],
  ['/api/owners', null], ['/api/records/sales', null],
  ['/api/records/signals', 'ai'], ['/api/crime', null],
];

// Independent consumer-contract check: a healthy old server is not compatible
// with the version-2 mobile summary parser. Do not infer active/risk from status.
function checkViolationSummary(body) {
  assert.equal(body.summary_version, 2);
  assert.equal(body.status_mapping, 'unverified');
  assert.ok(Number.isSafeInteger(body.total_count) && body.total_count >= 0);
  assert.ok(Array.isArray(body.status_counts));
  assert.ok(body.last_activity === null ||
    typeof body.last_activity === 'string' && Number.isFinite(Date.parse(body.last_activity)));
  for (const field of ['active_count', 'historical_count', 'risk_level']) assert.equal(body[field], null);
  const groups = new Set();
  let total = 0;
  for (const row of body.status_counts) {
    assert.ok(row && [row.casestatus, row.violationstatus].every(v => v === null || typeof v === 'string'));
    assert.ok(Number.isSafeInteger(row.count) && row.count > 0);
    const key = JSON.stringify([row.casestatus, row.violationstatus]);
    assert.ok(!groups.has(key));
    groups.add(key);
    total += row.count;
    assert.ok(Number.isSafeInteger(total));
  }
  assert.equal(total, body.total_count);
  if (total === 0) assert.equal(body.last_activity, null);
}

// Fixed staging destination; no real credential, mutation, redirect or retry.
// This negative-access audit cannot establish purchase/restore or release readiness.
export async function checkStagingAccess({ fetchImpl = fetch, timeoutMs = 15000, expectedCommit } = {}) {
  if (expectedCommit !== undefined && !/^[a-f0-9]{40}$/.test(expectedCommit)) throw new TypeError('Expected a full commit SHA');
  if (!Number.isInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 45000) throw new TypeError('Invalid timeout');
  const cases = [
    { name: 'deployment', path: '/_root' },
    { name: 'free-lookup', path: `/api/opa/search?opa=${OPA}` },
    ...['complaints', 'violations'].map(section => ({ name: `free-${section}`, path: `/api/${section}/summary?opa=${OPA}` })),
    ...recordRoutes.flatMap(([path, tier]) => ['anonymous', 'forged'].map(mode => ({
      name: `${mode}:${path}`, tier,
      path: `${path}?opa=${OPA}&name=Dorian%20Dishani&limit=1&radius_m=402` +
        (mode === 'forged' ? '&tier=ai&PRO_PREVIEW=true&INTERNAL_TESTING=true' : ''),
      headers: mode === 'forged' ? { Authorization: 'Bearer invalid-release-audit', 'X-Pro': 'true', 'X-Tier': 'ai', 'X-Preview': 'true' } : {},
    }))),
  ];
  let deployedCommit = null;
  const results = [];
  for (const item of cases) {
    let status;
    try {
      const response = await fetchImpl(HOST + item.path, {
        method: 'GET', headers: item.headers || {}, redirect: 'error', signal: AbortSignal.timeout(timeoutMs),
      });
      status = response.status;
      // Classify status before parsing: an HTML 404/503 must not pass as denial.
      if ([429, 500, 502, 503, 504].includes(status)) throw new Error('Service unavailable');
      if (status === 404) throw new Error('Route missing');
      if (item.tier && status >= 200 && status < 300) throw new Error('Protected route accepted an unverified request');
      assert.equal(status, item.tier ? 403 : 200, 'Unexpected HTTP status');
      const body = await response.json();
      if (item.tier) {
        assert.equal(response.headers.get('cache-control')?.trim().toLowerCase(), 'no-store', 'Denial must not be cached');
        assert.deepEqual(Object.keys(body).sort(), ['code', 'message', 'ok'], 'Unexpected denial payload fields');
        assert.equal(body.ok, false);
        assert.equal(body.code, item.tier + '_required', 'Wrong tier denial');
        assert.equal(body.message, item.tier === 'ai' ? 'AI Premium access required.' : 'Pro subscription required.');
      } else if (item.name === 'deployment') {
        assert.match(body.commit, /^[a-f0-9]{40}$/, 'Missing deployment commit');
        deployedCommit = body.commit;
        if (expectedCommit) assert.equal(body.commit, expectedCommit, 'Hosted/source commit mismatch');
      } else {
        assert.equal(body.ok, true);
        if (!item.name.includes('/api/owners')) assert.equal(String(item.name === 'free-lookup' ? body.result?.opa : body.opa ?? body.property?.parcel_number), OPA, 'Free property identity mismatch');
        if (item.name.includes('/api/owners')) assert.ok(Array.isArray(body.rows), 'Owner rows missing');
        if (item.name.startsWith('free-') && item.name !== 'free-lookup') assert.equal(body.rows, undefined, 'Free summary includes detail rows');
        if (item.name === 'free-violations') checkViolationSummary(body);
      }
      results.push({ name: item.name, outcome: 'passed', status });
    } catch (error) {
      const outcome = [429, 500, 502, 503, 504].includes(status) || ['TimeoutError', 'AbortError', 'TypeError'].includes(error.name)
        ? 'unavailable' : 'failed';
      // Do not serialize response bodies, request credentials or upstream errors.
      results.push({ name: item.name, outcome, status,
        reason: outcome === 'unavailable' ? 'Service or response unavailable' : status === 404 ? 'Route missing' :
          item.tier && status >= 200 && status < 300 ? 'Protected route accepted an unverified request' : 'Response contract failed' });
    }
  }
  return { checked_at: new Date().toISOString(), host: HOST, deployed_commit: deployedCommit,
    expected_commit: expectedCommit || null,
    source_match: expectedCommit ? deployedCommit === expectedCommit : null,
    scope: 'Public records must be free; interpretation must deny unverified access. Purchase/restore and runtime acceptance not tested',
    results, passed: results.filter(r => r.outcome === 'passed').length,
    failed: results.filter(r => r.outcome === 'failed').length,
    unavailable: results.filter(r => r.outcome === 'unavailable').length };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const result = await checkStagingAccess({ expectedCommit: process.argv[2] });
  console.log(JSON.stringify(result, null, 2));
  process.exitCode = result.failed || result.unavailable ? 1 : 0;
}
