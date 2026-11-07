import assert from 'node:assert/strict';
import { pathToFileURL } from 'node:url';

const HOST = 'https://philly-scan-repair-test.onrender.com';
const OPA = '363258000';
const tables = { '311': 'public_cases_fc', violations: 'violations', sales: 'opa_properties_public', facts: 'opa_properties_public' };

// Fixed staging destination, GET only, twelve sequential requests, no retries or schedule.
export async function checkStaging({ fetchImpl = fetch, timeoutMs = 45000 } = {}) {
  const results = [];
  let deployedCommit = null;
  const cases = [
    ['health', '/_root'],
    ['lookup', '/api/opa/search?opa=' + OPA],
    ...Object.keys(tables).map(section => [section, `/api/records/${section}?opa=${OPA}&limit=1&offset=0`]),
    ['311-page-2', `/api/records/311?opa=${OPA}&limit=1&offset=1`],
    ...[402, 805, 1609].map(radius => [`crime-${radius}`, `/api/crime?opa=${OPA}&radius_m=${radius}`]),
    ['invalid-opa', '/api/records/311?opa=invalid'],
    ['invalid-lookup', '/api/opa/search?opa=invalid'],
  ];
  let firstComplaint;
  for (const [name, path] of cases) {
    const start = Date.now();
    let status;
    try {
      const response = await fetchImpl(HOST + path, { method: 'GET', redirect: 'error', signal: AbortSignal.timeout(timeoutMs) });
      status = response.status;
      const body = await response.json();
      assert.equal(status, name.startsWith('invalid-') ? 400 : 200, 'Unexpected HTTP status');
      if (name === 'health') {
        assert.match(body.commit, /^[a-f0-9]{40}$/, 'Missing deployment commit');
        deployedCommit = body.commit;
      } else if (name.startsWith('invalid-')) {
        assert.equal(body.ok, false);
      } else if (name === 'lookup') {
        assert.equal(body.ok, true);
        assert.equal(String(body.result.opa), OPA, 'Lookup property mismatch');
      } else if (name.startsWith('crime-')) {
        assert.equal(body.ok, true);
        assert.equal(body.opa, OPA, 'Crime parcel mismatch');
        assert.equal(body.address, '1539 S LAMBERT ST', 'Crime address mismatch');
        assert.equal(body.radius_m, Number(name.slice(6)), 'Crime radius mismatch');
        assert.equal(body.source.url, 'https://opendataphilly.org/datasets/crime-incidents/');
        assert.ok(Number.isFinite(Date.parse(body.retrieved_at)));
        assert.ok(Number.isFinite(Date.parse(body.latest_source_date)));
        assert.equal(typeof body.stale, 'boolean');
        for (const count of [body.current_count, body.previous_count]) {
          assert.ok(Number.isSafeInteger(count) && count >= 0, 'Invalid crime count');
        }
        const day = 86400000;
        for (const period of [body.current_period, body.previous_period]) {
          assert.equal(Date.parse(period.through) - Date.parse(period.from), 89 * day, 'Expected 90-day period');
        }
        assert.equal(Date.parse(body.current_period.from) - Date.parse(body.previous_period.through), day);
        assert.ok(Date.parse(body.current_period.through) <= Date.parse(body.latest_source_date));
        assert.ok(Array.isArray(body.months) && body.months.length === 12);
        assert.equal(new Set(body.months.map(m => m.month)).size, 12);
        for (const m of body.months) {
          assert.match(m.month, /^\d{4}-(0[1-9]|1[0-2])$/);
          assert.ok(Number.isSafeInteger(m.count) && m.count >= 0);
        }
        assert.ok(Array.isArray(body.categories));
        let total = 0;
        for (const c of body.categories) {
          assert.ok(Number.isSafeInteger(c.count) && c.count >= 0);
          total += c.count;
        }
        assert.equal(total, body.current_count, 'Category total mismatch');
        if (body.stale || body.previous_count === 0) assert.equal(body.change_percent, null);
        else assert.equal(body.change_percent, Math.round((body.current_count - body.previous_count) / body.previous_count * 1000) / 10);
      } else {
        const section = name === '311-page-2' ? '311' : name;
        assert.equal(body.ok, true);
        assert.equal(body.opa, OPA, 'Record envelope property mismatch');
        assert.equal(body.source.table, tables[section]);
        assert.ok(body.source.url.startsWith('https://'));
        assert.ok(Number.isFinite(Date.parse(body.retrieved_at)), 'Missing retrieval timestamp');
        assert.equal(body.limit, 1);
        assert.equal(body.offset, name === '311-page-2' ? 1 : 0);
        assert.equal(typeof body.has_more, 'boolean');
        assert.ok(Array.isArray(body.rows) && body.rows.length <= 1, 'Unbounded page');
        for (const row of body.rows) {
          if (section === '311') {
            assert.equal(row.address, body.address, 'Complaint address mismatch');
            assert.equal(String(row.zipcode), '19146', 'Complaint ZIP mismatch');
            assert.ok(row.service_request_id != null);
            assert.ok(Number.isFinite(Date.parse(row.requested_datetime)));
            if (name === '311') firstComplaint = row.service_request_id;
            else assert.notEqual(row.service_request_id, firstComplaint, 'Repeated page record');
          } else if (section === 'facts') {
            assert.equal(String(row.parcel_number), OPA);
            assert.match(body.signals.listing_status, /Unknown/);
          } else if (section === 'sales') {
            assert.ok(row.parcel_number != null);
            assert.notEqual(String(row.parcel_number), OPA);
            assert.ok(Number(row.sale_price) > 0 && Number(row.distance_m) <= 800);
          } else {
            assert.ok(row.casenumber != null && row.violationnumber != null);
            assert.ok('casestatus' in row && 'violationstatus' in row);
          }
        }
      }
      results.push({ name, outcome: 'passed', status, elapsed_ms: Date.now() - start });
    } catch (error) {
      const outcome = [429, 502, 503, 504].includes(status) || ['TimeoutError', 'AbortError'].includes(error.name)
        ? 'unavailable' : 'failed';
      results.push({ name, outcome, status, elapsed_ms: Date.now() - start, reason: error.message });
    }
  }
  return { checked_at: new Date().toISOString(), host: HOST, deployed_commit: deployedCommit, results,
    passed: results.filter(r => r.outcome === 'passed').length,
    failed: results.filter(r => r.outcome === 'failed').length,
    unavailable: results.filter(r => r.outcome === 'unavailable').length };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const result = await checkStaging();
  console.log(JSON.stringify(result, null, 2));
  process.exitCode = result.failed || result.unavailable ? 1 : 0;
}
