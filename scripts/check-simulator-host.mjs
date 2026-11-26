import assert from 'node:assert/strict';
import { checkStaging } from './check-staging.mjs';
import { checkStagingAccess } from './check-staging-access.mjs';
const expected = process.argv[2];
assert.match(expected || '', /^[a-f0-9]{40}$/);
const host = 'https://philly-scan-repair-test.onrender.com';
let matched = false;
// Wait only for the existing staging auto-deploy; never publish an app that
// merely launches against an old/incompatible backend.
for (let attempt = 0; attempt < 12; attempt++) {
  try {
    const response = await fetch(host + '/_root', {cache: 'no-store', signal: AbortSignal.timeout(15000)});
    const body = await response.json();
    if (body.commit === expected) { matched = true; break; }
  } catch { /* bounded deployment/cold-start wait */ }
  await new Promise(resolve => setTimeout(resolve, 10000));
}
assert.ok(matched, 'Staging must run the exact candidate commit');
const data = await checkStaging();
const access = await checkStagingAccess({expectedCommit: expected, timeoutMs: 45000});
console.log(JSON.stringify({data, access}, null, 2));
assert.equal(data.failed + data.unavailable + access.failed + access.unavailable, 0,
  'All live data and contract checks must pass before native testing/packaging');
