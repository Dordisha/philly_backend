import test from 'node:test';
import assert from 'node:assert/strict';
import { ownerNameVariants, ownerSearchSql, searchOwnerProperties } from '../src/lib/ownerSearch.js';
const row = (opa, owner_1 = 'DISHANI DORIAN', owner_2 = null) => ({parcel_number: opa, location: 'TEST FIXTURE', owner_1, owner_2});
const run = (rows, options = {}) => searchOwnerProperties('Dorian Dishani', options, {query: async () => rows});
test('simple names reverse; complex and entity names retain order', () => {
  assert.deepEqual(ownerNameVariants(' dorian   Dishani '), ['DORIAN DISHANI', 'DISHANI DORIAN']);
  for (const n of ['SMITH JOHN A', 'ACME LLC', 'SMITH TRUST', 'MARY, ANN']) assert.deepEqual(ownerNameVariants(n), [n]);
});
test('both fields use full names, escaped literals and bounded deduplicated keyset paging', () => {
  const sql = ownerSearchSql("JOHN O'BRIEN", {limit: 1, after: '363258000'});
  assert.match(sql, /owner_1\).* IN/s); assert.match(sql, /OR UPPER.*owner_2/s);
  assert.match(sql, /O''BRIEN/); assert.match(sql, /DISTINCT ON \(parcel_number\)/);
  assert.match(sql, /parcel_number > '363258000'/); assert.match(sql, /LIMIT 2/);
  assert.doesNotMatch(sql, /ILIKE|LIKE|OFFSET/);
});
test('invalid input is rejected before querying', async () => {
  for (const name of ['', 'a', 'DOR%', 'DOR_', 'A\nB', 'X'.repeat(161), null]) {
    assert.throws(() => ownerNameVariants(name));
  }
  for (const options of [{limit: 0}, {limit: 51}, {limit: 1.5}, {after: "1' OR TRUE"}]) assert.throws(() => ownerSearchSql('Dorian Dishani', options));
});
test('primary and co-owner results retain raw ownership fields', async () => {
  const rows = [row('100000001'), row('100000002', 'OTHER PERSON', 'DORIAN DISHANI')];
  assert.deepEqual((await run(rows)).rows, rows);
});
test('pagination uses the last displayed OPA, not the lookahead', async () => {
  const page = await run([row('100000001'), row('100000002')], {limit: 1});
  assert.equal(page.next_cursor, '100000001'); assert.equal(page.has_more, true);
  const next = await run([row('100000002')], {limit: 1, after: page.next_cursor});
  assert.equal(next.has_more, false); assert.equal(next.next_cursor, null);
});
test('near names, duplicates, invalid IDs and out-of-order pages fail closed', async () => {
  for (const rows of [[row('100000001', 'DORIAN DISHAN')], [row('100000001'), row('100000001')], [row('invalid')], [row('100000002'), row('100000001')]]) await assert.rejects(run(rows));
  await assert.rejects(run([row('100000001')], {after: '100000001'}));
});
test('empty is distinct from unavailable; results disclose coverage and ambiguity', async () => {
  const empty = await run([]); assert.deepEqual(empty.rows, []); assert.equal(empty.has_more, false);
  assert.match(empty.note, /different people/); assert.match(empty.note, /Philadelphia/);
  await assert.rejects(searchOwnerProperties('Dorian Dishani', {}, {query: async () => {throw Error('outage');}}));
});
