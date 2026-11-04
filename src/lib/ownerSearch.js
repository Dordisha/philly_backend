import { queryCarto } from './carto.js';
import { literal, validOpa } from './propertyData.js';

// Internal data adapter only. Do not mount behind an unauthenticated route.
// Authentication + current server-verified entitlements (#2/#28) must precede use.
export function ownerNameVariants(value) {
  if (typeof value !== 'string' || /[\x00-\x1f]/.test(value)) throw new TypeError('Recorded owner name required');
  const name = value.trim().replace(/\s+/g, ' ').toUpperCase();
  if (name.length < 3 || name.length > 160 || /[\x00-\x1f%_]/.test(name)) {
    throw new TypeError('Invalid owner name');
  }
  const parts = name.split(' ');
  // Only reverse simple two-part names; never permute middle names or entities.
  const entity = /\b(LLC|INC|CORP|TRUST|LP|LLP|CO|COMPANY|ASSOCIATES|PARTNERS|HOLDINGS)\b/;
  return parts.length === 2 && parts.every(p => /^[A-Z'-]+$/.test(p)) && !entity.test(name)
    ? [...new Set([name, [...parts].reverse().join(' ')])] : [name];
}

export function ownerSearchSql(name, { limit = 25, after = '' } = {}) {
  if (!Number.isInteger(limit) || limit < 1 || limit > 50) throw new TypeError('Invalid limit');
  if (after !== '' && !validOpa(after)) throw new TypeError('Invalid cursor');
  const names = ownerNameVariants(name).map(literal).join(', ');
  const normalized = field => `UPPER(REGEXP_REPLACE(TRIM(${field}), '\\s+', ' ', 'g'))`;
  // Dedupe before paging; keyset pagination prevents duplicate OPAs across pages.
  return `SELECT DISTINCT ON (parcel_number) parcel_number, location, owner_1, owner_2
    FROM opa_properties_public
    WHERE (${normalized('owner_1')} IN (${names}) OR ${normalized('owner_2')} IN (${names}))
    AND parcel_number ~ '^[0-9]{9}$'
    ${after ? `AND parcel_number > ${literal(after)}` : ''}
    ORDER BY parcel_number, location, owner_1, owner_2 LIMIT ${limit + 1}`;
}

export async function searchOwnerProperties(name, options = {}, { query = queryCarto, now = () => new Date().toISOString() } = {}) {
  const sql = ownerSearchSql(name, options);
  const variants = ownerNameVariants(name);
  const limit = options.limit ?? 25;
  const rows = await query(sql);
  let previous = options.after || '';
  for (const row of rows) {
    const opa = String(row.parcel_number ?? '');
    const matches = [row.owner_1, row.owner_2].some(n => typeof n === 'string' && variants.includes(n.trim().replace(/\s+/g, ' ').toUpperCase()));
    if (!validOpa(opa) || opa <= previous || !row.location || !matches) {
      throw new Error('Owner result identity could not be verified');
    }
    previous = opa;
  }
  const page = rows.slice(0, limit);
  return {
    rows: page.map(({ parcel_number, location, owner_1, owner_2 }) => ({ parcel_number, location, owner_1, owner_2 })),
    has_more: rows.length > limit,
    next_cursor: rows.length > limit ? String(page.at(-1).parcel_number) : null,
    source: { table: 'opa_properties_public', url: 'https://cityofphiladelphia.github.io/carto-api-explorer/#opa_properties_public' },
    retrieved_at: now(),
    note: 'Philadelphia OPA recorded names only. Same-name matches may be different people. Simple two-part names are checked in both orders; complex names remain exact. This is not a verified identity or complete ownership portfolio.',
  };
}
