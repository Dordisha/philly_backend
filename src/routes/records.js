import { Router } from 'express';
import { queryCarto } from '../lib/carto.js';
import { literal, validOpa } from '../lib/propertyData.js';

const PROPERTY_FIELDS = `parcel_number, location, zip_code, unit, sale_date, sale_price,
  owner_1, owner_2, mailing_street, mailing_city_state, mailing_zip,
  total_livable_area, total_area, number_of_bedrooms, number_of_bathrooms,
  building_code_new, building_code_description_new, category_code,
  interior_condition, exterior_condition, assessment_date`;
export const propertyRecordSql = opa => `SELECT ${PROPERTY_FIELDS} FROM opa_properties_public WHERE parcel_number = ${literal(opa)} LIMIT 1`;
const source = (table, label) => ({
  label, table, url: `https://cityofphiladelphia.github.io/carto-api-explorer/#${table}`,
});
const pageNumber = (v, fallback, max) => {
  const n = Number(v);
  return Number.isSafeInteger(n) && n >= 0 ? Math.min(n, max) : fallback;
};

export function complaintsSql(property, limit, offset) {
  // 311 has no OPA column. Never silently attach neighboring requests to a parcel.
  return `SELECT service_request_id, status, service_name, service_code,
    requested_datetime, updated_datetime, closed_datetime, address, zipcode,
    agency_responsible, status_notes
    FROM public_cases_fc WHERE address = ${literal(property.location)}
    AND zipcode = ${literal(String(property.zip_code || '').slice(0, 5))}
    ORDER BY requested_datetime DESC NULLS LAST, service_request_id DESC
    LIMIT ${limit + 1} OFFSET ${offset}`;
}
export function nearbySalesSql(opa, limit, offset) {
  return `SELECT c.parcel_number, c.location, c.sale_date, c.sale_price,
    c.total_livable_area, c.total_area, c.number_of_bedrooms, c.number_of_bathrooms,
    c.building_code_description_new, c.interior_condition, c.exterior_condition,
    c.assessment_date, ST_Distance(c.the_geom::geography,s.the_geom::geography) AS distance_m
    FROM opa_properties_public c JOIN opa_properties_public s ON s.parcel_number = ${literal(opa)}
    WHERE c.parcel_number <> s.parcel_number AND c.category_code = s.category_code
    AND c.building_code_new = s.building_code_new
    AND s.total_livable_area > 0
    AND c.total_livable_area BETWEEN s.total_livable_area * 0.75 AND s.total_livable_area * 1.25
    AND c.sale_price > 0 AND c.sale_date >= CURRENT_DATE - INTERVAL '24 months'
    AND c.sale_date <= CURRENT_DATE
    AND ST_DWithin(c.the_geom, s.the_geom, 0.012)
    AND ST_DWithin(c.the_geom::geography, s.the_geom::geography, 800)
    ORDER BY distance_m, c.sale_date DESC, c.parcel_number
    LIMIT ${limit + 1} OFFSET ${offset}`;
}

export function propertySignals(property, at) {
  const factors = [];
  const sold = property.sale_date ? new Date(property.sale_date) : null;
  const checked = new Date(at);
  if (sold && Number.isFinite(sold.getTime()) && sold <= checked) {
    const years = Math.floor((checked - sold) / (365.25 * 24 * 60 * 60 * 1000));
    if (years >= 15) factors.push({
      title: 'Long interval since recorded sale', strength: 'Weak context',
      evidence: `Latest OPA sale date: ${property.sale_date}; approximately ${years} years ago.`,
      explanation: 'Rule: at least 15 years since recorded sale. This is not proof of ownership duration or an intention to sell.',
    });
  }
  const normalize = value => String(value || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
  const mailing = normalize(property.mailing_street);
  const site = normalize(property.location);
  if (mailing && site && mailing !== site) factors.push({
    title: 'Different published mailing address', strength: 'Weak context',
    evidence: `OPA mailing street: ${property.mailing_street}; property: ${property.location}.`,
    explanation: 'Rule: published mailing street differs from site address after punctuation/space normalization. Formatting or shared addresses may explain the difference; it does not prove absentee ownership or interest in selling.',
  });
  return {
    label: factors.length ? 'Weak evidence only' : 'Insufficient evidence',
    method_version: 'property-context-v1',
    listing_status: 'Unknown — no authorized listing feed connected',
    note: 'Property-record context only. No direct selling-intent or listing evidence is available. No probability of sale or accepted offer price is calculated.',
    factors,
  };
}

export function createRecordsRouter({ query = queryCarto, now = () => new Date().toISOString() } = {}) {
  const router = Router();
  for (const section of ['311', 'violations', 'sales', 'facts']) {
    router.get('/' + section, async (req, res) => {
      const opa = typeof req.query.opa === 'string' ? req.query.opa.trim() : '';
      if (!validOpa(opa)) return res.status(400).json({ ok: false, message: 'A valid OPA is required.' });
      const limit = Math.max(1, pageNumber(req.query.limit, 25, 50));
      const offset = pageNumber(req.query.offset, 0, 10000);
      try {
        const properties = await query(propertyRecordSql(opa));
        if (!properties.length) return res.status(404).json({ ok: false, message: 'Property not found.' });
        const property = properties[0];
        let rows;
        let provenance;
        let note;
        if (section === '311') {
          if (!property.location || !property.zip_code) {
            return res.status(422).json({ ok: false, message: 'Address matching is unavailable for this parcel.' });
          }
          rows = await query(complaintsSql(property, limit, offset));
          provenance = source('public_cases_fc', 'Philadelphia public 311 service requests');
          note = 'Exact published address and ZIP match; 311 records have no parcel ID. Reports may concern a street or shared building. Public records only; this is not a complete history and complaints are not confirmed violations.';
        } else if (section === 'violations') {
          rows = await query(`SELECT casenumber, casestatus, caseprioritydesc, casetype,
            casecreateddate, casecompleteddate, violationnumber, violationstatus,
            violationdate, violationresolutiondate, violationresolutioncode,
            violationcode, violationcodetitle FROM violations
            WHERE opa_account_num = ${literal(opa)}
            ORDER BY violationdate DESC NULLS LAST, violationnumber, casenumber
            LIMIT ${limit + 1} OFFSET ${offset}`);
          provenance = source('violations', 'Philadelphia L&I violation records');
          note = 'Matched by OPA parcel ID. Case and violation statuses are shown as published, without inferring current building condition. Dates/statuses may lag city updates.';
        } else if (section === 'sales') {
          if (!(Number(property.total_livable_area) > 0) || !property.building_code_new || !property.category_code) {
            return res.status(422).json({ ok: false, message: 'Property type or floor area is missing; nearby sale matching is unavailable.' });
          }
          rows = await query(nearbySalesSql(opa, limit, offset));
          provenance = source('opa_properties_public', 'Philadelphia OPA property records');
          note = 'Latest recorded sale per parcel, within 800 m, past 24 months, same OPA category and building code, floor area within 25%. Sorted nearest first. These are candidate sales, not verified comparable transactions. Nominal/related-party transfers may be present. Raw condition codes do not establish renovation status. Renovation status is not inferred from these codes.';
        } else {
          rows = [property];
          provenance = source('opa_properties_public', 'Philadelphia OPA property records');
          note = 'Published property facts only. No conclusion about selling interest, mortgage balance or renovation condition. Listing status is unknown: no listing feed is connected.';
        }
        return res.json({ ok: true, opa, address: property.location,
          retrieved_at: now(), source: provenance, note, limit, offset,
          subject_area: property.total_livable_area ?? null,
          ...(section === 'facts' ? { signals: propertySignals(property, now()) } : {}),
          has_more: rows.length > limit, rows: rows.slice(0, limit) });
      } catch (error) {
        return res.status(error.status === 504 ? 504 : 503).json({
          ok: false, message: 'City records are temporarily unavailable. Please retry.',
        });
      }
    });
  }
  return router;
}
export default createRecordsRouter();
