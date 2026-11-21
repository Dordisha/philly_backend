import express from "express";
import { queryCarto } from "../lib/carto.js";

export function createViolationsRouter({ query = queryCarto } = {}) {
const router = express.Router();

// Free raw-status aggregate. Case closure is not proof of violation resolution.
router.get("/summary", async (req, res) => {
  const opa = typeof req.query.opa === 'string' ? req.query.opa.trim() : '';
  res.set('Cache-Control', 'no-store');
  if (!/^\d{9}$/.test(opa)) {
    return res.status(400).json({ ok: false, code: 'OPA_REQUIRED',
      message: 'A nine-digit OPA is required.' });
  }
  try {
    const rows = await query(`
      SELECT opa_account_num, casestatus, violationstatus,
        COUNT(*)::bigint AS total_count,
        MAX(COALESCE(mostrecentinvestigation, violationdate)) AS last_activity
      FROM violations
      WHERE opa_account_num = '${opa}'
      GROUP BY opa_account_num, casestatus, violationstatus
      ORDER BY casestatus NULLS LAST, violationstatus NULLS LAST
    `.trim());
    if (!Array.isArray(rows)) throw new Error('Invalid violation summary');
    let total = 0;
    let last = null;
    const groups = new Set();
    const statuses = rows.map(row => {
      if (!row || String(row.opa_account_num) !== opa ||
          ![row.casestatus, row.violationstatus].every(v => v === null || typeof v === 'string') ||
          !(typeof row.total_count === 'number' ||
            typeof row.total_count === 'string' && /^\d+$/.test(row.total_count))) {
        throw new Error('Invalid violation group');
      }
      const count = Number(row.total_count);
      const key = JSON.stringify([row.casestatus, row.violationstatus]);
      if (!Number.isSafeInteger(count) || count < 1 || groups.has(key)) {
        throw new Error('Invalid violation count');
      }
      groups.add(key);
      total += count;
      if (!Number.isSafeInteger(total)) throw new Error('Invalid violation total');
      if (row.last_activity !== null) {
        if (typeof row.last_activity !== 'string' || !Number.isFinite(Date.parse(row.last_activity))) {
          throw new Error('Invalid violation date');
        }
        const date = new Date(row.last_activity).toISOString();
        if (last === null || date > last) last = date;
      }
      return { casestatus: row.casestatus, violationstatus: row.violationstatus, count };
    });
    return res.json({ ok: true, summary_version: 2, opa,
      total_count: total, status_counts: statuses, last_activity: last,
      active_count: null, historical_count: null, risk_level: null,
      status_mapping: 'unverified',
      source: { table: 'violations', url: 'https://cityofphiladelphia.github.io/carto-api-explorer/#violations' },
      retrieved_at: new Date().toISOString(),
      note: 'Recorded case and violation statuses are separate. Counts are records, not unique cases. Current condition, active violations and risk are not inferred.',
    });
  } catch (e) {
    return res.status(e?.status === 504 ? 504 : 503).json({ ok: false, code: 'CARTO_FAILED',
      message: 'Violation summary unavailable. No zero count or risk assessment is implied.' });
  }
});

// GET /api/violations/details?opa=363258000&limit=200
router.get("/details", async (req, res) => {
  res.set("Cache-Control", "no-store");
  const opa = String(req.query.opa || "").trim();
  const integerParam = (value, fallback) => value === undefined ? fallback :
    typeof value === 'string' && /^\d+$/.test(value) ? Number(value) : NaN;
  const limit = integerParam(req.query.limit, 200);
  const offset = integerParam(req.query.offset, 0);

  if (!/^\d{9}$/.test(opa)) {
    return res.status(400).json({
      ok: false,
      code: "OPA_REQUIRED",
      message: "Query param 'opa' is required.",
    });
  }

  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 500 ||
      !Number.isSafeInteger(offset) || offset < 0 || !Number.isSafeInteger(offset + limit)) {
    return res.status(400).json({ ok: false, code: 'INVALID_PAGINATION',
      message: 'A limit from 1 to 500 and a nonnegative integer offset are required.' });
  }

  try {
    const sql = `
      SELECT
        cartodb_id, opa_account_num,
        casenumber,
        casestatus,
        caseprioritydesc,
        casetype,
        casecreateddate,
        casecompleteddate,

        violationnumber,
        violationstatus,
        violationdate,
        violationresolutiondate,
        violationresolutioncode,
        violationcode,
        violationcodetitle
      FROM violations
      WHERE opa_account_num = '${opa}'
      ORDER BY violationdate DESC NULLS LAST, cartodb_id DESC
      LIMIT ${limit + 1} OFFSET ${offset}
    `.trim();

    const rows = await query(sql);
    if (!Array.isArray(rows) || rows.length > limit + 1) throw new Error('Invalid violation response');
    const ids = new Set();
    for (const row of rows) {
      if (!row || String(row.opa_account_num) !== opa ||
          !Number.isSafeInteger(row.cartodb_id) || row.cartodb_id < 1 || ids.has(row.cartodb_id)) {
        throw new Error('Violation identity unavailable');
      }
      ids.add(row.cartodb_id);
    }
    const page = rows.slice(0, limit);

    return res.json({
      ok: true,
      opa,
      count: page.length,
      rows: page,
      limit, offset,
      has_more: rows.length > limit,
      next_offset: rows.length > limit ? offset + limit : null,
      source: { table: 'violations', url: 'https://cityofphiladelphia.github.io/carto-api-explorer/#violations' },
      retrieved_at: new Date().toISOString(),
      note: 'Raw L&I case and violation statuses are separate. Records can change between pages.',
    });
  } catch (e) {
    return res.status(e.status === 504 ? 504 : 503).json({
      ok: false,
      code: "CARTO_FAILED",
      message: "Violation details are unavailable. Please retry.",
    });
  }
});

return router;
}

export default createViolationsRouter();

