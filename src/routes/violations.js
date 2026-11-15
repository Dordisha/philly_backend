import express from "express";
import { queryCarto } from "../lib/carto.js";
import { requirePaidAccess, resolvePaidAccess } from '../services/paidAccess.js';

export function createViolationsRouter({ query = queryCarto, authorize = resolvePaidAccess } = {}) {
const router = express.Router();

function riskLevelFromFlags({ activeCount, hasCourt, hasStopWork, hasUnsafe, hasHazardous }) {
  if (hasStopWork || hasCourt) return "Severe";
  if (hasUnsafe || hasHazardous) return "High";
  if (activeCount >= 2) return "High";
  if (activeCount === 1) return "Moderate";
  return "Low";
}

// GET /api/violations/summary?opa=363258000
router.get("/summary", async (req, res) => {
  const opa = String(req.query.opa || "").trim();

  if (!/^\d{6,12}$/.test(opa)) {
    return res.status(400).json({
      ok: false,
      code: "OPA_REQUIRED",
      message: "Query param 'opa' is required.",
    });
  }

  try {
    const sql = `
      SELECT
        COUNT(*) FILTER (WHERE casestatus <> 'CLOSED')::int AS active_count,
        COUNT(*) FILTER (WHERE casestatus = 'CLOSED')::int AS historical_count,
        MAX(COALESCE(mostrecentinvestigation, violationdate)) AS last_activity,

        MAX(CASE WHEN casestatus ILIKE '%COURT%' THEN 1 ELSE 0 END)::int AS has_court,
        MAX(CASE WHEN casestatus = 'STOP WORK' THEN 1 ELSE 0 END)::int AS has_stop_work,
        MAX(CASE WHEN violationcodetitle = 'UNSAFE STRUCTURE' THEN 1 ELSE 0 END)::int AS has_unsafe_structure,
        MAX(CASE WHEN caseprioritydesc = 'HAZARDOUS' THEN 1 ELSE 0 END)::int AS has_hazardous
      FROM violations
      WHERE opa_account_num = '${opa}'
    `.trim();

    const data = { rows: await query(sql) };

    const row = data?.rows?.[0];

    if (!row) {
      return res.json({
        ok: true,
        opa,
        active_count: 0,
        historical_count: 0,
        last_activity: null,
        risk_level: "Low",
        flags: {
          has_court: false,
          has_stop_work: false,
          has_unsafe_structure: false,
          has_hazardous: false,
        },
      });
    }

    const activeCount = Number(row.active_count || 0);
    const historicalCount = Number(row.historical_count || 0);

    const hasCourt = Number(row.has_court || 0) === 1;
    const hasStopWork = Number(row.has_stop_work || 0) === 1;
    const hasUnsafe = Number(row.has_unsafe_structure || 0) === 1;
    const hasHazardous = Number(row.has_hazardous || 0) === 1;

    const risk = riskLevelFromFlags({
      activeCount,
      hasCourt,
      hasStopWork,
      hasUnsafe,
      hasHazardous,
    });

    return res.json({
      ok: true,
      opa,
      active_count: activeCount,
      historical_count: historicalCount,
      last_activity: row.last_activity || null,
      risk_level: risk,
      flags: {
        has_court: hasCourt,
        has_stop_work: hasStopWork,
        has_unsafe_structure: hasUnsafe,
        has_hazardous: hasHazardous,
      },
    });
  } catch (e) {
    return res.status(e.status || 503).json({
      ok: false,
      code: "CARTO_FAILED",
      message: String(e?.message || e),
    });
  }
});

// GET /api/violations/details?opa=363258000&limit=200
router.get("/details", requirePaidAccess('pro', authorize), async (req, res) => {
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

