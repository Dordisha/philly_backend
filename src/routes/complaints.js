// src/routes/complaints.js
import express from "express";

const router = express.Router();
const CARTO_SQL_API = "https://phl.carto.com/api/v2/sql";

async function carto(sql) {
  const url = `${CARTO_SQL_API}?q=${encodeURIComponent(sql)}`;
  const res = await fetch(url, { headers: { Accept: "application/json" } });
  const text = await res.text();

  let json;
  try {
    json = JSON.parse(text);
  } catch {
    throw new Error(`CARTO non-JSON response: ${text.slice(0, 200)}`);
  }

  if (!res.ok || json?.error) {
    const msg = json?.error?.[0] || `CARTO error HTTP ${res.status}`;
    throw new Error(msg);
  }
  return json;
}

function cleanOpa(raw) {
  const opa = String(raw || "").trim();
  if (!opa) return null;
  if (!/^\d{6,12}$/.test(opa)) return null;
  return opa;
}

function opaRequired(res) {
  return res.status(400).json({
    ok: false,
    code: "OPA_REQUIRED",
    message: "Query param 'opa' is required.",
  });
}

/**
 * ✅ Schema probe (NO system tables)
 * GET /api/complaints/columns
 */
router.get("/columns", async (req, res) => {
  try {
    const out = await carto(`SELECT * FROM complaints LIMIT 0`);
    return res.json({
      ok: true,
      fields: out.fields || {},
    });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e.message });
  }
});

/**
 * ✅ MAIN PAGE AGGREGATE
 * GET /api/complaints/summary?opa=112273500
 *
 * IMPORTANT:
 * This "complaints" table does NOT have service_name/service_request_id.
 * It appears to have complaintdate (based on common Philly complaints table patterns).
 */
router.get("/summary", async (req, res) => {
  const opa = cleanOpa(req.query.opa);
  if (!opa) return opaRequired(res);

  try {
    const sql = `
      WITH base AS (
        SELECT
          opa_account_num,
          complaintdate::timestamptz AS complaint_at,
          COALESCE(status, '') AS status
        FROM complaints
        WHERE opa_account_num = '${opa}'
      )
      SELECT
        COUNT(*)::bigint AS total_count,
        COUNT(*) FILTER (
          WHERE status IS NOT NULL
          AND TRIM(status) <> ''
          AND UPPER(status) NOT IN ('CLOSED','RESOLVED','COMPLETED')
        )::bigint AS open_count,
        MAX(complaint_at) AS last_activity
      FROM base
    `;

    const out = await carto(sql);
    const row = out?.rows?.[0] || {};

    const total = Number(row.total_count || 0);
    const open = Number(row.open_count || 0);
    const last = row.last_activity || null;

    // Simple risk heuristic (tune later)
    let risk = "Low";
    if (open >= 3) risk = "High";
    else if (open >= 1) risk = "Medium";
    else if (total >= 10) risk = "Medium";

    return res.json({
      ok: true,
      opa,
      total_count: total,
      open_count: open,
      last_activity: last,
      risk_level: risk,
    });
  } catch (e) {
    return res.status(500).json({
      ok: false,
      error: e.message,
      hint:
        "Run GET /api/complaints/columns to confirm exact column names. If complaintdate/status differ, we will map them.",
    });
  }
});

/**
 * ✅ SECOND PAGE DETAILS (NOT PAID YET)
 * GET /api/complaints/details?opa=...&limit=10&offset=0
 *
 * We return a "safe" common set of likely columns.
 * If any column name differs, /columns will tell us and we adjust fast.
 */
router.get("/details", async (req, res) => {
  const opa = cleanOpa(req.query.opa);
  if (!opa) return opaRequired(res);

  const limit = Math.min(Math.max(parseInt(req.query.limit || "25", 10), 1), 100);
  const offset = Math.max(parseInt(req.query.offset || "0", 10), 0);

  try {
    const sql = `
      SELECT
        cartodb_id,
        complaintdate::timestamptz AS complaintdate,
        status,
        address,
        complaint,
        complainttype,
        agency,
        department
      FROM complaints
      WHERE opa_account_num = '${opa}'
      ORDER BY complaintdate DESC NULLS LAST
      LIMIT ${limit}
      OFFSET ${offset}
    `;

    const out = await carto(sql);

    return res.json({
      ok: true,
      opa,
      limit,
      offset,
      count: out?.rows?.length || 0,
      rows: out?.rows || [],
    });
  } catch (e) {
    return res.status(500).json({
      ok: false,
      error: e.message,
      hint:
        "Run GET /api/complaints/columns to confirm exact column names. Then we will align complaint/status/type fields.",
    });
  }
});

export default router;
