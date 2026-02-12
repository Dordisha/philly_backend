// src/routes/complaints.js
import express from "express";

const router = express.Router();
const CARTO_SQL_API = "https://phl.carto.com/api/v2/sql";

async function carto(sql) {
  const url = `${CARTO_SQL_API}?q=${encodeURIComponent(sql)}`;
  const res = await fetch(url, { headers: { Accept: "application/json" } });
  const json = await res.json().catch(() => null);

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

// ✅ MAIN PAGE AGGREGATE
router.get("/summary", async (req, res) => {
  const opa = cleanOpa(req.query.opa);
  if (!opa) return opaRequired(res);

  try {
    // Start with the common 311 complaints fields used in Philly CARTO datasets.
    // If CARTO complains about any column, we'll adjust using /columns quickly.
    const sql = `
      WITH base AS (
        SELECT
          opa_account_num,
          COALESCE(status, '') AS status,
          COALESCE(service_name, '') AS service_name,
          requested_datetime::timestamptz AS requested_at
        FROM complaints
        WHERE opa_account_num = '${opa}'
      )
      SELECT
        COUNT(*)::bigint AS total_count,
        COUNT(*) FILTER (WHERE UPPER(status) NOT IN ('CLOSED','RESOLVED','COMPLETED'))::bigint AS open_count,
        MAX(requested_at) AS last_activity
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
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ✅ SECOND PAGE DETAILS (NOT PAID YET)
router.get("/details", async (req, res) => {
  const opa = cleanOpa(req.query.opa);
  if (!opa) return opaRequired(res);

  const limit = Math.min(Math.max(parseInt(req.query.limit || "25", 10), 1), 100);
  const offset = Math.max(parseInt(req.query.offset || "0", 10), 0);

  try {
    const sql = `
      SELECT
        service_request_id,
        service_name,
        status,
        requested_datetime::timestamptz AS requested_datetime,
        updated_datetime::timestamptz AS updated_datetime,
        address,
        agency_responsible,
        subject,
        description
      FROM complaints
      WHERE opa_account_num = '${opa}'
      ORDER BY requested_datetime DESC NULLS LAST
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
    return res.status(500).json({ ok: false, error: e.message });
  }
});

export default router;
