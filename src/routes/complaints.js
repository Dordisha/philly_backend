// src/routes/complaints.js
import express from "express";

const router = express.Router();

const CARTO_SQL_API = "https://phl.carto.com/api/v2/sql";

// Small helper
async function cartoQuery(sql) {
  const url = `${CARTO_SQL_API}?q=${encodeURIComponent(sql)}`;
  const res = await fetch(url, {
    headers: { Accept: "application/json" },
  });
  const text = await res.text();
  let json;
  try {
    json = JSON.parse(text);
  } catch {
    throw new Error(`CARTO non-JSON response: ${text.slice(0, 200)}`);
  }
  if (!res.ok || json?.error) {
    const msg = json?.error?.[0] || `CARTO error: HTTP ${res.status}`;
    throw new Error(msg);
  }
  return json;
}

function opaRequired(res) {
  return res.status(400).json({
    ok: false,
    code: "OPA_REQUIRED",
    message: "Query param 'opa' is required.",
  });
}

// Normalize/validate OPA (Carto column is string)
function cleanOpa(raw) {
  const opa = String(raw || "").trim();
  if (!opa) return null;
  if (!/^\d{6,12}$/.test(opa)) return null;
  return opa;
}

/**
 * GET /api/complaints/summary?opa=112273500
 * Free summary: counts, last activity, risk flags, top complaint types
 */
router.get("/summary", async (req, res) => {
  const opa = cleanOpa(req.query.opa);
  if (!opa) return opaRequired(res);

  try {
    // NOTE: dataset is "complaints"
    // Common useful fields: service_request_id, service_name, status, requested_datetime
    // We'll keep it robust even if some fields differ by using COALESCE patterns.

    const sql = `
      WITH base AS (
        SELECT
          opa_account_num,
          service_name,
          status,
          requested_datetime::timestamptz AS requested_at
        FROM complaints
        WHERE opa_account_num = '${opa}'
      ),
      counts AS (
        SELECT
          COUNT(*)::bigint AS total_count,
          COUNT(*) FILTER (WHERE COALESCE(UPPER(status),'') NOT IN ('CLOSED','RESOLVED'))::bigint AS open_count,
          MAX(requested_at) AS last_activity
        FROM base
      ),
      top_types AS (
        SELECT
          COALESCE(service_name, 'Unknown') AS service_name,
          COUNT(*)::bigint AS cnt
        FROM base
        GROUP BY 1
        ORDER BY cnt DESC
        LIMIT 5
      )
      SELECT
        (SELECT total_count FROM counts) AS total_count,
        (SELECT open_count FROM counts) AS open_count,
        (SELECT last_activity FROM counts) AS last_activity,
        (SELECT json_agg(top_types) FROM top_types) AS top_types
    `;

    const out = await cartoQuery(sql);
    const row = out?.rows?.[0] || {};

    const total = Number(row.total_count || 0);
    const open = Number(row.open_count || 0);
    const last = row.last_activity || null;

    // Simple “signal strength” / risk heuristic (tune later)
    let risk = "Low";
    if (open >= 3) risk = "High";
    else if (open >= 1) risk = "Medium";
    else if (total >= 10) risk = "Medium";

    res.json({
      ok: true,
      opa,
      total_count: total,
      open_count: open,
      last_activity: last,
      risk_level: risk,
      top_types: Array.isArray(row.top_types) ? row.top_types : [],
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

/**
 * GET /api/complaints/details?opa=112273500&limit=25&offset=0
 * Paid details list (your app will blur if not subscribed)
 */
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
        description,
        (the_geom IS NOT NULL) AS has_geom
      FROM complaints
      WHERE opa_account_num = '${opa}'
      ORDER BY requested_datetime DESC NULLS LAST
      LIMIT ${limit}
      OFFSET ${offset}
    `;

    const out = await cartoQuery(sql);

    res.json({
      ok: true,
      opa,
      limit,
      offset,
      count: out?.rows?.length || 0,
      rows: out?.rows || [],
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

export default router;
