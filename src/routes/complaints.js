// src/routes/complaints.js
import express from "express";
import { requirePaidAccess, resolvePaidAccess } from '../services/paidAccess.js';

const CARTO_SQL_API = "https://phl.carto.com/api/v2/sql";

async function carto(sql) {
  const url = `${CARTO_SQL_API}?q=${encodeURIComponent(sql)}`;
  const res = await fetch(url, { headers: { Accept: "application/json" }, signal: AbortSignal.timeout(15000) });
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

export function createComplaintsRouter({ query = carto, authorize = resolvePaidAccess } = {}) {
const router = express.Router();

/**
 * ✅ Schema probe (NO system tables)
 * GET /api/complaints/columns
 */
router.get("/columns", async (req, res) => {
  try {
    const out = await query(`SELECT * FROM complaints LIMIT 0`);
    return res.json({
      ok: true,
      fields: out.fields || {},
    });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// Free aggregate of L&I complaints, distinct from public 311 service requests.
router.get("/summary", async (req, res) => {
  const opa = cleanOpa(req.query.opa);
  if (!opa) return opaRequired(res);

  try {
    const sql = `
      SELECT complaintstatus, casestatus,
        COUNT(*)::bigint AS total_count,
        MAX(complaintdate) AS last_activity
      FROM complaints
      WHERE opa_account_num = '${opa}'
      GROUP BY complaintstatus, casestatus
      ORDER BY complaintstatus NULLS LAST, casestatus NULLS LAST
    `;
    const out = await query(sql);
    if (!Array.isArray(out?.rows)) throw new Error('Invalid complaints response');
    let total = 0;
    let last = null;
    const statuses = out.rows.map(row => {
      const count = Number(row.total_count);
      if (row.total_count == null || !Number.isSafeInteger(count) || count <= 0) {
        throw new Error('Invalid complaint count');
      }
      total += count;
      if (!Number.isSafeInteger(total)) throw new Error('Invalid complaint total');
      if (row.last_activity != null) {
        const date = new Date(row.last_activity);
        if (!Number.isFinite(date.getTime())) throw new Error('Invalid complaint date');
        if (last == null || date.getTime() > Date.parse(last)) last = date.toISOString();
      }
      return { complaintstatus: row.complaintstatus ?? null,
        casestatus: row.casestatus ?? null, count };
    });

    return res.json({
      ok: true,
      opa,
      total_count: total,
      open_count: null,
      last_activity: last,
      risk_level: null,
      status_counts: statuses,
      status_mapping: 'unverified',
      source: 'Philadelphia L&I complaints (CARTO complaints)',
      note: 'Raw complaint and case statuses are separate. Open counts and risk are not inferred.',
      retrieved_at: new Date().toISOString(),
    });
  } catch (e) {
    return res.status(500).json({
      ok: false,
      error: e.message,
      hint:
        "Complaint data is unavailable; no zero count or risk assessment is implied.",
    });
  }
});

// Legacy detail aliases retain raw complaint fields; case status stays separate.
router.get("/details", requirePaidAccess('pro', authorize), async (req, res) => {
  const opa = cleanOpa(req.query.opa);
  if (!opa) return opaRequired(res);

  const limit = req.query.limit === undefined ? 25 : Number(req.query.limit);
  const offset = req.query.offset === undefined ? 0 : Number(req.query.offset);
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100 ||
      !Number.isSafeInteger(offset) || offset < 0) {
    return res.status(400).json({ ok: false, code: 'INVALID_PAGINATION' });
  }

  try {
    const sql = `
      SELECT
        cartodb_id,
        complaintdate::timestamptz AS complaintdate,
        opa_account_num, complaintnumber, casenumber,
        complaintstatus, casestatus,
        complaintstatus AS status,
        address,
        complaintcodename AS complaint,
        complaintcode AS complainttype,
        unitresponsible AS department,
        complaintresolution_date,
        ticket_num_311,
        systemofrecord
      FROM complaints
      WHERE opa_account_num = '${opa}'
      ORDER BY complaintdate DESC NULLS LAST, cartodb_id DESC
      LIMIT ${limit}
      OFFSET ${offset}
    `;

    const out = await query(sql);

    if (!Array.isArray(out?.rows)) throw new Error('Invalid complaints response');
    if (out.rows.some(row => String(row.opa_account_num) !== opa)) {
      throw new Error('Complaint property identity mismatch');
    }
    return res.json({
      ok: true,
      opa,
      source: 'Philadelphia L&I complaints (CARTO complaints)',
      retrieved_at: new Date().toISOString(),
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
        "Complaint data is unavailable; retry later.",
    });
  }
});

return router;
}

export default createComplaintsRouter();
