import express from "express";

const router = express.Router();

const CARTO_URL = "https://phl.carto.com/api/v2/sql";

function cartoUrl(sql) {
  return `${CARTO_URL}?q=${encodeURIComponent(sql)}`;
}

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

  if (!opa) {
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

    const r = await fetch(cartoUrl(sql));
    const data = await r.json();

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
    return res.status(500).json({
      ok: false,
      code: "CARTO_FAILED",
      message: String(e?.message || e),
    });
  }
});

// GET /api/violations/details?opa=363258000&limit=200
router.get("/details", async (req, res) => {
  const opa = String(req.query.opa || "").trim();
  const limit = Math.min(Number(req.query.limit || 200), 500);

  if (!opa) {
    return res.status(400).json({
      ok: false,
      code: "OPA_REQUIRED",
      message: "Query param 'opa' is required.",
    });
  }

  try {
    const sql = `
      SELECT
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
      ORDER BY violationdate DESC NULLS LAST
      LIMIT ${limit}
    `.trim();

    const r = await fetch(cartoUrl(sql));
    const data = await r.json();

    return res.json({
      ok: true,
      opa,
      count: Array.isArray(data?.rows) ? data.rows.length : 0,
      rows: data?.rows || [],
    });
  } catch (e) {
    return res.status(500).json({
      ok: false,
      code: "CARTO_FAILED",
      message: String(e?.message || e),
    });
  }
});

export default router;
