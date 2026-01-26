// src/routes/opa.js
import { Router } from "express";
import { fileURLToPath } from "url";
import { runAthena } from "../lib/athenaRun.js";

const router = Router();
const __filename = fileURLToPath(import.meta.url);
console.log(`📁 opa.js loaded from: file://${__filename}`);

// =========================
// Config
// =========================
const DATABASE = process.env.ATHENA_DATABASE || "philly_data";

// Optimized table (NO location column)
const TABLE_LOOKUP = process.env.ATHENA_TABLE || "opa_properties_lookup2";

// Raw table (HAS location column) for address search only
const TABLE_PUBLIC = process.env.OPA_PUBLIC_TABLE || "opa_properties_public";

// Column mappings (shared)
const COL_OPA = process.env.OPA_COL_OPA || "parcel_number";
const COL_OWNER1 = process.env.OPA_COL_OWNER || "owner_1";
const COL_MARKET = process.env.OPA_COL_MARKET_VALUE || "market_value";
const COL_SPRICE = process.env.OPA_COL_SALE_PRICE || "sale_price";
const COL_SDATE = process.env.OPA_COL_SALE_DATE || "sale_date";

// Known columns
const COL_OWNER2 = "owner_2";

// Address part columns
const COL_HNO = "house_number";
const COL_SDIR = "street_direction";
const COL_SNAME = "street_name";
const COL_SDES = "street_designation";
const COL_SUFFIX = "suffix";
const COL_UNIT = "unit";
const COL_ZIP = "zip_code";

// Only exists in PUBLIC table
const COL_LOC = "location";

// Exists in PUBLIC table
const COL_ZONING = process.env.OPA_COL_ZONING || "zoning";

// =========================
// Helpers
// =========================
const toNumber = (v) => {
  if (v == null) return null;
  const s = String(v).replace(/[, ]/g, "").trim();
  if (!s) return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
};

const sanitizeLike = (s) =>
  String(s)
    .replace(/'/g, "''")
    .replace(/%/g, "\\%")
    .replace(/_/g, "\\_")
    .trim();

const normalizeAddressForMatch = (raw) =>
  String(raw)
    .toUpperCase()
    .replace(/[,\t]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const normalizeAddressOut = (raw) =>
  raw == null ? null : String(raw).replace(/\s+/g, " ").trim();

const said = (v) => v != null && String(v).trim().length > 0;

// =========================
// Ping
// =========================
router.get("/_ping", (req, res) =>
  res.json({ ok: true, route: "/api/opa/_ping", ts: Date.now() })
);

// =========================
// Zoning (PUBLIC) by OPA
// =========================
async function fetchZoningFromPublicByOpa(opaRaw) {
  const q = `
    SELECT
      NULLIF(TRIM(${COL_ZONING}), '') AS zoning
    FROM ${DATABASE}.${TABLE_PUBLIC}
    WHERE ${COL_OPA} = '${opaRaw}'
    LIMIT 1
  `;

  const rows = await runAthena(q);
  if (!rows.length) return null;
  const v = rows[0].zoning;
  return v ? String(v).trim() : null;
}

// =========================
// OPA lookup (FAST, optimized)
// =========================
async function lookupByOpa(opa) {
  const opaRaw = String(opa).trim();
  const pnPrefix2 = opaRaw.substring(0, 2);

  const q = `
    SELECT
      ${COL_OPA}    AS opa_number,
      ${COL_OWNER1} AS owner_1,
      COALESCE(${COL_OWNER2}, '') AS owner_2,
      CAST(COALESCE(NULLIF(${COL_MARKET}, ''), '0') AS BIGINT) AS market_value,
      CAST(COALESCE(NULLIF(${COL_SPRICE}, ''), '0') AS BIGINT) AS sale_price,
      COALESCE(
        DATE_FORMAT(
          TRY(FROM_ISO8601_TIMESTAMP(REGEXP_REPLACE(NULLIF(${COL_SDATE}, ''), ' ', 'T'))),
          '%Y-%m-%d'
        ),
        SUBSTR(${COL_SDATE}, 1, 10)
      ) AS sale_date,
      ${COL_HNO} AS house_number,
      ${COL_SDIR} AS street_direction,
      ${COL_SNAME} AS street_name,
      ${COL_SDES} AS street_designation,
      ${COL_SUFFIX} AS suffix,
      ${COL_UNIT} AS unit,
      ${COL_ZIP} AS zip_code
    FROM ${DATABASE}.${TABLE_LOOKUP}
    WHERE pn_prefix2 = '${pnPrefix2}'
      AND ${COL_OPA} = '${opaRaw}'
    LIMIT 1
  `;

  const rows = await runAthena(q);
  if (!rows.length) return null;

  const r = rows[0];

  const ownerCombined =
    [r.owner_1, r.owner_2].filter(Boolean).join(" & ") || null;

  const streetLine = [
    r.house_number,
    r.street_direction,
    r.street_name,
    r.street_designation,
    r.suffix,
  ]
    .filter(Boolean)
    .join(" ");

  const address =
    normalizeAddressOut([streetLine, r.unit].filter(Boolean).join(" ")) || null;

  const zoning = await fetchZoningFromPublicByOpa(opaRaw);

  return {
    opa: r.opa_number,
    address,
    owner: ownerCombined,
    market_value: toNumber(r.market_value),
    sale_price: toNumber(r.sale_price),
    sale_date: r.sale_date || null,
    zoning,
    tax: { lookup_url: "https://tax-services.phila.gov/_/" },
  };
}

// =========================
// /search endpoint
// =========================
router.get("/search", async (req, res) => {
  try {
    const { opa, address, limit = "5" } = req.query;

    // ---- OPA mode
    if (opa && said(opa)) {
      if (!/^\d{6,12}$/.test(String(opa))) {
        return res.status(400).json({ error: "Invalid OPA format" });
      }
      const result = await lookupByOpa(opa);
      if (!result)
        return res.status(404).json({ error: "OPA not found", opa });
      return res.json({ ok: true, mode: "opa", result });
    }

    // ---- Address mode
    if (!address || !said(address)) {
      return res
        .status(400)
        .json({ error: "Missing ?address (or provide ?opa=#########)" });
    }

    const lim = Math.min(Math.max(parseInt(limit, 10) || 5, 1), 25);

    const addrNorm = normalizeAddressForMatch(address);
    const addrSansCity = addrNorm
      .replace(/\b(PHILADELPHIA|PA|PENNSYLVANIA|USA)\b/g, " ")
      .replace(/\s+/g, " ")
      .trim();

    const zipMatch = addrSansCity.match(/\b\d{5}\b/);
    const zip = zipMatch ? zipMatch[0] : null;

    const addrCore = zip
      ? addrSansCity.replace(zip, "").replace(/\s+/g, " ").trim()
      : addrSansCity;

    const addrLike = sanitizeLike(addrCore);

    const q = `
      WITH base AS (
        SELECT
          ${COL_OPA}      AS opa_number,
          ${COL_LOC}      AS location,
          ${COL_OWNER1}   AS owner_1,
          COALESCE(${COL_OWNER2}, '') AS owner_2,
          CAST(COALESCE(NULLIF(${COL_MARKET}, ''), '0') AS BIGINT) AS market_value,
          CAST(COALESCE(NULLIF(${COL_SPRICE}, ''), '0') AS BIGINT) AS sale_price,
          COALESCE(
            DATE_FORMAT(
              TRY(FROM_ISO8601_TIMESTAMP(REGEXP_REPLACE(NULLIF(${COL_SDATE}, ''), ' ', 'T'))),
              '%Y-%m-%d'
            ),
            SUBSTR(${COL_SDATE}, 1, 10)
          ) AS sale_date,
          ${COL_ZIP} AS zip_code,
          TRIM(CONCAT_WS(
            ' ',
            ${COL_HNO},
            NULLIF(${COL_SDIR}, ''),
            NULLIF(${COL_SNAME}, ''),
            NULLIF(${COL_SDES}, '')
          )) AS rebuilt_addr
        FROM ${DATABASE}.${TABLE_PUBLIC}
      )
      SELECT
        opa_number, location, owner_1, owner_2,
        market_value, sale_price, sale_date
      FROM base
      WHERE
        (
          UPPER(location) LIKE '%' || UPPER('${addrLike}') || '%'
          OR UPPER(rebuilt_addr) LIKE '%' || UPPER('${addrLike}') || '%'
        )
        ${zip ? `AND zip_code = '${zip}'` : ``}
      ORDER BY location
      LIMIT ${lim}
    `;

    const rows = await runAthena(q);

    const results = rows.map((r) => {
      const ownerCombined =
        [r.owner_1, r.owner_2].filter(Boolean).join(" & ") || null;
      return {
        opa: r.opa_number,
        address: normalizeAddressOut(r.location) || null,
        owner: ownerCombined,
        market_value: toNumber(r.market_value),
        sale_price: toNumber(r.sale_price),
        sale_date: r.sale_date || null,
        tax: { lookup_url: "https://tax-services.phila.gov/_/" },
      };
    });

    return res.json({
      ok: true,
      mode: "address",
      query: address,
      count: results.length,
      results,
    });
  } catch (e) {
    console.error("[OPA/SEARCH] error:", e);
    return res.status(500).json({ error: e.message });
  }
});

// =========================
// Detail by OPA
// =========================
router.get("/", async (req, res) => {
  try {
    const { opa } = req.query;
    if (!opa)
      return res.status(400).json({ error: "Missing ?opa=OPA_NUMBER" });

    if (!/^\d{6,12}$/.test(String(opa))) {
      return res.status(400).json({ error: "Invalid OPA format" });
    }

    const result = await lookupByOpa(opa);
    if (!result)
      return res.status(404).json({ error: "OPA not found", opa });

    return res.json({ ok: true, mode: "opa", ...result });
  } catch (e) {
    console.error("[OPA/DETAIL] error:", e);
    return res.status(500).json({ error: e.message });
  }
});

export default router;
