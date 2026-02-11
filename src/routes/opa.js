// ✅ FILE: philly_backend/src/routes/opa.js
// FULL FILE — SMART suggest (exact + range-aware + block fallback)
// - Fixes: 524 MARK no longer returns 5240... noise (when in exact mode)
// - Fixes: 526 MARK returns same-block suggestions even if 526 parcel doesn't exist
// - Keeps: strict address search on LOOKUP2 + fuzzy only when no house number
//
// NOTE: Debug routes are included and are defined BEFORE export default.

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

// Optimized table (structured)
const TABLE_LOOKUP = process.env.ATHENA_TABLE || "opa_properties_lookup2";

// Raw/public table (string location)
const TABLE_PUBLIC = process.env.OPA_PUBLIC_TABLE || "opa_properties_public";

// Column mappings
const COL_OPA = process.env.OPA_COL_OPA || "parcel_number";
const COL_OWNER1 = process.env.OPA_COL_OWNER || "owner_1";
const COL_MARKET = process.env.OPA_COL_MARKET_VALUE || "market_value";
const COL_SPRICE = process.env.OPA_COL_SALE_PRICE || "sale_price";
const COL_SDATE = process.env.OPA_COL_SALE_DATE || "sale_date";
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

// Zoning exists in PUBLIC table
const COL_ZONING = process.env.OPA_COL_ZONING || "zoning";

// =========================
// Build stamp (for Render verification)
// =========================
const BUILD_STAMP =
  process.env.RENDER_GIT_COMMIT ||
  process.env.GIT_COMMIT ||
  process.env.COMMIT_SHA ||
  process.env.SOURCE_VERSION ||
  `local-${new Date().toISOString()}`;

// =========================
// helpers
// =========================
const toNumber = (v) => {
  if (v == null) return null;
  const s = String(v).replace(/[, ]/g, "").trim();
  if (!s) return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
};

const sanitizeSql = (s) => String(s).replace(/'/g, "''").trim();

const normalizeAddressForMatch = (raw) =>
  String(raw).toUpperCase().replace(/[,\t]+/g, " ").replace(/\s+/g, " ").trim();

const normalizeAddressOut = (raw) =>
  raw == null ? null : String(raw).replace(/\s+/g, " ").trim();

function said(v) {
  return v != null && String(v).trim().length > 0;
}

// Parsing helpers
const DIRS = new Set(["N", "S", "E", "W", "NE", "NW", "SE", "SW"]);
const DESIG = new Set([
  "ST",
  "STREET",
  "AVE",
  "AV",
  "AVENUE",
  "RD",
  "ROAD",
  "BLVD",
  "BOULEVARD",
  "DR",
  "DRIVE",
  "LN",
  "LANE",
  "CT",
  "COURT",
  "PL",
  "PLACE",
  "PKWY",
  "PARKWAY",
  "CIR",
  "CIRCLE",
  "TER",
  "TERRACE",
  "WAY",
]);

function stripCityStateZip(addrUpper) {
  return addrUpper
    .replace(/\b(PHILADELPHIA|PA|PENNSYLVANIA|USA)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function extractZip(addrUpper) {
  const m = addrUpper.match(/\b\d{5}\b/);
  return m ? m[0] : null;
}

function removeZip(addrUpper, zip) {
  if (!zip) return addrUpper;
  return addrUpper.replace(zip, " ").replace(/\s+/g, " ").trim();
}

function parseAddressParts(addrCoreUpper) {
  const m = addrCoreUpper.match(/^(\d+)\s+(.+)$/);
  if (!m) {
    return {
      houseNumber: null,
      streetDirection: null,
      streetName: null,
      streetDesignation: null,
    };
  }

  const houseNumber = parseInt(m[1], 10);
  if (!Number.isFinite(houseNumber)) {
    return {
      houseNumber: null,
      streetDirection: null,
      streetName: null,
      streetDesignation: null,
    };
  }

  let rest = m[2].trim();
  rest = rest.replace(/\b(APT|APARTMENT|UNIT|STE|SUITE|#)\b.*$/i, "").trim();

  const tokens = rest.split(/\s+/).filter(Boolean).map((t) => t.toUpperCase());
  if (!tokens.length) {
    return { houseNumber, streetDirection: null, streetName: null, streetDesignation: null };
  }

  let streetDirection = null;
  if (tokens.length && DIRS.has(tokens[0])) streetDirection = tokens.shift();

  let streetDesignation = null;
  if (tokens.length && DESIG.has(tokens[tokens.length - 1])) streetDesignation = tokens.pop();

  const streetName = tokens.length ? tokens.join(" ") : null;
  return { houseNumber, streetDirection, streetName, streetDesignation };
}

// Suggest parser: supports "1539 S Lam", "526 mar", "1803 S BRO"
function parseSuggestParts(coreUpper) {
  const q = coreUpper.trim();
  const m = q.match(/^(\d+)\s*(.*)$/);
  if (!m) return { housePrefix: null, streetDir: null, streetPrefix: q };

  const housePrefix = m[1] || null;

  let rest = (m[2] || "").trim();
  rest = rest.replace(/\b(APT|APARTMENT|UNIT|STE|SUITE|#)\b.*$/i, "").trim();

  const tokens = rest.split(/\s+/).filter(Boolean).map((t) => t.toUpperCase());

  let streetDir = null;
  if (tokens.length && DIRS.has(tokens[0])) streetDir = tokens.shift();

  // remove trailing designation for prefix searches
  if (tokens.length && DESIG.has(tokens[tokens.length - 1])) tokens.pop();

  const streetPrefix = tokens.join(" ").trim();
  return { housePrefix, streetDir, streetPrefix };
}

function buildAddrSql() {
  return `
    TRIM(CONCAT_WS(
      ' ',
      CAST(${COL_HNO} AS VARCHAR),
      NULLIF(${COL_SDIR}, ''),
      NULLIF(${COL_SNAME}, ''),
      NULLIF(${COL_SDES}, ''),
      NULLIF(${COL_SUFFIX}, '')
    ))
  `;
}

// =========================
// Zoning (PUBLIC) by OPA
// =========================
async function fetchZoningFromPublicByOpa(opaRaw) {
  const q = `
    SELECT NULLIF(TRIM(${COL_ZONING}), '') AS zoning
    FROM ${DATABASE}.${TABLE_PUBLIC}
    WHERE ${COL_OPA} = '${sanitizeSql(opaRaw)}'
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
    WHERE pn_prefix2 = '${sanitizeSql(pnPrefix2)}'
      AND ${COL_OPA} = '${sanitizeSql(opaRaw)}'
    LIMIT 1
  `;

  const rows = await runAthena(q);
  if (!rows.length) return null;

  const r = rows[0];
  const ownerCombined = [r.owner_1, r.owner_2].filter(Boolean).join(" & ") || null;

  const streetLine = [
    r.house_number,
    r.street_direction,
    r.street_name,
    r.street_designation,
    r.suffix,
  ]
    .filter(Boolean)
    .join(" ");

  const address = normalizeAddressOut([streetLine, r.unit].filter(Boolean).join(" ")) || null;
  const zoning = await fetchZoningFromPublicByOpa(opaRaw);

  return {
    opa: r.opa_number,
    address,
    owner: ownerCombined,
    market_value: toNumber(r.market_value),
    sale_price: toNumber(r.sale_price),
    sale_date: r.sale_date || null,
    zoning: zoning || null,
    tax: { lookup_url: "https://tax-services.phila.gov/_/" },
  };
}

// =========================
// ✅ SUGGESTIONS — SMART (exact + range-aware + block fallback)
// =========================
async function fetchSuggestionsFromLookup(rawAddress, lim = 10) {
  try {
    const qNorm = normalizeAddressForMatch(rawAddress);
    const qSansCity = stripCityStateZip(qNorm);
    const zip = extractZip(qSansCity);
    const core = removeZip(qSansCity, zip);

    const { housePrefix, streetDir, streetPrefix } = parseSuggestParts(core);
    const limitSql = Math.min(Math.max(parseInt(lim, 10) || 10, 1), 25);

    if (!housePrefix && (!streetPrefix || streetPrefix.length < 3)) return [];

    const housePrefixSql = housePrefix ? sanitizeSql(housePrefix) : null;
    const streetDirSql = streetDir ? sanitizeSql(streetDir) : null;
    const streetPrefixSql = streetPrefix ? sanitizeSql(streetPrefix) : null;

    const addrSql = buildAddrSql();

    const streetComboSql = `
      UPPER(TRIM(CONCAT_WS(
        ' ',
        COALESCE(${COL_SNAME}, ''),
        COALESCE(${COL_SDES}, ''),
        COALESCE(${COL_SUFFIX}, '')
      )))
    `;

    const houseDigits = housePrefixSql ? String(housePrefixSql).length : 0;
    const houseInt = housePrefixSql ? parseInt(String(housePrefixSql), 10) : null;
    const isExactHouseMode = houseDigits >= 3 && Number.isFinite(houseInt);

    const rangeEndFrom = (tokenExpr) => `
      CASE
        WHEN length(split_part(${tokenExpr}, '-', 2)) < length(split_part(${tokenExpr}, '-', 1))
          THEN
            (
              CAST(
                floor(
                  TRY_CAST(split_part(${tokenExpr}, '-', 1) AS DOUBLE)
                  / pow(10, length(split_part(${tokenExpr}, '-', 2)))
                ) AS INTEGER
              )
              * CAST(pow(10, length(split_part(${tokenExpr}, '-', 2))) AS INTEGER)
            )
            + TRY_CAST(split_part(${tokenExpr}, '-', 2) AS INTEGER)
        ELSE TRY_CAST(split_part(${tokenExpr}, '-', 2) AS INTEGER)
      END
    `;

    const hnoStr = `TRIM(CAST(${COL_HNO} AS VARCHAR))`;

    const houseMatchLookup = isExactHouseMode
      ? `
        (
          ${hnoStr} = '${housePrefixSql}'
          OR (
            regexp_like(${hnoStr}, '^\\d+-\\d+$')
            AND ${houseInt} BETWEEN
              TRY_CAST(split_part(${hnoStr}, '-', 1) AS INTEGER)
              AND ${rangeEndFrom(hnoStr)}
          )
        )
      `
      : housePrefixSql
        ? `starts_with(${hnoStr}, '${housePrefixSql}')`
        : `1=1`;

    const whereLookupBase = `
      1=1
      ${zip ? `AND ${COL_ZIP} = '${sanitizeSql(zip)}'` : ``}
      ${housePrefixSql ? `AND ${houseMatchLookup}` : ``}
      ${streetPrefixSql ? `AND ${streetComboSql} LIKE UPPER('${streetPrefixSql}%')` : ``}
    `;

    const qLookupPreferredDir =
      streetDirSql && streetPrefixSql
        ? `
          SELECT ${COL_OPA} AS opa_number, ${addrSql} AS address_out, ${COL_ZIP} AS zip_code
          FROM ${DATABASE}.${TABLE_LOOKUP}
          WHERE ${whereLookupBase}
            AND UPPER(COALESCE(${COL_SDIR}, '')) = UPPER('${streetDirSql}')
          ORDER BY address_out
          LIMIT ${limitSql}
        `
        : null;

    const qLookupNoDir = `
      SELECT ${COL_OPA} AS opa_number, ${addrSql} AS address_out, ${COL_ZIP} AS zip_code
      FROM ${DATABASE}.${TABLE_LOOKUP}
      WHERE ${whereLookupBase}
      ORDER BY address_out
      LIMIT ${limitSql}
    `;

    let rows = [];
    if (qLookupPreferredDir) rows = await runAthena(qLookupPreferredDir);
    if (!rows.length) rows = await runAthena(qLookupNoDir);

    let suggestions =
      (rows || [])
        .map((r) => ({
          address: normalizeAddressOut(r.address_out) || null,
          opa: r.opa_number ? String(r.opa_number).trim() : null,
          zip: r.zip_code ? String(r.zip_code).trim() : null,
        }))
        .filter((s) => s.address && s.opa) || [];

    if (suggestions.length) return suggestions;

    if (!isExactHouseMode || !streetPrefixSql) return [];

    const blockStart = Math.floor(houseInt / 100) * 100;
    const blockEnd = blockStart + 99;

    const rangeOverlapsBlock = `
      (
        regexp_like(${hnoStr}, '^\\d+-\\d+$')
        AND (
          TRY_CAST(split_part(${hnoStr}, '-', 1) AS INTEGER) <= ${blockEnd}
          AND ${rangeEndFrom(hnoStr)} >= ${blockStart}
        )
      )
    `;

    const qBlockPreferredDir = streetDirSql
      ? `
        SELECT ${COL_OPA} AS opa_number, ${addrSql} AS address_out, ${COL_ZIP} AS zip_code
        FROM ${DATABASE}.${TABLE_LOOKUP}
        WHERE
          1=1
          ${zip ? `AND ${COL_ZIP} = '${sanitizeSql(zip)}'` : ``}
          AND ${streetComboSql} LIKE UPPER('${streetPrefixSql}%')
          AND UPPER(COALESCE(${COL_SDIR}, '')) = UPPER('${streetDirSql}')
          AND (
            (regexp_like(${hnoStr}, '^\\d+$') AND TRY_CAST(${hnoStr} AS INTEGER) BETWEEN ${blockStart} AND ${blockEnd})
            OR ${rangeOverlapsBlock}
          )
        ORDER BY address_out
        LIMIT ${limitSql}
      `
      : null;

    const qBlockNoDir = `
      SELECT ${COL_OPA} AS opa_number, ${addrSql} AS address_out, ${COL_ZIP} AS zip_code
      FROM ${DATABASE}.${TABLE_LOOKUP}
      WHERE
        1=1
        ${zip ? `AND ${COL_ZIP} = '${sanitizeSql(zip)}'` : ``}
        AND ${streetComboSql} LIKE UPPER('${streetPrefixSql}%')
        AND (
          (regexp_like(${hnoStr}, '^\\d+$') AND TRY_CAST(${hnoStr} AS INTEGER) BETWEEN ${blockStart} AND ${blockEnd})
          OR ${rangeOverlapsBlock}
        )
      ORDER BY address_out
      LIMIT ${limitSql}
    `;

    let blockRows = [];
    if (qBlockPreferredDir) blockRows = await runAthena(qBlockPreferredDir);
    if (!blockRows.length) blockRows = await runAthena(qBlockNoDir);

    suggestions =
      (blockRows || [])
        .map((r) => ({
          address: normalizeAddressOut(r.address_out) || null,
          opa: r.opa_number ? String(r.opa_number).trim() : null,
          zip: r.zip_code ? String(r.zip_code).trim() : null,
        }))
        .filter((s) => s.address && s.opa) || [];

    return suggestions;
  } catch (e) {
    console.error("[OPA/SUGGEST] error:", e);
    return [];
  }
}

async function addressNotFound(res, rawAddress) {
  const suggestions = await fetchSuggestionsFromLookup(rawAddress, 10);
  return res.status(404).json({
    ok: false,
    code: "ADDRESS_NOT_FOUND",
    message: "Address Not Found",
    query: String(rawAddress),
    suggestions,
  });
}

// =========================
// ping
// =========================
router.get("/_ping", (req, res) =>
  res.json({
    ok: true,
    route: "/api/opa/_ping",
    buildStamp: BUILD_STAMP,
    nodeEnv: process.env.NODE_ENV || null,
    ts: Date.now(),
  })
);

// =========================
// DEBUG (TEMP) — show file + build
// GET /api/opa/__whoami
// =========================
router.get("/__whoami", (req, res) => {
  return res.json({
    ok: true,
    route: "/api/opa/__whoami",
    buildStamp: BUILD_STAMP,
    file: `file://${__filename}`,
    ts: Date.now(),
  });
});

// =========================
// DEBUG (TEMP) — show suggest output quickly
// GET /api/opa/__debug_suggest?query=526%20MARK
// =========================
router.get("/__debug_suggest", async (req, res) => {
  try {
    const query = String(req.query.query || "").trim();
    if (!query) return res.status(400).json({ ok: false, error: "Missing ?query" });

    const suggestions = await fetchSuggestionsFromLookup(query, 25);
    return res.json({
      ok: true,
      buildStamp: BUILD_STAMP,
      query,
      count: suggestions.length,
      suggestions: suggestions.slice(0, 25),
    });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e.message, buildStamp: BUILD_STAMP });
  }
});

// =========================
// SUGGEST (autocomplete)
// GET /api/opa/suggest?query=1539 S Lam&limit=10
// =========================
router.get("/suggest", async (req, res) => {
  try {
    const { query, limit = "10" } = req.query;

    if (!query || !said(query)) {
      return res.status(400).json({ ok: false, error: "Missing ?query" });
    }

    const lim = Math.min(Math.max(parseInt(limit, 10) || 10, 1), 25);
    const suggestions = await fetchSuggestionsFromLookup(query, lim);

    return res.json({
      ok: true,
      query: String(query),
      count: suggestions.length,
      suggestions,
    });
  } catch (e) {
    console.error("[OPA/SUGGEST] error:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// =========================
// SEARCH
// =========================
router.get("/search", async (req, res) => {
  try {
    const { opa, address, limit = "1" } = req.query;

    // ---- OPA mode
    if (opa && said(opa)) {
      if (!/^\d{6,12}$/.test(String(opa))) {
        return res.status(400).json({ ok: false, error: "Invalid OPA format" });
      }
      const result = await lookupByOpa(opa);
      if (!result) {
        return res.status(404).json({
          ok: false,
          code: "OPA_NOT_FOUND",
          message: "OPA not found",
          opa: String(opa),
        });
      }
      return res.json({ ok: true, mode: "opa", result });
    }

    // ---- Address mode
    if (!address || !said(address)) {
      return res.status(400).json({
        ok: false,
        error: "Missing ?address (or provide ?opa=#########)",
      });
    }

    const lim = Math.min(Math.max(parseInt(limit, 10) || 1, 1), 25);

    const addrNorm = normalizeAddressForMatch(address);
    const addrSansCity = stripCityStateZip(addrNorm);
    const zip = extractZip(addrSansCity);
    const addrCore = removeZip(addrSansCity, zip);

    const parsed = parseAddressParts(addrCore);
    const hasHouse = Number.isFinite(parsed.houseNumber) && parsed.houseNumber > 0;
    const hasStreet = said(parsed.streetName);

    // -------------------------
    // 1) STRICT exact match (LOOKUP)
    // ✅ FIX: house (incl ranges) + dir (if present) + street_name "starts with" parsed name.
    // -------------------------
    if (hasHouse && hasStreet) {
      const streetNameSql = sanitizeSql(parsed.streetName);
      const streetDirSql = parsed.streetDirection ? sanitizeSql(parsed.streetDirection) : null;

      const addrSql = buildAddrSql();

      const hnoStr = `TRIM(CAST(${COL_HNO} AS VARCHAR))`;

      const rangeEndFrom = (tokenExpr) => `
        CASE
          WHEN length(split_part(${tokenExpr}, '-', 2)) < length(split_part(${tokenExpr}, '-', 1))
            THEN
              (
                CAST(
                  floor(
                    TRY_CAST(split_part(${tokenExpr}, '-', 1) AS DOUBLE)
                    / pow(10, length(split_part(${tokenExpr}, '-', 2)))
                  ) AS INTEGER
                )
                * CAST(pow(10, length(split_part(${tokenExpr}, '-', 2))) AS INTEGER)
              )
              + TRY_CAST(split_part(${tokenExpr}, '-', 2) AS INTEGER)
          ELSE TRY_CAST(split_part(${tokenExpr}, '-', 2) AS INTEGER)
        END
      `;

      const houseMatchLookup = `
        (
          (regexp_like(${hnoStr}, '^\\d+$') AND TRY_CAST(${hnoStr} AS INTEGER) = ${parsed.houseNumber})
          OR (
            regexp_like(${hnoStr}, '^\\d+-\\d+$')
            AND ${parsed.houseNumber} BETWEEN
              TRY_CAST(split_part(${hnoStr}, '-', 1) AS INTEGER)
              AND ${rangeEndFrom(hnoStr)}
          )
        )
      `;

      // 🔥 this is the key: match street_name by anchored prefix, not equality
      const streetNameMatch = `
        regexp_like(
          UPPER(COALESCE(${COL_SNAME}, '')),
          '^${streetNameSql}(\\\\b|\\\\s)'
        )
      `;

      const exactLookupQ = `
        SELECT
          ${COL_OPA} AS opa_number,
          ${addrSql} AS address_out,
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
          ) AS sale_date
        FROM ${DATABASE}.${TABLE_LOOKUP}
        WHERE
          ${houseMatchLookup}
          AND ${streetNameMatch}
          ${streetDirSql ? `AND UPPER(COALESCE(${COL_SDIR}, '')) = UPPER('${streetDirSql}')` : ``}
          ${zip ? `AND ${COL_ZIP} = '${sanitizeSql(zip)}'` : ``}
        LIMIT 1
      `;

      const rows = await runAthena(exactLookupQ);
      if (!rows.length) return await addressNotFound(res, address);

      const r = rows[0];
      const ownerCombined = [r.owner_1, r.owner_2].filter(Boolean).join(" & ") || null;
      const zoning = await fetchZoningFromPublicByOpa(String(r.opa_number).trim());

      return res.json({
        ok: true,
        mode: "address",
        result: {
          opa: r.opa_number,
          address: normalizeAddressOut(r.address_out) || null,
          owner: ownerCombined,
          market_value: toNumber(r.market_value),
          sale_price: toNumber(r.sale_price),
          sale_date: r.sale_date || null,
          zoning: zoning || null,
          tax: { lookup_url: "https://tax-services.phila.gov/_/" },
        },
      });
    }

    // -------------------------
    // 2) Fuzzy match ONLY if no house number (PUBLIC)
    // -------------------------
    const addrNeedle = sanitizeSql(addrCore);

    const fuzzyQ = `
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
          NULLIF(TRIM(${COL_ZONING}), '') AS zoning
        FROM ${DATABASE}.${TABLE_PUBLIC}
      )
      SELECT
        opa_number, location, owner_1, owner_2, market_value, sale_price, sale_date, zoning
      FROM base
      WHERE
        strpos(UPPER(COALESCE(location, '')), UPPER('${addrNeedle}')) > 0
        ${zip ? `AND zip_code = '${sanitizeSql(zip)}'` : ``}
      ORDER BY location
      LIMIT ${lim}
    `;

    const rows = await runAthena(fuzzyQ);
    if (!rows.length) return await addressNotFound(res, address);

    if (lim === 1) {
      const r = rows[0];
      const ownerCombined = [r.owner_1, r.owner_2].filter(Boolean).join(" & ");
      return res.json({
        ok: true,
        mode: "address",
        result: {
          opa: r.opa_number,
          address: normalizeAddressOut(r.location) || null,
          owner: ownerCombined || null,
          market_value: toNumber(r.market_value),
          sale_price: toNumber(r.sale_price),
          sale_date: r.sale_date || null,
          zoning: r.zoning ? String(r.zoning).trim() : null,
          tax: { lookup_url: "https://tax-services.phila.gov/_/" },
        },
      });
    }

    const results = rows.map((r) => {
      const ownerCombined = [r.owner_1, r.owner_2].filter(Boolean).join(" & ");
      return {
        opa: r.opa_number,
        address: normalizeAddressOut(r.location) || null,
        owner: ownerCombined || null,
        market_value: toNumber(r.market_value),
        sale_price: toNumber(r.sale_price),
        sale_date: r.sale_date || null,
        zoning: r.zoning ? String(r.zoning).trim() : null,
        tax: { lookup_url: "https://tax-services.phila.gov/_/" },
      };
    });

    return res.json({ ok: true, mode: "address", query: address, count: results.length, results });
  } catch (e) {
    console.error("[OPA/SEARCH] error:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// =========================
// Detail by OPA (kept)
// GET /api/opa?opa=#########
// =========================
router.get("/", async (req, res) => {
  try {
    const { opa } = req.query;
    if (!opa) return res.status(400).json({ ok: false, error: "Missing ?opa=OPA_NUMBER" });
    if (!/^\d{6,12}$/.test(String(opa))) {
      return res.status(400).json({ ok: false, error: "Invalid OPA format" });
    }

    const result = await lookupByOpa(opa);
    if (!result) {
      return res.status(404).json({
        ok: false,
        code: "OPA_NOT_FOUND",
        message: "OPA not found",
        opa: String(opa),
      });
    }

    return res.json({ ok: true, mode: "opa", ...result });
  } catch (e) {
    console.error("[OPA/DETAIL] error:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

export default router;
