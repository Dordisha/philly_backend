import { AthenaService } from "./athenaService.js";

// Use the table from .env (fallback provided)
const TABLE = process.env.ATHENA_TABLE || "opa_properties_public";

/** Escape value for single-quoted SQL string */
function sqlStringLiteral(s) {
  return `'${String(s).replace(/'/g, "''")}'`;
}

/** Validate OPA numeric */
function assertNumericOPA(opa) {
  if (!/^\d+$/.test(String(opa))) throw new Error("Invalid OPA format");
}

/** Exact by OPA */
export async function getByOPA(opa) {
  assertNumericOPA(opa);

  const sql = `
    SELECT
      opa_account  AS opa,
      owner_1      AS owner,
      sale_price   AS salePrice,
      sale_date    AS saleDate,
      market_value AS marketValue,
      address_full AS address
    FROM ${TABLE}
    WHERE opa_account = ${sqlStringLiteral(opa)}
    LIMIT 1
  `;
  const { rows } = await AthenaService.query(sql);
  return rows[0] ?? null;
}

/** Fuzzy by address */
export async function searchByAddress(addressFragment, { limit = 5 } = {}) {
  const frag = String(addressFragment || "").trim();
  if (!frag) throw new Error("Address query required");

  const like = `%${frag}%`;
  const safeLimit = Math.max(1, Math.min(50, Number(limit) || 5));

  // Athena/Presto doesn't support "NULLS LAST"; emulate via CASE
  const sql = `
    SELECT
      opa_account  AS opa,
      owner_1      AS owner,
      sale_price   AS salePrice,
      sale_date    AS saleDate,
      market_value AS marketValue,
      address_full AS address
    FROM ${TABLE}
    WHERE LOWER(address_full) LIKE LOWER(${sqlStringLiteral(like)})
    ORDER BY
      CASE WHEN sale_date IS NULL THEN 1 ELSE 0 END,
      sale_date DESC
    LIMIT ${safeLimit}
  `;
  const { rows } = await AthenaService.query(sql);
  return rows;
}
