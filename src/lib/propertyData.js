// OPA public data uses PostgreSQL via the City's CARTO endpoint.
export const TABLE = "opa_properties_public";
const FIELDS = "parcel_number, location, owner_1, owner_2, market_value, sale_price, sale_date, zoning, zip_code";
export const literal = (value) => "'" + String(value).replace(/'/g, "''") + "'";
export const validOpa = (value) => /^\d{6,12}$/.test(value);
export const limitValue = (raw, fallback = 10) =>
  Math.min(Math.max(Number.parseInt(raw, 10) || fallback, 1), 25);

const DESIGNATIONS = {
  STREET: "ST", AVENUE: "AVE", AV: "AVE", ROAD: "RD", BOULEVARD: "BLVD",
  DRIVE: "DR", LANE: "LN", COURT: "CT", PLACE: "PL", PARKWAY: "PKWY",
  CIRCLE: "CIR", TERRACE: "TER",
};

export function normalizeAddress(raw) {
  let address = String(raw).toUpperCase().replace(/[,\t.]/g, " ").replace(/\s+/g, " ").trim();
  // Strip only a trailing postal code, never a five-digit house number.
  const zipMatch = address.match(/\s+(\d{5})(?:-\d{4})?$/);
  const zip = zipMatch?.[1] || null;
  if (zipMatch) address = address.slice(0, zipMatch.index).trim();
  address = address.replace(/\s+(?:USA|UNITED STATES)$/, "")
    .replace(/\s+(?:PA|PENNSYLVANIA)$/, "")
    .replace(/\s+PHILADELPHIA$/, "").trim();
  address = address.split(" ").map((word) => DESIGNATIONS[word] || word).join(" ");
  address = address.replace(/\b(?:APARTMENT|APT|SUITE|STE)\s+/g, "UNIT ")
    .replace(/\s+#\s*/g, " UNIT ");
  return { address, zip };
}

const normalizedLocation = "UPPER(REGEXP_REPLACE(TRIM(location), '\\s+', ' ', 'g'))";
const prefix = (column, value) => `strpos(UPPER(COALESCE(${column}, '')), ${literal(value)}) = 1`;
const numericHouse = "CASE WHEN house_number ~ '^[0-9]+$' THEN house_number::bigint ELSE NULL END";
const streetCombo = "CONCAT_WS(' ', NULLIF(street_name, ''), NULLIF(street_designation, ''), NULLIF(suffix, ''))";

function rangeContains(house) {
  // location retains ranges such as 524-28 even when house_number is only 524.
  const token = "split_part(location, ' ', 1)";
  const start = `split_part(${token}, '-', 1)`;
  const end = `split_part(${token}, '-', 2)`;
  return `CASE WHEN location ~ '^[0-9]+-[0-9]+ ' THEN
    ${house} BETWEEN ${start}::bigint AND
      CASE WHEN length(${end}) < length(${start})
        THEN (floor(${start}::numeric / power(10, length(${end}))) * power(10, length(${end})) + ${end}::numeric)
        ELSE ${end}::numeric END
    ELSE FALSE END`;
}

export function propertyByOpaSql(opa) {
  if (!validOpa(opa)) throw new Error("Invalid OPA");
  return `SELECT ${FIELDS} FROM ${TABLE} WHERE parcel_number = ${literal(opa)} LIMIT 1`;
}

export function exactAddressSql(raw) {
  const { address, zip } = normalizeAddress(raw);
  return `SELECT ${FIELDS} FROM ${TABLE}
    WHERE ${normalizedLocation} = ${literal(address)}
    ${zip ? `AND zip_code = ${literal(zip)}` : ""}
    ORDER BY parcel_number LIMIT 2`;
}

export function suggestionsSql(raw, limit = 10, { block = false } = {}) {
  const { address, zip } = normalizeAddress(raw);
  const match = address.match(/^(\d+)\s*(.*)$/);
  const conditions = [];
  let order = "location, parcel_number";
  if (zip) conditions.push(`zip_code = ${literal(zip)}`);
  if (match) {
    const digits = match[1];
    const number = Number(digits);
    if (!Number.isSafeInteger(number) || digits.length > 9) return null;
    let street = match[2];
    const dir = street.match(/^(N|S|E|W|NE|NW|SE|SW)\s+/);
    if (dir) {
      street = street.slice(dir[0].length);
      // Direction is constrained; never silently substitute north for south.
      conditions.push(`UPPER(COALESCE(street_direction, '')) = ${literal(dir[1])}`);
    }
    if (street) conditions.push(prefix(streetCombo, street));
    if (block) {
      if (digits.length < 3 || !street) return null;
      const start = Math.floor(number / 100) * 100;
      conditions.push(`(${numericHouse}) BETWEEN ${start} AND ${start + 99}`);
    } else if (digits.length >= 3) {
      conditions.push(`((${numericHouse}) = ${number} OR (${rangeContains(number)}))`);
    } else {
      conditions.push(prefix("house_number", digits));
    }
    order = `CASE WHEN (${numericHouse}) = ${number} THEN 0 ELSE 1 END, location, parcel_number`;
  } else {
    if (block || address.length < 3) return null;
    conditions.push(prefix("location", address));
  }
  return `SELECT parcel_number, location, zip_code FROM ${TABLE}
    WHERE ${conditions.join(" AND ")} ORDER BY ${order} LIMIT ${limitValue(limit)}`;
}

function numberOrNull(value) {
  if (value == null || String(value).trim() === "") return null;
  const n = Number(String(value).replace(/,/g, ""));
  return Number.isFinite(n) ? n : null;
}

export function toProperty(row) {
  return {
    opa: String(row.parcel_number ?? "").trim(),
    address: String(row.location ?? "").replace(/\s+/g, " ").trim(),
    owner: [row.owner_1, row.owner_2].filter((v) => v && String(v).trim())
      .map((v) => String(v).trim()).join(" & ") || null,
    market_value: numberOrNull(row.market_value),
    sale_price: numberOrNull(row.sale_price),
    sale_date: row.sale_date ? String(row.sale_date).slice(0, 10) : null,
    zoning: row.zoning ? String(row.zoning).trim() : null,
    tax: { lookup_url: "https://tax-services.phila.gov/_/" },
  };
}

export function toSuggestion(row) {
  return { address: String(row.location).trim(), opa: String(row.parcel_number).trim(), zip: row.zip_code || null };
}

