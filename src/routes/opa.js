import { Router } from "express";
import { queryCarto } from "../lib/carto.js";
import {
  validOpa, limitValue, normalizeAddress, propertyByOpaSql, exactAddressSql,
  suggestionsSql, toProperty, toSuggestion,
} from "../lib/propertyData.js";

export function createOpaRouter({ query = queryCarto } = {}) {
  const router = Router();
  const stamp = process.env.RENDER_GIT_COMMIT || process.env.GIT_COMMIT || "local";
  const ping = (req, res) => res.json({
    ok: true, route: req.baseUrl + req.path, buildStamp: stamp,
    nodeEnv: process.env.NODE_ENV || null, dataSource: "city-carto", ts: Date.now(),
  });
  router.get("/_ping", ping);
  router.get("/__whoami", ping);

  const input = (raw) => typeof raw === "string" ? raw.trim() : "";
  const validInput = (raw) => raw.length > 0 && raw.length <= 200 && !/[\x00-\x1f]/.test(raw);
  function failure(res, error) {
    console.error("[OPA/CARTO]", error.cause?.message || error.message);
    return res.status(error.status || 503).json({
      ok: false, code: "PUBLIC_DATA_UNAVAILABLE",
      message: "Property data is temporarily unavailable. Please try again.",
      error: "Property data is temporarily unavailable. Please try again.",
    });
  }
  async function suggest(raw, limit) {
    const sql = suggestionsSql(raw, limit);
    if (!sql) return [];
    let rows = await query(sql);
    if (!rows.length) {
      const fallback = suggestionsSql(raw, limit, { block: true });
      if (fallback) rows = await query(fallback);
    }
    return rows.filter((r) => r.parcel_number && r.location).map(toSuggestion);
  }

  router.get(["/suggest", "/__debug_suggest"], async (req, res) => {
    const raw = input(req.query.query ?? req.query.q);
    if (!validInput(raw)) return res.status(400).json({ ok: false, error: "Provide a query of 1–200 characters." });
    try {
      const suggestions = await suggest(raw, limitValue(req.query.limit));
      return res.json({ ok: true, query: raw, count: suggestions.length, suggestions });
    } catch (error) {
      return failure(res, error);
    }
  });

  async function search(req, res, flattened = false) {
    const opa = input(req.query.opa);
    const address = input(req.query.address);
    try {
      let rows;
      let mode;
      if (opa) {
        if (!validOpa(opa)) return res.status(400).json({ ok: false, error: "Invalid OPA format" });
        rows = await query(propertyByOpaSql(opa));
        mode = "opa";
      } else {
        if (flattened || !validInput(address) || !normalizeAddress(address).address) {
          return res.status(400).json({ ok: false, error: "Provide ?address or ?opa." });
        }
        rows = await query(exactAddressSql(address));
        mode = "address";
      }
      if (!rows.length) {
        // Legacy clients auto-select the first 404 suggestion. Keep this empty so
        // a nonexistent address cannot silently become a different property.
        return res.status(404).json({
          ok: false, code: opa ? "OPA_NOT_FOUND" : "ADDRESS_NOT_FOUND",
          message: opa ? "OPA not found" : "Address not found. Please select an address from suggestions.",
          ...(opa ? { opa } : { query: address, suggestions: [] }),
        });
      }
      if (rows.length > 1) {
        return res.status(409).json({
          ok: false, code: "ADDRESS_AMBIGUOUS",
          message: "Multiple parcels match. Please select a property from suggestions.",
          error: "Multiple parcels match. Please select a property from suggestions.",
          suggestions: rows.map(toSuggestion),
        });
      }
      const result = toProperty(rows[0]);
      return res.json(flattened ? { ok: true, mode, ...result } : { ok: true, mode, result });
    } catch (error) {
      return failure(res, error);
    }
  }
  router.get("/search", (req, res) => search(req, res));
  router.get("/", (req, res) => search(req, res, true));
  return router;
}

export default createOpaRouter();

