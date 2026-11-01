// src/index.js
import express from "express";
import { fileURLToPath } from "url";
import { dirname } from "path";

import opaRouter from "./routes/opa.js";
import violationsRouter from "./routes/violations.js";
import complaintsRouter from "./routes/complaints.js";
import recordsRouter from "./routes/records.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const app = express();

const PORT = process.env.PORT || 3000;
const HOST = process.env.HOST || "0.0.0.0";

console.log(`🚦 index.js loaded from: file://${__filename}`);

app.use(express.json());

/* =========================
   BASIC HEALTH / ROOT
   ========================= */

app.get("/healthz", (req, res) => {
  res.json({
    ok: true,
    ts: Date.now(),
    fingerprint: "CITY-CARTO-001",
    file: `file://${__filename}`,
  });
});
app.get("/__whoami", (req, res) => {
  res.json({
    ok: true,
    fingerprint: "CITY-CARTO-001",
    file: `file://${__filename}`,
  });
});

app.get("/_root", (req, res) => {
  res.json({
    msg: "root alive",
    ts: Date.now(),
    service: process.env.RENDER_SERVICE_NAME || null,
    commit: process.env.RENDER_GIT_COMMIT || null,
    branch: process.env.RENDER_GIT_BRANCH || null,
  });
});

/* =========================
   MAIN ROUTERS
   ========================= */

console.log("🔗 Mounting OPA router at /api/opa");
app.use("/api/opa", opaRouter);

console.log("🔗 Mounting Violations router at /api/violations");
app.use("/api/violations", violationsRouter);

console.log("🔗 Mounting Complaints router at /api/complaints");
app.use("/api/complaints", complaintsRouter);
app.use("/api/records", recordsRouter);

/* =========================
   JSON 404 (NO HTML)
   ========================= */

app.use((req, res) => {
  res.status(404).json({
    ok: false,
    error: "Not found",
    path: req.path,
  });
});

/* =========================
   START SERVER
   ========================= */

app.listen(PORT, HOST, () => {
  console.log(`🚀 Server running at http://${HOST}:${PORT}`);
});

