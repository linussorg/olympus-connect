import express from "express";
import multer from "multer";
import cors from "cors";
import crypto from "crypto";
import nodemailer from "nodemailer";
import fs from "fs";
import path from "path";
import { analyzeBuffer } from "./mapping/engine.js";
import { importToDb, generateSqlDump } from "./db/loader.js";
import { connect, execSql, isDbAvailable, close } from "./db/connection.js";
import { ensureJobsTable, createJob, updateJob, getJob, listJobs, deleteJob } from "./db/jobs.js";
import { TARGET_SCHEMAS, getSchemaPromptForLLM } from "./schema.js";
import * as llm from "./llm.js";
import type { AnalysisResult } from "./types.js";
import { validateCrossTable, buildAnomalyReport } from "./validators/index.js";
import { getOverrides, getOverridesForTable, setOverride, deleteOverride, updateOverride } from "./overrides.js";
import { generateAthenaInsights, clearInsightsCache } from "./athena-insights.js";
import type { MappingSpec } from "./types.js";

const app = express();
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 100 * 1024 * 1024 } });
const PORT = parseInt(process.env.ENGINE_PORT || "3001", 10);

app.use(cors());
app.use(express.json());

// Disk-backed analysis store: fileId → AnalysisResult
const ANALYSIS_DIR = path.resolve("data", "analyses");
fs.mkdirSync(ANALYSIS_DIR, { recursive: true });

const analysisCache = new Map<string, AnalysisResult>();

const analysisStore = {
  get(id: string): AnalysisResult | undefined {
    if (analysisCache.has(id)) return analysisCache.get(id);
    const file = path.join(ANALYSIS_DIR, `${id}.json`);
    if (!fs.existsSync(file)) return undefined;
    try {
      const data = JSON.parse(fs.readFileSync(file, "utf-8")) as AnalysisResult;
      analysisCache.set(id, data);
      return data;
    } catch { return undefined; }
  },
  set(id: string, value: AnalysisResult) {
    analysisCache.set(id, value);
    fs.writeFileSync(path.join(ANALYSIS_DIR, `${id}.json`), JSON.stringify(value));
  },
  delete(id: string) {
    analysisCache.delete(id);
    const file = path.join(ANALYSIS_DIR, `${id}.json`);
    if (fs.existsSync(file)) fs.unlinkSync(file);
  },
  clear() {
    analysisCache.clear();
    for (const f of fs.readdirSync(ANALYSIS_DIR)) {
      if (f.endsWith(".json")) fs.unlinkSync(path.join(ANALYSIS_DIR, f));
    }
  },
  values(): AnalysisResult[] {
    const ids = new Set<string>();
    for (const f of fs.readdirSync(ANALYSIS_DIR)) {
      if (f.endsWith(".json")) ids.add(f.replace(/\.json$/, ""));
    }
    return Array.from(ids).map((id) => analysisStore.get(id)!).filter(Boolean);
  },
  get size(): number {
    return fs.readdirSync(ANALYSIS_DIR).filter((f) => f.endsWith(".json")).length;
  },
};

// Strip _rawHeaders/_rawRows from API responses (large, internal-only)
function toPublicResult(r: AnalysisResult) {
  const { _rawHeaders, _rawRows, ...rest } = r;
  return rest;
}

// ─── POST /api/upload ───
// Accepts multipart file upload, returns AnalysisResult
app.post("/api/upload", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) {
      res.status(400).json({ error: "No file uploaded" });
      return;
    }

    const fileName = req.file.originalname;
    const buffer = req.file.buffer;

    console.log(`[upload] Analyzing: ${fileName} (${(buffer.length / 1024).toFixed(1)} KB)`);
    const result = await analyzeBuffer(buffer, fileName);

    // Store for later import/dump
    analysisStore.set(result.fileId, result);

    console.log(`[upload] Done: ${result.mappings.length} mappings, ${result.issues.length} issues, confidence ${(result.confidence * 100).toFixed(0)}%`);
    res.json(toPublicResult(result));
  } catch (err: any) {
    console.error("[upload] Error:", err);
    res.status(500).json({ error: err.message || "Analysis failed" });
  }
});

// ─── POST /api/upload/stream ───
// Same as /api/upload but streams progress events via SSE
app.post("/api/upload/stream", upload.single("file"), async (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();

  const send = (data: any) => {
    res.write(`data: ${JSON.stringify(data)}\n\n`);
    if (typeof (res as any).flush === "function") (res as any).flush();
  };

  try {
    if (!req.file) {
      send({ step: "error", message: "No file uploaded" });
      res.end();
      return;
    }

    const fileName = req.file.originalname;
    const buffer = req.file.buffer;

    console.log(`[upload/stream] Analyzing: ${fileName} (${(buffer.length / 1024).toFixed(1)} KB)`);

    const result = await analyzeBuffer(buffer, fileName, (event) => {
      send(event);
    });

    analysisStore.set(result.fileId, result);

    console.log(`[upload/stream] Done: ${result.mappings.length} mappings, ${result.issues.length} issues, confidence ${(result.confidence * 100).toFixed(0)}%`);
    send({ step: "done", result: toPublicResult(result) });
  } catch (err: any) {
    console.error("[upload/stream] Error:", err);
    send({ step: "error", message: err.message || "Analysis failed" });
  }

  res.end();
});

// ─── POST /api/import ───
// Imports a previously analyzed file into SQL Server
app.post("/api/import", async (req, res) => {
  try {
    const { fileId } = req.body;
    if (!fileId) {
      res.status(400).json({ error: "Missing fileId" });
      return;
    }

    const analysis = analysisStore.get(fileId);
    if (!analysis) {
      res.status(404).json({ error: "Analysis not found. Upload the file first." });
      return;
    }

    console.log(`[import] Importing ${analysis.fileName} → ${analysis.targetTable}`);
    const result = await importToDb(analysis);
    console.log(`[import] Done: ${result.inserted} inserted, ${result.skipped} skipped`);
    res.json(result);
  } catch (err: any) {
    console.error("[import] Error:", err);
    res.status(500).json({ error: err.message || "Import failed" });
  }
});

// ─── POST /api/dump ───
// Returns .sql dump for a previously analyzed file
app.post("/api/dump", async (req, res) => {
  try {
    const { fileId } = req.body;
    if (!fileId) {
      res.status(400).json({ error: "Missing fileId" });
      return;
    }

    const analysis = analysisStore.get(fileId);
    if (!analysis) {
      res.status(404).json({ error: "Analysis not found. Upload the file first." });
      return;
    }

    const sql = generateSqlDump(analysis);
    res.setHeader("Content-Type", "application/sql");
    res.setHeader("Content-Disposition", `attachment; filename="${analysis.fileName.replace(/\.[^.]+$/, "")}_import.sql"`);
    res.send(sql);
  } catch (err: any) {
    console.error("[dump] Error:", err);
    res.status(500).json({ error: err.message || "Dump failed" });
  }
});

// ─── GET /api/schema ───
// Returns all target table schemas
app.get("/api/schema", (_req, res) => {
  res.json(TARGET_SCHEMAS);
});

// ─── POST /api/test-connection ───
// Tests DB connectivity
app.post("/api/test-connection", async (_req, res) => {
  try {
    const available = await isDbAvailable();
    res.json({ ok: available });
  } catch (err: any) {
    res.json({ ok: false, error: err.message });
  }
});

// ─── GET /api/tables ───
// List all tables with row counts
app.get("/api/tables", async (_req, res) => {
  try {
    const conn = await connect();
    const rawRows = await execSql(conn, `
      SELECT t.name, SUM(p.rows) as row_count
      FROM sys.tables t
      JOIN sys.partitions p ON t.object_id = p.object_id
      WHERE p.index_id IN (0, 1)
      GROUP BY t.name
      ORDER BY t.name
    `);
    const dbTables = new Map<string, number>();
    for (const row of rawRows) {
      const name = row[0]?.value as string;
      const count = Number(row[1]?.value) || 0;
      if (name) dbTables.set(name, count);
    }
    res.json(TARGET_SCHEMAS.map((s) => ({
      name: s.name,
      description: s.description,
      rowCount: dbTables.get(s.name) || 0,
      columnCount: s.columns.length,
    })));
  } catch (err: any) {
    console.error("[tables] Error:", err);
    res.status(503).json({ error: "Datenbank nicht erreichbar" });
  }
});

// ─── GET /api/tables/:name/preview ───
// Preview first 100 rows of a known table
app.get("/api/tables/:name/preview", async (req, res) => {
  try {
    const tableName = req.params.name;
    const schema = TARGET_SCHEMAS.find((s) => s.name === tableName);
    if (!schema) {
      res.status(404).json({ error: `Unknown table: ${tableName}` });
      return;
    }
    const conn = await connect();
    const rawRows = await execSql(conn, `SELECT TOP 100 * FROM [${tableName}]`);
    const table = tediousResultToTable(rawRows);
    res.json({
      table,
      columns: schema.columns.map((c) => ({ name: c.name, type: c.type, description: c.description || null })),
    });
  } catch (err: any) {
    console.error("[tables/preview] Error:", err);
    res.status(500).json({ error: err.message });
  }
});

// ─── POST /api/sql ───
// Execute a read-only SQL query
app.post("/api/sql", async (req, res) => {
  try {
    const { query } = req.body;
    if (!query || typeof query !== "string") {
      res.status(400).json({ error: "Missing query" });
      return;
    }
    if (!isSafeSelect(query)) {
      res.status(403).json({ error: "Nur SELECT-Abfragen erlaubt" });
      return;
    }
    const safeSql = injectTopLimit(query, 500);
    const conn = await connect();
    const rawRows = await execSql(conn, safeSql);
    const table = tediousResultToTable(rawRows);
    res.json({ sql: safeSql, ...table });
  } catch (err: any) {
    console.error("[sql] Error:", err);
    res.status(500).json({ error: err.message });
  }
});

// ─── POST /api/truncate-all ───
// Truncate all import tables (danger zone)
app.post("/api/truncate-all", async (_req, res) => {
  try {
    const conn = await connect();
    const tableNames = TARGET_SCHEMAS.map((s) => s.name);
    for (const name of tableNames) {
      await execSql(conn, `TRUNCATE TABLE [${name}]`).catch(() => {
        // Table might not exist yet — try DELETE as fallback
        return execSql(conn, `DELETE FROM [${name}]`).catch(() => {});
      });
    }
    // Clear jobs table
    await execSql(conn, `DELETE FROM tbImportJobs`).catch(() => {});
    // Clear in-memory analysis store
    analysisStore.clear();
    console.log(`[truncate-all] Truncated ${tableNames.length} tables, cleared jobs and analysis store`);
    res.json({ ok: true, tables: tableNames.length });
  } catch (err: any) {
    console.error("[truncate-all] Error:", err);
    res.status(500).json({ error: err.message });
  }
});

// ─── Chat helpers ───

interface ChatMessage { role: "user" | "assistant"; content: string; }

function tediousResultToTable(rawRows: any[][]): { headers: string[]; rows: string[][] } {
  if (!rawRows || rawRows.length === 0) return { headers: [], rows: [] };
  const headers = rawRows[0].map((col: any) => col.metadata.colName as string);
  const rows = rawRows.map((r: any[]) =>
    r.map((col: any) => (col.value === null || col.value === undefined ? "" : String(col.value)))
  );
  return { headers, rows };
}

function isSafeSelect(sql: string): boolean {
  const normalized = sql.trim().toUpperCase();
  if (!/^(SELECT|WITH)\s/.test(normalized)) return false;
  return !/\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|EXEC|EXECUTE|XP_|SP_)\b/.test(normalized);
}

function injectTopLimit(sql: string, limit = 100): string {
  return sql.replace(/^(\s*SELECT\s+)(?!TOP\s)/i, `$1TOP ${limit} `);
}

// ─── POST /api/chat ───
// LLM chat for natural language data queries — generates SQL, executes it, returns results
app.post("/api/chat", async (req, res) => {
  try {
    const { message, history } = req.body as { message: string; history?: ChatMessage[] };
    if (!message) {
      res.status(400).json({ error: "Missing message" });
      return;
    }

    const available = await llm.isAvailable();
    if (!available) {
      res.json({ text: "LLM ist derzeit nicht verfügbar. Bitte prüfen Sie die Konfiguration (Ollama oder OpenRouter)." });
      return;
    }

    const schemaInfo = getSchemaPromptForLLM();

    const historyContext = history && history.length > 0
      ? "\n\nBisheriger Gesprächsverlauf:\n" +
        history.slice(-6).map((m) => `${m.role === "user" ? "Nutzer" : "Assistent"}: ${m.content}`).join("\n")
      : "";

    const systemPrompt = `Du bist ein Healthcare-Datenanalyst. Der Nutzer hat eine SQL Server Datenbank (Hack2026):

${schemaInfo}

REGELN:
- Schreibe T-SQL für MS SQL Server
- Erkläre die Ergebnisse immer auf Deutsch
- Antworte NUR mit JSON: {"text":"<Erklärung auf Deutsch>","sql":"<SELECT-Abfrage>","chart":<optional>}
- Falls keine SQL-Abfrage nötig ist, lasse "sql" weg
- Nutze immer TOP um Ergebnisse zu begrenzen (max. 100 Zeilen)
- Nur SELECT-Abfragen — niemals INSERT, UPDATE, DELETE oder DROP

CHART-REGELN:
- Wenn die Ergebnisse sich gut als Diagramm darstellen lassen (Aggregationen, Vergleiche, Zeitreihen), füge ein "chart"-Feld hinzu
- Format: {"type":"bar"|"line"|"pie","labelKey":"<Spaltenname für X-Achse/Labels>","valueKeys":["<Spaltenname(n) für Werte>"]}
- "labelKey" muss ein Spaltenname aus dem SELECT sein (die Kategorie/X-Achse)
- "valueKeys" muss ein Array von Spaltennamen aus dem SELECT sein (die numerischen Werte)
- Verwende "bar" für Vergleiche, "line" für Zeitreihen, "pie" für Anteile
- Lasse "chart" weg wenn eine Tabelle sinnvoller ist (z.B. bei Detailabfragen mit vielen Spalten)`;

    const response = await llm.generate(`${historyContext}\n\nNutzer: ${message}`, {
      system: systemPrompt,
      format: "json",
      temperature: 0.3,
    });

    let parsed: { text: string; sql?: string; chart?: { type: string; labelKey: string; valueKeys: string[] } };
    try {
      parsed = JSON.parse(response);
    } catch {
      res.json({ text: response });
      return;
    }

    if (!parsed.sql) {
      res.json({ text: parsed.text });
      return;
    }

    if (!isSafeSelect(parsed.sql)) {
      console.warn("[chat] Blocked non-SELECT SQL:", parsed.sql);
      res.json({ text: parsed.text, sql: parsed.sql });
      return;
    }

    const safeSql = injectTopLimit(parsed.sql, 100);

    const dbAvailable = await isDbAvailable();
    if (!dbAvailable) {
      res.json({
        text: parsed.text + "\n\n(Hinweis: Die Datenbank ist nicht erreichbar — SQL wird angezeigt, aber nicht ausgeführt.)",
        sql: safeSql,
      });
      return;
    }

    try {
      const conn = await connect();
      const rawRows = await execSql(conn, safeSql);
      const table = tediousResultToTable(rawRows);
      const rowInfo = table.rows.length === 100
        ? " (auf 100 Zeilen begrenzt)"
        : ` (${table.rows.length} Zeile${table.rows.length !== 1 ? "n" : ""})`;
      // Validate chart config against actual result headers
      let chart = parsed.chart;
      if (chart && table.headers.length > 0) {
        const hdrs = new Set(table.headers);
        if (!hdrs.has(chart.labelKey) || !chart.valueKeys?.every((k: string) => hdrs.has(k))) {
          chart = undefined; // LLM hallucinated column names, skip chart
        }
      }

      res.json({
        text: parsed.text + rowInfo,
        sql: safeSql,
        table: table.rows.length > 0 ? table : undefined,
        chart: table.rows.length > 0 ? chart : undefined,
      });
    } catch (sqlErr: any) {
      const errMsg = sqlErr.message || String(sqlErr) || "Unbekannter Datenbankfehler";
      console.error("[chat] SQL execution error:", errMsg);
      res.json({ text: `${parsed.text}\n\nFehler bei der Ausführung: ${errMsg}`, sql: safeSql });
    }
  } catch (err: any) {
    console.error("[chat] Error:", err);
    res.status(500).json({ error: err.message || "Chat failed" });
  }
});

// ─── GET /api/overrides ───
app.get("/api/overrides", (_req, res) => {
  res.json(getOverrides());
});

// ─── POST /api/overrides ───
app.post("/api/overrides", (req, res) => {
  const { sourceColumn, targetColumn, targetTable } = req.body;
  if (!sourceColumn || !targetColumn || !targetTable) {
    res.status(400).json({ error: "Missing sourceColumn, targetColumn, or targetTable" });
    return;
  }
  const entry = setOverride(sourceColumn, targetColumn, targetTable);
  console.log(`[overrides] Saved: ${sourceColumn} → ${targetColumn} (${targetTable})`);
  res.json(entry);
});

// ─── PUT /api/overrides/:id ───
app.put("/api/overrides/:id", (req, res) => {
  const { targetColumn } = req.body;
  if (!targetColumn) {
    res.status(400).json({ error: "Missing targetColumn" });
    return;
  }
  const entry = updateOverride(req.params.id, targetColumn);
  if (!entry) {
    res.status(404).json({ error: "Override not found" });
    return;
  }
  console.log(`[overrides] Updated ${req.params.id} → ${targetColumn}`);
  res.json(entry);
});

// ─── DELETE /api/overrides/:id ───
app.delete("/api/overrides/:id", (req, res) => {
  const ok = deleteOverride(req.params.id);
  if (!ok) {
    res.status(404).json({ error: "Override not found" });
    return;
  }
  console.log(`[overrides] Deleted ${req.params.id}`);
  res.json({ ok: true });
});

// ─── POST /api/validate-db ───
// Run cross-table validation on existing DB data
app.post("/api/validate-db", async (_req, res) => {
  try {
    const available = await isDbAvailable();
    if (!available) {
      res.status(503).json({ error: "Database not available" });
      return;
    }
    const issues = await validateCrossTable();
    const report = buildAnomalyReport(issues);
    res.json(report);
  } catch (err: any) {
    console.error("[validate-db] Error:", err);
    res.status(500).json({ error: err.message || "Validation failed" });
  }
});

// ─── GET /api/llm-config ───
app.get("/api/llm-config", (_req, res) => {
  const info = llm.getProviderInfo();
  res.json(info);
});

// ─── POST /api/llm-config ───
// Update LLM provider / API key / model at runtime
app.post("/api/llm-config", async (req, res) => {
  const { provider, apiKey, model } = req.body;
  llm.setConfig({ provider, apiKey, model });
  const info = llm.getProviderInfo();
  console.log(`[llm-config] Updated: ${info.provider} (${info.model})`);
  res.json(info);
});

// ─── POST /api/test-llm ───
// Test LLM connectivity with current config
app.post("/api/test-llm", async (_req, res) => {
  try {
    const info = llm.getProviderInfo();
    console.log(`[test-llm] Testing ${info.provider} (${info.model})...`);
    const available = await llm.isAvailable();
    console.log(`[test-llm] Result: ${available ? "OK" : "FAILED"}`);
    res.json({ ok: available, ...info, error: available ? undefined : `${info.provider} nicht erreichbar — prüfen Sie API-Key und Modell` });
  } catch (err: any) {
    console.error("[test-llm] Error:", err);
    res.json({ ok: false, error: err.message });
  }
});

// ─── GET /api/quality ───
// Aggregated quality stats across all analyzed files (in-memory + jobs)
app.get("/api/quality", async (_req, res) => {
  const analyses = analysisStore.values();

  let totalRows = 0;
  let totalAnomalies = 0;
  let confidenceSum = 0;
  let entryCount = 0;
  const sourceTypes = new Set<string>();
  const categoryCounts: Record<string, number> = {};
  const recentUploads: { file: string; detectedType: string; rows: number; confidence: number; issues: number; status: string }[] = [];
  const seenFiles = new Set<string>();

  for (const a of analyses) {
    totalRows += Number(a.rowCount) || 0;
    confidenceSum += Number(a.confidence) || 0;
    entryCount++;
    sourceTypes.add(a.detectedType);
    seenFiles.add(a.fileId);

    const issueCount = a.issues.filter((i: any) => i.severity !== "info").length;
    totalAnomalies += issueCount;

    for (const issue of a.issues) {
      const cat = issue.category || "other";
      categoryCounts[cat] = (categoryCounts[cat] || 0) + 1;
    }

    const status = a.confidence >= 0.7 ? "ok" : a.confidence >= 0.4 ? "warning" : "error";
    recentUploads.push({
      file: a.fileName,
      detectedType: a.detectedType,
      rows: a.rowCount,
      confidence: a.confidence,
      issues: issueCount,
      status,
    });
  }

  // Also include completed jobs that aren't already in analysisStore
  try {
    const jobs = await listJobs();
    for (const j of jobs) {
      if (seenFiles.has(j.jobId)) continue;
      if (j.status !== "done" && j.status !== "awaiting_review") continue;
      if (!j.detectedType || !j.fileName) continue;

      const rows = Number(j.rowCount) || 0;
      const confidence = Number(j.confidence) || 0;
      const issues = Number(j.issueCount) || 0;

      totalRows += rows;
      confidenceSum += confidence;
      entryCount++;
      totalAnomalies += issues;
      sourceTypes.add(j.detectedType);

      const status = confidence >= 0.7 ? "ok" : confidence >= 0.4 ? "warning" : "error";
      recentUploads.push({
        file: j.fileName,
        detectedType: j.detectedType,
        rows,
        confidence,
        issues,
        status,
      });
    }
  } catch {
    // Jobs table may not exist
  }

  const errorsByCategory = Object.entries(categoryCounts)
    .map(([category, count]) => ({ category, count }))
    .sort((a, b) => b.count - a.count);

  res.json({
    totalRows,
    totalSources: sourceTypes.size,
    averageConfidence: entryCount > 0 ? confidenceSum / entryCount : 0,
    totalAnomalies,
    errorsByCategory,
    recentUploads: recentUploads.reverse(),
    fileCount: entryCount,
  });
});

// ─── GET /api/athena ───
// Aggregated data for Athena dashboard (anomalies, job history, overrides, tables)
app.get("/api/athena", async (_req, res) => {
  try {
    // 1. Try cross-table anomaly report from DB
    let anomalyReport: ReturnType<typeof buildAnomalyReport> | null = null;
    try {
      const available = await isDbAvailable();
      if (available) {
        const issues = await validateCrossTable();
        anomalyReport = buildAnomalyReport(issues);
      }
    } catch (err) {
      console.warn("[athena] DB validation unavailable:", (err as Error).message);
    }

    // Fallback: build report from in-memory analyses if no DB report
    if (!anomalyReport) {
      const allIssues = analysisStore.values().flatMap((a) => a.issues);
      if (allIssues.length > 0) {
        anomalyReport = buildAnomalyReport(allIssues);
      }
    }

    // 2. Job history
    let jobHistory: { fileName: string; createdAt: string; errorCount: number; issueCount: number; confidence: number; detectedType: string; autoFixRate: number }[] = [];
    try {
      const jobs = await listJobs();
      jobHistory = jobs
        .filter((j) => j.status === "done" || j.status === "awaiting_review")
        .map((j) => ({
          fileName: j.fileName || "unknown",
          createdAt: j.createdAt,
          errorCount: j.errorCount ?? 0,
          issueCount: j.issueCount ?? 0,
          confidence: j.confidence ?? 0,
          detectedType: j.detectedType || "unknown",
          autoFixRate: j.autoFixRate ?? 0,
        }));
    } catch {
      // Jobs table may not exist
    }

    // 3. Override count
    const overrideCount = getOverrides().length;

    // 4. Table row counts
    let tables: { name: string; rowCount: number }[] = [];
    try {
      const available = await isDbAvailable();
      if (available) {
        const conn = await connect();
        const rawRows = await execSql(conn, `
          SELECT t.name, SUM(p.rows) as row_count
          FROM sys.tables t
          JOIN sys.partitions p ON t.object_id = p.object_id
          WHERE p.index_id IN (0, 1)
          GROUP BY t.name
          ORDER BY t.name
        `);
        const dbTables = new Map<string, number>();
        for (const row of rawRows) {
          const name = row[0]?.value as string;
          const count = Number(row[1]?.value) || 0;
          if (name) dbTables.set(name, count);
        }
        tables = TARGET_SCHEMAS.map((s) => ({
          name: s.name,
          rowCount: dbTables.get(s.name) || 0,
        }));
      }
    } catch {
      // DB unavailable
    }

    res.json({ anomalyReport, jobHistory, overrideCount, tables });
  } catch (err: any) {
    console.error("[athena] Error:", err);
    res.status(500).json({ error: err.message || "Athena data fetch failed" });
  }
});

// ─── GET /api/athena/insights ───
// LLM-powered insights (separate so it doesn't block the dashboard)
// ?regenerate=true to force re-generation
app.get("/api/athena/insights", async (req, res) => {
  try {
    const regenerate = req.query.regenerate === "true";

    // Build anomaly report (same logic as /api/athena)
    let anomalyReport: ReturnType<typeof buildAnomalyReport> | null = null;
    try {
      const available = await isDbAvailable();
      if (available) {
        const issues = await validateCrossTable();
        anomalyReport = buildAnomalyReport(issues);
      }
    } catch {}

    if (!anomalyReport) {
      const allIssues = analysisStore.values().flatMap((a) => a.issues);
      if (allIssues.length > 0) {
        anomalyReport = buildAnomalyReport(allIssues);
      }
    }

    if (!anomalyReport || anomalyReport.totalIssues === 0) {
      res.json(null);
      return;
    }

    if (regenerate) {
      clearInsightsCache();
    }

    const insights = await generateAthenaInsights(anomalyReport);
    res.json(insights);
  } catch (err: any) {
    console.error("[athena/insights] Error:", err);
    res.status(500).json({ error: err.message || "Insights generation failed" });
  }
});

// ─── GET /api/status ───
// Server health and store stats
app.get("/api/status", (_req, res) => {
  res.json({
    ok: true,
    analyses: analysisStore.size,
    llm: llm.getProviderInfo(),
  });
});

// ══════════════════════════════════════════════════
// ─── Job-based endpoints (persistent, async) ─────
// ══════════════════════════════════════════════════

// ─── POST /api/jobs ───
// Upload file → create job → return jobId immediately, analyze in background
app.post("/api/jobs", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) {
      res.status(400).json({ error: "No file uploaded" });
      return;
    }

    const fileName = req.file.originalname;
    const buffer = req.file.buffer;
    const jobId = `job-${crypto.randomUUID()}`;

    await createJob(jobId, fileName, buffer.length);
    console.log(`[jobs] Created job ${jobId} for ${fileName} (${(buffer.length / 1024).toFixed(1)} KB)`);

    // Return immediately
    res.status(202).json({ jobId });

    // Fire-and-forget async analysis
    (async () => {
      try {
        await updateJob(jobId, { status: "analyzing", progressMsg: "Analyse gestartet..." });

        // Accumulate partial results for progressive display
        let partialIssues: any[] = [];
        let partialAnomalyReport: any = null;
        const partialMappings: any[] = [];
        let lastMappingFlush = 0;
        const pendingUpdates: Promise<void>[] = [];

        const flushPartial = async (msg: string) => {
          const partial: any = {};
          if (partialIssues.length > 0) partial.issues = partialIssues;
          if (partialAnomalyReport) partial.anomalyReport = partialAnomalyReport;
          if (partialMappings.length > 0) partial.mappings = partialMappings;
          await updateJob(jobId, { progressMsg: msg, analysisJson: JSON.stringify(partial) }).catch(() => {});
        };

        const result = await analyzeBuffer(buffer, fileName, (event) => {
          const p = (async () => {
            const update: Parameters<typeof updateJob>[1] = { progressMsg: event.message };

            if (event.step === "detected") {
              update.detectedType = event.detectedType;
              update.targetTable = event.targetTable;
              update.rowCount = event.rowCount;
              await updateJob(jobId, update).catch(() => {});
              return;
            }

            if (event.step === "validated" && event.issues && event.anomalyReport) {
              partialIssues = event.issues;
              partialAnomalyReport = event.anomalyReport;
              update.issueCount = event.issueCount;
              update.errorCount = event.errorCount;
              update.autoFixRate = event.autoFixRate;
              update.analysisJson = JSON.stringify({
                issues: event.issues,
                anomalyReport: event.anomalyReport,
              });
              await updateJob(jobId, update).catch(() => {});
              return;
            }

            if (event.step === "heuristic" && event.mapped > 0) {
              // Heuristic mappings aren't individually streamed, just report count
              await updateJob(jobId, update).catch(() => {});
              return;
            }

            if (event.step === "llm-mapping" && event.mapping) {
              partialMappings.push(event.mapping);
              // Batch flush every 5 mappings to avoid excessive DB writes
              if (partialMappings.length - lastMappingFlush >= 5) {
                lastMappingFlush = partialMappings.length;
                await flushPartial(event.message);
                return;
              }
            }

            await updateJob(jobId, update).catch(() => {});
          })();
          pendingUpdates.push(p);
        });

        // Wait for any in-flight progress DB writes before writing final result
        await Promise.all(pendingUpdates);

        // Overwrite fileId with jobId so import/dump can find it
        result.fileId = jobId;
        analysisStore.set(jobId, result);

        const report = result.anomalyReport;
        await updateJob(jobId, {
          status: "awaiting_review",
          detectedType: result.detectedType,
          targetTable: result.targetTable,
          mappingType: result.mappingType,
          rowCount: result.rowCount,
          confidence: result.confidence,
          issueCount: result.issues.length,
          errorCount: report?.bySeverity.error ?? 0,
          autoFixRate: report?.autoFixRate ?? 0,
          analysisJson: JSON.stringify(toPublicResult(result)),
          progressMsg: "Analyse abgeschlossen",
        });

        console.log(`[jobs] ${jobId} analysis complete: ${result.mappings.length} mappings, confidence ${(result.confidence * 100).toFixed(0)}%`);
      } catch (err: any) {
        console.error(`[jobs] ${jobId} analysis failed:`, err);
        await updateJob(jobId, { status: "failed", error: err.message || "Analysis failed" }).catch(() => {});
      }
    })();
  } catch (err: any) {
    console.error("[jobs] Error creating job:", err);
    res.status(500).json({ error: err.message || "Failed to create job" });
  }
});

// ─── GET /api/jobs ───
// List all jobs (summary, no analysisJson)
app.get("/api/jobs", async (_req, res) => {
  try {
    const jobs = await listJobs();
    res.json(jobs.map(({ analysisJson, resolutions, ...rest }) => rest));
  } catch (err: any) {
    console.error("[jobs] List error:", err);
    res.status(500).json({ error: err.message || "Failed to list jobs" });
  }
});

// ─── GET /api/jobs/:jobId ───
// Get single job with full detail
app.get("/api/jobs/:jobId", async (req, res) => {
  try {
    const job = await getJob(req.params.jobId);
    if (!job) {
      res.status(404).json({ error: "Job not found" });
      return;
    }

    // Parse analysisJson — available during analyzing (partial: issues only) and after completion (full)
    let analysis: any = null;
    if (job.analysisJson) {
      try { analysis = JSON.parse(job.analysisJson); } catch {}
    }

    // Parse resolutions
    let resolutions: any = null;
    if (job.resolutions) {
      try { resolutions = JSON.parse(job.resolutions); } catch {}
    }

    const { analysisJson, resolutions: _r, ...summary } = job;
    res.json({ ...summary, analysis, resolutions });
  } catch (err: any) {
    console.error("[jobs] Get error:", err);
    res.status(500).json({ error: err.message || "Failed to get job" });
  }
});

// ─── PATCH /api/jobs/:jobId/resolutions ───
// Save anomaly resolution states for a job
app.patch("/api/jobs/:jobId/resolutions", async (req, res) => {
  try {
    const { jobId } = req.params;
    const { resolutions } = req.body;
    if (!resolutions || !Array.isArray(resolutions)) {
      res.status(400).json({ error: "Missing resolutions array" });
      return;
    }
    const job = await getJob(jobId);
    if (!job) {
      res.status(404).json({ error: "Job not found" });
      return;
    }
    await updateJob(jobId, { resolutions: JSON.stringify(resolutions) });
    res.json({ ok: true });
  } catch (err: any) {
    console.error("[jobs] Resolutions save error:", err);
    res.status(500).json({ error: err.message || "Failed to save resolutions" });
  }
});

// ─── Apply user resolutions to analysis before import ───
function applyUserCorrections(analysis: AnalysisResult, resolutionsJson: string | null): void {
  // 1. Merge saved mapping overrides into analysis.mappings
  const overrides = getOverridesForTable(analysis.targetTable);
  if (overrides.size > 0) {
    const mappingsBySource = new Map(analysis.mappings.map((m) => [m.source, m]));
    for (const [sourceCol, targetCol] of overrides) {
      const existing = mappingsBySource.get(sourceCol);
      if (existing) {
        existing.target = targetCol;
        existing.confidence = 1.0;
      } else {
        // New mapping from override (was previously unmapped)
        const newMapping: MappingSpec = {
          source: sourceCol,
          target: targetCol,
          confidence: 1.0,
        };
        // Infer transform from target name
        if (/caseid/i.test(targetCol)) newMapping.transform = "normalizeCaseId";
        else if (/patient/i.test(targetCol)) newMapping.transform = "normalizePatientId";
        else if (/date|dt$/i.test(targetCol)) newMapping.transform = "normalizeDate";
        analysis.mappings.push(newMapping);
      }
    }
    // Remove from unmapped list
    analysis.unmapped = analysis.unmapped.filter((col) => !overrides.has(col));
    console.log(`[import] Applied ${overrides.size} mapping overrides`);
  }

  // 2. Apply data corrections from resolutions to raw rows
  if (!resolutionsJson) return;
  let resolutions: any[];
  try { resolutions = JSON.parse(resolutionsJson); } catch { return; }

  const dataFixes = resolutions.filter(
    (r: any) => r.manualValue && r.field && r.type === "data" && (r.status === "accepted" || r.status === "manual"),
  );
  if (dataFixes.length === 0) return;

  // Build reverse lookup: target field → source header index
  const targetToSourceIdx = new Map<string, number>();
  for (const m of analysis.mappings) {
    const idx = analysis._rawHeaders.indexOf(m.source);
    if (idx >= 0) targetToSourceIdx.set(m.target, idx);
  }

  let applied = 0;
  for (const fix of dataFixes) {
    const headerIdx = targetToSourceIdx.get(fix.field);
    if (headerIdx == null) continue;

    if (fix.row != null && fix.row >= 0 && fix.row < analysis._rawRows.length) {
      // Fix specific row
      analysis._rawRows[fix.row][headerIdx] = fix.manualValue;
      applied++;
    }
    // Column-wide fixes without a specific row are skipped — auto-normalizers handle those
  }
  if (applied > 0) {
    console.log(`[import] Applied ${applied} data corrections from resolutions`);
  }
}

// ─── POST /api/jobs/:jobId/import ───
// Trigger DB import for a job in awaiting_review
const importingJobs = new Set<string>();
app.post("/api/jobs/:jobId/import", async (req, res) => {
  try {
    const { jobId } = req.params;
    if (importingJobs.has(jobId)) {
      res.status(409).json({ error: "Import already in progress for this job" });
      return;
    }
    importingJobs.add(jobId);

    const job = await getJob(jobId);
    if (!job) {
      importingJobs.delete(jobId);
      res.status(404).json({ error: "Job not found" });
      return;
    }
    if (job.status !== "awaiting_review" && job.status !== "done" && job.status !== "failed") {
      importingJobs.delete(jobId);
      res.status(409).json({ error: `Job is in '${job.status}' state, expected 'awaiting_review', 'done', or 'failed'` });
      return;
    }

    const analysis = analysisStore.get(jobId);
    if (!analysis) {
      importingJobs.delete(jobId);
      res.status(410).json({ error: "Analysis data no longer in memory. Please re-upload the file." });
      return;
    }

    // Apply user corrections (overrides + data fixes) before import
    applyUserCorrections(analysis, job.resolutions);
    await updateJob(jobId, { status: "importing", progressMsg: "Import gestartet...", inserted: 0, skipped: 0 });
    res.status(202).json({ ok: true });

    // Fire-and-forget import
    (async () => {
      try {
        console.log(`[jobs] ${jobId} importing → ${analysis.targetTable}`);
        const result = await importToDb(analysis);
        await updateJob(jobId, {
          status: "done",
          inserted: result.inserted,
          skipped: result.skipped,
          progressMsg: `${result.inserted} eingefügt, ${result.skipped} übersprungen`,
        });
        console.log(`[jobs] ${jobId} import done: ${result.inserted} inserted, ${result.skipped} skipped`);
      } catch (err: any) {
        console.error(`[jobs] ${jobId} import failed:`, err);
        await updateJob(jobId, { status: "failed", error: err.message || "Import failed" }).catch(() => {});
      } finally {
        importingJobs.delete(jobId);
      }
    })();
  } catch (err: any) {
    console.error("[jobs] Import error:", err);
    res.status(500).json({ error: err.message || "Failed to start import" });
  }
});

// ─── POST /api/jobs/:jobId/dump ───
// Generate SQL dump for a job
app.post("/api/jobs/:jobId/dump", async (req, res) => {
  try {
    const { jobId } = req.params;
    const analysis = analysisStore.get(jobId);
    if (!analysis) {
      res.status(410).json({ error: "Analysis data no longer in memory. Please re-upload the file." });
      return;
    }

    const sql = generateSqlDump(analysis);
    res.setHeader("Content-Type", "application/sql");
    res.setHeader("Content-Disposition", `attachment; filename="${analysis.fileName.replace(/\.[^.]+$/, "")}_import.sql"`);
    res.send(sql);
  } catch (err: any) {
    console.error("[jobs] Dump error:", err);
    res.status(500).json({ error: err.message || "Dump failed" });
  }
});

// ─── GET /api/jobs/:jobId/sql-preview ───
// Return first N lines of SQL dump as text (for preview)
app.get("/api/jobs/:jobId/sql-preview", (req, res) => {
  try {
    const { jobId } = req.params;
    const analysis = analysisStore.get(jobId);
    if (!analysis) {
      res.status(410).json({ error: "Analysis data no longer in memory." });
      return;
    }
    const sql = generateSqlDump(analysis);
    const allLines = sql.split("\n");
    const lines = allLines.filter((l) => !l.startsWith("--"));
    const maxLines = Math.min(parseInt(req.query.lines as string) || 12, 50);
    res.json({ preview: lines.slice(0, maxLines).join("\n"), totalLines: lines.length });
  } catch (err: any) {
    res.status(500).json({ error: err.message || "Preview failed" });
  }
});

// ─── DELETE /api/jobs/:jobId ───
// Delete a job from DB and memory
app.delete("/api/jobs/:jobId", async (req, res) => {
  try {
    const { jobId } = req.params;
    analysisStore.delete(jobId);
    await deleteJob(jobId);
    console.log(`[jobs] Deleted ${jobId}`);
    res.json({ ok: true });
  } catch (err: any) {
    console.error("[jobs] Delete error:", err);
    res.status(500).json({ error: err.message || "Failed to delete job" });
  }
});

// ─── POST /api/hermes/send ───
// Send an email notification about an anomaly
app.post("/api/hermes/send", async (req, res) => {
  try {
    const { to, toEmail, subject, body } = req.body;
    if (!toEmail || !body) {
      res.status(400).json({ error: "Missing toEmail or body" });
      return;
    }

    const smtpHost = process.env.SMTP_HOST;
    const from = process.env.SMTP_FROM || "hermes@olympus-connect.local";
    const emailSubject = subject || "Hermes – Rückfrage zur Datenqualität";

    if (!smtpHost) {
      // No SMTP configured — log to console and return success (demo mode)
      console.log(`[hermes] (Demo) Email to ${to} <${toEmail}>:`);
      console.log(`  Subject: ${emailSubject}`);
      console.log(`  Body:\n${body.split("\n").map((l: string) => "    " + l).join("\n")}`);
      res.json({ ok: true, demo: true });
      return;
    }

    const transporter = nodemailer.createTransport({
      host: smtpHost,
      port: parseInt(process.env.SMTP_PORT || "587", 10),
      secure: parseInt(process.env.SMTP_PORT || "587", 10) === 465,
      auth: process.env.SMTP_USER ? {
        user: process.env.SMTP_USER,
        pass: process.env.SMTP_PASS || "",
      } : undefined,
    });

    await transporter.sendMail({
      from,
      to: toEmail,
      subject: emailSubject,
      text: body,
    });

    console.log(`[hermes] Email sent to ${to} <${toEmail}>`);
    res.json({ ok: true });
  } catch (err: any) {
    console.error("[hermes] Send error:", err);
    res.status(500).json({ error: err.message || "Failed to send email" });
  }
});

// ─── Start ───
app.listen(PORT, async () => {
  console.log(`epaCC Engine API running on http://localhost:${PORT}`);
  console.log(`LLM: ${llm.getProviderInfo().provider} (${llm.getProviderInfo().model})`);

  // Auto-migrate jobs table
  try {
    await ensureJobsTable();
    console.log("tbImportJobs table ready");
  } catch (err: any) {
    console.warn("Could not initialize jobs table (DB may be unavailable):", err.message);
  }
});

// Graceful shutdown
process.on("SIGINT", async () => {
  console.log("\nShutting down...");
  await close().catch(() => {});
  process.exit(0);
});
