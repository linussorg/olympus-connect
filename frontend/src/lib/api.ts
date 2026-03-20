// API client for the epaCC mapping engine

export interface MappingSpec {
  source: string;
  target: string;
  transform?: string;
  confidence: number;
}

export interface ValidationIssue {
  severity: "error" | "warning" | "info";
  field: string;
  row?: number;
  value?: string;
  message: string;
  suggestion?: string;
  category?: string;
  origin?: string;
  autoFix?: boolean;
  affectedCount?: number;
}

export interface AnomalyReport {
  byCategory: Record<string, { count: number; autoFixable: number; issues: ValidationIssue[] }>;
  bySeverity: { error: number; warning: number; info: number };
  topIssues: ValidationIssue[];
  totalIssues: number;
  autoFixRate: number;
  allIssues: ValidationIssue[];
}

export interface AnalysisResult {
  fileId: string;
  fileName: string;
  format: string;
  detectedType: string;
  targetTable: string;
  mappingType: string;
  mappings: MappingSpec[];
  ambiguous: { sourceColumn: string; candidates: { target: string; confidence: number; reason: string }[]; question: string }[];
  unmapped: string[];
  rowCount: number;
  columnCount?: number;
  preview: Record<string, string | null>[];
  issues: ValidationIssue[];
  anomalyReport?: AnomalyReport;
  confidence: number;
  needsUserInput: boolean;
}

export interface ImportResult {
  targetTable: string;
  inserted: number;
  skipped: number;
  errors: string[];
}

export interface TableSchema {
  name: string;
  description: string;
  columns: { name: string; type: string; nullable: boolean; description?: string }[];
}

export interface ChatChart {
  type: "bar" | "line" | "pie";
  labelKey: string;
  valueKeys: string[];
}

export interface ChatResponse {
  text: string;
  sql?: string;
  table?: { headers: string[]; rows: string[][] };
  chart?: ChatChart;
}

export interface ChatHistoryMessage {
  role: "user" | "assistant";
  content: string;
}

// ─── Helpers ───
function isNetworkError(err: unknown): boolean {
  return err instanceof TypeError && (err.message === "Failed to fetch" || err.message === "Load failed");
}

const ENGINE_DOWN_MSG = "Engine-Server nicht erreichbar. Bitte starten Sie den Server mit: cd engine && npm run serve";

// ─── Upload & Analyze ───
export async function uploadFile(file: File): Promise<AnalysisResult> {
  const form = new FormData();
  form.append("file", file);

  let res: Response;
  try {
    res = await fetch("/api/upload", { method: "POST", body: form });
  } catch (err) {
    throw new Error(isNetworkError(err) ? ENGINE_DOWN_MSG : String(err));
  }
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(err.error || "Upload failed");
  }
  return res.json();
}

// ─── Upload & Analyze (Streaming) ───
export interface UploadProgressEvent {
  step: string;
  message: string;
  detectedType?: string;
  targetTable?: string;
  rowCount?: number;
  issueCount?: number;
  errorCount?: number;
  autoFixRate?: number;
  mapped?: number;
  total?: number;
  attempt?: number;
  mapping?: { source: string; target: string; confidence?: number };
  issues?: ValidationIssue[];
  anomalyReport?: AnomalyReport;
  result?: AnalysisResult;
}

export async function uploadFileStream(
  file: File,
  onProgress: (event: UploadProgressEvent) => void,
): Promise<AnalysisResult> {
  const form = new FormData();
  form.append("file", file);

  let res: Response;
  try {
    res = await fetch("/api/upload/stream", { method: "POST", body: form });
  } catch (err) {
    throw new Error(isNetworkError(err) ? ENGINE_DOWN_MSG : String(err));
  }
  if (!res.ok) {
    throw new Error("Upload failed");
  }

  const reader = res.body?.getReader();
  if (!reader) throw new Error("No response stream");

  const decoder = new TextDecoder();
  let buffer = "";
  let finalResult: AnalysisResult | null = null;

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split("\n");
    buffer = lines.pop() || "";

    for (const line of lines) {
      if (!line.startsWith("data: ")) continue;
      try {
        const event = JSON.parse(line.slice(6));
        if (event.step === "done" && event.result) {
          finalResult = event.result;
        } else if (event.step === "error") {
          throw new Error(event.message);
        }
        onProgress(event);
      } catch (e) {
        if (e instanceof Error && e.message !== "Unexpected end of JSON input") throw e;
      }
    }
  }

  if (!finalResult) throw new Error("No result received from analysis stream");
  return finalResult;
}

// ─── Import to DB ───
export async function importToDb(fileId: string): Promise<ImportResult> {
  let res: Response;
  try {
    res = await fetch("/api/import", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ fileId }),
    });
  } catch (err) {
    throw new Error(isNetworkError(err) ? ENGINE_DOWN_MSG : String(err));
  }
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(err.error || "Import failed");
  }
  return res.json();
}

// ─── Download SQL Dump ───
export async function downloadSqlDump(fileId: string): Promise<void> {
  let res: Response;
  try {
    res = await fetch("/api/dump", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ fileId }),
    });
  } catch (err) {
    throw new Error(isNetworkError(err) ? ENGINE_DOWN_MSG : String(err));
  }
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(err.error || "Dump failed");
  }

  const disposition = res.headers.get("Content-Disposition");
  const match = disposition?.match(/filename="(.+)"/);
  const fileName = match?.[1] || "export.sql";

  const blob = await res.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = fileName;
  a.click();
  URL.revokeObjectURL(url);
}

// ─── Schema ───
export async function getSchemas(): Promise<TableSchema[]> {
  const res = await fetch("/api/schema");
  if (!res.ok) throw new Error("Failed to fetch schemas");
  return res.json();
}

// ─── Test DB Connection ───
export async function testConnection(): Promise<{ ok: boolean; error?: string }> {
  try {
    const res = await fetch("/api/test-connection", { method: "POST" });
    if (!res.ok) return { ok: false, error: res.statusText };
    return res.json();
  } catch (err) {
    return { ok: false, error: isNetworkError(err) ? ENGINE_DOWN_MSG : String(err) };
  }
}

// ─── Chat ───
export async function chat(
  message: string,
  history?: ChatHistoryMessage[]
): Promise<ChatResponse> {
  let res: Response;
  try {
    res = await fetch("/api/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message, history }),
    });
  } catch (err) {
    throw new Error(isNetworkError(err) ? ENGINE_DOWN_MSG : String(err));
  }
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(err.error || "Chat failed");
  }
  return res.json();
}

// ─── LLM Config ───
export interface LlmConfig {
  provider: string;
  model: string;
  url: string;
  apiKeyMasked: string;
  hasApiKey: boolean;
}

export async function getLlmConfig(): Promise<LlmConfig> {
  const res = await fetch("/api/llm-config");
  if (!res.ok) throw new Error("Failed to fetch LLM config");
  return res.json();
}

export async function setLlmConfig(cfg: { provider?: string; apiKey?: string; model?: string }): Promise<LlmConfig> {
  let res: Response;
  try {
    res = await fetch("/api/llm-config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(cfg),
    });
  } catch (err) {
    throw new Error(isNetworkError(err) ? ENGINE_DOWN_MSG : String(err));
  }
  if (!res.ok) throw new Error("Failed to update LLM config");
  return res.json();
}

export async function testLlm(): Promise<{ ok: boolean; provider: string; model: string; error?: string }> {
  try {
    const res = await fetch("/api/test-llm", { method: "POST" });
    if (!res.ok) return { ok: false, provider: "", model: "", error: res.statusText };
    return res.json();
  } catch (err) {
    return { ok: false, provider: "", model: "", error: isNetworkError(err) ? ENGINE_DOWN_MSG : String(err) };
  }
}

// ─── Quality Dashboard ───
export interface QualityData {
  totalRows: number;
  totalSources: number;
  averageConfidence: number;
  totalAnomalies: number;
  errorsByCategory: { category: string; count: number }[];
  recentUploads: { file: string; detectedType: string; rows: number; confidence: number; issues: number; status: string }[];
  fileCount: number;
}

export async function getQuality(): Promise<QualityData> {
  try {
    const res = await fetch("/api/quality");
    if (!res.ok) throw new Error("Quality fetch failed");
    return res.json();
  } catch (err) {
    throw new Error(isNetworkError(err) ? ENGINE_DOWN_MSG : String(err));
  }
}

// ─── Mapping Overrides ───
export interface MappingOverrideEntry {
  id: string;
  sourceColumn: string;
  targetColumn: string;
  targetTable: string;
  createdAt: string;
  updatedAt: string;
}

export async function getOverrides(): Promise<MappingOverrideEntry[]> {
  try {
    const res = await fetch("/api/overrides");
    if (!res.ok) return [];
    return res.json();
  } catch {
    return [];
  }
}

export async function saveOverride(sourceColumn: string, targetColumn: string, targetTable: string): Promise<MappingOverrideEntry> {
  const res = await fetch("/api/overrides", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ sourceColumn, targetColumn, targetTable }),
  });
  if (!res.ok) throw new Error("Failed to save override");
  return res.json();
}

export async function updateOverride(id: string, targetColumn: string): Promise<MappingOverrideEntry> {
  const res = await fetch(`/api/overrides/${encodeURIComponent(id)}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ targetColumn }),
  });
  if (!res.ok) throw new Error("Failed to update override");
  return res.json();
}

export async function deleteOverride(id: string): Promise<void> {
  const res = await fetch(`/api/overrides/${encodeURIComponent(id)}`, { method: "DELETE" });
  if (!res.ok) throw new Error("Failed to delete override");
}

// ─── Database Explorer ───
export interface TableInfo {
  name: string;
  description?: string;
  rowCount: number;
  columnCount: number;
}

export interface ColumnInfo {
  name: string;
  type: string;
  description: string | null;
}

export interface TablePreview {
  table: { headers: string[]; rows: string[][] };
  columns: ColumnInfo[];
}

export interface SqlResult {
  sql: string;
  headers: string[];
  rows: string[][];
  error?: string;
}

export async function getTableList(): Promise<TableInfo[]> {
  let res: Response;
  try {
    res = await fetch("/api/tables");
  } catch (err) {
    throw new Error(isNetworkError(err) ? ENGINE_DOWN_MSG : String(err));
  }
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(err.error || "Failed to load tables");
  }
  return res.json();
}

export async function getTablePreview(tableName: string): Promise<TablePreview> {
  const res = await fetch(`/api/tables/${encodeURIComponent(tableName)}/preview`);
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(err.error || "Preview failed");
  }
  return res.json();
}

export async function executeSql(query: string): Promise<SqlResult> {
  const res = await fetch("/api/sql", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(err.error || "Query failed");
  }
  return res.json();
}

// ─── Athena Dashboard ───
export interface HarmonizationInsight {
  id: string;
  title: string;
  description: string;
  recommendation: string;
  affectedRows: number;
  priority: "high" | "medium" | "low";
  categories: string[];
}

export interface RiskPrediction {
  id: string;
  risk: string;
  explanation: string;
  likelihood: "high" | "medium" | "low";
  impact: "high" | "medium" | "low";
  mitigation: string;
  relatedCategories: string[];
}

export interface AthenaInsights {
  insights: HarmonizationInsight[];
  risks: RiskPrediction[];
  generatedAt: string;
  llmGenerated: boolean;
}

export interface AthenaData {
  anomalyReport: AnomalyReport | null;
  jobHistory: { fileName: string; createdAt: string; errorCount: number; issueCount: number; confidence: number; detectedType: string; autoFixRate: number }[];
  overrideCount: number;
  tables: { name: string; rowCount: number }[];
}

export async function getAthenaData(): Promise<AthenaData> {
  try {
    const res = await fetch("/api/athena");
    if (!res.ok) throw new Error("Athena fetch failed");
    return res.json();
  } catch (err) {
    throw new Error(isNetworkError(err) ? ENGINE_DOWN_MSG : String(err));
  }
}

export async function getAthenaInsights(regenerate = false): Promise<AthenaInsights | null> {
  try {
    const url = regenerate ? "/api/athena/insights?regenerate=true" : "/api/athena/insights";
    const res = await fetch(url);
    if (!res.ok) return null;
    return res.json();
  } catch {
    return null;
  }
}

// ─── Danger Zone ───
export async function truncateAllTables(): Promise<{ ok: boolean; tables: number }> {
  let res: Response;
  try {
    res = await fetch("/api/truncate-all", { method: "POST" });
  } catch (err) {
    throw new Error(isNetworkError(err) ? ENGINE_DOWN_MSG : String(err));
  }
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(err.error || "Truncate failed");
  }
  return res.json();
}

// ─── Hermes Email ───
export async function sendHermesEmail(
  to: string, toEmail: string, subject: string, body: string,
): Promise<{ ok: boolean; demo?: boolean }> {
  let res: Response;
  try {
    res = await fetch("/api/hermes/send", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ to, toEmail, subject, body }),
    });
  } catch (err) {
    throw new Error(isNetworkError(err) ? ENGINE_DOWN_MSG : String(err));
  }
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(err.error || "Failed to send email");
  }
  return res.json();
}

// ─── Server Status ───
export async function getStatus(): Promise<{ ok: boolean; analyses: number; llm: { provider: string; model: string } }> {
  const res = await fetch("/api/status");
  if (!res.ok) throw new Error("Engine not reachable");
  return res.json();
}

// ─── Import Jobs ───
export type JobStatus = "uploading" | "analyzing" | "awaiting_review" | "importing" | "done" | "failed";

export interface JobSummary {
  jobId: string;
  status: JobStatus;
  fileName: string | null;
  fileSize: number | null;
  detectedType: string | null;
  targetTable: string | null;
  mappingType: string | null;
  rowCount: number | null;
  confidence: number | null;
  issueCount: number | null;
  errorCount: number | null;
  autoFixRate: number | null;
  inserted: number | null;
  skipped: number | null;
  progressMsg: string | null;
  error: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface AnomalyResolution {
  anomalyId: number;
  status: "accepted" | "ignored" | "manual";
  manualValue?: string;
  field?: string;
  row?: number;
  type?: string;
}

export interface JobDetail extends JobSummary {
  analysis: AnalysisResult | null;
  resolutions: AnomalyResolution[] | null;
}

export async function createJobUpload(file: File): Promise<{ jobId: string }> {
  const form = new FormData();
  form.append("file", file);
  let res: Response;
  try {
    res = await fetch("/api/jobs", { method: "POST", body: form });
  } catch (err) {
    throw new Error(isNetworkError(err) ? ENGINE_DOWN_MSG : String(err));
  }
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(err.error || "Upload failed");
  }
  return res.json();
}

export async function getJobDetail(jobId: string): Promise<JobDetail> {
  let res: Response;
  try {
    res = await fetch(`/api/jobs/${encodeURIComponent(jobId)}`);
  } catch (err) {
    throw new Error(isNetworkError(err) ? ENGINE_DOWN_MSG : String(err));
  }
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(err.error || "Job not found");
  }
  return res.json();
}

export async function listJobSummaries(): Promise<JobSummary[]> {
  let res: Response;
  try {
    res = await fetch("/api/jobs");
  } catch (err) {
    throw new Error(isNetworkError(err) ? ENGINE_DOWN_MSG : String(err));
  }
  if (!res.ok) return [];
  return res.json();
}

export async function saveJobResolutions(jobId: string, resolutions: AnomalyResolution[]): Promise<void> {
  let res: Response;
  try {
    res = await fetch(`/api/jobs/${encodeURIComponent(jobId)}/resolutions`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ resolutions }),
    });
  } catch (err) {
    throw new Error(isNetworkError(err) ? ENGINE_DOWN_MSG : String(err));
  }
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(err.error || "Failed to save resolutions");
  }
}

export async function importJobToDb(jobId: string): Promise<void> {
  let res: Response;
  try {
    res = await fetch(`/api/jobs/${encodeURIComponent(jobId)}/import`, { method: "POST" });
  } catch (err) {
    throw new Error(isNetworkError(err) ? ENGINE_DOWN_MSG : String(err));
  }
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(err.error || "Import failed");
  }
}

export async function downloadJobDump(jobId: string): Promise<void> {
  let res: Response;
  try {
    res = await fetch(`/api/jobs/${encodeURIComponent(jobId)}/dump`, { method: "POST" });
  } catch (err) {
    throw new Error(isNetworkError(err) ? ENGINE_DOWN_MSG : String(err));
  }
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(err.error || "Dump failed");
  }
  const disposition = res.headers.get("Content-Disposition");
  const match = disposition?.match(/filename="(.+)"/);
  const fileName = match?.[1] || "export.sql";
  const blob = await res.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = fileName;
  a.click();
  URL.revokeObjectURL(url);
}

export async function getJobSqlPreview(jobId: string, lines = 12): Promise<{ preview: string; totalLines: number }> {
  let res: Response;
  try {
    res = await fetch(`/api/jobs/${encodeURIComponent(jobId)}/sql-preview?lines=${lines}`);
  } catch (err) {
    throw new Error(isNetworkError(err) ? ENGINE_DOWN_MSG : String(err));
  }
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(err.error || "Preview failed");
  }
  return res.json();
}

export async function deleteJobById(jobId: string): Promise<void> {
  let res: Response;
  try {
    res = await fetch(`/api/jobs/${encodeURIComponent(jobId)}`, { method: "DELETE" });
  } catch (err) {
    throw new Error(isNetworkError(err) ? ENGINE_DOWN_MSG : String(err));
  }
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(err.error || "Delete failed");
  }
}
