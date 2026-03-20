// ─── Job Status ───

export type JobStatus = "uploading" | "analyzing" | "awaiting_review" | "importing" | "done" | "failed";

// ─── Anomaly Categories ───

export type AnomalyCategory =
  | "source-format"
  | "encoding"
  | "null-variant"
  | "date-format"
  | "id-format"
  | "out-of-range"
  | "flag-drift"
  | "duplicate"
  | "orphan"
  | "temporal"
  | "completeness"
  | "free-text"
  | "demographic";

// ─── Mapping Types ───

export type MappingType = "column-rename" | "pivot" | "code-translate";

export interface MappingSpec {
  source: string;
  target: string;
  transform?: "none" | "parseFloat" | "parseInt" | "normalizeCaseId" | "normalizePatientId" | "normalizeDate" | "normalizeNull" | "normalizeFlag" | "normalizeShift" | "normalizeFreeText";
  confidence: number; // 0-1
}

export interface AmbiguousMapping {
  sourceColumn: string;
  candidates: { target: string; confidence: number; reason: string }[];
  question: string;
}

export interface MappingOverride {
  sourceColumn: string;
  targetColumn: string;
}

// ─── Validation ───

export interface ValidationIssue {
  severity: "error" | "warning" | "info";
  field: string;
  row?: number;
  value?: string;
  message: string;
  suggestion?: string;
  category?: AnomalyCategory;
  origin?: string;
  autoFix?: boolean;
  affectedCount?: number;
}

export interface AnomalyCategoryStats {
  count: number;
  autoFixable: number;
  issues: ValidationIssue[];
}

export interface AnomalyReport {
  byCategory: Partial<Record<AnomalyCategory, AnomalyCategoryStats>>;
  bySeverity: { error: number; warning: number; info: number };
  topIssues: ValidationIssue[];
  autoFixRate: number;
  totalIssues: number;
  allIssues: ValidationIssue[];
}

// ─── Analysis Result ───

export interface AnalysisResult {
  fileId: string;
  fileName: string;
  format: string;
  detectedType: string;
  targetTable: string;
  mappingType: MappingType;

  mappings: MappingSpec[];
  ambiguous: AmbiguousMapping[];
  unmapped: string[];

  rowCount: number;
  columnCount: number;
  preview: Record<string, string | null>[];
  issues: ValidationIssue[];
  anomalyReport?: AnomalyReport;

  confidence: number;
  needsUserInput: boolean;

  // Raw parsed data (kept in memory for import)
  _rawHeaders: string[];
  _rawRows: string[][];
}

// ─── Import Result ───

export interface ImportResult {
  targetTable: string;
  inserted: number;
  skipped: number;
  deduplicated: number;
  errors: string[];
}

// ─── Parsed File ───

export interface ParsedFile {
  headers: string[];
  rows: string[][];
  delimiter: string;
  format: "csv" | "xlsx" | "pdf" | "unknown";
}

// ─── Detection Result ───

export interface DetectionResult {
  format: "csv" | "xlsx" | "pdf" | "unknown";
  delimiter: string;
  detectedType: string;
  targetTable: string;
  mappingType: MappingType;
  confidence: number;
}

// ─── Schema ───

export interface ColumnDef {
  name: string;
  type: "bigint" | "smallint" | "nvarchar" | "datetime" | "numeric";
  nullable: boolean;
  description?: string;
}

export interface TableSchema {
  name: string;
  description: string;
  columns: ColumnDef[];
}
