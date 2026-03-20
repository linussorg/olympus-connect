import type { AnalysisResult, MappingSpec } from "../types.js";
import { parseFile, parseBuffer } from "../parse.js";
import { detect } from "../detect.js";
import { validateAll, buildAnomalyReport, validateDemographics } from "../validators/index.js";
import { generateColumnMappings } from "./column-rename.js";
import { pivotEpaACData1 } from "./pivot.js";
import { translateCodeData } from "./code-translate.js";
import {
  normalizeNull,
  normalizeCaseId,
  normalizePatientId,
  normalizeDate,
  normalizeNumeric,
  normalizeFlag,
  normalizeShift,
  normalizeFreeText,
} from "../normalize.js";
import { inferNursingHeaders } from "../parse.js";
import type { ValidationIssue } from "../types.js";

const IDENTITY_TARGETS = new Set(["coCaseId", "coPatientId", "coPatient_id", "coE2I222"]);

function checkIdentityMapping(mappings: MappingSpec[], rowCount: number): ValidationIssue | null {
  if (mappings.length === 0) return null;
  const hasIdentity = mappings.some((m) => IDENTITY_TARGETS.has(m.target));
  if (hasIdentity) return null;
  return {
    severity: "error",
    field: "case_id / patient_id",
    message: "No column is mapped to a patient or case identifier (coCaseId, coPatientId). All rows would be skipped on import. Please map an identity column manually.",
    category: "id-format",
    origin: "None of the source columns were recognized as a case or patient ID — this may be due to non-standard column names.",
    autoFix: false,
    affectedCount: rowCount,
  };
}

export async function analyze(filePath: string): Promise<AnalysisResult> {
  const fileName = filePath.split("/").pop() || filePath;

  // 1. Parse
  let parsed = await parseFile(filePath);
  // Headerless CSV: try nursing inference if filename suggests nursing
  if (parsed.headers.length === 0 && parsed.rows.length > 0 && /nurs/i.test(fileName)) {
    parsed = inferNursingHeaders(parsed);
  }
  if (parsed.headers.length === 0) {
    return emptyResult(fileName, `Could not parse file (format: ${parsed.format})`);
  }

  // 2. Detect
  const detection = detect(parsed, fileName);

  // 3. Validate raw data + demographics
  const issues = validateAll(parsed, detection.detectedType);
  const demoIssues = await validateDemographics(parsed, detection.detectedType, detection.targetTable).catch(() => []);
  issues.push(...demoIssues);
  const anomalyReport = buildAnomalyReport(issues);

  // 4. Generate mappings based on type
  let mappings: MappingSpec[] = [];
  let ambiguous: AnalysisResult["ambiguous"] = [];
  let unmapped: string[] = [];

  if (detection.mappingType === "column-rename") {
    const result = await generateColumnMappings(parsed, detection.targetTable);
    mappings = result.mappings;
    ambiguous = result.ambiguous;
    unmapped = result.unmapped;
  } else if (detection.mappingType === "pivot") {
    const pivotResult = pivotEpaACData1(parsed);
    mappings = pivotResult.mappings;
    ambiguous = pivotResult.ambiguous;
    unmapped = pivotResult.unmapped;

    const pivotPreview = pivotResult.pivotedRows.slice(0, 5);

    return {
      fileId: `file-${Date.now()}`,
      fileName,
      format: parsed.format.toUpperCase(),
      detectedType: detection.detectedType,
      targetTable: detection.targetTable,
      mappingType: detection.mappingType,
      mappings,
      ambiguous,
      unmapped,
      rowCount: pivotResult.totalGroups,
      columnCount: parsed.headers.length,
      preview: pivotPreview,
      issues,
      anomalyReport,
      confidence: pivotResult.resolvedSids > 0
        ? Math.min(0.95, pivotResult.resolvedSids / (pivotResult.resolvedSids + pivotResult.unresolvedSids.length))
        : 0,
      needsUserInput: ambiguous.length > 0,
      _rawHeaders: parsed.headers,
      _rawRows: parsed.rows,
    };
  } else if (detection.mappingType === "code-translate") {
    const result = await translateCodeData(parsed, detection.detectedType);
    mappings = result.mappings;
    ambiguous = result.ambiguous;
    unmapped = result.unmapped;

    // For Data-3, use the corrected data rows (skip IID code row)
    // Code-translate computes its own confidence (accounts for intentionally skipped columns)
    const ctPreview = applyMappingsPreview(parsed.headers,
      (result.dataRows !== parsed.rows ? result.dataRows : parsed.rows).slice(0, 5), mappings);
    return {
      fileId: `file-${Date.now()}`,
      fileName, format: parsed.format.toUpperCase(),
      detectedType: detection.detectedType, targetTable: detection.targetTable,
      mappingType: detection.mappingType,
      mappings, ambiguous, unmapped,
      rowCount: result.dataRows.length,
      columnCount: parsed.headers.length,
      preview: ctPreview, issues, anomalyReport,
      confidence: Math.round(result.confidence * 100) / 100,
      needsUserInput: ambiguous.length > 0 || result.confidence < 0.5,
      _rawHeaders: parsed.headers,
      _rawRows: result.dataRows,
    };
  }

  // 5. Apply auto-fix normalizations to raw data
  if (mappings.length > 0) {
    normalizeRawRows(parsed.headers, parsed.rows, mappings);
  }

  // 6. Check if any mapping targets an identity column
  const identityIssue = checkIdentityMapping(mappings, parsed.rows.length);
  if (identityIssue) issues.push(identityIssue);

  // 7. Generate preview (first 5 rows — mapped if possible, raw otherwise)
  const preview = mappings.length > 0
    ? applyMappingsPreview(parsed.headers, parsed.rows.slice(0, 5), mappings)
    : rawPreview(parsed.headers, parsed.rows.slice(0, 5));

  // 8. Compute confidence
  const mappedRatio = mappings.length / Math.max(parsed.headers.length, 1);
  const avgConfidence = mappings.length > 0
    ? mappings.reduce((sum, m) => sum + m.confidence, 0) / mappings.length
    : 0;
  const confidence = mappedRatio * 0.6 + avgConfidence * 0.3 + detection.confidence * 0.1;

  return {
    fileId: `file-${Date.now()}`,
    fileName,
    format: parsed.format.toUpperCase(),
    detectedType: detection.detectedType,
    targetTable: detection.targetTable,
    mappingType: detection.mappingType,
    mappings,
    ambiguous,
    unmapped,
    rowCount: parsed.rows.length,
    columnCount: parsed.headers.length,
    preview,
    issues,
    anomalyReport,
    confidence: Math.round(confidence * 100) / 100,
    needsUserInput: ambiguous.length > 0 || confidence < 0.5 || !!identityIssue,
    _rawHeaders: parsed.headers,
    _rawRows: parsed.rows,
  };
}

export interface ProgressEvent {
  step: string;
  message: string;
  [key: string]: any;
}

export async function analyzeBuffer(
  buffer: Buffer,
  fileName: string,
  onProgress?: (event: ProgressEvent) => void,
): Promise<AnalysisResult> {
  onProgress?.({ step: "parsing", message: "Datei wird gelesen..." });
  let parsed = await parseBuffer(buffer, fileName);
  // Headerless CSV: try nursing inference if filename suggests nursing
  if (parsed.headers.length === 0 && parsed.rows.length > 0 && /nurs/i.test(fileName)) {
    parsed = inferNursingHeaders(parsed);
  }
  if (parsed.headers.length === 0) {
    return emptyResult(fileName, `Could not parse file (format: ${parsed.format})`);
  }

  onProgress?.({ step: "detecting", message: "Datentyp wird erkannt..." });
  const detection = detect(parsed, fileName);
  onProgress?.({
    step: "detected",
    message: `Erkannt: ${detection.detectedType}`,
    detectedType: detection.detectedType,
    targetTable: detection.targetTable,
    rowCount: parsed.rows.length,
  });

  onProgress?.({ step: "validating", message: "Daten werden validiert..." });
  const issues = validateAll(parsed, detection.detectedType);
  const demoIssues = await validateDemographics(parsed, detection.detectedType, detection.targetTable).catch(() => []);
  issues.push(...demoIssues);
  const anomalyReport = buildAnomalyReport(issues);
  onProgress?.({
    step: "validated",
    message: `${issues.length} Anomalien erkannt`,
    issueCount: issues.length,
    errorCount: anomalyReport.bySeverity.error,
    autoFixRate: anomalyReport.autoFixRate,
    issues: issues.filter((i) => i.severity !== "info"),
    anomalyReport,
  });

  let mappings: MappingSpec[] = [];
  let ambiguous: AnalysisResult["ambiguous"] = [];
  let unmapped: string[] = [];

  if (detection.mappingType === "column-rename") {
    const result = await generateColumnMappings(parsed, detection.targetTable, onProgress);
    mappings = result.mappings;
    ambiguous = result.ambiguous;
    unmapped = result.unmapped;
  } else if (detection.mappingType === "pivot") {
    const pivotResult = pivotEpaACData1(parsed);
    mappings = pivotResult.mappings;
    ambiguous = pivotResult.ambiguous;
    unmapped = pivotResult.unmapped;

    const pivotPreview = pivotResult.pivotedRows.slice(0, 5);
    return {
      fileId: `file-${Date.now()}`,
      fileName,
      format: parsed.format.toUpperCase(),
      detectedType: detection.detectedType,
      targetTable: detection.targetTable,
      mappingType: detection.mappingType,
      mappings, ambiguous, unmapped,
      rowCount: pivotResult.totalGroups,
      columnCount: parsed.headers.length,
      preview: pivotPreview,
      issues, anomalyReport,
      confidence: pivotResult.resolvedSids > 0
        ? Math.min(0.95, pivotResult.resolvedSids / (pivotResult.resolvedSids + pivotResult.unresolvedSids.length))
        : 0,
      needsUserInput: ambiguous.length > 0,
      _rawHeaders: parsed.headers,
      _rawRows: parsed.rows,
    };
  } else if (detection.mappingType === "code-translate") {
    const result = await translateCodeData(parsed, detection.detectedType, onProgress);
    mappings = result.mappings;
    ambiguous = result.ambiguous;
    unmapped = result.unmapped;

    // Code-translate computes its own confidence (accounts for intentionally skipped columns)
    const ctPreview = applyMappingsPreview(parsed.headers,
      (result.dataRows !== parsed.rows ? result.dataRows : parsed.rows).slice(0, 5), mappings);
    return {
      fileId: `file-${Date.now()}`,
      fileName, format: parsed.format.toUpperCase(),
      detectedType: detection.detectedType, targetTable: detection.targetTable,
      mappingType: detection.mappingType,
      mappings, ambiguous, unmapped,
      rowCount: result.dataRows.length,
      columnCount: parsed.headers.length,
      preview: ctPreview, issues, anomalyReport,
      confidence: Math.round(result.confidence * 100) / 100,
      needsUserInput: ambiguous.length > 0 || result.confidence < 0.5,
      _rawHeaders: parsed.headers,
      _rawRows: result.dataRows,
    };
  }

  // Apply auto-fix normalizations to raw data
  if (mappings.length > 0) {
    normalizeRawRows(parsed.headers, parsed.rows, mappings);
  }

  // Check if any mapping targets an identity column
  const identityIssueB = checkIdentityMapping(mappings, parsed.rows.length);
  if (identityIssueB) issues.push(identityIssueB);

  const preview = mappings.length > 0
    ? applyMappingsPreview(parsed.headers, parsed.rows.slice(0, 5), mappings)
    : rawPreview(parsed.headers, parsed.rows.slice(0, 5));
  const mappedRatio = mappings.length / Math.max(parsed.headers.length, 1);
  const avgConfidence = mappings.length > 0
    ? mappings.reduce((sum, m) => sum + m.confidence, 0) / mappings.length
    : 0;
  const confidence = mappedRatio * 0.6 + avgConfidence * 0.3 + detection.confidence * 0.1;

  return {
    fileId: `file-${Date.now()}`,
    fileName,
    format: parsed.format.toUpperCase(),
    detectedType: detection.detectedType,
    targetTable: detection.targetTable,
    mappingType: detection.mappingType,
    mappings, ambiguous, unmapped,
    rowCount: parsed.rows.length,
    preview, issues, anomalyReport,
    confidence: Math.round(confidence * 100) / 100,
    needsUserInput: ambiguous.length > 0 || confidence < 0.5 || !!identityIssueB,
    _rawHeaders: parsed.headers,
    _rawRows: parsed.rows,
  };
}

function rawPreview(headers: string[], rows: string[][]): Record<string, string | null>[] {
  return rows.map((row) => {
    const result: Record<string, string | null> = {};
    for (let i = 0; i < headers.length && i < row.length; i++) {
      result[headers[i]] = row[i] || null;
    }
    return result;
  });
}

function applyMappingsPreview(
  headers: string[],
  rows: string[][],
  mappings: MappingSpec[],
): Record<string, string | null>[] {
  const sourceToMapping = new Map(mappings.map((m) => [m.source, m]));

  return rows.map((row) => {
    const result: Record<string, string | null> = {};

    for (let i = 0; i < headers.length && i < row.length; i++) {
      const mapping = sourceToMapping.get(headers[i]);
      if (!mapping) continue;

      let value: string | null = row[i];
      value = applyTransform(value, mapping.transform);
      result[mapping.target] = value;
    }

    return result;
  });
}

/**
 * Apply mapping transforms to raw rows in-place.
 * This bridges the gap between validators marking issues as autoFix:true
 * and the data actually being normalized. Transforms are idempotent,
 * so re-applying them at import/dump time is safe.
 */
function normalizeRawRows(headers: string[], rows: string[][], mappings: MappingSpec[]): void {
  const sourceToMapping = new Map(mappings.map((m) => [m.source, m]));

  for (let i = 0; i < headers.length; i++) {
    const mapping = sourceToMapping.get(headers[i]);
    if (!mapping || !mapping.transform || mapping.transform === "none") continue;

    for (const row of rows) {
      if (row[i] != null && row[i] !== "") {
        const normalized = applyTransform(row[i], mapping.transform);
        row[i] = normalized ?? "";
      }
    }
  }
}

export function applyTransform(value: string | null, transform?: MappingSpec["transform"]): string | null {
  if (value == null || value.trim() === "") return null;

  switch (transform) {
    case "normalizeCaseId": {
      const id = normalizeCaseId(value);
      return id != null ? String(id) : null;
    }
    case "normalizePatientId": {
      const id = normalizePatientId(value);
      return id != null ? String(id) : null;
    }
    case "normalizeDate":
      return normalizeDate(value);
    case "normalizeNull":
      return normalizeNull(value);
    case "normalizeFlag":
      return normalizeFlag(value);
    case "parseFloat":
      return normalizeNumeric(value);
    case "parseInt": {
      const num = parseInt(value, 10);
      return isNaN(num) ? null : String(num);
    }
    case "normalizeShift":
      return normalizeShift(value);
    case "normalizeFreeText":
      return normalizeFreeText(value);
    default:
      return normalizeNull(value);
  }
}

function emptyResult(fileName: string, error: string): AnalysisResult {
  return {
    fileId: `file-${Date.now()}`,
    fileName,
    format: "UNKNOWN",
    detectedType: "unknown",
    targetTable: "unknown",
    mappingType: "column-rename",
    mappings: [],
    ambiguous: [{
      sourceColumn: "(file)",
      candidates: [],
      question: error,
    }],
    unmapped: [],
    rowCount: 0,
    columnCount: 0,
    preview: [],
    issues: [{ severity: "error", field: "(file)", message: error }],
    confidence: 0,
    needsUserInput: true,
    _rawHeaders: [],
    _rawRows: [],
  };
}
