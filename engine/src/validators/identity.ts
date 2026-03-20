import type { ValidationIssue, ParsedFile } from "../types.js";
import { isNullEquivalent } from "../normalize.js";

const CASE_ID_PATTERNS = /^(case_id|caseid|fallid|fall_id)$/i;
const PATIENT_ID_PATTERNS = /^(patient_id|patientid|pid|pat_id)$/i;

interface IdFormatInfo {
  format: string;
  example: string;
  count: number;
}

function classifyIdFormat(value: string): string {
  if (/^CASE-\d+-\d+$/.test(value)) return "CASE-XXXX-XX (suffixed)";
  if (/^CASE-\d+$/.test(value)) return "CASE-XXXX";
  if (/^PAT-\d+$/i.test(value)) return "PAT-XXXX";
  if (/^PAT_\d+$/i.test(value)) return "PAT_XXXX (underscore)";
  if (/^patientnr\d+/i.test(value)) return "patientnrXXXX (legacy)";
  if (/^\d+$/.test(value)) return "numeric only";
  if (/[ßäöü@#$%^]/.test(value)) return "contains special characters";
  return "other";
}

export function validateIdentity(
  parsed: ParsedFile,
  _detectedType: string,
): ValidationIssue[] {
  const issues: ValidationIssue[] = [];
  const { headers, rows } = parsed;

  const caseIdIdx = headers.findIndex((h) => CASE_ID_PATTERNS.test(h));
  const patientIdIdx = headers.findIndex((h) => PATIENT_ID_PATTERNS.test(h));

  // Missing required IDs
  if (caseIdIdx >= 0) {
    let missingCount = 0;
    const formatCounts = new Map<string, IdFormatInfo>();
    let specialCharCount = 0;

    for (let i = 0; i < rows.length; i++) {
      const val = rows[i][caseIdIdx];

      if (isNullEquivalent(val)) {
        missingCount++;
        continue;
      }

      // Classify format
      const fmt = classifyIdFormat(val);
      const existing = formatCounts.get(fmt);
      if (existing) {
        existing.count++;
      } else {
        formatCounts.set(fmt, { format: fmt, example: val, count: 1 });
      }

      // Special characters in ID
      if (/[ßäöü@#$%^]/.test(val)) {
        specialCharCount++;
        if (specialCharCount <= 3) {
          issues.push({
            severity: "warning",
            field: headers[caseIdIdx],
            row: i + 2,
            value: val,
            message: `Case ID contains special characters: "${val}"`,
            suggestion: "Special characters will be stripped during normalization",
            category: "encoding",
            origin: "Character encoding corruption during export or transfer",
            autoFix: true,
          });
        }
      }
    }

    if (missingCount > 0) {
      issues.push({
        severity: "error",
        field: headers[caseIdIdx],
        message: `${missingCount} rows missing required case_id`,
        suggestion: "These rows will be skipped during import. Verify with source system.",
        category: "id-format",
        origin: "Source system did not populate case_id for these records",
        autoFix: false,
        affectedCount: missingCount,
      });
    }

    if (specialCharCount > 3) {
      issues.push({
        severity: "warning",
        field: headers[caseIdIdx],
        message: `${specialCharCount} case_id values contain special characters (showing first 3)`,
        category: "encoding",
        origin: "Character encoding corruption during export or transfer",
        autoFix: true,
        affectedCount: specialCharCount,
      });
    }

    // Report format variations
    if (formatCounts.size > 1) {
      const formats = Array.from(formatCounts.values())
        .sort((a, b) => b.count - a.count)
        .map((f) => `${f.format} (${f.count}x, e.g. "${f.example}")`)
        .join(", ");

      issues.push({
        severity: "info",
        field: headers[caseIdIdx],
        message: `Multiple case_id formats detected: ${formats}`,
        suggestion: "All formats normalize to integer IDs automatically",
        category: "id-format",
        origin: "Data comes from multiple source systems with different ID conventions",
        autoFix: true,
        affectedCount: rows.length,
      });
    }
  }

  if (patientIdIdx >= 0) {
    let missingCount = 0;
    const formatCounts = new Map<string, IdFormatInfo>();
    let specialCharCount = 0;

    for (let i = 0; i < rows.length; i++) {
      const val = rows[i][patientIdIdx];

      if (isNullEquivalent(val)) {
        missingCount++;
        continue;
      }

      const fmt = classifyIdFormat(val);
      const existing = formatCounts.get(fmt);
      if (existing) {
        existing.count++;
      } else {
        formatCounts.set(fmt, { format: fmt, example: val, count: 1 });
      }

      if (/[ßäöü@#$%^]/.test(val)) {
        specialCharCount++;
        if (specialCharCount <= 3) {
          issues.push({
            severity: "warning",
            field: headers[patientIdIdx],
            row: i + 2,
            value: val,
            message: `Patient ID contains special characters: "${val}"`,
            suggestion: "Special characters will be stripped during normalization",
            category: "encoding",
            origin: "Character encoding corruption during export or transfer",
            autoFix: true,
          });
        }
      }
    }

    if (missingCount > 0) {
      issues.push({
        severity: "error",
        field: headers[patientIdIdx],
        message: `${missingCount} rows missing required patient_id`,
        suggestion: "These rows will be skipped during import. Verify with source system.",
        category: "id-format",
        origin: "Source system did not populate patient_id for these records",
        autoFix: false,
        affectedCount: missingCount,
      });
    }

    if (specialCharCount > 3) {
      issues.push({
        severity: "warning",
        field: headers[patientIdIdx],
        message: `${specialCharCount} patient_id values contain special characters (showing first 3)`,
        category: "encoding",
        origin: "Character encoding corruption during export or transfer",
        autoFix: true,
        affectedCount: specialCharCount,
      });
    }

    if (formatCounts.size > 1) {
      const formats = Array.from(formatCounts.values())
        .sort((a, b) => b.count - a.count)
        .map((f) => `${f.format} (${f.count}x, e.g. "${f.example}")`)
        .join(", ");

      issues.push({
        severity: "info",
        field: headers[patientIdIdx],
        message: `Multiple patient_id formats detected: ${formats}`,
        suggestion: "All formats normalize to integer IDs automatically",
        category: "id-format",
        origin: "Data comes from multiple source systems with different ID conventions",
        autoFix: true,
        affectedCount: rows.length,
      });
    }
  }

  return issues;
}
