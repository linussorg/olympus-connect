import type { ValidationIssue, ParsedFile } from "./types.js";
import { isNullEquivalent } from "./normalize.js";

export function validate(parsed: ParsedFile): ValidationIssue[] {
  const { headers, rows } = parsed;
  const issues: ValidationIssue[] = [];

  // Find case_id and patient_id columns
  const caseIdIdx = headers.findIndex((h) =>
    /^(case_id|caseid|fallid|fall_id)$/i.test(h),
  );
  const patientIdIdx = headers.findIndex((h) =>
    /^(patient_id|patientid|pid|pat_id)$/i.test(h),
  );

  let missingCaseId = 0;
  let missingPatientId = 0;

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];

    // Required field checks
    if (caseIdIdx >= 0 && isNullEquivalent(row[caseIdIdx])) {
      missingCaseId++;
      if (missingCaseId <= 5) {
        issues.push({
          severity: "error",
          field: headers[caseIdIdx],
          row: i + 2,
          value: row[caseIdIdx] || "(empty)",
          message: "Required field case_id is missing",
          suggestion: "Row will be skipped during import",
        });
      }
    }

    if (patientIdIdx >= 0 && isNullEquivalent(row[patientIdIdx])) {
      missingPatientId++;
      if (missingPatientId <= 5) {
        issues.push({
          severity: "error",
          field: headers[patientIdIdx],
          row: i + 2,
          value: row[patientIdIdx] || "(empty)",
          message: "Required field patient_id is missing",
          suggestion: "Row will be skipped during import",
        });
      }
    }
  }

  // Summary issues for truncated individual errors
  if (missingCaseId > 5) {
    issues.push({
      severity: "error",
      field: headers[caseIdIdx] || "case_id",
      message: `${missingCaseId} rows total missing case_id (showing first 5)`,
    });
  }
  if (missingPatientId > 5) {
    issues.push({
      severity: "error",
      field: headers[patientIdIdx] || "patient_id",
      message: `${missingPatientId} rows total missing patient_id (showing first 5)`,
    });
  }

  // Check for NULL equivalents in data
  const nullCounts = new Map<string, number>();
  for (const row of rows) {
    for (let ci = 0; ci < row.length && ci < headers.length; ci++) {
      const val = row[ci];
      if (val && isNullEquivalent(val) && val.trim() !== "") {
        nullCounts.set(headers[ci], (nullCounts.get(headers[ci]) || 0) + 1);
      }
    }
  }
  for (const [field, count] of nullCounts) {
    issues.push({
      severity: "info",
      field,
      message: `${count} NULL-equivalent values (e.g., "Missing", "N/A") will be normalized to NULL`,
    });
  }

  // Check for garbage characters in values
  let garbageCount = 0;
  for (let i = 0; i < Math.min(rows.length, 100); i++) {
    for (let ci = 0; ci < rows[i].length && ci < headers.length; ci++) {
      const val = rows[i][ci];
      if (val && /[ßäöü@#$%^]+/.test(val) && !/[a-zA-Z]{3,}/.test(val)) {
        garbageCount++;
        if (garbageCount <= 3) {
          issues.push({
            severity: "warning",
            field: headers[ci],
            row: i + 2,
            value: val,
            message: "Value contains unexpected special characters",
            suggestion: "Characters will be stripped during normalization",
          });
        }
      }
    }
  }
  if (garbageCount > 3) {
    issues.push({
      severity: "warning",
      field: "(multiple)",
      message: `${garbageCount} values with garbage characters found (showing first 3)`,
    });
  }

  return issues;
}
