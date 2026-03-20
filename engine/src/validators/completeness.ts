import type { ValidationIssue, ParsedFile } from "../types.js";
import { isNullEquivalent } from "../normalize.js";

// Data types where multiple rows per case_id are expected (time-series)
const TIME_SERIES_TYPES = new Set([
  "labs", "medication", "nursing", "device-motion", "device-1hz", "icd10",
]);

export function validateCompleteness(
  parsed: ParsedFile,
  _detectedType: string,
): ValidationIssue[] {
  const issues: ValidationIssue[] = [];
  const { headers, rows } = parsed;

  if (rows.length === 0) return issues;

  // Per-column fill rate
  const fillCounts = new Array(headers.length).fill(0);
  const nullEquivCounts = new Array(headers.length).fill(0);

  for (const row of rows) {
    for (let ci = 0; ci < headers.length && ci < row.length; ci++) {
      const val = row[ci];
      if (val && val.trim() !== "") {
        if (isNullEquivalent(val)) {
          nullEquivCounts[ci]++;
        } else {
          fillCounts[ci]++;
        }
      }
    }
  }

  // Report columns with low fill rate
  for (let ci = 0; ci < headers.length; ci++) {
    const fillRate = fillCounts[ci] / rows.length;
    const nullEquivRate = nullEquivCounts[ci] / rows.length;

    if (fillRate === 0 && nullEquivCounts[ci] === 0) {
      issues.push({
        severity: "warning",
        field: headers[ci],
        message: `Column is entirely empty (0% fill rate across ${rows.length} rows)`,
        suggestion: "This column has no data. Verify if it's expected or if data was lost during export.",
        category: "completeness",
        origin: "Source system did not populate this field, or it was lost during file export",
        autoFix: false,
        affectedCount: rows.length,
      });
    } else if (fillRate < 0.1 && fillRate > 0) {
      issues.push({
        severity: "info",
        field: headers[ci],
        message: `Very low fill rate: ${(fillRate * 100).toFixed(1)}% (${fillCounts[ci]}/${rows.length} rows)`,
        suggestion: "Most values in this column are missing. Check if this is expected for your data source.",
        category: "completeness",
        origin: "Field is sparsely populated — may be optional in the source system",
        autoFix: false,
        affectedCount: rows.length - fillCounts[ci],
      });
    }

    // Report NULL-equivalent values
    if (nullEquivCounts[ci] > 0) {
      issues.push({
        severity: "info",
        field: headers[ci],
        message: `${nullEquivCounts[ci]} NULL-equivalent values (e.g., "Missing", "N/A") will be normalized to NULL`,
        suggestion: "These values are treated as empty during import",
        category: "null-variant",
        origin: "Source system uses text placeholders instead of empty fields for missing data",
        autoFix: true,
        affectedCount: nullEquivCounts[ci],
      });
    }
  }

  // Detect duplicate rows (by case_id if present)
  // Skip for time-series data types where multiple rows per case_id are expected
  if (!TIME_SERIES_TYPES.has(_detectedType)) {
    const caseIdIdx = headers.findIndex((h) =>
      /^(case_id|caseid|fallid|fall_id)$/i.test(h),
    );
    if (caseIdIdx >= 0) {
      const caseIdCounts = new Map<string, number>();
      for (const row of rows) {
        const cid = row[caseIdIdx]?.trim();
        if (cid && !isNullEquivalent(cid)) {
          caseIdCounts.set(cid, (caseIdCounts.get(cid) || 0) + 1);
        }
      }

      const duplicates = Array.from(caseIdCounts.entries()).filter(([, count]) => count > 1);
      if (duplicates.length > 0) {
        const totalDuplicateRows = duplicates.reduce((sum, [, count]) => sum + count - 1, 0);
        const examples = duplicates.slice(0, 3).map(([id, count]) => `${id} (${count}x)`).join(", ");

        issues.push({
          severity: "warning",
          field: headers[caseIdIdx],
          message: `${duplicates.length} case_ids appear multiple times: ${examples}${duplicates.length > 3 ? `, ... (+${duplicates.length - 3} more)` : ""}`,
          category: "duplicate",
          origin: "Multiple records per case — likely a data export error (this source type expects one row per case)",
          autoFix: false,
          affectedCount: totalDuplicateRows,
        });
      }
    }
  }

  return issues;
}
