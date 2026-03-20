import type { ValidationIssue, ParsedFile } from "../types.js";
import { isNullEquivalent } from "../normalize.js";

const NUMERIC_COLUMN_PATTERNS = /mmol|mg|_dL|_L|_g|index|count|minutes|magnitude|score|dose|age|length_of_stay|pressure_zone|accel/i;
const GARBAGE_CHARS = /[ßäöü@#$%^]+/;

// Fields where negative values are clinically impossible
const NO_NEGATIVE_FIELDS = /sodium|potassium|creatinine|egfr|glucose|hemoglobin|hb|wbc|platelets|crp|alt|ast|bilirubin|albumin|inr|lactate|age|dose|movement|count|minutes|magnitude|pressure/i;

export function validateNumeric(
  parsed: ParsedFile,
  _detectedType: string,
): ValidationIssue[] {
  const issues: ValidationIssue[] = [];
  const { headers, rows } = parsed;

  // Find numeric-looking columns
  const numericCols = headers
    .map((h, i) => ({ header: h, index: i }))
    .filter((c) => NUMERIC_COLUMN_PATTERNS.test(c.header));

  for (const col of numericCols) {
    let trailingGarbageCount = 0;
    let negativeCount = 0;
    let germanCommaCount = 0;
    let percentCount = 0;

    for (let i = 0; i < rows.length; i++) {
      const val = rows[i][col.index];
      if (!val || isNullEquivalent(val)) continue;
      const trimmed = val.trim();

      // Trailing garbage characters
      if (GARBAGE_CHARS.test(trimmed) && /\d/.test(trimmed)) {
        trailingGarbageCount++;
        if (trailingGarbageCount <= 3) {
          issues.push({
            severity: "warning",
            field: col.header,
            row: i + 2,
            value: val,
            message: `Numeric value contains special characters: "${val}"`,
            suggestion: "Special characters will be stripped during normalization",
            category: "encoding",
            origin: "Character encoding corruption during data export or file transfer",
            autoFix: true,
          });
        }
      }

      // Percentage signs
      if (/%/.test(trimmed) && /\d/.test(trimmed)) {
        percentCount++;
        if (percentCount <= 2) {
          issues.push({
            severity: "warning",
            field: col.header,
            row: i + 2,
            value: val,
            message: `Numeric value contains percentage sign: "${val}"`,
            suggestion: "Percentage sign will be stripped. Verify the value is a raw number, not a percentage.",
            category: "encoding",
            origin: "Source system formatted the value as a percentage string",
            autoFix: true,
          });
        }
      }

      // German decimal comma
      if (/^\d+,\d+$/.test(trimmed)) {
        germanCommaCount++;
      }

      // Negative values where impossible
      const numVal = parseFloat(trimmed.replace(GARBAGE_CHARS, "").replace(",", "."));
      if (!isNaN(numVal) && numVal < 0 && NO_NEGATIVE_FIELDS.test(col.header)) {
        negativeCount++;
        if (negativeCount <= 3) {
          issues.push({
            severity: "error",
            field: col.header,
            row: i + 2,
            value: val,
            message: `Clinically impossible negative value: ${numVal}`,
            suggestion: "Verify with source system. This may indicate a data entry error or sign inversion.",
            category: "out-of-range",
            origin: "Data entry error, calculation error, or sign corruption in source system",
            autoFix: false,
          });
        }
      }
    }

    if (trailingGarbageCount > 3) {
      issues.push({
        severity: "warning",
        field: col.header,
        message: `${trailingGarbageCount} numeric values with special characters (showing first 3)`,
        category: "encoding",
        origin: "Systematic character encoding corruption in this column",
        autoFix: true,
        affectedCount: trailingGarbageCount,
      });
    }

    if (negativeCount > 3) {
      issues.push({
        severity: "error",
        field: col.header,
        message: `${negativeCount} clinically impossible negative values (showing first 3)`,
        category: "out-of-range",
        origin: "Systematic sign error in source system",
        autoFix: false,
        affectedCount: negativeCount,
      });
    }

    if (percentCount > 2) {
      issues.push({
        severity: "warning",
        field: col.header,
        message: `${percentCount} values with percentage signs (showing first 2)`,
        category: "encoding",
        origin: "Source system formats numbers as percentage strings",
        autoFix: true,
        affectedCount: percentCount,
      });
    }

    if (germanCommaCount > 0) {
      issues.push({
        severity: "info",
        field: col.header,
        message: `${germanCommaCount} values use German decimal comma (e.g., "1,5" instead of "1.5")`,
        suggestion: "Commas will be converted to periods automatically",
        category: "source-format",
        origin: "Source system uses German/European number formatting (locale de-DE)",
        autoFix: true,
        affectedCount: germanCommaCount,
      });
    }
  }

  return issues;
}
