import type { ValidationIssue, ParsedFile } from "../types.js";
import { isNullEquivalent } from "../normalize.js";

// Clinical plausible ranges (very broad — flags values that are clearly impossible)
const LAB_RANGES: Record<string, { min: number; max: number; unit: string }> = {
  sodium: { min: 100, max: 200, unit: "mmol/L" },
  potassium: { min: 1, max: 15, unit: "mmol/L" },
  creatinine: { min: 0, max: 30, unit: "mg/dL" },
  egfr: { min: 0, max: 200, unit: "mL/min/1.73m2" },
  glucose: { min: 10, max: 1000, unit: "mg/dL" },
  hemoglobin: { min: 2, max: 25, unit: "g/dL" },
  hb: { min: 2, max: 25, unit: "g/dL" },
  wbc: { min: 0, max: 200, unit: "10e9/L" },
  platelets: { min: 0, max: 1500, unit: "10e9/L" },
  crp: { min: 0, max: 500, unit: "mg/L" },
  alt: { min: 0, max: 10000, unit: "U/L" },
  ast: { min: 0, max: 10000, unit: "U/L" },
  bilirubin: { min: 0, max: 50, unit: "mg/dL" },
  albumin: { min: 0, max: 10, unit: "g/dL" },
  inr: { min: 0, max: 20, unit: "" },
  lactate: { min: 0, max: 30, unit: "mmol/L" },
};

const VALID_FLAGS = new Set(["", "H", "L", "N", "NORMAL"]);

export function validateLabs(
  parsed: ParsedFile,
  detectedType: string,
): ValidationIssue[] {
  if (detectedType !== "labs") return [];

  const issues: ValidationIssue[] = [];
  const { headers, rows } = parsed;

  // Find lab value columns and their flag columns
  for (const [labName, range] of Object.entries(LAB_RANGES)) {
    const valueIdx = headers.findIndex((h) =>
      h.toLowerCase().includes(labName) &&
      !h.toLowerCase().includes("flag") &&
      !h.toLowerCase().includes("ref"),
    );
    const flagIdx = headers.findIndex((h) =>
      h.toLowerCase().includes(labName.split("_")[0]) &&
      h.toLowerCase().includes("flag"),
    );
    // Also check for short aliases: Na, K, etc.
    const shortAliases: Record<string, string> = {
      sodium: "na", potassium: "k", creatinine: "crea", hemoglobin: "hb",
      glucose: "glu", bilirubin: "bili", albumin: "alb",
    };
    const alias = shortAliases[labName];
    const aliasValueIdx = alias
      ? headers.findIndex((h) => h.toLowerCase() === alias || h.toLowerCase() === `${alias}_value`)
      : -1;
    const effectiveValueIdx = valueIdx >= 0 ? valueIdx : aliasValueIdx;

    if (effectiveValueIdx < 0) continue;

    let outOfRangeCount = 0;
    let flagMismatchCount = 0;
    let nonStdFlagCount = 0;

    for (let i = 0; i < rows.length; i++) {
      const rawVal = rows[i][effectiveValueIdx];
      if (!rawVal || isNullEquivalent(rawVal)) continue;

      // Parse numeric value (strip garbage)
      const cleaned = rawVal.trim().replace(/[ßäöü@#$%^]+/g, "").replace(",", ".");
      const numVal = parseFloat(cleaned);

      if (!isNaN(numVal)) {
        // Out of plausible range
        if (numVal < range.min || numVal > range.max) {
          outOfRangeCount++;
          if (outOfRangeCount <= 3) {
            issues.push({
              severity: "error",
              field: headers[effectiveValueIdx],
              row: i + 2,
              value: rawVal,
              message: `${labName} value ${numVal} outside plausible range [${range.min}–${range.max}] ${range.unit}`,
              suggestion: "Verify with lab system. This may indicate a unit mismatch or transcription error.",
              category: "out-of-range",
              origin: "Lab value outside clinically plausible range — possible unit error, decimal point shift, or data entry mistake",
              autoFix: false,
            });
          }
        }

        // Flag consistency check
        if (flagIdx >= 0) {
          const flag = (rows[i][flagIdx] || "").trim().toUpperCase();
          if (!isNullEquivalent(rows[i][flagIdx]) && flag !== "") {
            // Check non-standard flags
            if (!VALID_FLAGS.has(flag)) {
              nonStdFlagCount++;
              if (nonStdFlagCount <= 3) {
                issues.push({
                  severity: "warning",
                  field: headers[flagIdx],
                  row: i + 2,
                  value: rows[i][flagIdx],
                  message: `Non-standard lab flag: "${rows[i][flagIdx]}" (expected H, L, or empty)`,
                  suggestion: "Non-standard flags will be normalized (e.g. HH→H, LL→L, HHÜ→H, LLÜ→L, HIGH→H, LOW→L, NORMAL→null)",
                  category: "flag-drift",
                  origin: "Source lab system uses a different flag vocabulary (e.g., HH for critically high)",
                  autoFix: true,
                });
              }
            }
          }
        }
      }
    }

    if (outOfRangeCount > 3) {
      issues.push({
        severity: "error",
        field: headers[effectiveValueIdx],
        message: `${outOfRangeCount} ${labName} values outside plausible range (showing first 3)`,
        category: "out-of-range",
        origin: "Systematic unit mismatch or data quality issue in lab interface",
        autoFix: false,
        affectedCount: outOfRangeCount,
      });
    }

    if (nonStdFlagCount > 3) {
      issues.push({
        severity: "warning",
        field: flagIdx >= 0 ? headers[flagIdx] : `${labName}_flag`,
        message: `${nonStdFlagCount} non-standard lab flags (showing first 3)`,
        category: "flag-drift",
        origin: "Source lab system uses extended flag vocabulary",
        autoFix: true,
        affectedCount: nonStdFlagCount,
      });
    }
  }

  return issues;
}
