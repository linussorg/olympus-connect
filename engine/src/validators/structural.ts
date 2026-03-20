import type { ValidationIssue, ParsedFile } from "../types.js";
import { getSchema } from "../schema.js";

const DETECTION_TO_TABLE: Record<string, string> = {
  "epaAC-Data-1": "tbImportAcData",
  "epaAC-Data-2": "tbImportAcData",
  "epaAC-Data-3": "tbImportAcData",
  "epaAC-Data-5": "tbImportAcData",
  labs: "tbImportLabsData",
  "icd10-ops": "tbImportIcd10Data",
  medication: "tbImportMedicationInpatientData",
  nursing: "tbImportNursingDailyReportsData",
  "device-motion": "tbImportDeviceMotionData",
  "device-1hz": "tbImportDevice1HzMotionData",
};

export function validateStructural(
  parsed: ParsedFile,
  detectedType: string,
): ValidationIssue[] {
  const issues: ValidationIssue[] = [];
  const { headers } = parsed;

  // Detect abbreviated / non-standard column names
  const tableName = DETECTION_TO_TABLE[detectedType];
  const schema = tableName ? getSchema(tableName) : undefined;

  if (schema) {
    const schemaColNames = new Set(schema.columns.map((c) => c.name.toLowerCase()));
    const matchingCols = headers.filter((h) => schemaColNames.has(h.toLowerCase()));

    if (matchingCols.length === 0 && headers.length > 0) {
      // Headers don't match schema at all — likely abbreviated or renamed
      const abbrevPatterns = headers.filter((h) => h.length <= 4 || /^[A-Z][a-z]$/.test(h));
      const origin =
        abbrevPatterns.length > headers.length * 0.5
          ? "Source system exports abbreviated column names (likely SAP, legacy EHR, or custom extract)"
          : "Column names differ from target schema — source uses a different naming convention";

      issues.push({
        severity: "info",
        field: "(headers)",
        message: `Column names don't match target schema ${schema.name} — mapping engine will translate`,
        suggestion: "Column mapping handles this automatically via heuristic or LLM matching",
        category: "source-format",
        origin,
        autoFix: true,
        affectedCount: headers.length,
      });
    }

    // Column count mismatch
    const targetColCount = schema.columns.length;
    if (headers.length > targetColCount * 1.5) {
      issues.push({
        severity: "warning",
        field: "(headers)",
        message: `Source has ${headers.length} columns but target ${schema.name} has ${targetColCount} — ${headers.length - targetColCount} columns will be unmapped`,
        suggestion: "Unmapped columns are ignored during import. Verify no important data is lost.",
        category: "source-format",
        origin: "Source file includes extra columns not in the target schema",
        autoFix: false,
      });
    }
  }

  // Detect encrypted/base64 headers
  const base64Headers = headers.filter((h) => {
    const b64Chars = (h.match(/[+/=]/g) || []).length;
    return b64Chars >= 2 && h.length > 8;
  });
  if (base64Headers.length > headers.length * 0.3) {
    issues.push({
      severity: "warning",
      field: "(headers)",
      message: `${base64Headers.length} of ${headers.length} headers appear to be base64-encoded or encrypted`,
      suggestion: "These headers need decryption before mapping. This is typical of epaAC Data-5 exports.",
      category: "source-format",
      origin: "Source system encrypts column headers for data protection (epaAC Data-5 format)",
      autoFix: false,
      affectedCount: base64Headers.length,
    });
  }

  // Detect delimiter issues within CSV (tab chars in values suggest misparse)
  let tabInValues = 0;
  for (let i = 0; i < Math.min(parsed.rows.length, 50); i++) {
    for (const val of parsed.rows[i]) {
      if (val && val.includes("\t")) tabInValues++;
    }
  }
  if (tabInValues > 0) {
    issues.push({
      severity: "warning",
      field: "(delimiter)",
      message: `${tabInValues} values contain tab characters — possible delimiter mismatch`,
      suggestion: "The file may use tabs as delimiters. Re-parse with correct delimiter.",
      category: "source-format",
      origin: "Source system uses a different delimiter than expected",
      autoFix: false,
      affectedCount: tabInValues,
    });
  }

  return issues;
}
