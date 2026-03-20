import type { ValidationIssue } from "../types.js";
import { connect, execSql, isDbAvailable } from "../db/connection.js";

const IMPORT_TABLES = [
  { name: "tbImportLabsData", idCol: "coCaseId", label: "Labs" },
  { name: "tbImportAcData", idCol: "coCaseId", label: "epaAC Assessments" },
  { name: "tbImportIcd10Data", idCol: "coCaseId", label: "ICD-10/OPS" },
  { name: "tbImportMedicationInpatientData", idCol: "coCaseId", label: "Medications" },
  { name: "tbImportNursingDailyReportsData", idCol: "coCaseId", label: "Nursing Reports" },
  { name: "tbImportDeviceMotionData", idCol: "coCaseId", label: "Device Motion" },
  { name: "tbImportDevice1HzMotionData", idCol: "coCaseId", label: "Device 1Hz" },
];

function rowsToObjects(rows: any[][]): Record<string, any>[] {
  return rows.map((row) => {
    const obj: Record<string, any> = {};
    for (const col of row) {
      obj[col.metadata.colName] = col.value;
    }
    return obj;
  });
}

export async function validateCrossTable(): Promise<ValidationIssue[]> {
  const dbAvail = await isDbAvailable();
  if (!dbAvail) {
    return [{
      severity: "info",
      field: "(database)",
      message: "Cross-table validation skipped — SQL Server not available",
      suggestion: "Set DB_HOST, DB_USER, DB_PASSWORD, DB_NAME to enable cross-table checks",
      category: "completeness",
      origin: "No database connection configured",
      autoFix: false,
    }];
  }

  const issues: ValidationIssue[] = [];
  const conn = await connect();

  // 1. Get all case IDs from tbCaseData
  let caseDataIds: Set<number>;
  try {
    const rawRows = await execSql(conn, "SELECT DISTINCT coE2I222 FROM tbCaseData WHERE coE2I222 IS NOT NULL");
    const rows = rowsToObjects(rawRows);
    caseDataIds = new Set(rows.map((r) => Number(r.coE2I222)));
  } catch {
    caseDataIds = new Set();
    issues.push({
      severity: "warning",
      field: "tbCaseData",
      message: "tbCaseData is empty or inaccessible — orphan detection limited",
      category: "completeness",
      origin: "No case demographic data loaded yet",
      autoFix: false,
    });
  }

  // 2. Check each import table for orphan case IDs
  for (const table of IMPORT_TABLES) {
    try {
      const rawRows = await execSql(
        conn,
        `SELECT DISTINCT ${table.idCol} FROM ${table.name} WHERE ${table.idCol} IS NOT NULL`,
      );
      const rows = rowsToObjects(rawRows);
      const tableIds = new Set(rows.map((r) => Number(r[table.idCol])));

      if (caseDataIds.size > 0) {
        const orphans = Array.from(tableIds).filter((id) => !caseDataIds.has(id));
        if (orphans.length > 0) {
          const examples = orphans.slice(0, 5).join(", ");
          issues.push({
            severity: "warning",
            field: table.name,
            message: `${orphans.length} case IDs in ${table.label} have no matching record in tbCaseData: ${examples}${orphans.length > 5 ? ` ... (+${orphans.length - 5} more)` : ""}`,
            suggestion: "Import the corresponding case demographics data, or verify these cases exist in the source system",
            category: "orphan",
            origin: `${table.label} data was imported without corresponding patient demographics`,
            autoFix: false,
            affectedCount: orphans.length,
          });
        }
      }

      issues.push({
        severity: "info",
        field: table.name,
        message: `${table.label}: ${tableIds.size} unique cases, ${rows.length} total records`,
        category: "completeness",
        origin: "Database content summary",
        autoFix: false,
      });
    } catch {
      // Table doesn't exist or is empty
    }
  }

  // 3. Completeness matrix: which tables have data for each case
  if (caseDataIds.size > 0) {
    const coverageByCase = new Map<number, Set<string>>();
    for (const id of caseDataIds) {
      coverageByCase.set(id, new Set());
    }

    for (const table of IMPORT_TABLES) {
      try {
        const rawRows = await execSql(
          conn,
          `SELECT DISTINCT ${table.idCol} FROM ${table.name} WHERE ${table.idCol} IS NOT NULL`,
        );
        const rows = rowsToObjects(rawRows);
        for (const row of rows) {
          const id = Number(row[table.idCol]);
          coverageByCase.get(id)?.add(table.label);
        }
      } catch {
        // skip
      }
    }

    const totalTables = IMPORT_TABLES.length;
    let sparseCount = 0;
    let emptyCount = 0;

    for (const [, tables] of coverageByCase) {
      if (tables.size === 0) emptyCount++;
      else if (tables.size < totalTables * 0.5) sparseCount++;
    }

    if (emptyCount > 0) {
      issues.push({
        severity: "warning",
        field: "tbCaseData",
        message: `${emptyCount} cases in tbCaseData have no data in any import table`,
        suggestion: "These cases exist in demographics but have no clinical data. Import the corresponding data files.",
        category: "completeness",
        origin: "Case demographics imported without corresponding clinical data",
        autoFix: false,
        affectedCount: emptyCount,
      });
    }

    if (sparseCount > 0) {
      issues.push({
        severity: "info",
        field: "(cross-table)",
        message: `${sparseCount} cases have data in fewer than half of the import tables`,
        suggestion: "Some cases may have incomplete clinical records. This is normal if not all data sources cover all patients.",
        category: "completeness",
        origin: "Not all data sources cover all patients — expected for multi-clinic setups",
        autoFix: false,
        affectedCount: sparseCount,
      });
    }
  }

  return issues;
}
