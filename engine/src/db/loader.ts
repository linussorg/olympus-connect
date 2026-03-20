import { Connection, Request, TYPES } from "tedious";
import { connectNew, closeConnection, execSql } from "./connection.js";
import { getSchema } from "../schema.js";
import { pivotEpaACData1 } from "../mapping/pivot.js";
import type { AnalysisResult, ImportResult, MappingSpec } from "../types.js";
import { applyTransform } from "../mapping/engine.js";

// ─── Direct SQL Server Insert ───

// Case ID column name per table (tbCaseData uses coE2I222, all others use coCaseId)
function getCaseIdColumn(targetTable: string): string {
  return targetTable === "tbCaseData" ? "coE2I222" : "coCaseId";
}

export type ImportProgressCallback = (inserted: number, skipped: number, total: number) => void;

export async function importToDb(analysis: AnalysisResult, onProgress?: ImportProgressCallback): Promise<ImportResult> {
  const { targetTable, mappings, _rawHeaders, _rawRows, mappingType } = analysis;

  const schema = getSchema(targetTable);
  if (!schema) {
    return { targetTable, inserted: 0, skipped: 0, deduplicated: 0, errors: [`Unknown target table: ${targetTable}`] };
  }

  const conn = await connectNew();

  // Build mapped rows
  let mappedRows: Record<string, string | null>[];
  if (mappingType === "pivot") {
    const { pivotEpaACData1 } = await import("../mapping/pivot.js");
    const pivotResult = pivotEpaACData1({ headers: _rawHeaders, rows: _rawRows, delimiter: ";", format: "csv" });
    mappedRows = pivotResult.pivotedRows;
  } else {
    mappedRows = applyAllMappings(_rawHeaders, _rawRows, mappings);
  }

  // Deduplicate: collect case IDs and delete existing rows before re-inserting
  const caseIdCol = getCaseIdColumn(targetTable);
  const caseIds = new Set<number>();
  for (const row of mappedRows) {
    const val = row[caseIdCol];
    if (val != null) {
      const num = parseInt(val, 10);
      if (!isNaN(num)) caseIds.add(num);
    }
  }

  let deduplicated = 0;
  if (caseIds.size > 0) {
    try {
      const idList = [...caseIds].join(",");
      // Count existing rows before deleting
      const countResult = await execSql(conn, `SELECT COUNT(*) FROM ${targetTable} WHERE ${caseIdCol} IN (${idList})`);
      deduplicated = countResult[0]?.[0]?.value as number || 0;
      if (deduplicated > 0) {
        await execSql(conn, `DELETE FROM ${targetTable} WHERE ${caseIdCol} IN (${idList})`);
        console.log(`[dedup] Deleted ${deduplicated} existing rows from ${targetTable} for ${caseIds.size} case IDs`);
      }
    } catch (err: any) {
      console.error(`[dedup] Warning: could not deduplicate: ${err.message}`);
      // Non-fatal — proceed with insert anyway
    }
  }

  let inserted = 0;
  let skipped = 0;
  const errors: string[] = [];

  if (onProgress) onProgress(0, 0, mappedRows.length);

  for (let i = 0; i < mappedRows.length; i++) {
    const row = mappedRows[i];

    if (!row["coCaseId"] && !row["coPatientId"] && !row["coPatient_id"] && !row["coE2I222"]) {
      skipped++;
      continue;
    }

    try {
      await insertRow(conn, targetTable, row, schema.columns);
      inserted++;
    } catch (err: any) {
      skipped++;
      if (errors.length < 5) {
        errors.push(`Row ${i + 1}: ${err.message}`);
      }
    }

    if (onProgress && (inserted + skipped) % 50 === 0) {
      onProgress(inserted, skipped, mappedRows.length);
    }
  }

  if (errors.length > 0 && skipped > 5) {
    errors.push(`... and ${skipped - 5} more`);
  }

  await closeConnection(conn);
  return { targetTable, inserted, skipped, deduplicated, errors };
}

function insertRow(
  conn: Connection,
  tableName: string,
  row: Record<string, string | null>,
  schemaCols: { name: string; type: string; nullable: boolean }[],
): Promise<void> {
  return new Promise((resolve, reject) => {
    // Filter to columns that have values and exist in schema
    const validCols = new Set(schemaCols.map((c) => c.name));
    const entries = Object.entries(row).filter(
      ([col, val]) => validCols.has(col) && val != null,
    );

    if (entries.length === 0) {
      resolve();
      return;
    }

    const colNames = entries.map(([col]) => col).join(", ");
    const paramPlaceholders = entries.map((_, i) => `@p${i}`).join(", ");
    const sql = `INSERT INTO ${tableName} (${colNames}) VALUES (${paramPlaceholders})`;

    const request = new Request(sql, (err) => {
      if (err) reject(err);
      else resolve();
    });

    // Add parameters with appropriate types
    const colTypeMap = new Map(schemaCols.map((c) => [c.name, c.type]));
    entries.forEach(([col, val], i) => {
      const colType = colTypeMap.get(col) || "nvarchar";
      const tediousType = getTediousType(colType);
      const convertedVal = convertValue(val, colType);
      request.addParameter(`p${i}`, tediousType, convertedVal);
    });

    conn.execSql(request);
  });
}

function getTediousType(sqlType: string) {
  switch (sqlType) {
    case "bigint": return TYPES.BigInt;
    case "smallint": return TYPES.SmallInt;
    case "datetime": return TYPES.DateTime;
    case "numeric": return TYPES.Numeric;
    default: return TYPES.NVarChar;
  }
}

function convertValue(val: string | null, sqlType: string): any {
  if (val == null) return null;

  switch (sqlType) {
    case "bigint": {
      const num = parseInt(val, 10);
      return isNaN(num) ? null : num;
    }
    case "smallint": {
      const num = parseInt(val, 10);
      return isNaN(num) ? null : num;
    }
    case "datetime": {
      const d = new Date(val);
      return isNaN(d.getTime()) ? null : d;
    }
    case "numeric": {
      const num = parseFloat(val);
      return isNaN(num) ? null : num;
    }
    default:
      return val.length > 253 ? val.slice(0, 253) + "..." : val;
  }
}

function applyAllMappings(
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

      const value = applyTransform(row[i], mapping.transform);
      result[mapping.target] = value;
    }

    return result;
  });
}

// ─── SQL Dump ───

export function generateSqlDump(analysis: AnalysisResult): string {
  const { targetTable, mappings, _rawHeaders, _rawRows, mappingType } = analysis;

  const schema = getSchema(targetTable);
  if (!schema) return `-- Error: Unknown target table ${targetTable}\n`;

  let mappedRows: Record<string, string | null>[];
  if (mappingType === "pivot") {
    const pivotResult = pivotEpaACData1({
      headers: _rawHeaders, rows: _rawRows, delimiter: ";", format: "csv",
    });
    mappedRows = pivotResult.pivotedRows;
  } else {
    mappedRows = applyAllMappings(_rawHeaders, _rawRows, mappings);
  }

  const validCols = new Set(schema.columns.map((c) => c.name));
  const colTypeMap = new Map(schema.columns.map((c) => [c.name, c.type]));

  const lines: string[] = [];
  lines.push(`-- Generated by epaCC Mapping Engine`);
  lines.push(`-- Source: ${analysis.fileName}`);
  lines.push(`-- Target: ${targetTable}`);
  lines.push(`-- Rows: ${mappedRows.length}`);
  lines.push(`-- Generated at: ${new Date().toISOString()}`);
  lines.push(`USE Hack2026;`);
  lines.push(`GO`);
  lines.push(``);

  let rowCount = 0;
  for (const row of mappedRows) {
    const entries = Object.entries(row).filter(
      ([col, val]) => validCols.has(col) && val != null,
    );

    if (entries.length === 0) continue;

    const colNames = entries.map(([col]) => col).join(", ");
    const values = entries.map(([col, val]) => {
      const colType = colTypeMap.get(col) || "nvarchar";
      return formatSqlValue(val, colType);
    }).join(", ");

    lines.push(`INSERT INTO ${targetTable} (${colNames}) VALUES (${values});`);
    rowCount++;
  }

  lines.push(``);
  lines.push(`-- ${rowCount} rows`);
  return lines.join("\n");
}

function formatSqlValue(val: string | null, sqlType: string): string {
  if (val == null) return "NULL";

  switch (sqlType) {
    case "bigint":
    case "smallint":
    case "numeric": {
      const num = parseFloat(val);
      return isNaN(num) ? "NULL" : String(num);
    }
    case "datetime": {
      const escaped = val.replace(/'/g, "''");
      return `'${escaped}'`;
    }
    default: {
      const escaped = val.replace(/'/g, "''");
      return `N'${escaped}'`;
    }
  }
}
