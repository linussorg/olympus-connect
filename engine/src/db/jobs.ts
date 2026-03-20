import { connectNew, execSql, closeConnection } from "./connection.js";
import type { JobStatus } from "../types.js";
import { Connection, Request, TYPES } from "tedious";

export interface JobRow {
  jobId: string;
  status: JobStatus;
  fileName: string | null;
  fileSize: number | null;
  detectedType: string | null;
  targetTable: string | null;
  mappingType: string | null;
  rowCount: number | null;
  confidence: number | null;
  issueCount: number | null;
  errorCount: number | null;
  autoFixRate: number | null;
  inserted: number | null;
  skipped: number | null;
  analysisJson: string | null;
  resolutions: string | null;
  progressMsg: string | null;
  error: string | null;
  createdAt: string;
  updatedAt: string;
}

const CREATE_TABLE_SQL = `
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'tbImportJobs')
CREATE TABLE tbImportJobs (
  coId              BIGINT          IDENTITY PRIMARY KEY,
  coJobId           NVARCHAR(64)    NOT NULL UNIQUE,
  coStatus          NVARCHAR(32)    NOT NULL,
  coFileName        NVARCHAR(512)   NULL,
  coFileSize        BIGINT          NULL,
  coDetectedType    NVARCHAR(128)   NULL,
  coTargetTable     NVARCHAR(128)   NULL,
  coMappingType     NVARCHAR(64)    NULL,
  coRowCount        BIGINT          NULL,
  coConfidence      NUMERIC(5,4)    NULL,
  coIssueCount      INT             NULL,
  coErrorCount      INT             NULL,
  coAutoFixRate     NUMERIC(5,4)    NULL,
  coInserted        BIGINT          NULL,
  coSkipped         BIGINT          NULL,
  coAnalysisJson    NVARCHAR(MAX)   NULL,
  coResolutions     NVARCHAR(MAX)   NULL,
  coProgressMsg     NVARCHAR(512)   NULL,
  coError           NVARCHAR(MAX)   NULL,
  coCreatedAt       DATETIME        NOT NULL DEFAULT GETDATE(),
  coUpdatedAt       DATETIME        NOT NULL DEFAULT GETDATE()
);
`;

const MIGRATE_RESOLUTIONS_COL = `
IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('tbImportJobs') AND name = 'coResolutions')
  ALTER TABLE tbImportJobs ADD coResolutions NVARCHAR(MAX) NULL;
`;

const CLEANUP_STALE_SQL = `
DELETE FROM tbImportJobs
WHERE coStatus = 'uploading' AND coCreatedAt < DATEADD(HOUR, -1, GETDATE());
`;

async function withConn<T>(fn: (conn: Connection) => Promise<T>): Promise<T> {
  const conn = await connectNew();
  try {
    return await fn(conn);
  } finally {
    await closeConnection(conn).catch(() => {});
  }
}

export async function ensureJobsTable(): Promise<void> {
  await withConn(async (conn) => {
    await execSql(conn, CREATE_TABLE_SQL);
    await execSql(conn, MIGRATE_RESOLUTIONS_COL);
    await execSql(conn, CLEANUP_STALE_SQL);
  });
}

export async function createJob(jobId: string, fileName: string, fileSize: number): Promise<void> {
  await withConn(async (conn) => {
    await new Promise<void>((resolve, reject) => {
      const request = new Request(
        "INSERT INTO tbImportJobs (coJobId, coStatus, coFileName, coFileSize) VALUES (@jobId, 'uploading', @fileName, @fileSize)",
        (err) => { if (err) reject(err); else resolve(); },
      );
      request.addParameter("jobId", TYPES.NVarChar, jobId);
      request.addParameter("fileName", TYPES.NVarChar, fileName);
      request.addParameter("fileSize", TYPES.BigInt, fileSize);
      conn.execSql(request);
    });
  });
}

export async function updateJob(
  jobId: string,
  fields: Partial<{
    status: JobStatus;
    detectedType: string;
    targetTable: string;
    mappingType: string;
    rowCount: number;
    confidence: number;
    issueCount: number;
    errorCount: number;
    autoFixRate: number;
    inserted: number;
    skipped: number;
    analysisJson: string;
    resolutions: string;
    progressMsg: string;
    error: string;
  }>,
): Promise<void> {
  const sets: string[] = [];
  const params: { name: string; type: any; value: any }[] = [];
  let paramIdx = 0;

  const addParam = (col: string, type: any, value: any) => {
    const name = `p${paramIdx++}`;
    sets.push(`${col} = @${name}`);
    params.push({ name, type, value });
  };

  if (fields.status !== undefined) addParam("coStatus", TYPES.NVarChar, fields.status);
  if (fields.detectedType !== undefined) addParam("coDetectedType", TYPES.NVarChar, fields.detectedType);
  if (fields.targetTable !== undefined) addParam("coTargetTable", TYPES.NVarChar, fields.targetTable);
  if (fields.mappingType !== undefined) addParam("coMappingType", TYPES.NVarChar, fields.mappingType);
  if (fields.rowCount !== undefined) addParam("coRowCount", TYPES.BigInt, fields.rowCount);
  if (fields.confidence !== undefined) addParam("coConfidence", TYPES.Numeric, fields.confidence);
  if (fields.issueCount !== undefined) addParam("coIssueCount", TYPES.Int, fields.issueCount);
  if (fields.errorCount !== undefined) addParam("coErrorCount", TYPES.Int, fields.errorCount);
  if (fields.autoFixRate !== undefined) addParam("coAutoFixRate", TYPES.Numeric, fields.autoFixRate);
  if (fields.inserted !== undefined) addParam("coInserted", TYPES.BigInt, fields.inserted);
  if (fields.skipped !== undefined) addParam("coSkipped", TYPES.BigInt, fields.skipped);
  if (fields.analysisJson !== undefined) addParam("coAnalysisJson", TYPES.NVarChar, fields.analysisJson);
  if (fields.resolutions !== undefined) addParam("coResolutions", TYPES.NVarChar, fields.resolutions);
  if (fields.progressMsg !== undefined) addParam("coProgressMsg", TYPES.NVarChar, (fields.progressMsg || "").slice(0, 500));
  if (fields.error !== undefined) addParam("coError", TYPES.NVarChar, fields.error);
  sets.push(`coUpdatedAt = GETDATE()`);

  if (params.length === 0) return;

  await withConn(async (conn) => {
    const sql = `UPDATE tbImportJobs SET ${sets.join(", ")} WHERE coJobId = @jobId`;
    await new Promise<void>((resolve, reject) => {
      const request = new Request(sql, (err) => {
        if (err) reject(err);
        else resolve();
      });
      request.addParameter("jobId", TYPES.NVarChar, jobId);
      for (const p of params) {
        request.addParameter(p.name, p.type, p.value);
      }
      conn.execSql(request);
    });
  });
}

function rowToJob(cols: any[]): JobRow {
  const val = (i: number) => cols[i]?.value ?? null;
  return {
    jobId: val(1),
    status: val(2) as JobStatus,
    fileName: val(3),
    fileSize: val(4),
    detectedType: val(5),
    targetTable: val(6),
    mappingType: val(7),
    rowCount: val(8),
    confidence: val(9) != null ? parseFloat(val(9)) : null,
    issueCount: val(10),
    errorCount: val(11),
    autoFixRate: val(12) != null ? parseFloat(val(12)) : null,
    inserted: val(13),
    skipped: val(14),
    analysisJson: val(15),
    resolutions: val(16),
    progressMsg: val(17),
    error: val(18),
    createdAt: val(19)?.toISOString?.() ?? String(val(19)),
    updatedAt: val(20)?.toISOString?.() ?? String(val(20)),
  };
}

export async function getJob(jobId: string): Promise<JobRow | null> {
  return withConn(async (conn) => {
    const rows = await execSql(conn, `SELECT coId, coJobId, coStatus, coFileName, coFileSize, coDetectedType, coTargetTable, coMappingType, coRowCount, coConfidence, coIssueCount, coErrorCount, coAutoFixRate, coInserted, coSkipped, coAnalysisJson, coResolutions, coProgressMsg, coError, coCreatedAt, coUpdatedAt FROM tbImportJobs WHERE coJobId = '${jobId}'`);
    if (rows.length === 0) return null;
    return rowToJob(rows[0]);
  });
}

export async function listJobs(limit = 50): Promise<JobRow[]> {
  return withConn(async (conn) => {
    const rows = await execSql(conn, `SELECT TOP ${limit} coId, coJobId, coStatus, coFileName, coFileSize, coDetectedType, coTargetTable, coMappingType, coRowCount, coConfidence, coIssueCount, coErrorCount, coAutoFixRate, coInserted, coSkipped, coAnalysisJson, coResolutions, coProgressMsg, coError, coCreatedAt, coUpdatedAt FROM tbImportJobs ORDER BY coCreatedAt DESC`);
    return rows.map(rowToJob);
  });
}

export async function deleteJob(jobId: string): Promise<boolean> {
  return withConn(async (conn) => {
    await execSql(conn, `DELETE FROM tbImportJobs WHERE coJobId = '${jobId}'`);
    return true;
  });
}
