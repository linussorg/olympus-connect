import type { TableSchema } from "./api";

const TABLE_LABELS: Record<string, string> = {
  tbCaseData: "Falldaten",
  tbImportLabsData: "Laborwerte",
  tbImportIcd10Data: "ICD-10 Diagnosen",
  tbImportDeviceMotionData: "Bewegungsdaten",
  tbImportDevice1HzMotionData: "1Hz Bewegungsdaten",
  tbImportMedicationInpatientData: "Medikation",
  tbImportNursingDailyReportsData: "Pflegeberichte",
  tbImportAcData: "epaAC Assessments",
};

// Cache: tableName → { columnName → description }
let fieldMap: Record<string, Record<string, string>> | null = null;
let fetchPromise: Promise<void> | null = null;

async function ensureLoaded() {
  if (fieldMap) return;
  if (fetchPromise) { await fetchPromise; return; }

  fetchPromise = fetch("/api/schema")
    .then((res) => (res.ok ? res.json() : []))
    .then((schemas: TableSchema[]) => {
      fieldMap = {};
      for (const table of schemas) {
        const cols: Record<string, string> = {};
        for (const col of table.columns) {
          cols[col.name] = col.description || col.name;
        }
        fieldMap[table.name] = cols;
      }
    })
    .catch(() => {
      fieldMap = {};
    });

  await fetchPromise;
}

export function tableLabel(tableName: string): string {
  return TABLE_LABELS[tableName] || tableName;
}

export function fieldLabel(tableName: string, columnName: string): string {
  if (!fieldMap) return columnName;
  return fieldMap[tableName]?.[columnName] || columnName;
}

export function fieldLabelWithFallback(tableName: string, columnName: string): { label: string; raw: string } {
  const label = fieldLabel(tableName, columnName);
  return { label, raw: label !== columnName ? columnName : "" };
}

export function getTargetFields(tableName: string): string[] {
  if (!fieldMap || !fieldMap[tableName]) return [];
  return Object.keys(fieldMap[tableName]);
}

export function getAllColumnNames(tableName: string): string[] {
  if (!fieldMap) return [];
  // Return ALL column names for the table (including those without descriptions)
  return Object.keys(fieldMap[tableName] || {});
}

export { ensureLoaded };
