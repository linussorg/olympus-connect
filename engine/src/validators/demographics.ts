import type { ParsedFile, ValidationIssue } from "../types.js";
import { isDbAvailable, connect, execSql } from "../db/connection.js";
import { normalizeCaseId, normalizeDate } from "../normalize.js";

// Column name patterns for demographic fields
const CASE_ID_RE = /^(case_id|caseid|fallnr|fall_id)$/i;
const PATIENT_ID_RE = /^(patient_id|patientid|pid|pat_id|patientnr)$/i;
const GENDER_RE = /^(sex|gender|geschlecht)$/i;
const AGE_RE = /^(age|age_years|alter)$/i;
const DOB_RE = /^(date_of_birth|dob|geburtsdatum|birth_date)$/i;
const SPECIMEN_DATE_RE = /^(specimen_datetime|specimen_date|entnahmedatum|probendatum)$/i;

function findCol(headers: string[], pattern: RegExp): number {
  return headers.findIndex((h) => pattern.test(h.trim()));
}

function parseDate(s: string | null): Date | null {
  if (!s) return null;
  const normalized = normalizeDate(s);
  if (!normalized) return null;
  const d = new Date(normalized);
  return isNaN(d.getTime()) ? null : d;
}

function ageAtDate(dob: Date, refDate: Date): number {
  let age = refDate.getFullYear() - dob.getFullYear();
  const monthDiff = refDate.getMonth() - dob.getMonth();
  if (monthDiff < 0 || (monthDiff === 0 && refDate.getDate() < dob.getDate())) {
    age--;
  }
  return age;
}

function normalizeGender(raw: string): string {
  const v = raw.trim().toLowerCase();
  if (v === "m" || v === "male" || v === "männlich") return "M";
  if (v === "f" || v === "female" || v === "weiblich" || v === "w") return "F";
  if (v === "d" || v === "diverse" || v === "divers") return "D";
  return v.toUpperCase();
}

export async function validateDemographics(
  parsed: ParsedFile,
  _detectedType: string,
  targetTable: string,
): Promise<ValidationIssue[]> {
  // Skip for case data itself — no cross-reference needed
  if (targetTable === "tbCaseData") return [];

  // Need DB to compare against
  const dbOk = await isDbAvailable().catch(() => false);
  if (!dbOk) return [];

  const { headers, rows } = parsed;
  const caseIdIdx = findCol(headers, CASE_ID_RE);
  if (caseIdIdx < 0) return []; // no case_id → can't cross-reference

  const patientIdIdx = findCol(headers, PATIENT_ID_RE);
  const genderIdx = findCol(headers, GENDER_RE);
  const ageIdx = findCol(headers, AGE_RE);
  const dobIdx = findCol(headers, DOB_RE);
  const specimenDateIdx = findCol(headers, SPECIMEN_DATE_RE);

  // Nothing demographic to check
  if (patientIdIdx < 0 && genderIdx < 0 && ageIdx < 0 && dobIdx < 0) return [];

  // Collect unique case IDs from upload
  const caseIdMap = new Map<number, number[]>(); // normalizedCaseId → row indices
  for (let i = 0; i < rows.length; i++) {
    const raw = rows[i][caseIdIdx];
    const id = normalizeCaseId(raw || "");
    if (id != null) {
      if (!caseIdMap.has(id)) caseIdMap.set(id, []);
      caseIdMap.get(id)!.push(i);
    }
  }

  if (caseIdMap.size === 0) return [];

  // Query DB for matching cases
  const caseIds = [...caseIdMap.keys()];
  const inClause = caseIds.join(",");
  const conn = await connect();
  const dbRows = await execSql(
    conn,
    `SELECT coE2I222, coPatientId, coGender, coDateOfBirth, coAgeYears FROM tbCaseData WHERE coE2I222 IN (${inClause})`,
  );

  // Build DB lookup: caseId → record
  const dbCases = new Map<number, { patientId: number | null; gender: string | null; dob: Date | null; age: number | null }>();
  for (const row of dbRows) {
    const caseId = row[0]?.value as number | null;
    if (caseId == null) continue;
    dbCases.set(caseId, {
      patientId: row[1]?.value as number | null,
      gender: row[2]?.value as string | null,
      dob: row[3]?.value ? new Date(row[3].value as string) : null,
      age: row[4]?.value as number | null,
    });
  }

  if (dbCases.size === 0) return []; // no matches in DB

  const issues: ValidationIssue[] = [];
  const seen = new Set<string>(); // deduplicate per case

  for (const [caseId, rowIndices] of caseIdMap) {
    const dbCase = dbCases.get(caseId);
    if (!dbCase) continue;

    const row = rows[rowIndices[0]]; // check first occurrence
    const rowNum = rowIndices[0] + 2; // 1-indexed + header

    // Patient ID mismatch
    if (patientIdIdx >= 0 && dbCase.patientId != null) {
      const srcPid = row[patientIdIdx]?.trim();
      if (srcPid && !seen.has(`pid-${caseId}`)) {
        const srcNorm = parseInt(srcPid.replace(/\D/g, ""), 10);
        if (!isNaN(srcNorm) && srcNorm !== dbCase.patientId) {
          seen.add(`pid-${caseId}`);
          issues.push({
            severity: "error",
            field: headers[patientIdIdx],
            row: rowNum,
            value: srcPid,
            message: `Patient-ID Konflikt für Fall ${caseId}: Upload enthält ${srcPid}, Datenbank enthält ${dbCase.patientId}`,
            suggestion: String(dbCase.patientId),
            category: "demographic",
            origin: "Abgleich mit tbCaseData",
          });
        }
      }
    }

    // Gender mismatch
    if (genderIdx >= 0 && dbCase.gender && !seen.has(`gender-${caseId}`)) {
      const srcGender = normalizeGender(row[genderIdx] || "");
      const dbGender = normalizeGender(dbCase.gender);
      if (srcGender && srcGender !== dbGender) {
        seen.add(`gender-${caseId}`);
        issues.push({
          severity: "warning",
          field: headers[genderIdx],
          row: rowNum,
          value: row[genderIdx],
          message: `Geschlecht-Konflikt für Fall ${caseId}: Upload „${row[genderIdx]}", Datenbank „${dbCase.gender}"`,
          suggestion: dbCase.gender,
          category: "demographic",
          origin: "Abgleich mit tbCaseData",
        });
      }
    }

    // Age check — compute expected age from DOB relative to a reference date
    if (ageIdx >= 0 && dbCase.dob && !isNaN(dbCase.dob.getTime()) && !seen.has(`age-${caseId}`)) {
      const srcAge = parseInt(row[ageIdx] || "", 10);
      if (!isNaN(srcAge)) {
        // Find best reference date: specimen date from same row, or current date
        let refDate: Date | null = null;
        if (specimenDateIdx >= 0) {
          refDate = parseDate(row[specimenDateIdx]);
        }
        if (!refDate) refDate = new Date();

        const expectedAge = ageAtDate(dbCase.dob, refDate);
        const diff = Math.abs(srcAge - expectedAge);
        if (diff > 1) {
          seen.add(`age-${caseId}`);
          const dobStr = dbCase.dob.toLocaleDateString("de-DE");
          const refStr = refDate.toLocaleDateString("de-DE");
          issues.push({
            severity: "warning",
            field: headers[ageIdx],
            row: rowNum,
            value: String(srcAge),
            message: `Alter-Diskrepanz für Fall ${caseId}: Upload ${srcAge} Jahre, aber laut Geburtsdatum (${dobStr}) zum Referenzdatum (${refStr}) erwartet: ${expectedAge} Jahre`,
            suggestion: String(expectedAge),
            category: "demographic",
            origin: "Berechnung aus Geburtsdatum und Referenzdatum",
          });
        }
      }
    }

    // DOB mismatch
    if (dobIdx >= 0 && dbCase.dob && !isNaN(dbCase.dob.getTime()) && !seen.has(`dob-${caseId}`)) {
      const srcDob = parseDate(row[dobIdx]);
      if (srcDob) {
        const dbDobTime = new Date(dbCase.dob.getFullYear(), dbCase.dob.getMonth(), dbCase.dob.getDate()).getTime();
        const srcDobTime = new Date(srcDob.getFullYear(), srcDob.getMonth(), srcDob.getDate()).getTime();
        if (dbDobTime !== srcDobTime) {
          seen.add(`dob-${caseId}`);
          issues.push({
            severity: "error",
            field: headers[dobIdx],
            row: rowNum,
            value: row[dobIdx],
            message: `Geburtsdatum-Konflikt für Fall ${caseId}: Upload „${row[dobIdx]}", Datenbank „${dbCase.dob.toLocaleDateString("de-DE")}"`,
            suggestion: dbCase.dob.toLocaleDateString("de-DE"),
            category: "demographic",
            origin: "Abgleich mit tbCaseData",
          });
        }
      }
    }
  }

  if (issues.length > 0) {
    issues.unshift({
      severity: "info",
      field: "(demographics)",
      message: `${issues.length} demografische Abweichung(en) zwischen Upload und Patientenstammdaten erkannt`,
      category: "demographic",
      origin: "Abgleich mit tbCaseData",
      affectedCount: issues.length,
    });
  }

  return issues;
}
