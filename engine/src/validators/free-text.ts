import type { ValidationIssue, ParsedFile } from "../types.js";
import { isNullEquivalent } from "../normalize.js";

const GERMAN_CLINICAL_TERMS = [
  "maßnahmen", "bewertung", "übelkeit", "atemnot", "lagerung",
  "schmerzen", "frühschicht", "spätschicht", "nachtschicht",
  "frühdienst", "spätdienst", "nachtdienst", "reduzierter az",
  "guter az", "verschlechtert", "unverändert",
];

function findFreeTextColIdx(headers: string[]): number {
  return headers.findIndex(
    (h) => /note|text|txt|bericht|nursing/i.test(h) && !/date|flag|ref/i.test(h),
  );
}

function findColIdx(headers: string[], pattern: RegExp): number {
  return headers.findIndex((h) => pattern.test(h));
}

export function validateFreeText(parsed: ParsedFile, detectedType: string): ValidationIssue[] {
  if (detectedType !== "nursing") return [];

  const issues: ValidationIssue[] = [];
  const { headers, rows } = parsed;
  const textIdx = findFreeTextColIdx(headers);
  if (textIdx < 0) return [];

  const textCol = headers[textIdx];
  const caseIdx = findColIdx(headers, /case.?id|cas$/i);
  const dateIdx = findColIdx(headers, /date|dat$/i);
  const shiftIdx = findColIdx(headers, /shift|shf$/i);

  let emptyCount = 0;
  let shortCount = 0;
  let garbageCount = 0;
  let mixedLangCount = 0;

  for (let r = 0; r < rows.length; r++) {
    const text = rows[r][textIdx] || "";

    // Empty/missing notes
    if (isNullEquivalent(text)) {
      emptyCount++;
      continue;
    }

    // Suspiciously short notes
    if (text.trim().length < 20) {
      shortCount++;
    }

    // Garbage markers
    if (/@\w+#/.test(text)) {
      garbageCount++;
    }

    // Mixed language detection
    const lower = text.toLowerCase();
    const germanHits = GERMAN_CLINICAL_TERMS.filter((t) => lower.includes(t));
    const hasEnglish = /\b(interventions|evaluation|patient|condition|stable|mobilized|administered|checked|documented)\b/i.test(text);
    if (germanHits.length >= 1 && hasEnglish) {
      mixedLangCount++;
    }
  }

  if (emptyCount > 0) {
    issues.push({
      severity: "error",
      field: textCol,
      message: `${emptyCount} row(s) have empty or missing nursing notes`,
      category: "free-text",
      origin: "Nursing note column is null, empty, or a NULL-equivalent string",
      autoFix: false,
      affectedCount: emptyCount,
    });
  }

  if (shortCount > 0) {
    issues.push({
      severity: "warning",
      field: textCol,
      message: `${shortCount} row(s) have suspiciously short nursing notes (< 20 chars)`,
      suggestion: "Review short notes — they may be incomplete or placeholder entries",
      category: "free-text",
      origin: "Nursing notes shorter than 20 characters are likely incomplete",
      autoFix: false,
      affectedCount: shortCount,
    });
  }

  if (garbageCount > 0) {
    issues.push({
      severity: "warning",
      field: textCol,
      message: `${garbageCount} row(s) contain garbage markers (e.g. @PRIORITY#) in text`,
      suggestion: "Markers like @PRIORITY# will be stripped during normalization",
      category: "free-text",
      origin: "Non-clinical markers embedded in free text, likely from upstream system tags",
      autoFix: true,
      affectedCount: garbageCount,
    });
  }

  if (mixedLangCount > 0) {
    issues.push({
      severity: "info",
      field: textCol,
      message: `${mixedLangCount} row(s) contain mixed German/English text`,
      suggestion: "Notes mix German clinical terms with English — this is common but may affect downstream NLP",
      category: "free-text",
      origin: "Bilingual nursing documentation from DACH-region facilities",
      autoFix: false,
      affectedCount: mixedLangCount,
    });
  }

  // Duplicate reports: same (case_id + date + shift)
  if (caseIdx >= 0 && dateIdx >= 0 && shiftIdx >= 0) {
    const seen = new Map<string, number>();
    let dupeCount = 0;
    for (const row of rows) {
      const key = `${row[caseIdx] || ""}|${row[dateIdx] || ""}|${row[shiftIdx] || ""}`;
      const prev = seen.get(key);
      if (prev !== undefined) {
        dupeCount++;
      } else {
        seen.set(key, 1);
      }
    }
    if (dupeCount > 0) {
      issues.push({
        severity: "warning",
        field: `${headers[caseIdx]}+${headers[dateIdx]}+${headers[shiftIdx]}`,
        message: `${dupeCount} duplicate report(s) detected (same case_id + date + shift)`,
        suggestion: "Duplicate reports may indicate re-exports or copy errors",
        category: "duplicate",
        origin: "Multiple nursing reports for the same case, date, and shift combination",
        autoFix: false,
        affectedCount: dupeCount,
      });
    }
  }

  return issues;
}
