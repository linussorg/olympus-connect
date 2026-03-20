import type { MappingSpec, AmbiguousMapping, ParsedFile } from "../types.js";
import { iidToColumnName, getIidToColumnMap } from "../iid-sid-map.js";
import { getEpaTargetColumn, stripEpaSuffix, isEpaSuffix } from "../epa-iid-map.js";
import { TARGET_SCHEMAS } from "../schema.js";
import { generate } from "../llm.js";
import type { ProgressEvent } from "./engine.js";

interface CodeTranslateResult {
  mappings: MappingSpec[];
  ambiguous: AmbiguousMapping[];
  unmapped: string[];
  dataRows: string[][];   // actual data rows (may differ from parsed.rows for Data-3)
  confidence: number;
}

// Valid target columns for tbImportAcData
const acSchema = TARGET_SCHEMAS.find((s) => s.name === "tbImportAcData")!;
const validTargetColumns = new Set(acSchema.columns.map((c) => c.name));

export async function translateCodeData(
  parsed: ParsedFile,
  detectedType: string,
  onProgress?: (event: ProgressEvent) => void,
): Promise<CodeTranslateResult> {
  switch (detectedType) {
    case "epaAC-data-3":
      onProgress?.({ step: "mapping", message: "IID-Codes aus Zeile 2 werden extrahiert..." });
      return translateData3(parsed);
    case "epaAC-data-2":
      onProgress?.({ step: "mapping", message: "EPA-Codes werden per LLM gemappt..." });
      return translateData2(parsed, onProgress);
    case "epaAC-data-5":
      onProgress?.({ step: "mapping", message: "Verschlüsselte Header werden per LLM gemappt..." });
      return translateData5(parsed, onProgress);
    default:
      return {
        mappings: [],
        ambiguous: [{ sourceColumn: "(headers)", candidates: [], question: `Unknown code-translate variant: ${detectedType}` }],
        unmapped: parsed.headers,
        dataRows: parsed.rows,
        confidence: 0,
      };
  }
}

// ─── Data-3: German labels + IID codes in row 2 ───

function translateData3(parsed: ParsedFile): CodeTranslateResult {
  // Row 0 of parsed.rows = IID code row (row 2 in original CSV)
  // Row 1+ = actual data
  const iidRow = parsed.rows[0] || [];
  const dataRows = parsed.rows.slice(1);

  const mappings: MappingSpec[] = [];
  const unmapped: string[] = [];
  const seenTargets = new Set<string>();

  for (let i = 0; i < parsed.headers.length; i++) {
    const header = parsed.headers[i];
    if (!header || !header.trim()) {
      unmapped.push(""); // preserve empty headers for anomaly reporting
      continue;
    }

    const iidCell = (iidRow[i] || "").trim();

    // Extract primary IID from patterns like "E2_I_222 (E3_I_0889)(STRING)" or "E0_I_001"
    const iid = extractPrimaryIID(iidCell);

    if (!iid) {
      // Try metadata mapping by header name
      const metaMapping = mapMetadataColumn(header);
      if (metaMapping && !seenTargets.has(metaMapping.target)) {
        mappings.push(metaMapping);
        seenTargets.add(metaMapping.target);
      } else {
        unmapped.push(header);
      }
      continue;
    }

    const targetCol = iidToColumnName(iid);
    if (validTargetColumns.has(targetCol) && !seenTargets.has(targetCol)) {
      mappings.push({
        source: header,
        target: targetCol,
        transform: guessTransform(targetCol),
        confidence: 0.95,
      });
      seenTargets.add(targetCol);
    } else if (!seenTargets.has(targetCol)) {
      // IID exists but target column not in schema — skip silently
      unmapped.push(header);
    }
    // Duplicate target → skip (first wins)
  }

  // Exclude entirely-empty columns from confidence denominator
  const nonEmptyColumns = parsed.headers.filter((_, i) =>
    dataRows.some(row => row[i] && row[i].trim() !== "")
  ).length;
  const confidence = mappings.length / Math.max(nonEmptyColumns, 1);

  return {
    mappings,
    ambiguous: [],
    unmapped,
    dataRows,
    confidence: Math.min(0.95, confidence),
  };
}

function extractPrimaryIID(cell: string): string | null {
  if (!cell) return null;
  // Match patterns: E0_I_001, E2_I_225, E0_I_0071, E3_I_0889
  // Also handle: "E2_I_222 (E3_I_0889)(STRING)" → take "E2_I_222"
  // And "ZWrt_E2_I_167" or "ZDat_E2_I_167" → skip these (they're supplemental)
  if (/^(ZWrt_|ZDat_)/i.test(cell)) return null;

  const match = cell.match(/^(E\d+_I_\d+)/);
  return match ? match[1] : null;
}

// ─── Data-2: SAP EPA codes ───
// Uses EPA-IID-MAP.csv for data-driven mapping (no LLM needed)

async function translateData2(
  parsed: ParsedFile,
  onProgress?: (event: ProgressEvent) => void,
): Promise<CodeTranslateResult> {
  const mappings: MappingSpec[] = [];
  const unmapped: string[] = [];
  const skipped: string[] = [];
  const seenTargets = new Set<string>();

  for (const header of parsed.headers) {
    // 1. Direct lookup in EPA-IID-MAP.csv
    const direct = getEpaTargetColumn(header);
    if (direct && validTargetColumns.has(direct.target) && !seenTargets.has(direct.target)) {
      mappings.push({
        source: header,
        target: direct.target,
        transform: direct.transform as MappingSpec["transform"],
        confidence: direct.confidence,
      });
      seenTargets.add(direct.target);
      continue;
    }

    // 2. Strip suffix (VO, ZI, ZT, ZD, AN, etc.) and check if base code has a mapping
    const stripped = stripEpaSuffix(header);
    if (stripped) {
      const baseMapping = getEpaTargetColumn(stripped.base);
      if (baseMapping) {
        // Base code is known but the suffixed variant has no target column → intentionally skip
        skipped.push(header);
        continue;
      }
    }

    // 3. Skip known non-mappable patterns (SAP metadata, risk indicators, timestamps, sub-items)
    if (/^(MANDT|PATGEB|PATFOE|PATDOE|PATADT|EPAOLDDAT|EPAONEKLP|EPAVMAS|EPADOKOE|EPASTONE|EPAVITA1Z|EPAKLAUX|LEPKBMIBER|EPAERHTIM|EPA00VORS|EPA001601)$/i.test(header)) {
      skipped.push(header);
      continue;
    }
    if (/^EPAST|^EPARID|^EPARIST|^EPARIPN|^EPARIER|^EPARIVD|^EPARIVE|^EPARIKO|^EPARISPI|^EPAPP|^X\d\d|^EPA90\d{3,4}$/i.test(header)) {
      skipped.push(header);
      continue;
    }
    // EPA009xxx sub-items (EPA00901A through EPA00901Z, EPA009011-13, EPA009004, etc.)
    if (/^EPA009\d{2}[A-Z0-9]?$/i.test(header) || /^EPA00901[A-Z]$/i.test(header)) {
      skipped.push(header);
      continue;
    }
    // EPA section items with numeric suffixes (EPA000599, EPA001499, EPA001599, EPA100599)
    if (/^EPA\d{4}99$/.test(header)) {
      skipped.push(header);
      continue;
    }
    // EPA section suffix patterns (ONE already handled, but catch EPA03169V, EPA03169)
    if (/^EPA\d{4,5}[A-Z]$/.test(header) && !/^EPA\d{4}$/.test(header)) {
      skipped.push(header);
      continue;
    }

    // 4. Also check if this is a suffixed form even without base match
    if (isEpaSuffix(header)) {
      skipped.push(header);
      continue;
    }

    unmapped.push(header);
  }

  onProgress?.({ step: "mapping", message: `${mappings.length} Spalten gemappt, ${skipped.length} übersprungen` });

  const totalMappable = mappings.length + unmapped.length; // exclude skipped from ratio
  const confidence = totalMappable > 0
    ? mappings.length / totalMappable
    : 0;

  return {
    mappings,
    ambiguous: unmapped.length > 0 ? [{
      sourceColumn: `(${unmapped.length} unmapped EPA columns)`,
      candidates: [],
      question: `${unmapped.length} SAP EPA columns could not be mapped. ${skipped.length} suffixed/metadata columns were intentionally skipped.`,
    }] : [],
    unmapped,
    dataRows: parsed.rows,
    confidence: Math.min(0.95, confidence * 0.7 + 0.25),
  };
}

// ─── Data-5: Binary-encoded headers ───

async function translateData5(
  parsed: ParsedFile,
  onProgress?: (event: ProgressEvent) => void,
): Promise<CodeTranslateResult> {
  const mappings: MappingSpec[] = [];
  const unmapped: string[] = [];
  const seenTargets = new Set<string>();

  // Step 1: Enhanced heuristic pre-mapping using data patterns
  const sampleRows = parsed.rows.slice(0, 10);

  for (let i = 0; i < parsed.headers.length; i++) {
    const values = sampleRows.map((row) => row[i] || "").filter(Boolean);
    const mapping = inferColumnFromValues(parsed.headers[i], values, i, seenTargets);
    if (mapping) {
      mappings.push(mapping);
      seenTargets.add(mapping.target);
    }
  }

  onProgress?.({ step: "mapping", message: `${mappings.length} Spalten aus Datenwerten erkannt, LLM wird aufgerufen...` });

  // Step 2: Batched LLM mapping for remaining columns
  const unmappedIndices = parsed.headers
    .map((h, i) => i)
    .filter((i) => !mappings.some((m) => m.source === parsed.headers[i]));

  const BATCH_SIZE = 40;
  const batches = [];
  for (let start = 0; start < unmappedIndices.length; start += BATCH_SIZE) {
    batches.push(unmappedIndices.slice(start, start + BATCH_SIZE));
  }

  for (let batchIdx = 0; batchIdx < batches.length; batchIdx++) {
    const batchIndices = batches[batchIdx];
    onProgress?.({
      step: "llm-mapping",
      message: `LLM Batch ${batchIdx + 1}/${batches.length}: ${mappings.length} Spalten bisher gemappt...`,
      mapped: mappings.length,
      total: parsed.headers.length,
    });

    try {
      const columnSamples = batchIndices.map((idx) => {
        const vals = sampleRows.slice(0, 3).map((row) => row[idx] || "").join(", ");
        return `Col[${idx}] (header="${parsed.headers[idx]}"): ${vals}`;
      }).join("\n");

      const targetCols = acSchema.columns
        .filter((c) => !seenTargets.has(c.name))
        .map((c) => `${c.name} (${c.type}${c.description ? ` — ${c.description}` : ""})`)
        .join(", ");

      const prompt = `These columns have encoded/encrypted headers. Use the SAMPLE VALUES to infer what each column represents, then map to the target epaAC assessment columns.

COLUMNS WITH SAMPLE VALUES:
${columnSamples}

AVAILABLE TARGET COLUMNS:
${targetCols}

Hints: This is epaAC care assessment data. Values 1-4 are ordinal ratings. Dates are DD.MM.YYYY HH:MM:SS. "Ersteinschätzung"=initial assessment type (coE0I001). Station codes (2-letter) are wards. Large numeric IDs are case numbers (coCaseId). Columns with all-empty values can be skipped.

Return JSON: {"mappings": [{"source":"<exact header string>","target":"<target col name>","confidence":<0-1>}]}
Only map columns where you are reasonably confident. Use the column descriptions to help match.`;

      const raw = await generate(prompt, {
        system: "You are a healthcare data mapping engine. Infer column identities from sample values and map to target schema. Return only valid JSON.",
        format: "json",
        temperature: 0.05,
      });

      const llmMappings = parseLLMResponse(raw);
      for (const m of llmMappings) {
        if (validTargetColumns.has(m.target) && !seenTargets.has(m.target) && (m.confidence || 0) >= 0.4) {
          mappings.push({
            source: m.source,
            target: m.target,
            transform: guessTransform(m.target),
            confidence: Math.min(m.confidence || 0.5, 0.7),
          });
          seenTargets.add(m.target);
        }
      }
      console.log(`[code-translate] Data-5 batch ${batchIdx + 1}/${batches.length}: +${llmMappings.filter(m => validTargetColumns.has(m.target) && (m.confidence || 0) >= 0.4).length} mappings (${mappings.length} total)`);
    } catch (err) {
      console.error(`[code-translate] LLM batch ${batchIdx + 1} failed:`, err);
    }
  }

  for (const h of parsed.headers) {
    if (!mappings.some((m) => m.source === h)) {
      unmapped.push(h);
    }
  }

  const confidence = mappings.length / Math.max(parsed.headers.length, 1);

  return {
    mappings,
    ambiguous: unmapped.length > 0 ? [{
      sourceColumn: `(${unmapped.length} encoded columns)`,
      candidates: [],
      question: `This file uses encrypted/encoded headers. ${mappings.length} columns were mapped from data patterns and LLM inference across ${batches.length} batches. ${unmapped.length} remain unmapped.`,
    }] : [],
    unmapped,
    dataRows: parsed.rows,
    confidence: Math.min(0.7, confidence * 0.6 + 0.1),
  };
}

// ─── Helpers ───

function inferColumnFromValues(
  header: string,
  values: string[],
  colIndex: number,
  seenTargets: Set<string>,
): MappingSpec | null {
  if (values.length === 0) return null;
  const nonEmpty = values.filter((v) => v.trim() !== "" && v !== "0" && v !== "00.00.0000");

  // Patient name pattern (first column with "Lastname, Firstname")
  if (values.some((v) => /^[A-ZÄÖÜa-zäöü]+,\s+[A-ZÄÖÜa-zäöü]+$/.test(v))) {
    return null; // No target column for patient name
  }

  // Gender — M/F/W/D single-letter column
  if (nonEmpty.length > 0 && nonEmpty.every((v) => /^[MFWD]$/.test(v))) {
    // Skip — no gender column in tbImportAcData
    return null;
  }

  // Assessment type — Ersteinschätzung / Zwischeneinschätzung / Abschlusseinschätzung
  if (values.some((v) => /Ersteinschätzung|Zwischeneinschätzung|Abschlusseinschätzung/.test(v))) {
    const target = "coE0I001";
    if (validTargetColumns.has(target) && !seenTargets.has(target)) {
      return { source: header, target, transform: "parseInt", confidence: 0.85 };
    }
  }

  // epaAC version string
  if (values.some((v) => /^epaAC/.test(v))) {
    return null; // metadata, no target
  }

  // Date pattern DD.MM.YYYY as early column → could be DOB or admission
  if (colIndex <= 2 && nonEmpty.length > 0 && nonEmpty.every((v) => /^\d{2}\.\d{2}\.\d{4}$/.test(v))) {
    return null; // DOB — no target column in tbImportAcData for this
  }

  // Case ID — numeric, small-to-medium values, early in the file
  if (colIndex <= 8 && nonEmpty.length > 0 && nonEmpty.every((v) => /^\d{1,6}$/.test(v))) {
    const maxVal = Math.max(...nonEmpty.map(Number));
    if (maxVal < 100) return null; // likely age, not case_id
    const target = "coCaseId";
    if (!seenTargets.has(target)) {
      return { source: header, target, transform: "normalizeCaseId", confidence: 0.7 };
    }
  }

  // Age — small integers (0-120) in early columns
  if (colIndex <= 5 && nonEmpty.length > 0 && nonEmpty.every((v) => /^\d{1,3}$/.test(v))) {
    const vals = nonEmpty.map(Number);
    if (vals.every((v) => v >= 0 && v <= 120)) {
      const target = "coE0I021"; // Age
      if (validTargetColumns.has(target) && !seenTargets.has(target)) {
        return { source: header, target, transform: "parseInt", confidence: 0.6 };
      }
    }
  }

  return null;
}

function mapMetadataColumn(header: string): MappingSpec | null {
  const h = header.toLowerCase().trim();

  if (/einschidfall|einsch.*id.*fall/i.test(h)) {
    return { source: header, target: "coCaseId", transform: "normalizeCaseId", confidence: 0.95 };
  }
  if (/^fallnr/i.test(h)) {
    return { source: header, target: "coE2I222", transform: "normalizeCaseId", confidence: 0.90 };
  }
  if (/^aufndat$/i.test(h)) {
    return { source: header, target: "coE2I223", transform: "normalizeDate", confidence: 0.95 };
  }
  if (/^entlassdat$/i.test(h)) {
    return { source: header, target: "coE2I228", transform: "normalizeDate", confidence: 0.95 };
  }
  if (/^einschdat$/i.test(h)) {
    return { source: header, target: "coE2I225", transform: "normalizeDate", confidence: 0.95 };
  }

  return null;
}

function guessTransform(targetCol: string): MappingSpec["transform"] {
  if (targetCol === "coCaseId") return "normalizeCaseId";
  const colDef = acSchema.columns.find((c) => c.name === targetCol);
  if (!colDef) return "normalizeNull";

  switch (colDef.type) {
    case "smallint":
    case "bigint":
      return "parseInt";
    case "numeric":
      return "parseFloat";
    case "datetime":
      return "normalizeDate";
    default:
      return "normalizeNull";
  }
}

function parseLLMResponse(raw: string): Array<{ source: string; target: string; confidence: number }> {
  try {
    // Try direct parse
    const obj = JSON.parse(raw);
    if (obj.mappings && Array.isArray(obj.mappings)) {
      return obj.mappings;
    }
    if (Array.isArray(obj)) return obj;
    return [];
  } catch {
    // Try to extract JSON from response
    const jsonMatch = raw.match(/\{[\s\S]*"mappings"[\s\S]*\}/);
    if (jsonMatch) {
      try {
        const obj = JSON.parse(jsonMatch[0]);
        return obj.mappings || [];
      } catch { /* fall through */ }
    }
    return [];
  }
}
