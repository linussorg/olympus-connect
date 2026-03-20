import type { MappingSpec, AmbiguousMapping, ParsedFile } from "../types.js";
import type { ProgressEvent } from "./engine.js";
import { getSchema, getSchemaPromptForLLM } from "../schema.js";
import * as llm from "../llm.js";
import { getOverridesForTable } from "../overrides.js";

// Nursing-specific aliases for abbreviated/alternative headers
const NURSING_ALIASES: Record<string, string> = {
  dat: "coReport_date",
  cas: "coCaseId",
  pat: "coPatient_id",
  war: "coWard",
  shf: "coShift",
  txt: "coNursing_note_free_text",
  nursingnote: "coNursing_note_free_text",
  reportdate: "coReport_date",
};

export async function generateColumnMappings(
  parsed: ParsedFile,
  targetTable: string,
  onProgress?: (event: ProgressEvent) => void,
): Promise<{ mappings: MappingSpec[]; ambiguous: AmbiguousMapping[]; unmapped: string[] }> {
  const schema = getSchema(targetTable);
  if (!schema) {
    return { mappings: [], ambiguous: [], unmapped: parsed.headers };
  }

  const validTargets = new Set(schema.columns.map((c) => c.name));
  const targetColumns = schema.columns.map((c) => c.name);

  // Load saved overrides for this table
  const savedOverrides = getOverridesForTable(targetTable);

  // Try heuristic first — it's instant and handles clean data perfectly
  const heuristic = heuristicMapping(parsed.headers, targetColumns);

  // Apply saved overrides on top of heuristic results
  if (savedOverrides.size > 0) {
    applyOverrides(heuristic, savedOverrides, validTargets, parsed.headers);
  }

  const heuristicMappedRatio = heuristic.mappings.length / Math.max(parsed.headers.length, 1);

  onProgress?.({
    step: "heuristic",
    message: `${heuristic.mappings.length}/${parsed.headers.length} Spalten heuristisch zugeordnet`,
    mapped: heuristic.mappings.length,
    total: parsed.headers.length,
  });

  if (heuristicMappedRatio >= 0.6 && heuristic.ambiguous.length === 0) {
    console.log(`Heuristic mapped ${heuristic.mappings.length}/${parsed.headers.length} columns (${(heuristicMappedRatio * 100).toFixed(0)}%), skipping LLM`);
    return heuristic;
  }

  // Heuristic didn't map enough — try LLM (with retry)
  console.log(`Heuristic only mapped ${heuristic.mappings.length}/${parsed.headers.length} columns (${(heuristicMappedRatio * 100).toFixed(0)}%), trying LLM...`);
  const available = await llm.isAvailable();
  if (available) {
    const MAX_ATTEMPTS = 2;
    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      onProgress?.({
        step: "llm",
        message: `LLM verbessert Mapping... (Versuch ${attempt}/${MAX_ATTEMPTS})`,
        attempt,
      });
      try {
        const schemaPrompt = getSchemaPromptForLLM(targetTable);
        let streamedCount = 0;

        const response = await llm.generateMappingStream(
          parsed.headers,
          parsed.rows.slice(0, 5),
          schemaPrompt,
          (mapping) => {
            if (validTargets.has(mapping.target)) {
              streamedCount++;
              onProgress?.({
                step: "llm-mapping",
                message: `LLM: ${streamedCount} Spalten gemappt...`,
                mapped: streamedCount,
                total: parsed.headers.length,
                mapping: { source: mapping.source, target: mapping.target, confidence: mapping.confidence },
              });
            }
          },
        );

        const specs = parseLLMResponse(response, validTargets);

        if (specs.length > heuristic.mappings.length) {
          const mappedSources = new Set(specs.map((m) => m.source));
          const unmapped = parsed.headers.filter((h) => !mappedSources.has(h));
          console.log(`LLM attempt ${attempt}: returned ${specs.length} valid mappings (better than heuristic's ${heuristic.mappings.length})`);
          const llmResult = { mappings: specs, ambiguous: [] as AmbiguousMapping[], unmapped };
          if (savedOverrides.size > 0) applyOverrides(llmResult, savedOverrides, validTargets, parsed.headers);
          return llmResult;
        } else {
          console.log(`LLM attempt ${attempt}: returned ${specs.length} mappings, not better than heuristic (${heuristic.mappings.length})`);
          console.log(`LLM raw response (truncated): ${response.slice(0, 500)}`);
          if (attempt < MAX_ATTEMPTS) {
            console.log(`Retrying...`);
          }
        }
      } catch (err) {
        console.error(`LLM attempt ${attempt} failed:`, err);
        if (attempt < MAX_ATTEMPTS) {
          console.log(`Retrying...`);
        }
      }
    }
  }

  return heuristic;
}

function parseLLMResponse(response: string, validTargets: Set<string>): MappingSpec[] {
  // Try parsing as-is
  let raw: any;
  try {
    raw = JSON.parse(response);
  } catch {
    // LLM might return multiple JSON objects on separate lines (JSONL)
    const lines = response.trim().split("\n").filter((l) => l.trim());
    const objects: any[] = [];
    for (const line of lines) {
      try {
        objects.push(JSON.parse(line.trim()));
      } catch {
        // Try extracting JSON from the line
        const match = line.match(/\{[^}]+\}/);
        if (match) {
          try { objects.push(JSON.parse(match[0])); } catch { /* skip */ }
        }
      }
    }
    if (objects.length > 0) {
      raw = objects;
    } else {
      // Last resort: try to find array in the response
      const arrMatch = response.match(/\[[\s\S]*\]/);
      if (arrMatch) {
        try { raw = JSON.parse(arrMatch[0]); } catch { return []; }
      } else {
        return [];
      }
    }
  }

  // Normalize to array
  let arr: any[];
  if (Array.isArray(raw)) {
    arr = raw;
  } else if (raw && typeof raw === "object") {
    if (raw.mappings && Array.isArray(raw.mappings)) {
      arr = raw.mappings;
    } else if (raw.source && raw.target) {
      // Single mapping object
      arr = [raw];
    } else {
      // Try all array-valued properties
      const arrayProp = Object.values(raw).find((v) => Array.isArray(v));
      arr = (arrayProp as any[]) || [];
    }
  } else {
    return [];
  }

  // Filter to valid mappings, override transform with heuristic inference
  return arr
    .filter((m: any) => m && typeof m === "object" && m.source && m.target && validTargets.has(m.target))
    .map((m: any) => {
      const inferred = inferTransform(m.source, m.target);
      return {
        source: m.source,
        target: m.target,
        transform: inferred !== "none" ? inferred : (m.transform || "none"),
        confidence: typeof m.confidence === "number" ? m.confidence : 0.7,
      };
    });
}

function heuristicMapping(
  headers: string[],
  targetColumns: string[],
): { mappings: MappingSpec[]; ambiguous: AmbiguousMapping[]; unmapped: string[] } {
  const mappings: MappingSpec[] = [];
  const ambiguous: AmbiguousMapping[] = [];
  const usedTargets = new Set<string>();

  for (const header of headers) {
    const hl = header.toLowerCase().replace(/[^a-z0-9]/g, "");

    let best: { col: string; confidence: number } | null = null;

    // Nursing aliases (abbreviated headers)
    const aliasTarget = NURSING_ALIASES[hl];
    if (aliasTarget && targetColumns.includes(aliasTarget) && !usedTargets.has(aliasTarget)) {
      mappings.push({
        source: header,
        target: aliasTarget,
        transform: inferTransform(header, aliasTarget),
        confidence: 0.95,
      });
      usedTargets.add(aliasTarget);
      continue;
    }

    // Special cases
    if (/^(case_id|caseid|fallid|fall_id)$/i.test(header)) {
      best = { col: "coCaseId", confidence: 1.0 };
    } else if (/^(patient_id|patientid|pid|pat_id)$/i.test(header)) {
      const patCol = targetColumns.find((c) => /coPatient/i.test(c));
      if (patCol) best = { col: patCol, confidence: 1.0 };
    }

    // Collect all candidates with scores
    const candidates: { target: string; confidence: number; reason: string }[] = [];

    if (!best) {
      for (const target of targetColumns) {
        if (usedTargets.has(target)) continue;
        const tl = target.replace(/^co/, "").toLowerCase().replace(/[^a-z0-9]/g, "");

        if (hl === tl) {
          best = { col: target, confidence: 1.0 };
          candidates.length = 0;
          break;
        }
        if (hl.includes(tl) || tl.includes(hl)) {
          const conf = Math.min(hl.length, tl.length) / Math.max(hl.length, tl.length);
          const score = Math.max(0.4, conf);
          candidates.push({ target, confidence: score, reason: "Partial name match" });
          if (!best || score > best.confidence) {
            best = { col: target, confidence: score };
          }
        }
      }
      // Sort candidates by confidence descending, keep top 3
      candidates.sort((a, b) => b.confidence - a.confidence);
      if (candidates.length > 3) candidates.length = 3;
    }

    if (best && best.confidence >= 0.6 && !usedTargets.has(best.col)) {
      mappings.push({
        source: header,
        target: best.col,
        transform: inferTransform(header, best.col),
        confidence: best.confidence,
      });
      usedTargets.add(best.col);
    } else if (candidates.length > 0) {
      ambiguous.push({
        sourceColumn: header,
        candidates,
        question: `Should "${header}" map to "${candidates[0].target}"?`,
      });
    }
  }

  const mappedSources = new Set([...mappings.map((m) => m.source), ...ambiguous.map((a) => a.sourceColumn)]);
  const unmapped = headers.filter((h) => !mappedSources.has(h));

  return { mappings, ambiguous, unmapped };
}

function applyOverrides(
  result: { mappings: MappingSpec[]; ambiguous: AmbiguousMapping[]; unmapped: string[] },
  overrides: Map<string, string>,
  validTargets: Set<string>,
  headers: string[],
) {
  const usedTargets = new Set(result.mappings.map((m) => m.target));

  for (const [source, target] of overrides) {
    if (!headers.includes(source)) continue; // source not in this file
    if (!validTargets.has(target)) continue; // target not in schema

    // Check if already mapped
    const existing = result.mappings.find((m) => m.source === source);
    if (existing) {
      usedTargets.delete(existing.target);
      existing.target = target;
      existing.confidence = 1.0;
      existing.transform = inferTransform(source, target);
      usedTargets.add(target);
    } else {
      result.mappings.push({
        source,
        target,
        transform: inferTransform(source, target),
        confidence: 1.0,
      });
      usedTargets.add(target);
    }

    // Remove from ambiguous/unmapped
    result.ambiguous = result.ambiguous.filter((a) => a.sourceColumn !== source);
    result.unmapped = result.unmapped.filter((u) => u !== source);
  }
}

function inferTransform(source: string, target: string): MappingSpec["transform"] {
  const combined = `${source} ${target}`;
  if (/case.?id|fallid|coCaseId/i.test(combined)) return "normalizeCaseId";
  if (/patient.?id|pid|pat.?id|coPatient/i.test(combined)) return "normalizePatientId";
  if (/date|datum|datetime|time|dt$/i.test(source) || /date|_dt$/i.test(target)) return "normalizeDate";
  if (/flag/i.test(source) || /_flag$/i.test(target)) return "normalizeFlag";
  if (/shift|schicht/i.test(combined) && !/date|flag|ref/i.test(source)) return "normalizeShift";
  if (/note|text|txt|bericht|nursing/i.test(combined) && !/date|flag|ref/i.test(source)) return "normalizeFreeText";
  // Numeric lab value columns (target has _value or known analyte name, not flag/ref/date)
  if (/_value$|_val$/i.test(target)) return "parseFloat";
  if (/sodium|potassium|creatinine|egfr|glucose|hemoglobin|wbc|platelets|crp|alt|ast|bilirubin|albumin|inr|lactate/i.test(target) && !/flag|ref|date|id/i.test(target)) return "parseFloat";
  return "none";
}
