import type { MappingSpec, AmbiguousMapping, ParsedFile } from "../types.js";
import { getTargetColumn, getSidName, getMapStats } from "../iid-sid-map.js";
import { getSchema } from "../schema.js";
import { normalizeCaseId, normalizeDate } from "../normalize.js";

interface PivotResult {
  mappings: MappingSpec[];
  ambiguous: AmbiguousMapping[];
  unmapped: string[];
  pivotedRows: Record<string, string | null>[];
  totalGroups: number;
  resolvedSids: number;
  unresolvedSids: string[];
}

export function pivotEpaACData1(parsed: ParsedFile): PivotResult {
  const { headers, rows } = parsed;

  // Find column indices
  const caseIdIdx = headers.findIndex((h) => /^(fallid|fall_id|case_id)$/i.test(h));
  const pidIdx = headers.findIndex((h) => /^(pid|patient_id)$/i.test(h));
  const assessIdx = headers.findIndex((h) => /^einsch/i.test(h));
  const sidIdx = headers.findIndex((h) => /^sid$/i.test(h));
  const sidValueIdx = headers.findIndex((h) => /^sid_value$/i.test(h));
  const stationIdx = headers.findIndex((h) => /^station$/i.test(h));

  if (sidIdx < 0 || sidValueIdx < 0) {
    return {
      mappings: [],
      ambiguous: [{ sourceColumn: "SID", candidates: [], question: "Could not find SID/SID_value columns in this file." }],
      unmapped: headers,
      pivotedRows: [],
      totalGroups: 0,
      resolvedSids: 0,
      unresolvedSids: [],
    };
  }

  // Load the IID-SID map
  const stats = getMapStats();
  console.log(`IID-SID map: ${stats.uniqueSids} SIDs → ${stats.uniqueIids} IIDs`);

  // Validate target columns exist in schema
  const schema = getSchema("tbImportAcData");
  const validColumns = new Set(schema?.columns.map((c) => c.name) || []);

  // Group rows by (case_id + assessment datetime) — each group becomes one wide row
  const groups = new Map<string, { caseId: string; pid: string; assessment: string; station: string; items: Map<string, string> }>();

  const allSids = new Set<string>();
  const resolvedSids = new Set<string>();
  const unresolvedSids = new Set<string>();

  for (const row of rows) {
    const caseIdRaw = caseIdIdx >= 0 ? row[caseIdIdx] : "";
    const pid = pidIdx >= 0 ? row[pidIdx] : "";
    const assessment = assessIdx >= 0 ? row[assessIdx] : "";
    const station = stationIdx >= 0 ? row[stationIdx] : "";
    const sid = row[sidIdx]?.trim();
    const sidValue = row[sidValueIdx]?.trim();

    if (!sid) continue;

    // Group key: case + assessment datetime (or just case if no assessment)
    const groupKey = `${caseIdRaw}|${assessment}`;

    if (!groups.has(groupKey)) {
      groups.set(groupKey, { caseId: caseIdRaw, pid, assessment, station, items: new Map() });
    }

    const group = groups.get(groupKey)!;
    allSids.add(sid);

    // Resolve SID → target column
    const targetCol = getTargetColumn(sid);
    if (targetCol && validColumns.has(targetCol)) {
      // Last value wins (handles duplicates)
      group.items.set(targetCol, sidValue || null);
      resolvedSids.add(sid);
    } else {
      unresolvedSids.add(sid);
    }
  }

  // Build pivoted rows
  const pivotedRows: Record<string, string | null>[] = [];
  for (const [, group] of groups) {
    const row: Record<string, string | null> = {};

    // Set case_id and patient_id
    const caseId = normalizeCaseId(group.caseId);
    if (caseId != null) row["coCaseId"] = String(caseId);
    if (group.pid) row["coPatientId"] = group.pid;
    if (group.assessment) row["coE2I225"] = normalizeDate(group.assessment) || group.assessment; // assessment datetime (NOT NULL in schema)

    // Set all resolved SID values
    for (const [col, val] of group.items) {
      row[col] = val;
    }

    pivotedRows.push(row);
  }

  // Build mapping specs (SID → target column)
  const mappings: MappingSpec[] = [];
  for (const sid of resolvedSids) {
    const targetCol = getTargetColumn(sid)!;
    const name = getSidName(sid);
    mappings.push({
      source: `SID:${sid}`,
      target: targetCol,
      transform: "none",
      confidence: 1.0,
    });
  }

  // Add metadata column mappings
  if (caseIdIdx >= 0) {
    mappings.unshift({ source: headers[caseIdIdx], target: "coCaseId", transform: "normalizeCaseId", confidence: 1.0 });
  }
  if (pidIdx >= 0) {
    mappings.unshift({ source: headers[pidIdx], target: "coPatientId", transform: "normalizePatientId", confidence: 1.0 });
  }

  const ambiguous: AmbiguousMapping[] = [];
  const unresolvedArr = [...unresolvedSids];
  if (unresolvedArr.length > 0) {
    ambiguous.push({
      sourceColumn: "SID (unresolved)",
      candidates: unresolvedArr.slice(0, 5).map((sid) => ({
        target: sid,
        confidence: 0,
        reason: `SID "${sid}" not found in IID-SID-ITEM.csv`,
      })),
      question: `${unresolvedArr.length} SID values could not be mapped to target columns. These values may be sub-items or structural codes.`,
    });
  }

  console.log(`Pivot: ${groups.size} assessment groups, ${resolvedSids.size} resolved SIDs, ${unresolvedSids.size} unresolved`);

  return {
    mappings,
    ambiguous,
    unmapped: unresolvedArr,
    pivotedRows,
    totalGroups: groups.size,
    resolvedSids: resolvedSids.size,
    unresolvedSids: unresolvedArr,
  };
}
