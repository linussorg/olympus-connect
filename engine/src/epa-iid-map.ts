import { readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { iidToColumnName } from "./iid-sid-map.js";

interface EpaIidEntry {
  epaCode: string;
  iid: string;
  section: string;
  nameEn: string;
}

let _entries: EpaIidEntry[] | null = null;
let _epaToIid: Map<string, string> | null = null;

// Known EPA suffixes that don't have their own target columns
const EPA_SUFFIXES = /^(.+?)(VO|ZI|ZT|ZD|AN|TX|ID|AZ|ONE|99|PO|TE|IC)$/;

function findCsvPath(): string {
  const candidates = [
    join(process.cwd(), "..", "EPA-IID-MAP.csv"),
    join(process.cwd(), "EPA-IID-MAP.csv"),
  ];
  try {
    const thisDir = dirname(fileURLToPath(import.meta.url));
    candidates.push(join(thisDir, "..", "..", "..", "EPA-IID-MAP.csv"));
    candidates.push(join(thisDir, "..", "..", "EPA-IID-MAP.csv"));
  } catch { /* ignore */ }

  for (const p of candidates) {
    try {
      readFileSync(p, "utf-8");
      return p;
    } catch { /* try next */ }
  }
  throw new Error("EPA-IID-MAP.csv not found. Expected in project root.");
}

function load(): EpaIidEntry[] {
  if (_entries) return _entries;

  const path = findCsvPath();
  const text = readFileSync(path, "utf-8");
  const lines = text.split(/\r?\n/).filter((l) => l.trim());

  // Skip header: EpaCode;IID;Section;NameEN
  _entries = [];
  for (let i = 1; i < lines.length; i++) {
    const parts = lines[i].split(";");
    if (parts.length < 2) continue;

    const epaCode = parts[0].trim();
    const iid = parts[1].trim();
    const section = parts[2]?.trim() || "";
    const nameEn = parts[3]?.trim() || "";

    if (!epaCode || !iid) continue;
    _entries.push({ epaCode, iid, section, nameEn });
  }

  console.log(`Loaded EPA-IID-MAP.csv: ${_entries.length} entries from ${path}`);
  return _entries;
}

/** Get the EPA code → IID lookup map */
export function getEpaToIidMap(): Map<string, string> {
  if (_epaToIid) return _epaToIid;

  const entries = load();
  _epaToIid = new Map();
  for (const entry of entries) {
    _epaToIid.set(entry.epaCode, entry.iid);
  }
  return _epaToIid;
}

/** Strip known EPA suffixes to get the base code */
export function stripEpaSuffix(code: string): { base: string; suffix: string } | null {
  const match = code.match(EPA_SUFFIXES);
  if (match) return { base: match[1], suffix: match[2] };
  return null;
}

/** Check if an EPA code is a known suffix variant (VO, ZI, ZT, ZD, etc.) */
export function isEpaSuffix(code: string): boolean {
  return EPA_SUFFIXES.test(code);
}

/**
 * Full chain: EPA code → IID → SQL column name.
 * Handles special cases like CASE_ID.
 * Returns null if EPA code not found.
 */
export function getEpaTargetColumn(epaCode: string): { target: string; transform: string; confidence: number } | null {
  const map = getEpaToIidMap();
  const iid = map.get(epaCode);
  if (!iid) return null;

  // Special: CASE_ID is not an IID
  if (iid === "CASE_ID") {
    return { target: "coCaseId", transform: "normalizeCaseId", confidence: 0.95 };
  }

  const target = iidToColumnName(iid);
  const transform = guessTransformForIid(iid, target);
  return { target, transform, confidence: 0.90 };
}

function guessTransformForIid(_iid: string, target: string): string {
  if (/date|E2I225|E2I223|E2I228/i.test(target)) return "normalizeDate";
  if (/caseid/i.test(target)) return "normalizeCaseId";
  if (/E0I005/i.test(target)) return "parseFloat"; // BMI
  if (/E2I089|E2I09[0-9]|E2I10[01]|E3I/i.test(target)) return "normalizeNull"; // text fields
  return "parseInt";
}

/** Get stats about the loaded map */
export function getEpaMapStats(): { totalEntries: number; sections: string[] } {
  const entries = load();
  const sections = [...new Set(entries.map((e) => e.section))];
  return { totalEntries: entries.length, sections };
}
