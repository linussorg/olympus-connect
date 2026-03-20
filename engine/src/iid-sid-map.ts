import { readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

interface IidSidEntry {
  iid: string;
  sid: string | null;
  nameDe: string;
  nameEn: string;
}

let _entries: IidSidEntry[] | null = null;
let _sidToIid: Map<string, string> | null = null;
let _iidToColumn: Map<string, string> | null = null;
let _iidToName: Map<string, { de: string; en: string }> | null = null;

function findCsvPath(): string {
  // Look relative to the engine directory, then up to project root
  const candidates = [
    join(process.cwd(), "..", "IID-SID-ITEM.csv"),
    join(process.cwd(), "IID-SID-ITEM.csv"),
  ];

  // Also try relative to this source file
  try {
    const thisDir = dirname(fileURLToPath(import.meta.url));
    candidates.push(join(thisDir, "..", "..", "..", "IID-SID-ITEM.csv"));
    candidates.push(join(thisDir, "..", "..", "IID-SID-ITEM.csv"));
  } catch { /* ignore */ }

  for (const p of candidates) {
    try {
      readFileSync(p, "utf-8");
      return p;
    } catch { /* try next */ }
  }

  throw new Error("IID-SID-ITEM.csv not found. Expected in project root.");
}

function load(): IidSidEntry[] {
  if (_entries) return _entries;

  const path = findCsvPath();
  const text = readFileSync(path, "utf-8");
  const lines = text.split(/\r?\n/).filter((l) => l.trim());

  // Skip header: ItmIID;ItmSID;ItmName255_DE;ItmName255_EN
  _entries = [];
  for (let i = 1; i < lines.length; i++) {
    const parts = lines[i].split(";");
    if (parts.length < 4) continue;

    const iid = parts[0].trim();
    const sid = parts[1].trim() || null;
    const nameDe = parts[2].trim();
    const nameEn = parts[3].trim();

    if (!iid) continue;
    _entries.push({ iid, sid, nameDe, nameEn });
  }

  console.log(`Loaded IID-SID-ITEM.csv: ${_entries.length} entries from ${path}`);
  return _entries;
}

/** Convert IID like "E0_I_001" to SQL column name "coE0I001" */
export function iidToColumnName(iid: string): string {
  const stripped = iid.replace(/_/g, "");
  return `co${stripped}`;
}

/** Get the SID → IID lookup map */
export function getSidToIidMap(): Map<string, string> {
  if (_sidToIid) return _sidToIid;

  const entries = load();
  _sidToIid = new Map();

  // Deduplicate: keep first occurrence per SID
  for (const entry of entries) {
    if (entry.sid && !_sidToIid.has(entry.sid)) {
      _sidToIid.set(entry.sid, entry.iid);
    }
  }

  return _sidToIid;
}

/** Get the IID → SQL column name map */
export function getIidToColumnMap(): Map<string, string> {
  if (_iidToColumn) return _iidToColumn;

  const entries = load();
  _iidToColumn = new Map();

  for (const entry of entries) {
    if (!_iidToColumn.has(entry.iid)) {
      _iidToColumn.set(entry.iid, iidToColumnName(entry.iid));
    }
  }

  return _iidToColumn;
}

/** Get the IID → human-readable name map */
export function getIidToNameMap(): Map<string, { de: string; en: string }> {
  if (_iidToName) return _iidToName;

  const entries = load();
  _iidToName = new Map();

  for (const entry of entries) {
    if (!_iidToName.has(entry.iid)) {
      _iidToName.set(entry.iid, { de: entry.nameDe, en: entry.nameEn });
    }
  }

  return _iidToName;
}

/** Full chain: SID → IID → SQL column name. Returns null if SID not found. */
export function getTargetColumn(sid: string): string | null {
  const sidMap = getSidToIidMap();
  const iid = sidMap.get(sid);
  if (!iid) return null;
  return iidToColumnName(iid);
}

/** Get human-readable name for a SID */
export function getSidName(sid: string): { de: string; en: string } | null {
  const sidMap = getSidToIidMap();
  const iid = sidMap.get(sid);
  if (!iid) return null;
  const nameMap = getIidToNameMap();
  return nameMap.get(iid) || null;
}

/** Get stats about the loaded map */
export function getMapStats(): { totalEntries: number; uniqueIids: number; uniqueSids: number } {
  const entries = load();
  const sidMap = getSidToIidMap();
  const iidMap = getIidToColumnMap();
  return {
    totalEntries: entries.length,
    uniqueIids: iidMap.size,
    uniqueSids: sidMap.size,
  };
}
