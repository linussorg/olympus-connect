import { readFileSync, writeFileSync, existsSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const DATA_DIR = join(__dirname, "..", "data");
const OVERRIDES_FILE = join(DATA_DIR, "mapping-overrides.json");

export interface MappingOverrideEntry {
  id: string;
  sourceColumn: string;
  targetColumn: string;
  targetTable: string;
  createdAt: string;
  updatedAt: string;
}

let overrides: MappingOverrideEntry[] | null = null;

function ensureDir() {
  if (!existsSync(DATA_DIR)) mkdirSync(DATA_DIR, { recursive: true });
}

function load(): MappingOverrideEntry[] {
  if (overrides) return overrides;
  ensureDir();
  if (existsSync(OVERRIDES_FILE)) {
    try {
      overrides = JSON.parse(readFileSync(OVERRIDES_FILE, "utf-8"));
    } catch {
      overrides = [];
    }
  } else {
    overrides = [];
  }
  return overrides!;
}

function save() {
  ensureDir();
  writeFileSync(OVERRIDES_FILE, JSON.stringify(overrides, null, 2), "utf-8");
}

function makeId(sourceColumn: string, targetTable: string): string {
  return `${targetTable}::${sourceColumn}`;
}

export function getOverrides(): MappingOverrideEntry[] {
  return load();
}

export function getOverridesForTable(targetTable: string): Map<string, string> {
  const result = new Map<string, string>();
  for (const o of load()) {
    if (o.targetTable === targetTable) {
      result.set(o.sourceColumn, o.targetColumn);
    }
  }
  return result;
}

export function setOverride(sourceColumn: string, targetColumn: string, targetTable: string): MappingOverrideEntry {
  const entries = load();
  const id = makeId(sourceColumn, targetTable);
  const now = new Date().toISOString();

  const existing = entries.find((e) => e.id === id);
  if (existing) {
    existing.targetColumn = targetColumn;
    existing.updatedAt = now;
    save();
    return existing;
  }

  const entry: MappingOverrideEntry = {
    id,
    sourceColumn,
    targetColumn,
    targetTable,
    createdAt: now,
    updatedAt: now,
  };
  entries.push(entry);
  save();
  return entry;
}

export function deleteOverride(id: string): boolean {
  const entries = load();
  const idx = entries.findIndex((e) => e.id === id);
  if (idx < 0) return false;
  entries.splice(idx, 1);
  save();
  return true;
}

export function updateOverride(id: string, targetColumn: string): MappingOverrideEntry | null {
  const entries = load();
  const entry = entries.find((e) => e.id === id);
  if (!entry) return null;
  entry.targetColumn = targetColumn;
  entry.updatedAt = new Date().toISOString();
  save();
  return entry;
}
