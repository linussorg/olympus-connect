const NULL_EQUIVALENTS = new Set([
  "null", "NULL", "missing", "Missing", "unknow", "unknown", "Unknown", "UNKNOWN",
  "NaN", "nan", "N/A", "n/a", "NA", "na", "none", "None", "NONE", "-", "--",
]);

export function isNullEquivalent(value: string | null | undefined): boolean {
  if (value == null) return true;
  if (value.trim() === "") return true;
  if (NULL_EQUIVALENTS.has(value.trim())) return true;
  return false;
}

export function normalizeNull(value: string): string | null {
  if (isNullEquivalent(value)) return null;
  return value;
}

export function normalizeCaseId(raw: string): number | null {
  if (isNullEquivalent(raw)) return null;
  // Strip prefix variations: CASE-0135, CASE-0135-01, 0135, 135
  let cleaned = raw.trim();
  cleaned = cleaned.replace(/[#%$@ß^äöü]+$/g, ""); // strip trailing garbage
  cleaned = cleaned.replace(/^CASE-/i, "");
  // Take first numeric segment (handles CASE-0135-01 → 0135)
  const match = cleaned.match(/(\d+)/);
  if (!match) return null;
  const num = parseInt(match[1], 10);
  return isNaN(num) ? null : num;
}

export function normalizePatientId(raw: string): number | null {
  if (isNullEquivalent(raw)) return null;
  let cleaned = raw.trim();
  cleaned = cleaned.replace(/[#%$@ß^äöü]+$/g, "");
  cleaned = cleaned.replace(/^pat(?:ient(?:nr|rn)?)?[\d]*[-_]/i, "");
  cleaned = cleaned.replace(/^patientnr/i, "");
  const match = cleaned.match(/(\d+)/);
  if (!match) return null;
  const num = parseInt(match[1], 10);
  return isNaN(num) ? null : num;
}

export function normalizeNumeric(raw: string): string | null {
  if (isNullEquivalent(raw)) return null;
  let cleaned = raw.trim();
  // Strip trailing garbage characters
  cleaned = cleaned.replace(/[ß@$^#äöü\t]+$/g, "");
  // German decimal comma → period (only if no period exists)
  if (cleaned.includes(",") && !cleaned.includes(".")) {
    cleaned = cleaned.replace(",", ".");
  }
  const num = parseFloat(cleaned);
  if (isNaN(num)) return null;
  return String(num);
}

const DATE_PATTERNS: { regex: RegExp; parse: (m: RegExpMatchArray) => string | null }[] = [
  // YYYY-MM-DD [HH:MM[:SS]]
  {
    regex: /^(\d{4})-(\d{2})-(\d{2})(?:\s+(\d{2}):(\d{2})(?::(\d{2}))?)?$/,
    parse: (m) => {
      const [, y, mo, d, h, mi, s] = m;
      if (+mo < 1 || +mo > 12 || +d < 1 || +d > 31) return null;
      return `${y}-${mo}-${d}${h ? ` ${h}:${mi}:${s || "00"}` : ""}`;
    },
  },
  // DD.MM.YYYY [HH:MM[:SS]]
  {
    regex: /^(\d{2})\.(\d{2})\.(\d{4})(?:\s+(\d{2}):(\d{2})(?::(\d{2}))?)?$/,
    parse: (m) => {
      const [, d, mo, y, h, mi, s] = m;
      if (+mo < 1 || +mo > 12 || +d < 1 || +d > 31) return null;
      if (y === "9999" || y === "0000") return null;
      return `${y}-${mo}-${d}${h ? ` ${h}:${mi}:${s || "00"}` : ""}`;
    },
  },
  // MM/DD/YYYY
  {
    regex: /^(\d{2})\/(\d{2})\/(\d{4})$/,
    parse: (m) => {
      const [, mo, d, y] = m;
      if (+mo < 1 || +mo > 12 || +d < 1 || +d > 31) return null;
      return `${y}-${mo}-${d}`;
    },
  },
  // YYYY/MM/DD
  {
    regex: /^(\d{4})\/(\d{2})\/(\d{2})$/,
    parse: (m) => {
      const [, y, mo, d] = m;
      if (+mo < 1 || +mo > 12 || +d < 1 || +d > 31) return null;
      return `${y}-${mo}-${d}`;
    },
  },
  // YYYYMMDD
  {
    regex: /^(\d{4})(\d{2})(\d{2})$/,
    parse: (m) => {
      const [, y, mo, d] = m;
      if (+mo < 1 || +mo > 12 || +d < 1 || +d > 31) return null;
      return `${y}-${mo}-${d}`;
    },
  },
  // DD_MM_YYYY (underscore separators)
  {
    regex: /^(\d{2})_(\d{2})_(\d{4})$/,
    parse: (m) => {
      const [, d, mo, y] = m;
      if (+mo < 1 || +mo > 12 || +d < 1 || +d > 31) return null;
      return `${y}-${mo}-${d}`;
    },
  },
  // YYYY.MM.DD
  {
    regex: /^(\d{4})\.(\d{2})\.(\d{2})$/,
    parse: (m) => {
      const [, y, mo, d] = m;
      if (+mo < 1 || +mo > 12 || +d < 1 || +d > 31) return null;
      return `${y}-${mo}-${d}`;
    },
  },
  // "Mar 02 2025", "Oct 02 2025"
  {
    regex: /^([A-Za-z]{3})\s+(\d{2})\s+(\d{4})$/,
    parse: (m) => {
      const months: Record<string, string> = {
        Jan: "01", Feb: "02", Mar: "03", Apr: "04", May: "05", Jun: "06",
        Jul: "07", Aug: "08", Sep: "09", Oct: "10", Nov: "11", Dec: "12",
      };
      const mo = months[m[1]];
      if (!mo) return null;
      return `${m[3]}-${mo}-${m[2]}`;
    },
  },
  // "06-Dec-2025", "29-Jan-2025"
  {
    regex: /^(\d{2})-([A-Za-z]{3})-(\d{4})$/,
    parse: (m) => {
      const months: Record<string, string> = {
        Jan: "01", Feb: "02", Mar: "03", Apr: "04", May: "05", Jun: "06",
        Jul: "07", Aug: "08", Sep: "09", Oct: "10", Nov: "11", Dec: "12",
      };
      const mo = months[m[2]];
      if (!mo) return null;
      return `${m[3]}-${mo}-${m[1]}`;
    },
  },
  // "2026-Feb-03", "2025-May-10"
  {
    regex: /^(\d{4})-([A-Za-z]{3})-(\d{2})$/,
    parse: (m) => {
      const months: Record<string, string> = {
        Jan: "01", Feb: "02", Mar: "03", Apr: "04", May: "05", Jun: "06",
        Jul: "07", Aug: "08", Sep: "09", Oct: "10", Nov: "11", Dec: "12",
      };
      const mo = months[m[2]];
      if (!mo) return null;
      return `${m[1]}-${mo}-${m[3]}`;
    },
  },
];

export function normalizeDate(raw: string): string | null {
  if (isNullEquivalent(raw)) return null;
  let trimmed = raw.trim();
  // Strip trailing garbage characters (same set as normalizeNumeric)
  trimmed = trimmed.replace(/[ß@$^#äöü%]+$/g, "");
  if (trimmed === "00.00.0000" || trimmed === "00/00/0000" || trimmed === "99.99.9999") return null;

  for (const pattern of DATE_PATTERNS) {
    const match = trimmed.match(pattern.regex);
    if (match) return pattern.parse(match);
  }

  return trimmed; // return as-is if no pattern matches
}

const SHIFT_MAP: Record<string, string> = {
  "frühschicht": "Early shift", "frühdienst": "Early shift", "early shift": "Early shift",
  "spätschicht": "Late shift", "spätdienst": "Late shift", "late shift": "Late shift",
  "nachtschicht": "Night shift", "nachtdienst": "Night shift", "night shift": "Night shift",
};

export function normalizeShift(raw: string): string | null {
  if (isNullEquivalent(raw)) return null;
  const key = raw.trim().toLowerCase();
  return SHIFT_MAP[key] ?? raw.trim();
}

export function normalizeFreeText(raw: string): string | null {
  if (isNullEquivalent(raw)) return null;
  let text = raw.trim();
  // Strip garbage markers like @PRIORITY#, @URGENT#
  text = text.replace(/@\w+#/g, "").trim();
  // Collapse multiple whitespace to single space
  text = text.replace(/\s+/g, " ").trim();
  return text || null;
}

export function normalizeFlag(raw: string): string | null {
  if (isNullEquivalent(raw)) return null;
  // Strip trailing garbage characters first
  let cleaned = raw.trim().replace(/[ß@$^#äöü%]+$/g, "");
  cleaned = cleaned.trim().toUpperCase();
  if (cleaned === "") return null;
  if (cleaned === "H" || cleaned === "HIGH" || cleaned === "HH" || cleaned === "HHÜ") return "H";
  if (cleaned === "L" || cleaned === "LOW" || cleaned === "LL" || cleaned === "LLÜ") return "L";
  if (cleaned === "N" || cleaned === "NORMAL") return null;
  return cleaned;
}
