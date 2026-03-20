import type { ValidationIssue, ParsedFile } from "../types.js";
import { isNullEquivalent } from "../normalize.js";

const DATE_COLUMN_PATTERNS = /date|datetime|time|timestamp|dt$|aufn|entlass|specimen/i;

interface DateFormatInfo {
  pattern: string;
  count: number;
  example: string;
}

function classifyDateFormat(value: string): string | null {
  const v = value.trim();
  if (isNullEquivalent(v)) return null;

  if (/^\d{4}-\d{2}-\d{2}/.test(v)) return "YYYY-MM-DD";
  if (/^\d{2}\.\d{2}\.\d{4}/.test(v)) return "DD.MM.YYYY";
  if (/^\d{2}\/\d{2}\/\d{4}/.test(v)) return "MM/DD/YYYY";
  if (/^\d{4}\/\d{2}\/\d{2}/.test(v)) return "YYYY/MM/DD";
  if (/^\d{8}$/.test(v)) return "YYYYMMDD";
  if (/^\d{2}_\d{2}_\d{4}/.test(v)) return "DD_MM_YYYY";
  if (/^\d{4}\.\d{2}\.\d{2}/.test(v)) return "YYYY.MM.DD";
  if (/^[A-Za-z]{3}\s+\d{2}\s+\d{4}/.test(v)) return "Mon DD YYYY";
  if (/^\d{2}-[A-Za-z]{3}-\d{4}/.test(v)) return "DD-Mon-YYYY";
  if (/^\d{4}-[A-Za-z]{3}-\d{2}/.test(v)) return "YYYY-Mon-DD";
  return "unrecognized";
}

function isImpossibleDate(value: string): string | null {
  const v = value.trim();

  // 99.99.9999 or 00.00.0000
  if (/^(99\.99\.9999|00\.00\.0000|00\/00\/0000)$/.test(v)) return "placeholder date";

  // Month 00 or 13+
  const yyyymmdd = v.match(/^(\d{4})[/.-](\d{2})[/.-](\d{2})/);
  if (yyyymmdd) {
    const mo = parseInt(yyyymmdd[2], 10);
    const d = parseInt(yyyymmdd[3], 10);
    if (mo === 0 || mo > 12) return `invalid month ${mo}`;
    if (d === 0 || d > 31) return `invalid day ${d}`;
  }

  const ddmmyyyy = v.match(/^(\d{2})[/._](\d{2})[/._](\d{4})/);
  if (ddmmyyyy) {
    const a = parseInt(ddmmyyyy[1], 10);
    const b = parseInt(ddmmyyyy[2], 10);
    if (v.includes('/')) {
      // Slash dates: could be MM/DD/YYYY or DD/MM/YYYY — only flag if neither works
      const mmddValid = a >= 1 && a <= 12 && b >= 1 && b <= 31;
      const ddmmValid = b >= 1 && b <= 12 && a >= 1 && a <= 31;
      if (!mmddValid && !ddmmValid) return `invalid date ${a}/${b}`;
    } else {
      // Dot/underscore dates are always DD.MM
      if (b === 0 || b > 12) return `invalid month ${b}`;
      if (a === 0 || a > 31) return `invalid day ${a}`;
    }
  }

  return null;
}

export function validateTemporal(
  parsed: ParsedFile,
  _detectedType: string,
): ValidationIssue[] {
  const issues: ValidationIssue[] = [];
  const { headers, rows } = parsed;

  // Find date columns
  const dateColIndices = headers
    .map((h, i) => ({ header: h, index: i }))
    .filter((c) => DATE_COLUMN_PATTERNS.test(c.header));

  for (const col of dateColIndices) {
    const formatCounts = new Map<string, DateFormatInfo>();
    let impossibleCount = 0;
    let trailingGarbageCount = 0;

    for (let i = 0; i < rows.length; i++) {
      const val = rows[i][col.index];
      if (!val || isNullEquivalent(val)) continue;

      // Trailing special chars on timestamps
      if (/[#@$ß%^äöü]$/.test(val.trim())) {
        trailingGarbageCount++;
        if (trailingGarbageCount <= 2) {
          issues.push({
            severity: "warning",
            field: col.header,
            row: i + 2,
            value: val,
            message: `Timestamp has trailing special character: "${val}"`,
            suggestion: "Trailing characters will be stripped during normalization",
            category: "encoding",
            origin: "Character encoding corruption during data export",
            autoFix: true,
          });
        }
      }

      // Check impossible dates
      const impossibleReason = isImpossibleDate(val);
      if (impossibleReason) {
        impossibleCount++;
        if (impossibleCount <= 3) {
          issues.push({
            severity: "error",
            field: col.header,
            row: i + 2,
            value: val,
            message: `Impossible date (${impossibleReason}): "${val}"`,
            suggestion: "This value will be set to NULL. Verify with source system.",
            category: "temporal",
            origin: "Data entry error or placeholder value from source system",
            autoFix: true,
          });
        }
      }

      // Classify format
      const fmt = classifyDateFormat(val);
      if (fmt) {
        const existing = formatCounts.get(fmt);
        if (existing) {
          existing.count++;
        } else {
          formatCounts.set(fmt, { pattern: fmt, count: 1, example: val });
        }
      }
    }

    if (impossibleCount > 3) {
      issues.push({
        severity: "error",
        field: col.header,
        message: `${impossibleCount} impossible dates total (showing first 3)`,
        category: "temporal",
        origin: "Data entry errors or placeholder values from source system",
        autoFix: true,
        affectedCount: impossibleCount,
      });
    }

    if (trailingGarbageCount > 2) {
      issues.push({
        severity: "warning",
        field: col.header,
        message: `${trailingGarbageCount} timestamps with trailing special characters (showing first 2)`,
        category: "encoding",
        origin: "Character encoding corruption during data export",
        autoFix: true,
        affectedCount: trailingGarbageCount,
      });
    }

    // Report format mix
    if (formatCounts.size > 1) {
      const formats = Array.from(formatCounts.values())
        .sort((a, b) => b.count - a.count)
        .map((f) => `${f.pattern} (${f.count}x, e.g. "${f.example}")`)
        .join(", ");

      issues.push({
        severity: "warning",
        field: col.header,
        message: `${formatCounts.size} different date formats detected: ${formats}`,
        suggestion: "All formats are normalized automatically during import",
        category: "date-format",
        origin: "Data merged from multiple source systems with different date conventions",
        autoFix: true,
        affectedCount: rows.length,
      });
    }
  }

  // Check discharge before admission
  const admissionIdx = headers.findIndex((h) => /admission|aufn/i.test(h) && DATE_COLUMN_PATTERNS.test(h));
  const dischargeIdx = headers.findIndex((h) => /discharge|entlass/i.test(h) && DATE_COLUMN_PATTERNS.test(h));

  if (admissionIdx >= 0 && dischargeIdx >= 0) {
    let invalidCount = 0;
    for (let i = 0; i < rows.length; i++) {
      const admVal = rows[i][admissionIdx];
      const disVal = rows[i][dischargeIdx];
      if (!admVal || !disVal || isNullEquivalent(admVal) || isNullEquivalent(disVal)) continue;

      const admDate = new Date(admVal);
      const disDate = new Date(disVal);
      if (!isNaN(admDate.getTime()) && !isNaN(disDate.getTime()) && disDate < admDate) {
        invalidCount++;
        if (invalidCount <= 2) {
          issues.push({
            severity: "error",
            field: `${headers[admissionIdx]} / ${headers[dischargeIdx]}`,
            row: i + 2,
            value: `${admVal} → ${disVal}`,
            message: `Discharge date is before admission date`,
            suggestion: "Verify dates with source system — likely a data entry error",
            category: "temporal",
            origin: "Date swap error during data entry or export",
            autoFix: false,
          });
        }
      }
    }
    if (invalidCount > 2) {
      issues.push({
        severity: "error",
        field: `${headers[admissionIdx]} / ${headers[dischargeIdx]}`,
        message: `${invalidCount} rows with discharge before admission (showing first 2)`,
        category: "temporal",
        origin: "Systematic date swap in source system export",
        autoFix: false,
        affectedCount: invalidCount,
      });
    }
  }

  return issues;
}
