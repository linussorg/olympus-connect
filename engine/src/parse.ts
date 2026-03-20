import type { ParsedFile } from "./types.js";
import { readFileSync } from "fs";
import XLSX from "xlsx";
import { PDFParse } from "pdf-parse";

function detectDelimiter(firstLine: string): string {
  const semicolons = (firstLine.match(/;/g) || []).length;
  const commas = (firstLine.match(/,/g) || []).length;
  return semicolons > commas ? ";" : ",";
}

function splitCSVLine(line: string, delimiter: string): string[] {
  const fields: string[] = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && i + 1 < line.length && line[i + 1] === '"') {
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === delimiter && !inQuotes) {
      fields.push(current.trim());
      current = "";
    } else {
      current += ch;
    }
  }
  fields.push(current.trim());
  return fields;
}

function looksLikeDataValue(value: string): boolean {
  const v = value.trim();
  if (/^(CASE|PAT|ID)[-_]/i.test(v)) return true;
  if (/^\d{4}-\d{2}-\d{2}/.test(v)) return true;
  if (/^\d{2}\.\d{2}\.\d{4}/.test(v)) return true;
  if (/^(Early|Late|Night)\s+shift$/i.test(v)) return true;
  if (/^(Früh|Spät|Nacht)(schicht|dienst)$/i.test(v)) return true;
  return false;
}

export function parseCSV(text: string): ParsedFile {
  const lines = text.split(/\r?\n/).filter((l) => l.trim());
  if (lines.length === 0) {
    return { headers: [], rows: [], delimiter: ",", format: "csv" };
  }

  const delimiter = detectDelimiter(lines[0]);
  const candidateHeaders = splitCSVLine(lines[0], delimiter).map((h) =>
    h.replace(/^"|"$/g, "").replace(/^\uFEFF/, ""), // strip BOM + quotes
  );

  // Detect headerless CSV: if most "headers" look like data values, treat all lines as rows
  const dataLikeCount = candidateHeaders.filter(looksLikeDataValue).length;
  if (dataLikeCount >= 2) {
    const allRows = lines.map((line) =>
      splitCSVLine(line, delimiter).map((cell) => cell.replace(/^"|"$/g, "")),
    );
    return { headers: [], rows: allRows, delimiter, format: "csv" };
  }

  const rows = lines.slice(1).map((line) =>
    splitCSVLine(line, delimiter).map((cell) => cell.replace(/^"|"$/g, "")),
  );

  return { headers: candidateHeaders, rows, delimiter, format: "csv" };
}

function parseXLSX(workbook: XLSX.WorkBook): ParsedFile {
  const sheetName = workbook.SheetNames[0];
  if (!sheetName) {
    return { headers: [], rows: [], delimiter: "", format: "xlsx" };
  }

  const sheet = workbook.Sheets[sheetName];
  // raw: false → dates come as formatted strings instead of serial numbers
  const data: any[][] = XLSX.utils.sheet_to_json(sheet, { header: 1, raw: false, defval: "" });

  if (data.length === 0) {
    return { headers: [], rows: [], delimiter: "", format: "xlsx" };
  }

  const headers = data[0].map((cell: any) => {
    const s = cell == null ? "" : String(cell).trim();
    return s.replace(/^\uFEFF/, ""); // strip BOM
  });

  const rows = data.slice(1)
    .filter((row) => row.some((cell: any) => cell != null && String(cell).trim() !== ""))
    .map((row) =>
      headers.map((_: any, i: number) => {
        const cell = row[i];
        if (cell == null) return "";
        return String(cell);
      }),
    );

  // Detect CSV-in-XLSX: first column contains delimiters and other columns are mostly empty
  // This happens when a CSV file is opened in Excel and saved as XLSX without proper import
  if (headers.length >= 1 && (headers[0].includes(",") || headers[0].includes(";"))) {
    const otherColsEmpty = headers.slice(1).every((h) => !h || !h.trim());
    if (otherColsEmpty || headers.length === 1) {
      const fullText = data.map((row: any[]) => row[0] != null ? String(row[0]) : "").join("\n");
      return { ...parseCSV(fullText), format: "xlsx" };
    }
  }

  return { headers, rows, delimiter: "", format: "xlsx" };
}

const KNOWN_WARDS = new Set([
  "surgery", "geriatrics", "internal medicine", "neurology",
  "cardiology", "icu", "pulmonology", "oncology", "orthopedics",
  "pneumologie", "innere medizin", "chirurgie", "neurologie",
  "kardiologie", "intensivstation", "onkologie", "orthopädie",
]);

const SHIFT_KEYWORDS = /^(early|late|night)\s+shift$|^(früh|spät|nacht)(schicht|dienst)$/i;

export function inferNursingHeaders(parsed: ParsedFile): ParsedFile {
  if (parsed.headers.length > 0 || parsed.rows.length === 0) return parsed;

  const numCols = parsed.rows[0].length;
  const sampleRows = parsed.rows.slice(0, Math.min(20, parsed.rows.length));
  const colTypes: string[] = new Array(numCols).fill("unknown");

  for (let col = 0; col < numCols; col++) {
    const values = sampleRows.map((r) => (r[col] || "").trim()).filter(Boolean);
    if (values.length === 0) continue;

    const caseIdHits = values.filter((v) => /^CASE[-_]\d+/i.test(v)).length;
    const patIdHits = values.filter((v) => /^(PAT|ID)[-_]\d+/i.test(v)).length;
    const shiftHits = values.filter((v) => SHIFT_KEYWORDS.test(v)).length;
    const wardHits = values.filter((v) => KNOWN_WARDS.has(v.toLowerCase())).length;
    const dateHits = values.filter((v) => /^\d{4}-\d{2}-\d{2}/.test(v) || /^\d{2}\.\d{2}\.\d{4}/.test(v)).length;
    const avgLen = values.reduce((s, v) => s + v.length, 0) / values.length;

    const ratio = values.length;
    if (caseIdHits / ratio > 0.5) colTypes[col] = "case_id";
    else if (patIdHits / ratio > 0.5) colTypes[col] = "patient_id";
    else if (shiftHits / ratio > 0.5) colTypes[col] = "shift";
    else if (wardHits / ratio > 0.5) colTypes[col] = "ward";
    else if (dateHits / ratio > 0.5) colTypes[col] = "report_date";
    else if (avgLen > 40) colTypes[col] = "nursing_note_free_text";
  }

  // Assign any remaining unknown with longest avg length as free text
  const freeTextAssigned = colTypes.includes("nursing_note_free_text");
  if (!freeTextAssigned) {
    let maxLen = 0, maxIdx = -1;
    for (let col = 0; col < numCols; col++) {
      if (colTypes[col] !== "unknown") continue;
      const avgLen = sampleRows.reduce((s, r) => s + (r[col]?.length || 0), 0) / sampleRows.length;
      if (avgLen > maxLen) { maxLen = avgLen; maxIdx = col; }
    }
    if (maxIdx >= 0) colTypes[maxIdx] = "nursing_note_free_text";
  }

  return { ...parsed, headers: colTypes };
}

async function parsePDF(buffer: Buffer): Promise<ParsedFile> {
  try {
    const parser = new PDFParse({ data: new Uint8Array(buffer) });
    const textResult = await parser.getText();

    // Use per-page text for structured extraction
    const reportBlocks: string[] = [];
    for (const page of textResult.pages) {
      if (page.text.trim()) reportBlocks.push(page.text);
    }
    // Fallback: if no pages, try splitting full text by Date pattern
    if (reportBlocks.length === 0 && textResult.text.trim()) {
      reportBlocks.push(...textResult.text.split(/(?=(?:Date|Datum):\s*\d{4}-\d{2}-\d{2})/i).filter((b: string) => b.trim()));
    }

    const headers = ["case_id", "patient_id", "ward", "report_date", "shift", "nursing_note_free_text"];
    const rows: string[][] = [];

    for (const block of reportBlocks) {
      const dateMatch = block.match(/(?:Date|Datum):?\s*(\d{4}-\d{2}-\d{2}|\d{2}\.\d{2}\.\d{4})/i);
      const shiftMatch = block.match(/(?:Shift|Schicht):?\s*(.+?)(?:\n|$)/i);
      const patientMatch = block.match(/Patient\s*(?:ID|Nr|Number|No):?\s*(\S+)/i);
      const caseMatch = block.match(/Case\s*(?:ID|Nr|Number|No):?\s*(\S+)/i);
      const wardMatch = block.match(/(?:Ward|Station|Abteilung):?\s*(.+?)(?:\n|$)/i);

      // Extract report text: everything after "Report" or "Bericht" heading
      const reportMatch = block.match(/(?:Report|Bericht)\s*\n([\s\S]*)/i);
      let reportText = reportMatch ? reportMatch[1].trim() : "";
      // Fallback: if no Report heading, take everything after last recognized label
      if (!reportText) {
        const lastLabelIdx = Math.max(
          ...[dateMatch, shiftMatch, patientMatch, caseMatch, wardMatch]
            .filter(Boolean)
            .map((m) => (m!.index || 0) + m![0].length),
        );
        if (lastLabelIdx > 0) {
          reportText = block.slice(lastLabelIdx).trim();
        }
      }

      if (dateMatch || caseMatch) {
        rows.push([
          caseMatch?.[1] || "",
          patientMatch?.[1] || "",
          wardMatch?.[1]?.trim() || "",
          dateMatch?.[1] || "",
          shiftMatch?.[1]?.trim() || "",
          reportText.replace(/\n+/g, " ").trim(),
        ]);
      }
    }

    await parser.destroy();
    return { headers, rows, delimiter: "", format: "pdf" };
  } catch {
    return { headers: [], rows: [], delimiter: "", format: "pdf" };
  }
}

export async function parseFile(filePath: string): Promise<ParsedFile> {
  const buffer = readFileSync(filePath);
  const fileName = filePath.split("/").pop() || filePath;
  return parseBuffer(buffer, fileName);
}

export async function parseBuffer(buffer: Buffer, fileName: string): Promise<ParsedFile> {
  const ext = fileName.split(".").pop()?.toLowerCase();

  if (ext === "csv" || ext === "txt") {
    const text = buffer.toString("utf-8");
    return parseCSV(text);
  }

  if (ext === "xlsx" || ext === "xls") {
    const workbook = XLSX.read(buffer);
    return parseXLSX(workbook);
  }

  if (ext === "pdf") {
    return await parsePDF(buffer);
  }

  return { headers: [], rows: [], delimiter: "", format: ext as any || "unknown" };
}
