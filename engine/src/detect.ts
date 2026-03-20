import type { DetectionResult, ParsedFile } from "./types.js";

export function detect(parsed: ParsedFile, fileName: string): DetectionResult {
  const { headers, format } = parsed;
  const joined = headers.join(" ").toLowerCase();
  const fileNameLower = fileName.toLowerCase();

  // ─── epaAC formats ───

  // Data-1: long format with SID/SID_value
  if (
    headers.some((h) => /^(fallid|fall_id)$/i.test(h)) &&
    headers.some((h) => /^sid$/i.test(h)) &&
    headers.some((h) => /^sid_value$/i.test(h))
  ) {
    return {
      format,
      delimiter: parsed.delimiter,
      detectedType: "epaAC-data-1",
      targetTable: "tbImportAcData",
      mappingType: "pivot",
      confidence: 0.95,
    };
  }

  // Data-2: SAP coded wide format (MANDT, PATFAL, EPA0001...)
  if (
    headers.some((h) => /^mandt$/i.test(h)) &&
    headers.some((h) => /^epa0/i.test(h))
  ) {
    return {
      format,
      delimiter: parsed.delimiter,
      detectedType: "epaAC-data-2",
      targetTable: "tbImportAcData",
      mappingType: "code-translate",
      confidence: 0.95,
    };
  }

  // Data-3: German labels with IID codes (EinschIDFall, FallNr...)
  if (
    joined.includes("einschid") ||
    (joined.includes("fallnr") && joined.includes("aufndat"))
  ) {
    return {
      format,
      delimiter: parsed.delimiter,
      detectedType: "epaAC-data-3",
      targetTable: "tbImportAcData",
      mappingType: "code-translate",
      confidence: 0.95,
    };
  }

  // Data-5: encrypted/base64 headers (contain +, /, = patterns)
  if (
    headers.length > 10 &&
    headers.filter((h) => /[+/]/.test(h) || /==$/.test(h)).length > headers.length * 0.3
  ) {
    return {
      format,
      delimiter: parsed.delimiter,
      detectedType: "epaAC-data-5",
      targetTable: "tbImportAcData",
      mappingType: "code-translate",
      confidence: 0.7,
    };
  }

  // ─── Structured data (simple column rename) ───

  // Labs data
  if (
    joined.includes("sodium") ||
    joined.includes("potassium") ||
    joined.includes("creatinin") ||
    joined.includes("hemoglobin") ||
    // Error dataset abbreviated headers
    (headers.some((h) => /^na$/i.test(h)) &&
      headers.some((h) => /^k$/i.test(h)) &&
      headers.some((h) => /^crp$/i.test(h)))
  ) {
    return {
      format,
      delimiter: parsed.delimiter,
      detectedType: "labs",
      targetTable: "tbImportLabsData",
      mappingType: "column-rename",
      confidence: 0.95,
    };
  }

  // ICD-10 / OPS
  if (
    joined.includes("icd10") ||
    joined.includes("icd_10") ||
    joined.includes("ops_code") ||
    joined.includes("icd10_haupt")
  ) {
    return {
      format,
      delimiter: parsed.delimiter,
      detectedType: "icd10",
      targetTable: "tbImportIcd10Data",
      mappingType: "column-rename",
      confidence: 0.95,
    };
  }

  // Medication
  if (
    (joined.includes("record_type") || joined.includes("rec_type")) &&
    (joined.includes("medication") || joined.includes("medikament") || joined.includes("dose") || joined.includes("dosis"))
  ) {
    return {
      format,
      delimiter: parsed.delimiter,
      detectedType: "medication",
      targetTable: "tbImportMedicationInpatientData",
      mappingType: "column-rename",
      confidence: 0.95,
    };
  }

  // Nursing reports
  if (
    joined.includes("nursing_note") ||
    joined.includes("free_text") ||
    joined.includes("nursingnote")
  ) {
    return {
      format,
      delimiter: parsed.delimiter,
      detectedType: "nursing",
      targetTable: "tbImportNursingDailyReportsData",
      mappingType: "column-rename",
      confidence: 0.95,
    };
  }

  // Nursing: abbreviated headers (clinic_2 format: DAT,CAS,PAT,WAR,SHF,TXT)
  if (headers.length === 6) {
    const hSet = new Set(headers.map((h) => h.toLowerCase()));
    if (hSet.has("dat") && hSet.has("cas") && hSet.has("pat") && hSet.has("war") && hSet.has("shf") && hSet.has("txt")) {
      return {
        format,
        delimiter: parsed.delimiter,
        detectedType: "nursing",
        targetTable: "tbImportNursingDailyReportsData",
        mappingType: "column-rename",
        confidence: 0.9,
      };
    }
  }

  // Nursing: headerless CSV (inferred headers from parse.ts)
  if (headers.length === 0 || headers.includes("nursing_note_free_text")) {
    if (headers.includes("nursing_note_free_text") || fileNameLower.includes("nurs")) {
      return {
        format,
        delimiter: parsed.delimiter,
        detectedType: "nursing",
        targetTable: "tbImportNursingDailyReportsData",
        mappingType: "column-rename",
        confidence: headers.includes("nursing_note_free_text") ? 0.85 : 0.7,
      };
    }
  }

  // Device motion 1Hz (has accel, pressure zones)
  if (
    joined.includes("accel_x") ||
    joined.includes("accelx") ||
    joined.includes("pressure_zone")
  ) {
    return {
      format,
      delimiter: parsed.delimiter,
      detectedType: "device-1hz",
      targetTable: "tbImportDevice1HzMotionData",
      mappingType: "column-rename",
      confidence: 0.95,
    };
  }

  // Device motion hourly
  if (
    joined.includes("movement_index") ||
    joined.includes("movementindex") ||
    (joined.includes("fall_event") && joined.includes("bed_exit"))
  ) {
    return {
      format,
      delimiter: parsed.delimiter,
      detectedType: "device-motion",
      targetTable: "tbImportDeviceMotionData",
      mappingType: "column-rename",
      confidence: 0.95,
    };
  }

  // Fallback: try to detect from filename
  if (fileNameLower.includes("lab")) {
    return { format, delimiter: parsed.delimiter, detectedType: "labs", targetTable: "tbImportLabsData", mappingType: "column-rename", confidence: 0.5 };
  }
  if (fileNameLower.includes("med")) {
    return { format, delimiter: parsed.delimiter, detectedType: "medication", targetTable: "tbImportMedicationInpatientData", mappingType: "column-rename", confidence: 0.5 };
  }
  if (fileNameLower.includes("nurs")) {
    return { format, delimiter: parsed.delimiter, detectedType: "nursing", targetTable: "tbImportNursingDailyReportsData", mappingType: "column-rename", confidence: 0.5 };
  }
  if (fileNameLower.includes("icd") || fileNameLower.includes("ops")) {
    return { format, delimiter: parsed.delimiter, detectedType: "icd10", targetTable: "tbImportIcd10Data", mappingType: "column-rename", confidence: 0.5 };
  }
  if (fileNameLower.includes("epaac") || fileNameLower.includes("epa_ac")) {
    return { format, delimiter: parsed.delimiter, detectedType: "epaAC-unknown", targetTable: "tbImportAcData", mappingType: "column-rename", confidence: 0.3 };
  }

  return {
    format,
    delimiter: parsed.delimiter,
    detectedType: "unknown",
    targetTable: "tbCaseData",
    mappingType: "column-rename",
    confidence: 0.1,
  };
}
