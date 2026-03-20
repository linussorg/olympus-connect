import { isAvailable, generate, generateMapping, getProviderInfo } from "./llm.js";
import { getSchemaPromptForLLM } from "./schema.js";
import { parseFile } from "./parse.js";
import { detect } from "./detect.js";
import { analyze } from "./mapping/engine.js";
import { normalizeShift } from "./normalize.js";
import { join } from "path";

const RESET = "\x1b[0m";
const BOLD = "\x1b[1m";
const RED = "\x1b[31m";
const GREEN = "\x1b[32m";
const YELLOW = "\x1b[33m";
const CYAN = "\x1b[36m";
const DIM = "\x1b[2m";
const MAGENTA = "\x1b[35m";

function log(msg: string) { console.log(msg); }
function pass(msg: string) { console.log(`  ${GREEN}PASS${RESET} ${msg}`); }
function fail(msg: string) { console.log(`  ${RED}FAIL${RESET} ${msg}`); }
function skip(msg: string) { console.log(`  ${YELLOW}SKIP${RESET} ${msg}`); }
function debug(msg: string) { console.log(`  ${DIM}     ${msg}${RESET}`); }
function section(msg: string) { console.log(`\n${BOLD}${CYAN}── ${msg} ──${RESET}\n`); }

const CLEAN_DIR = "../Endtestdaten_ohne_Fehler_ einheitliche ID";
const ERROR_DIR = "../Endtestdaten_mit_Fehlern_ einheitliche ID";

let passed = 0;
let failed = 0;
let skipped = 0;

function assert(condition: boolean, msg: string) {
  if (condition) { pass(msg); passed++; }
  else { fail(msg); failed++; }
}

function dumpMappings(mappings: any[], label: string) {
  log(`\n  ${MAGENTA}${BOLD}── ${label}: LLM returned ${mappings.length} mappings ──${RESET}`);
  for (const m of mappings) {
    const conf = m.confidence != null ? ` (${(m.confidence * 100).toFixed(0)}%)` : "";
    const transform = m.transform && m.transform !== "none" ? ` [${m.transform}]` : "";
    log(`  ${DIM}  ${m.source} → ${m.target}${conf}${transform}${RESET}`);
  }
  log("");
}

function dumpRawResponse(label: string, raw: string) {
  log(`\n  ${MAGENTA}${BOLD}── ${label}: Raw LLM response ──${RESET}`);
  // Truncate to 2000 chars for readability
  const display = raw.length > 2000 ? raw.slice(0, 2000) + `\n  ... (truncated, ${raw.length} chars total)` : raw;
  for (const line of display.split("\n")) {
    log(`  ${DIM}  ${line}${RESET}`);
  }
  log("");
}

async function main() {
  const info = getProviderInfo();
  log(`${BOLD}epaCC Mapping Engine — LLM Integration Test${RESET}`);
  log(`Provider: ${info.provider}`);
  log(`Model:    ${info.model}`);
  log(`URL:      ${info.url}\n`);

  // ═══ Test 1: Ollama connectivity ═══
  section("1. Ollama Connectivity");
  const available = await isAvailable();
  assert(available, "Ollama is reachable");
  if (!available) {
    log(`\n${RED}Ollama not available. Start it with: ollama serve${RESET}`);
    log(`Then pull a model: ollama pull qwen3:32b`);
    process.exit(1);
  }

  // Test basic generation
  try {
    const raw = await generate("Return the JSON: {\"ok\": true}", { format: "json", temperature: 0 });
    debug(`Raw response: ${raw}`);
    let parsed: any;
    try {
      parsed = JSON.parse(raw);
    } catch (parseErr) {
      fail(`Basic generation returned invalid JSON`);
      dumpRawResponse("Invalid JSON", raw);
      failed++;
      parsed = null;
    }
    if (parsed) {
      assert(parsed.ok === true, `Basic JSON generation works (got: ${JSON.stringify(parsed)})`);
    }
  } catch (e) {
    fail(`Basic generation error: ${e}`);
    failed++;
  }

  // ═══ Test 2: Labs mapping (clean data — easy) ═══
  section("2. Labs Data (clean — should be trivial)");
  try {
    const result = await analyze(join(CLEAN_DIR, "synth_labs_1000_cases.csv"));
    assert(result.detectedType === "labs", `Detected as labs (got: ${result.detectedType})`);
    assert(result.targetTable === "tbImportLabsData", `Target: tbImportLabsData (got: ${result.targetTable})`);
    assert(result.mappings.length >= 50, `Mapped ≥50 columns (got: ${result.mappings.length})`);
    assert(result.confidence >= 0.8, `Confidence ≥80% (got: ${(result.confidence * 100).toFixed(0)}%)`);

    dumpMappings(result.mappings.slice(0, 10), "First 10 mappings");

    const caseMap = result.mappings.find(m => m.source === "case_id");
    assert(caseMap?.target === "coCaseId", `case_id → coCaseId (got: ${caseMap?.target})`);

    const sodiumMap = result.mappings.find(m => m.source === "sodium_mmol_L");
    assert(!!sodiumMap, `sodium_mmol_L is mapped (to: ${sodiumMap?.target})`);

    assert(result.preview.length > 0, `Preview has rows (got: ${result.preview.length})`);
    if (result.preview.length > 0) {
      debug(`Preview row 1 keys: ${Object.keys(result.preview[0]).join(", ")}`);
      debug(`Preview row 1 vals: ${JSON.stringify(result.preview[0])}`);
    }
    const firstRow = result.preview[0];
    const caseIdVal = firstRow?.["coCaseId"] || firstRow?.[caseMap?.target || ""];
    assert(caseIdVal === "1", `First case_id normalized to 1 (got: ${caseIdVal})`);

    if (result.unmapped.length > 0) {
      debug(`Unmapped columns: ${result.unmapped.join(", ")}`);
    }
  } catch (e) {
    fail(`Labs analysis error: ${e}`);
    failed++;
  }

  // ═══ Test 3: Labs mapping (error data — abbreviated headers) ═══
  section("3. Labs Data (error — abbreviated headers: Na, K, Creat...)");
  try {
    const errorLabsPath = join(ERROR_DIR, "synth_labs.csv");
    const parsed = await parseFile(errorLabsPath);
    assert(parsed.headers.length > 0, `Parsed error labs (${parsed.headers.length} columns, ${parsed.rows.length} rows)`);
    debug(`Headers: ${parsed.headers.join(", ")}`);

    const detection = detect(parsed, "synth_labs.csv");
    assert(detection.detectedType === "labs", `Detected as labs despite abbreviated headers (got: ${detection.detectedType})`);

    const result = await analyze(errorLabsPath);
    dumpMappings(result.mappings, "All mappings from abbreviated headers");

    assert(result.mappings.length >= 30, `Mapped ≥30 columns with abbreviated names (got: ${result.mappings.length})`);

    const naMap = result.mappings.find(m => m.source === "Na");
    if (naMap) {
      assert(
        naMap.target.toLowerCase().includes("sodium"),
        `Na → sodium column (got: ${naMap.target})`,
      );
    } else {
      fail("Na column not mapped — LLM didn't figure out abbreviation");
      debug(`Available mappings for Na-like sources: ${result.mappings.filter(m => m.source.toLowerCase().includes("na")).map(m => `${m.source}→${m.target}`).join(", ") || "none"}`);
      failed++;
    }

    const kMap = result.mappings.find(m => m.source === "K");
    if (kMap) {
      assert(
        kMap.target.toLowerCase().includes("potassium"),
        `K → potassium column (got: ${kMap.target})`,
      );
    } else {
      fail("K column not mapped");
      failed++;
    }

    const caseMap = result.mappings.find(m => m.source === "CaseID");
    assert(caseMap?.target === "coCaseId", `CaseID → coCaseId (got: ${caseMap?.target})`);

    if (result.unmapped.length > 0) {
      debug(`Unmapped: ${result.unmapped.join(", ")}`);
    }
    if (result.ambiguous.length > 0) {
      debug(`Ambiguous: ${result.ambiguous.map(a => `${a.sourceColumn}: ${a.question}`).join("; ")}`);
    }
  } catch (e) {
    fail(`Error labs analysis: ${e}`);
    failed++;
  }

  // ═══ Test 4: Medication (clean) ═══
  section("4. Medication Data (clean)");
  try {
    const result = await analyze(join(CLEAN_DIR, "synthetic_medication_raw_inpatient.csv"));
    assert(result.detectedType === "medication", `Detected as medication (got: ${result.detectedType})`);
    assert(result.mappings.length >= 20, `Mapped ≥20 columns (got: ${result.mappings.length})`);
    assert(result.confidence >= 0.8, `Confidence ≥80% (got: ${(result.confidence * 100).toFixed(0)}%)`);
    dumpMappings(result.mappings.slice(0, 10), "First 10 mappings");
  } catch (e) {
    fail(`Medication analysis: ${e}`);
    failed++;
  }

  // ═══ Test 5: Medication (error — German headers) ═══
  section("5. Medication Data (error — German headers: dosis, medikament...)");
  try {
    const errorMedPath = join(ERROR_DIR, "synth_medication_raw_inpatient.csv");
    const parsed = await parseFile(errorMedPath);
    debug(`Headers: ${parsed.headers.join(", ")}`);

    const result = await analyze(errorMedPath);
    dumpMappings(result.mappings, "All mappings from German headers");

    assert(result.detectedType === "medication", `Detected as medication (got: ${result.detectedType})`);

    const doseMap = result.mappings.find(m => m.source === "dosis");
    if (doseMap) {
      assert(
        doseMap.target.toLowerCase().includes("dose"),
        `dosis → dose column (got: ${doseMap.target})`,
      );
    } else {
      fail("dosis column not mapped — LLM didn't translate German");
      debug(`All sources: ${result.mappings.map(m => m.source).join(", ")}`);
      failed++;
    }

    const medMap = result.mappings.find(m => m.source === "medikament");
    if (medMap) {
      assert(
        medMap.target.toLowerCase().includes("medication"),
        `medikament → medication column (got: ${medMap.target})`,
      );
    } else {
      fail("medikament column not mapped");
      failed++;
    }

    if (result.unmapped.length > 0) {
      debug(`Unmapped: ${result.unmapped.join(", ")}`);
    }
  } catch (e) {
    fail(`Error medication analysis: ${e}`);
    failed++;
  }

  // ═══ Test 6: ICD-10 (clean) ═══
  section("6. ICD-10 / OPS Data (clean)");
  try {
    const result = await analyze(join(CLEAN_DIR, "synthetic_cases_icd10_ops.csv"));
    assert(result.detectedType === "icd10", `Detected as icd10 (got: ${result.detectedType})`);
    assert(result.mappings.length >= 10, `Mapped ≥10 columns (got: ${result.mappings.length})`);
    dumpMappings(result.mappings, "All mappings");
  } catch (e) {
    fail(`ICD-10 analysis: ${e}`);
    failed++;
  }

  // ═══ Test 7: ICD-10 (error — German headers) ═══
  section("7. ICD-10 / OPS Data (error — German: ICD10_Haupt, OPS_Code...)");
  try {
    const errorIcdPath = join(ERROR_DIR, "synth_cases_icd10_ops.csv");
    const parsed = await parseFile(errorIcdPath);
    debug(`Headers: ${parsed.headers.join(", ")}`);

    const result = await analyze(errorIcdPath);
    dumpMappings(result.mappings, "All mappings from German headers");

    assert(result.detectedType === "icd10", `Detected as icd10 (got: ${result.detectedType})`);

    const icdMap = result.mappings.find(m => m.source === "ICD10_Haupt");
    if (icdMap) {
      assert(
        icdMap.target.toLowerCase().includes("primary_icd10"),
        `ICD10_Haupt → primary_icd10 (got: ${icdMap.target})`,
      );
    } else {
      fail("ICD10_Haupt not mapped");
      failed++;
    }
  } catch (e) {
    fail(`Error ICD-10 analysis: ${e}`);
    failed++;
  }

  // ═══ Test 8: Nursing Reports ═══
  section("8. Nursing Reports (clean)");
  try {
    const result = await analyze(join(CLEAN_DIR, "synthetic_nursing_daily_reports_en.csv"));
    assert(result.detectedType === "nursing", `Detected as nursing (got: ${result.detectedType})`);
    assert(result.mappings.length >= 5, `Mapped ≥5 columns (got: ${result.mappings.length})`);
    dumpMappings(result.mappings, "All mappings");
  } catch (e) {
    fail(`Nursing analysis: ${e}`);
    failed++;
  }

  // ═══ Test 8b: Nursing (abbreviated headers — clinic_2) ═══
  section("8b. Nursing Reports (abbreviated headers — clinic_2)");
  try {
    const SPLIT_DIR = join(CLEAN_DIR, "split_data_pat_case_altered/split_data_pat_case_altered");
    const result = await analyze(join(SPLIT_DIR, "clinic_2_nursing.csv"));
    assert(result.detectedType === "nursing", `Detected as nursing (got: ${result.detectedType})`);
    assert(result.mappings.length >= 5, `Mapped ≥5 columns (got: ${result.mappings.length})`);
    const datMap = result.mappings.find(m => m.source === "DAT");
    assert(!!datMap && datMap.target === "coReport_date", `DAT → coReport_date (got: ${datMap?.target})`);
    const txtMap = result.mappings.find(m => m.source === "TXT");
    assert(!!txtMap && txtMap.target === "coNursing_note_free_text", `TXT → coNursing_note_free_text (got: ${txtMap?.target})`);
    dumpMappings(result.mappings, "Clinic 2 mappings");
  } catch (e) {
    fail(`Nursing clinic_2: ${e}`);
    failed++;
  }

  // ═══ Test 8c: Nursing (headerless — clinic_3) ═══
  section("8c. Nursing Reports (headerless — clinic_3)");
  try {
    const SPLIT_DIR = join(CLEAN_DIR, "split_data_pat_case_altered/split_data_pat_case_altered");
    const result = await analyze(join(SPLIT_DIR, "clinic_3_nursing.csv"));
    assert(result.detectedType === "nursing", `Detected as nursing (got: ${result.detectedType})`);
    assert(result.mappings.length >= 5, `Mapped ≥5 columns (got: ${result.mappings.length})`);
    dumpMappings(result.mappings, "Clinic 3 mappings");
  } catch (e) {
    fail(`Nursing clinic_3: ${e}`);
    failed++;
  }

  // ═══ Test 8d: Nursing (PDF — clinic_4) ═══
  section("8d. Nursing Reports (PDF — clinic_4)");
  try {
    const SPLIT_DIR = join(CLEAN_DIR, "split_data_pat_case_altered/split_data_pat_case_altered");
    const result = await analyze(join(SPLIT_DIR, "clinic_4_nursing.pdf"));
    assert(result.detectedType === "nursing", `Detected as nursing (got: ${result.detectedType})`);
    assert(result.rowCount >= 8, `At least 8 rows extracted from PDF (got: ${result.rowCount})`);
    assert(result.mappings.length >= 5, `Mapped ≥5 columns (got: ${result.mappings.length})`);
    dumpMappings(result.mappings, "Clinic 4 PDF mappings");
  } catch (e) {
    fail(`Nursing clinic_4 PDF: ${e}`);
    failed++;
  }

  // ═══ Test 8e: Nursing (error dataset) ═══
  section("8e. Nursing Reports (error dataset)");
  try {
    const result = await analyze(join(ERROR_DIR, "synth_nursing_daily_reports.csv"));
    assert(result.detectedType === "nursing", `Detected as nursing (got: ${result.detectedType})`);
    assert(result.mappings.length >= 5, `Mapped ≥5 columns (got: ${result.mappings.length})`);
    assert(result.issues.length > 0, `Found issues (got: ${result.issues.length})`);
    const freeTextIssues = result.issues.filter(i => i.category === "free-text");
    assert(freeTextIssues.length > 0, `Found free-text issues (got: ${freeTextIssues.length})`);
    dumpMappings(result.mappings, "Error dataset mappings");
    for (const issue of freeTextIssues) {
      debug(`[${issue.severity}] ${issue.message}`);
    }
  } catch (e) {
    fail(`Nursing error dataset: ${e}`);
    failed++;
  }

  // ═══ Test 8f: normalizeShift ═══
  section("8f. normalizeShift() unit tests");
  assert(normalizeShift("Frühschicht") === "Early shift", `Frühschicht → Early shift`);
  assert(normalizeShift("Nachtdienst") === "Night shift", `Nachtdienst → Night shift`);
  assert(normalizeShift("Spätschicht") === "Late shift", `Spätschicht → Late shift`);
  assert(normalizeShift("Late shift") === "Late shift", `Late shift → Late shift`);
  assert(normalizeShift("") === null, `empty → null`);

  // ═══ Test 9: Device Motion ═══
  section("9. Device Motion (hourly, clean)");
  try {
    const result = await analyze(join(CLEAN_DIR, "synthetic_device_motion_fall_data.csv"));
    assert(result.detectedType === "device-motion", `Detected as device-motion (got: ${result.detectedType})`);
    assert(result.mappings.length >= 7, `Mapped ≥7 columns (got: ${result.mappings.length})`);
  } catch (e) {
    fail(`Device motion analysis: ${e}`);
    failed++;
  }

  // ═══ Test 10: epaAC Data-1 (pivot detection) ═══
  section("10. epaAC Data-1 (pivot format detection)");
  try {
    const parsed = await parseFile(join(CLEAN_DIR, "epaAC-Data-1.csv"));
    const detection = detect(parsed, "epaAC-Data-1.csv");
    assert(detection.detectedType === "epaAC-data-1", `Detected as epaAC-data-1 (got: ${detection.detectedType})`);
    assert(detection.mappingType === "pivot", `Mapping type is pivot (got: ${detection.mappingType})`);
    assert(detection.targetTable === "tbImportAcData", `Target is tbImportAcData (got: ${detection.targetTable})`);
  } catch (e) {
    fail(`epaAC Data-1 detection: ${e}`);
    failed++;
  }

  // ═══ Test 11: epaAC Data-2 (code translate detection) ═══
  section("11. epaAC Data-2 (SAP coded format detection)");
  try {
    const parsed = await parseFile(join(CLEAN_DIR, "epaAC-Data-2.csv"));
    const detection = detect(parsed, "epaAC-Data-2.csv");
    assert(detection.detectedType === "epaAC-data-2", `Detected as epaAC-data-2 (got: ${detection.detectedType})`);
    assert(detection.mappingType === "code-translate", `Mapping type is code-translate (got: ${detection.mappingType})`);
  } catch (e) {
    fail(`epaAC Data-2 detection: ${e}`);
    failed++;
  }

  // ═══ Test 12: epaAC Data-5 (encrypted headers detection) ═══
  section("12. epaAC Data-5 (encrypted headers detection)");
  try {
    const parsed = await parseFile(join(CLEAN_DIR, "epaAC-Data-5.csv"));
    const detection = detect(parsed, "epaAC-Data-5.csv");
    assert(detection.detectedType === "epaAC-data-5", `Detected as epaAC-data-5 (got: ${detection.detectedType})`);
    assert(detection.mappingType === "code-translate", `Mapping type is code-translate (got: ${detection.mappingType})`);
    debug(`First 5 headers: ${parsed.headers.slice(0, 5).join(", ")}`);
  } catch (e) {
    fail(`epaAC Data-5 detection: ${e}`);
    failed++;
  }

  // ═══ Test 13: LLM direct mapping quality ═══
  section("13. LLM Mapping Quality (raw prompt test)");
  try {
    const schemaPrompt = getSchemaPromptForLLM("tbImportLabsData");
    const testHeaders = ["CaseID", "PID", "Gender", "Age", "SpecDT", "Na", "Na_flag", "K", "K_flag", "Creat", "CRP"];
    const testRows = [
      ["CASE-0001", "180325", "M", "75", "2025-09-17", "139.6", "", "5.32", "H", "0.87", "12.5"],
      ["CASE-0002", "224401", "F", "68", "2025-11-30", "135.1", "", "4.1", "", "1.12", "3.2"],
    ];

    debug(`Sending ${testHeaders.length} headers to LLM...`);
    const raw = await generateMapping(testHeaders, testRows, schemaPrompt);
    dumpRawResponse("LLM raw mapping response", raw);

    let mappings: any[];
    try {
      const parsed = JSON.parse(raw);
      mappings = Array.isArray(parsed) ? parsed : parsed.mappings || [];
    } catch (parseErr) {
      fail(`LLM returned invalid JSON: ${parseErr}`);
      failed++;
      mappings = [];
    }

    if (mappings.length > 0) {
      dumpMappings(mappings, "Parsed mappings");
    }

    assert(mappings.length >= 8, `LLM returned ≥8 mappings (got: ${mappings.length})`);

    const naMapping = mappings.find((m: any) => m.source === "Na");
    if (naMapping) {
      assert(
        naMapping.target.toLowerCase().includes("sodium"),
        `LLM maps Na → sodium (got: ${naMapping.target})`,
      );
    } else {
      fail("LLM didn't map Na at all");
      debug(`All sources returned: ${mappings.map((m: any) => m.source).join(", ")}`);
      failed++;
    }

    const kMapping = mappings.find((m: any) => m.source === "K");
    if (kMapping) {
      assert(
        kMapping.target.toLowerCase().includes("potassium"),
        `LLM maps K → potassium (got: ${kMapping.target})`,
      );
    } else {
      fail("LLM didn't map K at all");
      debug(`All sources returned: ${mappings.map((m: any) => m.source).join(", ")}`);
      failed++;
    }

    const creatMapping = mappings.find((m: any) => m.source === "Creat");
    if (creatMapping) {
      assert(
        creatMapping.target.toLowerCase().includes("creatinine"),
        `LLM maps Creat → creatinine (got: ${creatMapping.target})`,
      );
    } else {
      fail("LLM didn't map Creat at all");
      failed++;
    }
  } catch (e) {
    fail(`LLM mapping quality error: ${e}`);
    failed++;
  }

  // ═══ Summary ═══
  section("RESULTS");
  log(`${GREEN}Passed:  ${passed}${RESET}`);
  log(`${RED}Failed:  ${failed}${RESET}`);
  log(`${YELLOW}Skipped: ${skipped}${RESET}`);
  log(`Total:   ${passed + failed + skipped}`);
  log("");

  if (failed > 0) {
    log(`${RED}${BOLD}Some tests failed. Check the output above.${RESET}`);
    process.exit(1);
  } else {
    log(`${GREEN}${BOLD}All tests passed!${RESET}`);
    process.exit(0);
  }
}

main().catch((e) => {
  console.error(`${RED}Fatal error: ${e}${RESET}`);
  process.exit(1);
});
