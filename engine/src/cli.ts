import { analyze } from "./mapping/engine.js";
import { importToDb, generateSqlDump } from "./db/loader.js";
import { isDbAvailable, close } from "./db/connection.js";
import { validateCrossTable } from "./validators/index.js";
import { buildAnomalyReport } from "./validators/index.js";
import type { AnomalyReport } from "./types.js";
import { readdirSync, statSync, writeFileSync } from "fs";
import { join } from "path";

// ─── Pretty Printing ───

const RESET = "\x1b[0m";
const BOLD = "\x1b[1m";
const DIM = "\x1b[2m";
const RED = "\x1b[31m";
const GREEN = "\x1b[32m";
const YELLOW = "\x1b[33m";
const BLUE = "\x1b[34m";
const CYAN = "\x1b[36m";

function printHeader(text: string) {
  console.log(`\n${BOLD}${CYAN}═══ ${text} ═══${RESET}\n`);
}

function printResult(result: Awaited<ReturnType<typeof analyze>>) {
  // File info
  console.log(`${BOLD}File:${RESET}         ${result.fileName}`);
  console.log(`${BOLD}Format:${RESET}       ${result.format} (${result.detectedType})`);
  console.log(`${BOLD}Target:${RESET}       ${result.targetTable}`);
  console.log(`${BOLD}Mapping Type:${RESET} ${result.mappingType}`);
  console.log(`${BOLD}Rows:${RESET}         ${result.rowCount}`);

  // Confidence bar
  const conf = Math.max(0, Math.min(1, result.confidence));
  const barLen = 20;
  const filled = Math.max(0, Math.min(barLen, Math.round(conf * barLen)));
  const bar = "█".repeat(filled) + "░".repeat(barLen - filled);
  const color = conf > 0.7 ? GREEN : conf > 0.4 ? YELLOW : RED;
  console.log(`${BOLD}Confidence:${RESET}   ${color}${bar}${RESET} ${(conf * 100).toFixed(0)}%`);

  // Mappings
  if (result.mappings.length > 0) {
    console.log(`\n${BOLD}${GREEN}Mappings (${result.mappings.length}):${RESET}`);
    for (const m of result.mappings) {
      const confIcon = m.confidence >= 0.8 ? `${GREEN}✓${RESET}` : `${YELLOW}~${RESET}`;
      const transform = m.transform && m.transform !== "none" ? ` ${DIM}[${m.transform}]${RESET}` : "";
      console.log(`  ${confIcon} ${m.source} ${DIM}→${RESET} ${BOLD}${m.target}${RESET}${transform}`);
    }
  }

  // Ambiguous
  if (result.ambiguous.length > 0) {
    console.log(`\n${BOLD}${YELLOW}Needs Input (${result.ambiguous.length}):${RESET}`);
    for (const a of result.ambiguous) {
      console.log(`  ${YELLOW}?${RESET} ${a.question}`);
      for (const c of a.candidates) {
        console.log(`    ${DIM}→ ${c.target} (${(c.confidence * 100).toFixed(0)}% - ${c.reason})${RESET}`);
      }
    }
  }

  // Unmapped
  if (result.unmapped.length > 0) {
    console.log(`\n${BOLD}${DIM}Unmapped (${result.unmapped.length}):${RESET}`);
    const display = result.unmapped.slice(0, 10);
    console.log(`  ${DIM}${display.join(", ")}${result.unmapped.length > 10 ? `, ... (+${result.unmapped.length - 10} more)` : ""}${RESET}`);
  }

  // Anomaly Report
  if (result.anomalyReport) {
    printAnomalyReport(result.anomalyReport);
  } else if (result.issues.length > 0) {
    console.log(`\n${BOLD}Issues (${result.issues.length}):${RESET}`);
    for (const issue of result.issues.slice(0, 10)) {
      const icon = issue.severity === "error" ? `${RED}✗${RESET}` : issue.severity === "warning" ? `${YELLOW}!${RESET}` : `${BLUE}i${RESET}`;
      const loc = issue.row ? ` ${DIM}(row ${issue.row})${RESET}` : "";
      console.log(`  ${icon} ${issue.field}${loc}: ${issue.message}`);
    }
    if (result.issues.length > 10) {
      console.log(`  ${DIM}... +${result.issues.length - 10} more${RESET}`);
    }
  }

  // Preview
  if (result.preview.length > 0) {
    console.log(`\n${BOLD}Preview (first ${result.preview.length} mapped rows):${RESET}`);
    const cols = Object.keys(result.preview[0]).slice(0, 6);
    // Header
    console.log(`  ${DIM}${cols.map((c) => c.padEnd(20)).join(" | ")}${RESET}`);
    console.log(`  ${DIM}${"─".repeat(cols.length * 23)}${RESET}`);
    for (const row of result.preview.slice(0, 3)) {
      const vals = cols.map((c) => {
        const v = row[c];
        return (v != null ? String(v) : "NULL").padEnd(20);
      });
      console.log(`  ${vals.join(" | ")}`);
    }
  }

  console.log();
}

const MAGENTA = "\x1b[35m";

const CATEGORY_LABELS: Record<string, string> = {
  "source-format": "Source Format",
  encoding: "Encoding",
  "null-variant": "NULL Variants",
  "date-format": "Date Formats",
  "id-format": "ID Formats",
  "out-of-range": "Out of Range",
  "flag-drift": "Flag Drift",
  duplicate: "Duplicates",
  orphan: "Orphan Records",
  temporal: "Temporal",
  completeness: "Completeness",
  "free-text": "Free Text",
};

function printAnomalyReport(report: AnomalyReport) {
  const { bySeverity, totalIssues, autoFixRate } = report;

  console.log(`\n${BOLD}${CYAN}─── Anomaly Report ───${RESET}`);
  console.log(
    `  ${RED}${bySeverity.error} errors${RESET}  ${YELLOW}${bySeverity.warning} warnings${RESET}  ${BLUE}${bySeverity.info} info${RESET}  │  ${totalIssues} total`,
  );

  // Auto-fix rate bar
  const fixBar = Math.round(autoFixRate * 20);
  console.log(
    `  Auto-fix: ${GREEN}${"█".repeat(fixBar)}${DIM}${"░".repeat(20 - fixBar)}${RESET} ${(autoFixRate * 100).toFixed(0)}% of issues resolved automatically`,
  );

  // Category breakdown
  const categories = Object.entries(report.byCategory);
  if (categories.length > 0) {
    console.log(`\n  ${BOLD}By Category:${RESET}`);
    for (const [cat, stats] of categories) {
      if (!stats) continue;
      const label = CATEGORY_LABELS[cat] || cat;
      const fixable = stats.autoFixable > 0 ? ` ${GREEN}(${stats.autoFixable} auto-fix)${RESET}` : "";
      const hasErrors = stats.issues.some((i) => i.severity === "error");
      const color = hasErrors ? RED : YELLOW;
      console.log(`    ${color}${label}${RESET}: ${stats.count} issues${fixable}`);
    }
  }

  // Top issues with origin
  if (report.topIssues.length > 0) {
    console.log(`\n  ${BOLD}Top Issues (by affected rows):${RESET}`);
    for (const issue of report.topIssues) {
      const icon = issue.severity === "error" ? `${RED}✗${RESET}` : issue.severity === "warning" ? `${YELLOW}!${RESET}` : `${BLUE}i${RESET}`;
      const fix = issue.autoFix ? ` ${GREEN}[auto-fix]${RESET}` : ` ${MAGENTA}[needs attention]${RESET}`;
      console.log(`    ${icon} ${issue.field}: ${issue.message}${fix}`);
      if (issue.origin) {
        console.log(`      ${DIM}Origin: ${issue.origin}${RESET}`);
      }
      if (issue.suggestion) {
        console.log(`      ${DIM}Action: ${issue.suggestion}${RESET}`);
      }
    }
  }

  // Detail list (capped)
  const nonInfoIssues = report.allIssues.filter((i) => i.severity !== "info");
  if (nonInfoIssues.length > 0) {
    console.log(`\n  ${BOLD}Details (errors & warnings):${RESET}`);
    for (const issue of nonInfoIssues.slice(0, 15)) {
      const icon = issue.severity === "error" ? `${RED}✗${RESET}` : `${YELLOW}!${RESET}`;
      const loc = issue.row ? ` ${DIM}(row ${issue.row})${RESET}` : "";
      const val = issue.value ? ` ${DIM}[${issue.value}]${RESET}` : "";
      console.log(`    ${icon} ${issue.field}${loc}: ${issue.message}${val}`);
    }
    if (nonInfoIssues.length > 15) {
      console.log(`    ${DIM}... +${nonInfoIssues.length - 15} more${RESET}`);
    }
  }

  console.log();
}

// ─── Commands ───

async function analyzeFile(filePath: string) {
  printHeader(`Analyzing: ${filePath.split("/").pop()}`);
  try {
    const result = await analyze(filePath);
    printResult(result);
    return result;
  } catch (err) {
    console.error(`${RED}Error analyzing ${filePath}: ${err}${RESET}`);
    return null;
  }
}

async function analyzeAll(dirPath: string) {
  printHeader(`Analyzing all files in: ${dirPath}`);

  const files = readdirSync(dirPath).filter((f) => {
    const ext = f.split(".").pop()?.toLowerCase();
    return ext === "csv" || ext === "xlsx" || ext === "txt" || ext === "pdf";
  });

  console.log(`Found ${files.length} data files\n`);

  let success = 0;
  let partial = 0;
  let failed = 0;

  for (const file of files) {
    const result = await analyzeFile(join(dirPath, file));
    if (!result) {
      failed++;
    } else if (result.confidence >= 0.7) {
      success++;
    } else if (result.confidence >= 0.3) {
      partial++;
    } else {
      failed++;
    }
  }

  printHeader("Summary");
  console.log(`${GREEN}✓ High confidence:${RESET} ${success}`);
  console.log(`${YELLOW}~ Partial/needs input:${RESET} ${partial}`);
  console.log(`${RED}✗ Failed/low confidence:${RESET} ${failed}`);
  console.log(`  Total: ${files.length}`);
}

async function importFile(filePath: string) {
  printHeader(`Import: ${filePath.split("/").pop()}`);

  const result = await analyze(filePath);
  if (!result || result.mappings.length === 0) {
    console.error(`${RED}Cannot import — no mappings available. Run analyze first.${RESET}`);
    return;
  }

  printResult(result);

  // Try direct DB insert
  const dbAvailable = await isDbAvailable();
  if (dbAvailable) {
    console.log(`\n${BOLD}${CYAN}Inserting into SQL Server...${RESET}`);
    const importResult = await importToDb(result);
    console.log(`${GREEN}✓ Inserted:${RESET} ${importResult.inserted}`);
    console.log(`${YELLOW}~ Skipped:${RESET}  ${importResult.skipped}`);
    if (importResult.errors.length > 0) {
      console.log(`${RED}✗ Errors:${RESET}`);
      importResult.errors.forEach((e) => console.log(`  ${RED}${e}${RESET}`));
    }
  } else {
    console.log(`\n${YELLOW}SQL Server not available — skipping direct insert.${RESET}`);
    console.log(`${DIM}Set DB_HOST, DB_USER, DB_PASSWORD, DB_NAME env vars to connect.${RESET}`);
  }

  // Always generate SQL dump
  const sql = generateSqlDump(result);
  const outPath = filePath.replace(/\.[^.]+$/, "") + "_import.sql";
  writeFileSync(outPath, sql, "utf-8");
  console.log(`\n${GREEN}SQL dump written to:${RESET} ${outPath}`);

  await close();
}

async function importAll(dirPath: string) {
  printHeader(`Import all files in: ${dirPath}`);

  const files = readdirSync(dirPath).filter((f) => {
    const ext = f.split(".").pop()?.toLowerCase();
    return ext === "csv" || ext === "txt";
  });

  console.log(`Found ${files.length} data files\n`);

  for (const file of files) {
    try {
      await importFile(join(dirPath, file));
    } catch (err) {
      console.error(`${RED}Error importing ${file}: ${err}${RESET}`);
    }
  }

  await close();
}

async function dumpFile(filePath: string) {
  printHeader(`SQL Dump: ${filePath.split("/").pop()}`);

  const result = await analyze(filePath);
  if (!result || result.mappings.length === 0) {
    console.error(`${RED}Cannot dump — no mappings available.${RESET}`);
    return;
  }

  const sql = generateSqlDump(result);
  const outPath = filePath.replace(/\.[^.]+$/, "") + "_import.sql";
  writeFileSync(outPath, sql, "utf-8");
  console.log(`${GREEN}SQL dump written to:${RESET} ${outPath}`);
  console.log(`${DIM}${sql.split("\n").length} lines, ${result.preview.length || result.rowCount} rows${RESET}`);
}

async function validateDb() {
  printHeader("Cross-Table Validation");
  console.log(`Checking SQL Server for orphan records, completeness, and consistency...\n`);

  const issues = await validateCrossTable();
  const report = buildAnomalyReport(issues);
  printAnomalyReport(report);

  await close();
}

// ─── Main ───

const [command, ...args] = process.argv.slice(2);

if (!command || command === "help") {
  console.log(`
${BOLD}epaCC Mapping Engine CLI${RESET}

Usage:
  tsx src/cli.ts analyze <file>        Analyze a single file
  tsx src/cli.ts analyze-all <dir>     Analyze all files in a directory
  tsx src/cli.ts import <file>         Analyze + insert into SQL Server + dump .sql
  tsx src/cli.ts import-all <dir>      Import all files in a directory
  tsx src/cli.ts dump <file>           Analyze + generate .sql dump only
  tsx src/cli.ts validate-db           Run cross-table validation against SQL Server
  tsx src/cli.ts help                  Show this help

Environment:
  DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME  — SQL Server connection
  LLM_PROVIDER=openrouter  OPENROUTER_API_KEY=...  — LLM backend
  OLLAMA_URL, OLLAMA_MODEL                          — Ollama backend
  `);
  process.exit(0);
}

if (command === "analyze" && args[0]) {
  analyzeFile(args[0]).then(() => process.exit(0));
} else if (command === "analyze-all" && args[0]) {
  analyzeAll(args[0]).then(() => process.exit(0));
} else if (command === "import" && args[0]) {
  importFile(args[0]).then(() => process.exit(0));
} else if (command === "import-all" && args[0]) {
  importAll(args[0]).then(() => process.exit(0));
} else if (command === "dump" && args[0]) {
  dumpFile(args[0]).then(() => process.exit(0));
} else if (command === "validate-db") {
  validateDb().then(() => process.exit(0));
} else {
  console.error(`Unknown command: ${command}. Run with "help" for usage.`);
  process.exit(1);
}
