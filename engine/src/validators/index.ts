import type { ValidationIssue, ParsedFile, AnomalyReport, AnomalyCategory, AnomalyCategoryStats } from "../types.js";
import { validateStructural } from "./structural.js";
import { validateIdentity } from "./identity.js";
import { validateTemporal } from "./temporal.js";
import { validateNumeric } from "./numeric.js";
import { validateLabs } from "./labs.js";
import { validateCompleteness } from "./completeness.js";
import { validateFreeText } from "./free-text.js";

export { validateCrossTable } from "./cross-table.js";
export { validateDemographics } from "./demographics.js";

export function validateAll(parsed: ParsedFile, detectedType: string): ValidationIssue[] {
  return [
    ...validateStructural(parsed, detectedType),
    ...validateIdentity(parsed, detectedType),
    ...validateTemporal(parsed, detectedType),
    ...validateNumeric(parsed, detectedType),
    ...validateLabs(parsed, detectedType),
    ...validateCompleteness(parsed, detectedType),
    ...validateFreeText(parsed, detectedType),
  ];
}

export function buildAnomalyReport(issues: ValidationIssue[]): AnomalyReport {
  const byCategory: Partial<Record<AnomalyCategory, AnomalyCategoryStats>> = {};
  let errorCount = 0;
  let warningCount = 0;
  let infoCount = 0;
  let autoFixableCount = 0;

  for (const issue of issues) {
    // Severity counts
    if (issue.severity === "error") errorCount++;
    else if (issue.severity === "warning") warningCount++;
    else infoCount++;

    if (issue.autoFix) autoFixableCount++;

    // Category grouping
    const cat = issue.category || "source-format";
    if (!byCategory[cat]) {
      byCategory[cat] = { count: 0, autoFixable: 0, issues: [] };
    }
    const catStats = byCategory[cat]!;
    catStats.count++;
    if (issue.autoFix) catStats.autoFixable++;
    catStats.issues.push(issue);
  }

  // Top 5 issues by affectedCount
  const topIssues = [...issues]
    .filter((i) => i.affectedCount != null && i.affectedCount > 0)
    .sort((a, b) => (b.affectedCount || 0) - (a.affectedCount || 0))
    .slice(0, 5);

  return {
    byCategory,
    bySeverity: { error: errorCount, warning: warningCount, info: infoCount },
    topIssues,
    autoFixRate: issues.length > 0 ? autoFixableCount / issues.length : 1,
    totalIssues: issues.length,
    allIssues: issues,
  };
}
