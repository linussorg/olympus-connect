// LLM-powered insights and risk predictions for the Athena dashboard
import crypto from "crypto";
import * as llm from "./llm.js";
import type { AnomalyReport, ValidationIssue } from "./types.js";

// ─── Types ───

export interface HarmonizationInsight {
  id: string;
  title: string;
  description: string;
  recommendation: string;
  affectedRows: number;
  priority: "high" | "medium" | "low";
  categories: string[];
}

export interface RiskPrediction {
  id: string;
  risk: string;
  explanation: string;
  likelihood: "high" | "medium" | "low";
  impact: "high" | "medium" | "low";
  mitigation: string;
  relatedCategories: string[];
}

export interface AthenaInsights {
  insights: HarmonizationInsight[];
  risks: RiskPrediction[];
  generatedAt: string;
  llmGenerated: boolean;
}

// ─── Cache ───

const CACHE_TTL_MS = 10 * 60 * 1000; // 10 minutes
const cache = new Map<string, { result: AthenaInsights; expiresAt: number }>();

function cacheKey(summary: object): string {
  return crypto.createHash("md5").update(JSON.stringify(summary)).digest("hex");
}

export function clearInsightsCache(): void {
  cache.clear();
  console.log("[athena-insights] Cache cleared");
}

// ─── Summary Builder ───

interface ReportSummary {
  totalIssues: number;
  autoFixRate: number;
  bySeverity: { error: number; warning: number; info: number };
  byCategory: { category: string; count: number; autoFixable: number; topIssues: Pick<ValidationIssue, "message" | "field" | "affectedCount" | "severity">[] }[];
  topIssues: Pick<ValidationIssue, "message" | "field" | "category" | "affectedCount" | "severity" | "suggestion">[];
}

function buildReportSummary(report: AnomalyReport): ReportSummary {
  const byCategory = Object.entries(report.byCategory).map(([cat, info]) => ({
    category: cat,
    count: info.count,
    autoFixable: info.autoFixable,
    topIssues: info.issues
      .sort((a, b) => (b.affectedCount ?? 0) - (a.affectedCount ?? 0))
      .slice(0, 2)
      .map((i) => ({ message: i.message, field: i.field, affectedCount: i.affectedCount, severity: i.severity })),
  }));

  const topIssues = report.topIssues.map((i) => ({
    message: i.message, field: i.field, category: i.category,
    affectedCount: i.affectedCount, severity: i.severity, suggestion: i.suggestion,
  }));

  return {
    totalIssues: report.totalIssues,
    autoFixRate: report.autoFixRate,
    bySeverity: report.bySeverity,
    byCategory,
    topIssues,
  };
}

// ─── LLM Prompt ───

const SYSTEM_PROMPT = `Du bist ein Healthcare-Datenqualitäts-Experte. Du analysierst einen Anomalie-Bericht aus einer Daten-Harmonisierung (Krankenhaus-Daten aus verschiedenen Quellsystemen werden in eine einheitliche SQL-Server-Datenbank importiert).

Erstelle basierend auf dem Bericht:
1. "insights": Genau 3 priorisierte Harmonisierungs-Empfehlungen. Fasse mehrere zusammenhängende Einzelprobleme zu übergreifenden, umsetzbaren Maßnahmen zusammen. Jede Empfehlung soll konkret und praktisch sein.
2. "risks": Genau 3 prognostizierte Risiken. Basierend auf den erkannten Mustern: Was passiert, wenn diese Probleme nicht behoben werden? Welche Downstream-Effekte drohen (z.B. fehlerhafte Auswertungen, Patientenverwechslungen, falsche klinische Entscheidungen)?

WICHTIG:
- Schreibe auf Deutsch
- Sei konkret und beziehe dich auf die Daten im Bericht (Feldnamen, Kategorien, Zahlen)
- Empfehlungen sollen umsetzbar sein (nicht nur "prüfen Sie die Daten")
- Risiken sollen realistisch und klinisch relevant sein

Antworte NUR mit JSON im folgenden Format:
{
  "insights": [{"title":"...","description":"...","recommendation":"...","affectedRows":<number>,"priority":"high"|"medium"|"low","categories":["..."]}],
  "risks": [{"risk":"...","explanation":"...","likelihood":"high"|"medium"|"low","impact":"high"|"medium"|"low","mitigation":"...","relatedCategories":["..."]}]
}`;

// ─── Deterministic Fallback ───

function buildFallbackInsights(report: AnomalyReport): AthenaInsights {
  const insights: HarmonizationInsight[] = [];
  const risks: RiskPrediction[] = [];

  // Group top issues by category for insights
  const catGroups = new Map<string, ValidationIssue[]>();
  for (const issue of report.topIssues) {
    const cat = issue.category || "other";
    if (!catGroups.has(cat)) catGroups.set(cat, []);
    catGroups.get(cat)!.push(issue);
  }

  let idx = 0;
  for (const [cat, issues] of catGroups) {
    const totalAffected = issues.reduce((sum, i) => sum + (i.affectedCount ?? 0), 0);
    const bestSuggestion = issues.find((i) => i.suggestion)?.suggestion;
    insights.push({
      id: `fallback-insight-${idx++}`,
      title: `${issues.length} Problem${issues.length > 1 ? "e" : ""} in Kategorie "${cat}"`,
      description: issues.map((i) => i.message).join(". "),
      recommendation: bestSuggestion || "Prüfen Sie die betroffenen Datensätze und korrigieren Sie die Quellsysteme.",
      affectedRows: totalAffected,
      priority: issues.some((i) => i.severity === "error") ? "high" : "medium",
      categories: [cat],
    });
  }

  // Build risks from error-severity issues
  const errorCats = new Map<string, number>();
  for (const issue of report.allIssues) {
    if (issue.severity === "error") {
      const cat = issue.category || "other";
      errorCats.set(cat, (errorCats.get(cat) || 0) + (issue.affectedCount ?? 1));
    }
  }

  let ridx = 0;
  for (const [cat, affected] of Array.from(errorCats.entries()).sort((a, b) => b[1] - a[1]).slice(0, 5)) {
    risks.push({
      id: `fallback-risk-${ridx++}`,
      risk: `Ungelöste Fehler in "${cat}" betreffen ${affected} Datensätze`,
      explanation: `Es gibt ${affected} fehlerhafte Datensätze in der Kategorie "${cat}", die nicht automatisch korrigiert werden können.`,
      likelihood: affected > 100 ? "high" : affected > 20 ? "medium" : "low",
      impact: cat === "id-format" || cat === "orphan" ? "high" : "medium",
      mitigation: "Betroffene Datensätze manuell prüfen und Quellsystem-Exporte korrigieren.",
      relatedCategories: [cat],
    });
  }

  return { insights: insights.slice(0, 3), risks: risks.slice(0, 3), generatedAt: new Date().toISOString(), llmGenerated: false };
}

// ─── Main Function ───

export async function generateAthenaInsights(report: AnomalyReport): Promise<AthenaInsights> {
  const summary = buildReportSummary(report);
  const key = cacheKey(summary);

  // Check cache
  const cached = cache.get(key);
  if (cached && cached.expiresAt > Date.now()) {
    return cached.result;
  }

  // Check LLM availability
  const available = await llm.isAvailable();
  if (!available) {
    console.log("[athena-insights] LLM unavailable, using fallback");
    return buildFallbackInsights(report);
  }

  // Call LLM
  try {
    console.log("[athena-insights] Generating insights via LLM...");
    const response = await llm.generate(
      `Hier ist der Anomalie-Bericht:\n\n${JSON.stringify(summary, null, 2)}`,
      { system: SYSTEM_PROMPT, format: "json", temperature: 0.3 },
    );

    const parsed = JSON.parse(response);

    // Validate structure
    if (!Array.isArray(parsed.insights) || !Array.isArray(parsed.risks)) {
      throw new Error("Invalid response structure");
    }

    const result: AthenaInsights = {
      insights: parsed.insights.slice(0, 3).map((i: any, idx: number) => ({
        id: `insight-${idx}`,
        title: String(i.title || ""),
        description: String(i.description || ""),
        recommendation: String(i.recommendation || ""),
        affectedRows: Number(i.affectedRows) || 0,
        priority: ["high", "medium", "low"].includes(i.priority) ? i.priority : "medium",
        categories: Array.isArray(i.categories) ? i.categories.map(String) : [],
      })),
      risks: parsed.risks.slice(0, 3).map((r: any, idx: number) => ({
        id: `risk-${idx}`,
        risk: String(r.risk || ""),
        explanation: String(r.explanation || ""),
        likelihood: ["high", "medium", "low"].includes(r.likelihood) ? r.likelihood : "medium",
        impact: ["high", "medium", "low"].includes(r.impact) ? r.impact : "medium",
        mitigation: String(r.mitigation || ""),
        relatedCategories: Array.isArray(r.relatedCategories) ? r.relatedCategories.map(String) : [],
      })),
      generatedAt: new Date().toISOString(),
      llmGenerated: true,
    };

    // Cache
    cache.set(key, { result, expiresAt: Date.now() + CACHE_TTL_MS });
    console.log(`[athena-insights] Generated ${result.insights.length} insights, ${result.risks.length} risks`);
    return result;
  } catch (err) {
    console.error("[athena-insights] LLM generation failed, using fallback:", (err as Error).message);
    return buildFallbackInsights(report);
  }
}
