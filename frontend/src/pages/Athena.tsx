import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import {
  PieChart, Pie, Cell, Tooltip, ResponsiveContainer,
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Legend,
} from "recharts";
import { AlertTriangle, Lightbulb, Shield, CheckCircle, FileWarning, Files, RefreshCw, Loader2 } from "lucide-react";
import { getAthenaData, getAthenaInsights, getQuality, type AthenaData, type AthenaInsights, type QualityData, type ValidationIssue } from "@/lib/api";
import { useI18n } from "@/lib/i18n";

const COLORS = [
  "hsl(168, 45%, 45%)",
  "hsl(12, 70%, 55%)",
  "hsl(45, 80%, 55%)",
  "hsl(220, 50%, 55%)",
  "hsl(280, 40%, 55%)",
  "hsl(340, 50%, 55%)",
  "hsl(90, 40%, 45%)",
];

function EmptyState({ message }: { message: string }) {
  return (
    <div className="flex items-center justify-center py-8 text-sm text-muted-foreground">
      {message}
    </div>
  );
}

const Athena = () => {
  const { t } = useI18n();
  const [data, setData] = useState<AthenaData | null>(null);
  const [quality, setQuality] = useState<QualityData | null>(null);
  const [insights, setInsights] = useState<AthenaInsights | null>(null);
  const [loading, setLoading] = useState(true);
  const [insightsLoading, setInsightsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const CATEGORY_LABELS: Record<string, string> = {
    "source-format": t("cat.source-format"), encoding: t("cat.encoding"), "null-variant": t("cat.null-variant"),
    "date-format": t("cat.date-format"), "id-format": t("cat.id-format"), "out-of-range": t("cat.out-of-range"),
    "flag-drift": t("cat.flag-drift"), duplicate: t("cat.duplicate"), orphan: t("cat.orphan"),
    temporal: t("cat.temporal"), completeness: t("cat.completeness"), "free-text": t("cat.free-text"),
  };

  const SOURCE_LABELS: Record<string, string> = {
    labs: t("src.labs"), nursing: t("src.nursing"), medication: t("src.medication"),
    "icd10-ops": t("src.icd10-ops"), "epaAC-Data-1": t("src.epaAC-Data-1"),
    "device-motion": t("src.device-motion"), "device-1hz": t("src.device-1hz"),
  };

  useEffect(() => {
    // Fetch dashboard data + quality (fast)
    Promise.all([
      getAthenaData().catch(() => null),
      getQuality().catch(() => null),
    ])
      .then(([athena, qual]) => {
        setData(athena);
        setQuality(qual);
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));

    // Fetch LLM insights separately (slow, non-blocking)
    getAthenaInsights()
      .then(setInsights)
      .finally(() => setInsightsLoading(false));
  }, []);

  const handleRegenerate = () => {
    setInsightsLoading(true);
    getAthenaInsights(true)
      .then(setInsights)
      .finally(() => setInsightsLoading(false));
  };

  if (loading) {
    return (
      <div className="p-6">
        <h2 className="text-xl font-semibold text-foreground">Athena</h2>
        <p className="text-sm text-muted-foreground mt-1">{t("athena.loadingData")}</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="p-6">
        <h2 className="text-xl font-semibold text-foreground">Athena</h2>
        <p className="text-sm text-destructive mt-2">{error}</p>
      </div>
    );
  }

  const report = data?.anomalyReport;
  const jobs = data?.jobHistory ?? [];
  const tables = data?.tables ?? [];

  // KPIs: aggregate from all sources (jobs + quality + anomalyReport)
  // Jobs are the primary source since they persist across sessions
  const jobTotalIssues = jobs.reduce((s, j) => s + j.issueCount, 0);
  const jobErrorCount = jobs.reduce((s, j) => s + j.errorCount, 0);
  const jobsWithRate = jobs.filter((j) => j.issueCount > 0);
  const jobAutoFixRate = jobsWithRate.length > 0
    ? jobsWithRate.reduce((s, j) => s + j.autoFixRate, 0) / jobsWithRate.length
    : null;

  // Use the best available data: jobs > quality > anomalyReport
  const totalIssues = jobTotalIssues || quality?.totalAnomalies || report?.totalIssues || 0;
  const errorCount = jobErrorCount || report?.bySeverity.error || 0;
  const autoFixRate = jobAutoFixRate ?? report?.autoFixRate ?? 0;

  // Pie chart: anomalies per uploaded file
  const hasQuality = quality && quality.fileCount > 0;
  const anomalyBySource = hasQuality
    ? quality.recentUploads.map((u) => ({
        name: u.file.replace(/\.[^.]+$/, ""),
        value: u.issues,
      })).filter((s) => s.value > 0)
    : [];

  // Job history bar chart
  const errorsKey = t("athena.chartErrors");
  const issuesKey = t("athena.chartIssues");
  const jobChartData = jobs.map((j) => ({
    name: j.fileName.replace(/\.[^.]+$/, "").slice(0, 20),
    [errorsKey]: j.errorCount,
    [issuesKey]: j.issueCount - j.errorCount,
  }));

  const topIssues = report?.topIssues ?? [];

  // LLM-generated insights and risks (max 3)
  const llmInsights = (insights?.insights ?? []).slice(0, 3);
  const llmRisks = (insights?.risks ?? []).slice(0, 3);

  // Risk assessment: error-severity issues grouped by category
  const errorIssues = report?.allIssues.filter((i) => i.severity === "error") ?? [];
  const risksByCategory = new Map<string, ValidationIssue[]>();
  for (const issue of errorIssues) {
    const cat = issue.category || "other";
    if (!risksByCategory.has(cat)) risksByCategory.set(cat, []);
    risksByCategory.get(cat)!.push(issue);
  }

  return (
  <div className="p-6 space-y-6">
    <h2 className="text-xl font-semibold text-foreground">Athena</h2>
    <p className="text-sm text-muted-foreground -mt-4">{t("athena.subtitle")}</p>

    {/* KPI Row */}
    <div className="grid grid-cols-4 gap-4">
      <Card>
        <CardContent className="pt-4 pb-3">
          <div className="flex items-center gap-2">
            <FileWarning className="h-4 w-4 text-muted-foreground" />
            <span className="text-xs text-muted-foreground">{t("athena.totalIssues")}</span>
          </div>
          <p className="text-2xl font-bold mt-1">{totalIssues}</p>
        </CardContent>
      </Card>
      <Card>
        <CardContent className="pt-4 pb-3">
          <div className="flex items-center gap-2">
            <AlertTriangle className="h-4 w-4 text-destructive" />
            <span className="text-xs text-muted-foreground">{t("athena.errors")}</span>
          </div>
          <p className="text-2xl font-bold mt-1 text-destructive">{errorCount}</p>
        </CardContent>
      </Card>
      <Card>
        <CardContent className="pt-4 pb-3">
          <div className="flex items-center gap-2">
            <CheckCircle className="h-4 w-4 text-primary" />
            <span className="text-xs text-muted-foreground">{t("athena.autoFixRate")}</span>
          </div>
          <p className="text-2xl font-bold mt-1">{autoFixRate.toFixed(0)}%</p>
        </CardContent>
      </Card>
      <Card>
        <CardContent className="pt-4 pb-3">
          <div className="flex items-center gap-2">
            <Files className="h-4 w-4 text-muted-foreground" />
            <span className="text-xs text-muted-foreground">{t("athena.analyzedFiles")}</span>
          </div>
          <p className="text-2xl font-bold mt-1">{quality?.fileCount ?? jobs.length}</p>
        </CardContent>
      </Card>
    </div>

    <div className="grid grid-cols-2 gap-4">
      {/* Anomaly Distribution — pie chart by source (original) */}
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium">{t("athena.anomaliesBySource")}</CardTitle>
        </CardHeader>
        <CardContent>
          {anomalyBySource.length > 0 ? (
            <ResponsiveContainer width="100%" height={250}>
              <PieChart>
                <Pie data={anomalyBySource} cx="50%" cy="50%" innerRadius={60} outerRadius={100} dataKey="value" label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}>
                  {anomalyBySource.map((_, i) => (
                    <Cell key={i} fill={COLORS[i % COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip />
              </PieChart>
            </ResponsiveContainer>
          ) : (
            <EmptyState message={t("athena.noAnomalies")} />
          )}
        </CardContent>
      </Card>

      {/* Error Trend — real job history */}
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium">{t("athena.importHistory")}</CardTitle>
        </CardHeader>
        <CardContent>
          {jobChartData.length > 0 ? (
            <ResponsiveContainer width="100%" height={250}>
              <BarChart data={jobChartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
                <XAxis dataKey="name" tick={{ fontSize: 10 }} angle={-20} textAnchor="end" height={50} />
                <YAxis tick={{ fontSize: 11 }} />
                <Tooltip />
                <Legend />
                <Bar dataKey={errorsKey} stackId="a" fill="hsl(12, 70%, 55%)" />
                <Bar dataKey={issuesKey} stackId="a" fill="hsl(45, 80%, 55%)" />
              </BarChart>
            </ResponsiveContainer>
          ) : (
            <EmptyState message={t("athena.noHistory")} />
          )}
        </CardContent>
      </Card>
    </div>

    {/* Data Harmonization Strategy — LLM insights with fallback to raw top issues */}
    <Card>
      <CardHeader className="pb-2">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Lightbulb className="h-4 w-4 text-primary" />
            <CardTitle className="text-sm font-medium">{t("athena.topIssues")}</CardTitle>
          </div>
          <button
            onClick={handleRegenerate}
            disabled={insightsLoading}
            className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors disabled:opacity-50"
            title={t("athena.regenerateTitle")}
          >
            {insightsLoading ? (
              <Loader2 className="h-3.5 w-3.5 animate-spin" />
            ) : (
              <RefreshCw className="h-3.5 w-3.5" />
            )}
            {insightsLoading ? t("athena.generating") : t("athena.regenerate")}
          </button>
        </div>
      </CardHeader>
      <CardContent className="space-y-3">
        {insightsLoading && llmInsights.length === 0 ? (
          <div className="flex items-center justify-center py-8 gap-2 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" />
            {t("athena.aiAnalysis")}
          </div>
        ) : llmInsights.length > 0 ? (
          llmInsights.map((insight) => (
            <div key={insight.id} className="border border-border rounded-lg p-4">
              <div className="flex items-start justify-between gap-3">
                <div className="flex-1">
                  <p className="text-sm font-medium">{insight.title}</p>
                  <p className="text-xs text-muted-foreground mt-1">{insight.description}</p>
                  <div className="mt-2 bg-sidebar-accent rounded p-3">
                    <p className="text-xs text-primary">↳ {insight.recommendation}</p>
                  </div>
                  {insight.categories.length > 0 && (
                    <div className="flex gap-1 mt-2">
                      {insight.categories.map((cat) => (
                        <Badge key={cat} variant="outline" className="text-xs">
                          {CATEGORY_LABELS[cat] || cat}
                        </Badge>
                      ))}
                    </div>
                  )}
                </div>
                <div className="text-right shrink-0 space-y-1">
                  <Badge variant="outline" className={`text-xs ${
                    insight.priority === "high" ? "text-destructive border-destructive/30" :
                    insight.priority === "medium" ? "text-yellow-600 border-yellow-500/30" :
                    "text-muted-foreground"
                  }`}>
                    {insight.priority === "high" ? t("common.high") : insight.priority === "medium" ? t("common.medium") : t("common.low")}
                  </Badge>
                  {insight.affectedRows > 0 && (
                    <p className="text-xs text-muted-foreground">{insight.affectedRows} {t("common.rows")}</p>
                  )}
                </div>
              </div>
            </div>
          ))
        ) : topIssues.length > 0 ? (
          topIssues.map((issue, i) => (
            <div key={i} className="border border-border rounded-lg p-4">
              <div className="flex items-start justify-between gap-3">
                <div className="flex-1">
                  <div className="flex items-center gap-2 mb-1">
                    <Badge variant="outline" className="text-xs">
                      {CATEGORY_LABELS[issue.category || ""] || issue.category || t("common.other")}
                    </Badge>
                    {issue.field && (
                      <span className="text-xs text-muted-foreground font-mono">{issue.field}</span>
                    )}
                  </div>
                  <p className="text-sm">{issue.message}</p>
                  {issue.suggestion && (
                    <div className="mt-2 bg-sidebar-accent rounded p-3">
                      <p className="text-xs text-primary">↳ {issue.suggestion}</p>
                    </div>
                  )}
                </div>
                <div className="text-right shrink-0 space-y-1">
                  <Badge variant="outline" className={`text-xs ${
                    issue.severity === "error" ? "text-destructive border-destructive/30" :
                    issue.severity === "warning" ? "text-yellow-600 border-yellow-500/30" :
                    "text-muted-foreground"
                  }`}>
                    {issue.severity === "error" ? t("common.error") : issue.severity === "warning" ? t("common.warning") : t("common.info")}
                  </Badge>
                  {(issue.affectedCount ?? 0) > 0 && (
                    <p className="text-xs text-muted-foreground">{issue.affectedCount} {t("common.rows")}</p>
                  )}
                  {issue.autoFix && (
                    <p className="text-xs text-primary">Auto-Fix</p>
                  )}
                </div>
              </div>
            </div>
          ))
        ) : (
          <EmptyState message={t("athena.noIssues")} />
        )}
      </CardContent>
    </Card>

    {/* Predicted Risks — LLM predictions with fallback to error-by-category */}
    <Card>
      <CardHeader className="pb-2">
        <div className="flex items-center gap-2">
          <Shield className="h-4 w-4 text-destructive" />
          <CardTitle className="text-sm font-medium">{t("athena.predictedRisks")}</CardTitle>
        </div>
      </CardHeader>
      <CardContent className="space-y-3">
        {insightsLoading && llmRisks.length === 0 ? (
          <div className="flex items-center justify-center py-8 gap-2 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" />
            {t("athena.aiAnalysis")}
          </div>
        ) : llmRisks.length > 0 ? (
          llmRisks.map((risk) => (
            <div key={risk.id} className="border border-border rounded-lg p-4">
              <div className="flex items-start justify-between gap-3">
                <div className="flex-1">
                  <div className="flex items-center gap-2 mb-1">
                    <AlertTriangle className="h-4 w-4 text-destructive" />
                    <p className="text-sm font-medium">{risk.risk}</p>
                  </div>
                  <p className="text-xs text-muted-foreground ml-6">{risk.explanation}</p>
                  <div className="mt-2 bg-sidebar-accent rounded p-3">
                    <p className="text-xs text-primary">↳ {risk.mitigation}</p>
                  </div>
                  {risk.relatedCategories.length > 0 && (
                    <div className="flex gap-1 mt-2">
                      {risk.relatedCategories.map((cat) => (
                        <Badge key={cat} variant="outline" className="text-xs">
                          {CATEGORY_LABELS[cat] || cat}
                        </Badge>
                      ))}
                    </div>
                  )}
                </div>
                <div className="text-right shrink-0 space-y-1">
                  <Badge variant="outline" className={`text-xs ${
                    risk.likelihood === "high" ? "text-destructive border-destructive/30" :
                    risk.likelihood === "medium" ? "text-yellow-600 border-yellow-500/30" :
                    "text-muted-foreground"
                  }`}>
                    {risk.likelihood === "high" ? t("common.high") : risk.likelihood === "medium" ? t("common.medium") : t("common.low")}
                  </Badge>
                  <p className="text-xs text-muted-foreground">
                    {t("athena.impact")}: {risk.impact === "high" ? t("common.high") : risk.impact === "medium" ? t("common.medium") : t("common.low")}
                  </p>
                </div>
              </div>
            </div>
          ))
        ) : risksByCategory.size > 0 ? (
          Array.from(risksByCategory.entries())
            .sort((a, b) => b[1].length - a[1].length)
            .map(([cat, issues]) => (
              <div key={cat} className="border border-border rounded-lg p-4">
                <div className="flex items-start justify-between gap-3">
                  <div className="flex-1">
                    <div className="flex items-center gap-2 mb-1">
                      <AlertTriangle className="h-4 w-4 text-destructive" />
                      <p className="text-sm font-medium">
                        {CATEGORY_LABELS[cat] || cat}
                      </p>
                    </div>
                    <ul className="ml-6 space-y-1">
                      {issues.slice(0, 3).map((issue, j) => (
                        <li key={j} className="text-xs text-muted-foreground">
                          {issue.field && <span className="font-mono">{issue.field}: </span>}
                          {issue.message}
                          {(issue.affectedCount ?? 0) > 0 && (
                            <span className="text-destructive ml-1">({issue.affectedCount} {t("common.rows")})</span>
                          )}
                        </li>
                      ))}
                      {issues.length > 3 && (
                        <li className="text-xs text-muted-foreground">
                          {t("athena.andMore", { count: issues.length - 3 })}
                        </li>
                      )}
                    </ul>
                  </div>
                  <div className="text-right shrink-0">
                    <Badge variant="outline" className="text-xs text-destructive border-destructive/30">
                      {t("athena.errorsCount", { count: issues.length })}
                    </Badge>
                  </div>
                </div>
              </div>
            ))
        ) : (
          <EmptyState message={t("athena.noCriticalRisks")} />
        )}

        {/* Table completeness */}
        {tables.length > 0 && tables.some((tbl) => tbl.rowCount > 0) && (
          <div className="border-t border-border pt-3 mt-3">
            <p className="text-xs font-medium text-muted-foreground mb-2">{t("athena.tablePopulation")}</p>
            <div className="grid grid-cols-4 gap-2">
              {tables.map((tbl) => (
                <div key={tbl.name} className="flex items-center justify-between text-xs border border-border rounded px-2 py-1">
                  <span className="font-mono truncate">{tbl.name.replace("tbImport", "").replace("Data", "")}</span>
                  <span className={tbl.rowCount === 0 ? "text-muted-foreground" : "text-primary font-medium"}>
                    {tbl.rowCount}
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  </div>
  );
};

export default Athena;
