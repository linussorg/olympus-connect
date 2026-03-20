import { useState, useRef, useEffect, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { FileText, CheckCircle, Link2, Database, Download, DatabaseZap, AlertCircle, ArrowRight, Users, ShieldAlert, Wrench, ChevronDown, ChevronUp, Upload, ArrowLeft, Loader2 } from "lucide-react";
import { Progress } from "@/components/ui/progress";
import { Table, TableHeader, TableBody, TableHead, TableRow, TableCell } from "@/components/ui/table";
import { Anomaly } from "./types";
import AnomalyCard from "./AnomalyCard";
import AnomalyEmailSection from "./AnomalyEmailSection";
import {
  getJobDetail, importJobToDb, downloadJobDump, getJobSqlPreview,
  saveOverride, saveJobResolutions,
  type AnalysisResult, type JobDetail, type AnomalyResolution,
} from "@/lib/api";
import { ensureLoaded, tableLabel, fieldLabel, getTargetFields } from "@/lib/schema-labels";
import { useI18n } from "@/lib/i18n";

function analysisToAnomalies(result: AnalysisResult, t: (key: string, params?: Record<string, string | number>) => string): Anomaly[] {
  const anomalies: Anomaly[] = [];
  let id = 1;
  const mappings = result.mappings || [];
  const unmapped = result.unmapped || [];
  const ambiguous = result.ambiguous || [];
  const issues = result.issues || [];

  for (const m of mappings) {
    if (m.confidence < 0.8) {
      anomalies.push({
        id: id++, type: "mapping", targetTable: result.targetTable, field: m.target,
        sourceColumn: m.source, confidence: m.confidence, currentValue: m.source,
        proposedValue: `${m.target}`,
        reason: t("detail.confidenceUnsure", { pct: (m.confidence * 100).toFixed(0) }),
        status: "pending",
      });
    }
  }

  const emptyColCount = unmapped.filter((c) => !c || !c.trim()).length;
  if (emptyColCount > 0) {
    anomalies.push({
      id: id++, type: "data", targetTable: result.targetTable, field: "(headers)",
      confidence: 0, currentValue: `${emptyColCount} empty`, proposedValue: "",
      reason: t("detail.emptyHeaders", { count: emptyColCount }),
      status: "ignored", category: "source-format",
    });
  }
  const nonEmptyUnmapped = unmapped.filter((c) => c && c.trim());
  if (result.mappingType === "code-translate" && nonEmptyUnmapped.length > 5) {
    // For code-translate files (especially encoded headers), group all unmapped into one entry
    anomalies.push({
      id: id++, type: "data", targetTable: result.targetTable, field: "(headers)",
      confidence: 0, currentValue: `${nonEmptyUnmapped.length} columns`, proposedValue: "",
      reason: t("detail.unmappedBulk", { count: nonEmptyUnmapped.length }),
      status: "ignored", category: "source-format",
    });
  } else {
    for (const col of nonEmptyUnmapped) {
      anomalies.push({
        id: id++, type: "mapping-source", targetTable: result.targetTable, field: "?",
        sourceColumn: col, confidence: 0, currentValue: col, proposedValue: "",
        reason: t("detail.unmappedSource", { col }),
        status: "pending",
      });
    }
  }

  // For code-translate files, unmapped target fields are expected (source has fewer columns than target)
  // Only flag unmapped targets for column-rename where 1:1 matching is expected
  if (result.mappingType !== "code-translate") {
    const mappedTargets = new Set(mappings.map((m) => m.target));
    const schemaFields = getTargetFields(result.targetTable);
    for (const field of schemaFields) {
      if (!mappedTargets.has(field)) {
        anomalies.push({
          id: id++, type: "mapping-target", targetTable: result.targetTable, field,
          confidence: 0, currentValue: "", proposedValue: "",
          reason: t("detail.unmappedTarget", { field }),
          status: "pending",
        });
      }
    }
  }

  for (const amb of ambiguous) {
    const best = amb.candidates[0];
    anomalies.push({
      id: id++, type: "mapping-source", targetTable: result.targetTable,
      field: best?.target || "?", sourceColumn: amb.sourceColumn,
      confidence: best?.confidence || 0, currentValue: amb.sourceColumn,
      proposedValue: best?.target || "", reason: amb.question,
      candidates: amb.candidates.slice(0, 3), status: "pending",
    });
  }

  for (const issue of issues) {
    if (issue.severity === "info") continue;
    anomalies.push({
      id: id++, type: issue.category === "demographic" ? "demographic" : "data",
      targetTable: result.targetTable, field: issue.field,
      currentValue: issue.value || "", proposedValue: issue.suggestion || "",
      reason: issue.message, status: issue.autoFix ? "accepted" : "pending",
      category: issue.category, origin: issue.origin, affectedCount: issue.affectedCount, row: issue.row,
    });
  }

  return anomalies;
}

interface HermesDetailViewProps {
  jobId: string;
}

const HermesDetailView = ({ jobId }: HermesDetailViewProps) => {
  const navigate = useNavigate();
  const { t } = useI18n();

  const [phase, setPhase] = useState<"loading" | "analyzing" | "review" | "importing" | "done" | "failed">("loading");
  const [anomalies, setAnomalies] = useState<Anomaly[]>([]);
  const [uploadedFile, setUploadedFile] = useState("");
  const [filter, setFilter] = useState<"all" | "mapping" | "mapping-source" | "mapping-target" | "data" | "demographic">("all");
  const [reportExpanded, setReportExpanded] = useState(false);
  const [expandedCats, setExpandedCats] = useState<Set<string>>(new Set());
  const [analysis, setAnalysis] = useState<AnalysisResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [progressMsg, setProgressMsg] = useState("");
  const [importResult, setImportResult] = useState<{ inserted: number | null; skipped: number | null } | null>(null);
  const [jobError, setJobError] = useState<string | null>(null);
  const [sqlPreview, setSqlPreview] = useState<{ preview: string; totalLines: number } | null>(null);
  const [sqlPreviewLoading, setSqlPreviewLoading] = useState(false);
  const [dumpLoading, setDumpLoading] = useState(false);
  const [importCount, setImportCount] = useState(0);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const saveTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const initialLoadDone = useRef(false);
  const importingRef = useRef(false);

  useEffect(() => { ensureLoaded(); }, []);

  // Auto-save anomaly resolutions (debounced 1s)
  useEffect(() => {
    if (!jobId || !initialLoadDone.current) return;
    const resolved = anomalies.filter((a) => a.status === "accepted" || a.status === "ignored" || a.status === "manual");
    if (resolved.length === 0) return;

    if (saveTimerRef.current) clearTimeout(saveTimerRef.current);
    saveTimerRef.current = setTimeout(() => {
      const resolutions: AnomalyResolution[] = anomalies
        .filter((a) => a.status !== "pending")
        .map((a) => ({
          anomalyId: a.id,
          status: a.status as AnomalyResolution["status"],
          ...(a.manualValue ? { manualValue: a.manualValue } : {}),
          ...(a.field ? { field: a.field } : {}),
          ...(a.type ? { type: a.type } : {}),
          ...(a.row != null ? { row: a.row } : {}),
        }));
      saveJobResolutions(jobId, resolutions).catch(() => {});
    }, 1000);

    return () => { if (saveTimerRef.current) clearTimeout(saveTimerRef.current); };
  }, [anomalies, jobId]);

  // Polling for active jobs
  const startPolling = useCallback((id: string) => {
    if (pollRef.current) clearInterval(pollRef.current);
    pollRef.current = setInterval(async () => {
      try {
        const job = await getJobDetail(id);
        setProgressMsg(job.progressMsg || "");

        if (job.status === "analyzing" && job.analysis) {
          const partial: AnalysisResult = {
            fileId: id,
            fileName: job.fileName || "",
            format: "",
            detectedType: job.detectedType || "",
            targetTable: job.targetTable || "",
            mappingType: job.mappingType || "column-rename",
            mappings: job.analysis.mappings || [],
            ambiguous: job.analysis.ambiguous || [],
            unmapped: job.analysis.unmapped || [],
            rowCount: job.rowCount || 0,
            columnCount: job.analysis.columnCount,
            preview: job.analysis.preview || [],
            issues: job.analysis.issues || [],
            anomalyReport: job.analysis.anomalyReport,
            confidence: job.confidence || 0,
            needsUserInput: false,
          };
          setAnalysis(partial);
          setAnomalies(analysisToAnomalies(partial, t).filter(a => a.type === "data" || a.type === "demographic"));
        }

        if (job.status === "awaiting_review") {
          clearInterval(pollRef.current!);
          pollRef.current = null;
          if (job.analysis) {
            setAnalysis(job.analysis);
            setAnomalies(analysisToAnomalies(job.analysis, t));
          }
          setPhase("review");
          setSqlPreviewLoading(true); getJobSqlPreview(jobId).then(setSqlPreview).catch(() => {}).finally(() => setSqlPreviewLoading(false));
        } else if (job.status === "done") {
          clearInterval(pollRef.current!);
          pollRef.current = null;
          importingRef.current = false;
          if (job.analysis) {
            setAnalysis(job.analysis);
            setAnomalies(analysisToAnomalies(job.analysis, t));
          }
          setImportResult({ inserted: job.inserted, skipped: job.skipped });
          setImportCount((c) => c + 1);
          setPhase("done");
        } else if (job.status === "failed") {
          clearInterval(pollRef.current!);
          pollRef.current = null;
          importingRef.current = false;
          setJobError(job.error || t("detail.unknownError"));
          if (job.analysis) {
            setAnalysis(job.analysis);
            setAnomalies(analysisToAnomalies(job.analysis, t));
          }
          setPhase("failed");
        }
      } catch {
        // Ignore transient poll errors
      }
    }, 2000);
  }, [t]);

  useEffect(() => {
    return () => { if (pollRef.current) clearInterval(pollRef.current); };
  }, []);

  // Apply saved resolutions on top of generated anomalies
  function applyResolutions(anomalies: Anomaly[], resolutions: AnomalyResolution[] | null): Anomaly[] {
    if (!resolutions || resolutions.length === 0) return anomalies;
    const map = new Map(resolutions.map((r) => [r.anomalyId, r]));
    return anomalies.map((a) => {
      const r = map.get(a.id);
      if (!r) return a;
      return { ...a, status: r.status, ...(r.manualValue != null ? { manualValue: r.manualValue, proposedValue: r.manualValue } : {}) };
    });
  }

  // Load job on mount
  useEffect(() => {
    const loadJob = async () => {
      try {
        const job = await getJobDetail(jobId);
        setUploadedFile(job.fileName || "");

        const setAnalysisWithResolutions = (job: JobDetail) => {
          if (!job.analysis) return;
          setAnalysis(job.analysis);
          const anomalies = analysisToAnomalies(job.analysis, t);
          setAnomalies(applyResolutions(anomalies, job.resolutions));
        };

        switch (job.status) {
          case "uploading":
          case "analyzing":
            setPhase("analyzing");
            setProgressMsg(job.progressMsg || t("detail.analyzingMsg"));
            if (job.analysis) {
              const partial: AnalysisResult = {
                fileId: jobId,
                fileName: job.fileName || "",
                format: "",
                detectedType: job.detectedType || "",
                targetTable: job.targetTable || "",
                mappingType: job.mappingType || "column-rename",
                mappings: job.analysis.mappings || [],
                ambiguous: job.analysis.ambiguous || [],
                unmapped: job.analysis.unmapped || [],
                rowCount: job.rowCount || 0,
                preview: job.analysis.preview || [],
                issues: job.analysis.issues || [],
                anomalyReport: job.analysis.anomalyReport,
                confidence: job.confidence || 0,
                needsUserInput: false,
              };
              setAnalysis(partial);
              setAnomalies(analysisToAnomalies(partial, t).filter(a => a.type === "data" || a.type === "demographic"));
            }
            startPolling(jobId);
            break;
          case "awaiting_review":
            setAnalysisWithResolutions(job);
            setPhase("review");
            setSqlPreviewLoading(true); getJobSqlPreview(jobId).then(setSqlPreview).catch(() => {}).finally(() => setSqlPreviewLoading(false));
            break;
          case "importing":
            setAnalysisWithResolutions(job);
            setPhase("importing");
            setProgressMsg(t("detail.importRunning"));
            startPolling(jobId);
            break;
          case "done":
            setAnalysisWithResolutions(job);
            setImportResult({ inserted: job.inserted, skipped: job.skipped });
            setImportCount(1);
            setPhase("done");
            setSqlPreviewLoading(true); getJobSqlPreview(jobId).then(setSqlPreview).catch(() => {}).finally(() => setSqlPreviewLoading(false));
            break;
          case "failed":
            setAnalysisWithResolutions(job);
            setJobError(job.error || t("detail.unknownError"));
            setPhase("failed");
            break;
        }
        initialLoadDone.current = true;
      } catch (err: any) {
        setError(err.message);
        setPhase("failed");
      }
    };
    loadJob();
  }, [jobId, startPolling]);

  const handleImport = async () => {
    if (importingRef.current) return;
    importingRef.current = true;
    setPhase("importing");
    setImportResult(null);
    setJobError(null);
    setProgressMsg(t("detail.importStarted"));
    try {
      await importJobToDb(jobId);
      startPolling(jobId);
    } catch (err: any) {
      importingRef.current = false;
      setJobError(err.message);
      setPhase("failed");
    }
  };

  const handleDump = async () => {
    setDumpLoading(true);
    try {
      await downloadJobDump(jobId);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setDumpLoading(false);
    }
  };

  const acceptChange = (id: number) => {
    const anomaly = anomalies.find((a) => a.id === id);
    if (anomaly && anomaly.proposedValue) {
      const isMap = anomaly.type === "mapping" || anomaly.type === "mapping-source";
      if (isMap && anomaly.sourceColumn) {
        saveOverride(anomaly.sourceColumn, anomaly.proposedValue, anomaly.targetTable).catch(() => {});
      }
    }
    setAnomalies((prev) => prev.map((a) => a.id === id ? { ...a, status: "accepted" } : a));
  };
  const startManualEdit = (id: number) => {
    setAnomalies((prev) => prev.map((a) => {
      if (a.id !== id) return a;
      const isMap = a.type === "mapping" || a.type === "mapping-source" || a.type === "mapping-target";
      const fields = getTargetFields(a.targetTable);
      const initial = isMap
        ? (a.proposedValue && fields.includes(a.proposedValue) ? a.proposedValue : "")
        : a.currentValue;
      return { ...a, status: "manual", manualValue: initial };
    }));
  };
  const updateManualValue = (id: number, value: string) => {
    setAnomalies((prev) => prev.map((a) => a.id === id ? { ...a, manualValue: value } : a));
  };
  const confirmManual = (id: number) => {
    const anomaly = anomalies.find((a) => a.id === id);
    if (anomaly && anomaly.manualValue) {
      if ((anomaly.type === "mapping" || anomaly.type === "mapping-source") && anomaly.sourceColumn) {
        saveOverride(anomaly.sourceColumn, anomaly.manualValue, anomaly.targetTable).catch(() => {});
      } else if (anomaly.type === "mapping-target") {
        saveOverride(anomaly.manualValue, anomaly.field, anomaly.targetTable).catch(() => {});
      }
    }
    setAnomalies((prev) => prev.map((a) => a.id === id ? { ...a, status: "accepted", proposedValue: a.manualValue || "" } : a));
  };
  const quickSelect = (id: number, value: string) => {
    const anomaly = anomalies.find((a) => a.id === id);
    if (anomaly && (anomaly.type === "mapping" || anomaly.type === "mapping-source") && anomaly.sourceColumn) {
      saveOverride(anomaly.sourceColumn, value, anomaly.targetTable).catch(() => {});
    }
    setAnomalies((prev) => prev.map((a) => a.id === id ? { ...a, status: "accepted", manualValue: value, proposedValue: value } : a));
  };
  const cancelManual = (id: number) => {
    setAnomalies((prev) => prev.map((a) => a.id === id ? { ...a, status: "pending" } : a));
  };
  const ignoreAnomaly = (id: number) => {
    setAnomalies((prev) => prev.map((a) => a.id === id ? { ...a, status: "ignored" } : a));
  };

  const resolvedCount = anomalies.filter((a) => a.status === "accepted" || a.status === "ignored").length;
  const isMapping = (typ: string) => typ === "mapping" || typ === "mapping-source" || typ === "mapping-target";
  const mappingCount = anomalies.filter((a) => isMapping(a.type)).length;
  const sourceCount = anomalies.filter((a) => a.type === "mapping-source").length;
  const targetCount = anomalies.filter((a) => a.type === "mapping-target").length;
  const dataCount = anomalies.filter((a) => a.type === "data").length;
  const demoCount = anomalies.filter((a) => a.type === "demographic").length;
  const statusOrder = (s: string) => s === "pending" || s === "manual" ? 0 : s === "accepted" ? 1 : 2;
  const filtered = (filter === "all" ? anomalies
    : filter === "mapping" ? anomalies.filter((a) => isMapping(a.type))
    : anomalies.filter((a) => a.type === filter)
  ).slice().sort((a, b) => statusOrder(a.status) - statusOrder(b.status));

  const showReview = phase === "analyzing" || phase === "review" || phase === "importing" || phase === "done" || phase === "failed";

  if (phase === "loading") {
    return (
      <div className="p-6 space-y-6">
        <div className="flex items-center gap-3">
          <div className="animate-spin h-4 w-4 border-2 border-primary border-t-transparent rounded-full" />
          <span className="text-sm text-muted-foreground">{t("detail.loadingJob")}</span>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {error && (
        <Card className="border-destructive/30 bg-destructive/5">
          <CardContent className="pt-4 pb-3 flex items-center gap-2 text-destructive">
            <AlertCircle className="h-4 w-4" />
            <span className="text-sm">{error}</span>
          </CardContent>
        </Card>
      )}

      {showReview && (
        <>
          {/* Status banners */}
          {phase === "analyzing" && (
            <Card className="border-blue-500/30 bg-blue-500/5">
              <CardContent className="pt-4 pb-3 flex items-center gap-3">
                <div className="animate-spin h-4 w-4 border-2 border-blue-500 border-t-transparent rounded-full" />
                <span className="text-sm font-medium text-blue-600">{progressMsg || t("detail.analysisRunning")}</span>
                <span className="text-xs text-muted-foreground ml-auto">{t("detail.canLeavePage")}</span>
              </CardContent>
            </Card>
          )}
          {/* importing/done/failed banners moved into the export card below */}

          {/* Summary Bar */}
          <div className="flex items-center justify-between flex-wrap gap-3">
            <div className="flex items-center gap-4 flex-wrap">
              <div className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-muted">
                <FileText className="h-4 w-4 text-primary" />
                <span className="text-sm font-medium">{uploadedFile}</span>
              </div>
              {analysis && analysis.rowCount != null && (
                <span className="text-sm text-muted-foreground">
                  {analysis.rowCount} {t("common.rows")} · {tableLabel(analysis.targetTable)} · {analysis.mappings.length}/{analysis.columnCount ?? analysis.mappings.length + (analysis.unmapped?.length || 0)} {t("detail.columnsMappedShort")} · {t("hermes.confidence")} {((analysis.confidence ?? 0) * 100).toFixed(0)}%
                </span>
              )}
              <span className="text-sm text-muted-foreground">
                {anomalies.length} {t("detail.anomalies")} · {resolvedCount}/{anomalies.length} {t("detail.resolved")}
              </span>
            </div>
            <Button variant="outline" size="sm" onClick={() => navigate("/hermes")}>
              <ArrowLeft className="h-3.5 w-3.5 mr-1.5" />
              {t("detail.backToOverview")}
            </Button>
          </div>

          {/* Anomaly Report */}
          {analysis?.anomalyReport && (() => {
            const r = analysis.anomalyReport;
            const catLabels: Record<string, string> = {
              "source-format": t("cat.source-format"), encoding: t("cat.encoding"), "null-variant": t("cat.null-variant"),
              "date-format": t("cat.date-format"), "id-format": t("cat.id-format"), "out-of-range": t("cat.out-of-range"),
              "flag-drift": t("cat.flag-drift"), duplicate: t("cat.duplicate"), orphan: t("cat.orphan"),
              temporal: t("cat.temporal"), completeness: t("cat.completeness"), "free-text": t("cat.free-text"), demographic: t("cat.demographic"),
            };
            const sevClass = (s: string) =>
              s === "error" ? "bg-destructive/10 text-destructive border-destructive/20" :
              s === "warning" ? "bg-yellow-500/10 text-yellow-600 border-yellow-500/20" :
              "bg-blue-500/10 text-blue-600 border-blue-500/20";
            const sevLabel = (s: string) => s === "error" ? t("common.error") : s === "warning" ? t("common.warning") : t("common.info");
            const categories = Object.entries(r.byCategory || {}).filter(([, v]) => v && v.count > 0)
              .sort(([, a], [, b]) => b!.count - a!.count);
            const toggleCat = (cat: string) => setExpandedCats((prev) => {
              const next = new Set(prev);
              if (next.has(cat)) next.delete(cat); else next.add(cat);
              return next;
            });
            return (
              <Card>
                <CardHeader className="pb-2">
                  <div className="flex items-center justify-between">
                    <CardTitle className="text-sm font-medium flex items-center gap-2">
                      <ShieldAlert className="h-4 w-4 text-primary" />
                      {t("detail.anomalyReport")}
                    </CardTitle>
                    <Button variant="ghost" size="sm" className="h-7 text-xs gap-1" onClick={() => setReportExpanded(!reportExpanded)}>
                      {reportExpanded ? <ChevronUp className="h-3.5 w-3.5" /> : <ChevronDown className="h-3.5 w-3.5" />}
                      {reportExpanded ? t("detail.summary") : t("detail.allDetails")}
                    </Button>
                  </div>
                </CardHeader>
                <CardContent className="space-y-3">
                  {/* Severity badges + auto-fix bar */}
                  <div className="flex items-center gap-4 flex-wrap">
                    <span className="text-xs px-2 py-0.5 rounded bg-destructive/10 text-destructive font-medium">{r.bySeverity.error} {t("common.error")}</span>
                    <span className="text-xs px-2 py-0.5 rounded bg-yellow-500/10 text-yellow-600 font-medium">{r.bySeverity.warning} {t("detail.warnings")}</span>
                    <span className="text-xs px-2 py-0.5 rounded bg-blue-500/10 text-blue-600 font-medium">{r.bySeverity.info} {t("common.info")}</span>
                    <span className="text-xs text-muted-foreground">{r.totalIssues} {t("common.total")}</span>
                  </div>
                  <div className="flex items-center gap-3">
                    <Wrench className="h-3.5 w-3.5 text-primary shrink-0" />
                    <div className="flex-1">
                      <Progress value={r.autoFixRate * 100} className="h-2" />
                    </div>
                    <span className="text-xs font-medium text-primary shrink-0">{Math.round(r.autoFixRate * 100)}% {t("detail.autoCorrect")}</span>
                  </div>

                  {!reportExpanded ? (
                    <>
                      {categories.length > 0 && (
                        <div className="flex gap-2 flex-wrap">
                          {categories.map(([cat, stats]) => (
                            <span key={cat} className="text-[10px] px-2 py-0.5 rounded bg-muted text-muted-foreground">
                              {catLabels[cat] || cat}: {stats!.count}
                              {stats!.autoFixable > 0 && <span className="text-primary ml-1">({stats!.autoFixable} fix)</span>}
                            </span>
                          ))}
                        </div>
                      )}
                      {r.topIssues && r.topIssues.length > 0 && (
                        <div className="space-y-1.5">
                          <p className="text-xs font-medium text-muted-foreground">{t("detail.topIssues")}</p>
                          {r.topIssues.slice(0, 5).map((issue, i) => (
                            <div key={i} className="text-xs pl-3 border-l-2 border-primary/30">
                              <span className="font-medium">{issue.field}</span>: {issue.message}
                              {issue.affectedCount != null && issue.affectedCount > 1 && (
                                <span className="text-muted-foreground ml-1">({issue.affectedCount} {t("common.rows")})</span>
                              )}
                              {issue.origin && <p className="text-muted-foreground mt-0.5">{issue.origin}</p>}
                            </div>
                          ))}
                        </div>
                      )}
                    </>
                  ) : (
                    <>
                      <div className="space-y-2 max-h-[600px] overflow-y-auto pr-1">
                        {categories.map(([cat, stats]) => {
                          const isOpen = expandedCats.has(cat);
                          const issues = stats!.issues || [];
                          return (
                            <div key={cat} className="border rounded-lg overflow-hidden">
                              <button
                                className="w-full flex items-center justify-between px-3 py-2 text-left hover:bg-muted/50 transition-colors"
                                onClick={() => toggleCat(cat)}
                              >
                                <div className="flex items-center gap-2">
                                  <span className="text-xs font-medium">{catLabels[cat] || cat}</span>
                                  <span className="text-[10px] px-1.5 py-0.5 rounded bg-muted text-muted-foreground">{stats!.count}</span>
                                  {stats!.autoFixable > 0 && (
                                    <span className="text-[10px] px-1.5 py-0.5 rounded bg-primary/10 text-primary">{stats!.autoFixable} auto-fix</span>
                                  )}
                                </div>
                                {isOpen ? <ChevronUp className="h-3.5 w-3.5 text-muted-foreground" /> : <ChevronDown className="h-3.5 w-3.5 text-muted-foreground" />}
                              </button>
                              {isOpen && (
                                <div className="border-t px-3 py-2 space-y-2">
                                  {issues.map((issue, i) => (
                                    <div key={i} className={`text-xs rounded px-2.5 py-1.5 border ${sevClass(issue.severity)}`}>
                                      <div className="flex items-start gap-2">
                                        <span className={`text-[9px] px-1 py-0.5 rounded font-medium shrink-0 ${sevClass(issue.severity)}`}>
                                          {sevLabel(issue.severity)}
                                        </span>
                                        <div className="flex-1 min-w-0">
                                          <span className="font-medium">{issue.field}</span>
                                          {issue.value && <span className="font-mono text-muted-foreground ml-1">= {issue.value}</span>}
                                          {issue.row != null && <span className="text-muted-foreground ml-1">({t("detail.row")} {issue.row})</span>}
                                          <p className="mt-0.5">{issue.message}</p>
                                          {issue.suggestion && (
                                            <p className="mt-0.5 text-primary">{t("detail.suggestion")} {issue.suggestion}</p>
                                          )}
                                          {issue.origin && (
                                            <p className="mt-0.5 text-muted-foreground italic">{issue.origin}</p>
                                          )}
                                          <div className="flex items-center gap-2 mt-0.5">
                                            {issue.affectedCount != null && issue.affectedCount > 1 && (
                                              <span className="text-muted-foreground">{issue.affectedCount} {t("detail.rowsAffected")}</span>
                                            )}
                                            {issue.autoFix && (
                                              <span className="text-primary font-medium">{t("detail.autoCorrect")}</span>
                                            )}
                                          </div>
                                        </div>
                                      </div>
                                    </div>
                                  ))}
                                </div>
                              )}
                            </div>
                          );
                        })}
                      </div>
                    </>
                  )}
                </CardContent>
              </Card>
            );
          })()}

          {/* Filter Tabs */}
          {anomalies.length > 0 && <div className="flex gap-2 flex-wrap">
            <Button size="sm" variant={filter === "all" ? "default" : "outline"} onClick={() => setFilter("all")}>
              {t("detail.filterAll")} ({anomalies.length})
            </Button>
            <Button size="sm" variant={filter === "mapping" ? "default" : "outline"} onClick={() => setFilter("mapping")}>
              <Link2 className="h-3.5 w-3.5 mr-1.5" />
              {t("detail.filterMapping")} ({mappingCount})
            </Button>
            {sourceCount > 0 && (
              <Button size="sm" variant={filter === "mapping-source" ? "default" : "outline"} onClick={() => setFilter("mapping-source")}>
                <Upload className="h-3.5 w-3.5 mr-1.5" />
                {t("detail.filterSource")} ({sourceCount})
              </Button>
            )}
            {targetCount > 0 && (
              <Button size="sm" variant={filter === "mapping-target" ? "default" : "outline"} onClick={() => setFilter("mapping-target")}>
                <Database className="h-3.5 w-3.5 mr-1.5" />
                {t("detail.filterTarget")} ({targetCount})
              </Button>
            )}
            <Button size="sm" variant={filter === "data" ? "default" : "outline"} onClick={() => setFilter("data")}>
              <AlertCircle className="h-3.5 w-3.5 mr-1.5" />
              {t("detail.filterData")} ({dataCount})
            </Button>
            {demoCount > 0 && (
              <Button size="sm" variant={filter === "demographic" ? "default" : "outline"} onClick={() => setFilter("demographic")}>
                <Users className="h-3.5 w-3.5 mr-1.5" />
                {t("detail.filterDemographic")} ({demoCount})
              </Button>
            )}
          </div>}

          {/* Anomaly List */}
          {anomalies.length > 0 ? (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium">
                  {filter === "mapping" ? t("detail.mappingAnomalies") : filter === "data" ? t("detail.dataAnomalies") : t("detail.detectedAnomalies")}
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="max-h-[520px] overflow-y-auto space-y-3 pr-1">
                  {filtered.map((a) => (
                    <AnomalyCard
                      key={a.id}
                      anomaly={a}
                      unmappedSourceColumns={analysis?.unmapped}
                      onAccept={acceptChange}
                      onStartManual={startManualEdit}
                      onUpdateManual={updateManualValue}
                      onConfirmManual={confirmManual}
                      onCancelManual={cancelManual}
                      onQuickSelect={quickSelect}
                      onIgnore={ignoreAnomaly}
                    />
                  ))}
                </div>
              </CardContent>
            </Card>
          ) : (
            <div className="flex items-center gap-2 px-3 py-2 rounded-lg border border-primary/30 bg-primary/5">
              <CheckCircle className="h-4 w-4 text-primary shrink-0" />
              <span className="text-sm font-medium text-foreground">{t("detail.noAnomalies")}</span>
              <span className="text-xs text-muted-foreground">{t("detail.allHighConfidence")}</span>
            </div>
          )}

          {/* Anomaly Email Section */}
          {(phase === "review" || phase === "done" || phase === "failed") && anomalies.length > 0 && (
            <AnomalyEmailSection analysis={analysis} anomalies={anomalies} />
          )}

          {/* Mapping Table */}
          {analysis && (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium flex items-center gap-2">
                  <Link2 className="h-4 w-4 text-primary" />
                  {t("detail.columnMapping")}
                  {(analysis.mappings?.length ?? 0) > 0 && (
                    <span className="text-xs text-muted-foreground font-normal">
                      {t("detail.columnsMapped", { mapped: analysis.mappings.length, total: analysis.mappings.length + (analysis.unmapped?.length || 0) })}
                    </span>
                  )}
                </CardTitle>
              </CardHeader>
              <CardContent>
                {(analysis.mappings?.length ?? 0) > 0 ? (
                  <>
                    <div className="max-h-[300px] overflow-auto">
                      <Table>
                        <TableHeader>
                          <TableRow>
                            <TableHead className="h-8 text-xs">{t("detail.source")}</TableHead>
                            <TableHead className="h-8 text-xs w-8"></TableHead>
                            <TableHead className="h-8 text-xs">{t("detail.target")}</TableHead>
                            <TableHead className="h-8 text-xs">{t("detail.description")}</TableHead>
                            <TableHead className="h-8 text-xs text-right">{t("hermes.confidence")}</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {analysis.mappings.map((m, i) => (
                            <TableRow key={i}>
                              <TableCell className="py-1.5 px-4 font-mono text-xs">{m.source}</TableCell>
                              <TableCell className="py-1.5 px-1 text-muted-foreground"><ArrowRight className="h-3 w-3" /></TableCell>
                              <TableCell className="py-1.5 px-4 font-mono text-xs font-medium">{m.target}</TableCell>
                              <TableCell className="py-1.5 px-4 text-xs text-muted-foreground">{fieldLabel(analysis.targetTable, m.target)}</TableCell>
                              <TableCell className="py-1.5 px-4 text-right">
                                <span className={`text-[10px] px-1.5 py-0.5 rounded font-mono ${
                                  m.confidence >= 0.8 ? "bg-primary/10 text-primary" :
                                  m.confidence >= 0.5 ? "bg-yellow-500/10 text-yellow-600" :
                                  "bg-destructive/10 text-destructive"
                                }`}>
                                  {Math.round(m.confidence * 100)}%
                                </span>
                              </TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </div>
                    {analysis.unmapped?.length > 0 && (
                      <p className="text-xs text-muted-foreground mt-2">
                        {t("detail.unmapped")} {analysis.unmapped.join(", ")}
                      </p>
                    )}
                  </>
                ) : (
                  <p className="text-sm text-muted-foreground">{t("detail.noMappings")}</p>
                )}
              </CardContent>
            </Card>
          )}

          {/* Data Preview */}
          {analysis && (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium flex items-center gap-2">
                  <Database className="h-4 w-4 text-primary" />
                  {t("detail.dataPreview")}
                  {(analysis.preview?.length ?? 0) > 0 && (
                    <span className="text-xs text-muted-foreground font-normal">
                      {t("detail.ofRows", { shown: analysis.preview.length, total: analysis.rowCount })}
                    </span>
                  )}
                </CardTitle>
              </CardHeader>
              <CardContent>
                {(analysis.preview?.length ?? 0) > 0 ? (() => {
                  const cols = Object.keys(analysis.preview[0]);
                  return (
                    <div className="overflow-auto max-h-[260px]">
                      <Table>
                        <TableHeader>
                          <TableRow>
                            {cols.map((col) => (
                              <TableHead key={col} className="h-8 text-xs whitespace-nowrap" title={col}>{fieldLabel(analysis.targetTable, col)}</TableHead>
                            ))}
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {analysis.preview.map((row, ri) => (
                            <TableRow key={ri}>
                              {cols.map((col) => (
                                <TableCell key={col} className="py-1.5 px-4 text-xs whitespace-nowrap">
                                  {row[col] != null ? (
                                    <span className="font-mono">{row[col]}</span>
                                  ) : (
                                    <span className="text-muted-foreground">&mdash;</span>
                                  )}
                                </TableCell>
                              ))}
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </div>
                  );
                })() : (
                  <p className="text-sm text-muted-foreground">{t("detail.noPreview")}</p>
                )}
              </CardContent>
            </Card>
          )}

          {/* Export & Import Card — visible in review, importing, done, and failed phases */}
          {(phase === "review" || phase === "importing" || phase === "done" || phase === "failed") && (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium flex items-center gap-2">
                  <DatabaseZap className="h-4 w-4 text-primary" />
                  {t("detail.exportData")}
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                {/* SQL Preview */}
                {sqlPreviewLoading && !sqlPreview && (
                  <div className="flex items-center gap-2 py-4 justify-center text-sm text-muted-foreground">
                    <Loader2 className="h-4 w-4 animate-spin" />
                    {t("common.loading")}
                  </div>
                )}
                {sqlPreview && (
                  <div>
                    <p className="text-xs text-muted-foreground mb-1.5">{t("detail.sqlPreview", { lines: sqlPreview.totalLines })}</p>
                    <pre className="text-[11px] font-mono bg-muted/50 border rounded-lg p-3 overflow-x-auto max-h-[180px] overflow-y-auto whitespace-pre text-muted-foreground leading-relaxed">
                      {sqlPreview.preview}
                      {sqlPreview.totalLines > 12 && (
                        <span className="text-primary/60">{"\n"}{t("detail.moreLines", { count: sqlPreview.totalLines - 12 })}</span>
                      )}
                    </pre>
                  </div>
                )}

                {/* Import status */}
                {phase === "importing" && (
                  <div className="flex items-center gap-3 px-3 py-2.5 rounded-lg border border-blue-500/30 bg-blue-500/5">
                    <div className="animate-spin h-4 w-4 border-2 border-blue-500 border-t-transparent rounded-full shrink-0" />
                    <span className="text-sm font-medium text-blue-600">
                      {t("detail.importRunning")}
                    </span>
                  </div>
                )}

                {phase === "done" && importResult && (
                  <div className="flex items-center gap-2 px-3 py-2.5 rounded-lg border border-primary/30 bg-primary/5">
                    <CheckCircle className="h-4 w-4 text-primary shrink-0" />
                    <p className="text-sm font-medium">
                      {t("detail.rowsInserted", { inserted: importResult.inserted ?? 0, skipped: importResult.skipped ?? 0 })}
                    </p>
                  </div>
                )}

                {phase === "failed" && jobError && (
                  <div className="flex items-center gap-2 px-3 py-2.5 rounded-lg border border-destructive/30 bg-destructive/5">
                    <AlertCircle className="h-4 w-4 text-destructive shrink-0" />
                    <span className="text-sm text-destructive">{jobError}</span>
                  </div>
                )}

                {/* Action buttons */}
                <div className="flex justify-center gap-3">
                  <Button variant="outline" size="default" onClick={handleDump} disabled={dumpLoading}>
                    {dumpLoading ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Download className="h-4 w-4 mr-2" />}
                    {t("detail.downloadSql")}
                  </Button>
                  <Button
                    size="default"
                    onClick={handleImport}
                    disabled={phase === "importing"}
                  >
                    <DatabaseZap className="h-4 w-4 mr-2" />
                    {importCount > 0 ? t("detail.updateDb") : t("detail.insertDb")}
                  </Button>
                </div>
              </CardContent>
            </Card>
          )}

        </>
      )}
    </div>
  );
};

export default HermesDetailView;
