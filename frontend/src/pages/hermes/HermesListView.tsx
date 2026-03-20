import { useState, useRef, useEffect, useCallback } from "react";
import { useSearchParams } from "react-router-dom";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Table, TableHeader, TableBody, TableHead, TableRow, TableCell } from "@/components/ui/table";
import { Loader2, Trash2, ExternalLink, RefreshCw, AlertCircle, CheckCircle, Clock, Upload, Check } from "lucide-react";
import { Progress } from "@/components/ui/progress";
import { listJobSummaries, deleteJobById, createJobUpload, type JobSummary, type JobStatus } from "@/lib/api";
import { tableLabel } from "@/lib/schema-labels";
import { useI18n } from "@/lib/i18n";

const HermesListView = () => {
  const { t, locale } = useI18n();

  const statusBadge = (status: JobStatus) => {
    switch (status) {
      case "uploading":
      case "analyzing":
        return (
          <span className="inline-flex items-center gap-1.5 text-xs px-2 py-0.5 rounded bg-blue-500/10 text-blue-600 font-medium">
            <Loader2 className="h-3 w-3 animate-spin" />
            {status === "uploading" ? t("hermes.uploading") : t("hermes.analyzing")}
          </span>
        );
      case "awaiting_review":
        return (
          <span className="inline-flex items-center gap-1.5 text-xs px-2 py-0.5 rounded bg-yellow-500/10 text-yellow-600 font-medium">
            <Clock className="h-3 w-3" />
            {t("hermes.awaitingReview")}
          </span>
        );
      case "importing":
        return (
          <span className="inline-flex items-center gap-1.5 text-xs px-2 py-0.5 rounded bg-blue-500/10 text-blue-600 font-medium">
            <Loader2 className="h-3 w-3 animate-spin" />
            {t("hermes.importing")}
          </span>
        );
      case "done":
        return (
          <span className="inline-flex items-center gap-1.5 text-xs px-2 py-0.5 rounded bg-primary/10 text-primary font-medium">
            <CheckCircle className="h-3 w-3" />
            {t("hermes.done")}
          </span>
        );
      case "failed":
        return (
          <span className="inline-flex items-center gap-1.5 text-xs px-2 py-0.5 rounded bg-destructive/10 text-destructive font-medium">
            <AlertCircle className="h-3 w-3" />
            {t("hermes.failed")}
          </span>
        );
    }
  };

  const formatDate = (iso: string) => {
    try {
      return new Date(iso).toLocaleString(locale === "de" ? "de-DE" : "en-US", { day: "2-digit", month: "2-digit", year: "numeric", hour: "2-digit", minute: "2-digit" });
    } catch {
      return iso;
    }
  };
  const [, setSearchParams] = useSearchParams();
  const [jobs, setJobs] = useState<JobSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [dragOver, setDragOver] = useState(false);
  const [uploadProgress, setUploadProgress] = useState<{
    active: boolean;
    total: number;
    completed: number;
    failed: number;
  } | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const fetchJobs = useCallback(async () => {
    try {
      const data = await listJobSummaries();
      setJobs(data);
      setError(null);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchJobs();
    const hasActive = () => jobs.some((j) => j.status === "analyzing" || j.status === "importing" || j.status === "uploading");
    const interval = setInterval(fetchJobs, hasActive() ? 2000 : 5000);
    return () => clearInterval(interval);
  }, [fetchJobs, jobs]);

  const handleFiles = async (files: File[]) => {
    if (files.length === 0) return;
    setError(null);
    const state = { active: true, total: files.length, completed: 0, failed: 0 };
    setUploadProgress({ ...state });

    const CONCURRENCY = 3;
    const queue = [...files];

    const next = async (): Promise<void> => {
      const file = queue.shift();
      if (!file) return;
      try {
        await createJobUpload(file);
      } catch {
        state.failed++;
      }
      state.completed++;
      setUploadProgress({ ...state });
      await next();
    };

    await Promise.all(
      Array.from({ length: Math.min(CONCURRENCY, files.length) }, () => next())
    );

    setUploadProgress({ active: false, total: state.total, completed: state.completed, failed: state.failed });
    fetchJobs();
    setTimeout(() => setUploadProgress(null), 3000);
  };

  const onDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(false);
    const files = Array.from(e.dataTransfer.files);
    if (files.length > 0) handleFiles(files);
  };

  const onFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = e.target.files ? Array.from(e.target.files) : [];
    if (files.length > 0) handleFiles(files);
    if (fileInputRef.current) fileInputRef.current.value = "";
  };

  const handleDelete = async (jobId: string) => {
    try {
      await deleteJobById(jobId);
      setJobs((prev) => prev.filter((j) => j.jobId !== jobId));
    } catch (err: any) {
      setError(err.message);
    }
  };

  const handleOpen = (job: JobSummary) => {
    setSearchParams({ jobId: job.jobId }, { replace: true });
  };

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

      {/* Drop Zone */}
      <Card>
        <CardContent className="pt-6">
          <input ref={fileInputRef} type="file" className="hidden" onChange={onFileSelect} accept=".csv,.txt,.xlsx,.xls,.pdf" multiple />
          <div
            onDragOver={(e) => { e.preventDefault(); setDragOver(true); }}
            onDragLeave={() => setDragOver(false)}
            onDrop={onDrop}
            onClick={() => fileInputRef.current?.click()}
            className={`border-2 border-dashed rounded-lg p-12 text-center cursor-pointer transition-colors ${
              uploadProgress?.active ? "border-blue-500 bg-blue-500/5 pointer-events-none" :
              dragOver ? "border-primary bg-primary/5" : "border-border hover:border-primary/50"
            }`}
          >
            {uploadProgress?.active ? (
              <div className="space-y-3">
                <div className="animate-spin h-10 w-10 border-2 border-blue-500 border-t-transparent rounded-full mx-auto" />
                <p className="text-sm text-blue-600 font-medium">
                  {t("hermes.uploadingFiles", { completed: uploadProgress.completed, total: uploadProgress.total })}
                </p>
                <Progress value={(uploadProgress.completed / uploadProgress.total) * 100} className="max-w-xs mx-auto" />
                {uploadProgress.failed > 0 && (
                  <p className="text-xs text-destructive">{t("hermes.uploadFailed", { count: uploadProgress.failed })}</p>
                )}
              </div>
            ) : uploadProgress && !uploadProgress.active ? (
              <div className="space-y-1">
                <Check className="h-10 w-10 mx-auto mb-2 text-primary" />
                <p className="text-sm text-primary font-medium">
                  {t("hermes.filesUploaded", { count: uploadProgress.completed - uploadProgress.failed, plural: uploadProgress.completed - uploadProgress.failed !== 1 ? (locale === "de" ? "en" : "s") : "" })}
                </p>
                {uploadProgress.failed > 0 && (
                  <p className="text-xs text-destructive">{t("hermes.uploadFailed", { count: uploadProgress.failed })}</p>
                )}
              </div>
            ) : (
              <>
                <Upload className="h-10 w-10 mx-auto mb-3 text-muted-foreground" />
                <p className="text-base text-foreground font-medium">{t("hermes.dragFiles")}</p>
                <p className="text-sm text-muted-foreground mt-1">{t("hermes.allFormats")}</p>
              </>
            )}
          </div>
        </CardContent>
      </Card>

      {/* Job List */}
      <Card>
        <CardHeader className="pb-2">
          <div className="flex items-center justify-between">
            <CardTitle className="text-sm font-medium">
              {jobs.length > 0 ? t("hermes.importJobs", { count: jobs.length }) : t("hermes.noJobs")}
            </CardTitle>
            <Button variant="outline" size="sm" onClick={fetchJobs} disabled={loading}>
              <RefreshCw className={`h-3.5 w-3.5 mr-1.5 ${loading ? "animate-spin" : ""}`} />
              {t("hermes.refresh")}
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          {jobs.length === 0 && !loading ? (
            <div className="text-center py-8">
              <p className="text-sm text-muted-foreground">{t("hermes.noJobsYet")}</p>
            </div>
          ) : (
            <div className="overflow-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="h-8 text-xs">{t("hermes.file")}</TableHead>
                    <TableHead className="h-8 text-xs">{t("hermes.type")}</TableHead>
                    <TableHead className="h-8 text-xs">{t("hermes.targetTable")}</TableHead>
                    <TableHead className="h-8 text-xs text-right">{t("common.rows")}</TableHead>
                    <TableHead className="h-8 text-xs text-right">{t("hermes.confidence")}</TableHead>
                    <TableHead className="h-8 text-xs">{t("hermes.status")}</TableHead>
                    <TableHead className="h-8 text-xs">{t("hermes.created")}</TableHead>
                    <TableHead className="h-8 text-xs text-right">{t("hermes.actions")}</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {jobs.map((job) => (
                    <TableRow key={job.jobId} className="cursor-pointer hover:bg-muted/50" onClick={() => handleOpen(job)}>
                      <TableCell className="py-2 px-4 text-sm font-medium max-w-[200px] truncate">{job.fileName || "—"}</TableCell>
                      <TableCell className="py-2 px-4 text-xs text-muted-foreground">{job.detectedType || "—"}</TableCell>
                      <TableCell className="py-2 px-4 text-xs text-muted-foreground">{job.targetTable ? tableLabel(job.targetTable) : "—"}</TableCell>
                      <TableCell className="py-2 px-4 text-xs text-right">{job.rowCount?.toLocaleString(locale === "de" ? "de-DE" : "en-US") ?? "—"}</TableCell>
                      <TableCell className="py-2 px-4 text-right">
                        {job.confidence != null ? (
                          <span className={`text-[10px] px-1.5 py-0.5 rounded font-mono ${
                            job.confidence >= 0.8 ? "bg-primary/10 text-primary" :
                            job.confidence >= 0.5 ? "bg-yellow-500/10 text-yellow-600" :
                            "bg-destructive/10 text-destructive"
                          }`}>
                            {Math.round(job.confidence * 100)}%
                          </span>
                        ) : "—"}
                      </TableCell>
                      <TableCell className="py-2 px-4">{statusBadge(job.status)}</TableCell>
                      <TableCell className="py-2 px-4 text-xs text-muted-foreground">{formatDate(job.createdAt)}</TableCell>
                      <TableCell className="py-2 px-4 text-right">
                        <div className="flex items-center justify-end gap-1" onClick={(e) => e.stopPropagation()}>
                          {(job.status === "awaiting_review" || job.status === "done" || job.status === "failed") && (
                            <Button variant="ghost" size="sm" className="h-7 px-2" onClick={() => handleOpen(job)}>
                              <ExternalLink className="h-3.5 w-3.5" />
                            </Button>
                          )}
                          <Button variant="ghost" size="sm" className="h-7 px-2 text-destructive hover:text-destructive" onClick={() => handleDelete(job.jobId)}>
                            <Trash2 className="h-3.5 w-3.5" />
                          </Button>
                        </div>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
};

export default HermesListView;
