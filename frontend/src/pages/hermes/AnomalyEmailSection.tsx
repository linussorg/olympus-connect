import { useState, useMemo } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Send, AlertTriangle, Mail, Loader2 } from "lucide-react";
import { sendHermesEmail, type AnalysisResult } from "@/lib/api";
import type { Anomaly } from "./types";
import { tableLabel, fieldLabel } from "@/lib/schema-labels";
import { useI18n } from "@/lib/i18n";

interface AnomalyFlag {
  id: number;
  field: string;
  targetTable: string;
  category: string;
  currentValue: string;
  proposedValue: string;
  reason: string;
  origin?: string;
  affectedCount?: number;
  contactEmail: string;
  draftMessage: string;
  sent: boolean;
}

function getCategoryLabel(t: (key: string, params?: Record<string, string | number>) => string, category: string | undefined): string {
  if (!category) return t("email.dataQualityIssue");
  const key = `ecat.${category}`;
  const val = t(key);
  return val !== key ? val : (category || t("email.dataQualityIssue"));
}

function generateDraftMessage(a: Anomaly, fileName: string, t: (key: string, params?: Record<string, string | number>) => string): string {
  const tbl = tableLabel(a.targetTable);
  const fld = fieldLabel(a.targetTable, a.field);
  const catLabel = getCategoryLabel(t, a.category);

  let lines = [
    t("email.greeting"),
    "",
    t("email.intro", { fileName, table: tbl }),
    "",
    t("email.catLabel", { cat: catLabel }),
    t("email.fieldLabel", { label: fld, field: a.field }),
  ];

  if (a.currentValue) lines.push(t("email.currentValue", { value: a.currentValue }));
  if (a.proposedValue) lines.push(t("email.proposedValue", { value: a.proposedValue }));
  if (a.affectedCount && a.affectedCount > 1) lines.push(t("email.affectedRows", { count: a.affectedCount }));
  if (a.row != null) lines.push(t("email.rowNumber", { row: a.row + 1 }));

  lines.push("");
  lines.push(t("email.descriptionLabel", { reason: a.reason }));

  if (a.origin) {
    lines.push(t("email.cause", { origin: a.origin }));
  }

  lines.push("");
  lines.push(t("email.request"));
  lines.push("");
  const signature = localStorage.getItem("hermes-signature") || t("email.defaultSignature");
  lines.push(signature);

  return lines.join("\n");
}

function generateFlags(analysis: AnalysisResult | null, anomalies: Anomaly[], t: (key: string, params?: Record<string, string | number>) => string): AnomalyFlag[] {
  if (!analysis) return [];

  const pending = anomalies.filter(
    (a) => a.status === "pending" && (a.type === "data" || a.type === "demographic"),
  );
  if (pending.length === 0) return [];

  return pending.map((a, i) => ({
    id: i + 1,
    field: a.field,
    targetTable: a.targetTable,
    category: a.category || "",
    currentValue: a.currentValue,
    proposedValue: a.proposedValue,
    reason: a.reason,
    origin: a.origin,
    affectedCount: a.affectedCount,
    contactEmail: "",
    draftMessage: generateDraftMessage(a, analysis.fileName, t),
    sent: false,
  }));
}

interface AnomalyEmailSectionProps {
  analysis: AnalysisResult | null;
  anomalies: Anomaly[];
}

const AnomalyEmailSection = ({ analysis, anomalies }: AnomalyEmailSectionProps) => {
  const { t } = useI18n();
  const initialFlags = useMemo(() => generateFlags(analysis, anomalies, t), [analysis, anomalies, t]);
  const [flags, setFlags] = useState<AnomalyFlag[]>(initialFlags);
  const [expandedId, setExpandedId] = useState<number | null>(null);
  const [sendingId, setSendingId] = useState<number | null>(null);
  const [sendError, setSendError] = useState<string | null>(null);

  if (flags.length === 0) return null;

  const sendHermes = async (id: number) => {
    const flag = flags.find((f) => f.id === id);
    if (!flag || !flag.contactEmail.trim()) return;
    setSendingId(id);
    setSendError(null);
    try {
      const catLabel = getCategoryLabel(t, flag.category || undefined);
      await sendHermesEmail(
        "",
        flag.contactEmail.trim(),
        `Hermes – ${catLabel}: ${fieldLabel(flag.targetTable, flag.field)}`,
        flag.draftMessage,
      );
      setFlags((prev) => prev.map((f) => f.id === id ? { ...f, sent: true } : f));
    } catch (err: any) {
      setSendError(err.message || t("email.sendFailed"));
    } finally {
      setSendingId(null);
    }
  };

  const updateMessage = (id: number, message: string) => {
    setFlags((prev) => prev.map((f) => f.id === id ? { ...f, draftMessage: message } : f));
  };

  const updateEmail = (id: number, email: string) => {
    setFlags((prev) => prev.map((f) => f.id === id ? { ...f, contactEmail: email } : f));
  };

  const fld = (f: AnomalyFlag) => fieldLabel(f.targetTable, f.field);
  const catLabelFn = (f: AnomalyFlag) => getCategoryLabel(t, f.category || undefined);

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-medium flex items-center gap-2">
          <Mail className="h-4 w-4 text-primary" />
          {t("email.title")}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        <p className="text-xs text-muted-foreground">
          {t("email.openAnomalies", { count: flags.length, plural: flags.length !== 1 ? "n" : "" })}
        </p>
        <div className="max-h-[520px] overflow-y-auto space-y-3 pr-1">
        {flags.map((f) => (
          <div key={f.id} className={`border rounded-lg transition-colors ${f.sent ? "border-primary/30 bg-primary/5" : "border-border"}`}>
            <div
              className="p-4 cursor-pointer"
              onClick={() => setExpandedId(expandedId === f.id ? null : f.id)}
            >
              <div className="flex items-start justify-between gap-4">
                <div className="flex-1">
                  <div className="flex items-center gap-2 mb-1">
                    {f.sent ? <Mail className="h-4 w-4 text-primary" /> : <AlertTriangle className="h-4 w-4 text-destructive" />}
                    <span className="text-sm font-medium">{fld(f)}</span>
                    <span className="text-[10px] px-1.5 py-0.5 rounded bg-muted text-muted-foreground">{catLabelFn(f)}</span>
                    <span className="text-xs text-muted-foreground">· {tableLabel(f.targetTable)}</span>
                  </div>
                  <p className="text-xs text-muted-foreground ml-6 truncate max-w-[600px]">{f.reason}</p>
                </div>
                <div className="flex items-center gap-2 shrink-0">
                  {f.affectedCount && f.affectedCount > 1 && (
                    <span className="text-[10px] px-1.5 py-0.5 rounded bg-muted text-muted-foreground">{f.affectedCount} {t("common.rows")}</span>
                  )}
                  {f.sent ? (
                    <Badge className="bg-primary/10 text-primary border-0 text-xs">{t("email.sent")}</Badge>
                  ) : (
                    <Badge variant="outline" className="text-xs">{t("email.open")}</Badge>
                  )}
                </div>
              </div>
            </div>

            {expandedId === f.id && !f.sent && (
              <div className="px-4 pb-4 space-y-3 border-t border-border pt-3">
                <div className="flex items-center gap-2">
                  <Mail className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                  <span className="text-xs text-muted-foreground shrink-0">{t("email.to")}</span>
                  <Input
                    type="email"
                    value={f.contactEmail}
                    onChange={(e) => updateEmail(f.id, e.target.value)}
                    placeholder={t("email.recipientPlaceholder")}
                    className="h-8 text-sm max-w-[320px]"
                  />
                </div>
                <Textarea
                  value={f.draftMessage}
                  onChange={(e) => updateMessage(f.id, e.target.value)}
                  className="text-sm min-h-[180px]"
                />
                {sendError && sendingId === null && expandedId === f.id && (
                  <p className="text-xs text-destructive">{sendError}</p>
                )}
                <Button
                  onClick={() => sendHermes(f.id)}
                  disabled={sendingId === f.id || !f.contactEmail.trim()}
                  className="bg-primary text-primary-foreground hover:bg-primary/90"
                >
                  {sendingId === f.id ? (
                    <><Loader2 className="h-3.5 w-3.5 mr-1.5 animate-spin" /> {t("email.sending")}</>
                  ) : (
                    <><Send className="h-3.5 w-3.5 mr-1.5" /> {t("email.sendHermes")}</>
                  )}
                </Button>
              </div>
            )}

            {expandedId === f.id && f.sent && (
              <div className="px-4 pb-4 border-t border-border pt-3">
                <div className="bg-muted rounded-lg p-3 text-sm text-muted-foreground whitespace-pre-line">
                  {f.draftMessage}
                </div>
                <p className="text-xs text-primary mt-2">&check; {t("email.messageSent", { email: f.contactEmail })}</p>
              </div>
            )}
          </div>
        ))}
        </div>
      </CardContent>
    </Card>
  );
};

export default AnomalyEmailSection;
