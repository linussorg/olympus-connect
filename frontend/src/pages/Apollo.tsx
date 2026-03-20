import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Treemap } from "recharts";
import { Database, AlertTriangle, CheckCircle, FileUp, Loader2 } from "lucide-react";
import DataChat from "./apollo/DataChat";
import { getQuality, type QualityData } from "@/lib/api";
import { useI18n } from "@/lib/i18n";

const SOURCE_COLORS: Record<string, string> = {
  labs: "hsl(200, 50%, 50%)", nursing: "hsl(168, 45%, 45%)", medication: "hsl(45, 80%, 55%)",
  "icd10-ops": "hsl(280, 40%, 55%)", "epaAC-Data-1": "hsl(12, 70%, 55%)",
  "epaAC-Data-2": "hsl(12, 50%, 65%)", "epaAC-Data-3": "hsl(12, 40%, 45%)",
  "device-motion": "hsl(220, 50%, 55%)", "device-1hz": "hsl(220, 40%, 65%)",
};

const TreemapContent = (props: any) => {
  const { x, y, width, height, name, size, entriesLabel } = props;
  if (!size || width < 40 || height < 30) return null;
  return (
    <g>
      <rect x={x} y={y} width={width} height={height} fill={props.fill} stroke="hsl(0, 0%, 100%)" strokeWidth={2} rx={4} />
      <text x={x + width / 2} y={y + height / 2 - 6} textAnchor="middle" fill="#fff" fontSize={width < 80 ? 10 : 12} fontWeight={600}>
        {name}
      </text>
      <text x={x + width / 2} y={y + height / 2 + 10} textAnchor="middle" fill="rgba(255,255,255,0.8)" fontSize={10}>
        {size.toLocaleString()} {entriesLabel}
      </text>
    </g>
  );
};

const Apollo = () => {
  const { t } = useI18n();
  const [quality, setQuality] = useState<QualityData | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    getQuality()
      .then(setQuality)
      .catch(() => setQuality(null))
      .finally(() => setLoading(false));
  }, []);

  const CATEGORY_LABELS: Record<string, string> = {
    "source-format": t("cat.source-format"), encoding: t("cat.encoding"), "null-variant": t("cat.null-variant"),
    "date-format": t("cat.date-format"), "id-format": t("cat.id-format"), "out-of-range": t("cat.out-of-range"),
    "flag-drift": t("cat.flag-drift"), duplicate: t("cat.duplicate"), orphan: t("cat.orphan"),
    temporal: t("cat.temporal"), completeness: t("cat.completeness"), "free-text": t("cat.free-text"), demographic: t("cat.demographic"),
  };

  const SOURCE_LABELS: Record<string, string> = {
    labs: t("src.labs"), nursing: t("src.nursing"), medication: t("src.medication"),
    "icd10-ops": t("src.icd10-ops"), "epaAC-Data-1": t("src.epaAC-Data-1"),
    "epaAC-Data-2": t("src.epaAC-Data-2"), "epaAC-Data-3": t("src.epaAC-Data-3"),
    "device-motion": t("src.device-motion"), "device-1hz": t("src.device-1hz"),
  };

  const q = quality;
  const hasData = q && q.fileCount > 0;

  const sourcesComposition = hasData
    ? Object.values(
        q.recentUploads.reduce<Record<string, { name: string; size: number; fill: string }>>((acc, u) => {
          const key = u.detectedType;
          if (!acc[key]) {
            acc[key] = {
              name: SOURCE_LABELS[key] || key,
              size: 0,
              fill: SOURCE_COLORS[key] || "hsl(0, 0%, 60%)",
            };
          }
          acc[key].size += u.rows;
          return acc;
        }, {}),
      )
    : [];

  const errorsByCategory = hasData
    ? q.errorsByCategory.slice(0, 8).map((e) => ({ category: CATEGORY_LABELS[e.category] || e.category, count: e.count }))
    : [];


  return (
    <div className="p-6 space-y-6">
      <div>
        <h2 className="text-xl font-semibold text-foreground">Apollo</h2>
        <p className="text-sm text-muted-foreground">
          {t("apollo.subtitle")}
        </p>
      </div>

      {/* KPIs */}
      <div className="grid grid-cols-4 gap-4">
        {[
          { label: t("apollo.totalRecords"), value: hasData ? q.totalRows.toLocaleString() : loading ? "..." : "0", icon: Database, color: "text-primary" },
          { label: t("apollo.dataSources"), value: hasData ? String(q.totalSources) : loading ? "..." : "0", icon: FileUp, color: "text-foreground" },
          { label: t("apollo.successfullyMapped"), value: hasData ? `${(q.averageConfidence * 100).toFixed(1)}%` : loading ? "..." : "–", icon: CheckCircle, color: "text-primary" },
          { label: t("apollo.openAnomalies"), value: hasData ? String(q.totalAnomalies) : loading ? "..." : "0", icon: AlertTriangle, color: "text-destructive" },
        ].map((kpi) => (
          <Card key={kpi.label}>
            <CardContent className="pt-4 pb-3 px-4 flex items-start gap-3">
              {loading ? <Loader2 className="h-5 w-5 mt-0.5 animate-spin text-muted-foreground" /> : <kpi.icon className={`h-5 w-5 mt-0.5 ${kpi.color}`} />}
              <div>
                <p className="text-xs text-muted-foreground">{kpi.label}</p>
                <p className={`text-2xl font-bold ${kpi.color}`}>{kpi.value}</p>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      <div className="grid grid-cols-2 gap-4">
        {/* Source Composition Treemap */}
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">{t("apollo.dbComposition")}</CardTitle>
          </CardHeader>
          <CardContent>
            <ResponsiveContainer width="100%" height={280}>
              <Treemap
                data={sourcesComposition}
                dataKey="size"
                aspectRatio={4 / 3}
                content={<TreemapContent entriesLabel={t("common.entries")} />}
              />
            </ResponsiveContainer>
          </CardContent>
        </Card>

        {/* Anomalies Bar Chart */}
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">{t("apollo.anomaliesByType")}</CardTitle>
          </CardHeader>
          <CardContent>
            <ResponsiveContainer width="100%" height={280}>
              <BarChart data={errorsByCategory} layout="vertical">
                <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
                <XAxis type="number" tick={{ fontSize: 12 }} />
                <YAxis dataKey="category" type="category" width={120} tick={{ fontSize: 12 }} />
                <Tooltip />
                <Bar dataKey="count" fill="hsl(var(--destructive))" name={t("apollo.anomalies")} radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>
      </div>

      {/* Chat - Talk to your data */}
      <DataChat />
    </div>
  );
};

export default Apollo;
