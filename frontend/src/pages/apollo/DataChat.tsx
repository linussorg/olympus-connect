import { useState, useRef, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Send, Download, Copy, Bot, User, Loader2, AlertTriangle } from "lucide-react";
import { chat as chatApi, type ChatHistoryMessage } from "@/lib/api";
import { useChat } from "@/hooks/use-chat";
import { useI18n } from "@/lib/i18n";
import hljs from "highlight.js/lib/core";
import sql from "highlight.js/lib/languages/sql";
import "highlight.js/styles/github.css";
import {
  BarChart, Bar, LineChart, Line, PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend,
} from "recharts";

const CHART_COLORS = [
  "hsl(var(--primary))",
  "hsl(var(--chart-2, 220 70% 50%))",
  "hsl(var(--chart-3, 150 60% 45%))",
  "hsl(var(--chart-4, 280 65% 55%))",
  "hsl(var(--chart-5, 30 80% 55%))",
];

hljs.registerLanguage("sql", sql);

const DataChat = () => {
  const { t } = useI18n();
  const [input, setInput] = useState("");
  const exampleQueries = [
    t("chat.example1"),
    t("chat.example2"),
    t("chat.example3"),
    t("chat.example4"),
  ];
  const { messages, setMessages } = useChat();
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight, behavior: "smooth" });
  }, [messages]);

  const [sending, setSending] = useState(false);

  const send = async (text?: string) => {
    const q = text || input;
    if (!q.trim() || sending) return;
    setInput("");

    // Build history from current messages (skip the initial static welcome message)
    const history: ChatHistoryMessage[] = messages
      .slice(1)
      .map((m) => ({
        role: m.role,
        content: m.sql ? `${m.text}\n\nSQL: ${m.sql}` : m.text,
      }));

    setMessages((prev) => [...prev, { role: "user", text: q }]);
    setSending(true);

    try {
      const response = await chatApi(q, history);
      setMessages((prev) => [...prev, { role: "assistant", text: response.text, sql: response.sql, table: response.table, chart: response.chart }]);
    } catch (err: any) {
      setMessages((prev) => [...prev, { role: "assistant", text: err.message || t("chat.requestFailed") }]);
    } finally {
      setSending(false);
    }
  };

  const copyToClipboard = (text: string) => navigator.clipboard.writeText(text);

  const exportCSV = (headers: string[], rows: string[][]) => {
    const csv = [headers.join(","), ...rows.map((r) => r.join(","))].join("\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "apollo_export.csv";
    a.click();
    URL.revokeObjectURL(url);
  };

  const exportJSON = (headers: string[], rows: string[][]) => {
    const data = rows.map((r) => Object.fromEntries(headers.map((h, i) => [h, r[i]])));
    const blob = new Blob([JSON.stringify(data, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "apollo_export.json";
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <Card className="flex flex-col h-[600px]">
      <CardHeader className="pb-2 shrink-0">
        <CardTitle className="text-sm font-medium flex items-center gap-2">
          <Bot className="h-4 w-4 text-primary" />
          {t("chat.title")}
        </CardTitle>
      </CardHeader>
      <CardContent className="flex flex-col flex-1 min-h-0 gap-3">
        {/* Example queries */}
        <div className="flex gap-1.5 flex-wrap">
          {exampleQueries.map((eq) => (
            <Badge
              key={eq}
              variant="outline"
              className="cursor-pointer text-[10px] hover:bg-muted transition-colors"
              onClick={() => send(eq)}
            >
              {eq.length > 40 ? eq.slice(0, 40) + "…" : eq}
            </Badge>
          ))}
        </div>

        {/* Messages */}
        <div ref={scrollRef} className="flex-1 overflow-y-auto space-y-3 border border-border rounded-lg p-3">
          {messages.map((m, i) => (
            <div key={i} className={`flex gap-2 ${m.role === "user" ? "justify-end" : ""}`}>
              {m.role === "assistant" && (
                <div className="shrink-0 mt-0.5 h-6 w-6 rounded-full bg-primary/10 flex items-center justify-center">
                  <Bot className="h-3 w-3 text-primary" />
                </div>
              )}
              <div className={`max-w-[85%] space-y-2 ${m.role === "user" ? "text-right" : ""}`}>
                <span className={`inline-block px-3 py-1.5 rounded-lg text-sm ${
                  m.role === "user"
                    ? "bg-primary text-primary-foreground"
                    : "bg-muted text-foreground"
                }`}>
                  {m.text}
                </span>

                {/* SQL preview */}
                {m.sql && (
                  <div className="relative rounded-md overflow-hidden border border-border bg-muted/50">
                    <button
                      onClick={() => copyToClipboard(m.sql!)}
                      className="absolute top-1.5 right-1.5 z-10 p-1 rounded hover:bg-muted transition-colors text-muted-foreground"
                      title={t("chat.copySql")}
                    >
                      <Copy className="h-3 w-3" />
                    </button>
                    <pre className="!m-0 p-2 text-xs overflow-x-auto"><code
                      className="hljs language-sql"
                      dangerouslySetInnerHTML={{ __html: hljs.highlight(m.sql, { language: "sql" }).value }}
                    /></pre>
                  </div>
                )}

                {/* No results warning */}
                {m.sql && !m.table && (
                  <div className="flex items-center gap-2 px-3 py-2 rounded-md bg-yellow-500/10 border border-yellow-500/30 text-yellow-700 dark:text-yellow-400 text-xs">
                    <AlertTriangle className="h-3.5 w-3.5 shrink-0" />
                    <span>{t("chat.queryFailed")}</span>
                  </div>
                )}

                {/* Chart */}
                {m.chart && m.table && (() => {
                  const { type, labelKey, valueKeys } = m.chart;
                  const labelIdx = m.table!.headers.indexOf(labelKey);
                  const valueIdxs = valueKeys.map((k) => m.table!.headers.indexOf(k));
                  if (labelIdx < 0 || valueIdxs.some((i) => i < 0)) return null;

                  const data = m.table!.rows.map((r) => {
                    const entry: Record<string, string | number> = { [labelKey]: r[labelIdx] };
                    valueKeys.forEach((k, ki) => {
                      entry[k] = parseFloat(r[valueIdxs[ki]]) || 0;
                    });
                    return entry;
                  });

                  return (
                    <div className="border border-border rounded-md p-2 bg-background">
                      <ResponsiveContainer width="100%" height={180}>
                        {type === "pie" ? (
                          <PieChart>
                            <Pie data={data} dataKey={valueKeys[0]} nameKey={labelKey} cx="50%" cy="50%" outerRadius={70} label={(e) => e[labelKey]}>
                              {data.map((_, idx) => <Cell key={idx} fill={CHART_COLORS[idx % CHART_COLORS.length]} />)}
                            </Pie>
                            <Tooltip />
                          </PieChart>
                        ) : type === "line" ? (
                          <LineChart data={data}>
                            <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
                            <XAxis dataKey={labelKey} tick={{ fontSize: 10 }} className="text-muted-foreground" />
                            <YAxis tick={{ fontSize: 10 }} className="text-muted-foreground" />
                            <Tooltip />
                            {valueKeys.length > 1 && <Legend />}
                            {valueKeys.map((k, ki) => <Line key={k} type="monotone" dataKey={k} stroke={CHART_COLORS[ki % CHART_COLORS.length]} strokeWidth={2} dot={false} />)}
                          </LineChart>
                        ) : (
                          <BarChart data={data}>
                            <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
                            <XAxis dataKey={labelKey} tick={{ fontSize: 10 }} className="text-muted-foreground" />
                            <YAxis tick={{ fontSize: 10 }} className="text-muted-foreground" />
                            <Tooltip />
                            {valueKeys.length > 1 && <Legend />}
                            {valueKeys.map((k, ki) => <Bar key={k} dataKey={k} fill={CHART_COLORS[ki % CHART_COLORS.length]} radius={[3, 3, 0, 0]} />)}
                          </BarChart>
                        )}
                      </ResponsiveContainer>
                    </div>
                  );
                })()}

                {/* Result table */}
                {m.table && (
                  <div className="space-y-1.5">
                    <div className="border border-border rounded-md overflow-hidden">
                      <table className="w-full text-xs">
                        <thead>
                          <tr className="bg-muted/50 border-b border-border">
                            {m.table.headers.map((h) => (
                              <th key={h} className="px-2 py-1 text-left font-medium text-muted-foreground">{h}</th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {m.table.rows.map((r, ri) => (
                            <tr key={ri} className="border-b border-border last:border-0">
                              {r.map((c, ci) => (
                                <td key={ci} className="px-2 py-1">{c}</td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                    <div className="flex gap-1.5">
                      <Button
                        variant="outline"
                        size="sm"
                        className="h-6 text-[10px] gap-1"
                        onClick={() => exportCSV(m.table!.headers, m.table!.rows)}
                      >
                        <Download className="h-2.5 w-2.5" /> CSV
                      </Button>
                      <Button
                        variant="outline"
                        size="sm"
                        className="h-6 text-[10px] gap-1"
                        onClick={() => exportJSON(m.table!.headers, m.table!.rows)}
                      >
                        <Download className="h-2.5 w-2.5" /> JSON
                      </Button>
                      <Button
                        variant="outline"
                        size="sm"
                        className="h-6 text-[10px] gap-1"
                        onClick={() => copyToClipboard(m.sql || "")}
                      >
                        <Copy className="h-2.5 w-2.5" /> SQL
                      </Button>
                    </div>
                  </div>
                )}
              </div>
              {m.role === "user" && (
                <div className="shrink-0 mt-0.5 h-6 w-6 rounded-full bg-primary flex items-center justify-center">
                  <User className="h-3 w-3 text-primary-foreground" />
                </div>
              )}
            </div>
          ))}
        </div>

        {/* Input */}
        <div className="flex gap-2 shrink-0">
          <Input
            placeholder={t("chat.placeholder")}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && send()}
            className="text-sm"
          />
          <Button size="icon" onClick={() => send()} className="shrink-0" disabled={sending}>
            {sending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Send className="h-4 w-4" />}
          </Button>
        </div>
      </CardContent>
    </Card>
  );
};

export default DataChat;
