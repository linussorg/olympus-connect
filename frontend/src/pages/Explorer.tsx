import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Textarea } from "@/components/ui/textarea";
import { Table, TableHeader, TableBody, TableHead, TableRow, TableCell } from "@/components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Database, Play, AlertCircle, Loader2, Download, Copy, Table2 } from "lucide-react";
import { getTableList, getTablePreview, executeSql, type TableInfo, type TablePreview, type SqlResult } from "@/lib/api";
import { tableLabel, fieldLabel } from "@/lib/schema-labels";
import { useI18n } from "@/lib/i18n";

const Explorer = () => {
  const { t, locale } = useI18n();
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [selectedTable, setSelectedTable] = useState<string | null>(null);
  const [preview, setPreview] = useState<TablePreview | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewError, setPreviewError] = useState<string | null>(null);

  const [sqlQuery, setSqlQuery] = useState("");
  const [sqlResult, setSqlResult] = useState<SqlResult | null>(null);
  const [sqlLoading, setSqlLoading] = useState(false);
  const [sqlError, setSqlError] = useState<string | null>(null);

  const [tab, setTab] = useState("preview");
  const [tablesLoading, setTablesLoading] = useState(true);
  const [tablesError, setTablesError] = useState<string | null>(null);

  useEffect(() => {
    setTablesLoading(true);
    getTableList()
      .then(setTables)
      .catch((err) => setTablesError(err.message))
      .finally(() => setTablesLoading(false));
  }, []);

  const selectTable = async (name: string) => {
    setSelectedTable(name);
    setPreviewLoading(true);
    setPreviewError(null);
    setTab("preview");
    try {
      const data = await getTablePreview(name);
      setPreview(data);
    } catch (err: any) {
      setPreviewError(err.message);
      setPreview(null);
    } finally {
      setPreviewLoading(false);
    }
  };

  const runSql = async () => {
    if (!sqlQuery.trim()) return;
    setSqlLoading(true);
    setSqlError(null);
    try {
      const result = await executeSql(sqlQuery);
      setSqlResult(result);
    } catch (err: any) {
      setSqlError(err.message);
      setSqlResult(null);
    } finally {
      setSqlLoading(false);
    }
  };

  const exportCsv = (headers: string[], rows: string[][]) => {
    const escape = (v: string) => v.includes(",") || v.includes('"') ? `"${v.replace(/"/g, '""')}"` : v;
    const csv = [headers.map(escape).join(","), ...rows.map((r) => r.map(escape).join(","))].join("\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${selectedTable || "query"}_export.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const totalRows = tables.reduce((sum, tbl) => sum + tbl.rowCount, 0);

  return (
    <div className="p-6 space-y-6">
      <div>
        <h2 className="text-xl font-semibold text-foreground">{t("sidebar.explorer")}</h2>
        <p className="text-sm text-muted-foreground">
          {t("explorer.subtitle")}
        </p>
      </div>

      <div className="flex gap-6">
        {/* Left panel — Table list */}
        <div className="w-64 shrink-0 space-y-2">
          <div className="flex items-center justify-between mb-1">
            <span className="text-xs font-medium text-muted-foreground">{t("explorer.tables")}</span>
            {tables.length > 0 && <span className="text-xs text-muted-foreground">{totalRows.toLocaleString(locale === "de" ? "de-DE" : "en-US")} {t("common.rows")}</span>}
          </div>
          {tablesLoading && (
            <div className="flex items-center gap-2 px-3 py-4 text-sm text-muted-foreground">
              <Loader2 className="h-4 w-4 animate-spin" /> {t("common.loading")}
            </div>
          )}
          {tablesError && (
            <div className="px-3 py-3 rounded-lg bg-destructive/5 border border-destructive/20">
              <div className="flex items-center gap-2 text-destructive text-xs">
                <AlertCircle className="h-3.5 w-3.5 shrink-0" />
                <span>{tablesError}</span>
              </div>
            </div>
          )}
          {tables.map((tbl) => (
            <button
              key={tbl.name}
              onClick={() => selectTable(tbl.name)}
              className={`w-full text-left px-3 py-2.5 rounded-lg transition-colors ${
                selectedTable === tbl.name
                  ? "bg-primary/10 border border-primary/30"
                  : "hover:bg-muted/50 border border-transparent"
              }`}
            >
              <div className="flex items-center gap-2">
                <Table2 className={`h-3.5 w-3.5 shrink-0 ${selectedTable === tbl.name ? "text-primary" : "text-muted-foreground"}`} />
                <span className="text-sm font-medium truncate">{tableLabel(tbl.name)}</span>
              </div>
              <div className="flex items-center gap-2 mt-1 ml-5.5">
                <span className="text-[10px] font-mono text-muted-foreground">{tbl.name}</span>
              </div>
              <div className="flex items-center gap-2 mt-0.5 ml-5.5">
                <Badge variant="outline" className="text-[10px] h-4 px-1.5">
                  {tbl.rowCount.toLocaleString(locale === "de" ? "de-DE" : "en-US")} {t("common.rows")}
                </Badge>
                <span className="text-[10px] text-muted-foreground">{tbl.columnCount} {t("common.columns")}</span>
              </div>
            </button>
          ))}
        </div>

        {/* Right panel */}
        <div className="flex-1 min-w-0">
          {!selectedTable && (
            <Card>
              <CardContent className="pt-12 pb-12 text-center">
                <Database className="h-12 w-12 mx-auto mb-4 text-muted-foreground/30" />
                <p className="text-sm text-muted-foreground">{t("explorer.selectTable")}</p>
              </CardContent>
            </Card>
          )}

          {selectedTable && (
            <Tabs value={tab} onValueChange={setTab}>
              <div className="flex items-center justify-between mb-3">
                <div>
                  <h3 className="text-base font-semibold">{tableLabel(selectedTable)}</h3>
                  <span className="text-xs font-mono text-muted-foreground">{selectedTable}</span>
                </div>
                <TabsList>
                  <TabsTrigger value="preview">{t("explorer.preview")}</TabsTrigger>
                  <TabsTrigger value="sql">SQL</TabsTrigger>
                </TabsList>
              </div>

              <TabsContent value="preview" className="space-y-4">
                {previewLoading && (
                  <Card>
                    <CardContent className="pt-8 pb-8 text-center">
                      <Loader2 className="h-6 w-6 animate-spin mx-auto text-primary" />
                      <p className="text-sm text-muted-foreground mt-2">{t("explorer.dataLoading")}</p>
                    </CardContent>
                  </Card>
                )}

                {previewError && (
                  <Card className="border-destructive/30 bg-destructive/5">
                    <CardContent className="pt-4 pb-3 flex items-center gap-2 text-destructive">
                      <AlertCircle className="h-4 w-4" />
                      <span className="text-sm">{previewError}</span>
                    </CardContent>
                  </Card>
                )}

                {preview && !previewLoading && (
                  <>
                    {/* Column schema */}
                    <Card>
                      <CardHeader className="pb-2">
                        <CardTitle className="text-sm font-medium">{t("explorer.columns")} ({preview.columns.length})</CardTitle>
                      </CardHeader>
                      <CardContent>
                        <div className="flex flex-wrap gap-1.5">
                          {preview.columns.map((col) => (
                            <span
                              key={col.name}
                              className="text-[11px] px-2 py-1 rounded bg-muted font-mono"
                              title={col.description || col.name}
                            >
                              <span className="font-medium">{col.name}</span>
                              <span className="text-muted-foreground ml-1.5">{col.type}</span>
                            </span>
                          ))}
                        </div>
                      </CardContent>
                    </Card>

                    {/* Data table */}
                    <Card>
                      <CardHeader className="pb-2">
                        <div className="flex items-center justify-between">
                          <CardTitle className="text-sm font-medium">
                            {t("explorer.dataTop100")}
                          </CardTitle>
                          {preview.table.rows.length > 0 && (
                            <Button
                              variant="outline"
                              size="sm"
                              className="h-7 text-xs"
                              onClick={() => exportCsv(preview.table.headers, preview.table.rows)}
                            >
                              <Download className="h-3 w-3 mr-1.5" /> CSV
                            </Button>
                          )}
                        </div>
                      </CardHeader>
                      <CardContent>
                        {preview.table.rows.length === 0 ? (
                          <p className="text-sm text-muted-foreground py-4 text-center">{t("explorer.tableEmpty")}</p>
                        ) : (
                          <div className="overflow-auto max-h-[500px] border rounded-lg">
                            <Table>
                              <TableHeader>
                                <TableRow>
                                  {preview.table.headers.map((h) => (
                                    <TableHead key={h} className="h-8 text-xs whitespace-nowrap sticky top-0 bg-muted">
                                      {fieldLabel(selectedTable, h) !== h ? fieldLabel(selectedTable, h) : h}
                                    </TableHead>
                                  ))}
                                </TableRow>
                              </TableHeader>
                              <TableBody>
                                {preview.table.rows.map((row, ri) => (
                                  <TableRow key={ri}>
                                    {row.map((cell, ci) => (
                                      <TableCell key={ci} className="py-1.5 px-3 text-xs whitespace-nowrap max-w-[300px] truncate">
                                        {cell || <span className="text-muted-foreground">NULL</span>}
                                      </TableCell>
                                    ))}
                                  </TableRow>
                                ))}
                              </TableBody>
                            </Table>
                          </div>
                        )}
                      </CardContent>
                    </Card>
                  </>
                )}
              </TabsContent>

              <TabsContent value="sql" className="space-y-4">
                <Card>
                  <CardContent className="pt-4 space-y-3">
                    <Textarea
                      value={sqlQuery}
                      onChange={(e) => setSqlQuery(e.target.value)}
                      placeholder={`SELECT TOP 10 * FROM ${selectedTable}`}
                      className="font-mono text-sm min-h-[100px]"
                      onKeyDown={(e) => {
                        if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
                          e.preventDefault();
                          runSql();
                        }
                      }}
                    />
                    <div className="flex items-center gap-2">
                      <Button onClick={runSql} disabled={sqlLoading || !sqlQuery.trim()} size="sm">
                        {sqlLoading ? <Loader2 className="h-3.5 w-3.5 mr-1.5 animate-spin" /> : <Play className="h-3.5 w-3.5 mr-1.5" />}
                        {t("explorer.execute")}
                      </Button>
                      <span className="text-xs text-muted-foreground">Ctrl+Enter</span>
                    </div>
                  </CardContent>
                </Card>

                {sqlError && (
                  <Card className="border-destructive/30 bg-destructive/5">
                    <CardContent className="pt-4 pb-3 flex items-center gap-2 text-destructive">
                      <AlertCircle className="h-4 w-4" />
                      <span className="text-sm">{sqlError}</span>
                    </CardContent>
                  </Card>
                )}

                {sqlResult && (
                  <Card>
                    <CardHeader className="pb-2">
                      <div className="flex items-center justify-between">
                        <CardTitle className="text-sm font-medium">
                          {t("explorer.resultRows", { count: sqlResult.rows.length })}
                        </CardTitle>
                        <div className="flex gap-1.5">
                          <Button
                            variant="outline"
                            size="sm"
                            className="h-7 text-xs"
                            onClick={() => navigator.clipboard.writeText(sqlResult.sql)}
                          >
                            <Copy className="h-3 w-3 mr-1.5" /> SQL
                          </Button>
                          {sqlResult.rows.length > 0 && (
                            <Button
                              variant="outline"
                              size="sm"
                              className="h-7 text-xs"
                              onClick={() => exportCsv(sqlResult.headers, sqlResult.rows)}
                            >
                              <Download className="h-3 w-3 mr-1.5" /> CSV
                            </Button>
                          )}
                        </div>
                      </div>
                    </CardHeader>
                    <CardContent>
                      {sqlResult.rows.length === 0 ? (
                        <p className="text-sm text-muted-foreground py-4 text-center">{t("explorer.noResults")}</p>
                      ) : (
                        <div className="overflow-auto max-h-[500px] border rounded-lg">
                          <Table>
                            <TableHeader>
                              <TableRow>
                                {sqlResult.headers.map((h) => (
                                  <TableHead key={h} className="h-8 text-xs whitespace-nowrap sticky top-0 bg-muted">{h}</TableHead>
                                ))}
                              </TableRow>
                            </TableHeader>
                            <TableBody>
                              {sqlResult.rows.map((row, ri) => (
                                <TableRow key={ri}>
                                  {row.map((cell, ci) => (
                                    <TableCell key={ci} className="py-1.5 px-3 text-xs whitespace-nowrap max-w-[300px] truncate">
                                      {cell || <span className="text-muted-foreground">NULL</span>}
                                    </TableCell>
                                  ))}
                                </TableRow>
                              ))}
                            </TableBody>
                          </Table>
                        </div>
                      )}
                    </CardContent>
                  </Card>
                )}
              </TabsContent>
            </Tabs>
          )}
        </div>
      </div>
    </div>
  );
};

export default Explorer;
