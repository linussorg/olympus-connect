import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Table, TableHeader, TableBody, TableHead, TableRow, TableCell } from "@/components/ui/table";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Command, CommandInput, CommandList, CommandEmpty, CommandGroup, CommandItem } from "@/components/ui/command";
import { Trash2, Check, ChevronsUpDown, ArrowRight, Link2 } from "lucide-react";
import { getOverrides, updateOverride, deleteOverride, type MappingOverrideEntry } from "@/lib/api";
import { ensureLoaded, tableLabel, fieldLabel, getTargetFields } from "@/lib/schema-labels";
import { useI18n } from "@/lib/i18n";

const MappingOverrides = () => {
  const { t, locale } = useI18n();
  const [overrides, setOverrides] = useState<MappingOverrideEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [comboOpen, setComboOpen] = useState(false);

  const load = async () => {
    await ensureLoaded();
    const data = await getOverrides();
    setOverrides(data);
    setLoading(false);
  };

  useEffect(() => { load(); }, []);

  const handleDelete = async (id: string) => {
    await deleteOverride(id);
    setOverrides((prev) => prev.filter((o) => o.id !== id));
  };

  const handleUpdate = async (id: string, newTarget: string) => {
    const updated = await updateOverride(id, newTarget);
    setOverrides((prev) => prev.map((o) => o.id === id ? updated : o));
    setEditingId(null);
  };

  const grouped = overrides.reduce<Record<string, MappingOverrideEntry[]>>((acc, o) => {
    (acc[o.targetTable] ||= []).push(o);
    return acc;
  }, {});

  return (
    <div className="p-6 space-y-6">
      <div>
        <h2 className="text-xl font-semibold text-foreground">{t("mappingRules.title")}</h2>
        <p className="text-sm text-muted-foreground">
          {t("mappingRules.subtitle")}
        </p>
      </div>

      {loading ? (
        <Card>
          <CardContent className="pt-6 text-center text-muted-foreground text-sm">{t("mappingRules.loading")}</CardContent>
        </Card>
      ) : overrides.length === 0 ? (
        <Card>
          <CardContent className="pt-6 text-center text-muted-foreground text-sm">
            {t("mappingRules.noRules")}
          </CardContent>
        </Card>
      ) : (
        Object.entries(grouped).map(([table, entries]) => (
          <Card key={table}>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium flex items-center gap-2">
                <Link2 className="h-4 w-4 text-primary" />
                {tableLabel(table)}
                <span className="text-xs text-muted-foreground font-normal font-mono">{table}</span>
                <span className="text-xs text-muted-foreground font-normal">· {t("mappingRules.rules", { count: entries.length, plural: entries.length !== 1 ? "n" : "" })}</span>
              </CardTitle>
            </CardHeader>
            <CardContent>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="h-8 text-xs">{t("mappingRules.sourceColumn")}</TableHead>
                    <TableHead className="h-8 text-xs w-8"></TableHead>
                    <TableHead className="h-8 text-xs">{t("mappingRules.targetField")}</TableHead>
                    <TableHead className="h-8 text-xs">{t("mappingRules.description")}</TableHead>
                    <TableHead className="h-8 text-xs">{t("mappingRules.created")}</TableHead>
                    <TableHead className="h-8 text-xs w-20"></TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {entries.map((o) => (
                    <TableRow key={o.id}>
                      <TableCell className="py-1.5 px-4 font-mono text-xs">{o.sourceColumn}</TableCell>
                      <TableCell className="py-1.5 px-1 text-muted-foreground"><ArrowRight className="h-3 w-3" /></TableCell>
                      <TableCell className="py-1.5 px-4">
                        {editingId === o.id ? (
                          <Popover open={comboOpen} onOpenChange={setComboOpen}>
                            <PopoverTrigger asChild>
                              <Button variant="outline" size="sm" className="h-7 w-[260px] justify-between text-xs">
                                {o.targetColumn}
                                <ChevronsUpDown className="ml-1 h-3 w-3 opacity-50" />
                              </Button>
                            </PopoverTrigger>
                            <PopoverContent className="w-[300px] p-0" align="start">
                              <Command>
                                <CommandInput placeholder={t("mappingRules.searchField")} className="h-9" />
                                <CommandList>
                                  <CommandEmpty>{t("mappingRules.noFieldFound")}</CommandEmpty>
                                  <CommandGroup>
                                    {(getTargetFields(table) || []).map((field) => {
                                      const label = fieldLabel(table, field);
                                      const hasLabel = label !== field;
                                      return (
                                        <CommandItem
                                          key={field}
                                          value={`${field} ${label}`}
                                          onSelect={() => {
                                            handleUpdate(o.id, field);
                                            setComboOpen(false);
                                          }}
                                          className="text-xs"
                                        >
                                          <Check className={`mr-2 h-3 w-3 shrink-0 ${o.targetColumn === field ? "opacity-100" : "opacity-0"}`} />
                                          <span className="truncate">
                                            {hasLabel ? <>{label} <span className="font-mono text-muted-foreground">({field})</span></> : <span className="font-mono">{field}</span>}
                                          </span>
                                        </CommandItem>
                                      );
                                    })}
                                  </CommandGroup>
                                </CommandList>
                              </Command>
                            </PopoverContent>
                          </Popover>
                        ) : (
                          <span className="font-mono text-xs font-medium">{o.targetColumn}</span>
                        )}
                      </TableCell>
                      <TableCell className="py-1.5 px-4 text-xs text-muted-foreground">
                        {fieldLabel(table, o.targetColumn)}
                      </TableCell>
                      <TableCell className="py-1.5 px-4 text-xs text-muted-foreground">
                        {new Date(o.createdAt).toLocaleDateString(locale === "de" ? "de-DE" : "en-US")}
                      </TableCell>
                      <TableCell className="py-1.5 px-4">
                        <div className="flex gap-1">
                          {editingId === o.id ? (
                            <Button size="sm" variant="ghost" className="h-7 px-2 text-xs" onClick={() => setEditingId(null)}>
                              {t("common.cancel")}
                            </Button>
                          ) : (
                            <Button size="sm" variant="ghost" className="h-7 px-2 text-xs" onClick={() => { setEditingId(o.id); setComboOpen(false); }}>
                              {t("common.edit")}
                            </Button>
                          )}
                          <Button size="sm" variant="ghost" className="h-7 px-2 text-destructive hover:text-destructive" onClick={() => handleDelete(o.id)}>
                            <Trash2 className="h-3.5 w-3.5" />
                          </Button>
                        </div>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        ))
      )}
    </div>
  );
};

export default MappingOverrides;
