import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Command, CommandInput, CommandList, CommandEmpty, CommandGroup, CommandItem } from "@/components/ui/command";
import { CheckCircle, AlertTriangle, X, Check, Link2, Database, ChevronsUpDown, Users, EyeOff } from "lucide-react";
import { Anomaly } from "./types";
import { tableLabel, fieldLabel, getTargetFields } from "@/lib/schema-labels";
import { useI18n } from "@/lib/i18n";

// Categories where manual edit doesn't make sense (info-only / structural)
const INFO_ONLY_CATEGORIES = new Set(["completeness", "duplicate", "null-variant", "source-format"]);

interface AnomalyCardProps {
  anomaly: Anomaly;
  unmappedSourceColumns?: string[];
  onAccept: (id: number) => void;
  onStartManual: (id: number) => void;
  onUpdateManual: (id: number, value: string) => void;
  onConfirmManual: (id: number) => void;
  onCancelManual: (id: number) => void;
  onQuickSelect?: (id: number, value: string) => void;
  onIgnore?: (id: number) => void;
}

const AnomalyCard = ({ anomaly: a, unmappedSourceColumns, onAccept, onStartManual, onUpdateManual, onConfirmManual, onCancelManual, onQuickSelect, onIgnore }: AnomalyCardProps) => {
  const { t } = useI18n();
  const isMapping = a.type === "mapping" || a.type === "mapping-source" || a.type === "mapping-target";
  const isDemographic = a.type === "demographic";
  const isInfoOnly = !isMapping && !isDemographic && INFO_ONLY_CATEGORIES.has(a.category || "");
  const [comboOpen, setComboOpen] = useState(false);

  // Smart accept label: "Übernehmen" for concrete values, "Bestätigen" for advice text
  const isConcreteValue = a.proposedValue && a.proposedValue.length <= 40;
  const acceptLabel = isConcreteValue ? t("anomaly.accept") : t("anomaly.confirm");

  const badgeLabel = a.type === "mapping-source" ? t("anomaly.sourceUnmapped")
    : a.type === "mapping-target" ? t("anomaly.targetUnmapped")
    : a.type === "demographic" ? t("anomaly.demographics")
    : isMapping ? t("anomaly.mapping") : t("anomaly.data");

  const badgeClass = a.type === "mapping-source" ? "bg-yellow-500/10 text-yellow-700"
    : a.type === "mapping-target" ? "bg-orange-500/10 text-orange-700"
    : a.type === "demographic" ? "bg-purple-500/10 text-purple-700"
    : isMapping ? "bg-accent text-accent-foreground"
    : "bg-muted text-muted-foreground";

  const valueLabel = a.type === "mapping-source" ? t("anomaly.sourceColumn")
    : a.type === "mapping-target" ? t("anomaly.targetField")
    : isMapping ? t("anomaly.sourceColumn") : t("anomaly.current");

  const TypeIcon = isDemographic ? Users : isMapping ? Link2 : Database;
  const fl = fieldLabel(a.targetTable, a.field);
  const fieldHasLabel = fl !== a.field;

  // Category-aware manual edit control
  const renderManualEditor = () => {
    // mapping-target: pick a SOURCE column to map to this target field
    if (a.type === "mapping-target") {
      if (!unmappedSourceColumns || unmappedSourceColumns.length === 0) {
        return <span className="text-xs text-muted-foreground italic">{t("anomaly.noAvailableSource")}</span>;
      }
      return (
        <Popover open={comboOpen} onOpenChange={setComboOpen}>
          <PopoverTrigger asChild>
            <Button variant="outline" role="combobox" aria-expanded={comboOpen} className="h-8 w-[360px] justify-between text-sm">
              {a.manualValue || t("anomaly.selectSource")}
              <ChevronsUpDown className="ml-2 h-3.5 w-3.5 shrink-0 opacity-50" />
            </Button>
          </PopoverTrigger>
          <PopoverContent className="w-[360px] p-0" align="start">
            <Command>
              <CommandInput placeholder={t("anomaly.searchSource")} className="h-9" />
              <CommandList>
                <CommandEmpty>{t("anomaly.noSource")}</CommandEmpty>
                <CommandGroup>
                  {unmappedSourceColumns.map((col) => (
                    <CommandItem
                      key={col}
                      value={col}
                      onSelect={() => {
                        onUpdateManual(a.id, col);
                        setComboOpen(false);
                      }}
                      className="text-xs"
                    >
                      <Check className={`mr-2 h-3 w-3 shrink-0 ${a.manualValue === col ? "opacity-100" : "opacity-0"}`} />
                      <span className="font-mono">{col}</span>
                    </CommandItem>
                  ))}
                </CommandGroup>
              </CommandList>
            </Command>
          </PopoverContent>
        </Popover>
      );
    }

    // mapping / mapping-source: pick a TARGET field
    if (isMapping) {
      return (
        <Popover open={comboOpen} onOpenChange={setComboOpen}>
          <PopoverTrigger asChild>
            <Button variant="outline" role="combobox" aria-expanded={comboOpen} className="h-8 w-[360px] justify-between text-sm">
              {a.manualValue ? `${fieldLabel(a.targetTable, a.manualValue)} (${a.manualValue})` : t("anomaly.searchTarget")}
              <ChevronsUpDown className="ml-2 h-3.5 w-3.5 shrink-0 opacity-50" />
            </Button>
          </PopoverTrigger>
          <PopoverContent className="w-[360px] p-0" align="start">
            <Command>
              <CommandInput placeholder={t("anomaly.searchField")} className="h-9" />
              <CommandList>
                <CommandEmpty>{t("anomaly.noField")}</CommandEmpty>
                <CommandGroup>
                  {getTargetFields(a.targetTable).map((field) => {
                    const label = fieldLabel(a.targetTable, field);
                    const hasLabel = label !== field;
                    return (
                      <CommandItem
                        key={field}
                        value={`${field} ${label}`}
                        onSelect={() => {
                          onUpdateManual(a.id, field);
                          setComboOpen(false);
                        }}
                        className="text-xs"
                      >
                        <Check className={`mr-2 h-3 w-3 shrink-0 ${a.manualValue === field ? "opacity-100" : "opacity-0"}`} />
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
      );
    }

    if (a.category === "flag-drift") {
      return (
        <Select value={a.manualValue || ""} onValueChange={(v) => onUpdateManual(a.id, v)}>
          <SelectTrigger className="h-8 w-[180px] text-sm">
            <SelectValue placeholder={t("anomaly.selectFlag")} />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="H">{t("anomaly.flagHigh")}</SelectItem>
            <SelectItem value="L">{t("anomaly.flagLow")}</SelectItem>
            <SelectItem value="N">{t("anomaly.flagNormal")}</SelectItem>
            <SelectItem value="">{t("anomaly.flagEmpty")}</SelectItem>
          </SelectContent>
        </Select>
      );
    }

    if (a.category === "out-of-range") {
      return (
        <Input
          type="number"
          value={a.manualValue || ""}
          onChange={(e) => onUpdateManual(a.id, e.target.value)}
          placeholder={t("anomaly.enterCorrectValue")}
          className="h-8 text-sm max-w-[180px]"
        />
      );
    }

    if (a.category === "temporal" || a.category === "date-format") {
      return (
        <Input
          value={a.manualValue || ""}
          onChange={(e) => onUpdateManual(a.id, e.target.value)}
          placeholder={t("anomaly.datePlaceholder")}
          className="h-8 text-sm max-w-[180px]"
        />
      );
    }

    // Default: free text input
    return (
      <Input
        value={a.manualValue || ""}
        onChange={(e) => onUpdateManual(a.id, e.target.value)}
        placeholder={t("anomaly.enterCorrectValue")}
        className="h-8 text-sm max-w-xs"
      />
    );
  };

  return (
    <div
      className={`border rounded-lg p-4 transition-colors ${
        a.status === "accepted" ? "border-primary/30 bg-primary/5"
        : a.status === "ignored" ? "border-muted bg-muted/30 opacity-60"
        : "border-border"
      }`}
    >
      <div className="flex items-start justify-between gap-4">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-1 flex-wrap">
            {a.status === "accepted" ? (
              <CheckCircle className="h-4 w-4 text-primary shrink-0" />
            ) : (
              <AlertTriangle className="h-4 w-4 text-destructive shrink-0" />
            )}
            <TypeIcon className="h-3.5 w-3.5 text-accent-foreground shrink-0" />
            <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium uppercase tracking-wide ${badgeClass}`}>
              {badgeLabel}
            </span>
            <span className="text-xs text-muted-foreground">{tableLabel(a.targetTable)}</span>
            <span className="text-sm font-medium">{fieldHasLabel ? fl : a.field}</span>
            {fieldHasLabel && (
              <span className="text-[10px] font-mono text-muted-foreground">{a.field}</span>
            )}
            {a.patientId && (
              <span className="text-xs text-muted-foreground">· {a.patientId} – {a.patientName}</span>
            )}
            {a.confidence !== undefined && a.confidence > 0 && (
              <span className={`text-[10px] px-1.5 py-0.5 rounded font-mono ${
                a.confidence < 0.5 ? "bg-destructive/10 text-destructive" : "bg-accent text-accent-foreground"
              }`}>
                {Math.round(a.confidence * 100)}%
              </span>
            )}
          </div>
          {a.category && (
            <span className="text-[9px] px-1.5 py-0.5 rounded bg-muted text-muted-foreground ml-6 inline-block mb-0.5">{a.category}</span>
          )}
          <p className="text-xs text-muted-foreground ml-6">{a.reason}</p>
          {a.origin && (
            <p className="text-[11px] text-muted-foreground/70 ml-6 mt-0.5 italic">{a.origin}</p>
          )}
          {a.affectedCount != null && a.affectedCount > 1 && (
            <span className="text-[10px] px-1.5 py-0.5 rounded bg-muted text-muted-foreground ml-6 mt-1 inline-block">{t("anomaly.rowsAffected", { count: a.affectedCount! })}</span>
          )}

          {/* Current vs Proposed */}
          <div className="ml-6 mt-2 flex items-center gap-3 text-sm flex-wrap">
            <div className="flex items-center gap-1.5">
              <span className="text-xs text-muted-foreground">{valueLabel}</span>
              <span className={`px-2 py-0.5 rounded text-xs font-mono ${
                a.currentValue ? "bg-destructive/10 text-destructive" : "bg-muted text-muted-foreground italic"
              }`}>
                {a.currentValue || (a.type === "mapping-target" ? fl : t("anomaly.empty"))}
              </span>
            </div>
            {a.proposedValue && (
              <>
                <span className="text-muted-foreground">&rarr;</span>
                <div className="flex items-center gap-1.5">
                  <span className="text-xs text-muted-foreground">{isMapping ? t("anomaly.targetField") : t("anomaly.proposal")}</span>
                  {isMapping ? (() => {
                    const pLabel = fieldLabel(a.targetTable, a.proposedValue);
                    const pHasLabel = pLabel !== a.proposedValue;
                    return (
                      <span className="px-2 py-0.5 rounded bg-primary/10 text-primary text-xs">
                        {pHasLabel ? <>{pLabel} <span className="font-mono text-primary/70">({a.proposedValue})</span></> : <span className="font-mono">{a.proposedValue}</span>}
                      </span>
                    );
                  })() : (
                    <span className="px-2 py-0.5 rounded bg-primary/10 text-primary text-xs font-mono">
                      {a.proposedValue}
                    </span>
                  )}
                </div>
              </>
            )}
          </div>
        </div>

        {/* Actions */}
        {a.status === "pending" && (
          <div className="flex items-center gap-2 shrink-0">
            {a.proposedValue && (
              <Button size="sm" variant="outline" onClick={() => onAccept(a.id)} className="text-primary border-primary/30 hover:bg-primary/10">
                <Check className="h-3.5 w-3.5 mr-1" /> {acceptLabel}
              </Button>
            )}
            {!isInfoOnly && (
              <Button size="sm" variant="outline" onClick={() => onStartManual(a.id)}>
                {t("anomaly.manual")}
              </Button>
            )}
            {onIgnore && (
              <Button size="sm" variant="ghost" onClick={() => onIgnore(a.id)} className="text-muted-foreground hover:text-foreground">
                <EyeOff className="h-3.5 w-3.5 mr-1" /> {t("anomaly.ignore")}
              </Button>
            )}
          </div>
        )}
        {a.status === "accepted" && (
          <span className="text-xs text-primary font-medium shrink-0">&check; {t("anomaly.resolved")}</span>
        )}
        {a.status === "ignored" && (
          <span className="text-xs text-muted-foreground font-medium shrink-0 flex items-center gap-1">
            <EyeOff className="h-3 w-3" /> {t("anomaly.ignored")}
          </span>
        )}
      </div>

      {/* Quick candidate selection for mapping anomalies */}
      {a.status === "pending" && isMapping && a.candidates && a.candidates.length > 0 && (
        <div className="ml-6 mt-2 flex items-center gap-1.5 flex-wrap">
          <span className="text-[10px] text-muted-foreground mr-1">{t("anomaly.suggestions")}</span>
          {a.candidates.map((c) => {
            const label = fieldLabel(a.targetTable, c.target);
            const hasLabel = label !== c.target;
            return (
              <Button
                key={c.target}
                size="sm"
                variant="outline"
                className="h-6 text-[11px] px-2 gap-1.5 font-normal"
                onClick={() => onQuickSelect?.(a.id, c.target)}
              >
                <span className="truncate max-w-[180px]">
                  {hasLabel ? label : c.target}
                </span>
                <span className={`text-[9px] font-mono px-1 py-0 rounded ${
                  c.confidence >= 0.5 ? "bg-primary/10 text-primary" : "bg-muted text-muted-foreground"
                }`}>
                  {Math.round(c.confidence * 100)}%
                </span>
              </Button>
            );
          })}
        </div>
      )}

      {/* Manual Edit Mode */}
      {a.status === "manual" && (
        <div className="ml-6 mt-3 flex items-center gap-2">
          {renderManualEditor()}
          <Button size="sm" onClick={() => onConfirmManual(a.id)} className="bg-primary text-primary-foreground hover:bg-primary/90 h-8">
            <Check className="h-3.5 w-3.5 mr-1" /> {t("anomaly.confirm")}
          </Button>
          <Button size="sm" variant="ghost" onClick={() => onCancelManual(a.id)} className="h-8">
            <X className="h-3.5 w-3.5" />
          </Button>
        </div>
      )}
    </div>
  );
};

export default AnomalyCard;
