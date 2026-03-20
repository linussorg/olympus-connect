import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Database, CheckCircle, AlertCircle, Brain, Eye, EyeOff, Trash2, Send, Globe } from "lucide-react";
import { testConnection, setLlmConfig, testLlm, getLlmConfig, truncateAllTables } from "@/lib/api";
import { useI18n, translate } from "@/lib/i18n";

const Settings = () => {
  const { t, locale, setLocale } = useI18n();
  const [connectionStatus, setConnectionStatus] = useState<"idle" | "testing" | "success" | "error">("idle");
  const [errorMessage, setErrorMessage] = useState("");
  const [llmStatus, setLlmStatus] = useState<"idle" | "saving" | "testing" | "success" | "error">("idle");
  const [llmError, setLlmError] = useState("");
  const [llmForm, setLlmForm] = useState({
    apiKey: "",
    model: "qwen/qwen3-32b",
  });
  const [apiKeyMasked, setApiKeyMasked] = useState("");
  const [hasApiKey, setHasApiKey] = useState(false);
  const [apiKeyEdited, setApiKeyEdited] = useState(false);
  const [showApiKey, setShowApiKey] = useState(false);
  const updateLlm = (key: string, value: string) => setLlmForm((prev) => ({ ...prev, [key]: value }));

  useEffect(() => {
    getLlmConfig()
      .then((cfg) => {
        setLlmForm((prev) => ({ ...prev, model: cfg.model }));
        setApiKeyMasked(cfg.apiKeyMasked || "");
        setHasApiKey(cfg.hasApiKey || false);
      })
      .catch(() => {});
  }, []);

  const [form, setForm] = useState({
    host: "localhost",
    port: "1433",
    database: "Hack2026",
    user: "sa",
    password: "Hack2026Pass",
  });

  const update = (key: string, value: string) => setForm((prev) => ({ ...prev, [key]: value }));

  const handleTestConnection = async () => {
    setConnectionStatus("testing");
    setErrorMessage("");
    try {
      const result = await testConnection();
      if (result.ok) {
        setConnectionStatus("success");
      } else {
        setConnectionStatus("error");
        setErrorMessage(result.error || t("settings.connectionFailed"));
      }
    } catch (err: any) {
      setConnectionStatus("error");
      setErrorMessage(err.message || t("settings.engineUnreachable"));
    }
  };

  const handleSaveLlm = async () => {
    setLlmStatus("saving");
    setLlmError("");
    try {
      const cfg: { provider?: string; apiKey?: string; model?: string } = {
        model: llmForm.model,
      };
      // Only send apiKey if the user actually edited it
      if (apiKeyEdited) {
        cfg.apiKey = llmForm.apiKey;
        cfg.provider = llmForm.apiKey ? "openrouter" : "ollama";
      } else if (hasApiKey) {
        cfg.provider = "openrouter";
      } else {
        cfg.provider = "ollama";
      }

      const result = await setLlmConfig(cfg);
      setApiKeyMasked(result.apiKeyMasked || "");
      setHasApiKey(result.hasApiKey || false);
      setApiKeyEdited(false);
      setLlmForm((prev) => ({ ...prev, apiKey: "" }));

      setLlmStatus("testing");
      const test = await testLlm();
      if (test.ok) {
        setLlmStatus("success");
      } else {
        setLlmStatus("error");
        setLlmError(test.error || t("settings.llmUnreachable"));
      }
    } catch (err: any) {
      setLlmStatus("error");
      setLlmError(err.message || t("settings.engineUnreachable"));
    }
  };

  const displayApiKey = apiKeyEdited ? llmForm.apiKey : apiKeyMasked;

  return (
    <div className="p-6 space-y-6 max-w-2xl">
      <div>
        <h2 className="text-xl font-semibold text-foreground">{t("settings.title")}</h2>
        <p className="text-sm text-muted-foreground">{t("settings.subtitle")}</p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-base flex items-center gap-2">
            <Database className="h-4 w-4 text-primary" />
            {t("settings.dbConnection")}
          </CardTitle>
          <CardDescription>
            {t("settings.dbDescription")}
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-1.5">
              <Label htmlFor="host">{t("settings.host")}</Label>
              <Input id="host" placeholder={t("settings.hostPlaceholder")} value={form.host} onChange={(e) => update("host", e.target.value)} />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="port">{t("settings.port")}</Label>
              <Input id="port" placeholder="1433" value={form.port} onChange={(e) => update("port", e.target.value)} />
            </div>
          </div>
          <div className="space-y-1.5">
            <Label htmlFor="database">{t("settings.database")}</Label>
            <Input id="database" placeholder={t("settings.dbPlaceholder")} value={form.database} onChange={(e) => update("database", e.target.value)} />
          </div>
          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-1.5">
              <Label htmlFor="user">{t("settings.user")}</Label>
              <Input id="user" placeholder={t("settings.userPlaceholder")} value={form.user} onChange={(e) => update("user", e.target.value)} />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="password">{t("settings.password")}</Label>
              <Input id="password" type="password" placeholder="••••••••" value={form.password} onChange={(e) => update("password", e.target.value)} />
            </div>
          </div>

          <div className="flex items-center gap-3 pt-2">
            <Button onClick={handleTestConnection} disabled={connectionStatus === "testing"}>
              {connectionStatus === "testing" ? t("settings.testingConnection") : t("settings.testConnection")}
            </Button>
            {connectionStatus === "success" && (
              <span className="flex items-center gap-1.5 text-sm text-primary">
                <CheckCircle className="h-4 w-4" /> {t("settings.connectionSuccess")}
              </span>
            )}
            {connectionStatus === "error" && (
              <span className="flex items-center gap-1.5 text-sm text-destructive">
                <AlertCircle className="h-4 w-4" /> {errorMessage || t("settings.connectionFailed")}
              </span>
            )}
          </div>
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle className="text-base flex items-center gap-2">
            <Brain className="h-4 w-4 text-primary" />
            {t("settings.llmTitle")}
          </CardTitle>
          <CardDescription>
            {t("settings.llmDescription")}
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-1.5">
            <Label htmlFor="apiKey">{t("settings.apiKey")}</Label>
            <div className="relative">
              <Input
                id="apiKey"
                type={showApiKey || apiKeyEdited ? "text" : "password"}
                placeholder="sk-or-..."
                value={displayApiKey}
                onChange={(e) => {
                  setApiKeyEdited(true);
                  updateLlm("apiKey", e.target.value);
                }}
                onFocus={() => {
                  if (!apiKeyEdited && hasApiKey) {
                    setApiKeyEdited(true);
                    updateLlm("apiKey", "");
                  }
                }}
                className="pr-10"
              />
              <Button
                type="button"
                variant="ghost"
                size="sm"
                className="absolute right-0 top-0 h-full px-3 hover:bg-transparent"
                onClick={() => setShowApiKey(!showApiKey)}
              >
                {showApiKey ? <EyeOff className="h-4 w-4 text-muted-foreground" /> : <Eye className="h-4 w-4 text-muted-foreground" />}
              </Button>
            </div>
            {hasApiKey && !apiKeyEdited && (
              <p className="text-xs text-muted-foreground">{t("settings.apiKeySaved")}</p>
            )}
          </div>
          <div className="space-y-1.5">
            <Label htmlFor="model">{t("settings.model")}</Label>
            <Input id="model" placeholder="qwen/qwen3-32b" value={llmForm.model} onChange={(e) => updateLlm("model", e.target.value)} />
          </div>

          <div className="flex items-center gap-3 pt-2">
            <Button onClick={handleSaveLlm} disabled={llmStatus === "saving" || llmStatus === "testing"}>
              {llmStatus === "saving" || llmStatus === "testing" ? t("settings.testing") : t("settings.saveAndTest")}
            </Button>
            {llmStatus === "success" && (
              <span className="flex items-center gap-1.5 text-sm text-primary">
                <CheckCircle className="h-4 w-4" /> {t("settings.llmReachable")}
              </span>
            )}
            {llmStatus === "error" && (
              <span className="flex items-center gap-1.5 text-sm text-destructive">
                <AlertCircle className="h-4 w-4" /> {llmError || t("settings.llmUnreachable")}
              </span>
            )}
          </div>
        </CardContent>
      </Card>
      <HermesSignatureSettings />
      <Card>
        <CardHeader>
          <CardTitle className="text-base flex items-center gap-2">
            <Globe className="h-4 w-4 text-primary" />
            {t("settings.languageTitle")}
          </CardTitle>
          <CardDescription>{t("settings.languageDescription")}</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="flex gap-2">
            <Button variant={locale === "de" ? "default" : "outline"} onClick={() => setLocale("de")}>Deutsch</Button>
            <Button variant={locale === "en" ? "default" : "outline"} onClick={() => setLocale("en")}>English</Button>
          </div>
        </CardContent>
      </Card>
      <DangerZone />
    </div>
  );
};

const HERMES_SIGNATURE_KEY = "hermes-signature";
const DEFAULT_HERMES_SIGNATURE = () => translate("email.defaultSignature");

function getHermesSignature(): string {
  return localStorage.getItem(HERMES_SIGNATURE_KEY) || DEFAULT_HERMES_SIGNATURE();
}

function HermesSignatureSettings() {
  const { t } = useI18n();
  const [signature, setSignature] = useState(() => getHermesSignature());
  const [saved, setSaved] = useState(false);

  const handleSave = () => {
    localStorage.setItem(HERMES_SIGNATURE_KEY, signature);
    setSaved(true);
    setTimeout(() => setSaved(false), 2000);
  };

  const handleReset = () => {
    const defaultSig = DEFAULT_HERMES_SIGNATURE();
    setSignature(defaultSig);
    localStorage.setItem(HERMES_SIGNATURE_KEY, defaultSig);
    setSaved(true);
    setTimeout(() => setSaved(false), 2000);
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base flex items-center gap-2">
          <Send className="h-4 w-4 text-primary" />
          {t("settings.signatureTitle")}
        </CardTitle>
        <CardDescription>
          {t("settings.signatureDescription")}
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="space-y-1.5">
          <Label htmlFor="hermes-signature">{t("settings.signature")}</Label>
          <Textarea
            id="hermes-signature"
            value={signature}
            onChange={(e) => setSignature(e.target.value)}
            className="text-sm min-h-[100px]"
            placeholder={t("settings.signaturePlaceholder")}
          />
        </div>
        <div className="flex items-center gap-3">
          <Button onClick={handleSave}>{t("common.save")}</Button>
          <Button variant="outline" onClick={handleReset}>{t("common.reset")}</Button>
          {saved && (
            <span className="flex items-center gap-1.5 text-sm text-primary">
              <CheckCircle className="h-4 w-4" /> {t("common.saved")}
            </span>
          )}
        </div>
      </CardContent>
    </Card>
  );
}

function DangerZone() {
  const { t } = useI18n();
  const [confirm, setConfirm] = useState(false);
  const [status, setStatus] = useState<"idle" | "deleting" | "done" | "error">("idle");
  const [errorMsg, setErrorMsg] = useState("");

  const handleTruncate = async () => {
    if (!confirm) {
      setConfirm(true);
      return;
    }
    setStatus("deleting");
    setErrorMsg("");
    try {
      await truncateAllTables();
      setStatus("done");
      setConfirm(false);
    } catch (err: any) {
      setStatus("error");
      setErrorMsg(err.message);
    }
  };

  return (
    <Card className="border-destructive/30">
      <CardHeader>
        <CardTitle className="text-base flex items-center gap-2 text-destructive">
          <Trash2 className="h-4 w-4" />
          {t("settings.dangerZone")}
        </CardTitle>
        <CardDescription>
          {t("settings.dangerDescription")}
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-3">
        <div className="flex items-center gap-3">
          <Button
            variant="destructive"
            onClick={handleTruncate}
            disabled={status === "deleting"}
          >
            <Trash2 className="h-3.5 w-3.5 mr-1.5" />
            {status === "deleting" ? t("settings.deleting") : confirm ? t("settings.confirmDelete") : t("settings.truncateAll")}
          </Button>
          {confirm && status === "idle" && (
            <Button variant="outline" size="sm" onClick={() => setConfirm(false)}>
              {t("common.cancel")}
            </Button>
          )}
          {status === "done" && (
            <span className="flex items-center gap-1.5 text-sm text-primary">
              <CheckCircle className="h-4 w-4" /> {t("settings.allTruncated")}
            </span>
          )}
          {status === "error" && (
            <span className="flex items-center gap-1.5 text-sm text-destructive">
              <AlertCircle className="h-4 w-4" /> {errorMsg}
            </span>
          )}
        </div>
      </CardContent>
    </Card>
  );
}

export default Settings;
