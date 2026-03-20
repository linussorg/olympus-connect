// ─── Configuration ───
// Set LLM_PROVIDER=openrouter to use OpenRouter instead of local Ollama
// Set OPENROUTER_API_KEY for authentication
// Set OPENROUTER_MODEL to override the model (default: qwen/qwen3-32b)

import { readFileSync, writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const CONFIG_PATH = join(__dirname, "..", ".llm-config.json");

let LLM_PROVIDER = process.env.LLM_PROVIDER || "ollama";

let OLLAMA_URL = process.env.OLLAMA_URL || "http://localhost:11434";
let OLLAMA_MODEL = process.env.OLLAMA_MODEL || "qwen3:32b";

const OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions";
let OPENROUTER_API_KEY = process.env.OPENROUTER_API_KEY || "";
let OPENROUTER_MODEL = process.env.OPENROUTER_MODEL || "qwen/qwen3-32b";

export const DEFAULT_MODEL = "qwen/qwen3-32b";

// Load persisted config on startup
try {
  const saved = JSON.parse(readFileSync(CONFIG_PATH, "utf-8"));
  if (saved.provider) LLM_PROVIDER = saved.provider;
  if (saved.apiKey) OPENROUTER_API_KEY = saved.apiKey;
  if (saved.openrouterModel) OPENROUTER_MODEL = saved.openrouterModel;
  if (saved.ollamaModel) OLLAMA_MODEL = saved.ollamaModel;
  console.log(`[llm] Loaded saved config: ${LLM_PROVIDER} (${getActiveModel()})`);
} catch {
  // No saved config — use defaults/env vars
}

function getActiveModel(): string {
  return getProvider() === "openrouter" ? OPENROUTER_MODEL : OLLAMA_MODEL;
}

function persistConfig() {
  try {
    mkdirSync(dirname(CONFIG_PATH), { recursive: true });
    writeFileSync(CONFIG_PATH, JSON.stringify({
      provider: LLM_PROVIDER,
      apiKey: OPENROUTER_API_KEY,
      openrouterModel: OPENROUTER_MODEL,
      ollamaModel: OLLAMA_MODEL,
    }, null, 2), "utf-8");
  } catch (err) {
    console.error("[llm] Failed to persist config:", err);
  }
}

export function setConfig(cfg: { provider?: string; apiKey?: string; model?: string }) {
  if (cfg.apiKey !== undefined) OPENROUTER_API_KEY = cfg.apiKey;
  if (cfg.model) {
    if (cfg.model.includes("/")) {
      OPENROUTER_MODEL = cfg.model;
    } else {
      OLLAMA_MODEL = cfg.model;
    }
  }
  if (cfg.provider) {
    LLM_PROVIDER = cfg.provider;
  } else if (OPENROUTER_API_KEY) {
    LLM_PROVIDER = "openrouter";
  }
  persistConfig();
}

function getProvider(): string {
  if (LLM_PROVIDER === "openrouter") return "openrouter";
  if (OPENROUTER_API_KEY && LLM_PROVIDER !== "ollama") return "openrouter";
  return "ollama";
}

function maskApiKey(key: string): string {
  if (!key || key.length < 8) return key ? "••••••••" : "";
  return key.slice(0, 5) + "••••••••" + key.slice(-4);
}

export function getProviderInfo(): { provider: string; model: string; url: string; apiKeyMasked: string; hasApiKey: boolean } {
  const p = getProvider();
  if (p === "openrouter") {
    return { provider: "openrouter", model: OPENROUTER_MODEL, url: OPENROUTER_URL, apiKeyMasked: maskApiKey(OPENROUTER_API_KEY), hasApiKey: !!OPENROUTER_API_KEY };
  }
  return { provider: "ollama", model: OLLAMA_MODEL, url: OLLAMA_URL, apiKeyMasked: "", hasApiKey: false };
}

// ─── Availability Check ───

export async function isAvailable(): Promise<boolean> {
  const provider = getProvider();

  if (provider === "openrouter") {
    if (!OPENROUTER_API_KEY) {
      console.error("OPENROUTER_API_KEY not set");
      return false;
    }
    try {
      // Just check that the key works with a minimal request
      const res = await fetch(OPENROUTER_URL, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${OPENROUTER_API_KEY}`,
        },
        body: JSON.stringify({
          model: OPENROUTER_MODEL,
          messages: [{ role: "user", content: "hi" }],
          max_tokens: 1,
        }),
        signal: AbortSignal.timeout(15000),
      });
      if (!res.ok) {
        const body = await res.text().catch(() => "");
        console.error(`OpenRouter availability check failed: ${res.status} ${body}`);
      }
      return res.ok;
    } catch (err) {
      console.error("OpenRouter availability check error:", err);
      return false;
    }
  }

  // Ollama
  try {
    const res = await fetch(`${OLLAMA_URL}/api/tags`, {
      signal: AbortSignal.timeout(2000),
    });
    return res.ok;
  } catch {
    return false;
  }
}

// ─── Generate (unified interface) ───

export async function generate(
  prompt: string,
  options?: { system?: string; temperature?: number; format?: "json" | object },
): Promise<string> {
  const provider = getProvider();

  if (provider === "openrouter") {
    return generateOpenRouter(prompt, options);
  }
  return generateOllama(prompt, options);
}

// ─── Ollama Backend ───

async function generateOllama(
  prompt: string,
  options?: { system?: string; temperature?: number; format?: "json" | object },
): Promise<string> {
  const res = await fetch(`${OLLAMA_URL}/api/generate`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      model: OLLAMA_MODEL,
      prompt,
      system: options?.system,
      stream: false,
      format: options?.format,
      options: {
        temperature: options?.temperature ?? 0.05,
        num_predict: 4096,
      },
    }),
  });

  if (!res.ok) {
    throw new Error(`Ollama error: ${res.status} ${await res.text()}`);
  }

  const data = await res.json();
  return data.response;
}

// ─── OpenRouter Backend ───

async function generateOpenRouter(
  prompt: string,
  options?: { system?: string; temperature?: number; format?: "json" | object },
): Promise<string> {
  const messages: { role: string; content: string }[] = [];

  if (options?.system) {
    messages.push({ role: "system", content: options.system });
  }
  messages.push({ role: "user", content: prompt });

  const body: Record<string, any> = {
    model: OPENROUTER_MODEL,
    messages,
    temperature: options?.temperature ?? 0.05,
    max_tokens: 4096,
  };

  // OpenRouter supports JSON mode via response_format
  if (options?.format === "json") {
    body.response_format = { type: "json_object" };
  }

  const res = await fetch(OPENROUTER_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${OPENROUTER_API_KEY}`,
      "X-Title": "epaCC Data Mapper",
    },
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    const errText = await res.text();
    throw new Error(`OpenRouter error: ${res.status} ${errText}`);
  }

  const data = await res.json();
  const content = data.choices?.[0]?.message?.content;
  if (!content) {
    throw new Error(`OpenRouter returned empty response: ${JSON.stringify(data)}`);
  }
  return content;
}

// ─── Streaming Generate ───

export async function generateStream(
  prompt: string,
  options: { system?: string; temperature?: number; format?: "json" | object },
  onChunk: (token: string) => void,
): Promise<string> {
  const provider = getProvider();
  if (provider === "openrouter") {
    return streamOpenRouter(prompt, options, onChunk);
  }
  return streamOllama(prompt, options, onChunk);
}

async function streamOllama(
  prompt: string,
  options: { system?: string; temperature?: number; format?: "json" | object },
  onChunk: (token: string) => void,
): Promise<string> {
  const res = await fetch(`${OLLAMA_URL}/api/generate`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      model: OLLAMA_MODEL,
      prompt,
      system: options.system,
      stream: true,
      format: options.format,
      options: { temperature: options.temperature ?? 0.05, num_predict: 4096 },
    }),
  });
  if (!res.ok) throw new Error(`Ollama error: ${res.status} ${await res.text()}`);

  let full = "";
  const reader = res.body as AsyncIterable<Uint8Array>;
  const decoder = new TextDecoder();
  let buf = "";

  for await (const chunk of reader) {
    buf += decoder.decode(chunk, { stream: true });
    const lines = buf.split("\n");
    buf = lines.pop() || "";
    for (const line of lines) {
      if (!line.trim()) continue;
      try {
        const data = JSON.parse(line);
        if (data.response) {
          full += data.response;
          onChunk(data.response);
        }
      } catch { /* skip malformed lines */ }
    }
  }
  return full;
}

async function streamOpenRouter(
  prompt: string,
  options: { system?: string; temperature?: number; format?: "json" | object },
  onChunk: (token: string) => void,
): Promise<string> {
  const messages: { role: string; content: string }[] = [];
  if (options.system) messages.push({ role: "system", content: options.system });
  messages.push({ role: "user", content: prompt });

  const body: Record<string, any> = {
    model: OPENROUTER_MODEL,
    messages,
    temperature: options.temperature ?? 0.05,
    max_tokens: 4096,
    stream: true,
  };
  if (options.format === "json") {
    body.response_format = { type: "json_object" };
  }

  const res = await fetch(OPENROUTER_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${OPENROUTER_API_KEY}`,
      "X-Title": "epaCC Data Mapper",
    },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`OpenRouter error: ${res.status} ${await res.text()}`);

  let full = "";
  const reader = res.body as AsyncIterable<Uint8Array>;
  const decoder = new TextDecoder();
  let buf = "";

  for await (const chunk of reader) {
    buf += decoder.decode(chunk, { stream: true });
    const lines = buf.split("\n");
    buf = lines.pop() || "";
    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed.startsWith("data: ") || trimmed === "data: [DONE]") continue;
      try {
        const data = JSON.parse(trimmed.slice(6));
        const delta = data.choices?.[0]?.delta?.content;
        if (delta) {
          full += delta;
          onChunk(delta);
        }
      } catch { /* skip */ }
    }
  }
  return full;
}

// ─── Mapping Prompt ───

export async function generateMapping(
  headers: string[],
  sampleRows: string[][],
  targetSchemaPrompt: string,
): Promise<string> {
  const sampleData = sampleRows
    .slice(0, 3)
    .map((row) => headers.map((h, i) => `${h}: ${row[i] || "(empty)"}`).join(", "))
    .join("\n");

  const prompt = `Map these source CSV columns to the target database table. Use the column names, data types, descriptions, and sample values to infer the correct mapping. Source columns may use abbreviations, different languages (e.g. German), or alternative naming conventions.

SOURCE COLUMNS: ${JSON.stringify(headers)}

SAMPLE DATA:
${sampleData}

TARGET TABLE SCHEMA:
${targetSchemaPrompt}

RULES:
- Every source column that has a plausible match in the target schema should be mapped
- The "target" field must be an exact column name from the schema above
- Use sample data values to disambiguate when column names alone are unclear
- Set confidence 0-1 based on how certain the match is

Return a JSON object with a "mappings" key containing an array of ALL mappings:
{"mappings": [{"source":"<exact source col>","target":"<exact target col>","transform":"<transform>","confidence":<0-1>}, ...]}
Valid transforms: "none", "parseFloat", "parseInt", "normalizeCaseId", "normalizePatientId", "normalizeDate", "normalizeNull", "normalizeShift", "normalizeFreeText"`;

  return generate(prompt, {
    system: `You are a data mapping engine. Return a JSON object: {"mappings": [...]}. The mappings array must contain one entry for every source column that has a plausible target. No extra text.`,
    format: "json",
    temperature: 0.05,
  });
}

/**
 * Stream a mapping request, calling onMapping for each parsed mapping object
 * as it completes in the JSON array. Returns the full raw response string.
 */
export async function generateMappingStream(
  headers: string[],
  sampleRows: string[][],
  targetSchemaPrompt: string,
  onMapping: (mapping: { source: string; target: string; transform?: string; confidence?: number }) => void,
): Promise<string> {
  const sampleData = sampleRows
    .slice(0, 3)
    .map((row) => headers.map((h, i) => `${h}: ${row[i] || "(empty)"}`).join(", "))
    .join("\n");

  const prompt = `Map these source CSV columns to the target database table. Use the column names, data types, descriptions, and sample values to infer the correct mapping. Source columns may use abbreviations, different languages (e.g. German), or alternative naming conventions.

SOURCE COLUMNS: ${JSON.stringify(headers)}

SAMPLE DATA:
${sampleData}

TARGET TABLE SCHEMA:
${targetSchemaPrompt}

RULES:
- Every source column that has a plausible match in the target schema should be mapped
- The "target" field must be an exact column name from the schema above
- Use sample data values to disambiguate when column names alone are unclear
- Set confidence 0-1 based on how certain the match is

Return a JSON object with a "mappings" key containing an array of ALL mappings:
{"mappings": [{"source":"<exact source col>","target":"<exact target col>","transform":"<transform>","confidence":<0-1>}, ...]}
Valid transforms: "none", "parseFloat", "parseInt", "normalizeCaseId", "normalizePatientId", "normalizeDate", "normalizeNull", "normalizeShift", "normalizeFreeText"`;

  // Accumulate JSON and try to extract complete objects as they arrive
  let accumulated = "";
  let lastParsedEnd = 0;

  const full = await generateStream(
    prompt,
    {
      system: `You are a data mapping engine. Return a JSON object: {"mappings": [...]}. The mappings array must contain one entry for every source column that has a plausible target. No extra text.`,
      format: "json",
      temperature: 0.05,
    },
    (token) => {
      accumulated += token;

      // Try to find complete JSON objects in the array
      // Look for }, patterns after the last parsed position
      let searchFrom = lastParsedEnd;
      while (true) {
        const braceEnd = accumulated.indexOf("}", searchFrom);
        if (braceEnd === -1) break;

        // Find the matching { before this }
        let braceStart = -1;
        let depth = 0;
        for (let i = braceEnd; i >= lastParsedEnd; i--) {
          if (accumulated[i] === "}") depth++;
          if (accumulated[i] === "{") {
            depth--;
            if (depth === 0) { braceStart = i; break; }
          }
        }

        if (braceStart >= 0) {
          const objStr = accumulated.slice(braceStart, braceEnd + 1);
          try {
            const obj = JSON.parse(objStr);
            if (obj.source && obj.target) {
              onMapping(obj);
              lastParsedEnd = braceEnd + 1;
            }
          } catch { /* incomplete JSON, wait for more */ }
        }
        searchFrom = braceEnd + 1;
      }
    },
  );

  return full;
}
