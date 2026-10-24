import type {
  ExtensionAPI,
  ExtensionContext,
  ExtensionCommandContext,
  ExtensionUIContext,
  ModelRegistry,
  CustomEntry,
  SessionEntry,
} from "@earendil-works/pi-coding-agent";
import {
  createAssistantMessageEventStream,
  type AssistantMessage,
  type AssistantMessageEvent,
  type AssistantMessageEventStream,
  type Api,
  type Context,
  type Model,
  type Provider,
  type SimpleStreamOptions,
} from "@earendil-works/pi-ai";
import { existsSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

// ─── Types ───────────────────────────────────────────────────────────

interface ProviderConfig {
  name: string;
  defaultModel: string;
  keys?: string[];
}

interface FallbackConfig {
  providers: ProviderConfig[];
}

type StreamFn = (
  model: Model<Api>,
  context: Context,
  options?: SimpleStreamOptions,
) => AssistantMessageEventStream;

type ProxyResult =
  | { result: "success" }
  | { result: "rate-limit"; lastError: string }
  | { result: "error"; lastError: string };

interface ManualCandidate {
  providerName: string;
  model: Model<Api>;
  label: string;
}

/** A single attempt within the fallback chain. */
interface Attempt {
  options?: SimpleStreamOptions;
  label: string;
  /** Notification shown when this attempt succeeds after a rate limit. */
  successNotice?: string;
}

const MANUAL_CANCEL = "Cancel — fail this request";
const MAX_MANUAL_OPTIONS_PER_PROVIDER = 3;
const MAX_MANUAL_PROMPT_CHOICES = 12;
const STARTER_EVENTS = new Set(["text_delta", "thinking_delta", "text_start", "thinking_start"]);

// ─── State ───────────────────────────────────────────────────────────

let fallbackConfig: FallbackConfig = { providers: [] };
let uiRef: ExtensionUIContext | null = null;
let modelsRef: ModelRegistry | null = null;
let currentProviderId: string | null = null;
let currentModelId: string | null = null;

/** Original `streamSimple` per wrapped provider, to call through on passthrough. */
const providerOriginals = new Map<string, StreamFn>();

const GLOBAL_CONFIG_PATH = join(homedir(), ".pi/agent/fallback.json");

function notify(message: string, type: "info" | "warning" | "error"): void {
  uiRef?.notify(message, type);
}

// ─── Config persistence ──────────────────────────────────────────────

function loadGlobalConfig(): void {
  fallbackConfig = { providers: [] };
  if (!existsSync(GLOBAL_CONFIG_PATH)) return;
  try {
    const raw = JSON.parse(readFileSync(GLOBAL_CONFIG_PATH, "utf8"));
    if (Array.isArray(raw?.providers)) fallbackConfig = raw as FallbackConfig;
  } catch {
    fallbackConfig = { providers: [] };
  }
}

// `saveGlobalConfig` is intentionally omitted: no command mutates the config
// today. Re-add it when a write path is introduced.

function restoreConfigFromSession(entries: SessionEntry[]): void {
  for (let i = entries.length - 1; i >= 0; i--) {
    const entry = entries[i] as Partial<CustomEntry> | SessionEntry;
    if (entry.type === "custom" && entry.customType === "fallback-config") {
      const data = (entry as CustomEntry<FallbackConfig>).data;
      if (Array.isArray(data?.providers)) fallbackConfig = data;
      break;
    }
  }
}

// ─── Provider wrapping ───────────────────────────────────────────────

function wrapProviderIfNeeded(providerId: string): void {
  if (!modelsRef) return;
  const provider = modelsRef.getProvider(providerId) as
    | (Provider<Api> & { __fallbackWrapped?: boolean })
    | undefined;
  if (!provider || provider.__fallbackWrapped) return;

  const original = provider.streamSimple.bind(provider) as StreamFn;
  providerOriginals.set(providerId, original);
  provider.__fallbackWrapped = true;
  provider.streamSimple = createFallbackStream(providerId, original);
}

// ─── Stream proxying ─────────────────────────────────────────────────

function isRateLimitError(message: string): boolean {
  const lower = message.toLowerCase();
  return (
    lower.includes("rate limit") ||
    lower.includes("429") ||
    lower.includes("too many requests") ||
    lower.includes("overloaded") ||
    lower.includes("529") ||
    lower.includes("quota exceeded")
  );
}

/**
 * Pump events from `source` into `target`, buffering until the first real
 * content/thinking delta so a banner-only preamble can be discarded on error.
 * Returns whether the stream succeeded, hit a rate-limit, or errored.
 */
async function proxyStream(
  source: AssistantMessageEventStream,
  target: AssistantMessageEventStream,
  attemptName: string,
): Promise<ProxyResult> {
  let buffer: AssistantMessageEvent[] = [];
  let flushed = false;
  let lastError = "";

  const flush = () => {
    if (flushed) return;
    for (const e of buffer) target.push(e);
    buffer = [];
    flushed = true;
  };

  for await (const event of source) {
    if (event.type === "error") {
      lastError = event.error.errorMessage ?? "";
      if (isRateLimitError(lastError)) {
        if (!flushed) {
          notify(`⚠️ Rate limit on ${attemptName}. Trying next fallback…`, "warning");
        }
        return { result: "rate-limit", lastError };
      }
      flush();
      target.push(event);
      target.end();
      return { result: "error", lastError };
    }

    if (event.type === "done") {
      flush();
      target.push(event);
      target.end();
      return { result: "success" };
    }

    if (flushed) {
      target.push(event);
    } else {
      buffer.push(event);
      if (STARTER_EVENTS.has(event.type)) flush();
    }
  }

  return { result: "rate-limit", lastError: "Stream ended unexpectedly" };
}

/** Run a sequence of attempts against the same `streamFn`/`model`. */
async function runAttempts(
  streamFn: StreamFn,
  model: Model<Api>,
  context: Context,
  attempts: Attempt[],
  target: AssistantMessageEventStream,
): Promise<ProxyResult> {
  let result: ProxyResult = {
    result: "rate-limit",
    lastError: "No attempts provided",
  };

  for (const attempt of attempts) {
    result = await proxyStream(streamFn(model, context, attempt.options), target, attempt.label);
    if (result.result === "success") {
      if (attempt.successNotice) notify(attempt.successNotice, "info");
      return result;
    }
    // On a hard error the stream is already finalized in `target`.
    if (result.result === "error") return result;
  }

  return result;
}

/** Build attempts for default auth then each extra key. */
function defaultThenKeysAttempts(
  options: SimpleStreamOptions | undefined,
  keys: string[] | undefined,
  baseLabel: string,
): Attempt[] {
  const attempts: Attempt[] = [
    {
      options,
      label: `${baseLabel} (default)`,
      successNotice: `✅ Switched to ${baseLabel} after rate limit`,
    },
  ];
  for (let i = 0; i < (keys?.length ?? 0); i++) {
    attempts.push({
      options: { ...options, apiKey: keys![i] },
      label: `${baseLabel} (key ${i + 1})`,
      successNotice: `✅ Switched to ${baseLabel} (key ${i + 1}) after rate limit`,
    });
  }
  return attempts;
}

/** Build attempts for only the extra keys (default already tried upstream). */
function keysOnlyAttempts(
  options: SimpleStreamOptions | undefined,
  keys: string[] | undefined,
  baseLabel: string,
): Attempt[] {
  const attempts: Attempt[] = [];
  for (let i = 0; i < (keys?.length ?? 0); i++) {
    attempts.push({
      options: { ...options, apiKey: keys![i] },
      label: `${baseLabel} (key ${i + 1})`,
      successNotice: `✅ Switched to ${baseLabel} (key ${i + 1}) after rate limit`,
    });
  }
  return attempts;
}

// ─── Manual model selection ───────────────────────────────────────────

function buildManualCandidates(cfg: ProviderConfig, fbProvider: Provider<Api>): ManualCandidate[] {
  const allModels = fbProvider.getModels();
  if (allModels.length === 0) return [];

  const added = new Set<string>();
  const out: ManualCandidate[] = [];
  const push = (m: Model<Api>, suffix: string) => {
    if (added.has(m.id)) return;
    out.push({ providerName: cfg.name, model: m, label: `${cfg.name}: ${m.id}${suffix}` });
    added.add(m.id);
  };

  const defaultModelObj = allModels.find((m) => m.id === cfg.defaultModel);
  if (defaultModelObj) push(defaultModelObj, " (default)");

  for (const m of allModels) {
    push(m, "");
    if (added.size >= MAX_MANUAL_OPTIONS_PER_PROVIDER) break;
  }
  return out;
}

async function tryManualSelection(
  manualCandidates: ManualCandidate[],
  model: Model<Api>,
  context: Context,
  options: SimpleStreamOptions | undefined,
  target: AssistantMessageEventStream,
): Promise<ProxyResult | null> {
  if (!uiRef) return null;

  const labels = manualCandidates.slice(0, MAX_MANUAL_PROMPT_CHOICES).map((c) => c.label);
  labels.push(MANUAL_CANCEL);

  const choice = await uiRef.select(
    `No provider has model "${model.id}". Pick another model to continue:`,
    labels,
  );
  if (!choice || choice === MANUAL_CANCEL) return null;

  const selected = manualCandidates.find((c) => c.label === choice);
  if (!selected) return null;

  const fbProvider = modelsRef?.getProvider(selected.providerName) as Provider<Api> | undefined;
  if (!fbProvider) return null;

  const baseLabel = `${selected.providerName}/${selected.model.id} (manual)`;
  const candidateCfg = fallbackConfig.providers.find((p) => p.name === selected.providerName);

  const attempts = defaultThenKeysAttempts(options, candidateCfg?.keys, baseLabel);
  // Override the success notice for the manual default to match the original wording.
  if (attempts[0]) {
    attempts[0].successNotice = `✅ Manually switched to ${baseLabel}`;
  }
  return runAttempts(
    fbProvider.streamSimple.bind(fbProvider) as StreamFn,
    selected.model,
    context,
    attempts,
    target,
  );
}

// ─── Fallback chain orchestration ────────────────────────────────────

function createFallbackStream(providerId: string, originalStreamSimple: StreamFn): StreamFn {
  return (model, context, options): AssistantMessageEventStream => {
    // Only intercept the active model — a fallback call into another provider's
    // wrapped object would otherwise recurse.
    const isActive = model.provider === currentProviderId && model.id === currentModelId;
    if (!isActive) return originalStreamSimple(model, context, options);

    const stream = createAssistantMessageEventStream();
    void runFallbackChain(providerId, originalStreamSimple, model, context, options, stream).catch(
      () => {
        stream.end();
      },
    );
    return stream;
  };
}

async function runFallbackChain(
  providerId: string,
  originalStreamSimple: StreamFn,
  model: Model<Api>,
  context: Context,
  options: SimpleStreamOptions | undefined,
  stream: AssistantMessageEventStream,
): Promise<void> {
  // 1. Original request (current key / default auth)
  let last = await proxyStream(
    originalStreamSimple(model, context, options),
    stream,
    `${providerId} (default)`,
  );
  if (last.result !== "rate-limit") return;

  // 2. Walk the fallback chain
  const manualCandidates: ManualCandidate[] = [];
  let anySameModelAttempted = false;

  for (const candidate of fallbackConfig.providers) {
    if (candidate.name === providerId) {
      // Same provider — default already tried above, only try extra keys
      last = await runAttempts(
        originalStreamSimple,
        model,
        context,
        keysOnlyAttempts(options, candidate.keys, providerId),
        stream,
      );
      if (last.result !== "rate-limit") return;
      continue;
    }

    const fbProvider = modelsRef?.getProvider(candidate.name) as Provider<Api> | undefined;
    if (!fbProvider) {
      notify(`⚠️ Provider ${candidate.name} not available, skipping`, "warning");
      continue;
    }

    const sameModel = fbProvider.getModels().find((m) => m.id === model.id);
    if (sameModel) {
      anySameModelAttempted = true;
      last = await runAttempts(
        fbProvider.streamSimple.bind(fbProvider) as StreamFn,
        sameModel,
        context,
        defaultThenKeysAttempts(options, candidate.keys, `${candidate.name}/${model.id}`),
        stream,
      );
      if (last.result !== "rate-limit") return;
    } else {
      // No matching model — collect options for manual selection later
      manualCandidates.push(...buildManualCandidates(candidate, fbProvider));
    }
  }

  // No provider had our model → ask the user to pick one manually
  if (!anySameModelAttempted && manualCandidates.length > 0) {
    const outcome = await tryManualSelection(manualCandidates, model, context, options, stream);
    if (outcome && outcome.result !== "rate-limit") return;
    if (outcome) last = outcome;
  }

  // 3. Everything exhausted — emit a terminal error
  pushTerminalError(stream, model, last.lastError);
}

function pushTerminalError(
  stream: AssistantMessageEventStream,
  model: Model<Api>,
  errorMessage: string | undefined,
): void {
  const message: AssistantMessage = {
    role: "assistant",
    content: [],
    api: model.api,
    provider: model.provider,
    model: model.id,
    usage: {
      input: 0,
      output: 0,
      cacheRead: 0,
      cacheWrite: 0,
      totalTokens: 0,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
    },
    stopReason: "error",
    timestamp: Date.now(),
    errorMessage:
      errorMessage ?? `All fallback providers exhausted for ${model.provider}/${model.id}`,
  };
  stream.push({ type: "error", reason: "error", error: message });
  stream.end();
}

// ─── Extension factory ───────────────────────────────────────────────

export default function (pi: ExtensionAPI): void {
  pi.on("session_start", (_event, ctx: ExtensionContext) => {
    uiRef = ctx.ui;
    modelsRef = ctx.modelRegistry;
    loadGlobalConfig();
    restoreConfigFromSession(ctx.sessionManager.getEntries());

    if (ctx.model) {
      currentProviderId = ctx.model.provider;
      currentModelId = ctx.model.id;
      wrapProviderIfNeeded(currentProviderId);
    }
  });

  pi.on("model_select", (event) => {
    currentProviderId = event.model.provider;
    currentModelId = event.model.id;
    wrapProviderIfNeeded(event.model.provider);
  });

  pi.on("session_shutdown", () => {
    uiRef = null;
    modelsRef = null;
    currentProviderId = null;
    currentModelId = null;
  });

  pi.registerCommand("fallback-status", {
    description: "Show the fallback chain for the current provider",
    handler: async (_args, ctx: ExtensionCommandContext) => {
      const activeProvider = currentProviderId ?? "none";
      const activeModel = currentModelId ?? "none";

      if (fallbackConfig.providers.length === 0) {
        ctx.ui.notify("No fallback configured", "info");
        return;
      }

      const lines = fallbackConfig.providers.map((p) => {
        const marker = p.name === activeProvider ? "* " : "  ";
        return `${marker}${p.name} [${p.keys?.length ?? 0}] → ${p.defaultModel}`;
      });

      ctx.ui.notify(`Current: ${activeProvider} / ${activeModel}\n${lines.join("\n")}`, "info");
    },
  });
}
