const baseURL = (process.env.NEXT_PUBLIC_API_BASE_URL as string | undefined) ?? "http://localhost:8000";

const buildUrl = (endpoint: string, params?: Record<string, any>) => {
  const url = new URL(endpoint, baseURL);
  if (params) {
    for (const [key, value] of Object.entries(params)) {
      if (value !== undefined && value !== null) {
        url.searchParams.set(key, String(value));
      }
    }
  }
  return url.toString();
};

const parseJsonSafe = async (response: Response, endpoint: string) => {
  try {
    return await response.json();
  } catch (error: any) {
    throw new Error(`${endpoint}: ${error?.message ?? String(error)}`);
  }
};

const requestJson = async (
  endpoint: string,
  init?: RequestInit,
  params?: Record<string, any>,
  opts?: { timeoutMs?: number },
) => {
  let response: Response;
  const controller = new AbortController();
  const timeoutMs = opts?.timeoutMs;
  const timer =
    timeoutMs && timeoutMs > 0
      ? setTimeout(() => {
          controller.abort();
        }, timeoutMs)
      : null;
  try {
    const headers: Record<string, string> = {
      "Cache-Control": "no-cache",
      ...(init?.headers as Record<string, string> | undefined),
    };
    response = await fetch(buildUrl(endpoint, params), {
      ...init,
      cache: "no-store",
      headers,
      signal: init?.signal ?? controller.signal,
    });
  } catch (error: any) {
    if (error?.name === "AbortError") {
      throw new Error(`${endpoint}: timeout after ${timeoutMs ?? "unknown"}ms`);
    }
    throw new Error(`${endpoint}: ${error?.message ?? String(error)}`);
  } finally {
    if (timer) clearTimeout(timer);
  }

  if (response.ok) {
    return parseJsonSafe(response, endpoint);
  }

  const bodyText = await response.text();
  let detail = bodyText || `${response.status} ${response.statusText}`;
  try {
    const parsed = bodyText ? JSON.parse(bodyText) : null;
    if (parsed?.detail !== undefined) {
      detail = typeof parsed.detail === "string" ? parsed.detail : JSON.stringify(parsed.detail);
    }
  } catch {
    // Keep non-JSON detail text as-is.
  }
  throw new Error(`${endpoint}: ${detail}`);
};

const get = async (endpoint: string, params?: Record<string, any>, opts?: { timeoutMs?: number }) =>
  requestJson(endpoint, undefined, params, opts);

const post = async (endpoint: string, body?: any, headers?: Record<string, string>, opts?: { timeoutMs?: number }) => {
  const outgoingHeaders: Record<string, string> = { ...(headers ?? {}) };
  const isFormData = body instanceof FormData;
  const isJsonObject = body && !isFormData && typeof body === "object" && typeof body !== "string";
  if (isFormData && outgoingHeaders["Content-Type"]) {
    delete outgoingHeaders["Content-Type"];
  }
  if (isJsonObject && !outgoingHeaders["Content-Type"]) {
    outgoingHeaders["Content-Type"] = "application/json";
  }
  return requestJson(endpoint, {
    method: "POST",
    headers: Object.keys(outgoingHeaders).length ? outgoingHeaders : undefined,
    body: isFormData || typeof body === "string" ? body : isJsonObject ? JSON.stringify(body) : undefined,
  }, undefined, opts);
};

const looksLikeTradeObject = (res: any): boolean => {
  if (!res || typeof res !== "object" || Array.isArray(res)) return false;
  const hasCore = res.id !== undefined && res.symbol !== undefined && res.side !== undefined;
  const hasPriceOrTime = res.entry_price !== undefined || res.exit_price !== undefined || res.opened_at !== undefined;
  return hasCore && hasPriceOrTime;
};

const looksLikeEventObject = (res: any): boolean => {
  if (!res || typeof res !== "object" || Array.isArray(res)) return false;
  const hasCore = res.id !== undefined || res.ts !== undefined || res.created_at !== undefined;
  const hasEventSignal = res.event_type !== undefined || res.decision !== undefined || res.rationale !== undefined;
  return hasCore && hasEventSignal;
};

const looksLikePositionObject = (res: any): boolean => {
  if (!res || typeof res !== "object" || Array.isArray(res)) return false;
  return res.id !== undefined && res.symbol !== undefined && res.side !== undefined;
};

const detectRawShape = (res: any, kind: "trades" | "events" | "positions" | "datasets"): string => {
  if (Array.isArray(res)) return "array";
  if (Array.isArray(res?.items)) return "items";
  if (Array.isArray(res?.data)) return "data";
  if (Array.isArray(res?.rows)) return "rows";
  if (Array.isArray(res?.list)) return "list";
  if (Array.isArray(res?.trades)) return "trades";
  if (Array.isArray(res?.events)) return "events";
  if (Array.isArray(res?.positions)) return "positions";
  if (Array.isArray(res?.datasets)) return "datasets";
  if (kind === "trades" && looksLikeTradeObject(res)) return "singleton";
  if (kind === "events" && looksLikeEventObject(res)) return "singleton";
  if (kind === "positions" && looksLikePositionObject(res)) return "singleton";
  if (res === null || res === undefined) return "empty";
  return "object";
};

export const normalizeTrades = (res: any): any[] => {
  if (Array.isArray(res)) return res;
  if (Array.isArray(res?.items)) return res.items;
  if (Array.isArray(res?.data)) return res.data;
  if (Array.isArray(res?.rows)) return res.rows;
  if (Array.isArray(res?.trades)) return res.trades;
  if (looksLikeTradeObject(res)) return [res];
  return [];
};

export const normalizeEvents = (res: any): any[] => {
  let rows: any[] = [];
  if (Array.isArray(res)) rows = res;
  else if (Array.isArray(res?.items)) rows = res.items;
  else if (Array.isArray(res?.data)) rows = res.data;
  else if (Array.isArray(res?.rows)) rows = res.rows;
  else if (Array.isArray(res?.events)) rows = res.events;
  else if (looksLikeEventObject(res)) rows = [res];
  return rows.map((event) => ({
    ...event,
    event_type: String(event?.event_type || event?.decision || event?.risk_state_snapshot?.event_type || "SIGNAL").toUpperCase(),
  }));
};

export const normalizePositions = (res: any): any[] => {
  if (Array.isArray(res)) return res;
  if (Array.isArray(res?.items)) return res.items;
  if (Array.isArray(res?.data)) return res.data;
  if (Array.isArray(res?.rows)) return res.rows;
  if (Array.isArray(res?.positions)) return res.positions;
  if (looksLikePositionObject(res)) return [res];
  return [];
};

export const normalizeDatasets = (res: any): any[] => {
  if (Array.isArray(res)) return res;
  if (Array.isArray(res?.items)) return res.items;
  if (Array.isArray(res?.data)) return res.data;
  if (Array.isArray(res?.rows)) return res.rows;
  if (Array.isArray(res?.datasets)) return res.datasets;
  return [];
};

export const apiBaseUrl = baseURL;
export const fetchOverview = async (timeoutMs?: number) => get("/overview", { _ts: Date.now() }, { timeoutMs });
export const fetchTrades = async (limit = 500, offset = 0, timeoutMs?: number) => {
  const raw = await get("/trades", { limit, offset, _ts: Date.now() }, { timeoutMs });
  return normalizeTrades(raw);
};
export const fetchPositions = async (timeoutMs?: number) => {
  const raw = await get("/positions", { _ts: Date.now() }, { timeoutMs });
  return normalizePositions(raw);
};
export const fetchEngineStatus = async () => get("/engine/status");
export const fetchReplayStatus = async () => get("/replay/status");
export const fetchDatasets = async () => {
  const raw = await get("/replay/datasets");
  return normalizeDatasets(raw);
};
export const fetchEvents = async (limit = 500, offset = 0, timeoutMs?: number) => {
  const raw = await get("/events", { limit, offset, _ts: Date.now() }, { timeoutMs });
  return normalizeEvents(raw);
};
export const fetchEventSummary = async (last_n = 200) => get("/events/summary", { last_n });

export const uploadDataset = async (file: File) => {
  const formData = new FormData();
  formData.append("file", file);
  return post("/replay/upload", formData);
};

export const replayStart = async (payload: any) => post("/replay/start", payload);
export const replayPause = async () => post("/replay/pause");
export const replayResume = async () => post("/replay/resume");
export const replayStep = async () => post("/replay/step");
export const replayStop = async () => post("/replay/stop");
export const replayReset = async () => post("/replay/reset");
export const controlClosePosition = async (trade_id: string) => post("/control/close_position", { trade_id });
export const controlSetProfile = async (profile: "TREND_STABLE" | "SCALPER_STABLE") =>
  post("/control/set_profile", { profile });
export const fetchSafetyStatus = async () => get("/api/safety/status", { _ts: Date.now() });
export const safetyArm = async (mode: "off" | "soft" | "hard") => post("/api/safety/arm", { mode });
export const safetyTrip = async (mode: "soft" | "hard", reason?: string) =>
  post("/api/safety/trip", { mode, reason: reason || "manual_trip" });
export const fetchSafetyIncidents = async () => get("/api/safety/incidents", { _ts: Date.now() });
export const fetchSafetyIncident = async (incidentId: string) =>
  get(`/api/safety/incidents/${encodeURIComponent(incidentId)}`, { _ts: Date.now() });
