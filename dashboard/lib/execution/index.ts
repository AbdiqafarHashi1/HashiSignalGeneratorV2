import { controlClosePosition } from "../api";
import { MockAdapter, type ExecutionAdapter } from "./adapter";
import type { ExecutionResult, VenueId } from "./types";

type CloseNowArgs = {
  mode?: string;
  tradeId?: string;
  symbol?: string;
  venueId?: VenueId;
};

type ReduceArgs = CloseNowArgs & {
  fraction?: number;
};

let sharedMock: MockAdapter | null = null;
type SafetyMode = "off" | "soft" | "hard";

const getEnvVenue = (): VenueId => {
  const raw = (
    process.env.NEXT_PUBLIC_EXEC_VENUE ||
    process.env.EXEC_VENUE ||
    "mock"
  ).toLowerCase();
  if (raw === "binance" || raw === "bybit" || raw === "oanda" || raw === "mock") return raw;
  return "mock";
};

const getEnvRole = (): "maker" | "taker" => {
  const raw = (process.env.NEXT_PUBLIC_EXEC_ROLE || process.env.EXEC_ROLE || "taker").toLowerCase();
  return raw === "maker" ? "maker" : "taker";
};

const getKillSwitchMode = (): SafetyMode => {
  const raw = (
    process.env.NEXT_PUBLIC_KILL_SWITCH_MODE ||
    process.env.KILL_SWITCH_MODE ||
    "off"
  ).toLowerCase();
  return raw === "soft" || raw === "hard" ? raw : "off";
};

const canExecuteIntent = (action: "entry" | "close" | "reduce", reduceOnly: boolean): { ok: boolean; reason?: string } => {
  const mode = getKillSwitchMode();
  if (mode === "hard" && action === "entry") return { ok: false, reason: "kill_switch_hard" };
  if (mode === "soft" && action === "entry") return { ok: false, reason: "kill_switch_soft" };
  if ((mode === "soft" || mode === "hard") && (action === "close" || action === "reduce") && !reduceOnly) {
    return { ok: false, reason: "reduce_only_required" };
  }
  return { ok: true };
};

export function getAdapter(venueId?: VenueId): ExecutionAdapter {
  const chosen = venueId || getEnvVenue();
  if (chosen === "mock") {
    if (!sharedMock) sharedMock = new MockAdapter();
    return sharedMock;
  }
  if (!sharedMock) sharedMock = new MockAdapter();
  return sharedMock;
}

export async function executeCloseNow(args: CloseNowArgs): Promise<ExecutionResult | unknown> {
  const guard = canExecuteIntent("close", true);
  if (!guard.ok) throw new Error(guard.reason || "execution_blocked");
  const mode = String(args.mode || "").toUpperCase();
  if (mode === "REPLAY") {
    if (!args.tradeId) throw new Error("Missing tradeId for replay close");
    return controlClosePosition(args.tradeId);
  }
  const adapter = getAdapter(args.venueId);
  return adapter.closePosition(args.symbol || "", "market");
}

export async function executeReduceHalf(args: ReduceArgs): Promise<ExecutionResult | unknown> {
  const guard = canExecuteIntent("reduce", true);
  if (!guard.ok) throw new Error(guard.reason || "execution_blocked");
  const mode = String(args.mode || "").toUpperCase();
  if (mode === "REPLAY") {
    throw new Error("Reduce 50% is not supported in replay yet");
  }
  const _role = getEnvRole();
  const adapter = getAdapter(args.venueId);
  return adapter.closePosition(args.symbol || "", "market");
}
