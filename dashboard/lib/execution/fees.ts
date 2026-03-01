import type { FeeModel } from "./types";

export const DEFAULT_FEE_MODEL: FeeModel = {
  maker_bps: 2,
  taker_bps: 6,
  fee_asset: "USDT",
};

export function calcFee(notional: number, feeModel: FeeModel, role: "maker" | "taker"): number {
  const safeNotional = Number.isFinite(notional) ? Math.max(0, notional) : 0;
  const bps = role === "maker" ? feeModel.maker_bps : feeModel.taker_bps;
  return safeNotional * (Math.max(0, bps) / 10_000);
}
