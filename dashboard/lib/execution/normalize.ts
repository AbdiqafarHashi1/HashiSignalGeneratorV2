import type { OrderIntent, SymbolSpec, ValidateOrderResult } from "./types";

const safe = (n: number, fallback = 0) => (Number.isFinite(n) ? n : fallback);

export function roundToTick(price: number, tick: number): number {
  const t = safe(tick, 0);
  if (t <= 0) return safe(price, 0);
  return Math.round(safe(price, 0) / t) * t;
}

export function roundToStep(qty: number, step: number): number {
  const s = safe(step, 0);
  if (s <= 0) return safe(qty, 0);
  return Math.floor(safe(qty, 0) / s) * s;
}

export function validateOrderIntent(
  intent: OrderIntent,
  spec: SymbolSpec,
  lastPrice?: number,
): ValidateOrderResult {
  const normalizedIntent: OrderIntent = {
    ...intent,
    qty: roundToStep(intent.qty, spec.qty_step),
    price: intent.price !== undefined ? roundToTick(intent.price, spec.price_tick) : intent.price,
  };

  if (normalizedIntent.qty < spec.min_qty) {
    return {
      ok: false,
      reason: `Quantity ${normalizedIntent.qty} is below min_qty ${spec.min_qty}`,
      normalizedIntent,
    };
  }

  if (normalizedIntent.type === "LIMIT") {
    if (normalizedIntent.price === undefined || normalizedIntent.price <= 0) {
      return {
        ok: false,
        reason: "Limit order requires a positive price",
        normalizedIntent,
      };
    }
  }

  const marketPrice =
    normalizedIntent.type === "LIMIT"
      ? normalizedIntent.price ?? 0
      : Number.isFinite(lastPrice) && (lastPrice as number) > 0
        ? (lastPrice as number)
        : normalizedIntent.price ?? 0;

  const estimatedNotional = normalizedIntent.qty * marketPrice;
  if (spec.min_notional > 0 && estimatedNotional < spec.min_notional) {
    return {
      ok: false,
      reason: `Estimated notional ${estimatedNotional.toFixed(6)} is below min_notional ${spec.min_notional}`,
      normalizedIntent,
    };
  }

  return {
    ok: true,
    reason: null,
    normalizedIntent,
  };
}
