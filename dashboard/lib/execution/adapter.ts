import { calcFee, DEFAULT_FEE_MODEL } from "./fees";
import { validateOrderIntent } from "./normalize";
import type { ExecutionResult, FeeModel, OrderIntent, SymbolSpec, VenueId } from "./types";

export interface ExecutionAdapter {
  venueId: VenueId;
  getSymbolSpec(symbol: string): Promise<SymbolSpec>;
  getFeeModel(symbol: string): Promise<FeeModel>;
  placeOrder(intent: OrderIntent): Promise<ExecutionResult>;
  cancelOrder(venueOrderId: string): Promise<boolean>;
  closePosition(symbol: string, mode: "market" | "limit"): Promise<ExecutionResult>;
}

const DEFAULT_SPEC: SymbolSpec = {
  symbol: "ETHUSDT",
  price_tick: 0.01,
  qty_step: 0.0001,
  min_qty: 0.0001,
  min_notional: 5,
};

const SYMBOL_SPEC_OVERRIDES: Record<string, Partial<SymbolSpec>> = {
  BTCUSDT: { price_tick: 0.1, qty_step: 0.0001, min_qty: 0.0001, min_notional: 5 },
  ETHUSDT: { price_tick: 0.01, qty_step: 0.0001, min_qty: 0.0001, min_notional: 5 },
};

export class MockAdapter implements ExecutionAdapter {
  venueId: VenueId = "mock";
  private orderCounter = 0;

  async getSymbolSpec(symbol: string): Promise<SymbolSpec> {
    const key = String(symbol || DEFAULT_SPEC.symbol).toUpperCase();
    return {
      ...DEFAULT_SPEC,
      ...(SYMBOL_SPEC_OVERRIDES[key] || {}),
      symbol: key,
    };
  }

  async getFeeModel(_symbol: string): Promise<FeeModel> {
    return DEFAULT_FEE_MODEL;
  }

  async placeOrder(intent: OrderIntent): Promise<ExecutionResult> {
    const spec = await this.getSymbolSpec(intent.symbol);
    const feeModel = await this.getFeeModel(intent.symbol);
    const validation = validateOrderIntent(intent, spec, intent.price);
    if (!validation.ok) {
      return {
        accepted: false,
        venueOrderId: "",
        filledQty: 0,
        avgPrice: 0,
        feePaid: 0,
        raw: { reason: validation.reason, intent, normalized: validation.normalizedIntent },
      };
    }

    const normalized = validation.normalizedIntent;
    const avgPrice = normalized.price ?? 0;
    const notional = Math.abs(normalized.qty * avgPrice);
    const feePaid = calcFee(notional, feeModel, "taker");
    this.orderCounter += 1;

    return {
      accepted: true,
      venueOrderId: `mock-${this.orderCounter}`,
      filledQty: normalized.qty,
      avgPrice,
      feePaid,
      raw: {
        venue: this.venueId,
        symbol: normalized.symbol,
        reduceOnly: Boolean(normalized.reduceOnly),
      },
    };
  }

  async cancelOrder(_venueOrderId: string): Promise<boolean> {
    return true;
  }

  async closePosition(symbol: string, _mode: "market" | "limit"): Promise<ExecutionResult> {
    this.orderCounter += 1;
    return {
      accepted: true,
      venueOrderId: `mock-close-${this.orderCounter}`,
      filledQty: 0,
      avgPrice: 0,
      feePaid: 0,
      raw: { venue: this.venueId, symbol: String(symbol || "").toUpperCase() },
    };
  }
}
