export type VenueId = "binance" | "bybit" | "oanda" | "mock";
export type Side = "BUY" | "SELL";
export type OrderType = "MARKET" | "LIMIT";
export type TimeInForce = "GTC" | "IOC" | "FOK";

export type SymbolSpec = {
  symbol: string;
  price_tick: number;
  qty_step: number;
  min_qty: number;
  min_notional: number;
  contract_multiplier?: number;
};

export type FeeModel = {
  maker_bps: number;
  taker_bps: number;
  fee_asset: string;
};

export type OrderIntent = {
  symbol: string;
  side: Side;
  type: OrderType;
  qty: number;
  price?: number;
  tif?: TimeInForce;
  reduceOnly?: boolean;
  clientOrderId?: string;
};

export type ExecutionResult = {
  accepted: boolean;
  venueOrderId: string;
  filledQty: number;
  avgPrice: number;
  feePaid: number;
  raw: Record<string, unknown>;
};

export type ValidateOrderResult = {
  ok: boolean;
  reason: string | null;
  normalizedIntent: OrderIntent;
};
