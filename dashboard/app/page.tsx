'use client';

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import ModeBadge from '../components/ModeBadge';
import TradeTable from '../components/TradeTable';
import {
  apiBaseUrl,
  controlSetProfile,
  fetchDatasets,
  fetchEvents,
  fetchOverview,
  fetchPositions,
  fetchTrades,
  replayPause,
  replayReset,
  replayResume,
  replayStart,
  replayStep,
  replayStop,
  uploadDataset,
} from '../lib/api';
import { executeCloseNow, executeReduceHalf } from '../lib/execution';

type EventFilter = 'ALL' | 'DECISION' | 'SIGNAL' | 'BLOCKED' | 'ENTRY' | 'PARTIAL' | 'EXIT' | 'ERROR';
const PROFILE_OPTIONS = ['TREND_STABLE', 'SCALPER_STABLE'] as const;
const DEFAULT_REPLAY_DATASET =
  (process.env.NEXT_PUBLIC_REPLAY_DATASET_DEFAULT as string | undefined) ||
  'data/datasets/ETHUSDT_15m.csv';
type EndpointKey = 'overview' | 'trades' | 'events' | 'positions';
type FetchMeta = {
  lastOkAt: number | null;
  lastErrAt: number | null;
  lastDurationMs: number | null;
  lastCount: number | null;
  lastErr: string | null;
};
type AccountState = {
  equity_start: number | null;
  actual_equity: number | null;
  closed_trades_sorted: any[];
  realized_net: number;
  realized_gross: number;
  fees_total: number;
  unrealized: number;
  expected_equity: number;
  delta: number | null;
  warnings: string[];
};

const text = (value: any) => (value === null || value === undefined || value === '' ? '-' : String(value));
const fmtNum = (value: any, digits = 2) => {
  if (value === null || value === undefined || value === '') return '-';
  const n = Number(value);
  return Number.isFinite(n) ? n.toFixed(digits) : String(value);
};
const fmtTs = (value: any) => {
  if (!value) return '-';
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return String(value);
  return d.toISOString().replace('T', ' ').replace('Z', ' UTC');
};
const isoFromMs = (value: any) => {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return null;
  return new Date(n).toISOString();
};
const errorText = (error: any) => {
  if (typeof error === 'string') return error;
  if (typeof error?.detail === 'string') return error.detail;
  if (typeof error?.message === 'string') return error.message;
  try {
    return JSON.stringify(error);
  } catch {
    return String(error);
  }
};

function usePollingLoop(task: () => Promise<void> | void, getDelayMs: () => number) {
  const taskRef = useRef(task);
  const delayRef = useRef(getDelayMs);

  useEffect(() => {
    taskRef.current = task;
  }, [task]);

  useEffect(() => {
    delayRef.current = getDelayMs;
  }, [getDelayMs]);

  useEffect(() => {
    let stopped = false;
    let timer: ReturnType<typeof setTimeout> | null = null;

    const run = async () => {
      try {
        await taskRef.current();
      } finally {
        if (stopped) return;
        const next = Number(delayRef.current()) || 1000;
        timer = setTimeout(run, Math.max(50, next));
      }
    };

    timer = setTimeout(run, 0);
    return () => {
      stopped = true;
      if (timer) clearTimeout(timer);
    };
  }, []);
}

function Led({ on }: { on: boolean }) {
  return <span className={`inline-block h-2.5 w-2.5 rounded-full ${on ? 'bg-emerald-400 animate-pulse' : 'bg-zinc-500'}`} />;
}

function Sparkline({ values }: { values: number[] }) {
  if (!values.length) return <div className='text-xs text-zinc-500'>Equity curve unavailable (no history fields returned).</div>;
  const w = 820;
  const h = 130;
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min || 1;
  const points = values
    .map((v, i) => {
      const x = (i / Math.max(1, values.length - 1)) * w;
      const y = h - ((v - min) / span) * h;
      return `${x},${y}`;
    })
    .join(' ');
  return (
    <svg viewBox={`0 0 ${w} ${h}`} className='w-full h-36'>
      <polyline fill='none' stroke='rgb(34,211,238)' strokeWidth='2' points={points} />
    </svg>
  );
}

const toNum = (value: any): number | null => {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
};

const computeAccountState = ({
  overview,
  trades,
  positions,
}: {
  overview: any;
  trades: any[];
  positions: any[];
}): AccountState => {
  const warnings: string[] = [];
  const equityStart = toNum(overview?.equity_start ?? overview?.starting_equity);
  const actualEquity = toNum(overview?.equity_now ?? overview?.equity);
  const closedTradesSorted = [...(trades || [])]
    .filter((trade) => String(trade?.status || '').toUpperCase() === 'CLOSED')
    .sort((a, b) => {
      const aTs = new Date(a?.closed_at || a?.opened_at || a?.created_at || 0).getTime();
      const bTs = new Date(b?.closed_at || b?.opened_at || b?.created_at || 0).getTime();
      return aTs - bTs;
    });

  let realizedNetByField = 0;
  let realizedGross = 0;
  let feesTotal = 0;
  let rowsWithNet = 0;
  let rowsWithGross = 0;
  let rowsWithFees = 0;
  let netModeMatches = 0;
  let netModeChecks = 0;

  for (const trade of closedTradesSorted) {
    const fees = toNum(trade?.fees_total) ?? (toNum(trade?.fee_entry) ?? 0) + (toNum(trade?.fee_exit) ?? 0);
    const pnlNet = toNum(trade?.pnl_net ?? trade?.pnl);
    const pnlGross = toNum(trade?.pnl_gross);

    if (pnlNet !== null) {
      realizedNetByField += pnlNet;
      rowsWithNet += 1;
    }
    if (pnlGross !== null) {
      realizedGross += pnlGross;
      rowsWithGross += 1;
    }
    if (fees !== null) {
      feesTotal += fees;
      rowsWithFees += 1;
    }
    if (pnlGross !== null && pnlNet !== null && fees !== null) {
      netModeChecks += 1;
      if (Math.abs((pnlNet + fees) - pnlGross) <= Math.max(0.01, Math.abs(pnlGross) * 0.001)) {
        netModeMatches += 1;
      }
    }
    if (pnlGross === null && pnlNet !== null && fees !== null) {
      realizedGross += pnlNet + fees;
    }
  }

  const likelyNetOfFees = netModeChecks > 0 && netModeMatches / netModeChecks >= 0.6;
  if (likelyNetOfFees) {
    warnings.push('Detected net-of-fees trade pnl mode; fees are tracked but not subtracted again.');
  }

  let realizedNet = 0;
  if (rowsWithNet > 0) {
    realizedNet = realizedNetByField;
    if (rowsWithGross === 0 && rowsWithFees > 0 && !likelyNetOfFees) {
      realizedNet = realizedGross - feesTotal;
    }
  } else {
    realizedNet = realizedGross - feesTotal;
  }

  const position = positions?.[0] || null;
  const posUnrealized = toNum(position?.unrealized_pnl ?? position?.unrealized ?? position?.upl);
  const entry = toNum(position?.entry_price);
  const mark = toNum(position?.mark_price ?? position?.price ?? position?.current_price);
  const qty = toNum(position?.quantity ?? position?.qty);
  const side = String(position?.side || '').toUpperCase();
  let unrealizedPnl = posUnrealized ?? 0;
  if (posUnrealized === null && entry !== null && mark !== null && qty !== null) {
    if (side === 'SELL') unrealizedPnl = (entry - mark) * qty;
    else unrealizedPnl = (mark - entry) * qty;
  }

  if (equityStart === null) warnings.push('Missing equity_start in /overview; delta unavailable.');
  if (actualEquity === null) warnings.push('Missing equity_now/equity in /overview.');
  if (Number(overview?.open_positions || 0) > 0 && (positions || []).length === 0) {
    warnings.push(`Mismatch: overview.open_positions=${overview?.open_positions} but /positions returned 0.`);
  }
  if ([realizedNet, realizedGross, feesTotal, unrealizedPnl].some((v) => !Number.isFinite(v))) {
    warnings.push('Non-finite accounting value detected.');
  }

  const expectedEquity = (equityStart ?? 0) + realizedNet + unrealizedPnl;
  const delta = equityStart === null || actualEquity === null ? null : actualEquity - expectedEquity;

  return {
    equity_start: equityStart,
    actual_equity: actualEquity,
    closed_trades_sorted: closedTradesSorted,
    realized_net: realizedNet,
    realized_gross: realizedGross,
    fees_total: feesTotal,
    unrealized: unrealizedPnl,
    expected_equity: expectedEquity,
    delta,
    warnings,
  };
};

export default function Page() {
  const [overview, setOverview] = useState<any>(null);
  const [trades, setTrades] = useState<any[]>([]);
  const [events, setEvents] = useState<any[]>([]);
  const [positions, setPositions] = useState<any[]>([]);
  const [datasets, setDatasets] = useState<any[]>([]);
  const [selectedDatasetId, setSelectedDatasetId] = useState('');
  const [speed, setSpeed] = useState(1);
  const [eventFilter, setEventFilter] = useState<EventFilter>('ALL');
  const [selectedEventId, setSelectedEventId] = useState('');
  const [error, setError] = useState('');
  const [toast, setToast] = useState<{ type: 'ok' | 'err'; message: string } | null>(null);
  const [profileLoading, setProfileLoading] = useState(false);
  const [lastRefreshAt, setLastRefreshAt] = useState<number | null>(null);
  const [uiNowMs, setUiNowMs] = useState<number>(Date.now());
  const [pollTick, setPollTick] = useState<number>(0);
  const [inspectorOpen, setInspectorOpen] = useState(false);
  const [tradesRawShape, setTradesRawShape] = useState<string>("-");
  const [eventsRawShape, setEventsRawShape] = useState<string>("-");
  const [positionsRawShape, setPositionsRawShape] = useState<string>("-");
  const [fetchMeta, setFetchMeta] = useState<Record<EndpointKey, FetchMeta>>({
    overview: { lastOkAt: null, lastErrAt: null, lastDurationMs: null, lastCount: null, lastErr: null },
    trades: { lastOkAt: null, lastErrAt: null, lastDurationMs: null, lastCount: null, lastErr: null },
    events: { lastOkAt: null, lastErrAt: null, lastDurationMs: null, lastCount: null, lastErr: null },
    positions: { lastOkAt: null, lastErrAt: null, lastDurationMs: null, lastCount: null, lastErr: null },
  });
  const fileRef = useRef<HTMLInputElement | null>(null);
  const overviewReqSeq = useRef(0);
  const streamsReqSeq = useRef(0);
  const isRunningRef = useRef(false);
  const tradesRef = useRef<any[]>([]);
  const eventsRef = useRef<any[]>([]);
  const positionsRef = useRef<any[]>([]);
  const [lastStablePosition, setLastStablePosition] = useState<any | null>(null);

  const isRunning = Boolean(overview?.replay?.is_running);
  const ageLabel = (ts: number | null) => (ts ? `${Math.max(0, uiNowMs - ts)} ms ago` : '-');

  useEffect(() => {
    isRunningRef.current = isRunning;
  }, [isRunning]);

  useEffect(() => {
    tradesRef.current = trades;
  }, [trades]);

  useEffect(() => {
    eventsRef.current = events;
  }, [events]);

  useEffect(() => {
    positionsRef.current = positions;
  }, [positions]);

  useEffect(() => {
    const timer = setInterval(() => setUiNowMs(Date.now()), 1000);
    return () => clearInterval(timer);
  }, []);

  const markFetchMeta = useCallback((key: EndpointKey, patch: Partial<FetchMeta>) => {
    setFetchMeta((prev) => ({
      ...prev,
      [key]: { ...prev[key], ...patch },
    }));
  }, []);

  const loadOverview = useCallback(async () => {
    const reqId = ++overviewReqSeq.current;
    const startedAt = Date.now();
    try {
      const res = await fetchOverview(1200);
      if (reqId !== overviewReqSeq.current) return;
      setOverview(res || {});
      markFetchMeta('overview', { lastOkAt: Date.now(), lastDurationMs: Date.now() - startedAt, lastCount: null, lastErr: null });
      setError('');
      setLastRefreshAt(Date.now());
      setPollTick((t) => t + 1);
    } catch (e: any) {
      markFetchMeta('overview', { lastErrAt: Date.now(), lastDurationMs: Date.now() - startedAt, lastErr: errorText(e) });
      setError(errorText(e));
      setPollTick((t) => t + 1);
    }
  }, [markFetchMeta]);

  const loadStreams = useCallback(async () => {
    const reqId = ++streamsReqSeq.current;
    const cycleStart = Date.now();
    let tradesCount = tradesRef.current.length;
    let eventsCount = eventsRef.current.length;
    let positionsCount = positionsRef.current.length;
    let hadError = false;

    const tradesTask = (async () => {
      const startedAt = Date.now();
      try {
        const items = await fetchTrades(500, 0, 2000);
        if (reqId !== streamsReqSeq.current) return;
        const nextTrades = [...(items || [])];
        tradesCount = nextTrades.length;
        setTrades(nextTrades);
        setTradesRawShape(nextTrades.length === 1 ? 'singleton|array' : 'array');
        markFetchMeta('trades', { lastOkAt: Date.now(), lastDurationMs: Date.now() - startedAt, lastCount: nextTrades.length, lastErr: null });
      } catch (e: any) {
        hadError = true;
        markFetchMeta('trades', { lastErrAt: Date.now(), lastDurationMs: Date.now() - startedAt, lastErr: errorText(e) });
      }
    })();

    const eventsTask = (async () => {
      const startedAt = Date.now();
      try {
        const items = await fetchEvents(500, 0, 2000);
        if (reqId !== streamsReqSeq.current) return;
        const nextEvents = [...(items || [])];
        eventsCount = nextEvents.length;
        setEvents(nextEvents);
        setEventsRawShape(nextEvents.length === 1 ? 'singleton|array' : 'array');
        markFetchMeta('events', { lastOkAt: Date.now(), lastDurationMs: Date.now() - startedAt, lastCount: nextEvents.length, lastErr: null });
      } catch (e: any) {
        hadError = true;
        markFetchMeta('events', { lastErrAt: Date.now(), lastDurationMs: Date.now() - startedAt, lastErr: errorText(e) });
      }
    })();

    const positionsTask = (async () => {
      const startedAt = Date.now();
      try {
        const items = await fetchPositions(2000);
        if (reqId !== streamsReqSeq.current) return;
        const nextPositions = [...(items || [])];
        positionsCount = nextPositions.length;
        setPositions(nextPositions);
        setPositionsRawShape(nextPositions.length === 1 ? 'singleton|array' : 'array');
        markFetchMeta('positions', { lastOkAt: Date.now(), lastDurationMs: Date.now() - startedAt, lastCount: nextPositions.length, lastErr: null });
      } catch (e: any) {
        hadError = true;
        markFetchMeta('positions', { lastErrAt: Date.now(), lastDurationMs: Date.now() - startedAt, lastErr: errorText(e) });
      }
    })();

    await Promise.allSettled([tradesTask, eventsTask, positionsTask]);
    if (reqId !== streamsReqSeq.current) return;
    if (!hadError) setError('');
    setLastRefreshAt(Date.now());
    setPollTick((t) => t + 1);
    if (process.env.NODE_ENV !== 'production') {
      console.debug(
        `[dashboard] poll cycle @${new Date().toISOString()} in ${Date.now() - cycleStart}ms | trades=${tradesCount} events=${eventsCount} positions=${positionsCount}`,
      );
    }
  }, [markFetchMeta]);

  const loadDatasets = useCallback(async () => {
    try {
      const ds = await fetchDatasets();
      setDatasets(ds || []);
      setSelectedDatasetId((prev) => {
        if (prev) return prev;
        const preferred = String(DEFAULT_REPLAY_DATASET || '').replace(/\\/g, '/').toLowerCase();
        const preferredName = preferred.split('/').pop() || preferred;
        const match = (ds || []).find((row: any) => {
          const filename = String(row?.filename || '').toLowerCase();
          const storedPath = String(row?.stored_path || '').replace(/\\/g, '/').toLowerCase();
          return filename === preferredName || storedPath.endsWith(preferred) || storedPath.endsWith(`/${preferredName}`);
        });
        return String(match?.id || ds?.[0]?.id || '');
      });
      setError('');
    } catch (e: any) {
      setError(errorText(e));
    }
  }, []);

  useEffect(() => {
    void loadDatasets();
  }, [loadDatasets]);

  usePollingLoop(() => loadOverview(), () => (isRunningRef.current ? 750 : 3000));
  usePollingLoop(() => loadStreams(), () => (isRunningRef.current ? 1200 : 3500));

  useEffect(() => {
    if (!toast) return;
    const timer = setTimeout(() => setToast(null), 2200);
    return () => clearTimeout(timer);
  }, [toast]);

  const canStart = Boolean(selectedDatasetId);
  const profile = String(overview?.active_profile || PROFILE_OPTIONS[0]).toUpperCase();
  const clock = overview?.replay?.candle_ts;
  const pointer = Number(overview?.replay?.pointer || 0);
  const safetyMode = String(overview?.safety?.kill_mode || 'off').toLowerCase();
  const safetyTripReason = text(overview?.safety?.last_trip_reason);
  const safetyTripAt = overview?.safety?.last_trip_at;
  const preTradeAllowed = overview?.pre_trade_decision?.allowed !== false;
  const preTradeReason = text(overview?.pre_trade_decision?.reasonCode);
  const reconOk = overview?.safety?.reconciler?.ok !== false;
  const reconCycles = Number(overview?.safety?.reconciler?.mismatch_cycles || 0);
  const reconStaleMs = overview?.safety?.staleness_ms;
  const dayKey = text(overview?.day?.day_key);
  const rolloverInEffect = Boolean(overview?.day?.rollover_in_effect);

  const openPosition = positions[0] || null;
  const openTrade = useMemo(
    () => trades.find((t) => String(t?.status || '').toUpperCase() === 'OPEN' && String(t?.symbol || '') === String(openPosition?.symbol || '')) || null,
    [trades, openPosition],
  );
  useEffect(() => {
    if (positions.length > 0) {
      setLastStablePosition(positions[0]);
      return;
    }
    const fallbackOpenTrade = trades.find((t) => String(t?.status || '').toUpperCase() === 'OPEN');
    if (fallbackOpenTrade) {
      setLastStablePosition({
        id: fallbackOpenTrade.id,
        symbol: fallbackOpenTrade.symbol,
        side: fallbackOpenTrade.side,
        quantity: fallbackOpenTrade.quantity,
        entry_price: fallbackOpenTrade.entry_price,
        unrealized_pnl: overview?.unrealized_pnl ?? 0,
      });
    }
  }, [positions, trades, overview?.unrealized_pnl]);

  const displayPosition = useMemo(() => {
    if (openPosition) return openPosition;
    if (Number(overview?.open_positions || 0) > 0) {
      const fallbackOpenTrade = trades.find((t) => String(t?.status || '').toUpperCase() === 'OPEN');
      if (fallbackOpenTrade) {
        return {
          id: fallbackOpenTrade.id,
          symbol: fallbackOpenTrade.symbol,
          side: fallbackOpenTrade.side,
          quantity: fallbackOpenTrade.quantity,
          entry_price: fallbackOpenTrade.entry_price,
          unrealized_pnl: overview?.unrealized_pnl ?? 0,
        };
      }
      if (lastStablePosition) return lastStablePosition;
    }
    return null;
  }, [openPosition, overview?.open_positions, overview?.unrealized_pnl, trades, lastStablePosition]);

  const displayOpenTrade = useMemo(() => {
    if (!displayPosition) return null;
    return trades.find((t) => String(t?.status || '').toUpperCase() === 'OPEN' && String(t?.symbol || '') === String(displayPosition?.symbol || '')) || null;
  }, [trades, displayPosition]);
  const latestExit = useMemo(() => events.find((e) => String(e?.event_type || '').toUpperCase() === 'EXIT') || null, [events]);
  const latestEntry = useMemo(() => events.find((e) => String(e?.event_type || '').toUpperCase() === 'ENTRY') || null, [events]);
  const latestDecisionEvent = useMemo(() => events.find((e) => String(e?.event_type || '').toUpperCase() === 'DECISION') || null, [events]);
  const latestDecisionSnapshot = useMemo(
    () => latestDecisionEvent?.risk_state_snapshot || {},
    [latestDecisionEvent],
  );
  const regimeGateOk = latestDecisionSnapshot?.regime_gate_ok ?? overview?.latest_decision?.regime_gate_ok ?? null;
  const regimeGateReasons: string[] =
    (Array.isArray(latestDecisionSnapshot?.regime_gate_reasons) ? latestDecisionSnapshot.regime_gate_reasons : null) ||
    (Array.isArray(overview?.latest_decision?.top_regime_gate_reasons) ? overview.latest_decision.top_regime_gate_reasons : []) ||
    [];
  const regimeGateMetrics = latestDecisionSnapshot?.regime_gate_metrics || overview?.latest_decision?.regime_gate_metrics || {};
  const activeMode = latestDecisionSnapshot?.active_mode || overview?.latest_decision?.active_mode;
  const modeReasons: string[] =
    (Array.isArray(latestDecisionSnapshot?.mode_reasons) ? latestDecisionSnapshot.mode_reasons : null) ||
    (Array.isArray(overview?.latest_decision?.mode_reasons) ? overview.latest_decision.mode_reasons : []) ||
    [];
  const regimeWarmup = regimeGateReasons.some((reason) => String(reason).includes('ltf_warmup'));
  const regimeFallbackUsed = regimeGateReasons.some((reason) => String(reason).includes('htf_missing_fallback_used'));
  const ltfBarsRemaining = Number(regimeGateMetrics?.ltf_bars_remaining);

  const filteredEvents = useMemo(() => {
    const sorted = [...events].sort((a, b) => {
      const at = Number(a?.ts || 0) || new Date(a?.created_at || 0).getTime();
      const bt = Number(b?.ts || 0) || new Date(b?.created_at || 0).getTime();
      return bt - at;
    });
    return sorted.filter((e) => {
      const t = String(e?.event_type || '').toUpperCase();
      if (eventFilter === 'ALL') return true;
      return t === eventFilter;
    });
  }, [events, eventFilter]);
  const selectedEvent = useMemo(() => filteredEvents.find((e) => String(e?.id) === String(selectedEventId)) || null, [filteredEvents, selectedEventId]);

  const activity = useMemo(() => {
    const closed = trades.filter((t) => String(t?.status || '').toUpperCase() === 'CLOSED');
    if (!closed.length) return { winRate: 0, avgWin: 0, avgLoss: 0, profitFactor: 0, expectancy: 0, feesTotal: 0, feesToday: null as number | null };
    const pnls = closed.map((t) => Number(t?.pnl_net ?? 0));
    const wins = pnls.filter((v) => v > 0);
    const losses = pnls.filter((v) => v < 0);
    const winsSum = wins.reduce((a, b) => a + b, 0);
    const lossSum = losses.reduce((a, b) => a + b, 0);
    const feesTotal = closed.reduce((a, t) => a + Number(t?.fees_total ?? 0), 0);
    const now = new Date();
    const k = `${now.getUTCFullYear()}-${now.getUTCMonth()}-${now.getUTCDate()}`;
    const feesToday = closed
      .filter((t) => {
        const d = new Date(t?.closed_at || t?.opened_at || t?.created_at);
        if (Number.isNaN(d.getTime())) return false;
        return `${d.getUTCFullYear()}-${d.getUTCMonth()}-${d.getUTCDate()}` === k;
      })
      .reduce((a, t) => a + Number(t?.fees_total ?? 0), 0);
    return {
      winRate: (wins.length / closed.length) * 100,
      avgWin: wins.length ? winsSum / wins.length : 0,
      avgLoss: losses.length ? Math.abs(lossSum) / losses.length : 0,
      profitFactor: Math.abs(lossSum) > 0 ? winsSum / Math.abs(lossSum) : 0,
      expectancy: pnls.reduce((a, b) => a + b, 0) / pnls.length,
      feesTotal,
      feesToday: feesToday || null,
    };
  }, [trades]);

  const equitySeries = useMemo(() => {
    const eqStart = Number(overview?.equity_start);
    const base = Number.isFinite(eqStart) ? eqStart : 0;
    const closed = trades
      .filter((t) => String(t?.status || '').toUpperCase() === 'CLOSED')
      .sort((a, b) => new Date(a?.closed_at || a?.created_at || 0).getTime() - new Date(b?.closed_at || b?.created_at || 0).getTime());
    if (!closed.length) return [];
    let eq = base;
    return closed.map((t) => {
      eq += Number(t?.pnl_net ?? 0);
      return eq;
    });
  }, [trades, overview?.equity_start]);

  const equityCurveNote = useMemo(() => {
    const eqStart = Number(overview?.equity_start);
    if (!Number.isFinite(eqStart)) return 'equity_start not provided; curve uses cumulative pnl_net only';
    return 'Derived from closed trades + equity_start';
  }, [overview?.equity_start]);

  const accountState = useMemo(() => computeAccountState({ overview, trades, positions }), [overview, trades, positions]);

  const positionsMismatch = useMemo(() => {
    const ov = Number(overview?.open_positions);
    if (!Number.isFinite(ov)) return null;
    if (ov > 0 && positions.length === 0) return `Mismatch: overview.open_positions=${ov} but /positions returned 0`;
    return null;
  }, [overview?.open_positions, positions.length]);

  const handleUpload = async (file: File) => {
    try {
      const res = await uploadDataset(file);
      if (res?.dataset_id) setSelectedDatasetId(String(res.dataset_id));
      await loadDatasets();
      await loadOverview();
      setToast({ type: 'ok', message: 'Dataset uploaded' });
    } catch (e: any) {
      const msg = errorText(e);
      setError(msg);
      setToast({ type: 'err', message: msg });
    }
  };

  return (
    <main className='w-full overflow-x-hidden px-3 md:px-4 py-4 space-y-4'>
      <section className='sticky top-0 z-40'>
        <div className='rounded-xl border border-zinc-800 bg-zinc-950/95 backdrop-blur px-4 py-3 space-y-3'>
          <div className='flex flex-wrap items-center gap-2 text-[11px] md:text-xs'>
            <ModeBadge mode={String(overview?.mode || 'LIVE').toUpperCase()} />
            <span className='inline-flex h-8 items-center gap-2 rounded border border-zinc-700 bg-zinc-900 px-2.5'><Led on={isRunning} />{isRunning ? 'running' : 'stopped'}</span>
            <select value={profile} disabled={profileLoading} onChange={async (e) => { try { setProfileLoading(true); await controlSetProfile(e.target.value as 'TREND_STABLE' | 'SCALPER_STABLE'); await loadOverview(); } catch (err: any) { setError(errorText(err)); } finally { setProfileLoading(false); } }} className='h-8 rounded border border-zinc-700 bg-zinc-900 px-2.5'>
              {PROFILE_OPTIONS.map((p) => <option key={p} value={p}>{p}</option>)}
            </select>
            <span className='inline-flex h-8 items-center rounded border border-zinc-700 bg-zinc-900 px-2.5'>Clock {text(clock)}</span>
            <span className='inline-flex h-8 items-center rounded border border-zinc-700 bg-zinc-900 px-2.5'>Ptr {text(pointer)}</span>
            <span className='inline-flex h-8 items-center rounded border border-zinc-700 bg-zinc-900 px-2.5'>API {apiBaseUrl}</span>
          </div>

          <div className='pt-3 border-t border-zinc-800/70 space-y-3'>
            <div className='flex flex-col gap-3 xl:flex-row xl:items-center xl:justify-between'>
              <div className='flex flex-wrap items-center gap-3'>
                <select value={selectedDatasetId} onChange={(e) => setSelectedDatasetId(e.target.value)} className='h-9 rounded border border-zinc-700 bg-zinc-900 px-3 min-w-[230px] text-sm'>
                  <option value=''>Select dataset</option>
                  {datasets.map((d) => <option key={d.id} value={d.id}>{d.filename}</option>)}
                </select>
                <input ref={fileRef} type='file' accept='.csv' className='hidden' onChange={(e) => e.target.files?.[0] && void handleUpload(e.target.files[0])} />
                <button className='h-9 rounded border border-zinc-700 bg-zinc-900 px-3 text-sm' onClick={() => fileRef.current?.click()}>Upload</button>
                <button className='h-9 rounded border border-zinc-700 bg-zinc-900 px-3 text-sm opacity-60 cursor-not-allowed' disabled title='Replace dataset action unavailable'>Replace</button>
                <button className='h-9 rounded border border-zinc-700 bg-zinc-900 px-3 text-sm opacity-60 cursor-not-allowed' disabled title='Clear dataset action unavailable'>Clear</button>
                <button className='h-9 rounded border border-zinc-700 bg-zinc-900 px-3 text-sm opacity-60 cursor-not-allowed' disabled title='Download action unavailable'>Download</button>
              </div>

              <div className='flex flex-wrap items-center gap-3'>
                <button className='h-9 rounded border border-cyan-700 bg-cyan-950/30 px-3 text-sm text-cyan-200 disabled:opacity-50 disabled:cursor-not-allowed' disabled={!canStart} title={canStart ? '' : 'Select or upload a dataset first'} onClick={() => replayStart({ dataset_id: selectedDatasetId, speed: Math.max(1, Math.round(speed)) }).then(loadOverview).then(loadStreams).catch((e) => setError(errorText(e)))}>Start</button>
                <button className='h-9 rounded border border-zinc-700 bg-zinc-900 px-3 text-sm' onClick={() => replayPause().then(loadOverview).catch((e) => setError(errorText(e)))}>Pause</button>
                <button className='h-9 rounded border border-zinc-700 bg-zinc-900 px-3 text-sm' onClick={() => replayResume().then(loadOverview).catch((e) => setError(errorText(e)))}>Resume</button>
                <button className='h-9 rounded border border-zinc-700 bg-zinc-900 px-3 text-sm' onClick={() => replayStep().then(loadOverview).then(loadStreams).catch((e) => setError(errorText(e)))}>Step</button>
                <button className='h-9 rounded border border-rose-700 bg-rose-950/30 px-3 text-sm text-rose-200' onClick={() => replayStop().then(loadOverview).catch((e) => setError(errorText(e)))}>Stop</button>
                <button className='h-9 rounded border border-rose-800 bg-rose-950/20 px-3 text-sm text-rose-100' onClick={() => replayReset().then(loadOverview).then(loadStreams).catch((e) => setError(errorText(e)))}>Reset</button>
                <div className='flex h-9 items-center gap-2 rounded border border-zinc-700 bg-zinc-900 px-3 text-sm'>
                  <span>Speed</span>
                  <input type='range' min='1' max='10' value={Math.round(speed)} onChange={(e) => setSpeed(Number(e.target.value) || 1)} />
                  <span>{Math.round(speed)}x</span>
                </div>
              </div>
            </div>
          </div>

        </div>
      </section>

      {inspectorOpen && (
        <section className='rounded-lg border border-zinc-800 bg-zinc-950 p-4 text-xs space-y-3'>
          <div className='flex items-center justify-between gap-2'>
            <h2 className='text-sm font-semibold'>Truth Inspector</h2>
            <button
              className='rounded border border-zinc-700 bg-zinc-900 px-2 py-1'
              onClick={async () => {
                const snapshot = {
                  overview,
                  trades,
                  positions,
                  events,
                  computedAccountState: accountState,
                  fetchMeta,
                };
                try {
                  await navigator.clipboard.writeText(JSON.stringify(snapshot, null, 2));
                  setToast({ type: 'ok', message: 'Snapshot copied' });
                } catch (e: any) {
                  setError(errorText(e));
                }
              }}
            >
              Copy Snapshot
            </button>
          </div>

          <div className='grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-2'>
            {(['overview', 'trades', 'positions', 'events'] as EndpointKey[]).map((key) => (
              <div key={key} className='rounded border border-zinc-800 p-2'>
                <div className='font-semibold uppercase text-zinc-300'>{key}</div>
                <div>last ok: {fmtTs(fetchMeta[key].lastOkAt)}</div>
                <div>last err: {fmtTs(fetchMeta[key].lastErrAt)}</div>
                <div>duration: {fetchMeta[key].lastDurationMs === null ? '-' : `${fetchMeta[key].lastDurationMs} ms`}</div>
                <div>count: {fetchMeta[key].lastCount === null ? '-' : fetchMeta[key].lastCount}</div>
                <div className='text-rose-300 truncate' title={text(fetchMeta[key].lastErr)}>{fetchMeta[key].lastErr ? `err: ${fetchMeta[key].lastErr}` : 'err: -'}</div>
              </div>
            ))}
          </div>

          <div className='rounded border border-zinc-800 p-2 overflow-x-auto'>
            <table className='min-w-[560px] w-full'>
              <tbody>
                <tr><td className='py-1 text-zinc-400'>equity_start</td><td>{fmtNum(accountState.equity_start, 2)}</td></tr>
                <tr><td className='py-1 text-zinc-400'>realized_net</td><td>{fmtNum(accountState.realized_net, 2)}</td></tr>
                <tr><td className='py-1 text-zinc-400'>realized_gross</td><td>{fmtNum(accountState.realized_gross, 2)}</td></tr>
                <tr><td className='py-1 text-zinc-400'>fees_total</td><td>{fmtNum(accountState.fees_total, 2)}</td></tr>
                <tr><td className='py-1 text-zinc-400'>unrealized</td><td>{fmtNum(accountState.unrealized, 2)}</td></tr>
                <tr><td className='py-1 text-zinc-400'>expected_equity</td><td>{fmtNum(accountState.expected_equity, 2)}</td></tr>
                <tr><td className='py-1 text-zinc-400'>actual_equity</td><td>{fmtNum(accountState.actual_equity, 2)}</td></tr>
                <tr><td className='py-1 text-zinc-400'>delta</td><td>{accountState.delta === null ? '-' : fmtNum(accountState.delta, 4)}</td></tr>
              </tbody>
            </table>
          </div>

          <div className='rounded border border-zinc-800 p-2'>
            <div className='font-semibold mb-1'>Counts</div>
            <div>Trades: {trades.length} | Events: {events.length} | Positions: {positions.length}</div>
            <div>Latest trade id: {text(trades[0]?.id)}</div>
            <div>Latest event ts: {fmtTs(isoFromMs(events[0]?.ts) || events[0]?.created_at)}</div>
            <div>Latest position id: {text(positions[0]?.id)}</div>
            <div>Latest position ts: {fmtTs(positions[0]?.updated_at || positions[0]?.created_at)}</div>
          </div>

          <div className='rounded border border-zinc-800 p-2'>
            <div className='font-semibold mb-1'>Warnings</div>
            {accountState.warnings.length ? (
              <ul className='list-disc pl-4 space-y-1'>
                {accountState.warnings.map((warning, idx) => <li key={`${warning}-${idx}`}>{warning}</li>)}
              </ul>
            ) : (
              <div className='text-zinc-500'>No warnings</div>
            )}
          </div>
        </section>
      )}

      {toast && <div className={`fixed top-24 right-4 z-50 rounded border px-3 py-2 text-xs ${toast.type === 'ok' ? 'border-emerald-700 bg-emerald-950 text-emerald-200' : 'border-rose-700 bg-rose-950 text-rose-200'}`}>{toast.message}</div>}
      {error && <div className='rounded border border-rose-700 bg-rose-950/30 px-3 py-2 text-sm'>{error}</div>}

      <section className='grid grid-cols-2 lg:grid-cols-4 gap-3'>
        <div className='rounded-lg border border-zinc-800 bg-gradient-to-b from-zinc-900 to-zinc-950 px-3 py-3'><div className='text-[10px] uppercase tracking-wider text-zinc-400'>EQUITY NOW</div><div className='text-2xl font-semibold'>{fmtNum(overview?.equity_now ?? overview?.equity, 2)}</div></div>
        <div className='rounded-lg border border-zinc-800 bg-gradient-to-b from-zinc-900 to-zinc-950 px-3 py-3'><div className='text-[10px] uppercase tracking-wider text-zinc-400'>REALIZED NET</div><div className='text-2xl font-semibold'>{fmtNum(overview?.realized_pnl_net ?? overview?.realized_net, 2)}</div></div>
        <div className='rounded-lg border border-zinc-800 bg-gradient-to-b from-zinc-900 to-zinc-950 px-3 py-3'><div className='text-[10px] uppercase tracking-wider text-zinc-400'>UNREALIZED</div><div className={`text-2xl font-semibold ${Number(overview?.unrealized_pnl ?? overview?.unrealized) > 0 ? 'text-emerald-400' : Number(overview?.unrealized_pnl ?? overview?.unrealized) < 0 ? 'text-rose-400' : ''}`}>{fmtNum(overview?.unrealized_pnl ?? overview?.unrealized, 2)}</div></div>
        <div className='rounded-lg border border-zinc-800 bg-gradient-to-b from-zinc-900 to-zinc-950 px-3 py-3'><div className='text-[10px] uppercase tracking-wider text-zinc-400'>FEES TOTAL</div><div className='text-2xl font-semibold'>{fmtNum(overview?.fees_total, 2)}</div></div>
        <div className='rounded-lg border border-zinc-800 bg-gradient-to-b from-zinc-900 to-zinc-950 px-3 py-3'><div className='text-[10px] uppercase tracking-wider text-zinc-400'>GLOBAL DD%</div><div className='text-2xl font-semibold'>{fmtNum(overview?.dd?.global_dd_pct ?? overview?.global_dd_pct, 2)}</div></div>
        <div className='col-span-2 rounded-lg border border-zinc-800 bg-gradient-to-b from-zinc-900 to-zinc-950 px-3 py-3'>
          <div className='text-[10px] uppercase tracking-wider text-zinc-400'>GOAL PROGRESS</div>
          <div className='text-sm mt-1'>{fmtNum(overview?.goal?.progress_pct, 2)}% / {fmtNum(overview?.goal?.target_pct, 2)}%</div>
          <div className='mt-2 grid grid-cols-20 gap-0.5'>{Array.from({ length: 20 }).map((_, i) => <div key={i} className={`h-1.5 rounded-[2px] ${i < Number(overview?.goal?.progress_ratio || 0) * 20 ? 'bg-zinc-200' : 'bg-zinc-800'}`} />)}</div>
        </div>
      </section>

      <section className='rounded-lg border border-zinc-800 bg-zinc-950 p-4'>
        <div className='flex items-center justify-between gap-2 mb-3'>
          <h2 className='text-sm font-semibold uppercase tracking-wide text-zinc-300'>TodayStrip</h2>
          <button className='h-8 rounded border border-zinc-700 bg-zinc-900 px-3 text-xs' onClick={() => setInspectorOpen((prev) => !prev)}>
            Truth Inspector {inspectorOpen ? 'On' : 'Off'}
          </button>
        </div>
        <div className='grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-6 gap-3 text-xs'>
          <div className='rounded border border-zinc-800 bg-zinc-900/60 p-3 space-y-1'>
            <div className='text-zinc-400 uppercase tracking-wide text-[10px]'>Trading Day</div>
            <div className='text-sm font-semibold'>{dayKey}</div>
            <div className={`inline-flex rounded border px-2 py-0.5 ${rolloverInEffect ? 'border-amber-700 bg-amber-950/30 text-amber-200' : 'border-zinc-700 bg-zinc-900 text-zinc-300'}`}>
              {rolloverInEffect ? 'Rollover in effect' : 'Rollover idle'}
            </div>
          </div>
          <div className='rounded border border-zinc-800 bg-zinc-900/60 p-3 space-y-1'>
            <div className='text-zinc-400 uppercase tracking-wide text-[10px]'>Pre-trade Decision</div>
            <div className={`inline-flex rounded border px-2 py-0.5 ${preTradeAllowed ? 'border-emerald-700 bg-emerald-950/30 text-emerald-300' : 'border-rose-700 bg-rose-950/30 text-rose-300'}`}>
              {preTradeAllowed ? 'ALLOWED' : 'BLOCKED'}
            </div>
            <div className='text-zinc-300 truncate' title={preTradeReason}>{preTradeAllowed ? '-' : preTradeReason}</div>
          </div>
          <div className='rounded border border-zinc-800 bg-zinc-900/60 p-3 space-y-1'>
            <div className='text-zinc-400 uppercase tracking-wide text-[10px]'>Daily Counters</div>
            <div>Trades: {text(overview?.day?.daily_trade_count ?? overview?.governor?.stats?.trades_today)} / {text(overview?.governor?.config?.max_trades_per_day)}</div>
            <div>Loss streak: {text(overview?.day?.daily_consecutive_losses ?? overview?.governor?.stats?.consecutive_losses)} / {text(overview?.governor?.config?.max_consecutive_losses)}</div>
          </div>
          <div className='rounded border border-zinc-800 bg-zinc-900/60 p-3 space-y-1'>
            <div className='text-zinc-400 uppercase tracking-wide text-[10px]'>Daily PnL</div>
            <div>Realized: {fmtNum(overview?.day?.daily_realized_pnl ?? overview?.governor?.stats?.daily_pnl_net, 2)}</div>
            <div>Fees: {fmtNum(overview?.day?.daily_fees, 2)}</div>
          </div>
          <div className='rounded border border-zinc-800 bg-zinc-900/60 p-3 space-y-1'>
            <div className='text-zinc-400 uppercase tracking-wide text-[10px]'>Recon Health</div>
            <div className={`inline-flex rounded border px-2 py-0.5 ${reconOk ? 'border-emerald-700 bg-emerald-950/30 text-emerald-300' : 'border-amber-700 bg-amber-950/30 text-amber-200'}`}>
              {reconOk ? 'OK' : 'MISMATCH'}
            </div>
            <div>Mismatch count: {text(reconCycles)}</div>
            <div>Staleness: {text(reconStaleMs)} ms</div>
          </div>
        <div className='rounded border border-zinc-800 bg-zinc-900/60 p-3 space-y-1'>
          <div className='text-zinc-400 uppercase tracking-wide text-[10px]'>Kill Switch / Trips</div>
          <div>Mode: {String(safetyMode || 'off').toUpperCase()}</div>
          <div className='truncate' title={safetyTripReason}>Reason: {safetyTripReason}</div>
          <div>Time: {fmtTs(safetyTripAt)}</div>
        </div>
      </div>
      <div className='mt-3 text-[11px] text-zinc-500'>
        Data sanity: Trades:{trades.length}, Events:{events.length}, Positions:{positions.length}, last fetch {ageLabel(lastRefreshAt)}, tick {pollTick}
      </div>
      </section>

      <section className='rounded-lg border border-zinc-800 bg-zinc-950 p-4'>
        <div className='flex items-center justify-between mb-2'>
          <h2 className='text-lg font-semibold'>Equity Curve</h2>
          <div className='text-xs text-zinc-400'>{equityCurveNote}</div>
        </div>
        <Sparkline values={equitySeries} />
      </section>

      <section className='grid grid-cols-1 xl:grid-cols-[3fr_2fr] gap-4'>
        <div className='rounded-lg border border-zinc-800 bg-zinc-950 p-4 overflow-hidden'>
            <div className='flex items-center justify-between mb-2'>
              <h2 className='text-lg font-semibold'>Open Position</h2>
              {positionsMismatch && <span className='text-[11px] text-amber-300 border border-amber-700 rounded px-2 py-0.5'>{positionsMismatch}</span>}
            </div>
            <div className='text-[11px] text-zinc-500 mb-2'>Positions: {positions.length} (rawShape={positionsRawShape}) | last {ageLabel(fetchMeta.positions.lastOkAt)}</div>
          {!displayPosition ? (
            <div className='text-sm text-zinc-400 space-y-2'>
              <div>No open position</div>
              {positionsMismatch && latestEntry && (
                <div className='rounded border border-amber-800 bg-amber-950/20 px-3 py-2 text-xs'>
                  Last ENTRY snapshot: {text(latestEntry?.symbol)} | {text(latestEntry?.rationale)} | ts {fmtTs(isoFromMs(latestEntry?.ts) || latestEntry?.created_at)}
                </div>
              )}
              {latestExit ? <div className='rounded border border-zinc-800 bg-zinc-900 px-3 py-2 text-xs'>Last EXIT: {text(latestExit?.symbol)} | {text(latestExit?.rationale)} | pnl {fmtNum(latestExit?.risk_state_snapshot?.pnl_net, 2)} | fees {fmtNum(latestExit?.risk_state_snapshot?.fees_total, 2)} | {fmtTs(isoFromMs(latestExit?.ts) || latestExit?.created_at)}</div> : <div className='text-xs text-zinc-500'>No exit event yet</div>}
            </div>
          ) : (
            <div className='grid grid-cols-1 md:grid-cols-2 gap-3 text-sm'>
              <div className='space-y-1'>
                <div className='flex items-center gap-2'>
                  <div className='text-2xl font-semibold'>{text(displayPosition?.symbol)}</div>
                  <span className={`rounded px-2 py-1 text-xs ${String(displayPosition?.side || '').toUpperCase() === 'BUY' ? 'bg-emerald-900 text-emerald-300' : 'bg-rose-900 text-rose-300'}`}>{String(displayPosition?.side || '').toUpperCase() === 'BUY' ? 'LONG' : 'SHORT'}</span>
                </div>
                <div>Entry {fmtNum(displayPosition?.entry_price, 2)}</div>
                <div>Qty {fmtNum(displayPosition?.quantity, 4)}</div>
                <div>SL {fmtNum(displayOpenTrade?.stop_price, 2)}</div>
                <div>TP1 {fmtNum(displayOpenTrade?.tp1_price, 2)}</div>
                <div>TP2 {fmtNum(displayOpenTrade?.tp2_price, 2)}</div>
                <div>Bars/TimeStop {text(displayOpenTrade?.bars_held)} / {text(displayOpenTrade?.time_stop_bars)}</div>
                <div>uPnL {displayPosition?.unrealized_pnl !== undefined ? fmtNum(displayPosition?.unrealized_pnl, 2) : 'uPnL not supported yet'}</div>
              </div>
              <div className='space-y-2 flex flex-col justify-end'>
                <button className='rounded border border-rose-700 bg-rose-900/35 px-3 py-2 text-rose-300' onClick={async () => { if (!displayOpenTrade?.id) return setError('open trade id missing'); if (!window.confirm('Close position now?')) return; try { await executeCloseNow({ mode: String(overview?.mode || 'REPLAY'), tradeId: String(displayOpenTrade.id), symbol: String(displayOpenTrade?.symbol || displayPosition?.symbol || '') }); await loadStreams(); await loadOverview(); } catch (e: any) { setError(errorText(e)); } }}>Close Now</button>
                <button
                  disabled
                  title='Coming later'
                  className='rounded border border-zinc-700 bg-zinc-900 px-3 py-2 text-zinc-400 cursor-not-allowed'
                  onClick={async () => {
                    try {
                      await executeReduceHalf({
                        mode: String(overview?.mode || 'REPLAY'),
                        tradeId: displayOpenTrade?.id ? String(displayOpenTrade.id) : undefined,
                        symbol: String(displayOpenTrade?.symbol || displayPosition?.symbol || ''),
                        fraction: 0.5,
                      });
                    } catch {
                      // intentionally noop while button remains disabled
                    }
                  }}
                >
                  Reduce 50%
                </button>
              </div>
            </div>
          )}
        </div>

        <div className='rounded-lg border border-zinc-800 bg-zinc-950 p-4'>
          <h2 className='text-lg font-semibold mb-2'>Signal / Reasoning</h2>
          <div className='space-y-2 text-xs'>
            <div className='flex justify-between'><span>Regime Gate</span><span className={`rounded border px-2 py-0.5 ${regimeGateOk === true ? 'border-emerald-700 text-emerald-300' : regimeGateOk === false ? 'border-rose-700 text-rose-300' : 'border-zinc-700 text-zinc-300'}`}>{regimeGateOk === true ? 'PASS' : regimeGateOk === false ? 'FAIL' : '-'}</span></div>
            {regimeWarmup && (
              <div className='rounded border border-amber-800 bg-amber-950/20 px-2 py-1'>
                Warmup (LTF){Number.isFinite(ltfBarsRemaining) ? ` - ~${Math.max(0, Math.round(ltfBarsRemaining))} bars remaining` : ''}
              </div>
            )}
            {regimeFallbackUsed && (
              <div className='rounded border border-zinc-700 bg-zinc-900 px-2 py-1'>
                HTF missing: using LTF fallback
              </div>
            )}
            {regimeGateOk === false && regimeGateReasons.length > 0 && !regimeWarmup && <div className='rounded border border-rose-800 bg-rose-950/20 px-2 py-1'>Gate reasons: {regimeGateReasons.slice(0, 2).join(', ')}</div>}
            <div className='flex justify-between'><span>Active mode</span><span className='rounded border border-zinc-700 px-2 py-0.5'>{text(activeMode)}</span></div>
            <div className='rounded border border-zinc-800 px-2 py-1'>mode_reasons: {modeReasons.length ? modeReasons.join(', ') : 'n/a'}</div>
            <div className='flex justify-between'><span>Regime</span><span className='rounded border border-zinc-700 px-2 py-0.5'>{text(overview?.latest_decision?.regime)}</span></div>
            <div className='flex justify-between font-mono'><span>Decision</span><span>{text(latestDecisionEvent?.risk_state_snapshot?.decision || overview?.latest_decision?.decision)}</span></div>
            <div className='flex justify-between font-mono'><span>Final Action</span><span>{text(latestDecisionEvent?.risk_state_snapshot?.final_action || overview?.latest_decision?.final_action)}</span></div>
            <div className='flex justify-between'><span>Entry Eligibility</span><span>{text(latestDecisionEvent?.risk_state_snapshot?.entry_eligibility ?? overview?.latest_decision?.entry_eligibility)}</span></div>
            <div className='flex justify-between'><span>Router Strategy</span><span>{text(latestDecisionEvent?.risk_state_snapshot?.router_selected_strategy || overview?.latest_decision?.router_selected_strategy)}</span></div>
            <div className='flex justify-between'><span>Trade Blocker (primary)</span><span>{text(latestDecisionEvent?.risk_state_snapshot?.trade_blocker_primary || overview?.latest_decision?.trade_blocker_primary)}</span></div>
            <div className='rounded border border-zinc-800 px-2 py-1'><span>Trade Blockers: {Array.isArray(latestDecisionEvent?.risk_state_snapshot?.trade_blockers) && latestDecisionEvent.risk_state_snapshot.trade_blockers.length ? latestDecisionEvent.risk_state_snapshot.trade_blockers.join(', ') : Array.isArray(overview?.latest_decision?.trade_blockers) && overview.latest_decision.trade_blockers.length ? overview.latest_decision.trade_blockers.join(', ') : 'n/a'}</span></div>
            <div className='flex justify-between'><span>Score</span><span>{fmtNum(latestDecisionEvent?.risk_state_snapshot?.score_total ?? overview?.latest_decision?.score, 2)}</span></div>
            <div className='grid grid-cols-1 gap-1'>
              <div className='flex justify-between rounded border border-zinc-800 px-2 py-1'><span>trend_strength</span><span>{fmtNum(regimeGateMetrics?.trend_strength, 3)}</span></div>
              <div className='flex justify-between rounded border border-zinc-800 px-2 py-1'><span>atr_pct</span><span>{fmtNum(regimeGateMetrics?.atr_pct, 3)}</span></div>
              <div className='flex justify-between rounded border border-zinc-800 px-2 py-1'><span>{regimeGateMetrics?.adx !== undefined && regimeGateMetrics?.adx !== null ? 'adx' : 'chop_ratio'}</span><span>{fmtNum(regimeGateMetrics?.adx ?? regimeGateMetrics?.chop_ratio, 3)}</span></div>
              <div className='flex justify-between rounded border border-zinc-800 px-2 py-1'><span>atr_expand_ratio</span><span>{fmtNum(regimeGateMetrics?.atr_expand_ratio, 3)}</span></div>
              <div className='flex justify-between rounded border border-zinc-800 px-2 py-1'><span>htf_slope_pct</span><span>{regimeGateMetrics?.htf_slope_pct === null || regimeGateMetrics?.htf_slope_pct === undefined ? 'n/a' : fmtNum(regimeGateMetrics?.htf_slope_pct, 4)}</span></div>
              <div className='flex justify-between rounded border border-zinc-800 px-2 py-1'><span>box_high/box_low</span><span>{text(latestDecisionSnapshot?.breakout_box_high)} / {text(latestDecisionSnapshot?.breakout_box_low)}</span></div>
              <div className='flex justify-between rounded border border-zinc-800 px-2 py-1'><span>compression/recent</span><span>{text(latestDecisionSnapshot?.breakout_compression)} / {text(latestDecisionSnapshot?.breakout_recent)}</span></div>
              <div className='flex justify-between rounded border border-zinc-800 px-2 py-1'><span>pullback_v2</span><span>{text(latestDecisionSnapshot?.pullback_v2_ok)}</span></div>
              <div className='rounded border border-zinc-800 px-2 py-1'><span>pb2_reasons: {Array.isArray(latestDecisionSnapshot?.pullback_v2_reasons) && latestDecisionSnapshot.pullback_v2_reasons.length ? latestDecisionSnapshot.pullback_v2_reasons.join(', ') : 'n/a'}</span></div>
              <div className='flex justify-between rounded border border-zinc-800 px-2 py-1'><span>size base/mult/final</span><span>{fmtNum(latestDecisionSnapshot?.base_qty, 4)} / {fmtNum(latestDecisionSnapshot?.size_mult, 3)} / {fmtNum(latestDecisionSnapshot?.final_qty, 4)}</span></div>
            </div>
            <div className='h-1.5 rounded bg-zinc-800 overflow-hidden'><div className='h-1.5 bg-zinc-500' style={{ width: `${Math.max(0, Math.min(100, Number(latestDecisionEvent?.risk_state_snapshot?.score_total ?? overview?.latest_decision?.score ?? 0)))}%` }} /></div>
            <div className='space-y-1'><div className='text-zinc-400'>Components</div>{latestDecisionEvent?.risk_state_snapshot?.score_components ? Object.entries(latestDecisionEvent.risk_state_snapshot.score_components).map(([k, v]) => <div key={k} className='flex justify-between rounded border border-zinc-800 px-2 py-1'><span>{k}</span><span>{fmtNum(v, 3)}</span></div>) : <div className='text-zinc-500'>No component payload</div>}</div>
            <div className='space-y-1'><div className='text-zinc-400'>Reasons</div>{(latestDecisionEvent?.risk_state_snapshot?.reasons || []).length ? (latestDecisionEvent.risk_state_snapshot.reasons as string[]).map((r, i) => <div key={`${r}-${i}`} className='rounded border border-zinc-800 px-2 py-1 font-mono'>{r}</div>) : <div className='text-zinc-500'>No reasons payload</div>}</div>
            {!overview?.governor?.eligible && <div className='space-y-1'>{(overview?.governor?.blockers || []).slice(0, 3).map((b: any, i: number) => <div key={`${b?.name}-${i}`} className='rounded border border-rose-800 bg-rose-950/30 px-2 py-1'>{text(b?.name)} | threshold {text(b?.threshold)} | current {text(b?.current)}</div>)}</div>}
          </div>
        </div>
      </section>

      <section className='grid grid-cols-1 xl:grid-cols-[3fr_2fr] gap-4'>
        <div className='rounded-lg border border-zinc-800 bg-zinc-950 p-4'>
          <h2 className='text-lg font-semibold mb-2'>Risk / Governor</h2>
          <div className='grid grid-cols-1 md:grid-cols-2 gap-3 text-xs'>
            <div className='rounded border border-zinc-800 p-2 space-y-1'><div className='text-zinc-400'>Limits</div><div>Max trades/day {text(overview?.governor?.config?.max_trades_per_day)}</div><div>Max daily loss % {text(overview?.governor?.config?.max_daily_loss_pct)}</div><div>Max global dd % {text(overview?.governor?.config?.max_global_dd_pct)}</div><div>Max consecutive losses {text(overview?.governor?.config?.max_consecutive_losses)}</div><div>Cooldown minutes {text(overview?.governor?.config?.cooldown_minutes)}</div></div>
            <div className='rounded border border-zinc-800 p-2 space-y-1'><div className='text-zinc-400'>Usage</div><div>Eligibility {overview?.governor?.eligible === false ? 'Blocked' : 'Eligible'}</div><div>Blockers {text(overview?.governor?.blockers?.length ?? 0)}</div><div>Global dd % {fmtNum(overview?.governor?.stats?.global_dd_pct ?? overview?.dd?.global_dd_pct, 2)}</div><div>Cooldown remaining {text(overview?.governor?.stats?.cooldown_remaining_minutes)}</div></div>
          </div>
        </div>
        <div className='rounded-lg border border-zinc-800 bg-zinc-950 p-4'>
          <h2 className='text-lg font-semibold mb-2'>Activity</h2>
          <div className='space-y-2 text-xs'>
            <div>Win rate {fmtNum(activity.winRate, 2)}%</div>
            <div>Avg win {fmtNum(activity.avgWin, 2)}</div>
            <div>Avg loss {fmtNum(activity.avgLoss, 2)}</div>
            <div>Profit factor {fmtNum(activity.profitFactor, 2)}</div>
            <div>Expectancy {fmtNum(activity.expectancy, 2)}</div>
            <div>Fees total {fmtNum(activity.feesTotal, 2)}</div>
            <div>Fees today {activity.feesToday === null ? '-' : fmtNum(activity.feesToday, 2)}</div>
          </div>
        </div>
      </section>

      <section className='space-y-4'>
        <div className='rounded-lg border border-zinc-800 bg-zinc-950 p-4 min-w-0 w-full'>
          <h2 className='text-lg font-semibold mb-2'>Trade Log</h2>
          <div className='text-[11px] text-zinc-500 mb-2'>Trades: {trades.length} (rawShape={tradesRawShape}) | last {ageLabel(fetchMeta.trades.lastOkAt)}</div>
          {trades.length ? <TradeTable items={trades} /> : <div className='text-sm text-zinc-500'>No trades returned by API. count: {trades.length}</div>}
        </div>
        <div className='rounded-lg border border-zinc-800 bg-zinc-950 p-4 min-w-0'>
          <div className='flex flex-wrap items-center justify-between gap-2 mb-2'>
            <h2 className='text-lg font-semibold'>Event Tape</h2>
            <div className='text-[11px] text-zinc-500'>Events: {events.length} (rawShape={eventsRawShape}) | last {ageLabel(fetchMeta.events.lastOkAt)}</div>
            <div className='flex flex-wrap gap-1 text-xs'>{(['ALL', 'DECISION', 'SIGNAL', 'BLOCKED', 'ENTRY', 'PARTIAL', 'EXIT', 'ERROR'] as EventFilter[]).map((f) => <button key={f} onClick={() => setEventFilter(f)} className={`rounded border px-2 py-1 ${eventFilter === f ? 'border-zinc-500 bg-zinc-700' : 'border-zinc-700 bg-zinc-900'}`}>{f}</button>)}</div>
          </div>
          <div className='h-[300px] overflow-y-auto border border-zinc-800 rounded'>
            {filteredEvents.length ? filteredEvents.map((e) => {
              const typ = String(e?.event_type || '').toUpperCase();
              const accent = typ === 'BLOCKED' ? 'bg-rose-950/30' : typ === 'ENTRY' ? 'bg-emerald-950/20' : '';
              return <button key={e?.id} className={`w-full text-left border-b border-zinc-900 px-2 py-1.5 text-xs ${accent}`} onClick={() => setSelectedEventId(String(e?.id))}>{fmtTs(isoFromMs(e?.ts) || e?.created_at)} | {text(e?.symbol)} | {typ} | {text(e?.rationale)}</button>;
            }) : <div className='text-sm text-zinc-500 p-2'>No events returned by API. count: {filteredEvents.length}</div>}
          </div>
          {selectedEvent && (
            <div className='mt-2 rounded border border-zinc-800 bg-zinc-900 p-2 text-xs'>
              <div className='flex items-center justify-between mb-1'><div className='font-semibold'>Event JSON</div><button onClick={() => setSelectedEventId('')} className='rounded border border-zinc-700 px-2 py-0.5'>Close</button></div>
              <pre className='max-w-full overflow-x-auto whitespace-pre-wrap break-words font-mono'>{JSON.stringify(selectedEvent?.risk_state_snapshot || selectedEvent, null, 2)}</pre>
            </div>
          )}
        </div>
      </section>
    </main>
  );
}
