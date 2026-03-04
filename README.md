# HashiSignalGeneratorV2 - Phase 1 Foundation

Production-grade modular algorithmic trading system with async FastAPI backend, PostgreSQL, Redis, and Next.js dashboard.

## Stack
- Backend: FastAPI, SQLAlchemy 2.0 async, PostgreSQL, Redis, Uvicorn, Pydantic v2
- Frontend: Next.js 14 App Router, TypeScript, Tailwind, Axios
- Infra: Docker + Docker Compose

## Services
- `api`: FastAPI service on `:8000` with Swagger at `/docs`
- `dashboard`: Next.js service on `:3000`
- `postgres`: PostgreSQL 16
- `redis`: Redis 7

## Run
```bash
cp .env.example .env
docker compose up --build
```

## API Endpoints
- `POST /engine/start`
- `POST /engine/stop`
- `GET /engine/status`
- `POST /replay/start`
- `POST /replay/stop`
- `GET /risk/status`
- `GET /positions`
- `GET /trades?limit=&offset=`
- `GET /executions?limit=&offset=`
- `POST /signals/test`
- `POST /telegram/test`
- `GET /overview`

## Testing
```bash
cd backend
pytest
```

## Build 15m Dataset From Master 5m
```bash
python backend/scripts/build_15m_from_5m.py
python backend/scripts/smoke_build_dataset.py
```

Defaults:
- Input: `data/uploads/ETHUSDT_5m_master.csv`
- Output: `data/datasets/ETHUSDT_15m.csv`
- Optional env default selection:
  - `REPLAY_DATASET_DEFAULT=data/datasets/ETHUSDT_15m.csv`
  - `NEXT_PUBLIC_REPLAY_DATASET_DEFAULT=data/datasets/ETHUSDT_15m.csv`

## How To Verify
1. `docker compose up --build`
2. Upload a dataset from the dashboard.
3. Start replay and step forward a bit.
4. Call `GET /trades` and confirm:
   - `opened_at` and `closed_at` are present and match replay candle time.
   - `close_reason` is present on `CLOSED` rows.
   - `fees_total > 0` for closed trades.
   - `pnl_net` is present.
5. In dashboard Trade Log, confirm columns show fees, net PnL, reason, opened/closed replay timestamps, leverage, and size.

## How To Verify Control Room
1. `docker compose down`
2. `docker compose up --build`
3. Open dashboard.
4. Upload dataset.
5. Start replay and step/run.
6. Confirm `/overview` includes:
   - `replay.candle_ts` matching dataset time.
   - `realized_pnl_net` and `fees_total` updating after closes.
   - `goal.progress_pct` updating.
   - `reconcile_ok` true and `reconcile_delta` near zero.
   - `dd.global_dd_pct` behaves reasonably.
   - `latest_decision` reflects most recent decision-like event.

```bash
curl -s http://localhost:8000/overview | jq
```

## How To Verify STEP 9 (Governor Gates)
1. `docker compose down`
2. `docker compose up --build`
3. Upload dataset and start replay.
4. Set strict limits (for quick blocking), e.g.:
   - `GOV_MAX_TRADES_PER_DAY=1` or `GOV_COOLDOWN_MINUTES=999`
5. Confirm:
   - New entries stop opening when governor blocks.
   - `/events` contains `BLOCKED` events with blocker details.
   - `/overview.governor.eligible` becomes `false` and `blockers` are populated.
   - Dashboard risk eligibility shows blocked and Explain displays blocker reasons.

## STEP 9 UI Verification
1. `docker compose down`
2. `docker compose up --build`
3. Open dashboard.
4. Upload dataset and start replay.
5. Force blocked state in `.env` (then restart), for example:
   - `GOV_MAX_TRADES_PER_DAY=1`
   - or `GOV_MAX_CONSECUTIVE_LOSSES=1`
   - or `GOV_MAX_DAILY_LOSS_PCT=0.1`
6. Confirm in UI:
   - Risk badge flips to `Blocked`.
   - Blockers panel shows blocker name and threshold/current.
   - Event tape shows `BLOCKED` rows with red emphasis.
   - No new trades open while blocked.
   - Trade log shows side/result pills and correct pnl/fees/timestamps.

## Step 10A Verification
1. `docker compose down`
2. `docker compose up --build`
3. Upload a 5m dataset and start replay.
4. Confirm Trade Log columns `Stop (SL)`, `TP1`, and `TP2` populate for trades (not dashes).
5. Confirm replay opens both long and short trades over sufficient history.
6. Confirm Event Tape includes `ENTRY`, `PARTIAL`, `EXIT`, and `DECISION` events with payload details.
7. Confirm `/overview` keeps `reconcile_ok=true` while replay runs and closes trades.
8. Set `GOV_MAX_TRADES_PER_DAY=1`, restart, and confirm governor still blocks new entries.

Notes:
- Entry fill rule is deterministic: signal candle close.
- SL/TP and time-stop are persisted on the trade row at entry.

## Step 10B UI Verification
1. `docker compose up --build`
2. Open dashboard at `http://localhost:3000`.
3. Start replay and confirm sticky header updates mode, clock, pointer, running state, and risk eligibility.
4. Change profile with the header dropdown and confirm `/overview.active_profile` updates.
5. Wait for an open trade and confirm Open Position card appears from `/positions`; click `Close Now` and confirm an `EXIT` event with `manual_close`.
6. Confirm Trade Log shows stop/tp1/tp2, score, regime, reason, result pill, and `PnL View: Net|Gross`.
7. Confirm Realized Net uses net PnL and is not double-subtracted by fees.
8. Confirm Event Tape shows ENTRY/PARTIAL/EXIT and BLOCKED rows when governor limits are hit.
9. On mobile width, confirm cards stack and only table/feed sections scroll horizontally/vertically, with no page overflow.

## Step 10B PRIME Verification
1. Open dashboard and confirm dataset upload is visible and functional.
2. Upload a dataset and verify:
   - Dataset list refreshes.
   - `/overview` refreshes.
   - Success toast appears.
3. Confirm `Start` is disabled when no dataset is selected, with tooltip guidance.
4. Start replay and confirm sticky header updates:
   - mode, engine LED, replay clock, pointer, progress bar, risk eligibility, active profile.
5. Confirm open position card reflects `/positions` and:
   - `Close Now` triggers `EXIT(manual_close)` in event tape.
6. Confirm trade log:
   - shows SL/TP1/TP2/score/regime columns,
   - supports expandable trade detail,
   - supports net/gross toggle when both backend fields exist.
7. Confirm event tape JSON viewer works and filters include `DECISION/SIGNAL/BLOCKED/ENTRY/PARTIAL/EXIT/ERROR`.
8. Confirm mobile behavior:
   - cards stack,
   - buttons remain tappable,
   - no page horizontal overflow,
   - only tables/feed scroll internally.

## Step 10B PRIME V2 Verification
1. Dataset upload is visible and works.
2. Control Block is separated from the Status Bar.
3. Engine running indicator updates and polling refreshes panel values.
4. `/positions` populates Open Position when a trade is open.
5. Trade Log shows rows when `/trades` returns data.
6. Event Tape shows rows when `/events` returns data.
7. Reconcile tile shows delta when mismatch exists (no false `OK`).
8. Equity curve panel is present with real derived series or explicit unavailable message.
9. Mobile layout stacks correctly and page has no horizontal overflow.

## Step 10B PRIME V2 Verification (Data Flow Fix)
1. Dataset upload is visible and still works.
2. Status Bar and Control Block are separate sticky rows.
3. `Trade Log` title shows `Trades: N (rawShape=...)`.
4. `Event Tape` title shows `Events: N (rawShape=...)`.
5. `Open Position` title shows `Positions: N (rawShape=...)`.
6. If `/overview.open_positions > 0` but `/positions` is empty, mismatch warning banner appears.
7. Trade Log populates even when `/trades` returns a singleton trade object.
8. Activity and Equity Curve derive from normalized trades.
9. Reconcile tile shows `OK` only when `abs(delta) < 0.01`; otherwise shows `Delta ...`.
10. Mobile has no page horizontal overflow; only table/feed areas scroll.

## Step 14A Regime Gate Verification
1. Run replay twice with the same CSV and identical `.env`; compare trade count and PnL to confirm deterministic outputs.
2. Confirm `GET /events` `DECISION` rows include regime gate fields in `risk_state_snapshot`:
   - `regime_gate_ok`
   - `regime_gate_reasons`
   - `regime_gate_metrics`
3. Confirm when gate blocks entries, decision remains `hold` and blockers include `regime_gate:*`.
4. Confirm `GET /overview` -> `latest_decision` includes:
   - `regime_gate_ok`
   - `top_regime_gate_reasons`
   - `regime_gate_metrics`
5. Compare replay windows before/after with same dataset and check trade frequency and PF from existing `/trades` statistics.

### Regime Gate Flags (15m Replay)
- `REGIME_REQUIRE_HTF=false`
- `REGIME_ALLOW_LTF_FALLBACK=true`
- `REGIME_MIN_LTF_BARS=250`
- `REGIME_HTF_TIMEFRAME=4h`

Recommended for `ETHUSDT_15m.csv` replay:
- Keep HTF optional (`REGIME_REQUIRE_HTF=false`) to avoid deadlock when HTF slope is unavailable.
- Keep LTF fallback enabled (`REGIME_ALLOW_LTF_FALLBACK=true`) so gate can classify with LTF metrics.

## Step 15 Feature Flags (Breakout / Pullback v2 / Vol Sizing)
New additive flags (default OFF):
- `FEATURE_BREAKOUT=false`
- `FEATURE_PULLBACK_V2=false`
- `FEATURE_VOL_SIZING=false`

When all three flags are `false`, strategy behavior remains baseline `TREND_STABLE`.

Enable examples:
- Breakout only: `FEATURE_BREAKOUT=true`
- Pullback v2 only: `FEATURE_PULLBACK_V2=true`
- Vol sizing only: `FEATURE_VOL_SIZING=true`
- All: set all three to `true`

Smoke fingerprint runner:
```bash
python backend/scripts/smoke_strategy_feature_flags.py --csv data/datasets/ETHUSDT_15m.csv
```
It runs:
1) baseline twice (asserts deterministic equality)
2) breakout-only
3) pullback-v2-only
4) vol-sizing-only
5) all-enabled
and prints trade-count/PnL/fees/win-rate/trade-hash fingerprints.

## How to debug no trades
1. **UI path**: open dashboard → **Truth Inspector / Signal / Reasoning** panel.
   - Check `Final Action`, `Entry Eligibility`, `Router Strategy`, `Trade Blocker (primary)`, and `Trade Blockers`.
   - These fields explain "PASS but no trade" with one primary blocker and full blocker list.
2. **API path**: query replay observability payload:
```bash
curl -s "http://localhost:8000/replay/observability?n=200" | jq
```
Response includes:
   - `decision_traces` (last N bars)
   - `blocker_counters` aggregate summary
   - `no_trade_streak` longest no-entry diagnostic
   - `replay` status (clock/running)
3. **Smoke script** (local or docker):
```bash
python backend/scripts/smoke_replay_observability.py
```
This runs a deterministic replay window (target 30 days for `ETHUSDT_15m`) and prints summary/top blockers/trade count/longest no-entry streak.

## Instant Backtest Mode (Dashboard)
1. Enable with env flag (default OFF): `INSTANT_BACKTEST_ENABLED=true`.
2. Open dashboard and switch mode toggle to `Backtest`.
3. Select/upload dataset, then click **Run Backtest**.
4. Watch progress from `/backtest/status` and results load from `/backtest/result`.

API:
- `POST /backtest/run` -> `{ run_id }`
- `GET /backtest/status?run_id=...` -> `{ state, progress_pct, message }`
- `GET /backtest/result?run_id=...` -> `{ summary, trades, equity_curve }`

Summary fields:
- `win_rate`: winning trades / closed trades (%).
- `profit_factor`: gross wins / abs(gross losses).
- `expectancy`: average net pnl per closed trade.
- `avg_win` / `avg_loss`: average net pnl over winning/losing trades.
- `max_dd`: peak-to-trough drawdown percent on sampled equity curve.
- `fees_total`: cumulative entry+exit fees.
- `trades_count`: number of closed trades.

Risk update behavior:
- TP1 no longer triggers partial close.
- TP1 hit now arms stop to breakeven (`entry +/- TP1_BE_OFFSET`) without loosening risk:
  - LONG: `new_sl >= old_sl`
  - SHORT: `new_sl <= old_sl`
- Replay emits `RISK_UPDATE {reason: tp1_to_be, old_sl, new_sl, price}` marker.

## Env Presets: Replay vs Instant Backtest (same strategy)
Use these exact commands:

```bash
docker compose --env-file .env.replay up --build
```

```bash
docker compose --env-file .env.instant up --build
```

Notes:
- `docker-compose.yml` reads `COMPOSE_ENV_FILE` so the selected preset file is also used as the API container env file.
- Both presets keep the same swing strategy/profile/symbol defaults and TP1->BE settings.

## Sanity Check (quick)
1. Start replay preset:
```bash
docker compose --env-file .env.replay up --build
```
2. Open dashboard (`http://localhost:3000`) and verify replay controls still work (start/pause/resume/step).
3. Confirm API replay status:
```bash
curl -s http://localhost:8000/replay/status
```

4. Start instant preset:
```bash
docker compose --env-file .env.instant up --build
```
5. Run instant backtest endpoints:
```bash
curl -s -X POST http://localhost:8000/backtest/run \
  -H 'Content-Type: application/json' \
  -d '{"dataset_path":"data/datasets/ETHUSDT_15m.csv","profile":"TREND_STABLE"}'
```
6. Use returned `run_id`:
```bash
curl -s "http://localhost:8000/backtest/status?run_id=<run_id>"
curl -s "http://localhost:8000/backtest/result?run_id=<run_id>"
```
