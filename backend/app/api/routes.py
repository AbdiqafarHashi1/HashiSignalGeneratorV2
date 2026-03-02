import asyncio
import logging
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from fastapi.responses import JSONResponse
from sqlalchemy import desc, func, select
from sqlalchemy.exc import DataError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db.session import get_db
from app.dependencies import get_engine_service
from app.models.entities import DecisionEvent, Execution, Position, ReplayDataset, Trade
from app.schemas.common import ReplayStartRequest
from app.services.accounting import PortfolioAccounting
from app.services.datasets import persist_upload
from app.services.engine import EngineService
from app.services.governor import GovernorService
from app.telegram.service import TelegramService

router = APIRouter()
logger = logging.getLogger(__name__)
_OVERVIEW_CACHE: dict[str, object] = {'snapshot': None, 'ts': 0.0, 'tick': -1}
_OVERVIEW_LOCK = asyncio.Lock()



def _invalidate_overview_cache() -> None:
    _OVERVIEW_CACHE['snapshot'] = None
    _OVERVIEW_CACHE['ts'] = 0.0
    _OVERVIEW_CACHE['tick'] = -1


async def _with_timeout(awaitable, timeout_s: float, default):
    try:
        return await asyncio.wait_for(awaitable, timeout=timeout_s), None
    except Exception as exc:
        return default, str(exc)



@router.post('/engine/start')
async def start_engine(engine: EngineService = Depends(get_engine_service)) -> dict:
    return await engine.start(mode='live')


@router.post('/engine/stop')
async def stop_engine(engine: EngineService = Depends(get_engine_service)) -> dict:
    return await engine.stop()


@router.get('/engine/status')
async def engine_status(engine: EngineService = Depends(get_engine_service)) -> dict:
    return engine.status()


@router.post('/replay/start')
async def replay_start(
    body: ReplayStartRequest,
    engine: EngineService = Depends(get_engine_service),
    db: AsyncSession = Depends(get_db),
) -> dict:
    speed = float(body.speed if body.speed is not None else body.speed_multiplier)
    csv_path = body.csv_path
    dataset: ReplayDataset | None = None
    dataset_id = str(body.dataset_id) if body.dataset_id else None
    if body.dataset_id:
        dataset = await db.get(ReplayDataset, body.dataset_id)
        if not dataset:
            raise HTTPException(status_code=400, detail='dataset_id not found')
    elif body.filename:
        dataset = (
            await db.execute(select(ReplayDataset).where(ReplayDataset.filename == body.filename).order_by(desc(ReplayDataset.created_at)).limit(1))
        ).scalar_one_or_none()
        if not dataset:
            raise HTTPException(status_code=400, detail=f'filename not found: {body.filename}')
    if dataset:
        dataset_id = str(dataset.id)
        csv_path = dataset.stored_path
    if not csv_path:
        csv_path = settings.replay_dataset_default
    if not csv_path:
        raise HTTPException(status_code=400, detail='Provide dataset_id, filename, or csv_path')
    if not Path(csv_path).exists():
        raise HTTPException(status_code=400, detail=f'csv_path not found: {csv_path}')
    try:
        await engine.start_replay(
            csv_path=csv_path,
            speed_multiplier=speed,
            resume=body.resume,
            dataset_id=dataset_id,
            dataset_symbol=(dataset.symbol if dataset else None),
            dataset_timeframe=(dataset.timeframe if dataset else None),
        )
        _invalidate_overview_cache()
        status = engine.replay_status()
        return {
            'ok': True,
            'dataset_id': dataset_id,
            'pointer_index': status['pointer_index'],
            'current_ts': status['current_ts'],
            'speed': status['speed'],
            'dataset_path': csv_path,
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f'Unable to start replay: {exc}') from exc


@router.post('/replay/stop')
async def replay_stop(engine: EngineService = Depends(get_engine_service)) -> dict:
    state = await engine.stop_replay()
    _invalidate_overview_cache()
    return {'ok': True, 'state': state, **state}


@router.post('/replay/pause')
async def replay_pause(engine: EngineService = Depends(get_engine_service)) -> dict:
    state = await engine.pause_replay()
    _invalidate_overview_cache()
    return {'ok': True, 'state': state, **state}


@router.post('/replay/resume')
async def replay_resume(engine: EngineService = Depends(get_engine_service)) -> dict:
    state = await engine.resume_replay()
    _invalidate_overview_cache()
    return {'ok': True, 'state': state, **state}


@router.post('/replay/step')
async def replay_step(engine: EngineService = Depends(get_engine_service)) -> dict:
    state = await engine.step_replay()
    _invalidate_overview_cache()
    return {'ok': True, 'state': state, **state}


@router.post('/replay/reset')
async def replay_reset(engine: EngineService = Depends(get_engine_service)) -> dict:
    state = await engine.reset_replay()
    _invalidate_overview_cache()
    return {'ok': True, 'state': state, **state}


@router.post('/control/close_position')
async def control_close_position(
    body: dict,
    engine: EngineService = Depends(get_engine_service),
) -> dict:
    trade_id = body.get('trade_id')
    if not trade_id:
        raise HTTPException(status_code=400, detail='trade_id is required')
    try:
        return await engine.manual_close_trade(UUID(str(trade_id)))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f'invalid trade_id: {exc}') from exc


@router.post('/control/set_profile')
async def control_set_profile(
    body: dict,
    engine: EngineService = Depends(get_engine_service),
) -> dict:
    profile = body.get('profile')
    if not profile:
        raise HTTPException(status_code=400, detail='profile is required')
    try:
        result = await engine.set_profile(str(profile))
        return {'ok': True, 'state': engine.status(), **result}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get('/replay/status')
async def replay_status(engine: EngineService = Depends(get_engine_service)) -> dict:
    return engine.replay_status()


@router.get('/replay/observability')
async def replay_observability(
    n: int = Query(default=100, ge=1, le=5000),
    engine: EngineService = Depends(get_engine_service),
) -> dict:
    return engine.observability_snapshot(last_n=n)


@router.post('/replay/upload')
async def replay_upload(file: UploadFile = File(...), db: AsyncSession = Depends(get_db)) -> dict:
    dataset_id, stored_path, metadata = await persist_upload(file=file, data_dir=settings.data_dir)
    dataset = ReplayDataset(
        id=UUID(dataset_id),
        filename=file.filename or 'unknown.csv',
        stored_path=stored_path,
        symbol=metadata.get('symbol'),
        timeframe=metadata.get('timeframe'),
        rows_count=metadata['rows_count'],
        start_ts=metadata['start_ts'],
        end_ts=metadata['end_ts'],
    )
    db.add(dataset)
    try:
        await db.commit()
    except (DataError, IntegrityError) as exc:
        await db.rollback()
        Path(stored_path).unlink(missing_ok=True)
        raise HTTPException(status_code=400, detail=f'Unable to save dataset metadata: {exc.__class__.__name__}') from exc
    _invalidate_overview_cache()
    return {
        'dataset_id': dataset_id,
        'filename': dataset.filename,
        'stored_path': dataset.stored_path,
        **metadata,
    }


@router.get('/replay/datasets')
async def replay_datasets(db: AsyncSession = Depends(get_db)) -> list[dict]:
    rows = (await db.execute(select(ReplayDataset).order_by(desc(ReplayDataset.created_at)).limit(500))).scalars()
    return [
        {
            'id': str(row.id),
            'filename': row.filename,
            'stored_path': row.stored_path,
            'symbol': row.symbol,
            'timeframe': row.timeframe,
            'rows_count': row.rows_count,
            'start_ts': row.start_ts,
            'end_ts': row.end_ts,
            'created_at': row.created_at,
        }
        for row in rows
    ]


@router.get('/replay/datasets/{dataset_id}')
async def replay_dataset(dataset_id: str, db: AsyncSession = Depends(get_db)) -> dict:
    row = await db.get(ReplayDataset, dataset_id)
    if not row:
        raise HTTPException(status_code=404, detail='Dataset not found')
    return {
        'id': str(row.id),
        'filename': row.filename,
        'stored_path': row.stored_path,
        'symbol': row.symbol,
        'timeframe': row.timeframe,
        'rows_count': row.rows_count,
        'start_ts': row.start_ts,
        'end_ts': row.end_ts,
        'created_at': row.created_at,
    }


@router.get('/risk/status')
async def risk_status(engine: EngineService = Depends(get_engine_service)) -> dict:
    return engine.risk.risk_status()


@router.get('/positions')
async def list_positions(db: AsyncSession = Depends(get_db)) -> list[dict]:
    rows = (await db.execute(select(Position).where(Position.is_open.is_(True)).order_by(desc(Position.created_at)).limit(200))).scalars()
    return [
        {
            'id': str(row.id),
            'symbol': row.symbol,
            'side': row.side,
            'quantity': float(row.quantity),
            'average_price': float(row.average_price),
            'entry_price': float(row.average_price),
            'unrealized_pnl': float(row.unrealized_pnl),
            'is_open': row.is_open,
            'status': 'OPEN' if row.is_open else 'CLOSED',
            'created_at': row.created_at,
        }
        for row in rows
    ]


@router.get('/trades')
async def list_trades(
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
) -> dict:
    rows = (await db.execute(select(Trade).order_by(desc(Trade.created_at)).limit(limit).offset(offset))).scalars().all()
    trade_ids = [row.id for row in rows]
    entry_payload_by_trade: dict = {}
    if trade_ids:
        exec_rows = (await db.execute(select(Execution).where(Execution.trade_id.in_(trade_ids)).order_by(desc(Execution.created_at)))).scalars().all()
        for exec_row in exec_rows:
            payload = exec_row.payload or {}
            if payload.get('reason') == 'entry' and exec_row.trade_id not in entry_payload_by_trade:
                entry_payload_by_trade[exec_row.trade_id] = payload
    return {
        'limit': limit,
        'offset': offset,
        'items': [
            {
                'id': str(row.id),
                'symbol': row.symbol,
                'side': row.side,
                'quantity': float(row.quantity),
                'entry_price': float(row.entry_price),
                'exit_price': float(row.exit_price) if row.exit_price else None,
                'stop_price': float(row.stop_price) if row.stop_price is not None else None,
                'tp1_price': float(row.tp1_price) if row.tp1_price is not None else None,
                'tp2_price': float(row.tp2_price) if row.tp2_price is not None else None,
                'time_stop_bars': row.time_stop_bars,
                'strategy_name': row.strategy_name,
                'strategy_profile': row.strategy_profile,
                'setup_name': row.setup_name,
                'regime_at_entry': row.regime_at_entry,
                'score_at_entry': float(row.score_at_entry) if row.score_at_entry is not None else None,
                'stop_loss': float(row.stop_price) if row.stop_price is not None else None,
                'take_profit': float(row.tp2_price if row.tp2_price is not None else row.tp1_price) if (row.tp2_price is not None or row.tp1_price is not None) else None,
                'pnl': float(row.pnl) if row.pnl else None,
                'fees': float(row.fees_total) if row.fees_total is not None else 0.0,
                'opened_at': row.opened_at,
                'closed_at': row.closed_at,
                'close_reason': row.close_reason,
                'fee_entry': float(row.fee_entry) if row.fee_entry is not None else None,
                'fee_exit': float(row.fee_exit) if row.fee_exit is not None else None,
                'fees_total': float(row.fees_total) if row.fees_total is not None else None,
                'pnl_gross': float(row.pnl_gross) if row.pnl_gross is not None else None,
                'pnl_net': float(row.pnl_net) if row.pnl_net is not None else None,
                'leverage': float(row.leverage) if row.leverage is not None else None,
                'notional': float(row.notional) if row.notional is not None else None,
                'base_qty': float(row.base_qty) if row.base_qty is not None else None,
                'size_mult': float(row.size_mult) if row.size_mult is not None else None,
                'final_qty': float(row.final_qty) if row.final_qty is not None else None,
                'status': row.status,
                'risk_pct_used': (entry_payload_by_trade.get(row.id) or {}).get('risk_pct_used'),
                'stop_distance': (entry_payload_by_trade.get(row.id) or {}).get('stop_distance'),
                'target_price': (entry_payload_by_trade.get(row.id) or {}).get('target_price'),
                'R_multiple': (entry_payload_by_trade.get(row.id) or {}).get('R_multiple'),
                'created_at': row.created_at,
            }
            for row in rows
        ],
    }


@router.get('/executions')
async def list_executions(
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
) -> dict:
    rows = (await db.execute(select(Execution).order_by(desc(Execution.created_at)).limit(limit).offset(offset))).scalars()
    return {
        'limit': limit,
        'offset': offset,
        'items': [
            {
                'id': str(row.id),
                'symbol': (row.payload or {}).get('symbol'),
                'side': (row.payload or {}).get('side'),
                'qty': (row.payload or {}).get('qty'),
                'price': (row.payload or {}).get('price'),
                'ts': (row.payload or {}).get('ts'),
                'reason': (row.payload or {}).get('reason'),
                'mode': (row.payload or {}).get('mode'),
                'fee': (row.payload or {}).get('fee'),
                'provider': row.provider,
                'status': row.status,
                'payload': row.payload,
                'created_at': row.created_at,
            }
            for row in rows
        ],
    }


@router.post('/signals/test')
async def signal_test(engine: EngineService = Depends(get_engine_service)) -> dict:
    sample_tick = {'symbol': 'BTCUSDT', 'price': 110}
    signal = await engine.strategy.generate_signal(sample_tick)
    return {'signal': signal, 'risk': engine.risk.risk_status()}


@router.post('/telegram/test')
async def telegram_test(db: AsyncSession = Depends(get_db)) -> dict:
    service = TelegramService()
    return await service.send_signal_message(db, 'Signal test fired')


@router.get('/overview')
async def overview(engine: EngineService = Depends(get_engine_service), db: AsyncSession = Depends(get_db)) -> dict:
    now = time.monotonic()
    status = engine.status()
    replay_status = engine.replay_status()
    cache_valid = (
        _OVERVIEW_CACHE.get('snapshot') is not None
        and (now - float(_OVERVIEW_CACHE.get('ts') or 0.0)) < 0.35
        and int(_OVERVIEW_CACHE.get('tick') or -1) == int(status.get('tick') or -1)
    )
    if cache_valid:
        return dict(_OVERVIEW_CACHE['snapshot'])

    async with _OVERVIEW_LOCK:
        now = time.monotonic()
        status = engine.status()
        cache_valid = (
            _OVERVIEW_CACHE.get('snapshot') is not None
            and (now - float(_OVERVIEW_CACHE.get('ts') or 0.0)) < 0.35
            and int(_OVERVIEW_CACHE.get('tick') or -1) == int(status.get('tick') or -1)
        )
        if cache_valid:
            return dict(_OVERVIEW_CACHE['snapshot'])

        t0 = time.perf_counter()
        accounting, accounting_err = await _with_timeout(PortfolioAccounting().snapshot(db), 0.8, {
            'equity_now': settings.equity_start,
            'equity_start': settings.equity_start,
            'realized_pnl_net': 0.0,
            'unrealized_pnl': 0.0,
            'fees_total': 0.0,
            'reconcile_delta': 0.0,
            'reconcile_ok': True,
            'accounting': {'closed_trades': []},
        })
        open_positions_count, open_pos_err = await _with_timeout(
            db.scalar(select(func.count()).select_from(Position).where(Position.is_open.is_(True))), 0.3, 0,
        )

        latest = (
            await db.execute(select(DecisionEvent).order_by(desc(DecisionEvent.created_at)).limit(1))
        ).scalar_one_or_none()
        latest_decision = {
            'ts': None, 'symbol': None, 'decision': None, 'regime': None, 'score': None, 'message': None,
            'blockers_top': None, 'regime_gate_ok': None, 'top_regime_gate_reasons': None,
            'regime_gate_metrics': None, 'active_mode': None, 'mode_reasons': None,
        }
        if latest:
            snapshot = latest.risk_state_snapshot or {}
            latest_decision = {
                'ts': datetime.fromtimestamp((latest.ts or 0) / 1000, tz=timezone.utc).isoformat() if latest.ts else None,
                'symbol': latest.symbol,
                'decision': latest.decision,
                'regime': latest.regime,
                'score': float(latest.signal_score) if latest.signal_score is not None else None,
                'message': latest.rationale,
                'blockers_top': (latest.blockers or [])[:5] if latest.blockers is not None else None,
                'regime_gate_ok': snapshot.get('regime_gate_ok') if isinstance(snapshot, dict) else None,
                'top_regime_gate_reasons': (snapshot.get('regime_gate_reasons') or [])[:2] if isinstance(snapshot, dict) else None,
                'regime_gate_metrics': snapshot.get('regime_gate_metrics') if isinstance(snapshot, dict) else None,
                'active_mode': snapshot.get('active_mode') if isinstance(snapshot, dict) else None,
                'mode_reasons': snapshot.get('mode_reasons') if isinstance(snapshot, dict) else None,
            }

        replay_payload = {'dataset_id': None, 'dataset_path': settings.replay_dataset_default, 'pointer': None, 'candle_ts': None, 'is_running': False, 'speed': None}
        if str(status.get('mode', '')).lower() == 'replay':
            replay_payload = {
                'dataset_id': replay_status.get('dataset_id'),
                'dataset_path': getattr(engine.replay, 'csv_path', None) if engine.replay else settings.replay_dataset_default,
                'pointer': replay_status.get('pointer_index'),
                'candle_ts': replay_status.get('current_ts'),
                'is_running': bool(replay_status.get('running')),
                'speed': replay_status.get('speed'),
            }

        equity_now = float(accounting['equity_now'])
        equity_start = float(accounting['equity_start'])
        risk_state_payload = dict(status['risk'])
        safety_state = status.get('safety') or engine.safety_status()
        governor = safety_state.get('governor_last') or {'eligible': True, 'blockers': [], 'stats': {}, 'config': {}}
        risk_state_payload['status'] = 'ELIGIBLE' if governor.get('eligible', True) else 'BLOCKED'
        risk_state_payload['reason'] = ', '.join([str(b.get('reason')) for b in governor.get('blockers', [])[:3]]) if governor.get('blockers') else None

        payload = {
            'equity': equity_now,
            'daily_dd_pct': status['risk']['daily_drawdown_pct'],
            'global_dd_pct': status['risk']['global_drawdown_pct'],
            'monthly_progress_pct': status['risk']['monthly_progress_pct'],
            'open_positions': int(open_positions_count or 0),
            'risk_state': risk_state_payload,
            'risk_state_reason': risk_state_payload.get('reason'),
            'mode': status['mode'].upper(),
            'leverage': status['risk']['leverage'],
            'replay': replay_payload,
            'equity_start': equity_start,
            'equity_now': equity_now,
            'realized_pnl_net': float(accounting['realized_pnl_net']),
            'unrealized_pnl': float(accounting['unrealized_pnl']),
            'fees_total': float(accounting['fees_total']),
            'reconcile_delta': float(accounting['reconcile_delta']),
            'reconcile_ok': bool(accounting['reconcile_ok']),
            'accounting': accounting['accounting'],
            'dd': {'hwm': equity_now, 'global_dd_pct': status['risk']['global_drawdown_pct'], 'daily_dd_pct': 0.0, 'dd_daily_supported': False},
            'goal': {'target_pct': float(settings.monthly_target_pct), 'progress_pct': status['risk']['monthly_progress_pct'], 'progress_ratio': 0.0},
            'governor': governor,
            'latest_decision': latest_decision,
            'active_profile': status.get('active_profile') or engine.active_profile,
            'safety': safety_state,
            'pre_trade_decision': status.get('pre_trade_decision') or {},
            'day': status.get('day') or {'day_key': None, 'rollover_in_effect': False, 'daily_consecutive_losses': 0, 'daily_realized_pnl': 0.0, 'daily_fees': 0.0, 'daily_trade_count': 0},
            'partial': bool(accounting_err or open_pos_err),
            'partial_reason': '; '.join([e for e in [accounting_err, open_pos_err] if e]) or None,
            'dataset_path': replay_payload.get('dataset_path'),
            'overview_latency_ms': round((time.perf_counter() - t0) * 1000, 2),
        }
        _OVERVIEW_CACHE.update({'snapshot': dict(payload), 'ts': time.monotonic(), 'tick': int(status.get('tick') or -1)})
        return payload


@router.get('/events')
async def list_events(
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
) -> dict:
    rows = (await db.execute(select(DecisionEvent).order_by(desc(DecisionEvent.created_at)).limit(limit).offset(offset))).scalars().all()
    return {
        'limit': limit,
        'offset': offset,
        'items': [
            {
                'id': str(row.id),
                'ts': row.ts,
                'symbol': row.symbol,
                'regime': row.regime,
                'signal_score': float(row.signal_score),
                'decision': row.decision,
                'blockers': row.blockers or [],
                'rationale': row.rationale,
                'event_type': (row.risk_state_snapshot or {}).get('event_type', row.decision),
                'active_profile': (row.risk_state_snapshot or {}).get('active_profile'),
                'active_mode': (row.risk_state_snapshot or {}).get('active_mode'),
                'primary_blocker': (row.risk_state_snapshot or {}).get('primary_blocker') or (row.risk_state_snapshot or {}).get('trade_blocker_primary'),
                'risk_state_snapshot': row.risk_state_snapshot or {},
                'created_at': row.created_at,
            }
            for row in rows
        ],
    }


@router.get('/events/summary')
async def events_summary(last_n: int = Query(default=200, ge=1, le=2000), db: AsyncSession = Depends(get_db)) -> dict:
    rows = (await db.execute(select(DecisionEvent).order_by(desc(DecisionEvent.created_at)).limit(last_n))).scalars().all()
    blocker_counts: Counter[str] = Counter()
    decision_counts: Counter[str] = Counter()
    for row in rows:
        decision_counts[row.decision] += 1
        for blocker in row.blockers or []:
            blocker_counts[blocker] += 1
    return {
        'last_n': last_n,
        'decisions': dict(decision_counts),
        'blockers': dict(blocker_counts),
    }


@router.get('/api/safety/status')
async def safety_status(engine: EngineService = Depends(get_engine_service)) -> dict:
    return engine.safety_status()


@router.post('/api/safety/arm')
async def safety_arm(body: dict, engine: EngineService = Depends(get_engine_service)) -> dict:
    mode = str(body.get('mode') or 'off').lower()
    if mode not in {'off', 'soft', 'hard'}:
        raise HTTPException(status_code=400, detail='mode must be one of off|soft|hard')
    return engine.safety_arm(mode)


@router.post('/api/safety/trip')
async def safety_trip(body: dict, engine: EngineService = Depends(get_engine_service)) -> dict:
    mode = str(body.get('mode') or 'soft').lower()
    if mode not in {'soft', 'hard'}:
        raise HTTPException(status_code=400, detail='mode must be soft|hard for trip')
    reason = str(body.get('reason') or 'manual_trip')
    evidence = body.get('evidence') if isinstance(body.get('evidence'), dict) else {}
    return await engine.safety_trip(mode=mode, reason=reason, evidence=evidence)


@router.get('/api/safety/incidents')
async def safety_incidents(engine: EngineService = Depends(get_engine_service)) -> dict:
    return {'items': engine.safety_incidents()}


@router.get('/api/safety/incidents/{incident_id}')
async def safety_incident(incident_id: str, engine: EngineService = Depends(get_engine_service)) -> dict:
    incident = engine.safety_incident(incident_id)
    if not incident:
        raise HTTPException(status_code=404, detail='incident not found')
    return incident


@router.post('/api/safety/incidents/{incident_id}/export')
async def safety_incident_export(incident_id: str, engine: EngineService = Depends(get_engine_service)) -> JSONResponse:
    incident = engine.safety_incident(incident_id)
    if not incident:
        raise HTTPException(status_code=404, detail='incident not found')
    headers = {'Content-Disposition': f'attachment; filename="incident-{incident_id}.json"'}
    return JSONResponse(content=incident, headers=headers)
