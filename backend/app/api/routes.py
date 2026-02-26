from fastapi import APIRouter, Depends, Query
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.dependencies import get_engine_service
from app.models.entities import Execution, Position, Trade
from app.schemas.common import ReplayStartRequest
from app.services.engine import EngineService
from app.telegram.service import TelegramService

router = APIRouter()


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
async def replay_start(body: ReplayStartRequest, engine: EngineService = Depends(get_engine_service)) -> dict:
    return await engine.start_replay(csv_path=body.csv_path, speed_multiplier=body.speed_multiplier, resume=body.resume)


@router.post('/replay/stop')
async def replay_stop(engine: EngineService = Depends(get_engine_service)) -> dict:
    return await engine.stop_replay()


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
            'unrealized_pnl': float(row.unrealized_pnl),
            'is_open': row.is_open,
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
    rows = (await db.execute(select(Trade).order_by(desc(Trade.created_at)).limit(limit).offset(offset))).scalars()
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
                'pnl': float(row.pnl) if row.pnl else None,
                'status': row.status,
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
    open_positions = (await db.execute(select(Position).where(Position.is_open.is_(True)))).scalars().all()
    status = engine.status()
    return {
        'equity': 100000,
        'daily_dd_pct': status['risk']['daily_drawdown_pct'],
        'global_dd_pct': status['risk']['global_drawdown_pct'],
        'monthly_progress_pct': status['risk']['monthly_progress_pct'],
        'open_positions': len(open_positions),
        'risk_state': status['risk'],
        'mode': status['mode'].upper(),
        'leverage': status['risk']['leverage'],
    }
