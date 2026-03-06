from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from app.api.routes import router
from app.config import settings
from app.db.base import Base
from app.db.session import engine
from app.services.dataset_resolver import DatasetResolver
from app.models import entities  # noqa: F401


async def _upgrade_bigint_columns(conn: AsyncConnection) -> None:
    if conn.dialect.name != 'postgresql':
        return

    targets = [
        ('replay_datasets', 'rows_count'),
        ('replay_datasets', 'start_ts'),
        ('replay_datasets', 'end_ts'),
        ('decision_events', 'ts'),
    ]
    for table_name, column_name in targets:
        result = await conn.execute(
            text(
                """
                SELECT data_type
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = :table_name
                  AND column_name = :column_name
                """
            ),
            {'table_name': table_name, 'column_name': column_name},
        )
        data_type = result.scalar_one_or_none()
        if data_type and data_type != 'bigint':
            await conn.execute(text(f'ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE BIGINT'))


async def _upgrade_trade_columns(conn: AsyncConnection) -> None:
    if conn.dialect.name != 'postgresql':
        return
    await conn.execute(text('ALTER TABLE trades ADD COLUMN IF NOT EXISTS opened_at TIMESTAMPTZ'))
    await conn.execute(text('ALTER TABLE trades ADD COLUMN IF NOT EXISTS closed_at TIMESTAMPTZ'))
    await conn.execute(text('ALTER TABLE trades ADD COLUMN IF NOT EXISTS close_reason VARCHAR(32)'))
    await conn.execute(text('ALTER TABLE trades ADD COLUMN IF NOT EXISTS fee_entry NUMERIC(20, 8)'))
    await conn.execute(text('ALTER TABLE trades ADD COLUMN IF NOT EXISTS fee_exit NUMERIC(20, 8)'))
    await conn.execute(text('ALTER TABLE trades ADD COLUMN IF NOT EXISTS fees_total NUMERIC(20, 8)'))
    await conn.execute(text('ALTER TABLE trades ADD COLUMN IF NOT EXISTS pnl_gross NUMERIC(20, 8)'))
    await conn.execute(text('ALTER TABLE trades ADD COLUMN IF NOT EXISTS pnl_net NUMERIC(20, 8)'))
    await conn.execute(text('ALTER TABLE trades ADD COLUMN IF NOT EXISTS leverage NUMERIC(12, 4)'))
    await conn.execute(text('ALTER TABLE trades ADD COLUMN IF NOT EXISTS notional NUMERIC(20, 8)'))
    await conn.execute(text('ALTER TABLE trades ADD COLUMN IF NOT EXISTS stop_price NUMERIC(20, 8)'))
    await conn.execute(text('ALTER TABLE trades ADD COLUMN IF NOT EXISTS tp1_price NUMERIC(20, 8)'))
    await conn.execute(text('ALTER TABLE trades ADD COLUMN IF NOT EXISTS tp2_price NUMERIC(20, 8)'))
    await conn.execute(text('ALTER TABLE trades ADD COLUMN IF NOT EXISTS tp1_be_armed BOOLEAN DEFAULT FALSE'))
    await conn.execute(text('ALTER TABLE trades ADD COLUMN IF NOT EXISTS time_stop_bars INTEGER'))
    await conn.execute(text('ALTER TABLE trades ADD COLUMN IF NOT EXISTS strategy_name VARCHAR(64)'))
    await conn.execute(text('ALTER TABLE trades ADD COLUMN IF NOT EXISTS setup_name VARCHAR(64)'))
    await conn.execute(text('ALTER TABLE trades ADD COLUMN IF NOT EXISTS regime_at_entry VARCHAR(32)'))
    await conn.execute(text('ALTER TABLE trades ADD COLUMN IF NOT EXISTS score_at_entry NUMERIC(12, 6)'))
    await conn.execute(text('ALTER TABLE trades ADD COLUMN IF NOT EXISTS base_qty NUMERIC(20, 8)'))
    await conn.execute(text('ALTER TABLE trades ADD COLUMN IF NOT EXISTS size_mult NUMERIC(12, 6)'))
    await conn.execute(text('ALTER TABLE trades ADD COLUMN IF NOT EXISTS final_qty NUMERIC(20, 8)'))


@asynccontextmanager
async def lifespan(_: FastAPI):
    dataset_resolver = DatasetResolver()
    (Path(settings.data_dir) / 'uploads').mkdir(parents=True, exist_ok=True)
    raw_default = settings.replay_dataset_default
    resolved_default = None
    exists = False
    try:
        resolved_default = dataset_resolver.resolve_default().resolved_path
        exists = Path(resolved_default).exists()
    except Exception:
        resolved_default = None
        exists = False
    print(f"[startup] REPLAY_DATASET_DEFAULT raw={raw_default!r} resolved={resolved_default!r} exists={exists}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await _upgrade_bigint_columns(conn)
        await _upgrade_trade_columns(conn)
    yield


def _parse_cors_origins(raw: str) -> list[str]:
    return [origin.strip() for origin in raw.split(',') if origin.strip()]


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=_parse_cors_origins(settings.cors_origins),
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)
app.include_router(router)


@app.get('/health')
async def health() -> dict:
    return {'status': 'ok'}
