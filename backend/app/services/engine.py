import asyncio
from datetime import datetime, timezone

from redis.asyncio import Redis

from app.config import settings
from app.execution.providers import BybitExecution
from app.providers.market_data import BybitProvider
from app.replay.replay_engine import ReplayEngine
from app.risk.manager import RiskManager
from app.strategies.base import MomentumStrategy


class EngineService:
    def __init__(self, redis_client: Redis):
        self.redis = redis_client
        self.running = False
        self.mode = settings.engine_mode
        self.tick = 0
        self.task: asyncio.Task | None = None
        self.last_event_at: datetime | None = None
        self.market_provider = BybitProvider()
        self.execution_provider = BybitExecution()
        self.strategy = MomentumStrategy()
        self.risk = RiskManager(leverage=settings.leverage)
        self.replay: ReplayEngine | None = None

    async def start(self, mode: str = 'live') -> dict:
        if self.running:
            return self.status()
        self.mode = mode
        self.running = True
        self.task = asyncio.create_task(self._loop())
        return self.status()

    async def stop(self) -> dict:
        self.running = False
        if self.replay:
            await self.replay.stop()
        if self.task:
            await asyncio.wait([self.task], timeout=1)
        return self.status()

    async def start_replay(self, csv_path: str, speed_multiplier: float = 1.0, resume: bool = False) -> dict:
        cursor = self.replay.cursor if (self.replay and resume) else 0
        self.replay = ReplayEngine(csv_path=csv_path, speed_multiplier=speed_multiplier, resume_cursor=cursor)
        await self.replay.start()
        return await self.start(mode='replay')

    async def stop_replay(self) -> dict:
        if self.replay:
            await self.replay.stop()
        return await self.stop()

    async def _loop(self) -> None:
        while self.running:
            tick = await self._next_tick()
            self.tick += 1
            self.last_event_at = datetime.now(timezone.utc)
            if tick and self.risk.can_trade():
                signal = await self.strategy.generate_signal(tick)
                if signal:
                    await self.execution_provider.execute(signal)
            await self._publish_state()
            await asyncio.sleep(0.5)

    async def _next_tick(self) -> dict | None:
        if self.mode == 'replay' and self.replay:
            return await self.replay.next_tick()
        return await self.market_provider.get_tick('BTCUSDT')

    async def _publish_state(self) -> None:
        await self.redis.set('engine:state', str(self.status()))

    def status(self) -> dict:
        payload = {
            'running': self.running,
            'mode': self.mode,
            'tick': self.tick,
            'last_event_at': self.last_event_at,
            'risk': self.risk.risk_status(),
        }
        if self.replay:
            payload['replay'] = self.replay.status()
        return payload
