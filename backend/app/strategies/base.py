from abc import ABC, abstractmethod


class StrategyBase(ABC):
    @abstractmethod
    async def score_signal(self, tick: dict) -> float:
        raise NotImplementedError

    @abstractmethod
    async def generate_signal(self, tick: dict) -> dict | None:
        raise NotImplementedError


class MomentumStrategy(StrategyBase):
    async def score_signal(self, tick: dict) -> float:
        return min(max((tick.get('price', 0) - 100) / 10, -1), 1)

    async def generate_signal(self, tick: dict) -> dict | None:
        score = await self.score_signal(tick)
        if abs(score) < 0.1:
            return None
        return {
            'symbol': tick.get('symbol', 'BTCUSDT'),
            'side': 'BUY' if score > 0 else 'SELL',
            'confidence': abs(score),
            'price': tick.get('price', 0),
        }
