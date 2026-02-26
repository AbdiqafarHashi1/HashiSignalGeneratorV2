from abc import ABC, abstractmethod
from datetime import datetime, timezone


class MarketDataProvider(ABC):
    name: str = 'abstract'

    @abstractmethod
    async def get_tick(self, symbol: str) -> dict:
        raise NotImplementedError


class BybitProvider(MarketDataProvider):
    name = 'bybit'

    async def get_tick(self, symbol: str) -> dict:
        return {'provider': self.name, 'symbol': symbol, 'price': 100.0, 'timestamp': datetime.now(timezone.utc).isoformat()}


class BinanceFallbackProvider(MarketDataProvider):
    name = 'binance'

    async def get_tick(self, symbol: str) -> dict:
        return {'provider': self.name, 'symbol': symbol, 'price': 100.2, 'timestamp': datetime.now(timezone.utc).isoformat()}


class OKXFallbackProvider(MarketDataProvider):
    name = 'okx'

    async def get_tick(self, symbol: str) -> dict:
        return {'provider': self.name, 'symbol': symbol, 'price': 99.9, 'timestamp': datetime.now(timezone.utc).isoformat()}


class OandaProvider(MarketDataProvider):
    name = 'oanda'

    async def get_tick(self, symbol: str) -> dict:
        return {'provider': self.name, 'symbol': symbol, 'price': 1.0, 'timestamp': datetime.now(timezone.utc).isoformat()}
