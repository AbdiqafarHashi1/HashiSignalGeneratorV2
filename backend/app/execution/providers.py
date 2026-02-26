from abc import ABC, abstractmethod


class ExecutionProvider(ABC):
    name: str = 'abstract'

    @abstractmethod
    async def execute(self, signal: dict) -> dict:
        raise NotImplementedError


class BybitExecution(ExecutionProvider):
    name = 'bybit'

    async def execute(self, signal: dict) -> dict:
        return {'provider': self.name, 'status': 'FILLED', 'signal': signal}


class OandaExecution(ExecutionProvider):
    name = 'oanda'

    async def execute(self, signal: dict) -> dict:
        return {'provider': self.name, 'status': 'SIMULATED', 'signal': signal}
