from app.providers.csv_replay import CSVReplayProvider


class ReplayEngine:
    def __init__(
        self,
        csv_path: str,
        speed_multiplier: float = 1.0,
        resume_cursor: int = 0,
        dataset_symbol: str | None = None,
        dataset_timeframe: str | None = None,
    ):
        self.provider = CSVReplayProvider(csv_path)
        self.speed_multiplier = speed_multiplier
        self.cursor = resume_cursor
        self.clock_index = resume_cursor
        self.dataset_symbol = dataset_symbol
        self.dataset_timeframe = dataset_timeframe
        self.rows = self.provider.load_rows()
        self.running = False
        self.paused = False
        self.last_error: str | None = None

    async def start(self) -> None:
        self.running = True

    async def stop(self) -> None:
        self.running = False
        self.paused = False

    async def pause(self) -> None:
        self.paused = True

    async def resume(self) -> None:
        self.paused = False

    async def step(self) -> dict | None:
        if self.clock_index >= len(self.rows):
            return None
        try:
            row = self.provider.normalize_row(self.rows[self.clock_index], default_symbol=self.dataset_symbol)
            tick = {
                'replay_clock': self.clock_index,
                'speed_multiplier': self.speed_multiplier,
                'timeframe': self.dataset_timeframe,
                **row,
            }
            self.clock_index += 1
            self.cursor = self.clock_index
            self.last_error = None
            return tick
        except Exception as exc:
            self.last_error = str(exc)
            return None

    async def next_tick(self) -> dict | None:
        if not self.running or self.paused:
            return None
        if self.clock_index >= len(self.rows):
            return None
        return await self.step()

    def status(self) -> dict:
        return {
            'running': self.running,
            'paused': self.paused,
            'pointer_index': self.cursor,
            'current_ts': self.provider.normalize_row(self.rows[self.cursor - 1], default_symbol=self.dataset_symbol).get('timestamp') if self.cursor > 0 and self.cursor <= len(self.rows) else None,
            'speed': self.speed_multiplier,
            'total_rows': len(self.rows),
            'last_error': self.last_error,
            'symbol': self.dataset_symbol,
            'timeframe': self.dataset_timeframe,
        }
