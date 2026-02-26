from app.providers.csv_replay import CSVReplayProvider


class ReplayEngine:
    def __init__(self, csv_path: str, speed_multiplier: float = 1.0, resume_cursor: int = 0):
        self.provider = CSVReplayProvider(csv_path)
        self.speed_multiplier = speed_multiplier
        self.cursor = resume_cursor
        self.clock_index = resume_cursor
        self.rows = self.provider.load_rows()
        self.running = False

    async def start(self) -> None:
        self.running = True

    async def stop(self) -> None:
        self.running = False

    async def next_tick(self) -> dict | None:
        if not self.running or self.clock_index >= len(self.rows):
            return None
        row = self.provider.normalize_row(self.rows[self.clock_index])
        tick = {
            'replay_clock': self.clock_index,
            'speed_multiplier': self.speed_multiplier,
            **row,
        }
        self.clock_index += 1
        self.cursor = self.clock_index
        return tick

    def status(self) -> dict:
        return {
            'running': self.running,
            'cursor': self.cursor,
            'speed_multiplier': self.speed_multiplier,
            'total_rows': len(self.rows),
        }
