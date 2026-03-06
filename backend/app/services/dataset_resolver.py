from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from app.config import settings


@dataclass
class DatasetResolution:
    resolved_path: str
    display_value: str
    attempted_paths: list[str]
    raw_value: str | None


class DatasetResolutionError(ValueError):
    def __init__(self, raw_value: str | None, attempted_paths: list[str]):
        self.raw_value = raw_value
        self.attempted_paths = attempted_paths
        label = raw_value if raw_value else '<default>'
        super().__init__(f"Dataset not found for '{label}'. Attempted: {', '.join(attempted_paths)}")


class DatasetResolver:
    def __init__(self, app_root: Path | None = None) -> None:
        self.app_root = app_root or Path('/app')
        self.cwd_root = Path.cwd()

    def resolve(self, dataset_value: str | None, fallback_default: str | None = None) -> DatasetResolution:
        raw = (dataset_value or '').strip() or None
        if not raw:
            raw = (fallback_default or '').strip() or None
        if not raw:
            raise DatasetResolutionError(raw_value=dataset_value, attempted_paths=[])

        attempted: list[str] = []
        for candidate in self._candidates(raw):
            normalized = str(candidate)
            if normalized not in attempted:
                attempted.append(normalized)
            if candidate.exists() and candidate.is_file():
                return DatasetResolution(
                    resolved_path=str(candidate.resolve()),
                    display_value=self._display_value(raw, candidate),
                    attempted_paths=attempted,
                    raw_value=dataset_value,
                )
        raise DatasetResolutionError(raw_value=raw, attempted_paths=attempted)

    def resolve_default(self) -> DatasetResolution:
        return self.resolve(dataset_value=None, fallback_default=settings.replay_dataset_default)

    def _candidates(self, value: str) -> list[Path]:
        normalized = value.replace('\\', '/').strip()
        path = Path(normalized)
        candidates: list[Path] = []

        if path.is_absolute():
            candidates.append(path)
            if not normalized.startswith(str(self.app_root)):
                candidates.append(self.app_root / normalized.lstrip('/'))
            return candidates

        has_dir = '/' in normalized
        if has_dir:
            candidates.append(self.app_root / normalized)
            candidates.append(self.cwd_root / normalized)
        else:
            candidates.append(self.app_root / 'data' / 'datasets' / normalized)
            candidates.append(self.app_root / 'data' / 'uploads' / normalized)
            candidates.append(self.cwd_root / 'data' / 'datasets' / normalized)
            candidates.append(self.cwd_root / 'data' / 'uploads' / normalized)
            candidates.append(self.cwd_root / normalized)

        if normalized.startswith('data/'):
            candidates.append(self.app_root / normalized)
            candidates.append(self.cwd_root / normalized)

        return candidates

    def _display_value(self, raw: str, resolved: Path) -> str:
        raw_norm = raw.replace('\\', '/').strip()
        if '/' not in raw_norm:
            return raw_norm
        try:
            rel = resolved.resolve().relative_to((self.app_root / 'data' / 'datasets').resolve())
            return rel.name
        except Exception:
            return raw_norm
