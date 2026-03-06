from pathlib import Path

import pytest

from app.services.dataset_resolver import DatasetResolutionError, DatasetResolver


def test_resolve_absolute_path(tmp_path: Path):
    datasets_dir = tmp_path / 'app' / 'data' / 'datasets'
    datasets_dir.mkdir(parents=True)
    file_path = datasets_dir / 'ETHUSDT_15m.csv'
    file_path.write_text('timestamp,open,high,low,close,volume\n1,1,1,1,1,1\n')

    resolver = DatasetResolver(app_root=tmp_path / 'app')
    out = resolver.resolve(str(file_path))
    assert out.resolved_path == str(file_path.resolve())


def test_resolve_relative_path(tmp_path: Path):
    app_root = tmp_path / 'app'
    file_path = app_root / 'data' / 'datasets' / 'ETHUSDT_15m.csv'
    file_path.parent.mkdir(parents=True)
    file_path.write_text('x')
    resolver = DatasetResolver(app_root=app_root)

    out = resolver.resolve('data/datasets/ETHUSDT_15m.csv')
    assert out.resolved_path == str(file_path.resolve())


def test_resolve_filename_only(tmp_path: Path):
    app_root = tmp_path / 'app'
    file_path = app_root / 'data' / 'datasets' / 'ETHUSDT_15m.csv'
    file_path.parent.mkdir(parents=True)
    file_path.write_text('x')
    resolver = DatasetResolver(app_root=app_root)

    out = resolver.resolve('ETHUSDT_15m.csv')
    assert out.resolved_path == str(file_path.resolve())


def test_resolve_omitted_uses_default(tmp_path: Path):
    app_root = tmp_path / 'app'
    file_path = app_root / 'data' / 'datasets' / 'ETHUSDT_15m.csv'
    file_path.parent.mkdir(parents=True)
    file_path.write_text('x')
    resolver = DatasetResolver(app_root=app_root)

    out = resolver.resolve(None, fallback_default='data/datasets/ETHUSDT_15m.csv')
    assert out.resolved_path == str(file_path.resolve())


def test_resolve_missing_returns_attempted_paths(tmp_path: Path):
    resolver = DatasetResolver(app_root=tmp_path / 'app')

    with pytest.raises(DatasetResolutionError) as exc:
        resolver.resolve('missing.csv')

    assert exc.value.attempted_paths
    assert any('missing.csv' in candidate for candidate in exc.value.attempted_paths)
