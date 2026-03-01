from __future__ import annotations

import csv
from pathlib import Path
import subprocess
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[2]
BUILD_SCRIPT = PROJECT_ROOT / "backend" / "scripts" / "build_15m_from_5m.py"
OUTPUT_CSV = PROJECT_ROOT / "data" / "datasets" / "ETHUSDT_15m.csv"


def main() -> int:
    result = subprocess.run([sys.executable, str(BUILD_SCRIPT)], cwd=str(PROJECT_ROOT), check=False)
    if result.returncode != 0:
        print("ERROR: build_15m_from_5m failed")
        return result.returncode

    if not OUTPUT_CSV.exists():
        print(f"ERROR: expected output missing: {OUTPUT_CSV}")
        return 1

    with OUTPUT_CSV.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
    if len(rows) < 2:
        print("ERROR: output dataset too small")
        return 1

    first_ts = int(float(rows[0]["timestamp"]))
    last_ts = int(float(rows[-1]["timestamp"]))
    if first_ts >= last_ts:
        print("ERROR: timestamps are not increasing")
        return 1

    print(
        f"smoke_build_dataset OK: rows={len(rows)} first_ts={first_ts} "
        f"last_ts={last_ts} output={OUTPUT_CSV}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
