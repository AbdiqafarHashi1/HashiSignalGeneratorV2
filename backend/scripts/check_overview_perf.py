import asyncio
import statistics
import time

import httpx


async def main() -> None:
    latencies = []
    async with httpx.AsyncClient(timeout=5.0) as client:
        for _ in range(200):
            t0 = time.perf_counter()
            resp = await client.get('http://localhost:8000/overview')
            resp.raise_for_status()
            latencies.append((time.perf_counter() - t0) * 1000)
    median = statistics.median(latencies)
    print(f'median_ms={median:.2f}')
    if median >= 50:
        raise SystemExit(1)


if __name__ == '__main__':
    asyncio.run(main())
