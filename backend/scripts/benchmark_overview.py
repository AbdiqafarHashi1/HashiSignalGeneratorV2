import statistics
import time
import requests

URL='http://localhost:8000/overview'
samples=[]
for _ in range(200):
    t0=time.perf_counter()
    r=requests.get(URL,timeout=5)
    r.raise_for_status()
    samples.append((time.perf_counter()-t0)*1000)
median=statistics.median(samples)
print({'count':len(samples),'median_ms':round(median,2),'p95_ms':round(statistics.quantiles(samples,n=20)[18],2)})
if median>=50:
    raise SystemExit(f'median too high: {median:.2f}ms')
