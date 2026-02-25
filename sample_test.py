import requests
import json
import time

API_KEY = "ds_d14ca21f7557aceeb53115930df70b62"
BASE_URL = "http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com/api/v1"
headers = {"X-API-Key": API_KEY}

results = {}

# 1. Basic events - time it
start = time.time()
r = requests.get(f"{BASE_URL}/events", headers=headers)
elapsed = time.time() - start
results["events_headers"] = dict(r.headers)
results["events_body"] = r.json()
results["events_latency_ms"] = round(elapsed * 1000)
print(f"Basic events: {elapsed*1000:.0f}ms | status={r.status_code}")

# 2. Events with limit=1000
start = time.time()
r2 = requests.get(f"{BASE_URL}/events?limit=1000", headers=headers)
elapsed = time.time() - start
results["events_limit1000_headers"] = dict(r2.headers)
results["events_limit1000"] = r2.json()
results["events_limit1000_latency_ms"] = round(elapsed * 1000)
print(f"Events limit=1000: {elapsed*1000:.0f}ms | status={r2.status_code}")

# 3. Events with limit=5000
start = time.time()
r2b = requests.get(f"{BASE_URL}/events?limit=5000", headers=headers)
elapsed = time.time() - start
results["events_limit5000_headers"] = dict(r2b.headers)
results["events_limit5000"] = r2b.json()
results["events_limit5000_latency_ms"] = round(elapsed * 1000)
print(f"Events limit=5000: {elapsed*1000:.0f}ms | status={r2b.status_code}")

# 4. Try cursor-based pagination (get cursor from first response)
first_body = results["events_body"]
print(f"\nFirst response keys: {list(first_body.keys()) if isinstance(first_body, dict) else type(first_body)}")
if isinstance(first_body, dict):
    for k, v in first_body.items():
        print(f"  {k}: {str(v)[:200]}")

# 5. Try cursor if present
cursor = None
if isinstance(first_body, dict):
    cursor = first_body.get("cursor") or first_body.get("next_cursor") or first_body.get("nextCursor")
    pagination = first_body.get("pagination", {})
    if pagination:
        cursor = cursor or pagination.get("cursor") or pagination.get("next")

if cursor:
    start = time.time()
    r_cursor = requests.get(f"{BASE_URL}/events?cursor={cursor}&limit=1000", headers=headers)
    elapsed = time.time() - start
    results["cursor_page2"] = r_cursor.json()
    results["cursor_page2_latency_ms"] = round(elapsed * 1000)
    print(f"\nCursor page2: {elapsed*1000:.0f}ms | status={r_cursor.status_code}")

# 6. Try export endpoint
start = time.time()
r3 = requests.get(f"{BASE_URL}/events/export", headers=headers)
elapsed = time.time() - start
results["export_status"] = r3.status_code
results["export_body"] = r3.text[:1000]
results["export_headers"] = dict(r3.headers)
print(f"Export: {elapsed*1000:.0f}ms | status={r3.status_code}")

# 7. Try bulk
r4 = requests.get(f"{BASE_URL}/events/bulk", headers=headers)
results["bulk_status"] = r4.status_code
results["bulk_body"] = r4.text[:500]

# 8. Metrics
r5 = requests.get(f"{BASE_URL}/metrics", headers=headers)
results["metrics"] = r5.json()
print(f"Metrics: {json.dumps(r5.json())[:300]}")

# 9. Sessions
r6 = requests.get(f"{BASE_URL}/sessions", headers=headers)
results["sessions_headers"] = dict(r6.headers)
results["sessions"] = r6.json()
print(f"Sessions: {json.dumps(r6.json())[:300]}")

# 10. Check rate limit headers
print("\n=== RATE LIMIT HEADERS ===")
for k, v in dict(r.headers).items():
    if any(x in k.lower() for x in ["rate", "limit", "retry", "remaining", "reset", "x-"]):
        print(f"  {k}: {v}")

# 11. Try parallel requests to test rate limit
print("\n=== TESTING PARALLEL/BURST ===")
import concurrent.futures
def fetch(i):
    start = time.time()
    resp = requests.get(f"{BASE_URL}/events?limit=100", headers=headers)
    return i, resp.status_code, round((time.time()-start)*1000)

with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
    futs = [ex.submit(fetch, i) for i in range(10)]
    for f in concurrent.futures.as_completed(futs):
        i, status, ms = f.result()
        print(f"  req {i}: status={status} {ms}ms")

with open("api_discovery.json", "w") as f:
    json.dump(results, f, indent=2, default=str)

print("\nDone! Saved to api_discovery.json")