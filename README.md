# EtcdLockMaster

A distributed locking library in Python built on **etcd**. Only one process across any number of machines can hold a lock at a time — with automatic crash cleanup, optional auto-renewal, and re-entrancy support.

---

## Prerequisites

- Python 3.9+
- etcd v3.5+ — Windows binaries included in `etcd/`, or use Docker
- `protobuf` must stay at `3.x` — etcd3 client is incompatible with 4.x

---

## How to Run

### Step 1 — Install dependencies

```cmd
python -m venv venv
venv\Scripts\pip install -r requirements.txt
```

### Step 2 — Start etcd

Open a **dedicated terminal** and keep it running.

```cmd
etcd\etcd-v3.5.10-windows-amd64\etcd.exe --name standalone --data-dir etcd-data/node1 --listen-client-urls http://localhost:2381 --advertise-client-urls http://localhost:2381 --listen-peer-urls http://localhost:2382 --initial-advertise-peer-urls http://localhost:2382 --initial-cluster standalone=http://localhost:2382
```

**Docker — 3-node cluster (for all 12 tests):**
```bash
docker compose up -d
```

> Ready when you see: `"msg":"serving client traffic insecurely","address":"127.0.0.1:2381"`

### Step 3 — Activate virtual environment

```cmd
venv\Scripts\activate.bat
```

### Step 4 — Run the demo

```cmd
python distributed_lock.py
```

Runs 4 built-in scenarios: basic lock, re-entrancy, auto-renew, `is_locked` check.

### Step 5 — See it live (2 terminals)

Activate venv in two terminals, then:

**Terminal 1:**
```cmd
python worker.py A
```
**Terminal 2:**
```cmd
python worker.py B
```

B waits while A holds the lock, then acquires it after A releases.

### Step 6 — Run tests

```cmd
pytest -v
```

- Single node: **11 passed, 1 skipped**
- Docker 3-node cluster: **12 passed**

---

## API

```python
from distributed_lock import DistributedLock

try:
    with DistributedLock("my-resource", ttl=30, timeout=10, auto_renew=True) as lock:
        print(lock.owner)       # UUID of this holder
        print(lock.is_locked)   # True if lock still held in etcd
except TimeoutError:
    print("Could not acquire lock in time")
```

| Parameter | Default | Description |
|---|---|---|
| `resource` | — | Lock name (any string) |
| `ttl` | `30` | Lease lifetime in seconds |
| `timeout` | `None` | Max wait time — `None` = wait forever |
| `auto_renew` | `False` | Refresh lease in background thread |

---

## Troubleshooting

| Error | Fix |
|---|---|
| `'.' is not recognized` | Use CMD syntax — no `.\`, no backtick line breaks |
| `bind: Only one usage of each socket address` | Port in use — change `--listen-peer-urls` to a free port (e.g. `2389`) |
| `db file is flocked by another process` | Kill old etcd, run `rmdir /s /q etcd-data`, restart |
| `server has been already initialized` | Run `rmdir /s /q etcd-data`, restart |
| `grpc._channel._InactiveRpcError` | etcd not running — check Step 2 terminal |
| `etcd connection failed` / `request timed out` | etcd stuck or not running — do the full reset below |
| 1 test skipped | Expected on single node — run `docker compose up -d` for all 12 |

### etcd stuck / full reset

If etcd is running but not responding (`request timed out`), do this:

**1. Kill existing etcd process:**
```cmd
taskkill /F /IM etcd.exe
```

**2. Delete stale data:**
```cmd
rmdir /s /q etcd-data
```

**3. Start fresh (keep this terminal open):**
```cmd
etcd\etcd-v3.5.10-windows-amd64\etcd.exe --name standalone --data-dir etcd-data/node1 --listen-client-urls http://localhost:2381 --advertise-client-urls http://localhost:2381 --listen-peer-urls http://localhost:2382 --initial-advertise-peer-urls http://localhost:2382 --initial-cluster standalone=http://localhost:2382
```

**4. Wait for:**
```
"msg":"serving client traffic insecurely","address":"127.0.0.1:2381"
```

**5. Run workers in separate terminals:**
```cmd
venv\Scripts\activate.bat
python worker.py A
```
```cmd
venv\Scripts\activate.bat
python worker.py B
```

---

## Files

```
distributed_lock.py             # the library
worker.py                       # 2-terminal live demo
tests/test_distributed_lock.py  # 12 tests
docker-compose.yml              # 3-node etcd cluster via Docker
requirements.txt
EtcdLockMaster.txt              # Explanation of everything
```
