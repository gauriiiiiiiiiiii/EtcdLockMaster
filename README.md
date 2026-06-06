# EtcdLockMaster

A distributed locking library in Python built on **etcd**. Guarantees only one process across any number of machines holds a lock at a time — with automatic crash cleanup, optional auto-renewal, and re-entrancy support.

---

## Prerequisites

- Python 3.9+
- etcd v3.5+ (binaries included in `etcd/` for Windows)
- `protobuf` must stay at `3.x` — etcd3 client is incompatible with 4.x

---

## How to Run

### Step 1 — Create virtual environment and install dependencies

```cmd
python -m venv venv
venv\Scripts\pip install -r requirements.txt
```

---

### Step 2 — Start etcd

Open a **dedicated terminal** and keep it running the entire time.

**CMD:**
```cmd
etcd\etcd-v3.5.10-windows-amd64\etcd.exe --name standalone --data-dir etcd-data/node1 --listen-client-urls http://localhost:2381 --advertise-client-urls http://localhost:2381 --listen-peer-urls http://localhost:2382 --initial-advertise-peer-urls http://localhost:2382 --initial-cluster standalone=http://localhost:2382
```

**PowerShell:**
```powershell
.\etcd\etcd-v3.5.10-windows-amd64\etcd.exe `
  --name standalone --data-dir etcd-data/node1 `
  --listen-client-urls http://localhost:2381 `
  --advertise-client-urls http://localhost:2381 `
  --listen-peer-urls http://localhost:2382 `
  --initial-advertise-peer-urls http://localhost:2382 `
  --initial-cluster standalone=http://localhost:2382
```

**Docker (alternative):**
```bash
docker compose up -d
```

> etcd is ready when you see:
> `"msg":"serving client traffic insecurely","address":"127.0.0.1:2381"`

---

### Step 3 — Activate the virtual environment

Open a **new terminal** (separate from the etcd terminal).

**CMD:**
```cmd
venv\Scripts\activate.bat
```

**PowerShell:**
```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
venv\Scripts\Activate.ps1
```

---

### Step 4 — Run the demo

```cmd
python distributed_lock.py
```

This runs 4 built-in scenarios:
1. Basic acquire and release
2. Re-entrant nested locking (same instance, no deadlock)
3. Auto-renew — holds a lock for 7 seconds with TTL=3s
4. `is_locked` property check

---

### Step 5 — See distributed locking live (2 terminals)

This shows two separate processes competing for the same lock.

Activate venv in **two terminals**, then run:

**Terminal 1:**
```cmd
python worker.py A
```

**Terminal 2 (immediately after):**
```cmd
python worker.py B
```

**What you will see:**
```
[A] Trying to acquire lock...
[A] GOT THE LOCK — owner: 14fe314b...     [B] Trying to acquire lock...
[A] Working for 5 seconds...              [B] (waiting — A holds it)
[A] Done. Releasing lock.
                                          [B] GOT THE LOCK — owner: 8b65e0d5...
                                          [B] Working for 5 seconds...
                                          [B] Done. Releasing lock.
```

B waits until A finishes and releases. This is the distributed lock in action.

---

### Step 6 — Run the test suite

```cmd
pytest -v
```

---

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `'.' is not recognized` | Running PowerShell command in CMD | Use CMD version (no `.\`, no backticks) |
| `bind: Only one usage of each socket address` | Port 2382 already in use | Change `--listen-peer-urls` and `--initial-cluster` to a free port (e.g. `2389`) |
| `db file is flocked by another process` | Old etcd process still running | Kill it, run `rmdir /s /q etcd-data`, then restart etcd |
| `grpc._channel._InactiveRpcError` | etcd not running | Make sure the etcd terminal (Step 2) is still open |
| `server has been already initialized` | Stale data from old run | Run `rmdir /s /q etcd-data` and restart etcd |

---

## API

### Context manager (recommended)

```python
from distributed_lock import DistributedLock

try:
    with DistributedLock("my-resource", ttl=30, timeout=10, auto_renew=True) as lock:
        print("Lock held by", lock.owner)   # UUID string
        print("Still locked?", lock.is_locked)
        # ... critical section ...
except TimeoutError:
    print("Could not acquire lock in time")
```

### Low-level functions

```python
from distributed_lock import acquire_lock, release_lock

owner, lease = acquire_lock("my-resource", ttl=30, timeout=10)
# ... critical section ...
release_lock("my-resource", owner, lease)
```

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `resource` | str | — | Lock name — any unique string |
| `ttl` | int | `30` | Seconds before lease auto-expires (crash safety) |
| `timeout` | float | `None` | Max seconds to wait for lock (`None` = wait forever) |
| `auto_renew` | bool | `False` | Refresh lease in background thread for long tasks |

### Properties

| Property | Type | Description |
|---|---|---|
| `lock.owner` | str | UUID identifying this lock holder |
| `lock.is_locked` | bool | Live etcd check — True if we still hold the key |

---

## Project Files

```
distributed_lock.py              # the entire library (~283 lines)
worker.py                        # 2-terminal demo script
tests/
  __init__.py                    # makes tests/ a Python package
  test_distributed_lock.py       # 12 automated tests
etcd/
  etcd-v3.5.10-windows-amd64/
    etcd.exe                     # etcd server binary (Windows)
    etcdctl.exe                  # CLI to inspect cluster
    etcdutl.exe                  # offline etcd utility
etcd-data/                       # runtime cluster data (gitignored)
docker-compose.yml               # 3-node etcd cluster via Docker
requirements.txt                 # Python dependencies
pytest.ini                       # test configuration
EtcdLockMaster.txt               # complete project explanation (theory, design, races, tests)
```
