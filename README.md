# EtcdLockMaster

A distributed locking library in Python built on [etcd](https://etcd.io). Guarantees that only one process across any number of machines holds a lock at a time — with automatic cleanup on crash, optional auto-renewal, and re-entrancy support.

## Features

- **Exclusive locking** via etcd's atomic compare-and-swap transactions
- **Auto-cleanup** — lease TTL means a crashed holder never blocks others forever
- **Auto-renewal** — background thread keeps long-running tasks safe
- **Re-entrant** — same instance can nest `with` blocks without deadlocking
- **Watch-based waiting** — no polling; waiters are woken the instant the lock is released
- **Structured logging** to file for audit trails

## Quick Start

```python
from distributed_lock import DistributedLock

try:
    with DistributedLock("my-resource", ttl=30, timeout=10, auto_renew=True) as lock:
        print("Lock held by", lock.owner)
        # ... do work ...
except TimeoutError:
    print("Could not acquire lock in time")
```

## Setup

**1. Start etcd** (choose one)

Local Windows binaries (included):
```bash
ETCD=etcd/etcd-v3.5.10-windows-amd64/etcd.exe
CLUSTER="etcd1=http://127.0.0.1:2380,etcd2=http://127.0.0.1:2382"

$ETCD --name=etcd1 --data-dir=etcd-data/node1 \
      --listen-client-urls=http://127.0.0.1:2379 \
      --advertise-client-urls=http://127.0.0.1:2379 \
      --listen-peer-urls=http://127.0.0.1:2380 \
      --initial-advertise-peer-urls=http://127.0.0.1:2380 \
      --initial-cluster=$CLUSTER --initial-cluster-state=new >> etcd-data/etcd1.log 2>&1 &

$ETCD --name=etcd2 --data-dir=etcd-data/node2 \
      --listen-client-urls=http://127.0.0.1:2381 \
      --advertise-client-urls=http://127.0.0.1:2381 \
      --listen-peer-urls=http://127.0.0.1:2382 \
      --initial-advertise-peer-urls=http://127.0.0.1:2382 \
      --initial-cluster=$CLUSTER --initial-cluster-state=new >> etcd-data/etcd2.log 2>&1 &
```

Or via Docker:
```bash
docker compose up -d
```

**2. Install dependencies**
```bash
python -m venv venv
./venv/Scripts/pip install -r requirements.txt   # Windows
# source venv/bin/activate && pip install -r requirements.txt  # Mac/Linux
```

**3. Run the demo**
```bash
./venv/Scripts/python distributed_lock.py
```

**4. Run tests**
```bash
./venv/Scripts/python -m pytest -v
```

## API

```python
# Low-level
owner, lease = acquire_lock("resource", ttl=30, timeout=10)
release_lock("resource", owner, lease)

# Context manager (recommended)
with DistributedLock("resource", ttl=30, timeout=10, auto_renew=True) as lock:
    lock.owner      # UUID string identifying this holder
    lock.is_locked  # True if etcd still has our key
```

| Parameter | Default | Description |
|---|---|---|
| `resource` | — | Lock name (any string) |
| `ttl` | `30` | Lease lifetime in seconds |
| `timeout` | `None` | Max wait time (`None` = wait forever) |
| `auto_renew` | `False` | Refresh lease in background |

## Project Structure

```
distributed_lock.py      # library
tests/
  test_distributed_lock.py  # 12 tests
docker-compose.yml       # 3-node etcd via Docker
requirements.txt
etcd/                    # Windows etcd binaries
etcd-data/               # runtime cluster data (gitignored)
PROJECT_EXPLAINED.txt    # deep-dive explanation of every piece
```

## Requirements

- Python 3.9+
- etcd v3.5+
- `etcd3==0.12.0`, `grpcio==1.62.3`, `protobuf==3.20.3`

> **Note:** `protobuf` must stay at `3.x` — the `etcd3` client library uses generated proto code that is incompatible with protobuf 4.x.
