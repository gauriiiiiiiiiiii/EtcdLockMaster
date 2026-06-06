import threading
import time
import pytest
import etcd3

from distributed_lock import acquire_lock, release_lock, DistributedLock
import distributed_lock

# Uses the same etcd instance as the main module (host=127.0.0.1, port=2381)
client = etcd3.client(host="127.0.0.1", port=2381)


@pytest.fixture(autouse=True)
def etcd_cleanup():
    client.delete_prefix("/locks/")
    yield
    client.delete_prefix("/locks/")


# ---------------------------------------------------------------------------
# Basic acquire / release
# ---------------------------------------------------------------------------

def test_basic_acquire_release():
    owner, lease = acquire_lock("test1", ttl=5, timeout=1)
    val, _ = client.get("/locks/test1")
    assert val.decode() == owner
    release_lock("test1", owner, lease)
    val, _ = client.get("/locks/test1")
    assert val is None


# ---------------------------------------------------------------------------
# Lease expiry
# ---------------------------------------------------------------------------

def test_lease_expiry_allows_reacquire():
    owner1, lease1 = acquire_lock("test2", ttl=1, timeout=1)
    time.sleep(2)  # let the lease expire naturally
    owner2, lease2 = acquire_lock("test2", ttl=1, timeout=1)
    assert owner2 != owner1
    release_lock("test2", owner2, lease2)


# ---------------------------------------------------------------------------
# Re-entrancy
# ---------------------------------------------------------------------------

def test_reentrant_behavior():
    with DistributedLock("test3", ttl=5, timeout=1) as lock:
        first_owner = lock.owner
        with lock:  # nested re-entry — same owner, no block
            assert lock.owner == first_owner
    val, _ = client.get("/locks/test3")
    assert val is None


# ---------------------------------------------------------------------------
# Auto-renew keeps the lock alive past its TTL
# ---------------------------------------------------------------------------

def test_auto_renew_holds_longer_than_ttl():
    t0 = time.time()
    with DistributedLock("test4", ttl=1, timeout=1, auto_renew=True) as lock:
        time.sleep(3)  # well past TTL
        val, _ = client.get("/locks/test4")
        assert val.decode() == lock.owner
    val, _ = client.get("/locks/test4")
    assert val is None
    assert time.time() - t0 >= 3


# ---------------------------------------------------------------------------
# Concurrent acquisition: one succeeds, one times out
# ---------------------------------------------------------------------------

def test_concurrent_acquire_times_out_one_and_succeeds_other():
    results = []

    def worker(name, sleep_before_release, timeout):
        try:
            with DistributedLock("test5", ttl=5, timeout=timeout) as lock:
                results.append((name, "acquired", lock.owner))
                time.sleep(sleep_before_release)
        except TimeoutError:
            results.append((name, "timeout", None))

    t1 = threading.Thread(target=worker, args=("t1", 2, 3))
    t2 = threading.Thread(target=worker, args=("t2", 0, 1))
    t1.start()
    time.sleep(0.1)  # ensure t1 grabs the lock first
    t2.start()
    t1.join()
    t2.join()

    assert ("t1", "acquired") in [(r[0], r[1]) for r in results]
    assert ("t2", "timeout") in [(r[0], r[1]) for r in results]


# ---------------------------------------------------------------------------
# is_locked property
# ---------------------------------------------------------------------------

def test_lock_is_locked():
    with DistributedLock("test6", ttl=5, timeout=1) as lock:
        assert lock.is_locked
    assert not lock.is_locked


# ---------------------------------------------------------------------------
# Exclusivity — direct function calls
# ---------------------------------------------------------------------------

def test_exclusive_acquire_direct():
    owner1, lease1 = acquire_lock("exclusive", ttl=5, timeout=1)
    with pytest.raises(TimeoutError):
        acquire_lock("exclusive", ttl=5, timeout=0.5)
    release_lock("exclusive", owner1, lease1)
    owner2, lease2 = acquire_lock("exclusive", ttl=5, timeout=1)
    assert owner2 != owner1
    release_lock("exclusive", owner2, lease2)


def test_exclusive_acquire_indirect():
    owner1, lease1 = acquire_lock("exclusive", ttl=5, timeout=1)
    with pytest.raises(TimeoutError):
        with DistributedLock("exclusive", ttl=5, timeout=0.5):
            pass
    release_lock("exclusive", owner1, lease1)
    owner2, lease2 = acquire_lock("exclusive", ttl=5, timeout=1)
    assert owner2 != owner1
    release_lock("exclusive", owner2, lease2)


def test_exclusive_acquire_context_manager():
    lock1 = DistributedLock("exclusive2", ttl=5, timeout=1)
    lock2 = DistributedLock("exclusive2", ttl=5, timeout=0.5)

    with lock1:
        assert lock1.is_locked
        with pytest.raises(TimeoutError):
            with lock2:
                pass

    with lock2 as l2:
        assert l2.is_locked


# ---------------------------------------------------------------------------
# TTL expiry detected inside the with block (no auto-renew)
# ---------------------------------------------------------------------------

def test_ttl_expiry_detected_inside_block():
    with DistributedLock("shortlived", ttl=1, timeout=1, auto_renew=False) as lock:
        assert lock.is_locked
        time.sleep(3)
        assert not lock.is_locked  # lease expired on the server


# ---------------------------------------------------------------------------
# Nested with counts: only one release on exit
# ---------------------------------------------------------------------------

def test_nested_with_counts_and_release_once():
    lock = DistributedLock("nested", ttl=3, timeout=1, auto_renew=False)
    with lock:
        first_owner = lock.owner
        with lock:  # re-entrant
            assert lock.owner == first_owner
            val, _ = client.get("/locks/nested")
            assert val.decode() == first_owner
        assert lock.is_locked
    val, _ = client.get("/locks/nested")
    assert val is None


# ---------------------------------------------------------------------------
# Multi-client exclusivity: two different etcd client connections
# ---------------------------------------------------------------------------

def test_concurrent_acquire_different_clients():
    """Thread A on port 2381, Thread B on port 2379 — same cluster, one lock."""
    try:
        etcd3.client(host="127.0.0.1", port=2379).status()
    except Exception:
        pytest.skip("port 2379 not reachable — requires Docker 3-node cluster")

    _original_client = distributed_lock.client
    results = []

    def worker(name, port, hold_time, timeout):
        distributed_lock.client = etcd3.client(host="127.0.0.1", port=port)
        try:
            with distributed_lock.DistributedLock(
                "shared", ttl=5, timeout=timeout, auto_renew=False
            ) as lock:
                results.append((name, "acquired", port))
                time.sleep(hold_time)
        except TimeoutError:
            results.append((name, "timeout", port))

    tA = threading.Thread(target=worker, args=("A", 2381, 2.0, 2.0))
    tB = threading.Thread(target=worker, args=("B", 2379, 0.0, 1.0))
    tA.start()
    time.sleep(0.1)
    tB.start()
    tA.join()
    tB.join()

    # Restore the original client so teardown fixtures work correctly
    distributed_lock.client = _original_client

    assert ("A", "acquired", 2381) in results
    assert ("B", "timeout", 2379) in results
