import logging
import etcd3
import uuid
import time
import threading
from etcd3.events import DeleteEvent

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("distributed_lock.log"),
    ]
)
logger = logging.getLogger(__name__)

# Module-level client; tests may monkey-patch this
client = etcd3.client(host="127.0.0.1", port=2381)


def safe_revoke(lease):
    """Revoke a lease, silently ignoring already-expired leases."""
    try:
        lease.revoke()
        logger.debug("Lease %s revoked", getattr(lease, "id", None))
    except Exception as e:
        if "requested lease not found" not in str(e):
            logger.error("Error revoking lease: %s", e)
        else:
            logger.warning("Lease not found (already expired)")


def _wait_for_deletion(key: str, wait_time) -> bool:
    """
    Watch `key` for a DeleteEvent for up to `wait_time` seconds.
    Pass wait_time=None for an unlimited wait.
    Returns True if the key is gone (or disappears), False on timeout.

    The watch is started before the existence check so we don't miss a
    deletion that races between the transaction failure and this call.
    """
    logger.debug("Watching key %s for deletion (wait_time=%s)", key, wait_time)

    if wait_time is not None and wait_time <= 0:
        # Already timed out — just check if key is already gone
        val, _ = client.get(key)
        return val is None

    events, cancel = client.watch(key)

    # Key may have been deleted in the window between the failed transaction
    # and us starting the watch; check now before blocking.
    val, _ = client.get(key)
    if val is None:
        logger.debug("Key %s already gone before watch loop", key)
        cancel()
        return True

    if wait_time is None:
        # No timeout — block until deletion
        for ev in events:
            if isinstance(ev, DeleteEvent):
                logger.debug("DeleteEvent received for key %s", key)
                cancel()
                return True
        return False

    timer = threading.Timer(wait_time, cancel)
    timer.start()
    try:
        for ev in events:
            if isinstance(ev, DeleteEvent):
                logger.debug("DeleteEvent received for key %s", key)
                timer.cancel()
                cancel()
                return True
    finally:
        timer.cancel()

    logger.debug("Watch for key %s ended without deletion", key)
    return False


def acquire_lock(resource: str, ttl: int = 30, timeout: float = None):
    """
    Try to acquire a lock named `resource`.

    Args:
        resource: lock name / identifier
        ttl:      lease time-to-live in seconds
        timeout:  max seconds to wait before giving up (None = wait forever)

    Returns:
        (owner_id: str, lease) on success.

    Raises:
        TimeoutError if the timeout expires.
    """
    key = f"/locks/{resource}"
    start = time.time()
    logger.info(
        "Attempting to acquire lock '%s' (ttl=%ss, timeout=%ss)",
        resource, ttl, timeout,
    )

    while True:
        elapsed = time.time() - start

        if timeout is not None and elapsed >= timeout:
            logger.error("Timeout acquiring lock '%s' after %.2fs", resource, elapsed)
            raise TimeoutError(f"Timed out after {timeout}s waiting for '{resource}'")

        lease = client.lease(ttl)
        owner = str(uuid.uuid4())
        logger.debug("Created lease %s for owner %s", lease.id, owner)

        got_it, _ = client.transaction(
            compare=[client.transactions.create(key) == 0],
            success=[client.transactions.put(key, owner, lease.id)],
            failure=[],
        )
        if got_it:
            logger.info(
                "Lock '%s' acquired by %s (lease %s)", resource, owner, lease.id
            )
            return owner, lease

        logger.debug(
            "Failed to acquire lock '%s'; revoking lease %s", resource, lease.id
        )
        safe_revoke(lease)

        # Wait for the current holder to release; remaining=None means no cap
        remaining = (timeout - elapsed) if timeout is not None else None
        saw_delete = _wait_for_deletion(key, remaining)
        if not saw_delete and timeout is not None:
            logger.error(
                "Timeout waiting for release of lock '%s' after %.2fs", resource, elapsed
            )
            raise TimeoutError(f"Timed out after {timeout}s waiting for '{resource}'")
        # loop and retry


def release_lock(resource: str, owner: str, lease):
    """Release the lock only if `owner` still holds it."""
    key = f"/locks/{resource}"
    val, _ = client.get(key)
    if val is None:
        logger.warning("Cannot release lock '%s': does not exist", resource)
    elif val.decode() == owner:
        logger.info(
            "Releasing lock '%s' held by %s (lease %s)", resource, owner, lease.id
        )
        safe_revoke(lease)
    else:
        logger.error(
            "Cannot release lock '%s': ownership mismatch (owner=%s)", resource, owner
        )
        raise RuntimeError("Cannot release lock: ownership mismatch")


class DistributedLock:
    """
    Re-entrant context manager for a distributed lock backed by etcd.

    Parameters:
        resource  (str):   lock name
        ttl       (int):   lease time-to-live in seconds
        timeout   (float): max seconds to wait for acquisition (None = forever)
        auto_renew(bool):  if True, refresh the lease in the background

    Usage:
        try:
            with DistributedLock("my-res", ttl=30, timeout=10, auto_renew=True) as lock:
                # critical section
                pass
        except TimeoutError:
            # could not acquire within timeout
            pass
    """

    def __init__(
        self,
        resource: str,
        ttl: int = 30,
        timeout: float = None,
        auto_renew: bool = False,
    ):
        self.resource = resource
        self.ttl = ttl
        self.timeout = timeout
        self.auto_renew = auto_renew
        self.owner = None
        self.lease = None
        self._stop_event = None
        self._renew_thread = None
        self._count = 0  # re-entrancy counter

    def __enter__(self):
        if self._count > 0:
            self._count += 1
            logger.debug(
                "Re-entering lock '%s', count=%d", self.resource, self._count
            )
            return self

        self.owner, self.lease = acquire_lock(self.resource, self.ttl, self.timeout)
        self._count = 1

        if self.auto_renew:
            self._stop_event = threading.Event()

            def _renew_loop():
                interval = self.ttl / 2.0
                while not self._stop_event.wait(interval):
                    try:
                        self.lease.refresh()
                        logger.debug("Lease %s refreshed", self.lease.id)
                    except Exception as e:
                        logger.error(
                            "Error refreshing lease %s: %s", self.lease.id, e
                        )
                        break

            self._renew_thread = threading.Thread(target=_renew_loop, daemon=True)
            self._renew_thread.start()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._count -= 1
        if self._count > 0:
            logger.debug(
                "Exit called, still holding lock '%s', count=%d",
                self.resource,
                self._count,
            )
            return

        if self.auto_renew and self._stop_event:
            self._stop_event.set()
            self._renew_thread.join()

        try:
            release_lock(self.resource, self.owner, self.lease)
        except Exception as e:
            logger.error("Error releasing lock for %s: %s", self.resource, e)

    @property
    def is_locked(self) -> bool:
        """True if the lock key still exists in etcd and is owned by this instance."""
        key = f"/locks/{self.resource}"
        val, _ = client.get(key)
        return val is not None and val.decode() == self.owner


if __name__ == "__main__":
    # 1) Basic acquire / release
    with DistributedLock("demo", ttl=10, timeout=5) as lock:
        print("Acquired lock as", lock.owner)
    print("Released lock\n")

    # 2) Re-entrant: nested with on the same instance
    with DistributedLock("demo2", ttl=10, timeout=5) as lock:
        print("First acquire", lock.owner)
        with lock:
            print("Re-entered", lock.owner)
    print("Released after nested\n")

    # 3) Auto-renew: holds past the initial TTL
    with DistributedLock("long_demo", ttl=3, timeout=5, auto_renew=True) as lock:
        print("Auto-renewed lock acquired, owner=", lock.owner)
        time.sleep(7)
        print("Releasing after long task")

    # 4) is_locked check
    with DistributedLock("job42", ttl=30, timeout=5, auto_renew=True) as lock:
        if not lock.is_locked:
            raise RuntimeError("Oops, we lost the lock!")
        else:
            print("Lock is still held")
