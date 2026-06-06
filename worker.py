import sys
import time
from distributed_lock import DistributedLock

name = sys.argv[1] if len(sys.argv) > 1 else "Worker"

print(f"[{name}] Trying to acquire lock...")
try:
    with DistributedLock("my-job", ttl=30, timeout=15) as lock:
        print(f"[{name}] GOT THE LOCK — owner: {lock.owner[:8]}...")
        print(f"[{name}] Working for 5 seconds...")
        time.sleep(5)
        print(f"[{name}] Done. Releasing lock.")
except TimeoutError:
    print(f"[{name}] Could not get lock — timed out!")
