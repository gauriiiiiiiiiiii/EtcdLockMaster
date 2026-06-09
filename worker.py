import sys
import time
from distributed_lock import DistributedLock, client

name = sys.argv[1] if len(sys.argv) > 1 else "Worker"

print(f"[{name}] Trying to acquire lock...")

val, _ = client.get("/locks/my-job")
if val:
    print(f"[{name}] Lock is held by {val.decode()[:8]}... — waiting to acquire")

try:
    with DistributedLock("my-job", ttl=30, timeout=30) as lock:
        print(f"[{name}] GOT THE LOCK — owner: {lock.owner[:8]}...")
        print(f"[{name}] Working for 20 seconds...")
        time.sleep(20)
        print(f"[{name}] Done. Releasing lock.")
except TimeoutError:
    print(f"[{name}] Could not get lock — timed out!")
