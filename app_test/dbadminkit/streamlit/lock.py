import redis
import time

# Redis connection (adjust host/port for your setup)
redis_client = redis.Redis(host='localhost', port=6379, db=0)

def acquire_lock(lock_key: str, timeout: int = 10) -> bool:
    """Acquire a lock with Redis; return False if already locked."""
    return redis_client.set(lock_key, "locked", nx=True, ex=timeout)

def release_lock(lock_key: str):
    """Release the lock by deleting the key."""
    redis_client.delete(lock_key)

def is_locked(lock_key: str) -> bool:
    """Check if the lock exists."""
    return redis_client.exists(lock_key)