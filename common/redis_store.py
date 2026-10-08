"""
Everything Redis: how we connect (through Sentinel) and the key layout.

KEY LAYOUT  (try them: docker exec -it redis-1 redis-cli)
─────────────────────────────────────────────────────────────────────────
job:<id>                HASH    status, filter, total_tiles, processed_tiles, ...
job:<id>:done           ZSET    member = tile index, score = time it arrived
job:<id>:tiles          HASH    tile index -> processed tile bytes (temporary)
job:<id>:workers        HASH    tile index -> worker that processed it (for the UI)
jobs:feed               ZSET    member = job id, score = upload time (the feed)
preview:<sha256>        STRING  small thumbnail of the original image
cache:<sha256>:<filter> STRING  final processed image bytes, TTL 24h
worker:<id>:heartbeat   STRING  refreshed every 3s with TTL 10s (liveness)
worker:<id>:info        HASH    group, filter, partitions, tiles processed
workers:last_seen       ZSET    member = worker id, score = last heartbeat time
events                  LIST    newest-first event log for the dashboard
stats                   HASH    counters: cache_hits, cache_misses, ...
"""
import json
import time

from redis.backoff import ExponentialBackoff
from redis.retry import Retry
from redis.sentinel import Sentinel

from common import config

_sentinel = None


def sentinel() -> Sentinel:
    """Client for the 3 Sentinels. They hold no data; they answer
    'which node is the master right now?' and run the failover."""
    global _sentinel
    if _sentinel is None:
        _sentinel = Sentinel(config.REDIS_SENTINELS, socket_timeout=1.0)
    return _sentinel


def connect(decode: bool = True):
    """A Redis client that always talks to the CURRENT master.

    master_for() asks Sentinel for the master's address whenever it opens a
    connection. If the master dies, commands fail -> Retry waits and reconnects
    -> Sentinel now returns the promoted replica. (~20s of retries in total.)

    decode=True -> str responses (metadata)   decode=False -> bytes (images)
    """
    return sentinel().master_for(
        config.REDIS_MASTER_NAME,
        decode_responses=decode,
        socket_timeout=3.0,
        socket_connect_timeout=2.0,
        retry=Retry(ExponentialBackoff(cap=1.0, base=0.1), retries=25),
    )


def wait_for_redis(log):
    while True:
        try:
            host, port = sentinel().discover_master(config.REDIS_MASTER_NAME)
            connect().ping()
            log.info("✅ Redis ready - Sentinel says the master is %s:%s", host, port)
            return
        except Exception as e:  # noqa: BLE001
            log.info("⏳ waiting for Redis (%s)", e)
            time.sleep(2)


# key names
def job_key(job_id):          return f"job:{job_id}"
def job_done_key(job_id):     return f"job:{job_id}:done"
def job_tiles_key(job_id):    return f"job:{job_id}:tiles"
def job_workers_key(job_id):  return f"job:{job_id}:workers"
def preview_key(image_hash):  return f"preview:{image_hash}"
def cache_key(image_hash, flt): return f"cache:{image_hash}:{flt}"
def heartbeat_key(worker_id): return f"worker:{worker_id}:heartbeat"
def worker_info_key(worker_id): return f"worker:{worker_id}:info"


FEED_KEY = "jobs:feed"
WORKERS_LAST_SEEN_KEY = "workers:last_seen"
EVENTS_KEY = "events"
STATS_KEY = "stats"


def record_tile(r, job_id, tile_index, worker_id, tile_bytes):
    """Record one processed tile. Safe against duplicates and races.

    Step 1  ZADD NX  -> adds the tile index to the job's sorted set ONLY if it is
            not there yet. Returns 1 for a new tile, 0 for a duplicate.
            Kafka is at-least-once, so the same tile CAN arrive twice; without
            this check we'd count it twice and finish the job with a tile missing.
    Step 2  HINCRBY  -> atomic +1 on processed_tiles, returns the NEW value.
            Redis runs commands one at a time, so even if many clients increment
            at once, each one gets a different number back. Exactly ONE caller sees
            processed == total, and only that caller reassembles the image.
            (A Python 'GET, +1, SET' could let two clients both read 5 and both write 6.)

    Returns (is_new, processed, total).
    """
    is_new = r.zadd(job_done_key(job_id), {tile_index: time.time()}, nx=True)
    if not is_new:
        return False, None, None
    # store the bytes BEFORE counting, so when the count says "done" every tile is there
    r.hset(job_tiles_key(job_id), tile_index, tile_bytes)
    r.hset(job_workers_key(job_id), tile_index, worker_id)
    processed = r.hincrby(job_key(job_id), "processed_tiles", 1)
    total = int(r.hget(job_key(job_id), "total_tiles"))
    return True, processed, total


def push_event(r, source, message, level="info"):
    """Add a line to the dashboard's event log (bounded to the newest N)."""
    try:
        entry = json.dumps({"ts": time.time(), "source": source, "level": level, "message": message})
        r.lpush(EVENTS_KEY, entry)
        r.ltrim(EVENTS_KEY, 0, config.EVENTS_MAX - 1)
    except Exception:  # noqa: BLE001 - logging must never break the caller
        pass
