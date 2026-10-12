"""
redis_store.py - shared Redis toolbox used by the master and the workers.

- connect():       a Redis client that always talks to the CURRENT master (via Sentinel)
- key helpers:     one place that defines every key name
- record_tile():   count a finished tile safely (no duplicates, no race conditions)
- push_event():    add a line to the dashboard's event log

KEY LAYOUT  (try them: docker exec -it redis-1 redis-cli)
job:<id>                HASH    status, filter, total_tiles, processed_tiles, ...
job:<id>:done           ZSET    tile indexes already received (score = arrival time)
job:<id>:tiles          HASH    tile index -> processed tile bytes (deleted after assembly)
job:<id>:workers        HASH    tile index -> worker that processed it (UI tile map)
jobs:feed               ZSET    job ids, score = upload time (newest first = the feed)
preview:<sha256>        STRING  thumbnail of the original image
cache:<sha256>:<filter> STRING  finished image bytes, TTL 24h
worker:<id>:heartbeat   STRING  refreshed every 3s, TTL 10s (exists = alive)
worker:<id>:info        HASH    group, filter, partitions, tiles processed
workers:last_seen       ZSET    every worker ever seen, score = last heartbeat
events                  LIST    newest-first event log for the dashboard
stats                   HASH    counters: cache_hits, cache_misses, tiles_processed
"""
import json
import time

from redis.backoff import ExponentialBackoff
from redis.retry import Retry
from redis.sentinel import Sentinel

from common import config

_sentinel = None


def sentinel() -> Sentinel:
    """Client for the 3 Sentinels (created once, then reused).
    Sentinels hold no data; they answer 'who is the master right now?'."""
    global _sentinel
    if _sentinel is None:
        _sentinel = Sentinel(config.REDIS_SENTINELS, socket_timeout=1.0)
    return _sentinel


def connect(decode: bool = True):
    """A Redis client that always talks to the CURRENT master.
    Input:  decode=True  -> answers come back as str   (job metadata)
            decode=False -> answers come back as bytes (image data)
    Output: redis client

    How failover stays invisible: each new connection first asks Sentinel for the
    master's address. If the master dies, commands fail, the client retries with
    backoff (~20s total), reconnects, asks Sentinel again and reaches the promoted replica."""
    return sentinel().master_for(
        config.REDIS_MASTER_NAME,          # "mymaster", never a fixed host
        decode_responses=decode,
        socket_timeout=3.0,
        socket_connect_timeout=2.0,
        retry=Retry(ExponentialBackoff(cap=1.0, base=0.1), retries=25),
    )


def wait_for_redis(log):
    """Block at startup until Sentinel knows a master and it answers PING.
    (A started container is not the same as a ready Redis.)"""
    while True:
        try:
            host, port = sentinel().discover_master(config.REDIS_MASTER_NAME)
            connect().ping()
            log.info("✅ Redis ready - Sentinel says the master is %s:%s", host, port)
            return
        except Exception as e:  # noqa: BLE001
            log.info("⏳ waiting for Redis (%s)", e)
            time.sleep(2)


# Key names, defined once so every file spells them the same way.
# Example: job_key("a3f9c1") -> "job:a3f9c1",  cache_key("9b2e...", "blur") -> "cache:9b2e...:blur"
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
    """Record one processed tile, safely. Called by the master for every result.
    Input:  redis client, job id, tile index, worker id, processed tile bytes
    Output: (is_new, processed, total)
            (True, 5, 16)        -> new tile, 5 of 16 done
            (False, None, None)  -> duplicate, ignore it

    1. ZADD NX: add the tile to the job's "done" set only if it isn't there.
       Kafka is at-least-once, so a tile can arrive twice; the 2nd time this returns 0.
    2. HINCRBY: +1 inside Redis in one atomic step, returns the new count.
       Every caller gets a different number, so exactly ONE sees processed == total
       and reassembles the image."""
    is_new = r.zadd(job_done_key(job_id), {tile_index: time.time()}, nx=True)
    if not is_new:
        return False, None, None
    # save the bytes BEFORE counting, so when the count says "done" every tile is stored
    r.hset(job_tiles_key(job_id), tile_index, tile_bytes)
    r.hset(job_workers_key(job_id), tile_index, worker_id)
    processed = r.hincrby(job_key(job_id), "processed_tiles", 1)
    total = int(r.hget(job_key(job_id), "total_tiles"))
    return True, processed, total


def push_event(r, source, message, level="info"):
    """Add a line to the dashboard event log; keep only the newest 200.
    Example: push_event(r, "monitor", "worker-blur-1 is DEAD", level="error")
    Never raises: a logging failure must not break the caller."""
    try:
        entry = json.dumps({"ts": time.time(), "source": source, "level": level, "message": message})
        r.lpush(EVENTS_KEY, entry)
        r.ltrim(EVENTS_KEY, 0, config.EVENTS_MAX - 1)
    except Exception:  # noqa: BLE001
        pass
