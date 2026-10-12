"""
result_assembler.py - the master's CONSUMER role (background thread).

Reads processed tiles from image_results and rebuilds finished images.
For each tile: record it in Redis -> if it was the last one, reassemble and cache
the image -> commit the Kafka offset.

Kafka side: the group coordinator of 'master-assemblers' assigns us image_results P0
and stores our committed offset; the messages come from that partition's leader.
"""
import logging
import threading
import time

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from common import config, imaging
from common import redis_store as rs

log = logging.getLogger("assembler")


class ResultAssembler(threading.Thread):
    def __init__(self):
        """Redis clients (text + bytes) and a consumer in group 'master-assemblers'."""
        super().__init__(daemon=True, name="result-assembler")
        self.r = rs.connect()                 # str responses
        self.rb = rs.connect(decode=False)    # bytes responses (image data)
        self.consumer = Consumer({
            "bootstrap.servers": config.KAFKA_BOOTSTRAP,
            "group.id": config.MASTER_GROUP,
            "client.id": "master-assembler",
            "enable.auto.commit": False,      # commit only after the tile is saved in Redis
            "auto.offset.reset": "earliest",
            "session.timeout.ms": config.KAFKA_SESSION_TIMEOUT_MS,
            "heartbeat.interval.ms": config.KAFKA_HEARTBEAT_INTERVAL_MS,
        })

    def on_assign(self, consumer, partitions):
        """Log which committed offset we resume from when we get image_results P0."""
        for tp in consumer.committed(partitions, timeout=10):
            start = tp.offset if tp.offset >= 0 else "the beginning (no commit yet)"
            log.info("✅ ASSIGNED %s[P%d] -> resuming from offset %s", tp.topic, tp.partition, start)

    def run(self):
        """Poll image_results forever. Commit each message only after handle() succeeded;
        on failure (e.g. Redis down too long) rewind and retry the same tile."""
        self.consumer.subscribe([config.RESULTS_TOPIC], on_assign=self.on_assign)
        log.info("🎧 listening on %s as group '%s'", config.RESULTS_TOPIC, config.MASTER_GROUP)
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.warning("consumer error: %s", msg.error())
                continue
            try:
                self.handle(msg)
                self.consumer.commit(message=msg, asynchronous=False)
            except Exception:  # noqa: BLE001
                log.exception("failed on %s - will retry", msg.key())
                self.consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                time.sleep(2)

    def handle(self, msg):
        """Record one processed tile.
        Input:  Kafka message, key "a3f9c1:5", value = processed tile bytes, header worker=...
        Effect: saved in Redis; if it completes the job (e.g. 16/16) -> assemble().
        Duplicates (same tile twice, at-least-once delivery) are ignored."""
        job_id, tile_index = msg.key().decode().rsplit(":", 1)
        headers = {k: v.decode() for k, v in (msg.headers() or [])}
        worker = headers.get("worker", "?")

        if not self.r.exists(rs.job_key(job_id)):
            log.info("ignoring tile for unknown/expired job %s", job_id)
            return

        is_new, processed, total = rs.record_tile(self.r, job_id, int(tile_index), worker, msg.value())
        if not is_new:
            # a worker redid this tile (e.g. crashed before committing); ZADD NX caught it
            log.warning("♻️  duplicate tile %s from %s ignored", msg.key().decode(), worker)
            return

        log.info("📥 tile %s from %s (partition %d, offset %d) -> %d/%d",
                 tile_index, worker, msg.partition(), msg.offset(), processed, total)
        if processed == total:      # only ONE call ever sees this (HINCRBY is atomic)
            self.assemble(job_id)

    def assemble(self, job_id):
        """Rebuild the full image from its tiles and cache it.
        Input:  job id whose tiles are all in Redis (job:<id>:tiles)
        Effect: cache:<sha256>:<filter> = finished JPEG (24h), job status = done,
                temporary tiles deleted."""
        started = time.perf_counter()
        job = self.r.hgetall(rs.job_key(job_id))
        self.r.hset(rs.job_key(job_id), "status", "assembling")

        raw_tiles = self.rb.hgetall(rs.job_tiles_key(job_id))        # {b"0": bytes, b"1": bytes, ...}
        tiles = {int(k): v for k, v in raw_tiles.items()}
        canvas = imaging.assemble(int(job["width"]), int(job["height"]), tiles)
        result = imaging.encode_jpeg(canvas)

        # One transaction (MULTI/EXEC): all of these happen together, or none do.
        ck = rs.cache_key(job["image_hash"], job["filter"])
        now = time.time()
        pipe = self.rb.pipeline(transaction=True)
        pipe.set(ck, result, ex=config.CACHE_TTL_SEC)            # the cache entry
        pipe.hset(rs.job_key(job_id), mapping={
            "status": "done",
            "result_key": ck,
            "completed_at": now,
            "duration_ms": int((now - float(job["created_at"])) * 1000),
        })
        pipe.delete(rs.job_tiles_key(job_id))                     # temp tiles no longer needed
        for key in (rs.job_key(job_id), rs.job_done_key(job_id), rs.job_workers_key(job_id)):
            pipe.expire(key, config.JOB_TTL_SEC)
        pipe.execute()

        log.info("🧩 job %s reassembled (%sx%s, %d tiles) in %.0f ms -> cached as %s…",
                 job_id, job["width"], job["height"], len(tiles),
                 (time.perf_counter() - started) * 1000, ck[:22])
        rs.push_event(self.r, "master", f"job {job_id} done ({len(tiles)} tiles) - result cached")
