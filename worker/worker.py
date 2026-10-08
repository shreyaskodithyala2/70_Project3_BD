"""
WORKER  -  one container = one consumer in a consumer group.

    image_tasks ──poll──▶ decode ▶ filter ▶ crop halo ▶ encode ──produce──▶ image_results
                                                                  │
                                       commit offset ◀── only after Kafka acked the result

Environment (set in docker-compose.yml):
    WORKER_ID      e.g. worker-blur-1     (also used as the Kafka client.id)
    WORKER_FILTER  blur | blackwhite      (picks the consumer group)

Delivery guarantee = AT-LEAST-ONCE
    We commit the offset only AFTER the result is safely in Kafka. If we crash
    in between, the tile is processed again by whoever owns the partition next.
    The master drops the duplicate result (ZADD NX in redis_store.py).
"""
import json
import logging
import os
import signal
import threading
import time

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition

from common import config, imaging
from common import redis_store as rs
from common.kafka_internals import offsets_partition_for_group

config.setup_logging()
WORKER_ID = os.environ["WORKER_ID"]
MY_FILTER = os.environ["WORKER_FILTER"]
GROUP_ID = config.WORKER_GROUPS[MY_FILTER]

log = logging.getLogger("task")
rlog = logging.getLogger("rebalance")
clog = logging.getLogger("commit")
hlog = logging.getLogger("heartbeat")


class Worker:
    def __init__(self):
        self.stop_event = threading.Event()
        self.assigned = set()               # partitions we own right now, e.g. {0}
        self.lock = threading.Lock()        # guards self.assigned (heartbeat thread reads it)

        rs.wait_for_redis(log)
        self.r = rs.connect()

        # ── the CONSUMER: reads tiles from image_tasks ──
        self.consumer = Consumer({
            "bootstrap.servers": config.KAFKA_BOOTSTRAP,
            "group.id": GROUP_ID,           # same group.id => the partitions are SPLIT between us
            "client.id": WORKER_ID,
            # We commit offsets ourselves, after the result is safely in Kafka.
            "enable.auto.commit": False,
            # A brand-new group has no committed offset yet -> start from the oldest message.
            "auto.offset.reset": "earliest",
            # Cooperative rebalancing: on a rebalance only the partitions that MOVE are
            # revoked; everyone keeps working on the rest. (The older 'eager' protocol
            # stops the whole group and revokes everything.)
            "partition.assignment.strategy": "cooperative-sticky",
            # Liveness towards the group coordinator: a background thread in the client
            # heartbeats every 3s; no heartbeat for 10s -> we're kicked out -> rebalance.
            "session.timeout.ms": config.KAFKA_SESSION_TIMEOUT_MS,
            "heartbeat.interval.ms": config.KAFKA_HEARTBEAT_INTERVAL_MS,
            # If we don't call poll() for 60s we're considered stuck and kicked out too.
            "max.poll.interval.ms": 60000,
        })

        # ── the PRODUCER: writes processed tiles to image_results ──
        self.producer = Producer({
            "bootstrap.servers": config.KAFKA_BOOTSTRAP,
            "client.id": f"{WORKER_ID}-producer",
            "acks": "all",                  # leader waits for all in-sync replicas
            "enable.idempotence": True,     # broker drops duplicates caused by producer retries
        })

    # ───────────────── rebalance callbacks (run INSIDE consumer.poll()) ─────────────────
    def on_assign(self, consumer, partitions):
        """The coordinator gave us partitions. Where do we start reading?
        From the group's COMMITTED offset for that partition. That offset is stored
        on the coordinator (in __consumer_offsets) and belongs to the group, not to
        a worker. So whoever gets the partition, even after a crash, continues
        exactly where the previous owner last committed."""
        try:
            committed = consumer.committed(partitions, timeout=10)
        except KafkaException as e:
            committed = partitions
            rlog.warning("could not fetch committed offsets: %s", e)
        for tp in committed:
            start = (f"committed offset {tp.offset}" if tp.offset >= 0
                     else "no committed offset yet -> auto.offset.reset=earliest")
            rlog.info("✅ ASSIGNED %s[P%d] -> resuming from %s", tp.topic, tp.partition, start)
            rs.push_event(self.r, WORKER_ID, f"assigned {tp.topic}[P{tp.partition}] -> resume from {start}")
        with self.lock:
            self.assigned |= {tp.partition for tp in partitions}

    def on_revoke(self, consumer, partitions):
        """Called BEFORE partitions are taken away during a normal rebalance. Nothing
        to flush here: we already commit after every tile."""
        for tp in partitions:
            rlog.info("↩️  REVOKED %s[P%d] (handing it to another group member)", tp.topic, tp.partition)
            rs.push_event(self.r, WORKER_ID, f"revoked {tp.topic}[P{tp.partition}]", level="warn")
        with self.lock:
            self.assigned -= {tp.partition for tp in partitions}

    def on_lost(self, consumer, partitions):
        """Partitions were taken from us WITHOUT a polite revoke: we missed the
        session timeout (e.g. the container was paused). Another worker may already
        be processing them, so any commit we try for them will now fail."""
        for tp in partitions:
            rlog.warning("💥 LOST %s[P%d] - we were kicked out of the group (session timeout)",
                         tp.topic, tp.partition)
            rs.push_event(self.r, WORKER_ID, f"LOST {tp.topic}[P{tp.partition}] (session timeout)", level="error")
        with self.lock:
            self.assigned -= {tp.partition for tp in partitions}

    # ───────────────────── Redis heartbeat (separate thread) ─────────────────────
    def heartbeat_loop(self):
        """Every 3s: SET worker:<id>:heartbeat ... EX 10.
        Alive = the key exists. If this process dies, nobody refreshes the key and
        Redis deletes it 10s later. The master's monitor notices it's gone."""
        while not self.stop_event.is_set():
            try:
                with self.lock:
                    partitions = sorted(self.assigned)
                now = time.time()
                beat = {"worker_id": WORKER_ID, "group": GROUP_ID, "filter": MY_FILTER,
                        "partitions": partitions, "ts": now}
                pipe = self.r.pipeline(transaction=False)   # 3 commands, 1 network round-trip
                pipe.set(rs.heartbeat_key(WORKER_ID), json.dumps(beat), ex=config.HEARTBEAT_TTL_SEC)
                pipe.zadd(rs.WORKERS_LAST_SEEN_KEY, {WORKER_ID: now})
                pipe.hset(rs.worker_info_key(WORKER_ID), mapping={
                    "group": GROUP_ID, "filter": MY_FILTER, "status": "running",
                    "partitions": ",".join(map(str, partitions)),
                })
                pipe.execute()
            except Exception as e:  # noqa: BLE001 - e.g. Redis mid-failover; try again next beat
                hlog.warning("heartbeat failed: %s", e)
            self.stop_event.wait(config.HEARTBEAT_INTERVAL_SEC)

    # ─────────────────────────────── main loop ───────────────────────────────
    def run(self):
        p = offsets_partition_for_group(GROUP_ID, config.OFFSETS_TOPIC_PARTITIONS)
        log.info("=" * 70)
        log.info("🤖 %s  filter=%s  group=%s", WORKER_ID, MY_FILTER, GROUP_ID)
        log.info("   group offsets live in __consumer_offsets P%d -> that partition's leader is our coordinator", p)
        log.info("=" * 70)

        threading.Thread(target=self.heartbeat_loop, daemon=True, name="heartbeat").start()

        # subscribe() only registers interest. The real JoinGroup / SyncGroup with the
        # coordinator happens inside poll(), and the callbacks above fire from there.
        self.consumer.subscribe([config.TASKS_TOPIC], on_assign=self.on_assign,
                                on_revoke=self.on_revoke, on_lost=self.on_lost)
        while not self.stop_event.is_set():
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.warning("consumer error: %s", msg.error())
                continue
            try:
                self.handle(msg)
            except Exception:  # noqa: BLE001
                # A "poison" message (e.g. corrupt bytes) would otherwise block the
                # partition forever. Log it and move on. (Production: dead-letter topic.)
                log.exception("failed to process %s - skipping it", msg.key())
                self.commit(msg)
        self.shutdown()

    def handle(self, msg):
        where = f"{msg.topic()}[P{msg.partition()}]@{msg.offset()}"
        key = msg.key().decode()
        headers = {k: v.decode() for k, v in (msg.headers() or [])}
        flt = headers.get("filter")

        if flt != MY_FILTER:
            # Both groups read EVERY message; this tile belongs to the other group.
            # We skip it but still advance OUR group's offset.
            self.consumer.commit(message=msg, asynchronous=True)
            self.r.hincrby(rs.worker_info_key(WORKER_ID), "tiles_skipped", 1)
            return

        job_id, tile_index = key.rsplit(":", 1)
        log.info("🎨 picked %s  key=%s  (tile %s of job %s)", where, key, tile_index, job_id)

        started = time.perf_counter()
        img = imaging.decode(msg.value())                               # bytes -> pixels
        time.sleep(config.PROCESSING_DELAY_SEC)                         # demo: simulate heavy work
        out = imaging.apply_filter(img, flt)                            # OpenCV
        crop = tuple(int(v) for v in headers["crop"].split(","))
        result = imaging.encode_jpeg(imaging.crop_core(out, crop))     # drop halo -> bytes
        proc_ms = (time.perf_counter() - started) * 1000

        # Send the result. Same key (job:tile) so the master knows where it goes.
        # Headers carry provenance so the UI can show who processed what.
        report = {}
        self.producer.produce(
            config.RESULTS_TOPIC, key=key, value=result,
            headers=[("worker", WORKER_ID), ("filter", flt),
                     ("src", f"{msg.partition()}:{msg.offset()}"), ("proc_ms", f"{proc_ms:.0f}")],
            on_delivery=lambda err, m: report.update(err=err, msg=m),
        )
        self.producer.flush(30)        # wait for the broker's ack (acks=all)

        if not report or report["err"] is not None:
            # NOT acknowledged -> must NOT commit. Rewind to this offset and retry.
            log.error("❌ result for %s not acknowledged (%s) - rewinding to retry",
                      key, report.get("err"))
            self.consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
            time.sleep(1)
            return

        m = report["msg"]
        log.info("📤 sent result -> %s[P%d]@%d  (%.0f ms, acked by all in-sync replicas)",
                 m.topic(), m.partition(), m.offset(), proc_ms)
        self.commit(msg)

        pipe = self.r.pipeline(transaction=False)
        pipe.hincrby(rs.worker_info_key(WORKER_ID), "tiles_processed", 1)
        pipe.hincrby(rs.STATS_KEY, "tiles_processed", 1)   # 4 workers increment this concurrently: HINCRBY is atomic
        pipe.execute()

    def commit(self, msg):
        """Synchronously store 'next offset to read' for this partition at the
        group coordinator. Committed offset = last processed offset + 1."""
        try:
            self.consumer.commit(message=msg, asynchronous=False)
            clog.info("✔ committed %s[P%d] offset=%d (next to read)",
                      msg.topic(), msg.partition(), msg.offset() + 1)
        except KafkaException as e:
            # Typical: ILLEGAL_GENERATION / UNKNOWN_MEMBER_ID -> we were kicked out of
            # the group (a "zombie"). The coordinator refuses our stale commit. The new
            # owner re-processes this tile; the master drops the duplicate result.
            clog.warning("❌ commit REJECTED by coordinator: %s - partition has a new owner, "
                         "the tile will be reprocessed (master de-duplicates it)", e.args[0].name())

    # ────────────────────────────── shutdown ──────────────────────────────
    def stop(self, *_):
        log.info("🛑 SIGTERM received - stopping after the current tile")
        self.stop_event.set()

    def shutdown(self):
        # close() sends LeaveGroup -> the coordinator rebalances IMMEDIATELY instead
        # of waiting session.timeout.ms. (`docker kill` skips this, `docker stop` doesn't.)
        self.consumer.close()
        self.producer.flush(10)
        try:
            self.r.delete(rs.heartbeat_key(WORKER_ID))
            self.r.hset(rs.worker_info_key(WORKER_ID), mapping={"status": "stopped", "partitions": ""})
            rs.push_event(self.r, WORKER_ID, "left the group gracefully (SIGTERM)", level="warn")
        except Exception:  # noqa: BLE001
            pass
        log.info("👋 left consumer group %s cleanly", GROUP_ID)


if __name__ == "__main__":
    worker = Worker()
    signal.signal(signal.SIGTERM, worker.stop)   # docker stop
    signal.signal(signal.SIGINT, worker.stop)    # Ctrl+C
    worker.run()
