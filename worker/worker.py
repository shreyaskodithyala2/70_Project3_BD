"""
worker.py - a worker container: one consumer in a Kafka consumer group.

For each tile:
    image_tasks ──poll──▶ decode ▶ filter ▶ crop halo ▶ encode ──produce──▶ image_results
                                                                  │
                                       commit offset ◀── only after Kafka acked the result

Settings from docker-compose.yml:
    WORKER_ID      e.g. worker-blur-1   (also the Kafka client.id)
    WORKER_FILTER  blur | blackwhite    (decides which consumer group it joins)

Also runs a heartbeat thread that writes "I'm alive" to Redis every 3s.

Delivery guarantee: AT-LEAST-ONCE. The offset is committed only after the result is
safely in Kafka. A crash in between means the tile is processed again by the next
owner of the partition; the master ignores the duplicate (ZADD NX).
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
GROUP_ID = config.WORKER_GROUPS[MY_FILTER]     # blur -> "workers-blur"

log = logging.getLogger("task")
rlog = logging.getLogger("rebalance")
clog = logging.getLogger("commit")
hlog = logging.getLogger("heartbeat")


class Worker:
    def __init__(self):
        """Connect to Redis and create the Kafka consumer (tiles in) and producer (results out)."""
        self.stop_event = threading.Event()
        self.assigned = set()               # partitions we own right now, e.g. {0}
        self.lock = threading.Lock()        # heartbeat thread reads self.assigned too

        rs.wait_for_redis(log)
        self.r = rs.connect()

        # CONSUMER: reads tiles from image_tasks
        self.consumer = Consumer({
            "bootstrap.servers": config.KAFKA_BOOTSTRAP,
            "group.id": GROUP_ID,                  # same group.id => partitions are split between us
            "client.id": WORKER_ID,
            "enable.auto.commit": False,           # we commit ourselves, after the result is sent
            "auto.offset.reset": "earliest",       # new group, no commit yet -> start from the oldest
            # on a rebalance only the partitions that MOVE are revoked; the rest keep working
            "partition.assignment.strategy": "cooperative-sticky",
            # heartbeat to the coordinator every 3s; silent for 10s -> kicked out -> rebalance
            "session.timeout.ms": config.KAFKA_SESSION_TIMEOUT_MS,
            "heartbeat.interval.ms": config.KAFKA_HEARTBEAT_INTERVAL_MS,
            "max.poll.interval.ms": 60000,         # no poll() for 60s -> considered stuck
        })

        # PRODUCER: writes processed tiles to image_results
        self.producer = Producer({
            "bootstrap.servers": config.KAFKA_BOOTSTRAP,
            "client.id": f"{WORKER_ID}-producer",
            "acks": "all",                  # leader waits for all in-sync replicas
            "enable.idempotence": True,     # producer retries can't create duplicates
        })

    # ───────── rebalance callbacks (Kafka calls these from inside consumer.poll()) ─────────
    def on_assign(self, consumer, partitions):
        """The coordinator gave us partitions.
        Input: list of TopicPartition, e.g. [image_tasks P0]
        We read where to start from the group's COMMITTED offset. It belongs to the
        group, not the worker, so whoever gets a partition continues where the last
        owner stopped. Log: 'ASSIGNED image_tasks[P0] -> resuming from committed offset 22'"""
        try:
            committed = consumer.committed(partitions, timeout=10)   # asks the coordinator
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
        """Partitions are being handed to another member (normal rebalance).
        Nothing to save: we already commit after every tile."""
        for tp in partitions:
            rlog.info("↩️  REVOKED %s[P%d] (handing it to another group member)", tp.topic, tp.partition)
            rs.push_event(self.r, WORKER_ID, f"revoked {tp.topic}[P{tp.partition}]", level="warn")
        with self.lock:
            self.assigned -= {tp.partition for tp in partitions}

    def on_lost(self, consumer, partitions):
        """Partitions taken away WITHOUT a revoke: we missed the session timeout
        (e.g. container paused). Another worker owns them now; our commits will fail."""
        for tp in partitions:
            rlog.warning("💥 LOST %s[P%d] - we were kicked out of the group (session timeout)",
                         tp.topic, tp.partition)
            rs.push_event(self.r, WORKER_ID, f"LOST {tp.topic}[P{tp.partition}] (session timeout)", level="error")
        with self.lock:
            self.assigned -= {tp.partition for tp in partitions}

    # ───────────────────── Redis heartbeat (background thread) ─────────────────────
    def heartbeat_loop(self):
        """Every 3s write 'I'm alive' to Redis:
            SET  worker:<id>:heartbeat {...} EX 10   (resets the 10s timer)
            ZADD workers:last_seen                   (remember this worker exists)
            HSET worker:<id>:info                    (group, filter, partitions)
        If the worker dies, nobody resets the timer, Redis deletes the key after 10s,
        and the master's monitor marks the worker DEAD."""
        while not self.stop_event.is_set():
            try:
                with self.lock:
                    partitions = sorted(self.assigned)
                now = time.time()
                beat = {"worker_id": WORKER_ID, "group": GROUP_ID, "filter": MY_FILTER,
                        "partitions": partitions, "ts": now}
                pipe = self.r.pipeline(transaction=False)   # 3 commands in 1 network round-trip
                pipe.set(rs.heartbeat_key(WORKER_ID), json.dumps(beat), ex=config.HEARTBEAT_TTL_SEC)
                pipe.zadd(rs.WORKERS_LAST_SEEN_KEY, {WORKER_ID: now})
                pipe.hset(rs.worker_info_key(WORKER_ID), mapping={
                    "group": GROUP_ID, "filter": MY_FILTER, "status": "running",
                    "partitions": ",".join(map(str, partitions)),
                })
                pipe.execute()
            except Exception as e:  # noqa: BLE001 - e.g. Redis mid-failover; retry next beat
                hlog.warning("heartbeat failed: %s", e)
            self.stop_event.wait(config.HEARTBEAT_INTERVAL_SEC)

    # ─────────────────────────────── main loop ───────────────────────────────
    def run(self):
        """Start the heartbeat thread, join the consumer group, then poll tiles until stopped."""
        p = offsets_partition_for_group(GROUP_ID, config.OFFSETS_TOPIC_PARTITIONS)
        log.info("=" * 70)
        log.info("🤖 %s  filter=%s  group=%s", WORKER_ID, MY_FILTER, GROUP_ID)
        log.info("   group offsets live in __consumer_offsets P%d -> that partition's leader is our coordinator", p)
        log.info("=" * 70)

        threading.Thread(target=self.heartbeat_loop, daemon=True, name="heartbeat").start()

        # subscribe() only registers interest; joining the group and getting partitions
        # happens inside poll(), which is also where the callbacks above fire.
        self.consumer.subscribe([config.TASKS_TOPIC], on_assign=self.on_assign,
                                on_revoke=self.on_revoke, on_lost=self.on_lost)
        while not self.stop_event.is_set():
            msg = self.consumer.poll(1.0)          # wait up to 1s for the next tile
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.warning("consumer error: %s", msg.error())
                continue
            try:
                self.handle(msg)
            except Exception:  # noqa: BLE001
                # A corrupt ("poison") message would block the partition forever:
                # log it, commit, move on. (Production: send it to a dead-letter topic.)
                log.exception("failed to process %s - skipping it", msg.key())
                self.commit(msg)
        self.shutdown()

    def handle(self, msg):
        """Process one tile message.
        Input:  Kafka message
                key     = "a3f9c1:5"  (job id : tile index)
                value   = padded tile JPEG bytes
                headers = filter="blur", crop="25,25,512,512"
        Output: processed tile sent to image_results (same key), then offset committed."""
        where = f"{msg.topic()}[P{msg.partition()}]@{msg.offset()}"
        key = msg.key().decode()
        headers = {k: v.decode() for k, v in (msg.headers() or [])}
        flt = headers.get("filter")

        if flt != MY_FILTER:
            # Both groups receive every tile; this one is for the other group.
            # Skip it, but still move OUR group's offset forward.
            self.consumer.commit(message=msg, asynchronous=True)
            self.r.hincrby(rs.worker_info_key(WORKER_ID), "tiles_skipped", 1)
            return

        job_id, tile_index = key.rsplit(":", 1)
        log.info("🎨 picked %s  key=%s  (tile %s of job %s)", where, key, tile_index, job_id)

        started = time.perf_counter()
        img = imaging.decode(msg.value())                               # bytes -> pixels
        time.sleep(config.PROCESSING_DELAY_SEC)                         # demo: simulate heavy work
        out = imaging.apply_filter(img, flt)                            # OpenCV blur / grayscale
        crop = tuple(int(v) for v in headers["crop"].split(","))
        result = imaging.encode_jpeg(imaging.crop_core(out, crop))     # drop the halo -> bytes
        proc_ms = (time.perf_counter() - started) * 1000

        # Send the result with the same key so the master knows which tile it is.
        # Headers say who processed it (shown in the UI).
        report = {}
        self.producer.produce(
            config.RESULTS_TOPIC, key=key, value=result,
            headers=[("worker", WORKER_ID), ("filter", flt),
                     ("src", f"{msg.partition()}:{msg.offset()}"), ("proc_ms", f"{proc_ms:.0f}")],
            on_delivery=lambda err, m: report.update(err=err, msg=m),
        )
        self.producer.flush(30)        # wait for Kafka's ack (acks=all)

        if not report or report["err"] is not None:
            # Not acknowledged -> must NOT commit. Rewind to this tile and retry.
            log.error("❌ result for %s not acknowledged (%s) - rewinding to retry",
                      key, report.get("err"))
            self.consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
            time.sleep(1)
            return

        m = report["msg"]
        log.info("📤 sent result -> %s[P%d]@%d  (%.0f ms, acked by all in-sync replicas)",
                 m.topic(), m.partition(), m.offset(), proc_ms)
        self.commit(msg)               # only now: "this tile is done"

        pipe = self.r.pipeline(transaction=False)
        pipe.hincrby(rs.worker_info_key(WORKER_ID), "tiles_processed", 1)
        pipe.hincrby(rs.STATS_KEY, "tiles_processed", 1)   # all 4 workers do this at once; HINCRBY is atomic
        pipe.execute()

    def commit(self, msg):
        """Tell the group coordinator 'done up to here' for this partition.
        Committed offset = this message's offset + 1 (the NEXT one to read).
        Example: finished offset 42 -> commit 43."""
        try:
            self.consumer.commit(message=msg, asynchronous=False)
            clog.info("✔ committed %s[P%d] offset=%d (next to read)",
                      msg.topic(), msg.partition(), msg.offset() + 1)
        except KafkaException as e:
            # e.g. UNKNOWN_MEMBER_ID: we were kicked out of the group (a "zombie").
            # The coordinator rejects our stale commit; the new owner redoes the tile
            # and the master drops the duplicate.
            clog.warning("❌ commit REJECTED by coordinator: %s - partition has a new owner, "
                         "the tile will be reprocessed (master de-duplicates it)", e.args[0].name())

    # ────────────────────────────── shutdown ──────────────────────────────
    def stop(self, *_):
        """Signal handler (docker stop / Ctrl+C): finish the current tile, then exit."""
        log.info("🛑 SIGTERM received - stopping after the current tile")
        self.stop_event.set()

    def shutdown(self):
        """Leave cleanly: close() sends LeaveGroup, so Kafka rebalances in ~3s instead of
        waiting 10s. Also removes the heartbeat so the UI shows 'stopped', not 'dead'.
        (docker kill skips all of this; docker stop runs it.)"""
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
