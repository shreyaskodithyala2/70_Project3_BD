"""
MASTER - PRODUCER ROLE: publishes tiles to image_tasks.

Which part of Kafka handles this?  Only the PARTITION LEADERS.
  1. The producer asks any broker for metadata: "image_tasks P0 -> leader broker 1,
     P1 -> leader broker 1".
  2. For each message it picks a partition from the KEY:
        partition = murmur2(key) % number_of_partitions
  3. It sends the message straight to that partition's leader.
  4. acks=all: the leader replies only after the in-sync follower (broker 2) has
     copied the message too. The reply contains the partition and the OFFSET the
     leader assigned, which we print in the delivery callback.
The group coordinator and the controller take no part in producing.
"""
import logging
import threading

from confluent_kafka import Producer

from common import config

log = logging.getLogger("producer")


class TilePublisher:
    def __init__(self):
        self.producer = Producer({
            "bootstrap.servers": config.KAFKA_BOOTSTRAP,
            "client.id": "master-producer",
            "acks": "all",                  # wait for leader + all in-sync replicas
            "enable.idempotence": True,     # retries can't create duplicates
            "partitioner": "murmur2_random",  # same key->partition hashing as Java Kafka
        })

    def publish(self, job_id, flt, tiles):
        """Send all tiles of a job and wait until Kafka acknowledged every one.
        Returns how many tiles went to each partition, e.g. {0: 9, 1: 7}."""
        per_partition, errors = {}, []
        lock = threading.Lock()  # several uploads can be publishing at the same time

        def on_delivery(err, msg):
            # Runs once the partition leader has answered.
            with lock:
                if err is not None:
                    errors.append(err)
                    log.error("❌ %s not delivered: %s", msg.key().decode(), err)
                    return
                per_partition[msg.partition()] = per_partition.get(msg.partition(), 0) + 1
            log.info("✔ key=%-16s -> %s[P%d] offset %d (acked by all in-sync replicas)",
                     msg.key().decode(), msg.topic(), msg.partition(), msg.offset())

        for tile in tiles:
            self.producer.produce(
                config.TASKS_TOPIC,
                key=f"{job_id}:{tile.index}",        # unique per tile -> spreads over partitions
                value=tile.data,                     # the tile's JPEG bytes
                headers=[("filter", flt),            # small metadata travels in headers
                         ("crop", ",".join(map(str, tile.crop)))],
                on_delivery=on_delivery,
            )
            self.producer.poll(0)   # let callbacks for already-acked messages run

        not_acked = self.producer.flush(30)   # block until every tile is acknowledged
        if not_acked or errors:
            raise RuntimeError(f"{not_acked} tiles unacknowledged, {len(errors)} failed")
        log.info("✅ job %s: %d tiles published, split %s", job_id, len(tiles),
                 {f"P{p}": n for p, n in sorted(per_partition.items())})
        return per_partition
