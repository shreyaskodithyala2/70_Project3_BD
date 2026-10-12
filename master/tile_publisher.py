"""
tile_publisher.py - the master's PRODUCER role: sends tiles to the image_tasks topic.

How a tile reaches Kafka:
  1. The producer gets the cluster map from any broker (who leads each partition).
  2. It picks the partition from the message KEY: murmur2(key) % 2.
  3. It sends the message straight to that partition's leader (broker 1).
  4. acks=all: the leader replies only after the follower (broker 2) has a copy too.
     The reply contains the partition and the offset, which we log.
Only partition leaders are involved; not the controller or the group coordinator.
"""
import logging
import threading

from confluent_kafka import Producer

from common import config

log = logging.getLogger("producer")


class TilePublisher:
    def __init__(self):
        """Create one Kafka producer, shared by all upload requests."""
        self.producer = Producer({
            "bootstrap.servers": config.KAFKA_BOOTSTRAP,
            "client.id": "master-producer",
            "acks": "all",                     # wait for leader + all in-sync replicas
            "enable.idempotence": True,        # network retries can't create duplicates
            "partitioner": "murmur2_random",   # key -> partition, same hash as Java Kafka
        })

    def publish(self, job_id, flt, tiles):
        """Send every tile of one job to image_tasks and wait until all are acknowledged.
        Input:  job_id "a3f9c1", filter "blur", list of Tile (from imaging.split_into_tiles)
        Output: tiles per partition, e.g. {0: 9, 1: 7}
        Raises: RuntimeError if any tile was not acknowledged.

        Each message:  key = "a3f9c1:5"  value = tile JPEG  headers = filter, crop"""
        per_partition, errors = {}, []
        lock = threading.Lock()  # several uploads can publish at the same time

        def on_delivery(err, msg):
            """Called once the partition leader answers for one tile."""
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
                key=f"{job_id}:{tile.index}",        # unique per tile -> tiles spread over both partitions
                value=tile.data,                     # padded tile JPEG bytes
                headers=[("filter", flt),            # which filter to apply
                         ("crop", ",".join(map(str, tile.crop)))],   # where the core is inside the halo
                on_delivery=on_delivery,
            )
            self.producer.poll(0)   # run callbacks for tiles already acknowledged

        not_acked = self.producer.flush(30)   # block until every tile is acknowledged
        if not_acked or errors:
            raise RuntimeError(f"{not_acked} tiles unacknowledged, {len(errors)} failed")
        log.info("✅ job %s: %d tiles published, split %s", job_id, len(tiles),
                 {f"P{p}": n for p, n in sorted(per_partition.items())})
        return per_partition
