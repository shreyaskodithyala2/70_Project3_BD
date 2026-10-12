"""
health_monitor.py - the master's monitor (background thread, every 3 seconds).

Builds the snapshot shown in the dashboard (GET /api/cluster):
  1. Workers  - Redis heartbeats: key gone -> worker is DEAD (logged + event)
  2. Kafka    - partition leaders / ISR, each group's coordinator, committed offset and lag
  3. Redis    - who is master right now (asks Sentinel)

Two independent ways a dead worker is noticed:
  - our Redis heartbeat (this file) tells the USER
  - Kafka's group coordinator moves the worker's partitions (we only read the result)
"""
import logging
import threading
import time

from confluent_kafka import Consumer, TopicPartition

from common import config
from common import redis_store as rs
from common.kafka_internals import offsets_partition_for_group, partition_table

log = logging.getLogger("monitor")


class HealthMonitor(threading.Thread):
    def __init__(self):
        """One read-only Kafka consumer per group, plus the snapshot the API serves."""
        super().__init__(daemon=True, name="health-monitor")
        self.r = rs.connect()
        # These consumers never subscribe, so they never join the group; they only read
        # its committed offsets. (AdminClient.describe_consumer_groups crashed the whole
        # process in librdkafka 2.15 when a coordinator broker died - found in testing.)
        self.readers = {
            group: Consumer({"bootstrap.servers": config.KAFKA_BOOTSTRAP, "group.id": group,
                             "enable.auto.commit": False})
            for group in config.ALL_GROUPS
        }
        self.snapshot = {"workers": [], "kafka": {}, "redis": {}}   # returned by GET /api/cluster
        self.was_alive = {}                                         # worker_id -> alive last time?

    def run(self):
        """Every 3s refresh the three parts of the snapshot. One failing check
        (e.g. a broker is down) doesn't stop the others."""
        while True:
            for name, check in (("workers", self.check_workers),
                                ("kafka", self.check_kafka),
                                ("redis", self.check_redis)):
                try:
                    self.snapshot[name] = check()
                except Exception as e:  # noqa: BLE001
                    log.warning("%s check failed: %s", name, e)
            time.sleep(config.MONITOR_INTERVAL_SEC)

    # ── 1. Redis heartbeats ────────────────────────────────────────────
    def check_workers(self):
        """Is each worker alive? Logs an event when one dies or comes back.
        Output: [{"worker_id": "worker-blur-1", "status": "alive"|"dead"|"stopped",
                  "ttl": 8, "partitions": "0", "tiles_processed": 20, ...}, ...]"""
        workers = []
        # workers:last_seen remembers every worker ever seen, even after its key expired
        for worker_id, last_seen in self.r.zrange(rs.WORKERS_LAST_SEEN_KEY, 0, -1, withscores=True):
            ttl = self.r.ttl(rs.heartbeat_key(worker_id))   # seconds left; -2 = key gone
            info = self.r.hgetall(rs.worker_info_key(worker_id))
            alive = ttl > 0
            status = "alive" if alive else ("stopped" if info.get("status") == "stopped" else "dead")

            if self.was_alive.get(worker_id) and not alive:          # alive -> dead
                text = (f"{worker_id} stopped gracefully" if status == "stopped" else
                        f"💀 {worker_id} is DEAD - no heartbeat for {time.time() - last_seen:.0f}s")
                log.warning(text)
                rs.push_event(self.r, "monitor", text, level="error")
            elif self.was_alive.get(worker_id) is False and alive:   # dead -> alive
                log.info("💚 %s is alive again", worker_id)
                rs.push_event(self.r, "monitor", f"{worker_id} is alive again")
            self.was_alive[worker_id] = alive

            workers.append({
                "worker_id": worker_id, "status": status, "ttl": max(ttl, 0),
                "last_seen_ago": round(time.time() - last_seen, 1),
                "group": info.get("group"), "filter": info.get("filter"),
                "partitions": info.get("partitions", "") if alive else "",
                "tiles_processed": int(info.get("tiles_processed", 0)),
            })
        return workers

    # ── 2. Kafka: leaders, coordinators, committed offsets ────────────
    def check_kafka(self):
        """Kafka's view for the dashboard.
        Output: {"brokers": [1, 2],
                 "topics": {"image_tasks": [{"partition": 0, "leader": 1, "isr": [1, 2]}, ...]},
                 "groups": [{"group": "workers-blur", "coordinator": 1,
                             "partitions": [{"partition": 0, "committed": 42, "end": 48,
                                             "lag": 6, "owner": "worker-blur-1"}, ...]}, ...]}"""
        any_reader = next(iter(self.readers.values()))
        md = any_reader.list_topics(timeout=3)          # cluster metadata from any live broker
        topics = {t: partition_table(md, t) for t in (config.TASKS_TOPIC, config.RESULTS_TOPIC)}
        offsets_leader = {row["partition"]: row["leader"]
                          for row in partition_table(md, "__consumer_offsets")}

        # which worker owns which partition, as reported in the workers' heartbeats
        owners = {(w["group"], int(p)): w["worker_id"]
                  for w in self.snapshot["workers"] for p in w["partitions"].split(",") if p}

        groups = []
        for group_id, reader in self.readers.items():
            topic = config.RESULTS_TOPIC if group_id == config.MASTER_GROUP else config.TASKS_TOPIC
            # coordinator = leader of __consumer_offsets[abs(hash(group)) % 2]
            p_offsets = offsets_partition_for_group(group_id, config.OFFSETS_TOPIC_PARTITIONS)
            group = {"group": group_id, "topic": topic, "offsets_partition": p_offsets,
                     "coordinator": offsets_leader.get(p_offsets), "partitions": []}
            try:
                tps = [TopicPartition(topic, row["partition"]) for row in topics[topic]]
                for tp in reader.committed(tps, timeout=3):                # asks the coordinator
                    _, end = reader.get_watermark_offsets(tp, timeout=3)   # asks the partition leader
                    group["partitions"].append({
                        "partition": tp.partition,
                        "committed": tp.offset,      # next offset the group will read (-1001 = none yet)
                        "end": end,                  # offset the next new message will get
                        "lag": end - max(tp.offset, 0),   # messages not processed yet
                        "owner": "master-assembler" if group_id == config.MASTER_GROUP
                                 else owners.get((group_id, tp.partition)),
                    })
            except Exception as e:  # noqa: BLE001 - e.g. coordinator is failing over
                group["error"] = str(e)
            groups.append(group)
        return {"brokers": sorted(md.brokers), "topics": topics, "groups": groups}

    # ── 3. Redis: who is master right now? ─────────────────────────────
    def check_redis(self):
        """Ask Sentinel for the current master and replicas.
        Output: {"master": "redis-1", "replicas": ["redis-2", "redis-3"]}
                (after a failover, e.g. master "redis-2")"""
        master = rs.sentinel().discover_master(config.REDIS_MASTER_NAME)
        replicas = rs.sentinel().discover_slaves(config.REDIS_MASTER_NAME)
        return {"master": master[0], "replicas": sorted(host for host, _ in replicas)}
