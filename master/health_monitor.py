"""
MASTER - HEALTH MONITOR (background thread, every 3 seconds)

Two independent ways of noticing a dead worker:

  1. REDIS heartbeats (our own mechanism)
       each worker:  SET worker:<id>:heartbeat ... EX 10   (every 3s)
       this thread:  is the key still there?  no -> the worker is DEAD
  2. KAFKA's group coordinator (built into Kafka)
       the consumer client heartbeats to the coordinator; if it goes quiet for
       session.timeout.ms the coordinator removes it and REBALANCES the group.
       Here we only READ the result: each group's committed offsets.

Redis tells the USER that a worker died. Kafka moves the worker's partitions
to someone else. They are independent: one is not built on the other.
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
        super().__init__(daemon=True, name="health-monitor")
        self.r = rs.connect()
        # One plain Consumer per group, used ONLY to read that group's committed
        # offsets. It never calls subscribe(), so it never joins the group or takes
        # partitions. (We don't use AdminClient.describe_consumer_groups: in
        # librdkafka 2.15 it crashes the whole process if the coordinator broker
        # dies mid-request. We hit that while testing a broker failure.)
        self.readers = {
            group: Consumer({"bootstrap.servers": config.KAFKA_BOOTSTRAP, "group.id": group,
                             "enable.auto.commit": False})
            for group in config.ALL_GROUPS
        }
        self.snapshot = {"workers": [], "kafka": {}, "redis": {}}   # served by GET /api/cluster
        self.was_alive = {}                                         # worker_id -> bool

    def run(self):
        while True:
            for name, check in (("workers", self.check_workers),
                                ("kafka", self.check_kafka),
                                ("redis", self.check_redis)):
                try:
                    self.snapshot[name] = check()
                except Exception as e:  # noqa: BLE001 - e.g. a broker is down right now
                    log.warning("%s check failed: %s", name, e)
            time.sleep(config.MONITOR_INTERVAL_SEC)

    # ── 1. Redis heartbeats ────────────────────────────────────────────
    def check_workers(self):
        workers = []
        # every worker that ever sent a heartbeat (sorted set: id -> last-seen time)
        for worker_id, last_seen in self.r.zrange(rs.WORKERS_LAST_SEEN_KEY, 0, -1, withscores=True):
            ttl = self.r.ttl(rs.heartbeat_key(worker_id))   # -2 = key no longer exists
            info = self.r.hgetall(rs.worker_info_key(worker_id))
            alive = ttl > 0
            status = "alive" if alive else ("stopped" if info.get("status") == "stopped" else "dead")

            if self.was_alive.get(worker_id) and not alive:
                text = (f"{worker_id} stopped gracefully" if status == "stopped" else
                        f"💀 {worker_id} is DEAD - no heartbeat for {time.time() - last_seen:.0f}s")
                log.warning(text)
                rs.push_event(self.r, "monitor", text, level="error")
            elif self.was_alive.get(worker_id) is False and alive:
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

    # ── 2. Kafka: partition leaders, coordinators, committed offsets ───
    def check_kafka(self):
        any_reader = next(iter(self.readers.values()))
        md = any_reader.list_topics(timeout=3)          # metadata from any live broker
        topics = {t: partition_table(md, t) for t in (config.TASKS_TOPIC, config.RESULTS_TOPIC)}
        offsets_leader = {row["partition"]: row["leader"]
                          for row in partition_table(md, "__consumer_offsets")}

        # who currently owns which partition, as reported by the workers' heartbeats
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
                for tp in reader.committed(tps, timeout=3):        # asks the coordinator
                    _, end = reader.get_watermark_offsets(tp, timeout=3)   # asks the leader
                    group["partitions"].append({
                        "partition": tp.partition,
                        "committed": tp.offset,      # next offset this group will read (-1001 = none yet)
                        "end": end,                  # offset the next new message will get
                        "lag": end - max(tp.offset, 0),
                        "owner": "master-assembler" if group_id == config.MASTER_GROUP
                                 else owners.get((group_id, tp.partition)),
                    })
            except Exception as e:  # noqa: BLE001 - e.g. coordinator is failing over
                group["error"] = str(e)
            groups.append(group)
        return {"brokers": sorted(md.brokers), "topics": topics, "groups": groups}

    # ── 3. Redis replication: who is master right now? (asks Sentinel) ─
    def check_redis(self):
        master = rs.sentinel().discover_master(config.REDIS_MASTER_NAME)
        replicas = rs.sentinel().discover_slaves(config.REDIS_MASTER_NAME)
        return {"master": master[0], "replicas": sorted(host for host, _ in replicas)}
