"""
setup_kafka.py - one-time Kafka setup. Runs in the kafka-init container, then exits.

  1. waits until both brokers have registered with the KRaft controller
  2. creates our 2 topics with the layout from config.TOPIC_LAYOUT
  3. prints who leads each partition and which broker coordinates each consumer group

Workers and master only start after this exits with code 0 (docker-compose
'service_completed_successfully'), so they never see a missing topic.

The script only talks to a BROKER; the broker forwards CreateTopics to the
controller, which records the new topic in the metadata log.
"""
import logging
import sys
import time

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from common import config
from common.kafka_internals import offsets_partition_for_group, partition_table

config.setup_logging()
log = logging.getLogger("kafka-setup")


def wait_for_brokers(admin):
    """Poll the cluster metadata every 2s until brokers 1 AND 2 are registered.
    (Docker only guarantees the containers started, not that Kafka is ready.)"""
    while True:
        try:
            md = admin.list_topics(timeout=5)          # cluster metadata from any broker
            registered = sorted(md.brokers)            # e.g. [1, 2]
            if set(config.BROKER_IDS) <= set(registered):
                for b in md.brokers.values():
                    log.info("   broker %s  at %s:%s", b.id, b.host, b.port)
                return
            log.info("⏳ brokers registered so far: %s (want %s)", registered, config.BROKER_IDS)
        except KafkaException as e:
            log.info("⏳ waiting for Kafka: %s", e.args[0].str())
        time.sleep(2)


def show_cluster(admin):
    """Print the cluster id. Note: in KRaft the 'controller' reported to clients is a
    random broker; the real controller (node 0) is never contacted by clients."""
    desc = admin.describe_cluster(request_timeout=10).result()
    log.info("cluster id: %s", desc.cluster_id)
    log.info("NOTE: in KRaft mode the 'controller' a client sees (broker %s) is just a",
             desc.controller.id if desc.controller else "?")
    log.info("      random broker, kept for old clients. The REAL controller is node 0.")
    log.info("      Check it:  docker exec kafka-broker-1 /opt/kafka/bin/kafka-metadata-quorum.sh "
             "--bootstrap-server kafka-broker-1:9092 describe --status")


def create_topics(admin):
    """Create image_tasks and image_results from config.TOPIC_LAYOUT.
    Example request: NewTopic("image_tasks", num_partitions=2, replica_assignment=[[1,2],[1,2]])
    Safe to re-run: TOPIC_ALREADY_EXISTS is ignored (idempotent)."""
    new_topics = [
        NewTopic(
            topic,
            num_partitions=len(layout),
            replica_assignment=layout,   # [[1,2],...]: first broker = leader, rest = followers
            config={
                # acks=all needs at least 1 in-sync copy -> writes continue if one broker is down
                "min.insync.replicas": "1",
                # Kafka deletes messages after 1 hour (tiles are temporary)
                "retention.ms": str(60 * 60 * 1000),
            },
        )
        for topic, layout in config.TOPIC_LAYOUT.items()
    ]
    # broker forwards this to the controller; we get one future (pending result) per topic
    for topic, future in admin.create_topics(new_topics, request_timeout=15).items():
        try:
            future.result()
            log.info("✅ created topic %s", topic)
        except KafkaException as e:
            if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                log.info("ℹ️  topic %s already exists", topic)
            else:
                raise


def wait_for_partitions(admin, topic, expected):
    """Topic creation is asynchronous: wait until every partition has a leader.
    Output: the partition table, e.g. [{"partition": 0, "leader": 1, "replicas": [1,2], "isr": [1,2]}, ...]"""
    while True:
        table = partition_table(admin.list_topics(topic=topic, timeout=5), topic)
        if len(table) == expected and all(row["leader"] >= 0 for row in table):
            return table
        time.sleep(1)


def print_partitions(topic, table):
    """Log one line per partition: leader, replicas, ISR, preferred leader."""
    log.info("topic %s", topic)
    for row in table:
        log.info("   P%d  leader=broker %d   replicas=%s   ISR=%s   (preferred leader = broker %d)",
                 row["partition"], row["leader"], row["replicas"], row["isr"], row["replicas"][0])


def show_coordinators(admin):
    """Print each consumer group's coordinator broker and check it against the formula.
    Output (log): workers-blur -> __consumer_offsets P1 -> leader broker 1 => coordinator = broker 1 ✓"""
    # Looking up a group makes Kafka find its coordinator. The very first lookup also
    # makes Kafka create __consumer_offsets, so retry until that's done.
    while True:
        try:
            futures = admin.describe_consumer_groups(config.ALL_GROUPS, request_timeout=10)
            groups = {g: f.result() for g, f in futures.items()}
            break
        except KafkaException as e:
            log.info("⏳ waiting for __consumer_offsets / coordinators: %s", e.args[0].str())
            time.sleep(2)

    offsets_table = wait_for_partitions(admin, "__consumer_offsets", config.OFFSETS_TOPIC_PARTITIONS)
    print_partitions("__consumer_offsets", offsets_table)
    leader_of = {row["partition"]: row["leader"] for row in offsets_table}

    log.info("group coordinators  (partition = abs(javaHash(group.id)) %% %d)",
             config.OFFSETS_TOPIC_PARTITIONS)
    for group_id, desc in groups.items():
        p = offsets_partition_for_group(group_id, config.OFFSETS_TOPIC_PARTITIONS)
        predicted = leader_of.get(p)                                   # our formula
        actual = desc.coordinator.id if desc.coordinator else None     # what Kafka says
        log.info("   %-20s -> __consumer_offsets P%d -> leader broker %s  =>  coordinator = broker %s %s",
                 group_id, p, predicted, actual, "✓" if predicted == actual else "✗ (mismatch!)")


def main():
    """Run the setup steps in order. AdminClient = Kafka's management client
    (creates topics, describes the cluster); it never sends or reads messages."""
    admin = AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP})
    log.info("=" * 70)
    log.info("Kafka setup - bootstrap %s", config.KAFKA_BOOTSTRAP)
    log.info("=" * 70)

    wait_for_brokers(admin)
    show_cluster(admin)
    create_topics(admin)
    for topic, layout in config.TOPIC_LAYOUT.items():
        print_partitions(topic, wait_for_partitions(admin, topic, len(layout)))
    show_coordinators(admin)
    log.info("✅ Kafka is ready")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        log.exception("❌ Kafka setup failed")
        sys.exit(1)   # exit code 1 -> docker compose won't start workers or master
