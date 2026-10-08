"""
kafka-init  (runs once, then exits)

  1. waits until both brokers have registered with the KRaft controller
  2. asks the cluster to create our topics with an explicit replica layout
  3. prints who leads each partition, and which broker coordinates each group

Who does the work? This script only talks to a BROKER. In KRaft mode the broker
FORWARDS CreateTopics to the active CONTROLLER (node 0). The controller writes the
new topic, its partitions and their leaders into the metadata log
(__cluster_metadata), and every broker picks the change up from there. Clients
never talk to the controller directly.
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
    while True:
        try:
            md = admin.list_topics(timeout=5)
            registered = sorted(md.brokers)
            if set(config.BROKER_IDS) <= set(registered):
                for b in md.brokers.values():
                    log.info("   broker %s  at %s:%s", b.id, b.host, b.port)
                return
            log.info("⏳ brokers registered so far: %s (want %s)", registered, config.BROKER_IDS)
        except KafkaException as e:
            log.info("⏳ waiting for Kafka: %s", e.args[0].str())
        time.sleep(2)


def show_cluster(admin):
    desc = admin.describe_cluster(request_timeout=10).result()
    log.info("cluster id: %s", desc.cluster_id)
    log.info("NOTE: in KRaft mode the 'controller' a client sees (broker %s) is just a",
             desc.controller.id if desc.controller else "?")
    log.info("      random broker, kept for old clients. The REAL controller is node 0.")
    log.info("      Check it:  docker exec kafka-broker-1 /opt/kafka/bin/kafka-metadata-quorum.sh "
             "--bootstrap-server kafka-broker-1:9092 describe --status")


def create_topics(admin):
    new_topics = [
        NewTopic(
            topic,
            num_partitions=len(layout),
            # replica_assignment[p] = broker ids holding partition p.
            # First id = preferred leader, the rest = followers.
            replica_assignment=layout,
            config={
                # acks=all waits for every IN-SYNC replica. With min ISR = 1 the
                # topic keeps accepting writes when one broker is down
                # (availability over durability; 2 would refuse writes instead).
                "min.insync.replicas": "1",
                # tiles are transient - keep them for 1 hour
                "retention.ms": str(60 * 60 * 1000),
            },
        )
        for topic, layout in config.TOPIC_LAYOUT.items()
    ]
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
    """Topic creation is asynchronous: wait until every partition has a leader."""
    while True:
        table = partition_table(admin.list_topics(topic=topic, timeout=5), topic)
        if len(table) == expected and all(row["leader"] >= 0 for row in table):
            return table
        time.sleep(1)


def print_partitions(topic, table):
    log.info("topic %s", topic)
    for row in table:
        log.info("   P%d  leader=broker %d   replicas=%s   ISR=%s   (preferred leader = broker %d)",
                 row["partition"], row["leader"], row["replicas"], row["isr"], row["replicas"][0])


def show_coordinators(admin):
    # Describing a group makes the broker look up the group's coordinator
    # (a FindCoordinator request). The very first lookup also makes Kafka create
    # the internal __consumer_offsets topic, so we retry until that is done.
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
        predicted = leader_of.get(p)
        actual = desc.coordinator.id if desc.coordinator else None
        log.info("   %-20s -> __consumer_offsets P%d -> leader broker %s  =>  coordinator = broker %s %s",
                 group_id, p, predicted, actual, "✓" if predicted == actual else "✗ (mismatch!)")


def main():
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
        sys.exit(1)
