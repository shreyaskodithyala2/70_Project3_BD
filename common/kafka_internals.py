"""
kafka_internals.py - two small Kafka helpers.

1. Which broker is a consumer group's COORDINATOR?
       p = abs(javaStringHashCode(group.id)) % offsets.topic.num.partitions
       coordinator = the broker that leads partition p of __consumer_offsets
   Kafka does this itself; we re-compute it only to print it and to show our
   two worker groups land on different brokers.
2. Turn Kafka's metadata object into a simple table (leader / replicas / ISR).
"""


def java_string_hashcode(s: str) -> int:
    """Java's String.hashCode() (what Kafka uses), as a signed 32-bit int.
    Example: java_string_hashcode("workers-blur") -> 894361215"""
    h = 0
    for ch in s:
        h = (31 * h + ord(ch)) & 0xFFFFFFFF   # keep it 32-bit, like a Java int
    return h - (1 << 32) if h >= (1 << 31) else h


def offsets_partition_for_group(group_id: str, offsets_partitions: int) -> int:
    """Which __consumer_offsets partition stores this group's committed offsets.
    Its leader broker is the group's coordinator.
    Example: ("workers-blur", 2) -> 1,  ("workers-blackwhite", 2) -> 0"""
    return abs(java_string_hashcode(group_id)) % offsets_partitions


def partition_table(cluster_metadata, topic):
    """Kafka metadata -> list of partitions for one topic.
    Input:  metadata from list_topics(), topic name
    Output: [{"partition": 0, "leader": 1, "replicas": [1, 2], "isr": [1, 2]}, ...]
            ([] if the topic doesn't exist yet)"""
    t = cluster_metadata.topics.get(topic)
    if t is None or t.error is not None:
        return []
    return [
        {
            "partition": p.id,
            "leader": p.leader,          # -1 = no leader right now
            "replicas": list(p.replicas),
            "isr": list(p.isrs),         # in-sync replicas
        }
        for p in sorted(t.partitions.values(), key=lambda p: p.id)
    ]
