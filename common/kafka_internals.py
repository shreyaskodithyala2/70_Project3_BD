"""
Small Kafka helpers.

How does Kafka pick a consumer group's COORDINATOR broker?
    p = abs(javaStringHashCode(group.id)) % offsets.topic.num.partitions
    coordinator = the broker that leads partition p of __consumer_offsets
That broker stores the group's committed offsets and runs its rebalances.
We re-compute it here only to PRINT it, and to prove our two worker groups
land on different brokers.
"""


def java_string_hashcode(s: str) -> int:
    """Java's String.hashCode() (32-bit signed int with overflow)."""
    h = 0
    for ch in s:
        h = (31 * h + ord(ch)) & 0xFFFFFFFF
    return h - (1 << 32) if h >= (1 << 31) else h


def offsets_partition_for_group(group_id: str, offsets_partitions: int) -> int:
    """Which __consumer_offsets partition holds this group's offsets."""
    return abs(java_string_hashcode(group_id)) % offsets_partitions


def partition_table(cluster_metadata, topic):
    """confluent-kafka metadata -> [{partition, leader, replicas, isr}]."""
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
