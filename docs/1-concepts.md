# Concepts: Kafka, Redis, Docker, as used in this project

Each section explains one idea, shows where it lives in the code, and how to see it running.
At the end: the points where the original plan differed from how things actually work, and
interview questions.

---

## Part 1: Kafka

### 1.1 The pieces

| Piece | What it is | In this project |
|---|---|---|
| **Topic** | a named stream of messages | `image_tasks` (master → workers), `image_results` (workers → master) |
| **Partition** | a topic is split into ordered logs; partitions are the unit of parallelism | `image_tasks` has 2 → up to 2 workers *per group* can work at once |
| **Offset** | position of a message inside one partition (0, 1, 2…), assigned by the leader | printed by the producer: `-> image_tasks[P1] offset 42` |
| **Broker** | a server that stores partitions and serves clients | `kafka-broker-1`, `kafka-broker-2` |
| **Replica / leader / follower** | every partition has copies on several brokers. One copy is the **leader** (all reads/writes go to it), the others are **followers** that copy from it | replication factor 2: leader on broker 1, follower on broker 2 |
| **ISR** (in-sync replicas) | replicas that are fully caught up with the leader | normally `[1, 2]`; becomes `[2]` when broker 1 dies |
| **Controller** | the brain of the cluster: metadata, topic creation, leader election | `kafka-controller` (node 0) |
| **Consumer group** | consumers sharing one `group.id` split the partitions between them | `workers-blur`, `workers-blackwhite`, `master-assemblers` |
| **Group coordinator** | the broker that manages one group: membership, rebalancing, committed offsets | `workers-blur` → broker 1, `workers-blackwhite` → broker 2 |

### 1.2 KRaft and the controller
Old Kafka used ZooKeeper to store cluster metadata. **KRaft** (Kafka Raft) removes ZooKeeper:
metadata lives in an internal Kafka log called `__cluster_metadata`, managed by **controller** nodes.

The controller:
- accepts **create/delete topic** requests. Our `kafka-init` sends `CreateTopics` to a *broker*, and the broker **forwards** it to the controller. Producers, consumers and our admin script only ever talk to brokers.
- decides **which broker holds which replica** and **who is leader** (we pinned this ourselves with `replica_assignment` in [config.py](../common/config.py)).
- receives **heartbeats from brokers**. A broker that goes silent (~9s) is fenced, and the controller elects a new leader from the ISR for every partition it led.
- moves leadership back to the **preferred leader** (first broker in the replica list) once it recovers. We check every 30s instead of the default 300s.

We run **one** controller. If it dies, existing leaders keep serving, but nothing can change
(no new leaders, no new topics). Production uses **3 or 5** controllers: Raft needs a majority,
so 3 survive 1 failure. (Two would be worse than one: a majority of 2 is 2.)

See it: `kafka-metadata-quorum.sh ... describe --status` → `LeaderId: 0`.

### 1.3 Producing: key → partition → leader → acks
[master/tile_publisher.py](../master/tile_publisher.py)

1. Each tile becomes one message: **key** = `"<job_id>:<tile_index>"`, **value** = tile JPEG bytes, **headers** = `filter`, `crop`.
2. The producer's **partitioner** picks the partition: `murmur2(key) % 2`. Same key → always the same partition.
   (We set `partitioner: murmur2_random` because confluent-kafka's default uses CRC32. murmur2 matches Java Kafka.)
3. The message goes straight to that partition's **leader** (broker 1).
4. **`acks=all`**: the leader answers only after every replica in the **ISR** has the message too.
   The answer contains the **offset**, which we print.
5. `enable.idempotence=True`: if the producer retries after a network blip, the broker drops
   the duplicate (it tracks a producer id + sequence number).

**`min.insync.replicas=1`**: with `acks=all`, the write succeeds as long as at least 1 replica
is in sync. When broker 1 dies the ISR is `[2]`, and writes still succeed (we tested this).
With `min.insync.replicas=2` the topic would *refuse* writes rather than risk losing data on one
copy. That's the durability-vs-availability trade-off.

**Why the key matters:** the key is unique per tile, so a job's tiles spread over both
partitions (e.g. `P0: 9, P1: 7`). If the key were the image id, *every tile of an image would
land in the same partition*, so only one worker would process it.

### 1.4 Consuming: groups, coordinator, rebalancing
[worker/worker.py](../worker/worker.py), [master/result_assembler.py](../master/result_assembler.py)

- **Inside one group**, each partition goes to exactly one consumer (load balancing).
- **Different groups** are independent: each group receives **every** message and keeps its
  **own offsets** (fan-out). In the dashboard, `workers-blur` and `workers-blackwhite` show
  different lags for the same partition.

**Who is a group's coordinator?**
```
p = abs(javaHash(group.id)) % offsets.topic.num.partitions      (we set the latter to 2)
coordinator = the leader of partition p of __consumer_offsets
```
You can't assign a coordinator directly. We chose the group names so they hash to different
partitions: `workers-blur` → P1, `workers-blackwhite` → P0. That puts them on different brokers.
`kafka-init` prints this, and [common/kafka_internals.py](../common/kafka_internals.py) has the formula.

**Rebalancing** (what happens when a worker joins or leaves):
1. Every consumer heartbeats to the coordinator (`heartbeat.interval.ms = 3s`).
2. If one is silent for `session.timeout.ms = 10s`, or leaves politely (LeaveGroup on `docker stop`),
   the coordinator starts a **rebalance**.
3. Members re-join. One member (the group leader) computes the new assignment with the
   **assignor**, and the coordinator hands it out.
4. Our assignor is **cooperative-sticky**: only the partitions that need to move are revoked;
   everyone keeps working on the rest. The old "eager" protocol stopped the whole group.
5. The client calls our callbacks: `on_revoke` (giving a partition back), `on_assign` (got one), `on_lost` (we were kicked out).

### 1.5 Committed offsets: the "resume where it left off" mechanism
- After a worker finishes a tile **and** Kafka acked the result, it **commits** `offset + 1`
  ("the next offset to read") to the coordinator, which stores it in `__consumer_offsets`.
- The committed offset belongs to the **group + partition**, not to a worker.
- When a partition is (re)assigned, `on_assign` reads the committed offset, and consumption
  continues from there. Whoever gets the partition (a survivor, or the restarted worker)
  continues exactly where the last commit was. The worker stores nothing itself.

Tested: `docker kill worker-blur-1` → `worker-blur-2 ✅ ASSIGNED image_tasks[P0] -> resuming from committed offset 22`.

**At-least-once:** if a worker crashes *after* sending a result but *before* committing, the new
owner processes that tile again, so the master can receive a tile twice. The master's
`ZADD NX` drops the second copy (see Redis below). Exactly-once would need Kafka transactions.

**Zombies:** a frozen worker (`docker pause`) is kicked out. When it wakes up, its commit is
rejected (`UNKNOWN_MEMBER_ID`) because the group has moved on. This is how Kafka fences zombies.

---

## Part 2: Redis

### 2.1 Why Redis
An in-memory key-value store, so reads/writes take microseconds. It runs **one command at a time**
(single-threaded execution), so **every single command is atomic**. We use it for:
1. **Shared job state** (master writes, API reads)
2. **Worker heartbeats** (TTL keys)
3. **Cache** of finished images
4. **Feed + event log** for the UI

### 2.2 Data structures used ([common/redis_store.py](../common/redis_store.py))
| Structure | Key | Why this structure |
|---|---|---|
| **Hash** | `job:<id>` | one object with fields (`status`, `total_tiles`, `processed_tiles`…); `HINCRBY` updates one field atomically |
| **Sorted set** | `job:<id>:done` | set of finished tile indexes (score = arrival time). `ZADD NX` → **dedupe** |
| **Sorted set** | `jobs:feed` | job ids ordered by upload time → `ZREVRANGE` = newest first |
| **Sorted set** | `workers:last_seen` | every worker ever seen + last heartbeat time (remembers workers whose key expired) |
| **String + TTL** | `worker:<id>:heartbeat` | **exists = alive**; Redis deletes it 10s after the last refresh |
| **String + TTL** | `cache:<sha256>:<filter>` | final image bytes, expires after 24h |
| **List** | `events` | `LPUSH` + `LTRIM` = bounded "latest 200" log |

### 2.3 The race-condition story (`record_tile`)
```python
is_new = r.zadd(f"job:{id}:done", {tile_index: now}, nx=True)   # 1 = new, 0 = already had it
if is_new:
    r.hset(f"job:{id}:tiles", tile_index, tile_bytes)
    processed = r.hincrby(f"job:{id}", "processed_tiles", 1)   # returns the NEW value
    if processed == total: assemble()
```
- **Lost updates**: two clients doing `GET → +1 → SET` could both read 5 and both write 6.
  `HINCRBY` does the read-modify-write *inside* Redis as one command, so it can't happen.
- **Double counting**: Kafka can deliver a tile twice. `ZADD NX` only adds if the member is new,
  so the second copy returns 0 and is ignored.
- **Who reassembles?** `HINCRBY` returns a different number to every caller, so exactly one
  caller sees `processed == total`.

### 2.4 Replication and Sentinel failover
- **Replication** (`--replicaof redis-1 6379`): redis-2 and redis-3 continuously copy redis-1.
  It is **asynchronous**: the master acks a write before replicas have it, so a failover can lose the last few ms of writes.
- Replication alone does **not** fail over. If the master dies, the replicas just wait.
- **Sentinel** (3 processes) does the failover:
  1. each Sentinel PINGs the master. No reply for 5s → **+sdown** (subjectively down, "I think it's dead")
  2. **quorum** (2 of 3) agree → **+odown** (objectively down)
  3. the Sentinels elect one of themselves to lead the failover. It promotes the best replica (`REPLICAOF NO ONE`)
     and points the other replica at it → **+switch-master**
  4. when the old master returns, it's turned into a replica → **+convert-to-slave**
- **Clients ask Sentinel** "who is master?" (`Sentinel.master_for("mymaster")`). On a
  connection error they retry, ask again, and reach the new master. No code change, no restart.

Tested: `docker kill redis-1` → `redis-2` promoted ~6s later, app kept working.

### 2.5 Cache
- **Key = SHA-256 of the uploaded bytes + filter**. Same file → same hash; one changed byte → a
  different hash. The filter is part of the key because the same image blurred ≠ black & white.
- **Why not CRC32?** CRC is 32 bits and built to detect transmission errors, not to identify
  content. Among ~77k images there's a ~50% chance two collide, and a collision would serve someone
  the wrong picture. SHA-256 collisions are practically impossible.
- Limitation: a re-saved or resized copy of the same photo has different bytes → a cache miss.
  Matching *visually* identical images needs a perceptual hash (pHash).
- **Eviction:** `maxmemory 512mb` + `maxmemory-policy volatile-lru`: when memory is full, Redis
  removes the least-recently-used keys *that have a TTL* (cached images), never running jobs.

### 2.6 Heartbeats: Redis vs Kafka
Two independent detectors:

| | Redis heartbeat (ours) | Kafka consumer heartbeat (built in) |
|---|---|---|
| Who sends | worker's own thread, every 3s | the Kafka client's background thread, every 3s |
| Who watches | master's `health_monitor` | the group coordinator broker |
| Dead after | key TTL 10s | `session.timeout.ms` 10s |
| Result | dashboard shows DEAD, event logged | **rebalance**: partitions move to a live worker |

---

## Part 3: Docker

| Term | Meaning here |
|---|---|
| **Image** | a frozen filesystem + default settings. We build one: `image-pipeline:latest` ([Dockerfile](../Dockerfile)) |
| **Container** | a running process started from an image, isolated, with its own filesystem and network address |
| **Dockerfile** | recipe for the image: base Python → install requirements → copy code. Each step is a cached **layer**, so code changes rebuild in seconds |
| **docker-compose.yml** | declares all 16 containers, their env vars, ports, start order (`depends_on`) |
| **Network + DNS** | Compose puts all containers on one virtual network. A container reaches another **by name** (`kafka-broker-1:9092`, `redis-1:6379`, `http://master:5000`) |
| **Ports** | only `web` (8090→8080) and `master` (5050→5000) are published to your Mac. Kafka and Redis are internal only |
| **Env vars** | how one image becomes different containers (`WORKER_ID`, `WORKER_FILTER`) |
| **Volumes** | `./infra/redis/redis.conf` is mounted into the Redis containers |
| **Healthcheck / depends_on** | `web` waits until `master` answers `/healthz`. Workers wait until `kafka-init` exited successfully |

`docker stop` = SIGTERM, then SIGKILL after 10s (our worker catches SIGTERM and leaves the group).
`docker kill` = immediate SIGKILL (a crash). `docker pause` = freeze the process (simulates a hang).

---

## Part 4: Where the original plan differed from how things work

| You thought | How it actually works |
|---|---|
| "Kafka stores key:value:offset for each message" | ✅ Mostly. The **producer** sets key, value (and headers); the **partition leader** assigns the offset when it writes. Offsets count per **partition**, not per topic. |
| "Kafka routes by key automatically" | ✅ `hash(key) % partitions`. But the key must be **unique per tile**. A per-image key would send all tiles of an image to one partition. |
| "One broker holds the leaders, the other the replicas" | Not by default: Kafka **spreads** leaders across brokers to share load. We forced it with `replica_assignment` for the demo. With RF=2 on 2 brokers, **both** brokers hold a full copy of everything. |
| "Assign each broker as coordinator of one group" | Can't assign directly: coordinator = leader of `__consumer_offsets[abs(hash(group.id)) % N]`. We set N=2 and picked names that hash differently. |
| "Controller decides placement and handles create/delete" | ✅ Plus it detects dead **brokers** and elects new partition leaders. Our clients only talk to brokers, which forward requests to it. Run 3 in production. |
| "Coordinator manages members, offsets, rebalancing" | ✅ One detail: in the classic protocol the coordinator *triggers* the rebalance, but one **consumer** (the group leader) computes the assignment. Kafka 4's new protocol (KIP-848) moves that to the broker. |
| "Committed offsets let the worker resume; the worker doesn't need to do anything" | ✅ Exactly right. Offsets belong to the group+partition. A returning worker may get a **different** partition, which doesn't matter. Commit **after** processing → at-least-once → duplicates possible → master dedupes. |
| "2 consumer groups, both consume topic 1" | Each group gets **every** message. Without a rule, every tile would be processed twice. So: **one group per filter**, and workers skip tiles for the other filter. |
| "acks default is all" | ✅ (both Java Kafka ≥ 3.0 and librdkafka). It only means "all **in-sync** replicas", so combine it with `min.insync.replicas`. |
| "Redis replicas fail over internally, we just configure it" | Replication only copies data. Failover needs **Sentinel** (or Redis Cluster). Clients must connect through Sentinel to find the new master. |
| "Hash + sorted set prevent race conditions" | Single Redis commands are already atomic (single-threaded). The sorted set's real job is **dedupe** (`ZADD NX`) for Kafka's duplicate deliveries. `HINCRBY`'s return value picks the one finisher. |
| "Use CRC to identify images" | Use **SHA-256**, and include the **filter** in the cache key. |
| "confluent-kafka" | That's the **Python client** (wrapper around the C library librdkafka). The brokers are Apache Kafka (Java, `apache/kafka` image). |

**Other improvements over the original project**
- **Tile halo:** the original blurred each tile alone, which left visible seams on the 512px grid. We send
  each tile with 25 extra pixels from its neighbours, blur, then crop. No seams ([imaging.py](../common/imaging.py)).
- **Manual commits** instead of auto-commit (auto-commit can commit a tile that was never processed).
- Tiles are kept in Redis, not on the master's disk, so the master can restart mid-job.

---

## Part 5: Interview questions

**Walk me through what happens when a user uploads an image.**
Web forwards it to the master. The master SHA-256-hashes it and checks the Redis cache. On a miss
it splits the image into 512px tiles (with a 25px halo), writes job metadata to a Redis hash, and
produces one Kafka message per tile (key `job:tile`, `acks=all`). The two partitions are consumed by
two workers in the filter's consumer group. Each worker applies OpenCV, produces the result to
`image_results`, then commits its offset. The master consumes results, dedupes with `ZADD NX`,
counts with `HINCRBY`, and the tile that brings the count to total triggers reassembly. The final
image is cached in Redis with a 24h TTL.

**What happens if a worker crashes mid-job?**
Two detectors. Its Redis heartbeat key expires (10s TTL) and the master marks it dead. Kafka's
coordinator stops getting consumer heartbeats and, after `session.timeout.ms`, rebalances its
partition to the other worker in the group. That worker starts from the group's **committed
offset**, so nothing is lost. A tile it was half-way through is processed again, and the master
ignores the duplicate.

**Why commit after processing and not before?**
Commit-before = at-most-once: a crash after committing loses the tile. Commit-after =
at-least-once: a crash reprocesses it. Duplicates are harmless because the consumer is idempotent.

**How do you guarantee the image is assembled exactly once?**
`HINCRBY` is atomic and returns the new count to each caller, so only one call sees `== total`.
`ZADD NX` makes sure duplicates never increment the count.

**What if a Kafka broker dies?**
The controller notices the missing broker heartbeat, removes it from the ISR, and elects the
follower (in ISR) as the new leader. Producers and consumers refresh metadata and continue.
With `min.insync.replicas=1`, writes continue on one replica. When the broker returns it catches
up, rejoins the ISR, and the controller moves leadership back to the preferred leader.

**What if the Redis master dies?**
Sentinels detect it (5s), agree by quorum (2/3), and promote a replica. Clients connect through
Sentinel, so they find the new master on retry. Replication is async, so the last few
milliseconds of writes could be lost (`WAIT` can reduce that).

**Why two consumer groups on one topic?**
To show fan-out: each group gets every message and has its own offsets, failures and rebalances
(killing a blur worker doesn't touch the black & white group). The cost is that each group
also reads the other filter's tiles and skips them. In production I'd use one topic per filter.

**How would you scale it?**
More partitions on `image_tasks`, then more workers per group. Within a group, max parallelism =
number of partitions. Add a 3rd broker, 3 controllers, and `min.insync.replicas=2` for durability.

**Why Redis and not a database for job state?**
In-memory speed, atomic counters, TTLs for heartbeats and cache expiry, and sorted sets for the
feed. It's state that can be rebuilt. Long-term records would go in a database.

**Why is there a separate KRaft controller?**
It manages metadata (topics, partition leaders, broker liveness) with Raft, replacing ZooKeeper.
Keeping it separate from the brokers isolates metadata work from data traffic. You need an odd
number (3) for a fault-tolerant majority.
