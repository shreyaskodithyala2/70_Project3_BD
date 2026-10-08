# Distributed Image Processing Pipeline (Kafka · Redis · Docker)

Upload an image, pick a filter (**Blur** or **Black & White**), and the image is cut into
512×512 tiles that are processed in parallel by worker containers. Then it is stitched back
together. Everything runs on one machine as 16 Docker containers.

```
 Browser ──▶ web (Flask :8090) ──HTTP──▶ master (Flask API)
                                          │  1. SHA-256 the image → Redis cache hit? return instantly
                                          │  2. split into tiles, write job state to Redis
                                          │  3. produce tiles ─────────────┐
                                          │                                ▼
                                          │                  ┌──── Kafka (KRaft) ────────────┐
                                          │                  │ controller (node 0)           │
                                          │                  │ broker 1: leader of all parts │
                                          │                  │ broker 2: follower replicas   │
                                          │                  │ topic image_tasks   (2 parts) │
                                          │                  │ topic image_results (1 part)  │
                                          │                  └───────────────────────────────┘
                                          │                     │ image_tasks       ▲ image_results
                                          │                     ▼                   │
                                          │      group workers-blur:        worker-blur-1 (P0), worker-blur-2 (P1)
                                          │      group workers-blackwhite:  worker-bw-1  (P0),  worker-bw-2  (P1)
                                          │                                         │
                                          │  4. consume results ◀───────────────────┘
                                          │  5. last tile? reassemble → store in Redis cache
                                          ▼
                     Redis: redis-1 (master) + redis-2, redis-3 (replicas) + 3 Sentinels (failover)
```

| Container | Role |
|---|---|
| `kafka-controller` | KRaft controller: cluster metadata, creates topics, elects partition leaders |
| `kafka-broker-1`, `kafka-broker-2` | store the partitions; broker 1 leads them, broker 2 holds the replicas |
| `kafka-init` | runs once: creates the topics, prints leaders and group coordinators, exits |
| `redis-1` / `redis-2`, `redis-3` | Redis master / its two replicas |
| `redis-sentinel-1..3` | watch the master, promote a replica if it dies |
| `worker-blur-1/2`, `worker-bw-1/2` | Kafka consumers that run the OpenCV filters (2 groups × 2 workers) |
| `master` | API + Kafka producer + Kafka consumer + health monitor |
| `web` | the website (port **8090**), forwards calls to the master |

**Docs for learning:**
- [docs/1-concepts.md](docs/1-concepts.md): how Kafka, Redis and Docker work here, what was different from the original plan, and interview Q&A
- [docs/2-code-walkthrough.md](docs/2-code-walkthrough.md): follows one upload through the code, file by file

---

## Run it step by step

You need Docker Desktop running. All commands are run from this folder.

### 0. Build the image (once, and after every code change)
```bash
docker compose build
```
One image (`image-pipeline:latest`) holds all the Python code. Every Python container is
started from it with a different command.

### 1. Kafka: controller first, then the two brokers
```bash
docker compose up -d kafka-controller
docker compose logs kafka-controller | grep "Kafka Server started"

docker compose up -d kafka-broker-1 kafka-broker-2
docker compose logs kafka-broker-1 | grep "registered broker"
```
Each broker registers with the controller. Check who the active controller is:
```bash
docker exec kafka-broker-1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kafka-broker-1:9092 describe --status
# LeaderId: 0  → node 0 (kafka-controller) is the active controller
# CurrentObservers: ids 1 and 2 → the brokers
```

### 2. Create the topics
```bash
docker compose up kafka-init
```
The output shows:
- `image_tasks` P0 and P1, and `image_results` P0: **leader = broker 1**, replicas `[1, 2]`, ISR `[1, 2]`
- the coordinator of each consumer group: `workers-blur` → one broker, `workers-blackwhite` → the other

Check it yourself with the Kafka CLI:
```bash
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-broker-1:9092 --describe
```

### 3. Redis: master, replicas, sentinels
```bash
docker compose up -d redis-1 redis-2 redis-3 redis-sentinel-1 redis-sentinel-2 redis-sentinel-3
docker exec redis-1 redis-cli INFO replication          # role:master, connected_slaves:2
docker exec redis-sentinel-1 redis-cli -p 26379 SENTINEL get-master-addr-by-name mymaster
```

### 4. Workers
```bash
docker compose up -d worker-blur-1 worker-blur-2 worker-bw-1 worker-bw-2
docker compose logs worker-blur-1 worker-blur-2 | grep rebalance
# worker-blur-1  ✅ ASSIGNED image_tasks[P0] -> resuming from ...
# worker-blur-2  ✅ ASSIGNED image_tasks[P1] -> resuming from ...
```
Each group splits the 2 partitions between its 2 workers.

### 5. Master and web
```bash
docker compose up -d master web
```
Open **http://localhost:8090**, upload one of the images in [samples/](samples/), and watch the
feed and the cluster panel on the right.

> Shortcut: `docker compose up -d --build` starts everything in the right order
> (it follows the `depends_on` rules in docker-compose.yml).

### Watch the logs while you upload
```bash
docker compose logs -f master          # [producer] tile -> partition/offset, [assembler] tile n/N
docker compose logs -f worker-blur-1   # [task] picked ..., [commit] committed offset ...
```

### Stop
```bash
docker compose stop        # stop, keep the data
docker compose down        # stop and delete the containers (and all Kafka/Redis data)
```

---

## Failure demos (try these while a large image is processing)

Upload `samples/large_4096x3072.jpg` (48 tiles, ~25 s), then:

| Command | What you'll see |
|---|---|
| `docker kill worker-blur-1` | **Crash.** After ~10s the master logs `💀 worker-blur-1 is DEAD` (Redis TTL expired) and the coordinator rebalances: `worker-blur-2 ✅ ASSIGNED image_tasks[P0] -> resuming from committed offset 22`. The job still finishes. |
| `docker start worker-blur-1` | It rejoins. Only one partition moves back (`REVOKED` on worker-blur-2, `ASSIGNED ... committed offset N` on worker-blur-1). |
| `docker stop worker-bw-2` | **Graceful.** The worker sends LeaveGroup, so the rebalance happens within ~3s with no 10s wait. |
| `docker pause worker-bw-2` then `docker unpause worker-bw-2` after 15s | **Zombie.** The frozen worker is kicked out. When it wakes up, its commit is `REJECTED ... UNKNOWN_MEMBER_ID`, and the master logs `♻️ duplicate tile ... ignored`. |
| `docker kill kafka-broker-1` | The controller makes broker 2 the leader of every partition (ISR shrinks to `[2]`). Uploads keep working. `docker start kafka-broker-1`: it catches up and the controller hands leadership back to broker 1 within ~30s. |
| `docker kill redis-1` | Sentinels agree it's down (`+sdown`, `+odown`) and promote a replica (`+switch-master ... redis-2`). The app reconnects by itself. `docker start redis-1`: it comes back as a replica (`+convert-to-slave`). |

Watch the Sentinel side with `docker compose logs -f redis-sentinel-1`.

---

## Command cheat sheet

**Docker**
```bash
docker compose ps                         # running containers
docker compose logs -f <name>             # follow logs
docker exec -it <name> sh                 # shell inside a container
docker kill / stop / start / pause / unpause <name>
docker stats                              # CPU / memory per container
```

**Kafka** (run inside a broker: `docker exec kafka-broker-1 /opt/kafka/bin/<tool> --bootstrap-server kafka-broker-1:9092 ...`)
```bash
kafka-topics.sh --describe                                       # partitions, leaders, ISR
kafka-consumer-groups.sh --describe --group workers-blur         # committed offset, end offset, lag, owners
kafka-consumer-groups.sh --describe --group workers-blur --state # coordinator broker, assignor, state
kafka-metadata-quorum.sh describe --status                       # who is the KRaft controller
kafka-console-consumer.sh --topic image_results --from-beginning --property print.key=true --property print.value=false
```

**Redis** (`docker exec -it redis-1 redis-cli`; use whichever node is master now)
```bash
ZREVRANGE jobs:feed 0 4                  # latest 5 job ids
HGETALL job:<id>                         # job metadata
ZRANGE job:<id>:done 0 -1 WITHSCORES     # finished tiles + arrival times
TTL worker:worker-blur-1:heartbeat       # counts down from 10, reset every 3s
KEYS cache:*                             # cached results (fine for a demo, never in production)
MONITOR                                  # print every command Redis receives, live
INFO replication
```
Sentinel: `docker exec redis-sentinel-1 redis-cli -p 26379 SENTINEL master mymaster`

---

## Project layout
```
docker-compose.yml        all 16 containers and how they connect
Dockerfile                the one Python image
infra/redis/              redis.conf (master + replicas), sentinel.conf
common/
  config.py               every setting (topics, groups, tile size, timeouts)
  redis_store.py          Sentinel connection, key names, record_tile() (dedupe + atomic count)
  imaging.py              split into tiles (with halo), filters, reassemble
  kafka_internals.py      group → coordinator formula, metadata helper
kafka_setup/setup_kafka.py   creates topics (kafka-init)
master/
  app.py                  HTTP API: POST /api/jobs, GET /api/jobs/<id>, GET /api/jobs/<id>/result
  tile_publisher.py       PRODUCER role  → image_tasks
  result_assembler.py     CONSUMER role  ← image_results, reassembly, cache write
  health_monitor.py       Redis heartbeats + Kafka offsets + Sentinel view
worker/worker.py          consume → filter → produce → commit
web/                      user-facing Flask app + page
tools/make_test_image.py  generates the images in samples/
```
