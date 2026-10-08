# Demo commands

Run everything from the project folder:
```bash
cd ~/Downloads/Sem-5/BD/Project_Remastered
```
Website: **http://localhost:8090**. The right panel shows workers, Kafka partitions, consumer groups and the Redis master, all live.

---

## 1. Start the system

```bash
docker version                 # Docker is running (Client + Server sections)
docker compose down            # clean slate: delete old containers (images are kept)
docker compose build           # Dockerfile → our image: image-pipeline:latest
docker compose up -d           # create + start all 16 containers in the right order
docker compose ps -a           # 15 "Up", master + redis healthy, kafka-init "Exited (0)"
```

---

## 2. Docker: image and containers

```bash
docker images image-pipeline                         # our image and its size
docker history image-pipeline:latest                 # its layers (one per Dockerfile line)
docker run --rm image-pipeline:latest ls /app        # our code is inside the image
docker compose config --services                     # the 16 services in docker-compose.yml
docker network inspect image-pipeline_default        # all containers on one private network
```

---

## 3. Kafka: setup, controller, topics, groups

```bash
# what setup_kafka.py did: topics, leaders, coordinators
docker compose logs kafka-init

# the KRaft controller → LeaderId: 0
docker exec kafka-broker-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka-broker-1:9092 describe --status

# config generated from our docker-compose env vars
docker exec kafka-controller cat /opt/kafka/config/server.properties

# topic layout → Leader: 1  Replicas: 1,2  Isr: 1,2
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-broker-1:9092 --describe --topic image_tasks
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-broker-1:9092 --describe --topic image_results

# consumer group: which worker owns which partition, committed offset, lag
docker exec kafka-broker-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-broker-1:9092 --describe --group workers-blur
docker exec kafka-broker-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-broker-1:9092 --describe --group workers-blackwhite

# group coordinator broker + assignment strategy
docker exec kafka-broker-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-broker-1:9092 --describe --group workers-blur --state
```

---

## 4. Redis: replication and Sentinel

```bash
# master + its replicas → role:master, connected_slaves:2
docker exec redis-1 redis-cli INFO replication

# who Sentinel says is master → redis-1 6379
docker exec redis-sentinel-1 redis-cli -p 26379 SENTINEL get-master-addr-by-name mymaster

# replication test: write on master, read on replica, replica refuses writes
docker exec redis-1 redis-cli SET hello world
docker exec redis-2 redis-cli GET hello              # → world
docker exec redis-2 redis-cli SET hello again        # → READONLY error

# worker heartbeats (one key per live worker) and the cache
docker exec redis-1 redis-cli KEYS "worker:*:heartbeat"
docker exec redis-1 redis-cli TTL worker:worker-blur-1:heartbeat   # counts down from 10, resets every 3s
docker exec redis-1 redis-cli KEYS "cache:*"

# every command Redis receives, live (Ctrl+C to stop)
docker exec -it redis-1 redis-cli MONITOR
```
After a Redis failover, use the **new master's** name instead of `redis-1` (ask Sentinel).

---

## 5. Run a job and watch it

Open two extra terminals:
```bash
docker compose logs -f master          # [producer] tiles → partitions/offsets, [assembler] n/16, reassembled
docker compose logs -f worker-blur-1   # [task] picked, sent result, [commit] committed offset
```
Upload `samples/medium_2048.jpg` with **Gaussian Blur** on the website (16 tiles).
Upload the **same file + same filter** again → `⚡ CACHE HIT ... served from Redis in 2 ms`.

From the terminal instead of the browser:
```bash
curl -s -F file=@samples/large_4096x3072.jpg -F filter=blur localhost:8090/api/jobs
```

---

## 6. Failover demo 1: Kafka broker

```bash
# before → Leader: 1  Isr: 1,2
docker exec kafka-broker-2 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-broker-2:9092 --describe --topic image_tasks | grep Partition

docker compose stop kafka-broker-1

# wait 15s → Leader: 2  Isr: 2          (controller promoted broker 2)
docker exec kafka-broker-2 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-broker-2:9092 --describe --topic image_tasks | grep Partition

docker compose start kafka-broker-1

# wait 45s → Leader: 1  Isr: 2,1        (caught up, preferred leader restored)
docker exec kafka-broker-2 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-broker-2:9092 --describe --topic image_tasks | grep Partition
```

---

## 7. Failover demo 2: worker (consumer group rebalance)

Start a big job first (48 tiles, ~25s): upload `samples/large_4096x3072.jpg` with **Gaussian Blur**.

```bash
# before → P0 and P1 owned by different workers
docker exec kafka-broker-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-broker-1:9092 --describe --group workers-blur

docker kill worker-blur-1              # crash

# wait 15s → both partitions owned by worker-blur-2
docker exec kafka-broker-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-broker-1:9092 --describe --group workers-blur
docker compose logs worker-blur-2 | grep rebalance    # ✅ ASSIGNED ... resuming from committed offset N
```
Website: worker-blur-1 shows a **red dot / dead**, Events shows `💀 worker-blur-1 is DEAD`, and **the job still finishes**.

```bash
docker start worker-blur-1

# wait 10s → partitions split between the two workers again
docker exec kafka-broker-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-broker-1:9092 --describe --group workers-blur
docker compose logs worker-blur-1 | grep rebalance    # ✅ ASSIGNED ... resuming from committed offset N
```

Graceful version (rebalance in ~3s instead of ~10s):
```bash
docker stop worker-blur-1
docker start worker-blur-1
```

---

## 8. Failover demo 3: Redis master

```bash
docker exec redis-sentinel-1 redis-cli -p 26379 SENTINEL get-master-addr-by-name mymaster   # → redis-1
docker exec redis-1 redis-cli KEYS "cache:*"                                                # note the cached results

docker compose stop redis-1

# wait 10s → redis-2 (or redis-3)        (Sentinels promoted a replica)
docker exec redis-sentinel-1 redis-cli -p 26379 SENTINEL get-master-addr-by-name mymaster
docker exec redis-2 redis-cli KEYS "cache:*"     # same keys → the cache survived (use the name printed above)
```
Website: upload a previously processed image + filter → instant **cache hit**, served by the new master.

```bash
docker compose start redis-1

# wait 15s → role:slave, master_host:redis-2    (old master rejoins as a replica)
docker exec redis-1 redis-cli INFO replication | grep -E "role|master_host"
docker exec redis-1 redis-cli KEYS "cache:*"     # same keys, copied from the new master
```
Watch the failover live in another terminal: `docker compose logs -f redis-sentinel-1` (`+sdown`, `+odown`, `+switch-master`, `+convert-to-slave`).

---

## 9. Shut down

```bash
docker compose stop        # stop everything, keep the data
docker compose down        # stop and delete the containers (and all Kafka/Redis data)
```
