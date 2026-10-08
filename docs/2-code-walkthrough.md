# Code walkthrough: following one upload through the system

Read this with the code open. Every step links to the exact function. The log lines are real
output from a 3072×4096 image with the Blur filter (48 tiles).

```
browser ─(1)─▶ web ─(2)─▶ master API ─(3)─▶ Redis (cache? job state)
                               └─(4)─▶ Kafka image_tasks ─(5)─▶ worker ─(6)─▶ Kafka image_results
                                                                                     │
                     browser ◀─(9)─ web ◀─ master API ◀─(8)─ Redis cache ◀─(7)─ master assembler
```

---

## 0. Before any upload: startup order

1. **`kafka-controller`** starts the KRaft quorum (node 0 becomes the leader).
2. **`kafka-broker-1/2`** register with the controller.
3. **`kafka-init`** ([setup_kafka.py](../kafka_setup/setup_kafka.py)):
   - [`wait_for_brokers`](../kafka_setup/setup_kafka.py#L28) waits until metadata lists brokers 1 and 2
   - [`create_topics`](../kafka_setup/setup_kafka.py#L53) creates `image_tasks` / `image_results` with `replica_assignment=[[1,2],[1,2]]`: first id = leader
   - [`show_coordinators`](../kafka_setup/setup_kafka.py#L99) computes `abs(hash(group)) % 2` and checks it against Kafka's answer
4. **Redis**: `redis-1` (master), `redis-2/3` (`--replicaof redis-1`), 3 sentinels watching `mymaster`.
5. **Workers** ([worker.py](../worker/worker.py)) subscribe. The coordinator assigns partitions and `on_assign` logs where each starts.
6. **Master** ([app.py](../master/app.py#L179)) starts two background threads (`ResultAssembler`, `HealthMonitor`) and the HTTP server.
7. **Web** starts and waits until master's `/healthz` answers.

---

## 1. Browser → web
[web/static/app.js](../web/static/app.js): the form posts `file` + `filter` to `/api/jobs`.
Several files are uploaded **in parallel** (`Promise.all`).

## 2. Web → master
[web/app.py `upload()`](../web/app.py#L35) forwards the multipart request to `http://master:5000/api/jobs`.
`master` is a container name, resolved by Docker's DNS. The web app knows nothing about Kafka or Redis.

## 3. Master: hash, cache lookup, job state
[master/app.py `create_job()`](../master/app.py#L48)

```python
image_hash = hashlib.sha256(data).hexdigest()         # identity of the exact file
ck = rs.cache_key(image_hash, flt)                    # "cache:<sha256>:blur"
if r.exists(ck): ...                                  # HIT -> create a 'done' job, return in ~2 ms
```
On a **miss**:
- `imaging.decode` → NumPy array, check ≥ 1024×1024
- [`imaging.split_into_tiles`](../common/imaging.py#L70) → list of `Tile` (index, position, crop box, JPEG bytes of the tile **plus a 25px halo**)
- write the job hash **before** publishing, so a fast result can never arrive for an unknown job:
  ```
  HSET job:b2070d5d8f02 status processing total_tiles 48 processed_tiles 0 width 3072 ...
  ZADD jobs:feed <now> b2070d5d8f02
  ```

## 4. Master → Kafka (producer role)
[master/tile_publisher.py `publish()`](../master/tile_publisher.py#L35)

```python
self.producer.produce(TASKS_TOPIC, key=f"{job_id}:{tile.index}", value=tile.data,
                      headers=[("filter", flt), ("crop", "25,25,512,512")], on_delivery=on_delivery)
...
self.producer.flush(30)       # wait until the leader + ISR acknowledged every tile
```
```
[producer] ✔ key=b2070d5d8f02:0 -> image_tasks[P0] offset 80 (acked by all in-sync replicas)
[producer] ✔ key=b2070d5d8f02:1 -> image_tasks[P0] offset 81 (acked by all in-sync replicas)
[producer] ✅ job b2070d5d8f02: 48 tiles published, split {'P0': 27, 'P1': 21}
```
- partition = `murmur2(key) % 2`, which is why the split is 27/21 and not exactly half
- the **offset** in each line was assigned by the partition leader (broker 1)
- the HTTP response (`202 processing`) is sent only after `flush()`, so the work is durably in Kafka

## 5. Kafka → worker (consumer role)
[worker/worker.py `run()`](../worker/worker.py#L144) → `consumer.poll(1.0)` → [`handle()`](../worker/worker.py#L174)

Both groups receive this message:
- `workers-blackwhite` sees `filter=blur` → **skips** it, commits, moves on
- `workers-blur`: whichever worker owns that partition processes it

```
[task]   🎨 picked image_tasks[P1]@84  key=b2070d5d8f02:5  (tile 5 of job b2070d5d8f02)
```
```python
img = imaging.decode(msg.value())             # bytes -> pixels
out = imaging.apply_filter(img, flt)          # cv2.GaussianBlur 51x51 (or BGR->GRAY->BGR)
result = encode_jpeg(crop_core(out, crop))    # cut the 25px halo off again
```

## 6. Worker → Kafka, then commit (at-least-once)
```python
self.producer.produce(RESULTS_TOPIC, key=key, value=result, headers=[("worker", WORKER_ID), ...])
self.producer.flush(30)                       # wait for the ack
self.commit(msg)                              # ONLY now: offset 84 done -> commit 85
```
```
[task]   📤 sent result -> image_results[P0]@165  (508 ms, acked by all in-sync replicas)
[commit] ✔ committed image_tasks[P1] offset=85 (next to read)
```
If the result isn't acknowledged, the worker does **not** commit. It `seek()`s back and retries.
If it crashes between send and commit, the next owner re-processes the tile, and step 7 drops the duplicate.

Meanwhile the [heartbeat thread](../worker/worker.py#L120) runs every 3s:
`SET worker:worker-blur-1:heartbeat {...} EX 10` + `ZADD workers:last_seen`.

## 7. Kafka → master (consumer role) → Redis
[master/result_assembler.py `handle()`](../master/result_assembler.py#L64) → [`redis_store.record_tile()`](../common/redis_store.py#L87)

```python
is_new = r.zadd(f"job:{id}:done", {tile_index: now}, nx=True)    # dedupe
if is_new:
    r.hset(f"job:{id}:tiles", tile_index, tile_bytes)               # keep the bytes
    processed = r.hincrby(f"job:{id}", "processed_tiles", 1)        # atomic count
```
```
[assembler] 📥 tile 47 from worker-blur-2 (partition 0, offset 212) -> 48/48
```
The call that gets `processed == total` runs [`assemble()`](../master/result_assembler.py#L85):
- `HGETALL job:<id>:tiles` → all 48 tiles
- [`imaging.assemble`](../common/imaging.py#L106) pastes each at `(row, col) = divmod(index, cols)`
- one **MULTI/EXEC transaction**: `SET cache:<sha>:blur <bytes> EX 86400`, `HSET status done`, `DEL` the temp tiles
- only then is the Kafka offset committed

```
[assembler] 🧩 job b2070d5d8f02 reassembled (3072x4096, 48 tiles) in 43 ms -> cached as cache:5e51d27b26805e54…
```

## 8–9. Status and result back to the user
The browser polls every 1.5s:
- `GET /api/feed` → [`feed()`](../master/app.py#L162): `ZREVRANGE jobs:feed` + `HGETALL job:<id>` for each, which drives the progress bar and tile colours
- `GET /api/jobs/<id>` → [`get_job()`](../master/app.py#L136): one job's status
- `GET /api/jobs/<id>/result` → [`get_result()`](../master/app.py#L143): `GET cache:<sha>:<filter>` → JPEG

The next upload of the same file with the same filter stops at step 3:
```
[api] ⚡ CACHE HIT ba024d045ab6… (blur) -> job 36333d25b77f served from Redis in 2 ms
```

---

## The health monitor (runs the whole time)
[master/health_monitor.py](../master/health_monitor.py), every 3s:
1. [`check_workers`](../master/health_monitor.py#L59): for every id in `workers:last_seen`, `TTL worker:<id>:heartbeat`.
   `-2` (key gone) → DEAD → log + event.
2. [`check_kafka`](../master/health_monitor.py#L88): partition leaders/ISR from metadata, each group's committed offsets
   (`consumer.committed`) and end offsets → **lag**, and the coordinator = leader of `__consumer_offsets[hash % 2]`.
3. [`check_redis`](../master/health_monitor.py#L124): asks Sentinel who the master and replicas are.

A real bug we hit while testing: the first version used `AdminClient.describe_consumer_groups`.
When we killed broker 1 (a group coordinator), librdkafka 2.15 hit an internal assertion and
**crashed the whole master process**. We reproduced it in isolation, confirmed the plain
`Consumer.committed()` path survives, and switched to it.

---

## File-by-file summary

| File | Read it for |
|---|---|
| [common/config.py](../common/config.py) | every setting: topic layout, group names (and why), timeouts |
| [common/redis_store.py](../common/redis_store.py) | Sentinel connection with retry, key layout, `record_tile` |
| [common/imaging.py](../common/imaging.py) | halo tiling, filters, reassembly |
| [common/kafka_internals.py](../common/kafka_internals.py) | the group → coordinator formula |
| [kafka_setup/setup_kafka.py](../kafka_setup/setup_kafka.py) | AdminClient: create topics, describe cluster and groups |
| [master/app.py](../master/app.py) | the 3 API calls + cache logic |
| [master/tile_publisher.py](../master/tile_publisher.py) | producer: keys, headers, acks, delivery callback |
| [master/result_assembler.py](../master/result_assembler.py) | consumer: manual commit, dedupe, reassembly, cache write |
| [master/health_monitor.py](../master/health_monitor.py) | Redis TTL detection, offsets/lag, Sentinel view |
| [worker/worker.py](../worker/worker.py) | consumer group config, rebalance callbacks, commit-after-produce, graceful shutdown |
| [web/app.py](../web/app.py) | thin proxy to the master |
| [docker-compose.yml](../docker-compose.yml) | every container, env vars, start order |
