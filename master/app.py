"""
MASTER - HTTP API (port 5000, called by the web container).

The three calls between the user's web app and the master:
    POST /api/jobs                  upload image + filter   -> job id
    GET  /api/jobs/<id>             progress of the job
    GET  /api/jobs/<id>/result      the processed image
Plus some read-only helpers for the UI: /api/feed, /api/cluster, /preview.

On startup it also launches the two background threads:
    ResultAssembler  (Kafka consumer of image_results)
    HealthMonitor    (Redis heartbeats + Kafka cluster view)
"""
import hashlib
import json
import logging
import time
import uuid

from flask import Flask, Response, jsonify, request
from waitress import serve

from common import config, imaging
from common import redis_store as rs
from master.health_monitor import HealthMonitor
from master.result_assembler import ResultAssembler
from master.tile_publisher import TilePublisher

config.setup_logging()
log = logging.getLogger("api")

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = config.MAX_UPLOAD_BYTES

rs.wait_for_redis(log)
r = rs.connect()                 # str responses
rb = rs.connect(decode=False)    # bytes responses
publisher = TilePublisher()
monitor = HealthMonitor()


def error(message, status=400):
    return jsonify(error=message), status


# ─────────────────────────── 1. POST /api/jobs ───────────────────────────
@app.post("/api/jobs")
def create_job():
    started = time.perf_counter()
    file = request.files.get("file")
    flt = request.form.get("filter", "")
    if not file or not file.filename:
        return error("no file uploaded")
    if file.filename.rsplit(".", 1)[-1].lower() not in config.ALLOWED_EXTENSIONS:
        return error("unsupported file type")
    if flt not in config.FILTERS:
        return error(f"filter must be one of {list(config.FILTERS)}")

    data = file.read()
    # SHA-256 of the raw bytes: identical files -> identical hash, any change -> new hash.
    # (CRC32 is only 32 bits and built for error detection; collisions are easy.)
    image_hash = hashlib.sha256(data).hexdigest()
    job_id = uuid.uuid4().hex[:12]
    job = {"job_id": job_id, "filename": file.filename, "filter": flt,
           "image_hash": image_hash, "created_at": time.time()}

    # ── Step 1: cache lookup. Same image + same filter processed before? ──
    ck = rs.cache_key(image_hash, flt)
    if r.exists(ck):
        elapsed = int((time.perf_counter() - started) * 1000)
        job.update(status="done", from_cache=1, result_key=ck, duration_ms=elapsed,
                   total_tiles=0, processed_tiles=0, completed_at=time.time())
        r.hset(rs.job_key(job_id), mapping=job)
        r.expire(rs.job_key(job_id), config.JOB_TTL_SEC)
        r.expire(ck, config.CACHE_TTL_SEC)        # popular images stay cached longer
        r.zadd(rs.FEED_KEY, {job_id: job["created_at"]})
        r.hincrby(rs.STATS_KEY, "cache_hits", 1)
        log.info("⚡ CACHE HIT %s… (%s) -> job %s served from Redis in %d ms",
                 image_hash[:12], flt, job_id, elapsed)
        rs.push_event(r, "master", f"⚡ cache hit for {file.filename} ({flt}) in {elapsed} ms")
        return jsonify(job_id=job_id, status="done", cached=True, elapsed_ms=elapsed)

    # ── Step 2: cache miss -> split the image into tiles ──
    try:
        img = imaging.decode(data)
    except ValueError as e:
        return error(str(e))
    height, width = img.shape[:2]
    if width < config.MIN_IMAGE_SIZE or height < config.MIN_IMAGE_SIZE:
        return error(f"image is {width}x{height}; minimum is "
                     f"{config.MIN_IMAGE_SIZE}x{config.MIN_IMAGE_SIZE}")
    tiles = imaging.split_into_tiles(img)
    cols, rows = imaging.grid_size(width, height)

    # ── Step 3: job metadata -> Redis (BEFORE publishing, so results always find it) ──
    job.update(status="processing", from_cache=0, width=width, height=height,
               cols=cols, rows=rows, total_tiles=len(tiles), processed_tiles=0)
    r.hset(rs.job_key(job_id), mapping=job)
    rb.set(rs.preview_key(image_hash), imaging.make_preview(img), ex=config.CACHE_TTL_SEC)
    r.zadd(rs.FEED_KEY, {job_id: job["created_at"]})
    r.hincrby(rs.STATS_KEY, "cache_misses", 1)

    # ── Step 4: publish every tile to Kafka (waits for acks=all) ──
    try:
        split = publisher.publish(job_id, flt, tiles)
    except Exception as e:  # noqa: BLE001
        log.exception("publishing failed")
        r.hset(rs.job_key(job_id), "status", "failed")
        return error(f"could not queue tiles: {e}", 503)

    r.hset(rs.job_key(job_id), "partition_split", json.dumps(split))
    split_text = ", ".join(f"P{p}: {n}" for p, n in sorted(split.items()))
    rs.push_event(r, "master", f"job {job_id}: {len(tiles)} tiles -> image_tasks ({split_text})")
    return jsonify(job_id=job_id, status="processing", total_tiles=len(tiles),
                   partition_split=split, elapsed_ms=int((time.perf_counter() - started) * 1000)), 202


# ─────────────────────────── 2. GET /api/jobs/<id> ───────────────────────────
INT_FIELDS = {"width", "height", "cols", "rows", "total_tiles", "processed_tiles",
              "from_cache", "duration_ms"}


def job_view(job_id):
    job = r.hgetall(rs.job_key(job_id))
    if not job:
        return None
    for field in INT_FIELDS & job.keys():
        job[field] = int(float(job[field]))
    job["partition_split"] = json.loads(job.get("partition_split", "{}"))
    job["tile_workers"] = r.hgetall(rs.job_workers_key(job_id))   # tile index -> worker
    job.pop("result_key", None)
    return job


@app.get("/api/jobs/<job_id>")
def get_job(job_id):
    job = job_view(job_id)
    return jsonify(job) if job else error("job not found", 404)


# ─────────────────────── 3. GET /api/jobs/<id>/result ───────────────────────
@app.get("/api/jobs/<job_id>/result")
def get_result(job_id):
    result_key = r.hget(rs.job_key(job_id), "result_key")
    if not result_key:
        return error("result not ready yet", 404)
    data = rb.get(result_key)
    if data is None:
        return error("result expired from the cache", 410)
    return Response(data, mimetype="image/jpeg")


# ─────────────────────────── helpers for the UI ───────────────────────────
@app.get("/api/jobs/<job_id>/preview")
def get_preview(job_id):
    image_hash = r.hget(rs.job_key(job_id), "image_hash")
    data = rb.get(rs.preview_key(image_hash)) if image_hash else None
    return Response(data, mimetype="image/jpeg") if data else error("no preview", 404)


@app.get("/api/feed")
def feed():
    """Newest jobs first - a sorted set ordered by upload time."""
    job_ids = r.zrevrange(rs.FEED_KEY, 0, config.FEED_SIZE - 1)
    return jsonify([j for j in map(job_view, job_ids) if j])


@app.get("/api/cluster")
def cluster():
    events = [json.loads(e) for e in r.lrange(rs.EVENTS_KEY, 0, 39)]
    return jsonify(**monitor.snapshot, events=events, stats=r.hgetall(rs.STATS_KEY))


@app.get("/healthz")
def healthz():
    return "ok"


if __name__ == "__main__":
    ResultAssembler().start()
    monitor.start()
    log.info("🌐 master API on :%d", config.MASTER_PORT)
    serve(app, host="0.0.0.0", port=config.MASTER_PORT, threads=8)
