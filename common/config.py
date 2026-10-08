"""
Central configuration shared by every Python container (master, workers, web, kafka-init).

Every value can be overridden with an environment variable, which is how
docker-compose.yml gives each container its own identity (WORKER_ID, WORKER_FILTER...).
"""
import logging
import os


def _env(name, default):
    return os.getenv(name, default)


# ─────────────────────────────── Kafka ────────────────────────────────
# "bootstrap" = the brokers a client contacts FIRST. The client only needs one
# reachable broker; from it, it downloads the full cluster metadata (all brokers,
# which broker leads which partition) and then talks to the right broker directly.
KAFKA_BOOTSTRAP = _env("KAFKA_BOOTSTRAP", "kafka-broker-1:9092,kafka-broker-2:9092")

TASKS_TOPIC = "image_tasks"        # master  -> workers   (raw tiles)
RESULTS_TOPIC = "image_results"    # workers -> master    (processed tiles)

BROKER_IDS = [1, 2]                # node.id of the two brokers (controller is node 0)

# Which broker holds each replica of each partition. The FIRST broker in each list
# is the "preferred leader". We pin every leader on broker 1 and every follower on
# broker 2 so the roles are easy to see in a demo. (Kafka's default would spread
# leaders across brokers for load balancing.)
TOPIC_LAYOUT = {
    TASKS_TOPIC:   [[1, 2], [1, 2]],   # 2 partitions, replication factor 2
    RESULTS_TOPIC: [[1, 2]],           # 1 partition,  replication factor 2
}
TASKS_PARTITIONS = len(TOPIC_LAYOUT[TASKS_TOPIC])

# Must match KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS in docker-compose.yml.
# __consumer_offsets partition = abs(javaHash(group.id)) % this number, and the
# leader of that partition is the group's COORDINATOR broker.
OFFSETS_TOPIC_PARTITIONS = 2

# ─────────────────────────── Filters & groups ─────────────────────────
FILTERS = {
    "blur": "Gaussian Blur",
    "blackwhite": "Black & White",
}

# One consumer group per filter. Both groups read ALL of image_tasks (that's what
# separate groups do), each worker only processes tiles whose 'filter' header
# matches its group. The names were chosen so they hash to DIFFERENT
# __consumer_offsets partitions -> different coordinator brokers
# (see common/kafka_internals.py: workers-blur -> P1, workers-blackwhite -> P0).
WORKER_GROUPS = {
    "blur": "workers-blur",
    "blackwhite": "workers-blackwhite",
}
MASTER_GROUP = "master-assemblers"   # the master's group on image_results
ALL_GROUPS = list(WORKER_GROUPS.values()) + [MASTER_GROUP]

# Consumer liveness (Kafka side). If the coordinator hears no heartbeat from a
# consumer for SESSION_TIMEOUT_MS it kicks it out and rebalances the group.
KAFKA_SESSION_TIMEOUT_MS = int(_env("KAFKA_SESSION_TIMEOUT_MS", "10000"))
KAFKA_HEARTBEAT_INTERVAL_MS = int(_env("KAFKA_HEARTBEAT_INTERVAL_MS", "3000"))

# ─────────────────────────────── Redis ────────────────────────────────
# Clients never hard-code the Redis master. They ask a Sentinel
# "who is the master of 'mymaster' right now?" - that's what makes failover work.
REDIS_SENTINELS = [
    (hp.split(":")[0], int(hp.split(":")[1]))
    for hp in _env(
        "REDIS_SENTINELS",
        "redis-sentinel-1:26379,redis-sentinel-2:26379,redis-sentinel-3:26379",
    ).split(",")
]
REDIS_MASTER_NAME = _env("REDIS_MASTER_NAME", "mymaster")

# ─────────────────────────────── Images ───────────────────────────────
TILE_SIZE = 512             # same as the original project
MIN_IMAGE_SIZE = 1024       # same as the original project
BLUR_KERNEL = 51            # GaussianBlur kernel (51x51) - same as the original
HALO = BLUR_KERNEL // 2     # extra border pixels sent with each tile (see imaging.py)
JPEG_QUALITY = 95
PREVIEW_MAX_SIDE = 480
MAX_UPLOAD_BYTES = 50 * 1024 * 1024
ALLOWED_EXTENSIONS = {"png", "jpg", "jpeg", "bmp", "tiff", "webp"}

# Artificial per-tile delay so you can watch (and kill) workers mid-job in a demo.
PROCESSING_DELAY_SEC = float(_env("PROCESSING_DELAY_SEC", "0.5"))

# ───────────────────────── Heartbeats / timing ────────────────────────
HEARTBEAT_INTERVAL_SEC = 3      # worker writes its heartbeat key this often
HEARTBEAT_TTL_SEC = 10          # ...with this TTL; key vanishes 10s after last write
MONITOR_INTERVAL_SEC = 3        # master's health-monitor loop period

CACHE_TTL_SEC = 24 * 3600       # processed images stay cached for 24h
JOB_TTL_SEC = 24 * 3600
INFLIGHT_TTL_SEC = 600
FEED_SIZE = 24
EVENTS_MAX = 200

# ───────────────────────────── HTTP ports ─────────────────────────────
MASTER_PORT = int(_env("MASTER_PORT", "5000"))
MASTER_URL = _env("MASTER_URL", "http://master:5000")
WEB_PORT = int(_env("WEB_PORT", "8080"))


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    # werkzeug/waitress are noisy at INFO
    logging.getLogger("waitress").setLevel(logging.WARNING)
