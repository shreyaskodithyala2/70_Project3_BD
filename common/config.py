"""
config.py - every setting of the project in one place.

Imported by all Python containers (kafka-init, workers, master, web).
Any value can be overridden with an environment variable; that is how
docker-compose.yml gives each container its own identity (WORKER_ID, WORKER_FILTER).
"""
import logging
import os


def _env(name, default):
    """Read an environment variable, or fall back to the default.
    Example: _env("MASTER_PORT", "5000") -> "5000" unless MASTER_PORT is set."""
    return os.getenv(name, default)


# ─────────────────────────────── Kafka ────────────────────────────────
# Bootstrap servers: the brokers a client contacts FIRST to download the cluster
# map (all brokers, who leads each partition). One reachable broker is enough.
KAFKA_BOOTSTRAP = _env("KAFKA_BOOTSTRAP", "kafka-broker-1:9092,kafka-broker-2:9092")

TASKS_TOPIC = "image_tasks"        # master  -> workers   (raw tiles)
RESULTS_TOPIC = "image_results"    # workers -> master    (processed tiles)

BROKER_IDS = [1, 2]                # node ids of the two brokers (the controller is node 0)

# Topic layout sent to Kafka by setup_kafka.py.
#   number of inner lists = number of partitions
#   numbers in a list     = brokers holding a copy (= replication factor)
#   first number          = preferred LEADER (we pin broker 1 so failover is easy to demo)
TOPIC_LAYOUT = {
    TASKS_TOPIC:   [[1, 2], [1, 2]],   # 2 partitions, RF 2, leader = broker 1
    RESULTS_TOPIC: [[1, 2]],           # 1 partition,  RF 2, leader = broker 1
}
TASKS_PARTITIONS = len(TOPIC_LAYOUT[TASKS_TOPIC])

# Partitions of Kafka's internal __consumer_offsets topic (must match docker-compose.yml).
# A group's coordinator = leader of __consumer_offsets[abs(hash(group.id)) % 2].
OFFSETS_TOPIC_PARTITIONS = 2

# ─────────────────────────── Filters & groups ─────────────────────────
FILTERS = {
    "blur": "Gaussian Blur",
    "blackwhite": "Black & White",
}

# One consumer group per filter. Both groups receive EVERY tile; each worker only
# processes tiles whose 'filter' header matches its group.
# Names chosen so they hash to different __consumer_offsets partitions
# -> each worker group gets its coordinator on a different broker.
WORKER_GROUPS = {
    "blur": "workers-blur",
    "blackwhite": "workers-blackwhite",
}
MASTER_GROUP = "master-assemblers"   # the master's group on image_results
ALL_GROUPS = list(WORKER_GROUPS.values()) + [MASTER_GROUP]

# Kafka consumer liveness: heartbeat every 3s; silent for 10s -> kicked out -> rebalance.
KAFKA_SESSION_TIMEOUT_MS = int(_env("KAFKA_SESSION_TIMEOUT_MS", "10000"))
KAFKA_HEARTBEAT_INTERVAL_MS = int(_env("KAFKA_HEARTBEAT_INTERVAL_MS", "3000"))

# ─────────────────────────────── Redis ────────────────────────────────
# We never hard-code the Redis master; we ask a Sentinel who it is (survives failover).
# Parsed into [("redis-sentinel-1", 26379), ...]
REDIS_SENTINELS = [
    (hp.split(":")[0], int(hp.split(":")[1]))
    for hp in _env(
        "REDIS_SENTINELS",
        "redis-sentinel-1:26379,redis-sentinel-2:26379,redis-sentinel-3:26379",
    ).split(",")
]
REDIS_MASTER_NAME = _env("REDIS_MASTER_NAME", "mymaster")   # name used in sentinel.conf

# ─────────────────────────────── Images ───────────────────────────────
TILE_SIZE = 512             # tiles are 512x512 (same as the original project)
MIN_IMAGE_SIZE = 1024       # smallest accepted image side
BLUR_KERNEL = 51            # GaussianBlur kernel size (51x51)
HALO = BLUR_KERNEL // 2     # 25px border sent with each tile so the blur has no seams
JPEG_QUALITY = 95
PREVIEW_MAX_SIDE = 480      # thumbnail size for the feed
MAX_UPLOAD_BYTES = 50 * 1024 * 1024
ALLOWED_EXTENSIONS = {"png", "jpg", "jpeg", "bmp", "tiff", "webp"}

# Artificial delay per tile so you can watch (and kill) workers mid-job in a demo.
PROCESSING_DELAY_SEC = float(_env("PROCESSING_DELAY_SEC", "0.5"))

# ───────────────────────── Heartbeats / timing ────────────────────────
HEARTBEAT_INTERVAL_SEC = 3      # worker refreshes its Redis heartbeat key this often
HEARTBEAT_TTL_SEC = 10          # key expires 10s after the last refresh -> worker is dead
MONITOR_INTERVAL_SEC = 3        # how often the master's health monitor checks

CACHE_TTL_SEC = 24 * 3600       # finished images stay cached for 24h
JOB_TTL_SEC = 24 * 3600         # job records expire after 24h
INFLIGHT_TTL_SEC = 600
FEED_SIZE = 24                  # jobs shown in the feed
EVENTS_MAX = 200                # events kept in the dashboard log

# ───────────────────────────── HTTP ports ─────────────────────────────
MASTER_PORT = int(_env("MASTER_PORT", "5000"))
MASTER_URL = _env("MASTER_URL", "http://master:5000")   # "master" = container name (Docker DNS)
WEB_PORT = int(_env("WEB_PORT", "8080"))


def setup_logging():
    """Same log format in every container: '12:01:02 INFO  [worker] message'."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("waitress").setLevel(logging.WARNING)   # hide web-server noise
