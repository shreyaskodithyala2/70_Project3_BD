"""
WEB - the user-facing Flask app (http://localhost:8090 on your Mac).

It knows nothing about Kafka or Redis. It serves the page and forwards the
browser's API calls to the master over HTTP (inside the Docker network the
master is reachable at http://master:5000 - Docker's DNS resolves the name).
"""
import logging

import requests
from flask import Flask, Response, jsonify, render_template, request
from waitress import serve

from common import config

config.setup_logging()
log = logging.getLogger("web")

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = config.MAX_UPLOAD_BYTES


def relay(resp):
    """Pass the master's response (JSON or image) back to the browser."""
    return Response(resp.content, status=resp.status_code,
                    content_type=resp.headers.get("Content-Type"))


@app.get("/")
def index():
    return render_template("index.html", filters=config.FILTERS)


@app.post("/api/jobs")
def upload():
    """API call 1: user -> web -> master. Forward the file + chosen filter."""
    file = request.files.get("file")
    if not file:
        return jsonify(error="no file"), 400
    try:
        resp = requests.post(
            f"{config.MASTER_URL}/api/jobs",
            files={"file": (file.filename, file.stream, file.mimetype)},
            data={"filter": request.form.get("filter", "")},
            timeout=120,
        )
    except requests.RequestException as e:
        return jsonify(error=f"master unreachable: {e}"), 502
    return relay(resp)


@app.get("/api/<path:path>")
def proxy_get(path):
    """API calls 2 & 3 (status, result image) and the dashboard data."""
    try:
        return relay(requests.get(f"{config.MASTER_URL}/api/{path}", timeout=10))
    except requests.RequestException as e:
        return jsonify(error=f"master unreachable: {e}"), 502


if __name__ == "__main__":
    log.info("🌐 web UI listening on :%d (open http://localhost:8090)", config.WEB_PORT)
    serve(app, host="0.0.0.0", port=config.WEB_PORT, threads=8)
