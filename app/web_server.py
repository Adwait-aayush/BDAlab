"""Small, dependency-light web UI and API for the sentiment pipeline.

The dashboard intentionally lives outside Streamlit so it can be served by the
same Python image as the consumer.  The API only exposes derived metrics,
read-only HDFS inventory, and the existing ``SentimentPredictor``.
"""

from __future__ import annotations

import json
import logging
import mimetypes
import os
import sys
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, unquote, urlparse

import requests
from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient

ROOT_DIR = Path(__file__).resolve().parent.parent
WEB_DIR = ROOT_DIR / "web"
MAX_REQUEST_BYTES = 32 * 1024
logger = logging.getLogger("sentiment_web")

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

try:
    from model.predict import SentimentPredictor
except ImportError:  # pragma: no cover - supports direct execution from app/
    from ..model.predict import SentimentPredictor


def _path_from_env(name: str, default: str) -> Path:
    """Resolve a configured path relative to the project root."""
    value = os.getenv(name, default)
    path = Path(value)
    return path if path.is_absolute() else ROOT_DIR / path


def _empty_metrics() -> Dict[str, Any]:
    return {
        "total_processed": 0,
        "total_positive": 0,
        "total_negative": 0,
        "global_positive_percentage": 0.0,
        "global_negative_percentage": 0.0,
        "overall_mean_score": 0.0,
        "movies": {},
        "recent_reviews": [],
        "timeline": [],
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def _read_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            value = json.load(handle)
        return value if isinstance(value, dict) else None
    except (OSError, ValueError, TypeError):
        return None


def load_metrics() -> Dict[str, Any]:
    """Read live metrics, preferring the consumer's shared artifact."""
    metrics_path = _path_from_env("METRICS_DATA_PATH", "artifacts/live_metrics.json")
    data = _read_json(metrics_path)
    source = "live_metrics.json" if data is not None else "empty fallback"

    if data is None:
        mirror_summary = _path_from_env(
            "HDFS_MIRROR_PATH", "data/hdfs_mirror"
        ) / "movie-sentiment" / "metrics" / "summary.json"
        data = _read_json(mirror_summary)
        if data is not None:
            source = "local HDFS mirror"

    result = _empty_metrics()
    if data:
        result.update(data)
    result["_source"] = source
    return result


def _format_bytes(value: Any) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def _local_hdfs_inventory() -> Dict[str, Any]:
    mirror_root = _path_from_env("HDFS_MIRROR_PATH", "data/hdfs_mirror") / "movie-sentiment"
    files: List[Dict[str, Any]] = []
    if mirror_root.is_dir():
        for path in mirror_root.rglob("*"):
            if not path.is_file():
                continue
            relative = path.relative_to(mirror_root).as_posix()
            try:
                stat = path.stat()
                modified = datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat()
                size = stat.st_size
            except OSError:
                continue
            files.append(
                {
                    "path": f"/movie-sentiment/{relative}",
                    "zone": relative.split("/", 1)[0],
                    "type": "FILE",
                    "bytes": size,
                    "modified": modified,
                }
            )
    return _inventory_payload(files, "Local mirror", False)


def _inventory_payload(files: List[Dict[str, Any]], source: str, online: bool) -> Dict[str, Any]:
    files.sort(key=lambda item: item.get("modified", ""), reverse=True)
    by_zone: Dict[str, int] = {}
    for item in files:
        zone = str(item.get("zone", "unknown"))
        by_zone[zone] = by_zone.get(zone, 0) + 1
    return {
        "files": files[:100],
        "source": source,
        "online": online,
        "total_bytes": sum(_format_bytes(item.get("bytes")) for item in files),
        "by_zone": by_zone,
    }


def load_hdfs_inventory() -> Dict[str, Any]:
    """List HDFS files, falling back to the consumer's local mirror."""
    base_url = os.getenv("WEBHDFS_URL", "http://localhost:9870/webhdfs/v1").rstrip("/")
    user = os.getenv("HDFS_USER", "root")
    files: List[Dict[str, Any]] = []
    visited: set[str] = set()

    def collect(path: str) -> None:
        if path in visited or len(files) >= 100:
            return
        visited.add(path)
        try:
            response = requests.get(
                f"{base_url}/{path}",
                params={"op": "LISTSTATUS", "user.name": user},
                timeout=2,
            )
            response.raise_for_status()
            entries = response.json().get("FileStatuses", {}).get("FileStatus", [])
            for item in entries:
                suffix = str(item.get("pathSuffix", ""))
                if not suffix:
                    continue
                child = f"{path}/{suffix}"
                if item.get("type") == "DIRECTORY":
                    collect(child)
                else:
                    modified_ms = item.get("modificationTime")
                    try:
                        modified = datetime.fromtimestamp(
                            float(modified_ms) / 1000, timezone.utc
                        ).isoformat()
                    except (TypeError, ValueError, OSError):
                        modified = ""
                    files.append(
                        {
                            "path": f"/{child}",
                            "zone": path.rsplit("/", 1)[-1],
                            "type": item.get("type", "FILE"),
                            "bytes": _format_bytes(item.get("length")),
                            "modified": modified,
                        }
                    )
        except (requests.RequestException, ValueError, TypeError, KeyError):
            return

    collect("movie-sentiment")
    if files:
        return _inventory_payload(files, "WebHDFS", True)
    return _local_hdfs_inventory()


def load_kafka_status() -> Dict[str, Any]:
    """Read live topic and partition metadata for the control room."""
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topics: List[Dict[str, Any]] = []
    admin = None
    consumer = None
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=[item.strip() for item in bootstrap.split(",")],
            client_id="sentinel-dashboard",
            request_timeout_ms=2000,
        )
        topic_names = sorted(
            name for name in admin.list_topics()
            if not name.startswith("__")
        )
        consumer = KafkaConsumer(
            bootstrap_servers=[item.strip() for item in bootstrap.split(",")],
            request_timeout_ms=2000,
            consumer_timeout_ms=1000,
        )
        for name in topic_names:
            partitions = consumer.partitions_for_topic(name) or set()
            partition_rows = []
            total_messages = 0
            recent_messages = []
            for partition in sorted(partitions):
                tp = TopicPartition(name, partition)
                end = consumer.end_offsets([tp]).get(tp, 0)
                beginning = consumer.beginning_offsets([tp]).get(tp, 0)
                count = max(0, end - beginning)
                total_messages += count
                partition_rows.append({
                    "partition": partition,
                    "earliest_offset": beginning,
                    "latest_offset": end,
                    "messages": count,
                })
                if end > beginning:
                    consumer.assign([tp])
                    consumer.seek(tp, max(beginning, end - 5))
                    for message in consumer.poll(timeout_ms=500, max_records=5).get(tp, []):
                        try:
                            value = json.loads(message.value.decode("utf-8"))
                        except (AttributeError, UnicodeDecodeError, ValueError):
                            value = {"raw": str(message.value)}
                        recent_messages.append({
                            "topic": name,
                            "partition": partition,
                            "offset": message.offset,
                            "key": message.key.decode("utf-8", errors="replace") if message.key else None,
                            "value": value,
                        })
            topics.append({
                "name": name,
                "partitions": len(partitions),
                "messages": total_messages,
                "partition_details": partition_rows,
                "recent_messages": recent_messages[-5:],
            })
        return {
            "online": True,
            "bootstrap_servers": bootstrap,
            "topics": topics,
            "topic_count": len(topics),
        }
    except Exception:
        logger.exception("Kafka telemetry unavailable")
        return {
            "online": False,
            "bootstrap_servers": bootstrap,
            "topics": [],
            "topic_count": 0,
        }
    finally:
        if consumer is not None:
            consumer.close()
        if admin is not None:
            admin.close()


def read_kafka_message(topic: str, partition: int, offset: int) -> Dict[str, Any]:
    """Fetch one exact Kafka record for dashboard inspection."""
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    consumer = None
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=[item.strip() for item in bootstrap.split(",")],
            request_timeout_ms=3000,
            consumer_timeout_ms=1000,
            enable_auto_commit=False,
        )
        tp = TopicPartition(topic, partition)
        consumer.assign([tp])
        consumer.seek(tp, offset)
        for message in consumer.poll(timeout_ms=1500, max_records=1).get(tp, []):
            if message.offset != offset:
                continue
            try:
                value = json.loads(message.value.decode("utf-8"))
            except (AttributeError, UnicodeDecodeError, ValueError):
                value = {"raw": str(message.value)}
            return {
                "topic": topic,
                "partition": partition,
                "offset": offset,
                "key": message.key.decode("utf-8", errors="replace") if message.key else None,
                "value": value,
                "timestamp": message.timestamp,
            }
        return {"error": "Message is no longer available"}
    except Exception:
        logger.exception("Kafka message lookup failed")
        return {"error": "Kafka message is unavailable"}
    finally:
        if consumer is not None:
            consumer.close()


def read_hdfs_file(path: str) -> Dict[str, Any]:
    """Read a small text/JSONL file through WebHDFS for in-dashboard inspection."""
    clean_path = path.strip("/")
    if not clean_path.startswith("movie-sentiment/"):
        return {"error": "Only movie-sentiment files can be inspected"}
    base_url = os.getenv("WEBHDFS_URL", "http://localhost:9870/webhdfs/v1").rstrip("/")
    user = os.getenv("HDFS_USER", "root")
    try:
        response = requests.get(
            f"{base_url}/{clean_path}",
            params={"op": "OPEN", "user.name": user},
            timeout=4,
        )
        response.raise_for_status()
        content = response.text[-200000:]
        return _hdfs_file_payload(clean_path, content, "WebHDFS")
    except (requests.RequestException, ValueError):
        mirror = _path_from_env("HDFS_MIRROR_PATH", "data/hdfs_mirror") / clean_path
        try:
            content = mirror.read_text(encoding="utf-8")[-200000:]
            return _hdfs_file_payload(clean_path, content, "Local mirror")
        except OSError:
            return {"error": "File is not available"}


def _hdfs_file_payload(path: str, content: str, source: str) -> Dict[str, Any]:
    records = []
    if path.endswith(".jsonl"):
        for index, line in enumerate(content.splitlines()[:100]):
            try:
                value = json.loads(line)
            except ValueError:
                value = {"raw": line}
            records.append({"index": index, "value": value})
    return {"path": f"/{path}", "source": source, "content": content, "records": records}


_predictor: Optional[SentimentPredictor] = None
_predictor_lock = Lock()


def get_predictor() -> SentimentPredictor:
    """Lazily load the production predictor once per web process."""
    global _predictor
    if _predictor is None:
        with _predictor_lock:
            if _predictor is None:
                model_path = str(_path_from_env("MODEL_PATH", "artifacts/sentiment_model.joblib"))
                _predictor = SentimentPredictor(model_path=model_path, auto_train=True)
    return _predictor


def model_info() -> Dict[str, Any]:
    try:
        predictor = get_predictor()
        return {
            "available": True,
            "name": "TF-IDF + Logistic Regression",
            "metrics": predictor.get_metrics(),
            "threshold": predictor.threshold,
        }
    except Exception:
        logger.exception("Model metadata unavailable")
        return {
            "available": False,
            "name": "TF-IDF + Logistic Regression",
            "metrics": {},
            "threshold": 0.5,
        }


class DashboardHandler(BaseHTTPRequestHandler):
    """Serve the static dashboard and small JSON endpoints."""

    server_version = "SentimentControlRoom/1.0"

    def log_message(self, format: str, *args: Any) -> None:
        logger.info("%s - %s", self.address_string(), format % args)

    def _send_json(self, payload: Dict[str, Any], status: int = HTTPStatus.OK) -> None:
        encoded = json.dumps(payload, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _send_file(self, path: Path) -> None:
        try:
            data = path.read_bytes()
        except OSError:
            self._send_json({"error": "Resource unavailable"}, HTTPStatus.NOT_FOUND)
            return
        content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", f"{content_type}; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        route = parsed.path.rstrip("/") or "/"
        try:
            if route == "/api/metrics":
                self._send_json(load_metrics())
            elif route == "/api/hdfs":
                self._send_json(load_hdfs_inventory())
            elif route == "/api/hdfs/file":
                requested = parse_qs(parsed.query).get("path", [""])[0]
                self._send_json(read_hdfs_file(requested))
            elif route == "/api/kafka":
                self._send_json(load_kafka_status())
            elif route == "/api/kafka/message":
                query = parse_qs(parsed.query)
                topic = query.get("topic", [""])[0]
                try:
                    partition = int(query.get("partition", ["0"])[0])
                    offset = int(query.get("offset", ["-1"])[0])
                except ValueError:
                    self._send_json({"error": "Invalid Kafka location"}, HTTPStatus.BAD_REQUEST)
                    return
                if not topic or offset < 0:
                    self._send_json({"error": "Topic, partition, and offset are required"}, HTTPStatus.BAD_REQUEST)
                    return
                self._send_json(read_kafka_message(topic, partition, offset))
            elif route == "/api/model":
                self._send_json(model_info())
            elif route == "/api/health":
                metrics = load_metrics()
                self._send_json(
                    {
                        "status": "ok",
                        "metrics_source": metrics.get("_source", "unknown"),
                        "processed": metrics.get("total_processed", 0),
                        "hdfs": load_hdfs_inventory(),
                    }
                )
            elif route == "/":
                self._send_file(WEB_DIR / "index.html")
            elif route.startswith("/web/"):
                relative = unquote(route[len("/web/"):]).lstrip("/")
                candidate = (WEB_DIR / relative).resolve()
                if WEB_DIR.resolve() not in candidate.parents:
                    self._send_json({"error": "Resource unavailable"}, HTTPStatus.NOT_FOUND)
                else:
                    self._send_file(candidate)
            else:
                self._send_json({"error": "Not found"}, HTTPStatus.NOT_FOUND)
        except Exception:
            logger.exception("Unhandled GET request for %s", route)
            self._send_json({"error": "Service temporarily unavailable"}, HTTPStatus.INTERNAL_SERVER_ERROR)

    def do_POST(self) -> None:
        route = urlparse(self.path).path.rstrip("/") or "/"
        if route not in {"/api/predict", "/api/explain"}:
            self._send_json({"error": "Not found"}, HTTPStatus.NOT_FOUND)
            return
        try:
            content_length = int(self.headers.get("Content-Length", "0"))
            if content_length <= 0 or content_length > MAX_REQUEST_BYTES:
                self._send_json({"error": "Request body is missing or too large"}, HTTPStatus.BAD_REQUEST)
                return
            raw_body = self.rfile.read(content_length)
            body = json.loads(raw_body.decode("utf-8"))
            review_text = body.get("review_text") if isinstance(body, dict) else None
            if not isinstance(review_text, str) or not review_text.strip():
                self._send_json({"error": "Enter a review before scoring it"}, HTTPStatus.BAD_REQUEST)
                return
            if len(review_text) > 5000:
                self._send_json({"error": "Review is limited to 5,000 characters"}, HTTPStatus.BAD_REQUEST)
                return
            predictor = get_predictor()
            prediction = (
                predictor.explain_one(review_text.strip())
                if route == "/api/explain"
                else predictor.predict_one(review_text.strip())
            )
            self._send_json({"ok": True, "review_text": review_text.strip(), **prediction})
        except (ValueError, UnicodeDecodeError, json.JSONDecodeError):
            self._send_json({"error": "Request must contain valid JSON"}, HTTPStatus.BAD_REQUEST)
        except Exception:
            logger.exception("Prediction request failed")
            self._send_json({"error": "Prediction is temporarily unavailable"}, HTTPStatus.SERVICE_UNAVAILABLE)


def main() -> None:
    host = os.getenv("WEB_HOST", "0.0.0.0")
    port = int(os.getenv("WEB_PORT", "8501"))
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s [%(levelname)s] [Web]: %(message)s",
    )
    server = ThreadingHTTPServer((host, port), DashboardHandler)
    logger.info("Dashboard listening on http://%s:%s", host, port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Dashboard shutting down")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
