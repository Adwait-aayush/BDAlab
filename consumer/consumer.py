"""
Kafka Sentiment Consumer & HDFS Pipeline.
1. Consumes incoming movie reviews from 'movie-reviews'.
2. Classifies sentiment via TF-IDF + Logistic Regression.
3. Publishes scored review records to 'movie-sentiment-results'.
4. Persists raw reviews to HDFS /movie-sentiment/raw/
5. Persists processed predictions to HDFS /movie-sentiment/processed/
6. Aggregates per-movie statistics and writes to /movie-sentiment/metrics/
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from typing import Dict, Any, List, Optional

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

# Local imports
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from model.predict import SentimentPredictor
try:
    from consumer.hdfs_writer import HDFSWriter
except (ImportError, ModuleNotFoundError):
    from hdfs_writer import HDFSWriter
from app.metrics import SentimentMetricsManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Consumer]: %(message)s"
)
logger = logging.getLogger("sentiment_consumer")


def connect_consumer(
    bootstrap_servers: str, topic: str, group_id: str, max_retries: int = 20, retry_interval: float = 3.0
) -> KafkaConsumer:
    """Connects KafkaConsumer with retries for container readiness."""
    servers = [s.strip() for s in bootstrap_servers.split(",")]
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Connecting Consumer to {servers} topic='{topic}' (Attempt {attempt}/{max_retries})...")
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=servers,
                group_id=group_id,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8", errors="replace")),
                consumer_timeout_ms=1000,  # non-blocking poll
            )
            logger.info("Kafka Consumer successfully connected!")
            return consumer
        except NoBrokersAvailable as e:
            logger.warning(f"Kafka brokers unavailable: {e}. Retrying in {retry_interval}s...")
            time.sleep(retry_interval)
        except Exception as e:
            logger.warning(f"Consumer connection error: {e}. Retrying in {retry_interval}s...")
            time.sleep(retry_interval)

    raise RuntimeError(f"Could not connect Kafka Consumer after {max_retries} attempts.")


def connect_producer(bootstrap_servers: str, max_retries: int = 10) -> KafkaProducer:
    """Connects KafkaProducer for sending output sentiment results."""
    servers = [s.strip() for s in bootstrap_servers.split(",")]
    for _ in range(max_retries):
        try:
            return KafkaProducer(
                bootstrap_servers=servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                retries=3,
            )
        except Exception:
            time.sleep(2.0)
    raise RuntimeError("Could not connect output Kafka Producer.")


class MovieSentimentPipelineConsumer:
    """
    Main real-time consumer service managing streaming ML inference,
    Kafka results publishing, and HDFS batch storage.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        input_topic: str,
        output_topic: str,
        group_id: str,
        webhdfs_url: str,
        hdfs_user: str,
        model_path: str,
        batch_flush_size: int = 5,
        batch_flush_interval: float = 5.0,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.group_id = group_id
        self.batch_flush_size = batch_flush_size
        self.batch_flush_interval = batch_flush_interval

        # 1. Initialize ML Predictor
        logger.info(f"Initializing Sentiment Predictor with model at: {model_path}")
        self.predictor = SentimentPredictor(model_path=model_path, auto_train=True)

        # 2. Initialize HDFS Writer
        logger.info(f"Initializing HDFS Writer pointing to {webhdfs_url}")
        self.hdfs_writer = HDFSWriter(webhdfs_url=webhdfs_url, hdfs_user=hdfs_user)
        self.hdfs_writer.ensure_directory("movie-sentiment/raw")
        self.hdfs_writer.ensure_directory("movie-sentiment/processed")
        self.hdfs_writer.ensure_directory("movie-sentiment/metrics")
        self.hdfs_writer.remove_legacy_zone_files()

        # 3. Initialize Metrics Manager
        metrics_file = os.getenv("METRICS_DATA_PATH", "artifacts/live_metrics.json")
        self.metrics_mgr = SentimentMetricsManager(storage_path=metrics_file)

        # Buffers for HDFS batch flushing
        self.raw_buffer: List[Dict[str, Any]] = []
        self.processed_buffer: List[Dict[str, Any]] = []
        self.last_flush_time = time.time()

        # Connect Kafka
        self.consumer = connect_consumer(bootstrap_servers, input_topic, group_id)
        self.producer = connect_producer(bootstrap_servers)

    def process_message(self, raw_msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Validates incoming message and scores review text with TF-IDF + Logistic Regression.
        """
        if not isinstance(raw_msg, dict):
            logger.warning(f"Ignored non-dictionary message: {raw_msg}")
            return None

        review_text = raw_msg.get("review_text", "")
        if not review_text or not isinstance(review_text, str):
            logger.warning(f"Ignored message with invalid review_text: {raw_msg}")
            return None

        # Predict sentiment
        pred = self.predictor.predict_one(review_text)

        processed_msg = {
            "review_id": raw_msg.get("review_id", f"rev-{int(time.time()*1000)}"),
            "movie_id": raw_msg.get("movie_id", "UNKNOWN"),
            "movie_title": raw_msg.get("movie_title", "Unknown Title"),
            "review_text": review_text,
            "predicted_sentiment": pred["predicted_sentiment"],
            "positive_probability": pred["positive_probability"],
            "sentiment_score": pred["sentiment_score"],
            "created_at": raw_msg.get("created_at", datetime.utcnow().isoformat() + "Z"),
            "processed_at": datetime.utcnow().isoformat() + "Z",
        }
        return processed_msg

    def flush_to_hdfs(self) -> None:
        """
        Writes accumulated raw and processed records to HDFS and updates metrics.
        """
        if not self.raw_buffer and not self.processed_buffer:
            return

        raw_path = "movie-sentiment/raw/reviews.jsonl"
        processed_path = "movie-sentiment/processed/scored.jsonl"
        metrics_path = "movie-sentiment/metrics/summary.json"

        # Write to HDFS
        if self.raw_buffer:
            self.hdfs_writer.append_jsonl_records(raw_path, self.raw_buffer)
            self.raw_buffer.clear()

        if self.processed_buffer:
            # 1. Write processed data
            self.hdfs_writer.append_jsonl_records(processed_path, self.processed_buffer)
            # 2. Update aggregations
            self.metrics_mgr.update_with_records(self.processed_buffer)
            # 3. Write latest metrics summary to HDFS
            summary = self.metrics_mgr.get_summary()
            self.hdfs_writer.write_json(metrics_path, summary, overwrite=True)
            self.processed_buffer.clear()

        self.last_flush_time = time.time()
        logger.info(f"Flushed batch to HDFS. Total processed so far: {self.metrics_mgr.total_processed}")

    def run(self) -> None:
        """Main consumer loop."""
        logger.info(f"Consumer listening on '{self.input_topic}'...")
        try:
            while True:
                # Poll messages
                msg_pack = self.consumer.poll(timeout_ms=1000, max_records=20)
                for tp, messages in msg_pack.items():
                    for msg in messages:
                        try:
                            raw_data = msg.value
                            scored_data = self.process_message(raw_data)
                            if scored_data:
                                # 1. Publish to Kafka output topic
                                key = scored_data["movie_id"]
                                self.producer.send(self.output_topic, key=key, value=scored_data)

                                # 2. Add to HDFS buffers
                                self.raw_buffer.append(raw_data)
                                self.processed_buffer.append(scored_data)

                                logger.info(
                                    f"Scored review '{scored_data['review_id']}' for '{scored_data['movie_title']}': "
                                    f"{scored_data['predicted_sentiment'].upper()} (score: {scored_data['sentiment_score']:.2f}, "
                                    f"prob: {scored_data['positive_probability']:.2f})"
                                )
                        except Exception as e:
                            logger.error(f"Error processing record at offset {msg.offset}: {e}", exc_info=True)

                # Check if buffer flush is required
                time_since_flush = time.time() - self.last_flush_time
                if len(self.processed_buffer) >= self.batch_flush_size or (
                    self.processed_buffer and time_since_flush >= self.batch_flush_interval
                ):
                    self.flush_to_hdfs()

        except KeyboardInterrupt:
            logger.info("Consumer stopping on user interrupt...")
        finally:
            self.flush_to_hdfs()
            self.consumer.close()
            self.producer.close()
            logger.info("Consumer shutdown complete.")


def main():
    parser = argparse.ArgumentParser(description="Movie Review Sentiment Consumer")
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument(
        "--input-topic",
        type=str,
        default=os.getenv("KAFKA_INPUT_TOPIC", "movie-reviews"),
    )
    parser.add_argument(
        "--output-topic",
        type=str,
        default=os.getenv("KAFKA_OUTPUT_TOPIC", "movie-sentiment-results"),
    )
    parser.add_argument(
        "--group-id",
        type=str,
        default=os.getenv("KAFKA_CONSUMER_GROUP", "movie-sentiment-group"),
    )
    parser.add_argument(
        "--webhdfs-url",
        type=str,
        default=os.getenv("WEBHDFS_URL", "http://namenode:9870/webhdfs/v1"),
    )
    parser.add_argument(
        "--hdfs-user",
        type=str,
        default=os.getenv("HDFS_USER", "root"),
    )
    parser.add_argument(
        "--model-path",
        type=str,
        default=os.getenv("MODEL_PATH", "artifacts/sentiment_model.joblib"),
    )
    args = parser.parse_args()

    pipeline = MovieSentimentPipelineConsumer(
        bootstrap_servers=args.bootstrap_servers,
        input_topic=args.input_topic,
        output_topic=args.output_topic,
        group_id=args.group_id,
        webhdfs_url=args.webhdfs_url,
        hdfs_user=args.hdfs_user,
        model_path=args.model_path,
    )
    pipeline.run()


if __name__ == "__main__":
    main()
