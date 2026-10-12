"""
Kafka Producer for Streaming Movie Reviews.
Ingests movie reviews from sample JSONL or synthetic generator and publishes
them to the 'movie-reviews' Kafka topic with configurable delays and finite batch support.
"""

import argparse
import json
import logging
import os
import random
import sys
import time
from datetime import datetime
from typing import Dict, Any, Generator, List

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Producer]: %(message)s"
)
logger = logging.getLogger("movie_review_producer")

SAMPLE_MOVIES = [
    ("MOV-001", "Inception"),
    ("MOV-002", "The Godfather"),
    ("MOV-003", "Interstellar"),
    ("MOV-004", "The Matrix"),
    ("MOV-005", "The Room"),
    ("MOV-006", "Parasite"),
    ("MOV-007", "Cats (2019)"),
    ("MOV-008", "Pulp Fiction"),
    ("MOV-009", "The Dark Knight"),
    ("MOV-010", "Dune: Part Two"),
]

POSITIVE_TEMPLATES = [
    "An absolute masterpiece with incredible performances and brilliant directing.",
    "A visually stunning and deeply emotional cinematic experience. Loved it!",
    "Superb storyline, captivating pacing, and unforgettable acting from the whole cast.",
    "Utterly brilliant! A triumph of modern cinema that exceeds every expectation.",
    "Highly recommended! The soundtrack and cinematography alone are worth the price.",
    "Astonishing storytelling and powerful emotional resonance. 10 out of 10.",
]

NEGATIVE_TEMPLATES = [
    "A complete waste of time. Dull characters, boring plot, and terrible pacing.",
    "Horrendously written dialogue and lifeless wooden acting throughout.",
    "Disaster of a film. Painfully slow and completely fails to entertain.",
    "Uninspired, tedious, and cringe-inducing. The worst movie I have seen this year.",
    "Lacks any coherent narrative. Cheap effects and dreadful execution.",
    "A total mess with zero redeeming qualities. Avoid this at all costs.",
]


def load_file_reviews(filepath: str) -> List[Dict[str, Any]]:
    """Loads reviews from a jsonl file."""
    reviews = []
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        record = json.loads(line)
                        reviews.append(record)
                    except json.JSONDecodeError:
                        logger.warning(f"Skipping malformed json line: {line}")
    return reviews


def generate_synthetic_review(seq_id: int) -> Dict[str, Any]:
    """Generates a synthetic review message adhering strictly to the specification schema."""
    movie_id, movie_title = random.choice(SAMPLE_MOVIES)
    is_pos = random.random() > 0.45
    templates = POSITIVE_TEMPLATES if is_pos else NEGATIVE_TEMPLATES
    text = random.choice(templates)

    return {
        "review_id": f"rev-synth-{seq_id:05d}",
        "movie_id": movie_id,
        "movie_title": movie_title,
        "review_text": text,
        "source": "synthetic",
        "created_at": datetime.utcnow().isoformat() + "Z",
    }


def review_stream_generator(filepath: str, max_count: int = None) -> Generator[Dict[str, Any], None, None]:
    """
    Yields review records. If file reviews are exhausted, generates synthetic reviews.
    """
    file_records = load_file_reviews(filepath)
    count = 0
    seq_id = 1000

    # First stream known file records
    for record in file_records:
        yield record
        count += 1
        if max_count and count >= max_count:
            return

    # Then stream synthetic records
    while True:
        seq_id += 1
        yield generate_synthetic_review(seq_id)
        count += 1
        if max_count and count >= max_count:
            return


def create_kafka_producer(bootstrap_servers: str, max_retries: int = 15, retry_interval: float = 2.0) -> KafkaProducer:
    """
    Connects to Kafka with retry logic for container readiness.
    """
    servers = [s.strip() for s in bootstrap_servers.split(",")]
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Connecting to Kafka brokers at {servers} (Attempt {attempt}/{max_retries})...")
            producer = KafkaProducer(
                bootstrap_servers=servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                retries=5,
                request_timeout_ms=10000,
            )
            logger.info("Kafka Producer successfully connected!")
            return producer
        except NoBrokersAvailable as e:
            logger.warning(f"Brokers not available yet: {e}. Retrying in {retry_interval}s...")
            time.sleep(retry_interval)
        except Exception as e:
            logger.warning(f"Connection attempt failed ({e}). Retrying in {retry_interval}s...")
            time.sleep(retry_interval)

    raise RuntimeError(f"Could not connect to Kafka at {bootstrap_servers} after {max_retries} attempts.")


def run_producer(
    bootstrap_servers: str,
    topic: str,
    mode: str = "stream",
    delay: float = 1.0,
    batch_size: int = 20,
    sample_file: str = "producer/sample_reviews.jsonl",
):
    """
    Executes the producer loop.
    """
    logger.info(f"Starting producer | Topic: {topic} | Mode: {mode} | Delay: {delay}s | BatchSize: {batch_size}")
    producer = create_kafka_producer(bootstrap_servers)

    max_count = batch_size if mode == "batch" else None
    stream = review_stream_generator(sample_file, max_count=max_count)

    sent = 0
    try:
        for review in stream:
            key = review.get("movie_id", "default")
            future = producer.send(topic, key=key, value=review)
            record_metadata = future.get(timeout=10)

            sent += 1
            logger.info(
                f"[{sent}] Sent review_id='{review.get('review_id')}' | Movie='{review.get('movie_title')}' | "
                f"Partition={record_metadata.partition} Offset={record_metadata.offset}"
            )

            if mode == "batch" and sent >= batch_size:
                logger.info(f"Completed finite batch of {sent} reviews.")
                break

            if delay > 0:
                time.sleep(delay)

    except KeyboardInterrupt:
        logger.info("Producer interrupted by user.")
    finally:
        producer.flush()
        producer.close()
        logger.info(f"Producer closed. Total messages sent: {sent}")


def main():
    parser = argparse.ArgumentParser(description="Movie Review Kafka Producer")
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers comma-separated",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default=os.getenv("KAFKA_INPUT_TOPIC", "movie-reviews"),
        help="Target Kafka topic",
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["stream", "batch"],
        default=os.getenv("PRODUCER_MODE", "stream"),
        help="Running mode: continuous stream or finite batch",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=float(os.getenv("PRODUCER_STREAM_DELAY_SECONDS", "1.0")),
        help="Delay between review messages in seconds",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=int(os.getenv("PRODUCER_BATCH_SIZE", "20")),
        help="Number of reviews to send in batch mode",
    )
    parser.add_argument(
        "--sample-file",
        type=str,
        default=os.getenv("SAMPLE_REVIEWS_FILE", "producer/sample_reviews.jsonl"),
        help="Path to sample jsonl file",
    )
    args = parser.parse_args()

    run_producer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        mode=args.mode,
        delay=args.delay,
        batch_size=args.batch_size,
        sample_file=args.sample_file,
    )


if __name__ == "__main__":
    main()
