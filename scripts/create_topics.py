"""
Kafka Topic Creation Script.
Automatically creates the required topics:
- movie-reviews
- movie-sentiment-results
Handles TopicAlreadyExistsError gracefully.
"""

import argparse
import os
import sys
import time
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable


def create_topics(bootstrap_servers: str, topics: list, max_retries: int = 15):
    servers = [s.strip() for s in bootstrap_servers.split(",")]
    admin = None
    for attempt in range(1, max_retries + 1):
        try:
            print(f"Connecting KafkaAdminClient to {servers} (Attempt {attempt}/{max_retries})...")
            admin = KafkaAdminClient(
                bootstrap_servers=servers,
                client_id="topic_creator"
            )
            break
        except NoBrokersAvailable:
            time.sleep(2)
        except Exception as e:
            print(f"Admin connection error ({e}). Retrying in 2s...")
            time.sleep(2)

    if not admin:
        print(f"Failed to connect to Kafka at {bootstrap_servers}")
        sys.exit(1)

    new_topics = []
    for topic_name in topics:
        new_topics.append(NewTopic(name=topic_name, num_partitions=1, replication_factor=1))

    try:
        existing = admin.list_topics()
        to_create = [t for t in new_topics if t.name not in existing]
        if to_create:
            admin.create_topics(new_topics=to_create, validate_only=False)
            print(f"Successfully created topics: {[t.name for t in to_create]}")
        else:
            print("All required topics already exist.")
    except TopicAlreadyExistsError:
        print("Topics already exist.")
    except Exception as e:
        print(f"Topic creation warning: {e}")
    finally:
        admin.close()


def main():
    parser = argparse.ArgumentParser(description="Create Kafka Topics")
    parser.add_argument("--bootstrap-servers", type=str, default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    parser.add_argument("--topics", nargs="+", default=["movie-reviews", "movie-sentiment-results"])
    args = parser.parse_args()

    create_topics(args.bootstrap_servers, args.topics)


if __name__ == "__main__":
    main()
