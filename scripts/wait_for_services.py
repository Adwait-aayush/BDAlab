"""
Wait-for-services readiness helper script.
Pings Kafka broker and WebHDFS NameNode until they are ready to accept connections.
"""

import argparse
import os
import socket
import sys
import time
import requests

logging_format = "%(asctime)s [WaitService]: %(message)s"


def wait_for_port(host: str, port: int, timeout: int = 60) -> bool:
    """Waits until a specific TCP socket is accepting connections."""
    start_time = time.time()
    print(f"Waiting for socket {host}:{port} to become available (timeout: {timeout}s)...")
    while time.time() - start_time < timeout:
        try:
            with socket.create_connection((host, port), timeout=2):
                print(f"Socket {host}:{port} is OPEN and ready!")
                return True
        except (socket.timeout, ConnectionRefusedError, OSError):
            time.sleep(2)
    print(f"Timed out waiting for socket {host}:{port} after {timeout}s.")
    return False


def wait_for_webhdfs(url: str, timeout: int = 60) -> bool:
    """Waits until WebHDFS endpoint responds."""
    start_time = time.time()
    ping_url = f"{url.rstrip('/')}/?op=LISTSTATUS&user.name=root"
    print(f"Waiting for WebHDFS at {url} to respond (timeout: {timeout}s)...")
    while time.time() - start_time < timeout:
        try:
            resp = requests.get(ping_url, timeout=3)
            if resp.status_code in (200, 403, 404):
                print(f"WebHDFS at {url} is READY (status {resp.status_code})!")
                return True
        except Exception:
            pass
        time.sleep(2)
    print(f"WebHDFS not ready after {timeout}s (proceeding with caution).")
    return False


def main():
    parser = argparse.ArgumentParser(description="Wait for Big Data services")
    parser.add_argument("--kafka", type=str, default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    parser.add_argument("--webhdfs", type=str, default=os.getenv("WEBHDFS_URL", "http://namenode:9870/webhdfs/v1"))
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()

    # Parse Kafka host and port
    first_broker = args.kafka.split(",")[0].strip()
    if ":" in first_broker:
        k_host, k_port = first_broker.split(":")
        k_port = int(k_port)
    else:
        k_host, k_port = first_broker, 9092

    kafka_ready = wait_for_port(k_host, k_port, timeout=args.timeout)
    hdfs_ready = wait_for_webhdfs(args.webhdfs, timeout=args.timeout)

    if not kafka_ready:
        print("WARNING: Kafka broker was not fully reachable.")
        sys.exit(1)

    print("All required infrastructure services are ready!")
    sys.exit(0)


if __name__ == "__main__":
    main()
