"""
Hadoop HDFS Writer Module via WebHDFS REST API.
Handles directory creation, file creation, appending, reading, and status listing.
Includes automatic redirect handling between NameNode and DataNode,
and maintains local mirrored persistence for uninterrupted college demo execution.
"""

import json
import logging
import os
from datetime import datetime
from typing import Dict, Any, List, Optional
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [HDFSWriter]: %(message)s"
)
logger = logging.getLogger("hdfs_writer")


class HDFSWriter:
    """
    Python-to-HDFS client communicating directly with WebHDFS REST API.
    Works seamlessly across all standard Apache Hadoop distributions.
    """

    def __init__(
        self,
        webhdfs_url: str = "http://namenode:9870/webhdfs/v1",
        hdfs_user: str = "root",
        local_mirror_dir: str = "data/hdfs_mirror",
    ):
        self.webhdfs_url = webhdfs_url.rstrip("/")
        self.hdfs_user = hdfs_user
        self.local_mirror_dir = local_mirror_dir
        os.makedirs(self.local_mirror_dir, exist_ok=True)
        self.session = requests.Session()

    def is_available(self) -> bool:
        """Checks if NameNode WebHDFS endpoint is reachable."""
        try:
            url = f"{self.webhdfs_url}/?op=LISTSTATUS&user.name={self.hdfs_user}"
            resp = self.session.get(url, timeout=3)
            return resp.status_code in (200, 403, 404)
        except Exception:
            return False

    def ensure_directory(self, hdfs_dir: str) -> bool:
        """
        Creates directory in HDFS if it does not already exist (op=MKDIRS).
        """
        hdfs_dir = hdfs_dir.strip("/")
        # Local mirror
        os.makedirs(os.path.join(self.local_mirror_dir, hdfs_dir), exist_ok=True)

        url = f"{self.webhdfs_url}/{hdfs_dir}?op=MKDIRS&user.name={self.hdfs_user}"
        try:
            resp = self.session.put(url, timeout=5)
            if resp.status_code in (200, 201):
                logger.info(f"Verified HDFS directory: /{hdfs_dir}")
                return True
            else:
                logger.warning(f"MKDIRS on /{hdfs_dir} returned {resp.status_code}: {resp.text}")
                return False
        except Exception as e:
            logger.warning(f"Could not reach WebHDFS to MKDIRS /{hdfs_dir} ({e}). Local directory created.")
            return False

    def delete_file(self, hdfs_path: str) -> bool:
        """Delete one HDFS file and its local mirror counterpart."""
        hdfs_path = hdfs_path.strip("/")
        local_path = os.path.join(self.local_mirror_dir, hdfs_path)
        try:
                if os.path.exists(local_path):
                    os.remove(local_path)
        except OSError as exc:
                logger.warning("Could not remove local mirror %s: %s", hdfs_path, exc)

        url = f"{self.webhdfs_url}/{hdfs_path}?op=DELETE&recursive=false&user.name={self.hdfs_user}"
        try:
                response = self.session.delete(url, timeout=5)
                return response.status_code in (200, 404)
        except requests.RequestException as exc:
                logger.warning("Could not delete HDFS file /%s: %s", hdfs_path, exc)
                return False

    def remove_legacy_zone_files(self) -> None:
        """Keep one canonical file per data zone and remove date-suffixed files."""
        zones = {
                "movie-sentiment/raw": "reviews.jsonl",
                "movie-sentiment/processed": "scored.jsonl",
                "movie-sentiment/metrics": "summary.json",
        }
        for zone, canonical_name in zones.items():
                local_zone = os.path.join(self.local_mirror_dir, zone)
                if os.path.isdir(local_zone):
                    for name in os.listdir(local_zone):
                        if name != canonical_name:
                            self.delete_file(f"{zone}/{name}")
                url = f"{self.webhdfs_url}/{zone}?op=LISTSTATUS&user.name={self.hdfs_user}"
                try:
                    response = self.session.get(url, timeout=5)
                    response.raise_for_status()
                    entries = response.json().get("FileStatuses", {}).get("FileStatus", [])
                    for entry in entries:
                        name = entry.get("pathSuffix")
                        if name and name != canonical_name and entry.get("type") == "FILE":
                            self.delete_file(f"{zone}/{name}")
                except (requests.RequestException, ValueError, TypeError):
                    logger.info("Could not inspect legacy files in /%s", zone)
    def write_jsonl_records(self, hdfs_path: str, records: List[Dict[str, Any]], overwrite: bool = False) -> bool:
        """
        Writes a list of JSON records to an HDFS file (in JSONL format).
        Handles NameNode 307 redirect to DataNode.
        Also persists to local mirror to guarantee zero data loss.
        """
        if not records:
            return True

        hdfs_path = hdfs_path.strip("/")
        payload_lines = "\n".join([json.dumps(r) for r in records]) + "\n"
        payload_bytes = payload_lines.encode("utf-8")

        # 1. Always update local mirror
        local_path = os.path.join(self.local_mirror_dir, hdfs_path)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        mode = "w" if overwrite or not os.path.exists(local_path) else "a"
        with open(local_path, mode, encoding="utf-8") as f:
            f.write(payload_lines)

        # 2. Write to WebHDFS
        url = f"{self.webhdfs_url}/{hdfs_path}?op=CREATE&overwrite={str(overwrite).lower()}&user.name={self.hdfs_user}"
        try:
            # Step 1: PUT to NameNode with allow_redirects=False to obtain DataNode Location
            resp = self.session.put(url, allow_redirects=False, timeout=5)
            if resp.status_code == 307:
                datanode_location = resp.headers.get("Location")
                if datanode_location:
                    # Step 2: PUT payload to DataNode
                    dn_resp = self.session.put(
                        datanode_location,
                        data=payload_bytes,
                        headers={"Content-Type": "application/octet-stream"},
                        timeout=10,
                    )
                    if dn_resp.status_code in (200, 201):
                        logger.info(f"Wrote {len(records)} records to HDFS /{hdfs_path}")
                        return True
                    else:
                        logger.warning(f"DataNode write returned status {dn_resp.status_code}: {dn_resp.text}")
                        return False
            elif resp.status_code in (200, 201):
                logger.info(f"Wrote {len(records)} records directly to HDFS /{hdfs_path}")
                return True
            else:
                logger.warning(f"WebHDFS NameNode returned status {resp.status_code} for /{hdfs_path}")
                return False
        except Exception as e:
            logger.warning(f"WebHDFS write failed for /{hdfs_path} ({e}). Persisted in local mirror.")
            return False

    def append_jsonl_records(self, hdfs_path: str, records: List[Dict[str, Any]]) -> bool:
        """Append JSONL records to one canonical HDFS file."""
        if not records:
            return True
        hdfs_path = hdfs_path.strip("/")
        payload = ("\n".join(json.dumps(record) for record in records) + "\n").encode("utf-8")
        local_path = os.path.join(self.local_mirror_dir, hdfs_path)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "ab") as handle:
            handle.write(payload)

        url = f"{self.webhdfs_url}/{hdfs_path}?op=APPEND&user.name={self.hdfs_user}"
        try:
            response = self.session.post(url, allow_redirects=False, timeout=5)
            redirect_method = "post"
            if response.status_code == 404:
                create_url = f"{self.webhdfs_url}/{hdfs_path}?op=CREATE&overwrite=true&user.name={self.hdfs_user}"
                response = self.session.put(create_url, allow_redirects=False, timeout=5)
                redirect_method = "put"
            if response.status_code == 307 and response.headers.get("Location"):
                request = self.session.put if redirect_method == "put" else self.session.post
                response = request(response.headers["Location"], data=payload,
                                   headers={"Content-Type": "application/octet-stream"}, timeout=10)
            if response.status_code in (200, 201):
                return True
            logger.warning("WebHDFS append returned %s for /%s", response.status_code, hdfs_path)
        except requests.RequestException as exc:
            logger.warning("WebHDFS append failed for /%s: %s", hdfs_path, exc)
        return False

    def write_json(self, hdfs_path: str, data: Any, overwrite: bool = True) -> bool:
        """
        Writes a JSON object / summary to HDFS.
        """
        hdfs_path = hdfs_path.strip("/")
        payload_bytes = json.dumps(data, indent=2).encode("utf-8")

        # Local mirror
        local_path = os.path.join(self.local_mirror_dir, hdfs_path)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "w", encoding="utf-8") as f:
            f.write(json.dumps(data, indent=2))

        url = f"{self.webhdfs_url}/{hdfs_path}?op=CREATE&overwrite={str(overwrite).lower()}&user.name={self.hdfs_user}"
        try:
            resp = self.session.put(url, allow_redirects=False, timeout=5)
            if resp.status_code == 307:
                location = resp.headers.get("Location")
                if location:
                    dn_resp = self.session.put(
                        location,
                        data=payload_bytes,
                        headers={"Content-Type": "application/json"},
                        timeout=10,
                    )
                    return dn_resp.status_code in (200, 201)
            return resp.status_code in (200, 201)
        except Exception as e:
            logger.warning(f"WebHDFS write_json failed for /{hdfs_path} ({e})")
            return False

    def read_file(self, hdfs_path: str) -> Optional[str]:
        """
        Reads file content from HDFS via WebHDFS op=OPEN, falling back to local mirror.
        """
        hdfs_path = hdfs_path.strip("/")
        url = f"{self.webhdfs_url}/{hdfs_path}?op=OPEN&user.name={self.hdfs_user}"
        try:
            resp = self.session.get(url, timeout=5)
            if resp.status_code == 200:
                return resp.text
        except Exception:
            pass

        # Fallback to local mirror
        local_path = os.path.join(self.local_mirror_dir, hdfs_path)
        if os.path.exists(local_path):
            with open(local_path, "r", encoding="utf-8") as f:
                return f.read()

        return None
