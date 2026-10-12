"""
Movie Sentiment Aggregation and Statistics Engine.
Calculates per-movie and global sentiment metrics:
- Total reviews
- Positive / negative counts and percentages
- Mean sentiment score
- Distribution over time
- Recent scored reviews
Handles edge cases (0 reviews, missing values) with zero-division safety.
"""

import json
import logging
import os
from datetime import datetime
from typing import Dict, Any, List, Optional
import pandas as pd

logger = logging.getLogger("sentiment_metrics")


class SentimentMetricsManager:
    """
    Manages running sentiment analytics aggregated by movie and system-wide.
    """

    def __init__(self, storage_path: str = "artifacts/live_metrics.json"):
        self.storage_path = storage_path
        self.movies: Dict[str, Dict[str, Any]] = {}
        self.recent_reviews: List[Dict[str, Any]] = []
        self.timeline: List[Dict[str, Any]] = []
        self.total_processed: int = 0
        self.total_positive: int = 0
        self.total_negative: int = 0
        self.load()

    def update_with_records(self, records: List[Dict[str, Any]]) -> None:
        """
        Incrementally aggregates a batch of newly scored review records.
        """
        for r in records:
            if not isinstance(r, dict):
                continue

            movie_id = str(r.get("movie_id", "UNKNOWN"))
            movie_title = str(r.get("movie_title", "Unknown Title"))
            sentiment = str(r.get("predicted_sentiment", "negative")).lower()
            score = float(r.get("sentiment_score", 0.0))
            created_at = r.get("created_at") or r.get("processed_at") or datetime.utcnow().isoformat() + "Z"

            is_positive = sentiment == "positive"

            # Global counters
            self.total_processed += 1
            if is_positive:
                self.total_positive += 1
            else:
                self.total_negative += 1

            # Per-movie counters
            if movie_id not in self.movies:
                self.movies[movie_id] = {
                    "movie_id": movie_id,
                    "movie_title": movie_title,
                    "total_reviews": 0,
                    "positive_count": 0,
                    "negative_count": 0,
                    "positive_percentage": 0.0,
                    "negative_percentage": 0.0,
                    "scores_sum": 0.0,
                    "mean_sentiment_score": 0.0,
                    "last_updated": created_at,
                }

            entry = self.movies[movie_id]
            entry["total_reviews"] += 1
            if is_positive:
                entry["positive_count"] += 1
            else:
                entry["negative_count"] += 1

            entry["scores_sum"] += score
            total_m = entry["total_reviews"]
            entry["positive_percentage"] = round((entry["positive_count"] / total_m) * 100.0, 2)
            entry["negative_percentage"] = round((entry["negative_count"] / total_m) * 100.0, 2)
            entry["mean_sentiment_score"] = round(entry["scores_sum"] / total_m, 4)
            entry["last_updated"] = created_at

            # Add to timeline for time-series charts
            self.timeline.append({
                "timestamp": created_at,
                "movie_id": movie_id,
                "movie_title": movie_title,
                "sentiment": sentiment,
                "sentiment_score": score,
            })

            # Add to recent reviews list
            self.recent_reviews.insert(0, {
                "review_id": r.get("review_id", f"rev-{self.total_processed}"),
                "movie_id": movie_id,
                "movie_title": movie_title,
                "review_text": r.get("review_text", ""),
                "predicted_sentiment": sentiment,
                "sentiment_score": score,
                "positive_probability": r.get("positive_probability", 0.5),
                "created_at": created_at,
            })

        # Cap memory of recent records and timeline
        self.recent_reviews = self.recent_reviews[:100]
        self.timeline = self.timeline[-500:]

        self.save()

    def get_summary(self) -> Dict[str, Any]:
        """
        Returns full serializable metrics report.
        """
        global_pos_pct = (
            round((self.total_positive / self.total_processed) * 100.0, 2)
            if self.total_processed > 0
            else 0.0
        )
        global_neg_pct = (
            round((self.total_negative / self.total_processed) * 100.0, 2)
            if self.total_processed > 0
            else 0.0
        )

        scores = [item["sentiment_score"] for item in self.timeline]
        overall_mean_score = round(sum(scores) / len(scores), 4) if scores else 0.0

        return {
            "total_processed": self.total_processed,
            "total_positive": self.total_positive,
            "total_negative": self.total_negative,
            "global_positive_percentage": global_pos_pct,
            "global_negative_percentage": global_neg_pct,
            "overall_mean_score": overall_mean_score,
            "movies": self.movies,
            "recent_reviews": self.recent_reviews,
            "timeline": self.timeline,
            "updated_at": datetime.utcnow().isoformat() + "Z",
        }

    def save(self) -> None:
        """Persists current state to json file."""
        try:
            os.makedirs(os.path.dirname(os.path.abspath(self.storage_path)), exist_ok=True)
            with open(self.storage_path, "w", encoding="utf-8") as f:
                json.dump(self.get_summary(), f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to persist metrics to {self.storage_path}: {e}")

    def load(self) -> None:
        """Loads metrics from storage if available."""
        if os.path.exists(self.storage_path):
            try:
                with open(self.storage_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.total_processed = data.get("total_processed", 0)
                    self.total_positive = data.get("total_positive", 0)
                    self.total_negative = data.get("total_negative", 0)
                    self.movies = data.get("movies", {})
                    self.recent_reviews = data.get("recent_reviews", [])
                    self.timeline = data.get("timeline", [])
            except Exception as e:
                logger.warning(f"Could not load existing metrics from {self.storage_path}: {e}")

    def get_movies_dataframe(self) -> pd.DataFrame:
        """Returns a Pandas DataFrame of per-movie statistics."""
        if not self.movies:
            return pd.DataFrame(columns=[
                "movie_id", "movie_title", "total_reviews", "positive_count",
                "negative_count", "positive_percentage", "negative_percentage",
                "mean_sentiment_score"
            ])
        rows = list(self.movies.values())
        df = pd.DataFrame(rows)
        # Drop internal summation column
        if "scores_sum" in df.columns:
            df = df.drop(columns=["scores_sum"])
        return df
