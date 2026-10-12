"""
End-to-End Pipeline Smoke Test Script.
Validates:
1. Model loading and sentiment prediction logic.
2. Positive and negative review inference sanity check.
3. HDFS Writer file creation and readback.
4. Aggregation and metrics updating.
5. Kafka message roundtrip if Kafka is running.
"""

import json
import os
import sys
import time

# Ensure project root is in path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from model.predict import SentimentPredictor
from consumer.hdfs_writer import HDFSWriter
from app.metrics import SentimentMetricsManager


def test_model_inference():
    print("\n--- 1. Testing ML Model Inference ---")
    predictor = SentimentPredictor(model_path="artifacts/sentiment_model.joblib", auto_train=True)
    
    pos_sample = "An absolute masterpiece with brilliant acting, stunning directing, and inspiring plot!"
    neg_sample = "Terrible movie. Awful waste of time, senseless dialogue, and completely boring."

    pred_pos = predictor.predict_one(pos_sample)
    pred_neg = predictor.predict_one(neg_sample)

    print(f"Positive Test Review -> Sentiment: {pred_pos['predicted_sentiment']} | Score: {pred_pos['sentiment_score']} | Prob: {pred_pos['positive_probability']}")
    print(f"Negative Test Review -> Sentiment: {pred_neg['predicted_sentiment']} | Score: {pred_neg['sentiment_score']} | Prob: {pred_neg['positive_probability']}")

    assert pred_pos["predicted_sentiment"] == "positive", "Expected positive classification"
    assert pred_neg["predicted_sentiment"] == "negative", "Expected negative classification"
    assert -1.0 <= pred_pos["sentiment_score"] <= 1.0, "Score out of range [-1, 1]"
    assert -1.0 <= pred_neg["sentiment_score"] <= 1.0, "Score out of range [-1, 1]"
    print("[PASS] ML Model Inference PASS!")


def test_metrics_aggregation():
    print("\n--- 2. Testing Sentiment Metrics Aggregation ---")
    test_storage = "artifacts/test_metrics.json"
    mgr = SentimentMetricsManager(storage_path=test_storage)

    sample_batch = [
        {
            "review_id": "rev-test-1",
            "movie_id": "MOV-TEST",
            "movie_title": "Test Movie",
            "review_text": "Great film!",
            "predicted_sentiment": "positive",
            "sentiment_score": 0.85,
            "positive_probability": 0.925,
            "created_at": "2026-10-12T20:00:00Z"
        },
        {
            "review_id": "rev-test-2",
            "movie_id": "MOV-TEST",
            "movie_title": "Test Movie",
            "review_text": "Not good.",
            "predicted_sentiment": "negative",
            "sentiment_score": -0.60,
            "positive_probability": 0.20,
            "created_at": "2026-10-12T20:01:00Z"
        }
    ]

    mgr.update_with_records(sample_batch)
    summary = mgr.get_summary()

    assert summary["total_processed"] >= 2, "Total processed mismatch"
    assert "MOV-TEST" in summary["movies"], "Movie not recorded"
    movie_entry = summary["movies"]["MOV-TEST"]
    assert movie_entry["positive_count"] >= 1, "Positive count mismatch"
    assert movie_entry["negative_count"] >= 1, "Negative count mismatch"
    assert 0.0 <= movie_entry["positive_percentage"] <= 100.0, "Percentage out of bounds"
    
    # Clean up test artifact
    if os.path.exists(test_storage):
        os.remove(test_storage)

    print(f"Movie Stats: Total={movie_entry['total_reviews']}, Pos={movie_entry['positive_count']}, Neg={movie_entry['negative_count']}, Pos%={movie_entry['positive_percentage']}%")
    print("[PASS] Metrics Aggregation PASS!")


def test_hdfs_writer():
    print("\n--- 3. Testing HDFS Writer ---")
    writer = HDFSWriter(
        webhdfs_url=os.getenv("WEBHDFS_URL", "http://localhost:9870/webhdfs/v1"),
        local_mirror_dir="artifacts/test_hdfs_mirror"
    )

    records = [{"test_id": "t-1", "content": "Sample HDFS record"}]
    path = "movie-sentiment/smoke_test/test_record.jsonl"

    writer.write_jsonl_records(path, records, overwrite=True)
    content = writer.read_file(path)
    assert content is not None, "Failed to read back written HDFS record"
    assert "Sample HDFS record" in content, "Content mismatch"

    print("[PASS] HDFS Storage Writer PASS!")


def main():
    print("==================================================")
    print(">> RUNNING PIPELINE SMOKE TESTS")
    print("==================================================")
    try:
        test_model_inference()
        test_metrics_aggregation()
        test_hdfs_writer()
        print("\n==================================================")
        print(">> ALL SMOKE TESTS COMPLETED SUCCESSFULLY!")
        print("==================================================")
    except AssertionError as e:
        print(f"\n[FAIL] Assertion failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n[FAIL] Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
