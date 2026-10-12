"""
Model Training Script for Movie Review Sentiment Classification.
Combines TF-IDF Feature Extraction with Logistic Regression.
Evaluates accuracy, precision, recall, F1, and confusion matrix.
Persists the fitted pipeline to an artifact file.
"""

import argparse
import os
import sys
import logging
from datetime import datetime
from typing import Dict, Any, Tuple

import joblib
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    confusion_matrix,
    classification_report,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline

# Ensure project root is in sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from model.preprocessing import clean_text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("model_trainer")


def load_dataset(data_path: str) -> pd.DataFrame:
    """
    Loads dataset from CSV. Handles standard column names ('review', 'sentiment')
    or falls back to sample dataset if the primary full dataset is not yet downloaded.
    """
    if os.path.exists(data_path):
        logger.info(f"Loading dataset from: {data_path}")
        df = pd.read_csv(data_path)
    else:
        fallback_path = os.path.join(
            os.path.dirname(__file__), "..", "data", "sample_imdb.csv"
        )
        if os.path.exists(fallback_path):
            logger.warning(
                f"Dataset not found at '{data_path}'. Using fallback sample dataset: {fallback_path}"
            )
            df = pd.read_csv(fallback_path)
        else:
            raise FileNotFoundError(
                f"Neither requested dataset '{data_path}' nor fallback '{fallback_path}' exists."
            )

    # Standardize column names
    col_mapping = {c.lower(): c for c in df.columns}
    review_col = col_mapping.get("review") or col_mapping.get("review_text") or df.columns[0]
    sentiment_col = col_mapping.get("sentiment") or col_mapping.get("label") or df.columns[1]

    df = df[[review_col, sentiment_col]].dropna()
    df.columns = ["review", "sentiment"]
    df["sentiment"] = df["sentiment"].astype(str).str.lower().str.strip()

    # Map labels to binary: 1 for positive, 0 for negative
    label_map = {"positive": 1, "pos": 1, "1": 1, 1: 1, "negative": 0, "neg": 0, "0": 0, 0: 0}
    df["label"] = df["sentiment"].map(label_map)
    df = df.dropna(subset=["label"])
    df["label"] = df["label"].astype(int)

    logger.info(f"Loaded {len(df)} reviews. Positive: {(df['label'] == 1).sum()}, Negative: {(df['label'] == 0).sum()}")
    return df


def build_pipeline(max_features: int = 5000) -> Pipeline:
    """
    Builds an scikit-learn Pipeline with TF-IDF Vectorizer and Logistic Regression.
    """
    vectorizer = TfidfVectorizer(
        preprocessor=clean_text,
        max_features=max_features,
        ngram_range=(1, 2),
        stop_words="english",
        sublinear_tf=True,
    )
    classifier = LogisticRegression(
        C=1.0,
        max_iter=1000,
        solver="lbfgs",
        random_state=42
    )
    return Pipeline([
        ("tfidf", vectorizer),
        ("clf", classifier)
    ])


def train_and_evaluate(
    df: pd.DataFrame, test_size: float = 0.2, random_state: int = 42
) -> Tuple[Pipeline, Dict[str, Any]]:
    """
    Splits data, trains TF-IDF + Logistic Regression, and computes evaluation metrics.
    """
    X = df["review"].values
    y = df["label"].values

    # Stratified split to ensure class balance across splits
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state, stratify=y
    )

    logger.info(f"Training set: {len(X_train)} samples, Test set: {len(X_test)} samples")

    pipeline = build_pipeline()
    pipeline.fit(X_train, y_train)

    # Evaluate on held-out test set
    y_pred = pipeline.predict(X_test)
    y_prob = pipeline.predict_proba(X_test)[:, 1]

    acc = float(accuracy_score(y_test, y_pred))
    prec = float(precision_score(y_test, y_pred, zero_division=0))
    rec = float(recall_score(y_test, y_pred, zero_division=0))
    f1 = float(f1_score(y_test, y_pred, zero_division=0))
    cm = confusion_matrix(y_test, y_pred).tolist()
    report = classification_report(y_test, y_pred, target_names=["negative", "positive"])

    metrics = {
        "accuracy": acc,
        "precision": prec,
        "recall": rec,
        "f1_score": f1,
        "confusion_matrix": cm,
        "classification_report": report,
        "train_samples": int(len(X_train)),
        "test_samples": int(len(X_test)),
        "trained_at": datetime.utcnow().isoformat() + "Z",
    }

    logger.info(f"Evaluation Results:\nAccuracy: {acc:.4f} | Precision: {prec:.4f} | Recall: {rec:.4f} | F1: {f1:.4f}")
    logger.info(f"Confusion Matrix: {cm}")
    return pipeline, metrics


def save_model(pipeline: Pipeline, metrics: Dict[str, Any], output_path: str) -> None:
    """
    Persists pipeline and associated evaluation metadata to a joblib file.
    """
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    payload = {
        "pipeline": pipeline,
        "metrics": metrics,
        "classes": ["negative", "positive"],
        "threshold": 0.5,
    }
    joblib.dump(payload, output_path)
    logger.info(f"Model successfully saved to: {output_path}")


def train_and_save_model(data_path: str, output_path: str) -> Dict[str, Any]:
    """
    Convenience function to load data, train pipeline, evaluate, and save artifact.
    """
    df = load_dataset(data_path)
    pipeline, metrics = train_and_evaluate(df)
    save_model(pipeline, metrics, output_path)
    return metrics


def main():
    parser = argparse.ArgumentParser(description="Train Movie Sentiment Classifier")
    parser.add_argument(
        "--data",
        type=str,
        default=os.getenv("TRAIN_DATA_PATH", "data/sample_imdb.csv"),
        help="Path to training CSV file (e.g. data/IMDB Dataset.csv or data/sample_imdb.csv)"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=os.getenv("MODEL_PATH", "artifacts/sentiment_model.joblib"),
        help="Path where the model artifact will be saved"
    )
    args = parser.parse_args()

    metrics = train_and_save_model(args.data, args.output)
    print("\n--- Training Completed Successfully ---")
    print(f"Accuracy : {metrics['accuracy']:.4f}")
    print(f"F1 Score : {metrics['f1_score']:.4f}")
    print(f"Artifact : {args.output}")


if __name__ == "__main__":
    main()
