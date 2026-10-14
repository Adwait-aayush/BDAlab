"""
Inference module for Movie Review Sentiment Classification.
Loads the persisted TF-IDF + Logistic Regression pipeline and scores reviews.
Calculates positive_probability and maps sentiment_score to [-1.0, 1.0].
"""

import os
import sys
import logging
from typing import Dict, Any, List

import joblib

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from model.train import train_and_save_model

logger = logging.getLogger("sentiment_predictor")


class SentimentPredictor:
    """
    Production-ready wrapper for loading and running predictions with
    the persisted TF-IDF + Logistic Regression model.
    """

    def __init__(self, model_path: str = "artifacts/sentiment_model.joblib", auto_train: bool = True):
        self.model_path = model_path
        self.pipeline = None
        self.metrics = {}
        self.threshold = 0.5
        self._load_or_train(auto_train)

    def _load_or_train(self, auto_train: bool) -> None:
        """
        Loads the model artifact. If missing and auto_train is True,
        initializes training automatically using the default fixture.
        """
        if not os.path.exists(self.model_path):
            if auto_train:
                logger.warning(f"Model artifact not found at '{self.model_path}'. Triggering automated initialization...")
                train_data_path = os.getenv("TRAIN_DATA_PATH", "data/sample_imdb.csv")
                self.metrics = train_and_save_model(train_data_path, self.model_path)
            else:
                raise FileNotFoundError(f"Model artifact not found at '{self.model_path}'.")

        artifact = joblib.load(self.model_path)
        if isinstance(artifact, dict) and "pipeline" in artifact:
            self.pipeline = artifact["pipeline"]
            self.metrics = artifact.get("metrics", {})
            self.threshold = artifact.get("threshold", 0.5)
        else:
            # Direct pipeline object fallback
            self.pipeline = artifact
            self.threshold = 0.5

        logger.info(f"Loaded sentiment model from '{self.model_path}' successfully.")

    def predict_one(self, review_text: str) -> Dict[str, Any]:
        """
        Predicts sentiment for a single review.
        
        Formula:
            sentiment_score = 2 * positive_probability - 1  (in range [-1, 1])
            predicted_sentiment = 'positive' if positive_probability >= threshold else 'negative'
        """
        if not self.pipeline:
            raise RuntimeError("Pipeline is not loaded.")

        text = review_text if review_text else ""
        probabilities = self.pipeline.predict_proba([text])[0]
        # Class 1 is positive
        positive_prob = float(probabilities[1])
        sentiment_score = float(2.0 * positive_prob - 1.0)
        predicted_sentiment = "positive" if positive_prob >= self.threshold else "negative"

        return {
            "predicted_sentiment": predicted_sentiment,
            "positive_probability": round(positive_prob, 4),
            "sentiment_score": round(sentiment_score, 4),
        }

    def explain_one(self, review_text: str, limit: int = 8) -> Dict[str, Any]:
        """Return the real sparse TF-IDF calculation used for one prediction."""
        if not self.pipeline:
            raise RuntimeError("Pipeline is not loaded.")
        if limit < 1 or limit > 30:
            raise ValueError("Explanation limit must be between 1 and 30.")

        text = review_text if review_text else ""
        vectorizer = self.pipeline.named_steps.get("tfidf")
        classifier = self.pipeline.named_steps.get("clf")
        if vectorizer is None or classifier is None:
            raise RuntimeError("Loaded pipeline does not contain tfidf and clf steps.")

        features = vectorizer.transform([text])
        names = vectorizer.get_feature_names_out()
        coefficients = classifier.coef_[0]
        nonzero_indices = features.indices
        calculations = []
        for index in nonzero_indices:
            tfidf_value = float(features[0, index])
            coefficient = float(coefficients[index])
            calculations.append({
                "feature": str(names[index]),
                "tfidf": round(tfidf_value, 6),
                "weight": round(coefficient, 6),
                "contribution": round(tfidf_value * coefficient, 6),
            })
        calculations.sort(key=lambda item: abs(item["contribution"]), reverse=True)

        decision = float(classifier.decision_function(features)[0])
        positive_probability = float(self.pipeline.predict_proba([text])[0][1])
        return {
            **self.predict_one(text),
            "bias": round(float(classifier.intercept_[0]), 6),
            "decision_value": round(decision, 6),
            "sigmoid_probability": round(positive_probability, 6),
            "feature_count": int(len(nonzero_indices)),
            "calculations": calculations[:limit],
        }

    def predict_batch(self, reviews: List[str]) -> List[Dict[str, Any]]:
        """
        Predicts sentiment for an array of review texts.
        """
        if not self.pipeline:
            raise RuntimeError("Pipeline is not loaded.")

        if not reviews:
            return []

        cleaned_inputs = [r if r else "" for r in reviews]
        probabilities = self.pipeline.predict_proba(cleaned_inputs)

        results = []
        for prob in probabilities:
            pos_prob = float(prob[1])
            score = float(2.0 * pos_prob - 1.0)
            sentiment = "positive" if pos_prob >= self.threshold else "negative"
            results.append({
                "predicted_sentiment": sentiment,
                "positive_probability": round(pos_prob, 4),
                "sentiment_score": round(score, 4),
            })
        return results

    def get_metrics(self) -> Dict[str, Any]:
        """Returns the training evaluation metrics stored with the artifact."""
        return self.metrics
