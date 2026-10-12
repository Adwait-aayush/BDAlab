# Datasets

This directory holds the training and streaming datasets for the Big Data Movie Review Sentiment Analysis project.

## 1. IMDb 50K Dataset (Full Dataset)
- **Source**: [Kaggle IMDb Dataset of 50K Movie Reviews](https://www.kaggle.com/datasets/lakshmi25npathi/imdb-dataset-of-50k-movie-reviews)
- **File**: `IMDB Dataset.csv` (contains 50,000 reviews with columns `review`, `sentiment`).
- **How to use**: Download the CSV from Kaggle and place it in this directory as `data/IMDB Dataset.csv`. Then run:
  ```bash
  python model/train.py --data data/IMDB\ Dataset.csv
  ```

## 2. Bundled Sample Dataset (`sample_imdb.csv`)
For instant offline smoke tests, continuous integration, and Docker builds without downloading gigabytes of external data, this repository includes `data/sample_imdb.csv`.
- Contains balanced positive and negative movie reviews.
- Allows automatic training and pipeline verification out-of-the-box.

## 3. Streaming Review Dataset
The streaming pipeline uses `producer/sample_reviews.jsonl` or synthetic generators with movie metadata (`review_id`, `movie_id`, `movie_title`, `review_text`, `source`, `created_at`).
