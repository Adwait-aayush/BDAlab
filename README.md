# 🎬 Movie Review Sentiment Classification Using Big Data Technologies

An end-to-end containerized Big Data pipeline that streams movie reviews through **Apache Kafka**, classifies sentiment using an NLP pipeline (**TF-IDF + Logistic Regression**), stores raw and processed records in distributed storage (**Hadoop HDFS** via WebHDFS), and visualizes live sentiment statistics on an interactive **Streamlit** dashboard.

---

## 📌 Architecture Overview

```
                      +-----------------------------+
                      |   IMDb Reviews / Synthetic  |
                      +--------------+--------------+
                                     |
                                     v
                      +-----------------------------+
                      |     Kafka Review Producer   |
                      +--------------+--------------+
                                     |
                                     v
                      +-----------------------------+
                      | Topic: 'movie-reviews'      |
                      +--------------+--------------+
                                     |
                                     v
                      +-----------------------------+
                      |  Sentiment Consumer Engine  |
                      |  (TF-IDF + Logistic Reg.)   |
                      +--------------+--------------+
                                     |
             +-----------------------+-----------------------+
             |                                               |
             v                                               v
+--------------------------+                   +--------------------------+
|  Kafka Output Topic:     |                   |  Hadoop HDFS Storage     |
|  'movie-sentiment-results|                   |  /movie-sentiment/raw/   |
+--------------------------+                   |  /movie-sentiment/proc/  |
                                               |  /movie-sentiment/metr/  |
                                               +-------------+------------+
                                                             |
                                                             v
                                               +--------------------------+
                                               |   Streamlit Dashboard    |
                                               |  (Real-Time Analytics)   |
                                               +--------------------------+
```

### How the Technologies Work Together:
1. **Apache Kafka (KRaft Mode)**: Acts as the decoupled, fault-tolerant message broker. Ingests raw reviews on `movie-reviews` and streams scored predictions to `movie-sentiment-results`.
2. **TF-IDF + Logistic Regression**: Preprocesses text (HTML stripping, normalization), extracts n-gram features with inverse document frequency weights, and computes the probability $P(\text{pos})$.
3. **Sentiment Score Formula**:
   $$\text{sentiment\_score} = 2 \times P(\text{positive}) - 1 \quad \in [-1.0, +1.0]$$
4. **Hadoop HDFS (WebHDFS)**: Distributed storage layer persisting raw reviews, processed predictions, and aggregated summaries into distinct partitioned directories.
5. **Streamlit Dashboard**: Polls the queryable metrics layer and visualizes live per-movie trends, timelines, KPI cards, and an interactive review tester.

---

## 🚀 Quick Start with Docker

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (running on Windows, macOS, or Linux)
- Git

### 1. Clone & Enter Project Directory
```bash
git clone <repo-url>
cd BDAlab
```

### 2. Launch the Entire Pipeline
Run the following single command:
```bash
docker compose up --build
```

Docker Compose will automatically:
- Start the **Kafka broker** (KRaft mode).
- Start **Hadoop HDFS NameNode & DataNode** with WebHDFS enabled.
- Run `init-setup` to create Kafka topics and train the TF-IDF + Logistic Regression model.
- Start the **Sentiment Consumer** to process reviews in real time and write to HDFS.
- Start the **Review Producer** streaming reviews with a configurable delay.
- Launch the **Streamlit Dashboard** on port 8501.

---

## 🌐 Endpoints & Web UIs

| Service | URL | Description |
| :--- | :--- | :--- |
| **Streamlit Dashboard** | [http://localhost:8501](http://localhost:8501) | Live sentiment analytics & interactive review classifier |
| **Hadoop NameNode UI** | [http://localhost:9870](http://localhost:9870) | HDFS cluster overview, datanodes, and file browser |
| **WebHDFS REST API** | [http://localhost:9870/webhdfs/v1/](http://localhost:9870/webhdfs/v1/) | HDFS RESTful file access |
| **Kafka Broker** | `localhost:29092` (Host) / `kafka:9092` (Docker) | Kafka bootstrap server |

---

## 📂 HDFS Storage Structure

Data is persisted in the following HDFS directories:
- `/movie-sentiment/raw/`: Original JSON review records partitioned by date (`reviews_YYYYMMDD.jsonl`).
- `/movie-sentiment/processed/`: Scored review records with sentiment label, probability, and score (`scored_YYYYMMDD.jsonl`).
- `/movie-sentiment/metrics/`: Grouped per-movie statistics and summary metrics (`summary.json`).

### Inspecting HDFS Files
You can view the directory contents in the NameNode web UI or query WebHDFS directly:
```bash
# List files in the processed folder
curl "http://localhost:9870/webhdfs/v1/movie-sentiment/processed/?op=LISTSTATUS&user.name=root"

# Read the latest aggregated metrics JSON
curl -L "http://localhost:9870/webhdfs/v1/movie-sentiment/metrics/summary.json?op=OPEN&user.name=root"
```

---

## 💻 Service Commands & Operations

### View Live Logs
```bash
# View consumer logs
docker compose logs -f consumer

# View producer logs
docker compose logs -f producer

# View dashboard logs
docker compose logs -f dashboard
```

### Run Finite Batch Mode
To run a finite batch of reviews (e.g. 30 reviews) and cleanly stop the producer:
```bash
docker compose run --rm producer python producer/producer.py --mode batch --batch-size 30
```

### Retrain Model on Full IMDb 50K Dataset
1. Download `IMDB Dataset.csv` from [Kaggle](https://www.kaggle.com/datasets/lakshmi25npathi/imdb-dataset-of-50k-movie-reviews).
2. Place the file inside `data/IMDB Dataset.csv`.
3. Train the model:
   ```bash
   python model/train.py --data "data/IMDB Dataset.csv" --output artifacts/sentiment_model.joblib
   ```

### Run Smoke Test Locally
```bash
python scripts/smoke_test.py
```

### Stopping & Resetting
```bash
# Stop all containers
docker compose down

# Stop containers and DELETE all persistent volumes (resets Kafka & HDFS)
docker compose down -v
```
> [!WARNING]
> Running `docker compose down -v` permanently removes the Kafka and HDFS persistent storage volumes!

---

## 📋 Schema Definitions

### Incoming Review Message (`movie-reviews`)
```json
{
  "review_id": "rev-1001",
  "movie_id": "MOV-001",
  "movie_title": "Inception",
  "review_text": "A breathtaking masterpiece with mind-bending visuals and an incredible score.",
  "source": "sample_stream",
  "created_at": "2026-10-12T20:00:00Z"
}
```

### Processed Scored Message (`movie-sentiment-results`)
```json
{
  "review_id": "rev-1001",
  "movie_id": "MOV-001",
  "movie_title": "Inception",
  "review_text": "A breathtaking masterpiece with mind-bending visuals and an incredible score.",
  "predicted_sentiment": "positive",
  "positive_probability": 0.9421,
  "sentiment_score": 0.8842,
  "created_at": "2026-10-12T20:00:00Z",
  "processed_at": "2026-10-12T20:00:02Z"
}
```

---

## 📁 Repository Structure

```
BDAlab/
├── app/
│   ├── dashboard.py         # Interactive Streamlit analytics dashboard
│   └── metrics.py           # Per-movie sentiment aggregation engine
├── producer/
│   ├── producer.py          # Kafka streaming & batch review producer
│   └── sample_reviews.jsonl # Seed dataset with diverse movie reviews
├── consumer/
│   ├── consumer.py          # Real-time sentiment consumer & ML pipeline
│   └── hdfs_writer.py       # WebHDFS client with resilient fallback
├── model/
│   ├── train.py             # TF-IDF + Logistic Regression trainer
│   ├── predict.py           # Inference wrapper and probability scorer
│   └── preprocessing.py     # Text cleaning & HTML normalization
├── scripts/
│   ├── create_topics.py     # Automated Kafka topic provisioning
│   ├── wait_for_services.py # Service readiness health check
│   └── smoke_test.py        # End-to-end pipeline validation test
├── data/
│   ├── sample_imdb.csv      # Offline balanced training dataset
│   └── README.md            # Dataset documentation and download guide
├── artifacts/
│   └── sentiment_model.joblib # Persisted trained model artifact
├── docker/
│   └── hadoop/
│       ├── core-site.xml    # Hadoop core cluster configuration
│       └── hdfs-site.xml    # HDFS WebHDFS configuration
├── Dockerfile               # Multi-service Python application container
├── docker-compose.yml       # Orchestration for Kafka, HDFS, & App services
├── requirements.txt         # Pinned Python package dependencies
├── .env.example             # Configuration template
├── .gitignore               # Git ignore rules
└── README.md                # Project documentation
```
