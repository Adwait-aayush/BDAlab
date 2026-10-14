# BDAlab: Movie Review Streaming and Sentiment Pipeline

This project is a teaching/demo implementation of a Big Data pipeline.
It takes movie-review events, moves them through Kafka, scores them with a
machine-learning model, stores the data in HDFS, aggregates metrics, and
shows the result in a browser dashboard.

This document is intentionally written for someone who did not create the
project. Read it before changing code or presenting the project.

## 1. The whole project in one picture

```text
sample reviews + generated reviews
                |
                v
        producer/producer.py
                |
                | JSON message
                v
        Kafka topic: movie-reviews
                |
                v
        consumer/consumer.py
          |          |           |
          |          |           +--> Kafka topic: movie-sentiment-results
          |          |
          |          +--> model/predict.py
          |                    |
          |                    +--> positive/negative + probability + score
          |
          +--> consumer/hdfs_writer.py
          |          |
          |          +--> HDFS /movie-sentiment/raw/
          |          +--> HDFS /movie-sentiment/processed/
          |          +--> HDFS /movie-sentiment/metrics/
          |
          +--> app/metrics.py
                     |
                     +--> artifacts/live_metrics.json

browser
  |
  +--> app/web_server.py
          |
          +--> reads live_metrics.json
          +--> reads HDFS metadata through WebHDFS
          +--> calls the same SentimentPredictor for manual tests
          +--> serves web/index.html, web/styles.css, web/app.js
```

The important idea is that the browser is not doing the stream processing.
The consumer does the processing. The browser reads the results and explains
what is happening.

## 2. What is real and what is mocked?

### Real in this project

- A real Kafka broker runs in Docker.
- Real Kafka topics are created and used.
- A real Python producer publishes JSON messages.
- A real Python consumer reads Kafka messages.
- A real scikit-learn model is trained and loaded.
- Real predictions are produced for each review.
- A real Hadoop NameNode and DataNode run in Docker.
- WebHDFS REST calls create directories and write files.
- The metrics file is updated by the running consumer.
- The browser dashboard reads live metrics and HDFS metadata.

### Simulated for the demonstration

- There is no real movie website or mobile application sending reviews.
- `producer/sample_reviews.jsonl` supplies seed events.
- After those records are used, `producer/producer.py` generates reviews
  from positive and negative templates forever.
- The Docker deployment is a small single-node lab cluster, not a
  production-sized distributed cluster.

This is not a deception. The producer is a test replacement for an external
source. It uses the same JSON contract that a real application could use.
In a presentation, say:

> "The infrastructure and processing are real. The external review source is
> simulated so the complete streaming pipeline can run locally and repeatably."

## 3. What role does each layer play?

### Producer: creates events

File: `producer/producer.py`

The producer represents the upstream application that receives reviews.
It:

1. Reads seed records from `producer/sample_reviews.jsonl`.
2. Generates more records when the seed file is exhausted.
3. Adds an ID, movie information, text, source, and timestamp.
4. Serializes each dictionary to JSON.
5. Publishes it to Kafka topic `movie-reviews`.

The producer does not classify sentiment and does not write HDFS files.
Its only job is to create and publish input events.

Important configuration:

```text
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_INPUT_TOPIC=movie-reviews
PRODUCER_MODE=stream
PRODUCER_STREAM_DELAY_SECONDS=2.0
```

`stream` mode runs continuously. `batch` mode sends a finite number of
messages and is useful for tests.

### Kafka: transports and buffers events

Kafka is the middle layer between the producer and consumer.

In this project:

```text
movie-reviews             input topic
movie-sentiment-results   scored output topic
```

Kafka is useful because the producer and consumer do not have to run at the
same speed. The producer writes events to the topic. The consumer reads them
when it is ready. Kafka keeps the ordered log of messages for the consumer
group.

Kafka is not the ML model and it is not HDFS. It is the event transport and
buffer.

The project uses Kafka 3.7 in KRaft mode. KRaft means Kafka manages its own
metadata and does not need a separate ZooKeeper container.

The topic creation code is:

`scripts/create_topics.py`

The local demo intentionally uses one broker, one partition, and replication
factor one. That is enough to demonstrate the architecture, but it is not
fault tolerant production configuration.

### Consumer: coordinates the processing pipeline

File: `consumer/consumer.py`

The consumer is the main application service. For every Kafka message it:

1. Validates that the message is a dictionary with review text.
2. Calls `SentimentPredictor.predict_one`.
3. Adds prediction fields to the record.
4. Publishes the scored record to `movie-sentiment-results`.
5. Buffers the original record for raw HDFS storage.
6. Buffers the scored record for processed HDFS storage.
7. Updates aggregate metrics.
8. Periodically flushes the buffers to HDFS.

The consumer uses a Kafka consumer group named
`movie-sentiment-group`. Its offset is the position in the Kafka topic that
the consumer has read.

The HDFS write buffer is flushed when it reaches five processed records or
when the flush timer reaches its interval. Batching avoids making one HDFS
network request for every individual review.

### Machine learning: turns text into a decision

Training file:

`model/train.py`

Prediction wrapper:

`model/predict.py`

Text cleanup:

`model/preprocessing.py`

Training data:

`data/sample_imdb.csv`

The model is a scikit-learn pipeline:

```text
review text
    |
    v
clean_text()
    |
    v
TF-IDF vectorizer
    |
    v
Logistic Regression
    |
    v
positive probability
```

TF-IDF converts words and two-word phrases into numeric features. Logistic
Regression uses those features to estimate the probability of a positive
review.

The model is trained once by `init-setup` and saved as:

`artifacts/sentiment_model.joblib`

For a review, the predictor returns:

```json
{
  "predicted_sentiment": "positive",
  "positive_probability": 0.73,
  "sentiment_score": 0.46
}
```

The score is calculated as:

```text
sentiment_score = 2 * positive_probability - 1
```

Therefore:

```text
-1.0 = strongly negative
 0.0 = uncertain/middle
+1.0 = strongly positive
```

The dashboard's manual predictor calls this same `SentimentPredictor`; it
does not use a second fake model.

### HDFS: durable data lake storage

HDFS has two important processes:

- NameNode: manages directories, file metadata, and block locations.
- DataNode: stores the actual file blocks.

The Python code talks to HDFS using WebHDFS, which is Hadoop's HTTP REST
interface.

The client implementation is:

`consumer/hdfs_writer.py`

The writer creates this logical data-lake layout:

```text
/movie-sentiment/
    raw/
    processed/
    metrics/
```

The zones have different meanings:

```text
raw/
    Original input reviews. Keep this so data can be reprocessed later.

processed/
    Reviews enriched with prediction, probability, score, and timestamps.

metrics/
    Current aggregate summary for the dashboard and reports.
```

The pipeline deliberately keeps one canonical file per zone:

```text
/movie-sentiment/raw/reviews.jsonl
/movie-sentiment/processed/scored.jsonl
/movie-sentiment/metrics/summary.json
```

Raw and processed records are appended to these files in batches. On consumer
startup, old date-suffixed files in these three zones are removed so the
dashboard and the real NameNode show one unambiguous source of truth.

When WebHDFS writes a file, the client normally:

1. Sends a CREATE request to the NameNode.
2. Receives an HTTP redirect to the DataNode.
3. Sends the file bytes to the DataNode.

The local mirror at `data/hdfs_mirror/` is only a fallback for development
and demo continuity. The primary target in Docker is real HDFS.

### Metrics: calculates dashboard state

File:

`app/metrics.py`

The metrics manager consumes scored records and maintains:

- Total processed reviews
- Positive and negative counts
- Percentages
- Mean score
- Per-movie statistics
- Recent scored reviews
- A capped timeline of the latest 500 events

It saves the current state to:

`artifacts/live_metrics.json`

The Docker volume `app-artifacts` is shared by `init-setup`, `consumer`, and
`dashboard`, so they can access the model and metrics artifacts.

### Browser dashboard: presents the results

Server:

`app/web_server.py`

Frontend:

```text
web/index.html
web/styles.css
web/app.js
```

The server uses Python's standard-library HTTP server. It exposes:

```text
GET  /api/health
GET  /api/metrics
GET  /api/hdfs
GET  /api/model
POST /api/predict
```

The browser polls the read-only endpoints every three seconds. It displays:

- Pipeline health
- Review counts and sentiment percentages
- Animated Kafka -> ML -> HDFS flow
- A plain-English review journey
- HDFS raw, processed, and metrics zones
- Recent scored reviews
- Recent HDFS writes
- A manual review predictor

The dashboard is organized into presentation tabs:

- **Introduction**: the live system story and proof counters.
- **Kafka**: broker connection, topics, partitions, offsets, message counts,
  and sampled recent messages from each topic.
- **Model**: loaded artifact status and evaluation metrics.
- **HDFS**: visible files plus an in-dashboard file reader for raw reviews,
  scored predictions, and the metrics summary.
- **Live results**: the latest predictions produced by the consumer.

This means the dashboard is the presentation source of truth. The Hadoop UI
and Kafka logs remain useful low-level troubleshooting tools, but a reviewer
does not need to leave the dashboard to see the evidence.

The animation is a presentation aid. It represents the real architecture,
but it is not a packet-level trace of one exact review. The actual evidence
comes from the live metrics, consumer logs, and HDFS files.

## 4. Docker startup order

The services are defined in:

`docker-compose.yml`

Startup sequence:

```text
Kafka and HDFS start
        |
        v
init-setup waits for them
        |
        +--> creates Kafka topics
        +--> trains the model
        |
        v
consumer starts and prepares HDFS directories
        |
        v
producer starts publishing reviews
        |
        v
dashboard serves the browser UI
```

The readiness helper is:

`scripts/wait_for_services.py`

The initialization service exits successfully after setup. It is expected
to show `Exited (0)` in `docker compose ps`; it is not a continuously
running application.

## 5. Run and inspect the project

Start everything:

```powershell
docker compose up --build -d
```

Open:

```text
Dashboard: http://127.0.0.1:8501
HDFS UI:   http://127.0.0.1:9870
```

Check services:

```powershell
docker compose ps
```

Watch the actual producer and consumer:

```powershell
docker compose logs --tail 20 -f producer
docker compose logs --tail 20 -f consumer
```

Inspect the WebHDFS directory:

```powershell
Invoke-WebRequest -UseBasicParsing `
  "http://127.0.0.1:9870/webhdfs/v1/movie-sentiment?op=LISTSTATUS&user.name=root"
```

Stop the demo:

```powershell
docker compose down
```

Reset Kafka and HDFS data as well:

```powershell
docker compose down -v
```

Use `down -v` only when you intentionally want to erase the Docker volumes.

## 6. What to demonstrate in a review

Use this order:

1. Show the browser dashboard and explain the four-step journey.
2. Show the producer log publishing review IDs and Kafka offsets.
3. Show the consumer log proving prediction and HDFS flushes.
4. Open the HDFS NameNode UI and browse `/movie-sentiment/`.
5. Explain the difference between `raw`, `processed`, and `metrics`.
6. Enter a custom review in the dashboard and explain that it calls the same
   production predictor.
7. Explain that the dashboard animation is illustrative, while the counters,
   logs, model output, and HDFS files are live evidence.

Short presentation script:

> "The producer represents a movie-review application and publishes JSON
> events to Kafka. Kafka decouples ingestion from processing and preserves
> the event stream. The consumer reads each event, applies the trained
> TF-IDF and Logistic Regression model, publishes the scored result, and
> stores both the original and enriched records in HDFS. The metrics manager
> creates the aggregate view, and the browser dashboard reads that state and
> HDFS metadata to explain the live pipeline."

## 7. Project map

```text
BDAlab/
├── app/
│   ├── metrics.py       running sentiment aggregation
│   ├── web_server.py    browser dashboard API and static-file server
│   └── dashboard.py     old Streamlit compatibility module; not used by Docker
├── consumer/
│   ├── consumer.py      Kafka consumer, ML processing, batching, orchestration
│   └── hdfs_writer.py   WebHDFS client and local mirror fallback
├── producer/
│   ├── producer.py      seed and synthetic event producer
│   └── sample_reviews.jsonl
├── model/
│   ├── train.py         model training and evaluation
│   ├── predict.py       model loading and inference
│   └── preprocessing.py text cleanup
├── scripts/
│   ├── create_topics.py Kafka topic setup
│   ├── wait_for_services.py readiness checks
│   └── smoke_test.py    local model/metrics/HDFS writer checks
├── web/
│   ├── index.html       dashboard structure
│   ├── styles.css       dashboard appearance and animation
│   └── app.js           polling, rendering, and predictor interaction
├── data/
│   └── sample_imdb.csv  small local training fixture
├── artifacts/
│   └── sentiment_model.joblib saved model artifact
├── docker-compose.yml   complete local infrastructure
└── Dockerfile           shared Python image for application services
```

## 8. Honest limitations

This is a local educational prototype:

- One Kafka broker and one partition are configured.
- HDFS replication is one.
- The incoming review source is synthetic/sample data.
- The training fixture is small compared with a production IMDb dataset.
- Kafka and HDFS security are disabled for local development.
- The dashboard's animated flow is conceptual, not distributed tracing.

The architecture can be extended to multiple brokers, multiple DataNodes,
replication, real review ingestion, authentication, monitoring, and a larger
training dataset.
