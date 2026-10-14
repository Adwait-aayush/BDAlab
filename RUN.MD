# 🚀 Quick Run Guide (For Project Review / Presentation)

This document provides a simple, foolproof step-by-step guide to run and demonstrate the **Movie Review Sentiment Analytics Pipeline** during your lab review, viva, or project presentation.

> 📘 **For in-depth explanations, architecture diagrams, and viva answers, see [FLOW.MD](FLOW.MD).**

---

## ⚡ Option 1: Run with Docker (Recommended for Review)

### Step 1: Ensure Docker Desktop is Running
Open Docker Desktop and make sure the engine is active (green indicator in the bottom-left corner).

### Step 2: Start All Services
Open your terminal (PowerShell, CMD, or bash) in the project folder and run:

```bash
docker compose up -d
```
*(If running for the first time on a new machine, use `docker compose up --build -d`).*

> **What happens automatically in background:**
> 1. Starts **Apache Kafka 3.7** (KRaft mode message streaming broker).
> 2. Starts **Hadoop HDFS NameNode & DataNode** (distributed cluster storage).
> 3. Executes `init-setup`: creates Kafka topics & trains the **TF-IDF + Logistic Regression** model.
> 4. Starts the **Consumer**: ingests streams, predicts sentiment, and writes to HDFS.
> 5. Starts the **Producer**: continuously streams realistic movie review records.
> 6. Launches the **Sentinel Browser Control Room** (vanilla HTML/CSS/JS served by Python).

---

## 🖥️ What to Open & Demonstrate to Reviewers

> ⚠️ **Important URL Tip for Windows**: Always use `http://127.0.0.1` rather than `localhost` to avoid Windows IPv6 resolution drops (`ERR_EMPTY_RESPONSE`).

### 1. The Interactive Browser Dashboard
Open your web browser and navigate to:
👉 **[http://127.0.0.1:8501](http://127.0.0.1:8501)**

**What to show the evaluator:**
- **Live KPIs**: Total reviews processed, positive %, negative %, and average sentiment score updating live.
- **Sentiment Timeline**: Real-time line chart showing sentiment trends as reviews are ingested.
- **Animated pipeline**: Follow particles from Kafka through the ML predictor to HDFS.
- **HDFS zones and write log**: Inspect raw, processed, and metrics landing zones with WebHDFS/local-mirror status.
- **Custom Predictor**: Type a review on the spot and send it through the production `SentimentPredictor` without writing demo data.

### 2. The Hadoop HDFS Cluster (Big Data Proof)
Open another browser tab:
👉 **[http://127.0.0.1:9870](http://127.0.0.1:9870)**

**What to show the evaluator:**
- **Overview**: Shows cluster health, 1 active DataNode, and configured capacity.
- **Live File Explorer**:
  1. Click on **Utilities** ➔ **Browse the file system** in the top navigation bar.
  2. Navigate into `/movie-sentiment/`:
     - `/movie-sentiment/raw/` ➔ Raw incoming review JSONL files.
     - `/movie-sentiment/processed/` ➔ Scored predictions with sentiment scores.
     - `/movie-sentiment/metrics/` ➔ Aggregated movie summary statistics (`summary.json`).

---

## 💻 Handy Terminal Commands During Evaluation

Keep a terminal open to demonstrate live activity:

### 1. Show All Running Containers
```bash
docker compose ps
```
*(All 6 containers should display as `Up` / `Healthy`).*

### 2. Show Live Review Scoring in Consumer
```bash
docker compose logs --tail 15 -f consumer
```
*(Shows real-time inference: review ID, movie title, sentiment score, and HDFS batch flushes).*

### 3. Show Kafka Messages Being Streamed
```bash
docker compose logs --tail 15 -f producer
```
*(Shows partitions and offsets of reviews being published to Kafka).*

### 4. Stop All Services After Presentation
```bash
docker compose down
```

---

## 🔧 Troubleshooting Tips (Lab Computers)

### 1. "Empty reply from server" or "localhost didn't send any data"
* Cause: Windows resolves `localhost` to IPv6 (`::1`).
* Fix: Always use explicit IPv4: **[http://127.0.0.1:8501](http://127.0.0.1:8501)**.

### 2. `httpReadSeeker ... EOF` error during image pull
* Cause: Docker Desktop containerd snapshotter bug on unstable or IPv6-restricted networks.
* Fix:
  1. In Docker Desktop, go to **Settings** ➔ **Docker Engine**.
  2. Add:
     ```json
     {
       "dns": ["8.8.8.8", "1.1.1.1"]
     }
     ```
  3. Turn off *"Use containerd for storing images"* if enabled.
  4. Pull base images individually:
     ```bash
     docker pull apache/kafka:3.7.0
     docker pull bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
     docker pull bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
     ```

---

## 🎯 30-Second Summary for Evaluators

> *"In this project, we built an end-to-end Big Data streaming and analytics pipeline. Incoming movie reviews are published as JSON events to **Apache Kafka (KRaft mode)**. A streaming consumer processes each review through an NLP pipeline using **TF-IDF vectorization and Logistic Regression** to score sentiment. Both raw reviews and scored predictions are persisted in **Hadoop HDFS** for long-term Data Lake storage. Finally, movie statistics are aggregated dynamically and presented in a lightweight browser control room."*
