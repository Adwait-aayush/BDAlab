# 🚀 Quick Run Guide (For Project Review / Presentation)

This document provides a simple, step-by-step guide to run and demonstrate the **Movie Review Sentiment Classification Pipeline** during your project review or viva.

---

## ⚡ Option 1: Run with Docker (Recommended for Review)

### Step 1: Ensure Docker Desktop is Running
Make sure Docker Desktop is open and running on your system.

### Step 2: Open Terminal & Run One Command
Open your terminal (PowerShell, Command Prompt, or bash) in the project folder and run:

```bash
docker compose up --build
```

> **What happens automatically:**
> 1. Starts **Kafka** (message streaming broker).
> 2. Starts **Hadoop HDFS NameNode & DataNode** (distributed big data storage).
> 3. Trains the **TF-IDF + Logistic Regression** machine learning model.
> 4. Starts the **Consumer** (classifies reviews & saves to HDFS).
> 5. Starts the **Producer** (streams reviews into Kafka).
> 6. Launches the **Streamlit Dashboard**.

---

## 🖥️ What to Open & Demonstrate to Reviewers

### 1. The Interactive Dashboard
Open your web browser and go to:
👉 **[http://localhost:8501](http://localhost:8501)**

**What to show:**
- **Top KPIs**: Total reviews processed, positive %, negative %, and average sentiment score.
- **Per-Movie Breakdown Tab**: Select movies like *Inception*, *The Godfather*, or *Parasite* to view movie-specific statistics.
- **Live Scored Reviews Tab**: Shows reviews as they arrive through Kafka with model predictions and sentiment scores.
- **ML Model Metrics Tab**: Displays model accuracy (~88%), confusion matrix, precision, recall, and F1-score.
- **Live Review Sandbox**: Type your own review (e.g., *"This movie was brilliant and breathtaking!"*) and click **Classify Sentiment** to show live prediction on the spot!

### 2. The Hadoop HDFS Cluster (Big Data Proof)
Open another browser tab:
👉 **[http://localhost:9870](http://localhost:9870)**

**What to show:**
- Click on **Utilities** ➔ **Browse the file system** in the top menu.
- Navigate to:
  - `/movie-sentiment/raw/` ➔ Raw incoming review JSONL files.
  - `/movie-sentiment/processed/` ➔ Scored predictions with probabilities.
  - `/movie-sentiment/metrics/` ➔ Aggregated movie summary statistics (`summary.json`).

---

## 💻 Handy Terminal Commands During the Review

Open a second terminal window to demonstrate live system activity:

### Show Running Containers
```bash
docker compose ps
```

### Show Live Review Scoring in Consumer
```bash
docker compose logs -f consumer
```
*(Shows reviews being scored in real-time as positive/negative).*

### Show Kafka Messages Being Produced
```bash
docker compose logs -f producer
```

### How to Stop the Project After the Review
```bash
docker compose down
```

---

## 🛠️ Option 2: Run Without Docker (Local Fallback)

If Docker is ever unavailable on the evaluation computer, you can run locally:

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Train the model
python model/train.py

# 3. Run smoke test to verify all components
python scripts/smoke_test.py

# 4. Launch the dashboard directly
streamlit run app/dashboard.py
```
Open **[http://localhost:8501](http://localhost:8501)** in your browser.

---

## 🎯 30-Second Project Explanation Pitch for Evaluators
> *"In this project, we built a Big Data streaming and analytics pipeline. Incoming movie reviews are published as JSON messages to **Apache Kafka**. A streaming Python consumer processes each review through an NLP pipeline using **TF-IDF vectorization and Logistic Regression** to compute a sentiment score between -1 and +1. Both raw reviews and scored predictions are persisted in **Hadoop HDFS** via WebHDFS for distributed storage. Finally, movie-level statistics are dynamically aggregated and visualized on a real-time **Streamlit dashboard**."*
