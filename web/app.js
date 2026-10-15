(() => {
  "use strict";

  const state = { metrics: null, hdfs: null, model: null, kafka: null };
  const $ = (id) => document.getElementById(id);
  const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (char) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
  }[char]));
  const number = (value) => Number(value || 0).toLocaleString();
  const percent = (value) => `${Number(value || 0).toFixed(1)}%`;
  const score = (value) => `${Number(value || 0) >= 0 ? "+" : ""}${Number(value || 0).toFixed(2)}`;

  async function getJson(url) {
    const response = await fetch(url, { cache: "no-store" });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return response.json();
  }

  function timeAgo(value) {
    if (!value) return "just now";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return "recently";
    const seconds = Math.max(0, Math.floor((Date.now() - date.getTime()) / 1000));
    if (seconds < 10) return "just now";
    if (seconds < 60) return `${seconds}s ago`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
    return `${Math.floor(seconds / 3600)}h ago`;
  }

  function renderMetrics(data) {
    state.metrics = data || {};
    const total = Number(data.total_processed || 0);
    $("total-processed").textContent = number(total);
    $("positive-pct").textContent = percent(data.global_positive_percentage);
    $("negative-pct").textContent = percent(data.global_negative_percentage);
    $("positive-count").textContent = number(data.total_positive);
    $("negative-count").textContent = number(data.total_negative);
    $("mean-score").textContent = score(data.overall_mean_score);
    $("flow-events-status").textContent = `${number(total)} reviews scored`;
    $("distribution-total").textContent = `${number(total)} events`;
    $("legend-positive").textContent = percent(data.global_positive_percentage);
    $("legend-negative").textContent = percent(data.global_negative_percentage);
    $("legend-total").textContent = number(total);
    $("donut-label").textContent = total ? percent(data.global_positive_percentage) : "—";
    const positiveDegrees = Math.max(0, Math.min(360, Number(data.global_positive_percentage || 0) * 3.6));
    $("donut").style.background = `conic-gradient(var(--green) 0deg ${positiveDegrees}deg, var(--coral) ${positiveDegrees}deg 360deg)`;

    const timeline = Array.isArray(data.timeline) ? data.timeline.slice(-28) : [];
    const bars = $("spark-bars");
    bars.innerHTML = timeline.length ? timeline.map((entry) => {
      const height = Math.max(8, Math.min(100, (Number(entry.sentiment_score || 0) + 1) * 50));
      const color = String(entry.sentiment || "").toLowerCase() === "negative" ? "var(--coral)" : "var(--green)";
      return `<i style="height:${height}%;background:${color}" title="${esc(score(entry.sentiment_score))}"></i>`;
    }).join("") : "<span class='empty-state' style='padding:0;width:100%'>No scored events yet</span>";
    renderTimeline(timeline);
    renderReviews(Array.isArray(data.recent_reviews) ? data.recent_reviews.slice(0, 6) : []);
    const source = data._source || "telemetry";
    $("metrics-badge").textContent = source === "empty fallback" ? "● WAITING" : "● LIVE";
    $("metrics-badge").className = `badge ${source === "empty fallback" ? "badge-amber" : "badge-green"}`;
    $("last-updated").textContent = data.updated_at ? `Updated ${timeAgo(data.updated_at)}` : "Telemetry connected";
  }

  function renderTimeline(entries) {
    const chart = $("timeline-chart");
    if (!entries.length) {
      chart.innerHTML = "<div class='empty-state'>Waiting for scored events…</div>";
      return;
    }
    const width = 680;
    const height = 168;
    const points = entries.map((entry, index) => {
      const x = entries.length === 1 ? width / 2 : (index / (entries.length - 1)) * width;
      const y = height - ((Number(entry.sentiment_score || 0) + 1) / 2) * (height - 16) - 8;
      return `${x.toFixed(1)},${Math.max(8, Math.min(height - 8, y)).toFixed(1)}`;
    });
    const dots = entries.map((entry, index) => {
      const [x, y] = points[index].split(",");
      const fill = String(entry.sentiment || "").toLowerCase() === "negative" ? "var(--coral)" : "var(--green)";
      return `<circle cx="${x}" cy="${y}" r="3" fill="${fill}" stroke="#07111f" stroke-width="2"><title>${esc(score(entry.sentiment_score))} · ${esc(entry.movie_title || "review")}</title></circle>`;
    }).join("");
    chart.innerHTML = `<svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" role="img" aria-label="Sentiment score timeline"><defs><linearGradient id="lineGradient" x1="0" x2="1"><stop stop-color="#54e29b"/><stop offset=".5" stop-color="#55d8ff"/><stop offset="1" stop-color="#a88bff"/></linearGradient></defs><polyline points="${points.join(" ")}" fill="none" stroke="url(#lineGradient)" stroke-width="2.5" stroke-linejoin="round" stroke-linecap="round"/>${dots}</svg>`;
  }

  function renderReviews(reviews) {
    $("review-count").textContent = `${reviews.length} shown`;
    const list = $("review-list");
    if (!reviews.length) {
      list.innerHTML = "<div class='empty-state'>Waiting for reviews from Kafka…</div>";
      return;
    }
    list.innerHTML = reviews.map((review) => {
      const negative = String(review.predicted_sentiment).toLowerCase() === "negative";
      return `<div class="review-item ${negative ? "negative" : ""}"><i></i><div><div class="review-title">${esc(review.movie_title || "Unknown title")}</div><div class="review-text">${esc(review.review_text || "No review text")}</div><div class="review-meta">${esc(review.review_id || "review")} · ${esc(timeAgo(review.created_at))}</div></div><span class="review-score">${esc(score(review.sentiment_score))}</span></div>`;
    }).join("");
  }

  function formatBytes(bytes) {
    let value = Number(bytes || 0);
    for (const unit of ["B", "KB", "MB", "GB"]) {
      if (value < 1024 || unit === "GB") return `${value.toFixed(unit === "B" ? 0 : 1)} ${unit}`;
      value /= 1024;
    }
    return "0 B";
  }

  function renderHdfs(data) {
    state.hdfs = data || {};
    const zones = data.by_zone || {};
    $("raw-count").textContent = number(zones.raw || 0);
    $("processed-count").textContent = number(zones.processed || 0);
    $("metrics-count").textContent = number(zones.metrics || 0);
    $("storage-size").textContent = formatBytes(data.total_bytes);
    $("flow-files-status").textContent = `${number((data.files || []).length)} files visible`;
    $("hdfs-source").textContent = `Source: ${data.source || "fallback"}`;
    $("file-count").textContent = `${number((data.files || []).length)} files`;
    const online = Boolean(data.online);
    $("hdfs-status").textContent = online ? "CONNECTED" : "MIRROR";
    $("hdfs-status").style.color = online ? "var(--green)" : "var(--gold)";
    $("hdfs-node-state").textContent = online ? "WEBHDFS CONNECTED" : "MIRROR FALLBACK";
    const files = Array.isArray(data.files) ? data.files.slice(0, 6) : [];
    $("write-list").innerHTML = files.length ? files.map((file) => `<div class="write-item"><i></i><div><div class="write-path">${esc(file.path || "unknown")}</div><div class="write-time">${esc(timeAgo(file.modified))}</div></div><span class="write-size">${esc(formatBytes(file.bytes))}</span></div>`).join("") : "<div class='empty-state'>No HDFS writes visible yet.</div>";
    $("hdfs-detail-status").textContent = online ? `CONNECTED · ${files.length} files` : "MIRROR FALLBACK";
    $("hdfs-file-list").innerHTML = files.length ? files.map((file, index) => `<button class="file-row" data-file-index="${index}"><span>${esc(file.path)}</span><b>${esc(formatBytes(file.bytes))}</b></button>`).join("") : "<div class='empty-state'>No HDFS files visible.</div>";
    $("hdfs-file-list").querySelectorAll("[data-file-index]").forEach((button) => {
      button.addEventListener("click", () => inspectHdfsFile(files[Number(button.dataset.fileIndex)]?.path));
    });
  }

  async function inspectHdfsFile(path) {
    if (!path) return;
    $("hdfs-file-content").textContent = "Loading file through WebHDFS…";
    try {
      const data = await getJson(`/api/hdfs/file?path=${encodeURIComponent(path)}`);
      $("hdfs-file-content").textContent = data.error || data.content || "File is empty.";
      $("hdfs-record-list").innerHTML = data.records?.length ? `<label>INDIVIDUAL RECORDS (first ${data.records.length})</label>${data.records.map((record) => `<button class="record-row"><span>#${record.index}</span><code>${esc(JSON.stringify(record.value))}</code></button>`).join("")}` : "";
      (data.records || []).forEach((record, index) => {
        $("hdfs-record-list").querySelectorAll(".record-row")[index]?.addEventListener("click", () => {
          $("hdfs-file-content").textContent = JSON.stringify(record.value, null, 2);
        });
      });
    } catch (error) {
      $("hdfs-file-content").textContent = "Unable to read this file right now.";
    }
  }

  function renderKafka(data) {
    state.kafka = data || {};
    const topics = Array.isArray(data.topics) ? data.topics : [];
    const online = Boolean(data.online);
    $("kafka-detail-status").textContent = online ? "BROKER CONNECTED" : "BROKER UNAVAILABLE";
    $("kafka-detail-status").className = `mini-chip ${online ? "green" : "gold"}`;
    $("kafka-bootstrap").textContent = data.bootstrap_servers || "—";
    $("kafka-proof-broker").textContent = data.bootstrap_servers || "—";
    const inputTopic = topics.find((topic) => topic.name === "movie-reviews");
    const outputTopic = topics.find((topic) => topic.name === "movie-sentiment-results");
    $("kafka-message-count").textContent = number(inputTopic?.messages);
    $("output-message-count").textContent = number(outputTopic?.messages);
    $("output-partitions").textContent = number(outputTopic?.partitions);
    $("output-kafka-status").textContent = outputTopic ? "TOPIC CONNECTED" : "TOPIC UNAVAILABLE";
    $("flow-topic-status").textContent = `${number(data.topic_count)} topics live`;
    $("kafka-input-list").innerHTML = renderMessageList(inputTopic, "raw review");
    $("kafka-output-list").innerHTML = renderMessageList(outputTopic, "scored prediction");
    document.querySelectorAll(".topic-message").forEach((button) => {
      button.addEventListener("click", () => inspectKafkaMessage(button.dataset.topic, button.dataset.partition, button.dataset.offset, button.dataset.output === "true"));
    });
  }

  function renderMessageList(topic, label) {
    if (!topic) return `<div class="empty-state">The ${label} topic is unavailable.</div>`;
    const output = label !== "raw review";
    return `<div class="topic-focus"><div class="topic-focus-head"><span class="lane-direction">${output ? "OUT" : "IN"}</span><div><strong>${esc(topic.name)}</strong><small>${topic.partitions} partition${topic.partitions === 1 ? "" : "s"} · ${number(topic.messages)} messages</small></div><span class="stream-live"><i></i> LIVE</span></div><div class="topic-message-stack">${(topic.recent_messages || []).slice(0, 8).map((message) => {
      const value = message.value || {};
      const title = value.movie_title || value.movie_id || "Review event";
      const summary = output
        ? `${String(value.predicted_sentiment || "pending").toUpperCase()} · score ${score(value.sentiment_score)} · ${percent(Number(value.positive_probability || 0) * 100)} positive`
        : (value.review_text || "Raw review event");
      return `<button class="topic-message" data-output="${output}" data-topic="${esc(topic.name)}" data-partition="${message.partition}" data-offset="${message.offset}"><span class="message-index">OFFSET ${message.offset}<small>P${message.partition}</small></span><span class="message-summary"><strong>${esc(title)}</strong><span>${esc(summary)}</span><em>${esc(timeAgo(value.processed_at || value.created_at))}</em></span><span class="message-open">OPEN ↗</span></button>`;
    }).join("") || "<em>No messages sampled</em>"}</div></div>`;
  }

  async function inspectKafkaMessage(topic, partition, offset, output = false) {
    $(output ? "kafka-output-content" : "kafka-message-content").textContent = "Loading exact Kafka record…";
    try {
      const data = await getJson(`/api/kafka/message?topic=${encodeURIComponent(topic)}&partition=${partition}&offset=${offset}`);
      $(output ? "kafka-output-content" : "kafka-message-content").textContent = data.error || JSON.stringify(data, null, 2);
    } catch {
      $(output ? "kafka-output-content" : "kafka-message-content").textContent = "Unable to read this Kafka record.";
    }
  }

  function renderDetailModel(data) {
    const metrics = data.metrics || {};
    $("model-detail-status").textContent = data.available ? "MODEL READY" : "MODEL OFFLINE";
    $("model-threshold").textContent = Number(data.threshold || 0.5).toFixed(2);
    $("model-detail-accuracy").textContent = metrics.accuracy == null ? "—" : percent(metrics.accuracy * 100);
    $("model-metrics-grid").innerHTML = ["precision", "recall", "f1_score"].map((key) => `<div><span>${esc(key.replace("_", " "))}</span><b>${metrics[key] == null ? "—" : percent(metrics[key] * 100)}</b></div>`).join("");
    $("model-console-count").textContent = `${number(state.metrics?.total_processed || 0)} events through inference`;
  }

  function renderExplanation(data) {
    const calculations = Array.isArray(data.calculations) ? data.calculations : [];
    calculations.slice(0, 4).forEach((item, index) => {
      const slot = index + 1;
      $(`explain-feature-${slot}`).textContent = item.feature;
      $(`explain-weight-${slot}`).textContent = `${Number(item.contribution) >= 0 ? "+" : ""}${Number(item.contribution).toFixed(4)}`;
    });
    for (let index = calculations.length; index < 4; index += 1) {
      const slot = index + 1;
      $(`explain-feature-${slot}`).textContent = "—";
      $(`explain-weight-${slot}`).textContent = "—";
    }
    $("explain-bias").textContent = `${Number(data.bias) >= 0 ? "+" : ""}${Number(data.bias).toFixed(4)}`;
    $("explain-sentiment").textContent = String(data.predicted_sentiment || "waiting").toUpperCase();
    $("explain-probability").textContent = `probability ${(Number(data.positive_probability) * 100).toFixed(2)}% · z ${Number(data.decision_value).toFixed(4)}`;
    $("explain-summary").textContent = `${data.feature_count} non-zero features · ${calculations.length} strongest shown · real coefficients`;
  }

  function renderResults(data) {
    const reviews = Array.isArray(data.recent_reviews) ? data.recent_reviews.slice(0, 12) : [];
    $("results-detail-count").textContent = `${number(data.total_processed)} total · ${reviews.length} latest`;
    $("results-table").innerHTML = reviews.length ? `<div class="result-head"><span>Movie</span><span>Prediction</span><span>Probability</span><span>Score</span><span>Processed</span></div>${reviews.map((review) => `<div class="result-row"><span>${esc(review.movie_title)}</span><b class="${review.predicted_sentiment === "negative" ? "negative-text" : "positive-text"}">${esc(String(review.predicted_sentiment || "").toUpperCase())}</b><span>${percent(Number(review.positive_probability || 0) * 100)}</span><span>${esc(score(review.sentiment_score))}</span><span>${esc(timeAgo(review.processed_at || review.created_at))}</span></div>`).join("")}` : "<div class='empty-state'>No processed predictions yet.</div>";
  }

  function renderModel(data) {
    state.model = data || {};
    if ($("predictor-status")) {
      $("predictor-status").textContent = data.available ? "READY" : "OFFLINE";
      $("predictor-status").style.color = data.available ? "var(--green)" : "var(--coral)";
    }
    if ($("model-name")) $("model-name").textContent = data.name || "SentimentPredictor";
    const accuracy = data.metrics && (data.metrics.accuracy ?? data.metrics.test_accuracy);
    if ($("model-accuracy")) $("model-accuracy").textContent = accuracy == null ? "—" : percent(Number(accuracy) * 100);
  }

  function renderHealth(data) {
    const healthy = data && data.status === "ok";
    $("health-score").textContent = healthy ? "OK" : "—";
    $("health-copy").textContent = healthy ? `${number(data.processed)} events observed` : "Telemetry reconnecting";
    $("kafka-status").textContent = healthy ? "READY" : "WAITING";
  }

  async function refresh() {
    const results = await Promise.allSettled([getJson("/api/metrics"), getJson("/api/hdfs"), getJson("/api/model"), getJson("/api/health"), getJson("/api/kafka")]);
    if (results[0].status === "fulfilled") renderMetrics(results[0].value);
    if (results[1].status === "fulfilled") renderHdfs(results[1].value);
    if (results[2].status === "fulfilled") { renderModel(results[2].value); renderDetailModel(results[2].value); }
    if (results[3].status === "fulfilled") renderHealth(results[3].value);
    if (results[4].status === "fulfilled") renderKafka(results[4].value);
    if (results[0].status === "fulfilled") renderResults(results[0].value);
    if (results.every((result) => result.status === "rejected")) $("last-updated").textContent = "Telemetry unavailable — retrying";
  }

  $("refresh-now").addEventListener("click", refresh);
  const flowMap = document.querySelector(".system-flow");
  let lastFlowNode = null;
  document.querySelectorAll("[data-target].intro-action, [data-target].flow-node").forEach((action) => {
    action.addEventListener("click", () => {
      const target = document.getElementById(action.dataset.target);
      if (!target) return;
      if (action.classList.contains("flow-node")) lastFlowNode = action;
      document.getElementById("intro-view").style.display = "none";
      document.querySelectorAll(".detail-view").forEach((view) => {
        view.classList.remove("active");
        view.style.display = "none";
      });
      target.classList.add("active");
      target.style.display = "block";
      target.scrollIntoView({ behavior: "smooth", block: "start" });
    });
  });
  document.querySelectorAll("[data-back]").forEach((button) => {
    button.addEventListener("click", () => {
      document.querySelectorAll(".detail-view").forEach((view) => {
        view.classList.remove("active");
        view.style.display = "none";
      });
      document.getElementById("intro-view").style.display = "block";
      window.scrollTo({ top: 0, behavior: "smooth" });
      requestAnimationFrame(() => {
        if (lastFlowNode) {
          lastFlowNode.focus();
        } else if (flowMap) {
          flowMap.tabIndex = -1;
          flowMap.focus();
        }
      });
    });
  });
  $("review-input").addEventListener("input", (event) => { $("char-count").textContent = event.target.value.length.toLocaleString(); });
  $("predict-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const input = $("review-input");
    const button = event.target.querySelector("button");
    const result = $("predict-result");
    if (!input.value.trim()) { result.innerHTML = "<div class='error-box'>Enter a review to score.</div>"; return; }
    button.disabled = true; button.querySelector("span").textContent = "Scoring…"; result.innerHTML = "";
    try {
      const data = await fetch("/api/predict", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ review_text: input.value }) }).then(async (response) => {
        const payload = await response.json();
        if (!response.ok) throw new Error(payload.error || "Prediction unavailable");
        return payload;
      });
      const negative = data.predicted_sentiment === "negative";
      result.innerHTML = `<div class="result-box ${negative ? "negative" : ""}"><span class="signal ${negative ? "" : "green"}"></span><span class="result-label">${esc(data.predicted_sentiment.toUpperCase())}</span><span class="result-score">score ${esc(score(data.sentiment_score))} · ${esc(percent(data.positive_probability * 100))} positive probability</span></div>`;
    } catch (error) {
      result.innerHTML = `<div class="error-box">${esc(error.message || "Prediction is temporarily unavailable")}</div>`;
    } finally {
      button.disabled = false; button.querySelector("span").textContent = "Run prediction";
    }
  });
  $("explain-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const input = $("explain-input");
    const button = event.target.querySelector("button");
    if (!input.value.trim()) {
      $("explain-summary").textContent = "Enter a review to calculate its real features.";
      return;
    }
    button.disabled = true;
    button.textContent = "Calculating…";
    try {
      const response = await fetch("/api/explain", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ review_text: input.value.trim() })
      });
      const data = await response.json();
      if (!response.ok) throw new Error(data.error || "Explanation unavailable");
      renderExplanation(data);
    } catch (error) {
      $("explain-summary").textContent = error.message || "Explanation unavailable";
    } finally {
      button.disabled = false;
      button.textContent = "Calculate weights →";
    }
  });

  refresh();
  window.setInterval(refresh, 3000);
})();
