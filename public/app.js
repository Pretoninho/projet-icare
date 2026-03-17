const appState = {
  analytics: null,
  backtest: null,
  candles: [],
  events: [],
  health: null,
  signal: null,
  snapshot: null,
  wsConnected: false
};

const refs = {
  analyticsList: document.getElementById("analytics-list"),
  analyticsRange: document.getElementById("analytics-range"),
  candlesState: document.getElementById("candles-state"),
  candlesTableBody: document.getElementById("candles-table-body"),
  candlesTableWrap: document.getElementById("candles-table-wrap"),
  channelSelect: document.getElementById("channel-select"),
  chartCanvas: document.getElementById("price-chart"),
  chartTitle: document.getElementById("chart-title"),
  connectionPill: document.getElementById("connection-pill"),
  eventStream: document.getElementById("event-stream"),
  lastSync: document.getElementById("last-sync"),
  metricConnectedAt: document.getElementById("metric-connected-at"),
  metricChangePct: document.getElementById("metric-change-pct"),
  metricEventsCount: document.getElementById("metric-events-count"),
  metricLastMessage: document.getElementById("metric-last-message"),
  metricLastPrice: document.getElementById("metric-last-price"),
  metricPriceChange: document.getElementById("metric-price-change"),
  metricStatus: document.getElementById("metric-status"),
  metricVolatility: document.getElementById("metric-volatility"),
  refreshButton: document.getElementById("refresh-button"),
  signalDirection: document.getElementById("signal-direction"),
  signalProbLong: document.getElementById("signal-prob-long"),
  signalProbShort: document.getElementById("signal-prob-short"),
  signalConfidence: document.getElementById("signal-confidence"),
  signalReasons: document.getElementById("signal-reasons"),
  signalReliability: document.getElementById("signal-reliability"),
  signalReliabilitySparkline: document.getElementById("signal-reliability-sparkline"),
  signalSample: document.getElementById("signal-sample"),
  signalSl: document.getElementById("signal-sl"),
  signalTp: document.getElementById("signal-tp"),
  signalUpdatedAt: document.getElementById("signal-updated-at"),
  snapshotGeneratedAt: document.getElementById("snapshot-generated-at"),
  snapshotTableBody: document.getElementById("snapshot-table-body"),
  topChannelList: document.getElementById("top-channel-list"),
  windowSelect: document.getElementById("window-select"),
  wsIndicator: document.getElementById("ws-indicator")
};

function getSelectedChannel() {
  return refs.channelSelect.value || "";
}

function getEventPrice(event) {
  if (!event || !event.data) {
    return null;
  }

  const raw = event.data.last_price ?? event.data.price ?? event.data.mark_price ?? null;
  const price = Number(raw);
  return Number.isFinite(price) ? price : null;
}

function formatDate(value) {
  if (!value) {
    return "--";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "--";
  }

  return new Intl.DateTimeFormat("fr-FR", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    day: "2-digit",
    month: "2-digit"
  }).format(date);
}

function formatNumber(value, digits = 2) {
  if (value === null || typeof value === "undefined" || Number.isNaN(Number(value))) {
    return "--";
  }

  return new Intl.NumberFormat("fr-FR", {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits
  }).format(Number(value));
}

function formatPercent(value) {
  if (value === null || typeof value === "undefined" || Number.isNaN(Number(value))) {
    return "--";
  }

  const sign = Number(value) > 0 ? "+" : "";
  return `${sign}${formatNumber(value, 3)} %`;
}

function formatProbability(value) {
  if (value === null || typeof value === "undefined" || Number.isNaN(Number(value))) {
    return "--";
  }

  return `${formatNumber(Number(value) * 100, 2)} %`;
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function drawReliabilitySparkline(history) {
  const canvas = refs.signalReliabilitySparkline;
  if (!canvas) {
    return;
  }

  const context = canvas.getContext("2d");
  const width = canvas.width;
  const height = canvas.height;
  context.clearRect(0, 0, width, height);

  const points = Array.isArray(history)
    ? history
      .map((item) => item && item.metrics ? item.metrics.reliabilityScore : null)
      .filter((value) => Number.isFinite(value))
    : [];

  context.strokeStyle = "rgba(139, 154, 179, 0.35)";
  context.lineWidth = 1;
  context.beginPath();
  context.moveTo(0, height - 1);
  context.lineTo(width, height - 1);
  context.stroke();

  if (points.length < 2) {
    context.fillStyle = "rgba(139, 154, 179, 0.9)";
    context.font = "11px IBM Plex Mono";
    context.fillText("rolling: n/a", 8, 20);
    return;
  }

  const min = Math.min(...points);
  const max = Math.max(...points);
  const paddingX = 6;
  const paddingY = 7;
  const usableWidth = width - (paddingX * 2);
  const usableHeight = height - (paddingY * 2);

  const gradient = context.createLinearGradient(0, 0, width, 0);
  gradient.addColorStop(0, "#ff6b6b");
  gradient.addColorStop(0.5, "#f5b84d");
  gradient.addColorStop(1, "#2ecc8f");
  context.strokeStyle = gradient;
  context.lineWidth = 2;
  context.beginPath();

  points.forEach((value, index) => {
    const x = paddingX + (usableWidth * index) / Math.max(points.length - 1, 1);
    const ratio = max === min ? 0.5 : (value - min) / (max - min);
    const y = height - paddingY - (ratio * usableHeight);
    if (index === 0) {
      context.moveTo(x, y);
    } else {
      context.lineTo(x, y);
    }
  });
  context.stroke();
}

async function fetchJson(url) {
  const response = await fetch(url);
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.message || `HTTP ${response.status}`);
  }

  return payload;
}

function syncChannelOptions() {
  const channels = appState.health && Array.isArray(appState.health.channels) ? appState.health.channels : [];
  const previous = getSelectedChannel();

  refs.channelSelect.innerHTML = channels
    .map((channel) => `<option value="${escapeHtml(channel)}">${escapeHtml(channel)}</option>`)
    .join("");

  if (channels.length === 0) {
    refs.channelSelect.innerHTML = '<option value="">Aucun canal</option>';
    return;
  }

  refs.channelSelect.value = channels.includes(previous) ? previous : channels[0];
}

function renderHealth() {
  const health = appState.health;
  if (!health) {
    return;
  }

  refs.connectionPill.textContent = health.status === "connected" ? "Connecté à Deribit" : "Connexion interrompue";
  refs.connectionPill.className = `status-pill ${health.status}`;
  refs.metricStatus.textContent = health.status;
  refs.metricConnectedAt.textContent = `Ouverte: ${formatDate(health.connectedAt)}`;
  refs.metricEventsCount.textContent = String(appState.snapshot && appState.snapshot.meta ? appState.snapshot.meta.eventsInMemory : 0);
  refs.metricLastMessage.textContent = `Dernier message: ${formatDate(health.lastMessageAt)}`;
  refs.wsIndicator.textContent = `WS local: ${appState.wsConnected ? "connecté" : "hors ligne"}`;
}

function renderSnapshot() {
  const snapshot = appState.snapshot;
  if (!snapshot || !snapshot.latestByChannel) {
    refs.snapshotTableBody.innerHTML = '<tr><td colspan="3" class="muted">Aucune donnée disponible</td></tr>';
    return;
  }

  refs.snapshotGeneratedAt.textContent = `Généré: ${formatDate(snapshot.meta && snapshot.meta.generatedAt)}`;
  const rows = Object.entries(snapshot.latestByChannel)
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([channel, event]) => {
      const price = getEventPrice(event);
      return `
        <tr>
          <td>${escapeHtml(channel)}</td>
          <td>${formatNumber(price, 2)}</td>
          <td>${formatDate(event.receivedAt)}</td>
        </tr>
      `;
    });

  refs.snapshotTableBody.innerHTML = rows.join("") || '<tr><td colspan="3" class="muted">Aucune donnée disponible</td></tr>';
}

function getChannelEvents() {
  const channel = getSelectedChannel();
  return appState.events.filter((event) => !channel || event.channel === channel);
}

function renderEvents() {
  const events = getChannelEvents().slice(-12).reverse();
  if (events.length === 0) {
    refs.eventStream.innerHTML = '<div class="empty-state">Aucun flux pour cet instrument.</div>';
    return;
  }

  refs.eventStream.innerHTML = events
    .map((event) => {
      const price = getEventPrice(event);
      const markPrice = event && event.data ? (event.data.mark_price ?? event.data.price ?? null) : null;
      return `
        <article class="event-row">
          <header>
            <strong>${escapeHtml(event.channel || "unknown")}</strong>
            <small>${formatDate(event.receivedAt)}</small>
          </header>
          <div>Last: ${formatNumber(price, 2)}</div>
          <div class="muted">Mark: ${formatNumber(markPrice, 2)}</div>
        </article>
      `;
    })
    .join("");
}

function renderAnalytics() {
  const analytics = appState.analytics;
  if (!analytics) {
    return;
  }

  refs.analyticsRange.textContent = `${formatDate(analytics.from)} -> ${formatDate(analytics.to)}`;
  refs.metricLastPrice.textContent = formatNumber(analytics.lastPrice, 2);
  refs.metricPriceChange.textContent = `Delta: ${formatNumber(analytics.changeAbs, 2)}`;
  refs.metricVolatility.textContent = formatNumber(analytics.volatilityStdDev, 6);
  refs.metricChangePct.textContent = `Variation: ${formatPercent(analytics.changePct)}`;

  const items = [
    ["Canal", analytics.channel],
    ["Événements", analytics.eventsInWindow],
    ["Prix observés", analytics.pricedEvents],
    ["Min", formatNumber(analytics.minPrice, 2)],
    ["Max", formatNumber(analytics.maxPrice, 2)],
    ["Log return moyen", formatNumber(analytics.avgLogReturn, 8)]
  ];

  refs.analyticsList.innerHTML = items
    .map(([label, value]) => `<div><dt>${escapeHtml(label)}</dt><dd>${escapeHtml(String(value))}</dd></div>`)
    .join("");

  refs.topChannelList.innerHTML = (analytics.topChannels || [])
    .map((item) => `<li><span>${escapeHtml(item.name)}</span><strong>${escapeHtml(String(item.count))}</strong></li>`)
    .join("") || '<li class="muted">Aucune activité récente</li>';
}

function renderSignal() {
  const payload = appState.signal;
  const backtest = appState.backtest;
  if (!payload || !payload.signal) {
    refs.signalDirection.textContent = "Signal indisponible";
    refs.signalProbLong.textContent = "--";
    refs.signalProbShort.textContent = "--";
    refs.signalSl.textContent = "--";
    refs.signalTp.textContent = "--";
    refs.signalConfidence.textContent = "Confiance: --";
    refs.signalReasons.textContent = "Raisons: --";
    refs.signalReliability.textContent = "--";
    refs.signalSample.textContent = "--";
    refs.signalUpdatedAt.textContent = "En attente";
    drawReliabilitySparkline([]);
    return;
  }

  const signal = payload.signal;
  const decision = payload.decision || {};
  const direction = decision.signal || signal.direction || "neutral";
  const isShort = direction === "short";
  const riskBranch = isShort ? signal.risk.short : signal.risk.long;

  refs.signalDirection.textContent = direction === "neutral"
    ? "Neutral"
    : (direction === "long" ? "Long" : "Short");
  refs.signalProbLong.textContent = formatProbability(signal.score && signal.score.long);
  refs.signalProbShort.textContent = formatProbability(signal.score && signal.score.short);
  refs.signalSl.textContent = Number.isFinite(decision.stopLoss)
    ? formatNumber(decision.stopLoss, 2)
    : (riskBranch ? formatNumber(riskBranch.stopLoss, 2) : "--");
  const tp1 = Number.isFinite(decision.takeProfit1)
    ? formatNumber(decision.takeProfit1, 2)
    : (riskBranch && Number.isFinite(riskBranch.takeProfit1) ? formatNumber(riskBranch.takeProfit1, 2) : "--");
  const tp2 = Number.isFinite(decision.takeProfit2)
    ? formatNumber(decision.takeProfit2, 2)
    : (riskBranch ? formatNumber(riskBranch.takeProfit, 2) : "--");
  refs.signalTp.textContent = `${tp1} / ${tp2}`;
  refs.signalConfidence.textContent = `Confiance: ${escapeHtml(String(decision.confidence || signal.confidenceBand || "low")).toUpperCase()}`;
  const reasons = Array.isArray(decision.reasons) ? decision.reasons : [];
  refs.signalReasons.textContent = reasons.length > 0
    ? `Raisons: ${reasons.join(" | ")}`
    : "Raisons: --";

  const reliability = backtest && backtest.metrics
    ? backtest.metrics.reliabilityScore
    : null;
  const sample = backtest && backtest.sample
    ? backtest.sample.decided
    : null;
  refs.signalReliability.textContent = formatProbability(reliability);
  refs.signalSample.textContent = Number.isFinite(sample)
    ? `Échantillon: ${sample}`
    : "Échantillon: --";
  drawReliabilitySparkline(backtest && Array.isArray(backtest.history) ? backtest.history : []);

  refs.signalUpdatedAt.textContent = `Mis à jour: ${formatDate(signal.ts)}`;
}

function renderCandles() {
  if (!appState.candles || appState.candles.error) {
    refs.candlesState.textContent = appState.candles && appState.candles.message
      ? appState.candles.message
      : "TimescaleDB n'est pas disponible pour les bougies.";
    refs.candlesState.classList.remove("hidden");
    refs.candlesTableWrap.classList.add("hidden");
    return;
  }

  const candles = Array.isArray(appState.candles) ? appState.candles : [];
  if (candles.length === 0) {
    refs.candlesState.textContent = "Aucune bougie disponible pour ce canal.";
    refs.candlesState.classList.remove("hidden");
    refs.candlesTableWrap.classList.add("hidden");
    return;
  }

  refs.candlesTableBody.innerHTML = candles
    .slice(0, 12)
    .map((candle) => `
      <tr>
        <td>${formatDate(candle.bucket)}</td>
        <td>${formatNumber(candle.open, 2)}</td>
        <td>${formatNumber(candle.high, 2)}</td>
        <td>${formatNumber(candle.low, 2)}</td>
        <td>${formatNumber(candle.close, 2)}</td>
        <td>${escapeHtml(String(candle.volume))}</td>
      </tr>
    `)
    .join("");
  refs.candlesState.classList.add("hidden");
  refs.candlesTableWrap.classList.remove("hidden");
}

function renderChart() {
  const events = getChannelEvents().filter((event) => getEventPrice(event) !== null);
  const canvas = refs.chartCanvas;
  const context = canvas.getContext("2d");
  const width = canvas.width;
  const height = canvas.height;
  context.clearRect(0, 0, width, height);

  refs.chartTitle.textContent = getSelectedChannel() || "Aucun canal sélectionné";
  if (events.length < 2) {
    context.fillStyle = "rgba(139, 154, 179, 0.92)";
    context.font = "16px IBM Plex Sans";
    context.fillText("Pas assez de points pour tracer une courbe.", 24, 40);
    return;
  }

  const values = events.slice(-40).map((event) => getEventPrice(event));
  const min = Math.min(...values);
  const max = Math.max(...values);
  const padding = 24;
  const usableWidth = width - padding * 2;
  const usableHeight = height - padding * 2;

  context.strokeStyle = "rgba(139, 154, 179, 0.25)";
  context.lineWidth = 1;
  for (let index = 0; index < 4; index += 1) {
    const y = padding + (usableHeight / 3) * index;
    context.beginPath();
    context.moveTo(padding, y);
    context.lineTo(width - padding, y);
    context.stroke();
  }

  const gradient = context.createLinearGradient(0, 0, width, height);
  gradient.addColorStop(0, "#7aa2ff");
  gradient.addColorStop(0.65, "#2ecc8f");
  gradient.addColorStop(1, "#f5b84d");

  context.strokeStyle = gradient;
  context.lineWidth = 3;
  context.beginPath();

  values.forEach((value, index) => {
    const x = padding + (usableWidth / Math.max(values.length - 1, 1)) * index;
    const ratio = max === min ? 0.5 : (value - min) / (max - min);
    const y = height - padding - ratio * usableHeight;
    if (index === 0) {
      context.moveTo(x, y);
    } else {
      context.lineTo(x, y);
    }
  });
  context.stroke();

  context.fillStyle = "rgba(139, 154, 179, 0.95)";
  context.font = "12px IBM Plex Mono";
  context.fillText(`Min ${formatNumber(min, 2)}`, padding, height - 6);
  context.fillText(`Max ${formatNumber(max, 2)}`, width - 120, 18);
}

function renderAll() {
  renderHealth();
  renderSnapshot();
  renderAnalytics();
  renderSignal();
  renderEvents();
  renderCandles();
  renderChart();
  refs.lastSync.textContent = `Dernière synchro: ${formatDate(new Date().toISOString())}`;
}

async function loadHealthAndSnapshot() {
  const [health, snapshot, events] = await Promise.all([
    fetchJson("/health"),
    fetchJson("/snapshot"),
    fetchJson("/events?limit=80")
  ]);

  appState.health = health;
  appState.snapshot = snapshot;
  appState.events = Array.isArray(events.events) ? events.events : [];
  syncChannelOptions();
}

async function loadAnalyticsAndCandles() {
  const channel = encodeURIComponent(getSelectedChannel());
  const windowMs = encodeURIComponent(refs.windowSelect.value);
  appState.analytics = await fetchJson(`/analytics?channel=${channel}&windowMs=${windowMs}`);

  try {
    const candlesPayload = await fetchJson(`/candles?channel=${channel}&limit=12`);
    appState.candles = candlesPayload.candles || [];
  } catch (error) {
    appState.candles = {
      error: true,
      message: error.message
    };
  }

  try {
    appState.signal = await fetchJson(`/signal?channel=${channel}`);
  } catch (_error) {
    appState.signal = null;
  }

  try {
    appState.backtest = await fetchJson(`/backtest/rolling?channel=${channel}&labels=200&windows=10`);
  } catch (_error) {
    appState.backtest = null;
  }
}

function connectLocalWebSocket() {
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  const wsPath = (appState.health && appState.health.localWsPath) ? appState.health.localWsPath : "/stream";
  const socket = new WebSocket(`${protocol}//${window.location.host}${wsPath}`);

  socket.addEventListener("open", () => {
    appState.wsConnected = true;
    renderHealth();
  });

  socket.addEventListener("close", () => {
    appState.wsConnected = false;
    renderHealth();
    window.setTimeout(connectLocalWebSocket, 1500);
  });

  socket.addEventListener("message", (event) => {
    const message = JSON.parse(event.data);
    if (message.type === "snapshot" && message.payload) {
      appState.snapshot = message.payload;
      renderAll();
      return;
    }

    if (message.type === "event" && message.payload) {
      const payload = message.payload;
      appState.events.push(payload);
      appState.events = appState.events.slice(-80);
      if (appState.snapshot && appState.snapshot.latestByChannel) {
        appState.snapshot.latestByChannel[payload.channel] = payload;
        if (appState.snapshot.meta) {
          appState.snapshot.meta.generatedAt = new Date().toISOString();
          appState.snapshot.meta.eventsInMemory = appState.events.length;
        }
      }
      if (appState.health) {
        appState.health.lastMessageAt = payload.receivedAt;
      }
      renderAll();
    }
  });
}

async function refreshDashboard() {
  refs.refreshButton.disabled = true;
  try {
    await loadHealthAndSnapshot();
    await loadAnalyticsAndCandles();
    renderAll();
  } finally {
    refs.refreshButton.disabled = false;
  }
}

refs.refreshButton.addEventListener("click", () => {
  refreshDashboard();
});

refs.channelSelect.addEventListener("change", () => {
  refreshDashboard();
});

refs.windowSelect.addEventListener("change", () => {
  refreshDashboard();
});

window.addEventListener("load", async () => {
  await refreshDashboard();
  connectLocalWebSocket();
  window.setInterval(() => {
    loadAnalyticsAndCandles()
      .then(renderAll)
      .catch(() => {});
  }, 10000);
});