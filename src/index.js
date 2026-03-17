const fs = require("fs");
const http = require("http");
const path = require("path");
const WebSocket = require("ws");

const DERIBIT_WS_URL = process.env.DERIBIT_WS_URL || "wss://www.deribit.com/ws/api/v2";
const HEARTBEAT_INTERVAL_SECONDS = Number(process.env.HEARTBEAT_INTERVAL_SECONDS || 10);
const RECONNECT_DELAY_MS = Number(process.env.RECONNECT_DELAY_MS || 3000);
const SNAPSHOT_INTERVAL_MS = Number(process.env.SNAPSHOT_INTERVAL_MS || 30000);
const MAX_EVENTS_IN_MEMORY = Number(process.env.MAX_EVENTS_IN_MEMORY || 2000);
const API_PORT = Number(process.env.API_PORT || 3000);
const LOCAL_WS_PATH = process.env.LOCAL_WS_PATH || "/stream";
const DATABASE_URL = process.env.DATABASE_URL || "";
const DB_SCHEMA = process.env.DB_SCHEMA || "public";
const DB_TABLE = process.env.DB_TABLE || "deribit_events";
const TIMESCALE_ENABLED = process.env.TIMESCALE_ENABLED === "true";
const TS_CHUNK_INTERVAL = process.env.TS_CHUNK_INTERVAL || "1 day";
const TS_RETENTION_INTERVAL = process.env.TS_RETENTION_INTERVAL || "30 days";
const TS_AGG_BUCKET = process.env.TS_AGG_BUCKET || "1 minute";
const TS_AGG_POLICY_START_OFFSET = process.env.TS_AGG_POLICY_START_OFFSET || "7 days";
const TS_AGG_POLICY_END_OFFSET = process.env.TS_AGG_POLICY_END_OFFSET || "1 minute";
const TS_AGG_POLICY_SCHEDULE = process.env.TS_AGG_POLICY_SCHEDULE || "1 minute";
const ANALYTICS_DEFAULT_WINDOW_MS = Number(process.env.ANALYTICS_DEFAULT_WINDOW_MS || 300000);
const FEATURE_WINDOW_FAST = Number(process.env.FEATURE_WINDOW_FAST || 5);
const FEATURE_WINDOW_SLOW = Number(process.env.FEATURE_WINDOW_SLOW || 20);
const SIGNAL_HORIZON_POINTS = Number(process.env.SIGNAL_HORIZON_POINTS || 15);
const SIGNAL_MIN_PROBABILITY = Number(process.env.SIGNAL_MIN_PROBABILITY || 0.55);
const BACKTEST_DEFAULT_LABELS = Number(process.env.BACKTEST_DEFAULT_LABELS || 200);
const DERIBIT_SERIES_PATH = process.env.DERIBIT_SERIES_PATH || "series_deribit.jsonl";
const FEATURES_SERIES_PATH = process.env.FEATURES_SERIES_PATH || "series_features.jsonl";
const LABELS_SERIES_PATH = process.env.LABELS_SERIES_PATH || "series_labels.jsonl";
const DEFAULT_CHANNELS = [
  "ticker.BTC-PERPETUAL.raw",
  "ticker.ETH-PERPETUAL.raw",
  "deribit_price_index.btc_usd",
  "deribit_price_index.eth_usd"
];

const channels = process.env.DERIBIT_CHANNELS
  ? process.env.DERIBIT_CHANNELS.split(",").map((channel) => channel.trim()).filter(Boolean)
  : DEFAULT_CHANNELS;

const dataDir = path.join(process.cwd(), "data");
const publicDir = path.join(process.cwd(), "public");
const eventsPath = path.join(dataDir, "events.jsonl");
const snapshotPath = path.join(dataDir, "snapshot.json");
const deribitSeriesPath = path.join(dataDir, DERIBIT_SERIES_PATH);
const featuresSeriesPath = path.join(dataDir, FEATURES_SERIES_PATH);
const labelsSeriesPath = path.join(dataDir, LABELS_SERIES_PATH);

const mimeTypes = {
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml; charset=utf-8"
};

fs.mkdirSync(dataDir, { recursive: true });

const state = {
  connectedAt: null,
  lastMessageAt: null,
  reconnectCount: 0,
  channels,
  latestByChannel: {},
  events: [],
  seriesByChannel: {},
  latestSignalByChannel: {}
};

let ws = null;
let rpcId = 0;
let apiServer = null;
let localWss = null;
const localClients = new Set();
let dbPool = null;
let dbEnabled = false;
let dbReady = false;
let DbPoolClass = null;
let timescaleEnabled = false;
let timescaleReady = false;

function sanitizeIdentifier(input, fallback) {
  const sanitized = String(input || "").replace(/[^a-zA-Z0-9_]/g, "");
  return sanitized || fallback;
}

const safeDbSchema = sanitizeIdentifier(DB_SCHEMA, "public");
const safeDbTable = sanitizeIdentifier(DB_TABLE, "deribit_events");
const dbTableIdentifier = `"${safeDbSchema}"."${safeDbTable}"`;
const dbIndexName = `${safeDbTable}_received_at_idx`;
const tsAggViewName = `${safeDbTable}_ohlc`;
const tsAggViewIdentifier = `"${safeDbSchema}"."${tsAggViewName}"`;

function nextRpcId() {
  rpcId += 1;
  return rpcId;
}

function log(message, extra) {
  const timestamp = new Date().toISOString();
  if (typeof extra === "undefined") {
    console.log(`[${timestamp}] ${message}`);
    return;
  }

  console.log(`[${timestamp}] ${message}`, extra);
}

function appendEvent(event) {
  state.events.push(event);
  if (state.events.length > MAX_EVENTS_IN_MEMORY) {
    state.events.shift();
  }

  fs.appendFile(eventsPath, `${JSON.stringify(event)}\n`, (error) => {
    if (error) {
      log("Erreur ecriture events.jsonl", error.message);
    }
  });

  persistEventToDb(event);
}

function appendSeriesLine(filePath, record, label) {
  fs.appendFile(filePath, `${JSON.stringify(record)}\n`, (error) => {
    if (error) {
      log(`Erreur ecriture ${label}`, error.message);
    }
  });
}

function ensureSeriesState(channel) {
  if (!state.seriesByChannel[channel]) {
    state.seriesByChannel[channel] = {
      market: [],
      features: [],
      labels: [],
      pending: []
    };
  }

  return state.seriesByChannel[channel];
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function sigmoid(value) {
  return 1 / (1 + Math.exp(-value));
}

function extractInstrument(channel) {
  const parts = String(channel || "").split(".");
  return parts.length >= 2 ? parts[1] : channel || "unknown";
}

function extractNumberFromData(data, key) {
  if (!data || !key) {
    return null;
  }

  const pathKeys = String(key).split(".");
  let cursor = data;
  for (const pathKey of pathKeys) {
    if (!cursor || typeof cursor !== "object" || !Object.prototype.hasOwnProperty.call(cursor, pathKey)) {
      return null;
    }
    cursor = cursor[pathKey];
  }

  const value = Number(cursor);
  return Number.isFinite(value) ? value : null;
}

function extractMarketPoint(event) {
  const data = event && event.data ? event.data : {};
  const lastPrice = extractNumberFromData(data, "last_price")
    ?? extractNumberFromData(data, "price")
    ?? extractNumberFromData(data, "mark_price");

  if (!Number.isFinite(lastPrice)) {
    return null;
  }

  return {
    type: "market_point",
    ts: event.receivedAt,
    channel: event.channel,
    instrument: extractInstrument(event.channel),
    lastPrice,
    markPrice: extractNumberFromData(data, "mark_price"),
    indexPrice: extractNumberFromData(data, "index_price"),
    bestBid: extractNumberFromData(data, "best_bid_price"),
    bestAsk: extractNumberFromData(data, "best_ask_price"),
    openInterest: extractNumberFromData(data, "open_interest"),
    volume24h: extractNumberFromData(data, "stats.volume"),
    source: "deribit_ws"
  };
}

function pickPrice(series, indexFromEnd) {
  const idx = series.length - 1 - indexFromEnd;
  if (idx < 0 || idx >= series.length) {
    return null;
  }

  const point = series[idx];
  return point && Number.isFinite(point.lastPrice) ? point.lastPrice : null;
}

function spreadBps(point) {
  if (!point || !Number.isFinite(point.bestBid) || !Number.isFinite(point.bestAsk) || !Number.isFinite(point.lastPrice)) {
    return null;
  }

  if (point.lastPrice === 0) {
    return null;
  }

  return ((point.bestAsk - point.bestBid) / point.lastPrice) * 10000;
}

function buildFeaturePoint(channel, marketSeries) {
  if (marketSeries.length < 2) {
    return null;
  }

  const latest = marketSeries[marketSeries.length - 1];
  const previous = marketSeries[marketSeries.length - 2];
  const lastPrice = latest.lastPrice;
  const prevPrice = previous.lastPrice;
  if (!Number.isFinite(lastPrice) || !Number.isFinite(prevPrice) || prevPrice <= 0 || lastPrice <= 0) {
    return null;
  }

  const fastRef = pickPrice(marketSeries, FEATURE_WINDOW_FAST);
  const slowRef = pickPrice(marketSeries, FEATURE_WINDOW_SLOW);

  const fastReturn = Number.isFinite(fastRef) && fastRef > 0 ? Math.log(lastPrice / fastRef) : null;
  const slowReturn = Number.isFinite(slowRef) && slowRef > 0 ? Math.log(lastPrice / slowRef) : null;
  const tickReturn = Math.log(lastPrice / prevPrice);

  const pricesForSlow = marketSeries.slice(-FEATURE_WINDOW_SLOW).map((point) => point.lastPrice).filter((value) => Number.isFinite(value));
  const meanPriceSlow = pricesForSlow.length > 0 ? computeMean(pricesForSlow) : null;
  const stdPriceSlow = pricesForSlow.length > 1 ? computeStdDev(pricesForSlow) : null;
  const zScore = Number.isFinite(meanPriceSlow) && Number.isFinite(stdPriceSlow) && stdPriceSlow > 0
    ? (lastPrice - meanPriceSlow) / stdPriceSlow
    : null;

  const returnsSlow = [];
  for (let index = 1; index < pricesForSlow.length; index += 1) {
    const prev = pricesForSlow[index - 1];
    const curr = pricesForSlow[index];
    if (prev > 0 && curr > 0) {
      returnsSlow.push(Math.log(curr / prev));
    }
  }

  const realizedVol = returnsSlow.length > 1 ? computeStdDev(returnsSlow) : 0;
  const spread = spreadBps(latest);
  const oiPrev = previous.openInterest;
  const oiNow = latest.openInterest;
  const oiDelta = Number.isFinite(oiNow) && Number.isFinite(oiPrev) ? oiNow - oiPrev : null;

  return {
    type: "feature_point",
    ts: latest.ts,
    channel,
    instrument: latest.instrument,
    lastPrice,
    tickReturn,
    returnFast: fastReturn,
    returnSlow: slowReturn,
    zScore,
    realizedVol,
    spreadBps: spread,
    oiDelta,
    windows: {
      fast: FEATURE_WINDOW_FAST,
      slow: FEATURE_WINDOW_SLOW
    }
  };
}

function computeHitOutcome(pricePath, entryPrice, tpPct, slPct, side) {
  if (!Array.isArray(pricePath) || pricePath.length === 0 || !Number.isFinite(entryPrice) || entryPrice <= 0) {
    return "unknown";
  }

  for (const price of pricePath) {
    if (!Number.isFinite(price) || price <= 0) {
      continue;
    }

    const movePct = (price - entryPrice) / entryPrice;
    if (side === "long") {
      if (movePct >= tpPct) {
        return "tp";
      }
      if (movePct <= -slPct) {
        return "sl";
      }
    }

    if (side === "short") {
      if (movePct <= -tpPct) {
        return "tp";
      }
      if (movePct >= slPct) {
        return "sl";
      }
    }
  }

  return "timeout";
}

function buildLabelRecord(pendingFeature, marketSeries, horizonPoints, slPct, tpPct) {
  const startIndex = pendingFeature.marketIndex;
  const endIndex = startIndex + horizonPoints;
  if (endIndex >= marketSeries.length) {
    return null;
  }

  const entry = marketSeries[startIndex];
  const futureWindow = marketSeries.slice(startIndex + 1, endIndex + 1);
  const futurePrices = futureWindow.map((point) => point.lastPrice).filter((price) => Number.isFinite(price));
  if (!entry || !Number.isFinite(entry.lastPrice) || futurePrices.length === 0) {
    return null;
  }

  const lastFuturePrice = futurePrices[futurePrices.length - 1];
  const realizedReturn = (lastFuturePrice - entry.lastPrice) / entry.lastPrice;

  const longOutcome = computeHitOutcome(futurePrices, entry.lastPrice, tpPct, slPct, "long");
  const shortOutcome = computeHitOutcome(futurePrices, entry.lastPrice, tpPct, slPct, "short");
  const predictedDirection = pendingFeature.prediction && pendingFeature.prediction.direction
    ? pendingFeature.prediction.direction
    : "neutral";

  let predictedSuccess = null;
  if (predictedDirection === "long") {
    predictedSuccess = longOutcome === "tp";
  } else if (predictedDirection === "short") {
    predictedSuccess = shortOutcome === "tp";
  }

  return {
    type: "label_point",
    ts: marketSeries[endIndex].ts,
    channel: pendingFeature.channel,
    instrument: pendingFeature.instrument,
    featureTs: pendingFeature.ts,
    horizonPoints,
    entryPrice: entry.lastPrice,
    closePrice: lastFuturePrice,
    realizedReturn,
    long: {
      outcome: longOutcome,
      success: longOutcome === "tp"
    },
    short: {
      outcome: shortOutcome,
      success: shortOutcome === "tp"
    },
    riskModel: {
      slPct,
      tpPct
    },
    prediction: {
      model: pendingFeature.prediction && pendingFeature.prediction.model
        ? pendingFeature.prediction.model
        : "heuristic-v1",
      direction: predictedDirection,
      confidence: pendingFeature.prediction && Number.isFinite(pendingFeature.prediction.confidence)
        ? pendingFeature.prediction.confidence
        : null,
      score: pendingFeature.prediction && pendingFeature.prediction.score
        ? pendingFeature.prediction.score
        : null,
      success: predictedSuccess
    }
  };
}

function computeBacktestMetrics(labels) {
  const directional = labels.filter((label) => {
    const direction = label && label.prediction ? label.prediction.direction : "neutral";
    return direction === "long" || direction === "short";
  });

  const decided = directional.filter((label) => {
    const success = label && label.prediction ? label.prediction.success : null;
    return typeof success === "boolean";
  });
  const wins = decided.filter((label) => label.prediction.success).length;
  const accuracy = decided.length > 0 ? wins / decided.length : null;
  const coverage = labels.length > 0 ? directional.length / labels.length : null;
  const reliabilityScore = accuracy === null || coverage === null
    ? null
    : (accuracy * 0.7) + (coverage * 0.3);

  return {
    sample: {
      labels: labels.length,
      directional: directional.length,
      decided: decided.length,
      wins,
      losses: decided.length - wins
    },
    metrics: {
      accuracy,
      coverage,
      reliabilityScore
    }
  };
}

function buildBacktestRollingPayload(channel, limit, windows) {
  const channels = channel ? [channel] : Object.keys(state.seriesByChannel);
  const labels = channels
    .flatMap((name) => (state.seriesByChannel[name] ? state.seriesByChannel[name].labels : []))
    .slice(-limit);
  const rollWindows = Math.max(1, Math.min(windows, 50));
  const chunkSize = Math.max(1, Math.floor(limit / rollWindows));
  const history = [];
  for (let index = 0; index < rollWindows; index += 1) {
    const end = labels.length - ((rollWindows - 1 - index) * chunkSize);
    const start = Math.max(0, end - chunkSize);
    const slice = labels.slice(start, Math.max(start, end));
    if (slice.length === 0) {
      continue;
    }

    const sliceStats = computeBacktestMetrics(slice);
    history.push({
      windowIndex: index,
      fromTs: slice[0].ts,
      toTs: slice[slice.length - 1].ts,
      ...sliceStats
    });
  }

  const summary = computeBacktestMetrics(labels);
  const decided = summary.sample.decided;

  return {
    mode: "rolling",
    channel: channel || "all",
    requestedLabels: limit,
    windows: {
      requested: rollWindows,
      chunkSize,
      computed: history.length
    },
    sample: summary.sample,
    metrics: summary.metrics,
    history,
    message: decided === 0
      ? "Pas assez de labels décisionnels pour calculer une accuracy rolling"
      : "Backtest rolling calculé sur labels récents"
  };
}

function makeSignalFromFeature(feature) {
  if (!feature) {
    return null;
  }

  const mFast = Number.isFinite(feature.returnFast) ? feature.returnFast : 0;
  const mSlow = Number.isFinite(feature.returnSlow) ? feature.returnSlow : 0;
  const z = Number.isFinite(feature.zScore) ? feature.zScore : 0;
  const vol = Number.isFinite(feature.realizedVol) ? feature.realizedVol : 0;
  const spread = Number.isFinite(feature.spreadBps) ? feature.spreadBps : 0;
  const oi = Number.isFinite(feature.oiDelta) ? feature.oiDelta : 0;

  const trendCore = (mFast * 120) + (mSlow * 80);
  const meanRevert = clamp(-z * 0.35, -1.2, 1.2);
  const microPenalty = clamp((spread - 6) / 20, 0, 1.5);
  const volPenalty = clamp((vol - 0.008) / 0.03, 0, 1.2);
  const oiBoost = clamp(oi / 10000, -0.5, 0.5);

  const rawLong = trendCore + meanRevert + oiBoost - microPenalty - volPenalty;
  const rawShort = (-trendCore) - meanRevert - oiBoost - microPenalty - volPenalty;
  const longProbability = sigmoid(rawLong);
  const shortProbability = sigmoid(rawShort);

  const bestSide = longProbability >= shortProbability ? "long" : "short";
  const bestProbability = Math.max(longProbability, shortProbability);
  const direction = bestProbability >= SIGNAL_MIN_PROBABILITY ? bestSide : "neutral";

  const volForRisk = Math.max(vol, 0.0015);
  const slPct = clamp(volForRisk * 2.2, 0.002, 0.03);
  const tpPct = clamp(volForRisk * 3.4, slPct * 1.2, 0.05);
  const entry = feature.lastPrice;

  const slLong = entry * (1 - slPct);
  const tpLong = entry * (1 + tpPct);
  const slShort = entry * (1 + slPct);
  const tpShort = entry * (1 - tpPct);

  return {
    ts: feature.ts,
    channel: feature.channel,
    instrument: feature.instrument,
    model: "heuristic-v1",
    direction,
    score: {
      long: longProbability,
      short: shortProbability
    },
    confidence: Math.abs(longProbability - shortProbability),
    risk: {
      slPct,
      tpPct,
      long: {
        entry,
        stopLoss: slLong,
        takeProfit: tpLong
      },
      short: {
        entry,
        stopLoss: slShort,
        takeProfit: tpShort
      }
    },
    factors: {
      returnFast: feature.returnFast,
      returnSlow: feature.returnSlow,
      zScore: feature.zScore,
      realizedVol: feature.realizedVol,
      spreadBps: feature.spreadBps,
      oiDelta: feature.oiDelta
    }
  };
}

function updateDerivedSeries(event) {
  const marketPoint = extractMarketPoint(event);
  if (!marketPoint) {
    return;
  }

  const series = ensureSeriesState(event.channel);
  series.market.push(marketPoint);
  if (series.market.length > 1000) {
    series.market.shift();
  }
  appendSeriesLine(deribitSeriesPath, marketPoint, DERIBIT_SERIES_PATH);

  const featurePoint = buildFeaturePoint(event.channel, series.market);
  if (!featurePoint) {
    return;
  }

  series.features.push(featurePoint);
  if (series.features.length > 1000) {
    series.features.shift();
  }
  appendSeriesLine(featuresSeriesPath, featurePoint, FEATURES_SERIES_PATH);

  const signal = makeSignalFromFeature(featurePoint);
  if (signal) {
    state.latestSignalByChannel[event.channel] = signal;
  }

  const pendingRecord = {
    ...featurePoint,
    marketIndex: series.market.length - 1,
    prediction: signal
      ? {
        model: signal.model,
        direction: signal.direction,
        confidence: signal.confidence,
        score: signal.score
      }
      : {
        model: "heuristic-v1",
        direction: "neutral",
        confidence: null,
        score: null
      }
  };
  series.pending.push(pendingRecord);
  if (series.pending.length > 2000) {
    series.pending.shift();
  }

  while (series.pending.length > 0) {
    const candidate = series.pending[0];
    const signalForRisk = state.latestSignalByChannel[event.channel];
    const risk = signalForRisk && signalForRisk.risk
      ? signalForRisk.risk
      : { slPct: 0.004, tpPct: 0.008 };

    const label = buildLabelRecord(
      candidate,
      series.market,
      SIGNAL_HORIZON_POINTS,
      risk.slPct,
      risk.tpPct
    );

    if (!label) {
      break;
    }

    series.pending.shift();
    series.labels.push(label);
    if (series.labels.length > 1000) {
      series.labels.shift();
    }
    appendSeriesLine(labelsSeriesPath, label, LABELS_SERIES_PATH);
  }
}

function getSeriesSchema() {
  return {
    marketSeries: {
      storage: DERIBIT_SERIES_PATH,
      key: ["ts", "channel"],
      fields: {
        ts: "ISO timestamp",
        channel: "Deribit channel",
        instrument: "Instrument code",
        lastPrice: "Number",
        markPrice: "Number|null",
        indexPrice: "Number|null",
        bestBid: "Number|null",
        bestAsk: "Number|null",
        openInterest: "Number|null",
        volume24h: "Number|null"
      }
    },
    featureSeries: {
      storage: FEATURES_SERIES_PATH,
      key: ["ts", "channel"],
      fields: {
        tickReturn: "log return t vs t-1",
        returnFast: `log return t vs t-${FEATURE_WINDOW_FAST}`,
        returnSlow: `log return t vs t-${FEATURE_WINDOW_SLOW}`,
        zScore: "z-score on slow window prices",
        realizedVol: "std dev of log returns on slow window",
        spreadBps: "(ask-bid)/price in bps",
        oiDelta: "open interest delta"
      }
    },
    labelSeries: {
      storage: LABELS_SERIES_PATH,
      key: ["featureTs", "channel"],
      fields: {
        horizonPoints: "future ticks horizon",
        realizedReturn: "close(entry+h)-entry / entry",
        long: "{ outcome: tp|sl|timeout, success: boolean }",
        short: "{ outcome: tp|sl|timeout, success: boolean }",
        riskModel: "{ slPct, tpPct }"
      }
    }
  };
}

async function setupDatabase() {
  if (!DATABASE_URL) {
    log("PostgreSQL desactive (DATABASE_URL non defini)");
    return;
  }

  try {
    ({ Pool: DbPoolClass } = require("pg"));
  } catch (_error) {
    log("PostgreSQL desactive (module pg non installe)");
    return;
  }

  dbEnabled = true;
  dbPool = new DbPoolClass({ connectionString: DATABASE_URL });

  try {
    await dbPool.query(`CREATE SCHEMA IF NOT EXISTS "${safeDbSchema}"`);
    await dbPool.query(
      `
        CREATE TABLE IF NOT EXISTS ${dbTableIdentifier} (
          id BIGSERIAL PRIMARY KEY,
          received_at TIMESTAMPTZ NOT NULL,
          channel TEXT NOT NULL,
          payload JSONB NOT NULL
        )
      `
    );
    await dbPool.query(`CREATE INDEX IF NOT EXISTS ${dbIndexName} ON ${dbTableIdentifier} (received_at DESC)`);

    if (TIMESCALE_ENABLED) {
      await setupTimescale();
    }

    dbReady = true;
    log(`PostgreSQL actif: table ${safeDbSchema}.${safeDbTable}`);
  } catch (error) {
    log("Initialisation PostgreSQL en echec", error.message);
  }
}

async function setupTimescale() {
  if (!dbPool) {
    return;
  }

  timescaleEnabled = true;

  try {
    await dbPool.query("CREATE EXTENSION IF NOT EXISTS timescaledb");
  } catch (error) {
    log("TimescaleDB indisponible (extension non installable)", error.message);
    return;
  }

  try {
    await dbPool.query(
      `
        SELECT create_hypertable(
          $1,
          'received_at',
          chunk_time_interval => INTERVAL '${TS_CHUNK_INTERVAL}',
          if_not_exists => TRUE
        )
      `,
      [`${safeDbSchema}.${safeDbTable}`]
    );

    await dbPool.query(
      `
        SELECT add_retention_policy(
          $1,
          INTERVAL '${TS_RETENTION_INTERVAL}',
          if_not_exists => TRUE
        )
      `,
      [`${safeDbSchema}.${safeDbTable}`]
    );

    await dbPool.query(
      `
        CREATE MATERIALIZED VIEW IF NOT EXISTS ${tsAggViewIdentifier}
        WITH (timescaledb.continuous) AS
        SELECT
          time_bucket(INTERVAL '${TS_AGG_BUCKET}', received_at) AS bucket,
          channel,
          (array_agg((payload->'data'->>'last_price')::double precision ORDER BY received_at ASC))[1] AS open,
          max((payload->'data'->>'last_price')::double precision) AS high,
          min((payload->'data'->>'last_price')::double precision) AS low,
          (array_agg((payload->'data'->>'last_price')::double precision ORDER BY received_at DESC))[1] AS close,
          count(*)::bigint AS volume
        FROM ${dbTableIdentifier}
        WHERE payload->'data'->>'last_price' IS NOT NULL
        GROUP BY bucket, channel
        WITH NO DATA
      `
    );

    await dbPool.query(
      `
        SELECT add_continuous_aggregate_policy(
          $1,
          start_offset => INTERVAL '${TS_AGG_POLICY_START_OFFSET}',
          end_offset => INTERVAL '${TS_AGG_POLICY_END_OFFSET}',
          schedule_interval => INTERVAL '${TS_AGG_POLICY_SCHEDULE}'
        )
      `,
      [`${safeDbSchema}.${tsAggViewName}`]
    );

    timescaleReady = true;
    log(`TimescaleDB actif: hypertable ${safeDbSchema}.${safeDbTable}, vue ${safeDbSchema}.${tsAggViewName}`);
  } catch (error) {
    log("Initialisation TimescaleDB partielle", error.message);
  }
}

function persistEventToDb(event) {
  if (!dbEnabled || !dbReady || !dbPool) {
    return;
  }

  const payload = JSON.stringify(event);
  const receivedAt = event.receivedAt || new Date().toISOString();

  dbPool
    .query(
      `
        INSERT INTO ${dbTableIdentifier} (received_at, channel, payload)
        VALUES ($1::timestamptz, $2::text, $3::jsonb)
      `,
      [receivedAt, event.channel || "unknown", payload]
    )
    .catch((error) => {
      log("Insertion PostgreSQL en echec", error.message);
    });
}

function buildSnapshot() {
  return {
    meta: {
      generatedAt: new Date().toISOString(),
      connectedAt: state.connectedAt,
      lastMessageAt: state.lastMessageAt,
      reconnectCount: state.reconnectCount,
      channels: state.channels,
      eventsInMemory: state.events.length
    },
    latestByChannel: state.latestByChannel,
    latestSignalByChannel: state.latestSignalByChannel
  };
}

function sendJson(res, statusCode, payload) {
  res.writeHead(statusCode, { "Content-Type": "application/json" });
  res.end(JSON.stringify(payload));
}

function sendText(res, statusCode, contentType, payload) {
  res.writeHead(statusCode, { "Content-Type": contentType });
  res.end(payload);
}

function resolveStaticAsset(urlPathname) {
  const relativePath = urlPathname === "/" ? "index.html" : urlPathname.replace(/^\/+/, "");
  const normalizedPath = path.normalize(relativePath);
  if (normalizedPath.startsWith("..") || path.isAbsolute(normalizedPath)) {
    return null;
  }

  return path.join(publicDir, normalizedPath);
}

function serveStaticAsset(res, urlPathname) {
  const assetPath = resolveStaticAsset(urlPathname);
  if (!assetPath) {
    sendJson(res, 404, {
      error: "Not Found",
      message: "Use /health, /snapshot, /events, /candles, /analytics, /signal, /features, /labels, /series/schema, /backtest/rolling or /"
    });
    return;
  }

  fs.readFile(assetPath, (error, content) => {
    if (error) {
      if (error.code === "ENOENT") {
        sendJson(res, 404, {
          error: "Not Found",
          message: "Use /health, /snapshot, /events, /candles, /analytics, /signal, /features, /labels, /series/schema, /backtest/rolling or /"
        });
        return;
      }

      sendJson(res, 500, {
        error: "static_asset_failed",
        message: error.message
      });
      return;
    }

    const extension = path.extname(assetPath).toLowerCase();
    const contentType = mimeTypes[extension] || "application/octet-stream";
    sendText(res, 200, contentType, content);
  });
}

function buildHealth() {
  return {
    status: state.connectedAt ? "connected" : "disconnected",
    connectedAt: state.connectedAt,
    lastMessageAt: state.lastMessageAt,
    reconnectCount: state.reconnectCount,
    channels: state.channels,
    localWsPath: LOCAL_WS_PATH,
    localWsClients: localClients.size,
    database: {
      enabled: dbEnabled,
      ready: dbReady,
      schema: safeDbSchema,
      table: safeDbTable,
      timescale: {
        enabled: timescaleEnabled,
        ready: timescaleReady,
        aggregateView: `${safeDbSchema}.${tsAggViewName}`
      }
    }
  };
}

function parseLimit(rawValue, fallback, maxValue) {
  const parsed = Number(rawValue);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }

  return Math.max(1, Math.min(Math.floor(parsed), maxValue));
}

function buildSignalPayload(channel) {
  const availableChannels = Object.keys(state.latestSignalByChannel);
  const selected = channel || availableChannels[0] || null;
  const signal = selected ? state.latestSignalByChannel[selected] : null;

  if (!signal) {
    return {
      availableChannels,
      signal: null,
      message: "Signal indisponible (insufficient market/features data)"
    };
  }

  return {
    availableChannels,
    signal
  };
}

function extractPrice(event) {
  const rawPrice = event && event.data
    ? (event.data.last_price ?? event.data.price ?? event.data.mark_price ?? null)
    : null;
  const price = Number(rawPrice);
  if (!Number.isFinite(price)) {
    return null;
  }

  return price;
}

function computeMean(values) {
  if (values.length === 0) {
    return 0;
  }

  const total = values.reduce((acc, value) => acc + value, 0);
  return total / values.length;
}

function computeStdDev(values) {
  if (values.length <= 1) {
    return 0;
  }

  const mean = computeMean(values);
  const variance = values.reduce((acc, value) => acc + ((value - mean) ** 2), 0) / (values.length - 1);
  return Math.sqrt(variance);
}

function buildAnalytics(channel, windowMs, topChannelsLimit) {
  const nowMs = Date.now();
  const cutoffMs = nowMs - windowMs;

  const windowEvents = state.events.filter((event) => {
    const eventMs = Date.parse(event.receivedAt || "");
    return Number.isFinite(eventMs) && eventMs >= cutoffMs;
  });

  const scopedEvents = channel
    ? windowEvents.filter((event) => event.channel === channel)
    : windowEvents;

  const pricedEvents = scopedEvents
    .map((event) => ({
      receivedAt: event.receivedAt,
      price: extractPrice(event)
    }))
    .filter((item) => item.price !== null);

  const prices = pricedEvents.map((item) => item.price);
  const firstPrice = prices.length > 0 ? prices[0] : null;
  const lastPrice = prices.length > 0 ? prices[prices.length - 1] : null;
  const minPrice = prices.length > 0 ? Math.min(...prices) : null;
  const maxPrice = prices.length > 0 ? Math.max(...prices) : null;

  const returns = [];
  for (let index = 1; index < prices.length; index += 1) {
    const previous = prices[index - 1];
    const current = prices[index];
    if (previous > 0 && current > 0) {
      returns.push(Math.log(current / previous));
    }
  }

  const topChannels = Object.entries(
    windowEvents.reduce((acc, event) => {
      const key = event.channel || "unknown";
      acc[key] = (acc[key] || 0) + 1;
      return acc;
    }, {})
  )
    .sort((a, b) => b[1] - a[1])
    .slice(0, topChannelsLimit)
    .map(([name, count]) => ({ name, count }));

  let changeAbs = null;
  let changePct = null;
  if (firstPrice !== null && lastPrice !== null) {
    changeAbs = lastPrice - firstPrice;
    changePct = firstPrice !== 0 ? (changeAbs / firstPrice) * 100 : null;
  }

  return {
    channel: channel || "all",
    windowMs,
    from: new Date(cutoffMs).toISOString(),
    to: new Date(nowMs).toISOString(),
    eventsInWindow: scopedEvents.length,
    pricedEvents: prices.length,
    firstPrice,
    lastPrice,
    minPrice,
    maxPrice,
    changeAbs,
    changePct,
    avgLogReturn: returns.length > 0 ? computeMean(returns) : null,
    volatilityStdDev: returns.length > 1 ? computeStdDev(returns) : null,
    topChannels
  };
}

async function queryCandles(channel, limit) {
  if (!timescaleEnabled || !timescaleReady) {
    return { error: "timescale_unavailable", message: "TimescaleDB is not enabled or ready" };
  }

  if (!dbEnabled || !dbReady || !dbPool) {
    return { error: "database_unavailable", message: "Database is not enabled" };
  }

  const result = await dbPool.query(
    `
      SELECT
        bucket,
        channel,
        open,
        high,
        low,
        close,
        volume
      FROM ${tsAggViewIdentifier}
      WHERE ($1::text IS NULL OR channel = $1::text)
      ORDER BY bucket DESC
      LIMIT $2
    `,
    [channel || null, limit]
  );

  return { rows: result.rows };
}

function broadcastToLocalClients(event) {
  if (localClients.size === 0) {
    return;
  }

  const payload = JSON.stringify({
    type: "event",
    payload: event
  });

  for (const client of localClients) {
    if (client.readyState !== WebSocket.OPEN) {
      localClients.delete(client);
      continue;
    }

    client.send(payload);
  }
}

function startApiServer() {
  apiServer = http.createServer((req, res) => {
    const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);

    if (req.method === "GET" && (url.pathname === "/" || url.pathname.startsWith("/assets/"))) {
      serveStaticAsset(res, url.pathname === "/" ? "/" : url.pathname.replace(/^\/assets\//, "/"));
      return;
    }

    if (req.method === "GET" && url.pathname === "/health") {
      sendJson(res, 200, buildHealth());
      return;
    }

    if (req.method === "GET" && url.pathname === "/snapshot") {
      sendJson(res, 200, buildSnapshot());
      return;
    }

    if (req.method === "GET" && url.pathname === "/events") {
      const safeLimit = parseLimit(url.searchParams.get("limit") || 200, 200, MAX_EVENTS_IN_MEMORY);
      const events = state.events.slice(-safeLimit);

      sendJson(res, 200, {
        count: events.length,
        events
      });
      return;
    }

    if (req.method === "GET" && url.pathname === "/candles") {
      const channel = (url.searchParams.get("channel") || "").trim();
      const limit = parseLimit(url.searchParams.get("limit") || 200, 200, 5000);

      queryCandles(channel || null, limit)
        .then((result) => {
          if (result.error) {
            sendJson(res, 503, result);
            return;
          }

          sendJson(res, 200, {
            count: result.rows.length,
            candles: result.rows
          });
        })
        .catch((error) => {
          sendJson(res, 500, {
            error: "candles_query_failed",
            message: error.message
          });
        });
      return;
    }

    if (req.method === "GET" && url.pathname === "/analytics") {
      const channel = (url.searchParams.get("channel") || "").trim();
      const windowMs = parseLimit(
        url.searchParams.get("windowMs") || ANALYTICS_DEFAULT_WINDOW_MS,
        ANALYTICS_DEFAULT_WINDOW_MS,
        86400000
      );
      const topChannelsLimit = parseLimit(url.searchParams.get("topChannelsLimit") || 5, 5, 50);

      sendJson(res, 200, buildAnalytics(channel || null, windowMs, topChannelsLimit));
      return;
    }

    if (req.method === "GET" && url.pathname === "/signal") {
      const channel = (url.searchParams.get("channel") || "").trim();
      sendJson(res, 200, buildSignalPayload(channel || null));
      return;
    }

    if (req.method === "GET" && url.pathname === "/features") {
      const channel = (url.searchParams.get("channel") || "").trim();
      const limit = parseLimit(url.searchParams.get("limit") || 100, 100, 1000);
      const channels = channel ? [channel] : Object.keys(state.seriesByChannel);
      const features = channels
        .flatMap((name) => (state.seriesByChannel[name] ? state.seriesByChannel[name].features : []))
        .slice(-limit);

      sendJson(res, 200, {
        count: features.length,
        features
      });
      return;
    }

    if (req.method === "GET" && url.pathname === "/labels") {
      const channel = (url.searchParams.get("channel") || "").trim();
      const limit = parseLimit(url.searchParams.get("limit") || 100, 100, 1000);
      const channels = channel ? [channel] : Object.keys(state.seriesByChannel);
      const labels = channels
        .flatMap((name) => (state.seriesByChannel[name] ? state.seriesByChannel[name].labels : []))
        .slice(-limit);

      sendJson(res, 200, {
        count: labels.length,
        labels
      });
      return;
    }

    if (req.method === "GET" && url.pathname === "/series/schema") {
      sendJson(res, 200, getSeriesSchema());
      return;
    }

    if (req.method === "GET" && url.pathname === "/backtest/rolling") {
      const channel = (url.searchParams.get("channel") || "").trim();
      const labelsLimit = parseLimit(
        url.searchParams.get("labels") || BACKTEST_DEFAULT_LABELS,
        BACKTEST_DEFAULT_LABELS,
        5000
      );
      const windows = parseLimit(url.searchParams.get("windows") || 10, 10, 50);
      sendJson(res, 200, buildBacktestRollingPayload(channel || null, labelsLimit, windows));
      return;
    }

    sendJson(res, 404, {
      error: "Not Found",
      message: "Use /health, /snapshot, /events, /candles, /analytics, /signal, /features, /labels, /series/schema or /backtest/rolling"
    });
  });

  localWss = new WebSocket.WebSocketServer({
    server: apiServer,
    path: LOCAL_WS_PATH
  });

  localWss.on("connection", (socket) => {
    localClients.add(socket);

    socket.send(
      JSON.stringify({
        type: "snapshot",
        payload: buildSnapshot()
      })
    );

    socket.on("close", () => {
      localClients.delete(socket);
    });
  });

  apiServer.listen(API_PORT, () => {
    log(`API locale demarree sur http://localhost:${API_PORT}`);
    log(`Flux WebSocket local sur ws://localhost:${API_PORT}${LOCAL_WS_PATH}`);
  });
}

function writeSnapshot() {
  const snapshot = buildSnapshot();

  fs.writeFile(snapshotPath, JSON.stringify(snapshot, null, 2), (error) => {
    if (error) {
      log("Erreur ecriture snapshot.json", error.message);
    }
  });
}

function sendRpc(method, params) {
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    return;
  }

  ws.send(
    JSON.stringify({
      jsonrpc: "2.0",
      id: nextRpcId(),
      method,
      params
    })
  );
}

function subscribeToChannels() {
  sendRpc("public/subscribe", { channels });
  log(`Souscription a ${channels.length} canaux Deribit`);
}

function setupHeartbeat() {
  sendRpc("public/set_heartbeat", { interval: HEARTBEAT_INTERVAL_SECONDS });
}

function handleNotification(message) {
  if (!message.params || !message.params.channel) {
    return;
  }

  const event = {
    type: "channel_update",
    receivedAt: new Date().toISOString(),
    channel: message.params.channel,
    data: message.params.data
  };

  state.lastMessageAt = event.receivedAt;
  state.latestByChannel[event.channel] = event;
  appendEvent(event);
  updateDerivedSeries(event);
  broadcastToLocalClients(event);
}

function handleHeartbeat(message) {
  if (message.method === "heartbeat") {
    sendRpc("public/test", {});
  }
}

function connect() {
  log(`Connexion Deribit -> ${DERIBIT_WS_URL}`);

  ws = new WebSocket(DERIBIT_WS_URL);

  ws.on("open", () => {
    state.connectedAt = new Date().toISOString();
    setupHeartbeat();
    subscribeToChannels();
    log("Connexion ouverte");
  });

  ws.on("message", (rawBuffer) => {
    try {
      const rawMessage = rawBuffer.toString();
      const message = JSON.parse(rawMessage);

      if (message.method === "subscription") {
        handleNotification(message);
        return;
      }

      if (message.method === "heartbeat") {
        handleHeartbeat(message);
        return;
      }

      if (message.error) {
        log("Erreur JSON-RPC recu", message.error);
      }
    } catch (error) {
      log("Message invalide recu", error.message);
    }
  });

  ws.on("error", (error) => {
    log("Erreur websocket", error.message);
  });

  ws.on("close", () => {
    log("Connexion fermee, tentative de reconnexion...");
    state.connectedAt = null;
    state.reconnectCount += 1;

    setTimeout(connect, RECONNECT_DELAY_MS);
  });
}

setInterval(writeSnapshot, SNAPSHOT_INTERVAL_MS);

setupDatabase().finally(() => {
  startApiServer();
  connect();
});

process.on("SIGINT", () => {
  log("Arret demande (SIGINT)");
  writeSnapshot();
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.close();
  }

  if (localWss) {
    localWss.close();
  }

  if (apiServer) {
    apiServer.close();
  }

  if (dbPool) {
    dbPool.end().catch((error) => {
      log("Fermeture PostgreSQL en echec", error.message);
    });
  }

  setTimeout(() => process.exit(0), 300);
});
