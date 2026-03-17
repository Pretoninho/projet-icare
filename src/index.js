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
const eventsPath = path.join(dataDir, "events.jsonl");
const snapshotPath = path.join(dataDir, "snapshot.json");

fs.mkdirSync(dataDir, { recursive: true });

const state = {
  connectedAt: null,
  lastMessageAt: null,
  reconnectCount: 0,
  channels,
  latestByChannel: {},
  events: []
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
    latestByChannel: state.latestByChannel
  };
}

function sendJson(res, statusCode, payload) {
  res.writeHead(statusCode, { "Content-Type": "application/json" });
  res.end(JSON.stringify(payload));
}

function buildHealth() {
  return {
    status: state.connectedAt ? "connected" : "disconnected",
    connectedAt: state.connectedAt,
    lastMessageAt: state.lastMessageAt,
    reconnectCount: state.reconnectCount,
    channels: state.channels,
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

function extractPrice(event) {
  const rawPrice = event && event.data ? event.data.last_price : null;
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
  if (!dbEnabled || !dbReady || !dbPool) {
    return { error: "database_unavailable", message: "Database is not enabled" };
  }

  if (!timescaleEnabled || !timescaleReady) {
    return { error: "timescale_unavailable", message: "TimescaleDB is not enabled or ready" };
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

    sendJson(res, 404, {
      error: "Not Found",
      message: "Use /health, /snapshot, /events, /candles or /analytics"
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
