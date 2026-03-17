const test = require("node:test");
const assert = require("node:assert/strict");
const http = require("http");
const { spawn } = require("child_process");
const path = require("path");
const WebSocket = require("ws");

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function getFreePort() {
  return new Promise((resolve, reject) => {
    const server = http.createServer();
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      server.close((error) => {
        if (error) {
          reject(error);
          return;
        }

        resolve(address.port);
      });
    });
  });
}

function httpGetJson(port, pathname) {
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        host: "127.0.0.1",
        port,
        path: pathname,
        method: "GET"
      },
      (res) => {
        let data = "";
        res.on("data", (chunk) => {
          data += chunk.toString();
        });

        res.on("end", () => {
          try {
            resolve({
              statusCode: res.statusCode,
              body: JSON.parse(data || "{}")
            });
          } catch (error) {
            reject(error);
          }
        });
      }
    );

    req.on("error", reject);
    req.end();
  });
}

function httpGetText(port, pathname) {
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        host: "127.0.0.1",
        port,
        path: pathname,
        method: "GET"
      },
      (res) => {
        let data = "";
        res.on("data", (chunk) => {
          data += chunk.toString();
        });

        res.on("end", () => {
          resolve({
            statusCode: res.statusCode,
            body: data,
            headers: res.headers
          });
        });
      }
    );

    req.on("error", reject);
    req.end();
  });
}

async function waitFor(condition, timeoutMs, message) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await condition()) {
      return;
    }

    await delay(100);
  }

  throw new Error(message);
}

test("replication et API locale fonctionnent avec mock Deribit", async (t) => {
  const deribitPort = await getFreePort();
  const apiPort = await getFreePort();
  const channel = "ticker.BTC-PERPETUAL.raw";

  const deribitServer = new WebSocket.WebSocketServer({
    host: "127.0.0.1",
    port: deribitPort
  });

  let subscribed = false;
  let eventCounter = 0;
  let broadcastTimer = null;

  deribitServer.on("connection", (socket) => {
    socket.on("message", (raw) => {
      const msg = JSON.parse(raw.toString());

      if (msg.method === "public/subscribe") {
        subscribed = true;
        socket.send(JSON.stringify({ jsonrpc: "2.0", id: msg.id, result: { subscribed: msg.params.channels } }));
      }

      if (msg.method === "public/set_heartbeat") {
        socket.send(JSON.stringify({ jsonrpc: "2.0", id: msg.id, result: "ok" }));
      }

      if (msg.method === "public/test") {
        socket.send(JSON.stringify({ jsonrpc: "2.0", id: msg.id, result: "ok" }));
      }
    });

    broadcastTimer = setInterval(() => {
      if (!subscribed || socket.readyState !== WebSocket.OPEN) {
        return;
      }

      eventCounter += 1;
      socket.send(
        JSON.stringify({
          jsonrpc: "2.0",
          method: "subscription",
          params: {
            channel,
            data: {
              last_price: 100000 + eventCounter,
              mark_price: 100000 + eventCounter
            }
          }
        })
      );
    }, 150);
  });

  t.after(async () => {
    if (broadcastTimer) {
      clearInterval(broadcastTimer);
    }

    for (const client of deribitServer.clients) {
      try {
        client.terminate();
      } catch (_error) {
      }
    }

    await new Promise((resolve) => deribitServer.close(resolve));
  });

  const app = spawn("node", [path.join("src", "index.js")], {
    cwd: path.join(__dirname, ".."),
    env: {
      ...process.env,
      DERIBIT_WS_URL: `ws://127.0.0.1:${deribitPort}`,
      DERIBIT_CHANNELS: channel,
      API_PORT: String(apiPort),
      LOCAL_WS_PATH: "/stream-test",
      SNAPSHOT_INTERVAL_MS: "500"
    },
    stdio: ["ignore", "pipe", "pipe"]
  });

  let appLogs = "";
  app.stdout.on("data", (chunk) => {
    appLogs += chunk.toString();
  });
  app.stderr.on("data", (chunk) => {
    appLogs += chunk.toString();
  });

  t.after(async () => {
    if (!app.killed) {
      app.kill("SIGINT");
      await new Promise((resolve) => {
        app.once("exit", resolve);
        setTimeout(resolve, 1500);
      });
    }
  });

  await waitFor(async () => {
    try {
      const health = await httpGetJson(apiPort, "/health");
      return health.statusCode === 200;
    } catch (_error) {
      return false;
    }
  }, 10000, "API locale indisponible");

  await waitFor(async () => {
    const health = await httpGetJson(apiPort, "/health");
    return health.body.status === "connected";
  }, 10000, "Connexion mock Deribit non etablie");

  const homePage = await httpGetText(apiPort, "/");
  assert.equal(homePage.statusCode, 200);
  assert.match(homePage.headers["content-type"], /text\/html/);
  assert.match(homePage.body, /iCare Live Console/);

  await waitFor(async () => {
    const snapshot = await httpGetJson(apiPort, "/snapshot");
    return Boolean(snapshot.body.latestByChannel && snapshot.body.latestByChannel[channel]);
  }, 10000, "Snapshot vide apres connexion");

  const localWsPayload = await new Promise((resolve, reject) => {
    const client = new WebSocket(`ws://127.0.0.1:${apiPort}/stream-test`);
    const timeout = setTimeout(() => {
      client.close();
      reject(new Error("Aucun message event recu du WS local"));
    }, 10000);

    client.on("message", (raw) => {
      const msg = JSON.parse(raw.toString());
      if (msg.type === "event") {
        clearTimeout(timeout);
        client.close();
        resolve(msg);
      }
    });

    client.on("error", (error) => {
      clearTimeout(timeout);
      reject(error);
    });
  });

  assert.equal(localWsPayload.type, "event");
  assert.equal(localWsPayload.payload.channel, channel);

  const events = await httpGetJson(apiPort, "/events?limit=5");
  assert.equal(events.statusCode, 200);
  assert.ok(Array.isArray(events.body.events));
  assert.ok(events.body.events.length > 0);

  const candles = await httpGetJson(apiPort, "/candles?channel=ticker.BTC-PERPETUAL.raw&limit=10");
  assert.equal(candles.statusCode, 503);
  assert.equal(candles.body.error, "timescale_unavailable");

  const analytics = await httpGetJson(apiPort, "/analytics?channel=ticker.BTC-PERPETUAL.raw&windowMs=10000");
  assert.equal(analytics.statusCode, 200);
  assert.equal(analytics.body.channel, "ticker.BTC-PERPETUAL.raw");
  assert.ok(analytics.body.eventsInWindow > 0);
  assert.ok(analytics.body.pricedEvents > 0);
  assert.ok(typeof analytics.body.lastPrice === "number");
  assert.ok(Array.isArray(analytics.body.topChannels));

  if (appLogs.includes("Erreur")) {
    assert.fail(`Logs applicatifs contiennent une erreur: ${appLogs}`);
  }
});
