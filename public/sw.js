const CACHE_VERSION = "icare-v3-production";
const APP_SHELL = [
  "/",
  "/manifest.webmanifest",
  "/sw.js",
  "/assets/app.js",
  "/assets/styles.css",
  "/assets/offline.html",
  "/assets/icons/icon.svg",
  "/assets/icons/icon-192.svg",
  "/assets/icons/icon-512.svg"
];

function isSameOrigin(url) {
  return url.origin === self.location.origin;
}

function isApiRequest(pathname) {
  return pathname.startsWith("/health")
    || pathname.startsWith("/snapshot")
    || pathname.startsWith("/events")
    || pathname.startsWith("/analytics")
    || pathname.startsWith("/candles")
    || pathname.startsWith("/signal")
    || pathname.startsWith("/features")
    || pathname.startsWith("/labels")
    || pathname.startsWith("/series/schema")
    || pathname.startsWith("/backtest/rolling")
    || pathname.startsWith("/quality/regime")
    || pathname.startsWith("/notifications")
    || pathname.startsWith("/push/");
}

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(CACHE_VERSION)
      .then((cache) => cache.addAll(APP_SHELL))
      .then(() => self.skipWaiting())
  );
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys()
      .then((names) => Promise.all(names
        .filter((name) => name !== CACHE_VERSION)
        .map((name) => caches.delete(name))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener("fetch", (event) => {
  if (event.request.method !== "GET") {
    return;
  }

  const url = new URL(event.request.url);
  if (!isSameOrigin(url)) {
    return;
  }

  if (event.request.mode === "navigate") {
    event.respondWith(
      fetch(event.request)
        .then((response) => {
          const copy = response.clone();
          caches.open(CACHE_VERSION).then((cache) => cache.put("/", copy));
          return response;
        })
        .catch(async () => {
          const cache = await caches.open(CACHE_VERSION);
          const fallback = await cache.match("/");
          return fallback || cache.match("/assets/offline.html");
        })
    );
    return;
  }

  if (isApiRequest(url.pathname)) {
    event.respondWith(
      fetch(event.request)
        .then((response) => {
          const copy = response.clone();
          caches.open(CACHE_VERSION).then((cache) => cache.put(event.request, copy));
          return response;
        })
        .catch(async () => {
          const cache = await caches.open(CACHE_VERSION);
          const cached = await cache.match(event.request);
          if (cached) {
            return cached;
          }

          return new Response(
            JSON.stringify({
              error: "offline_unavailable",
              message: "Aucune donnée en cache disponible hors ligne."
            }),
            {
              status: 503,
              headers: {
                "Content-Type": "application/json; charset=utf-8"
              }
            }
          );
        })
    );
    return;
  }

  event.respondWith(
    caches.match(event.request).then((cached) => {
      if (cached) {
        return cached;
      }

      return fetch(event.request).then((response) => {
        const copy = response.clone();
        caches.open(CACHE_VERSION).then((cache) => cache.put(event.request, copy));
        return response;
      });
    })
  );
});

self.addEventListener("push", (event) => {
  let payload = {};
  try {
    payload = event.data ? event.data.json() : {};
  } catch (_error) {
    payload = {};
  }

  const title = payload.title || "Alerte iCare";
  const body = payload.message || "Nouvelle alerte";
  const level = payload.level || "info";

  event.waitUntil(
    self.registration.showNotification(title, {
      body,
      tag: `icare-${level}`,
      data: {
        url: "/"
      }
    })
  );
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const targetUrl = (event.notification.data && event.notification.data.url) || "/";

  event.waitUntil(
    self.clients.matchAll({ type: "window", includeUncontrolled: true }).then((clients) => {
      for (const client of clients) {
        if (client.url.includes(targetUrl) && "focus" in client) {
          return client.focus();
        }
      }

      if (self.clients.openWindow) {
        return self.clients.openWindow(targetUrl);
      }

      return null;
    })
  );
});
