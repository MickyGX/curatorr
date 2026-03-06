const CACHE_NAME = 'launcharr-static-v36';
const STATIC_ASSETS = [
  '/manifest.webmanifest',
  '/styles.css',
  '/icons/launcharr-icon.png',
  '/icons/app.svg',
  '/icons/app-arr.svg',
  '/icons/prowlarr.png',
  '/icons/dashboard.svg',
  '/icons/overview.svg',
  '/icons/launch.svg',
  '/icons/settings.svg',
  '/icons/logout.svg',
  '/icons/collapse.svg',
  '/icons/expand.svg',
  '/icons/all-type.svg',
  '/icons/window.svg',
  '/icons/status.svg'
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches
      .open(CACHE_NAME)
      .then((cache) => cache.addAll(STATIC_ASSETS))
      .then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key)))
    ).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (event) => {
  const { request } = event;

  if (request.method !== 'GET') return;
  if (request.mode === 'navigate') return;

  const url = new URL(request.url);
  if (url.origin !== self.location.origin) return;

  const isStaticAsset =
    request.destination === 'style' ||
    request.destination === 'image' ||
    request.destination === 'font' ||
    url.pathname.endsWith('.webmanifest');

  if (!isStaticAsset) return;

  event.respondWith(
    (async () => {
      const cache = await caches.open(CACHE_NAME);
      try {
        const fresh = await fetch(request);
        cache.put(request, fresh.clone());
        return fresh;
      } catch (err) {
        const cached = await cache.match(request);
        if (cached) return cached;
        throw err;
      }
    })()
  );
});
