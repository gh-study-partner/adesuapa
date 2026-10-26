/* Adesua Pa service worker: makes the app installable and lets it open when the network is slow or off.
   It NEVER touches /api (accounts, quizzes, payments always go straight to the server) or other sites. */
const V = 'adesuapa-v1';
const SHELL = ['/', '/manifest.json', '/icons/icon-192.png', '/icons/icon-512.png'];

self.addEventListener('install', (e) => {
  e.waitUntil(caches.open(V).then((c) => c.addAll(SHELL)).catch(() => {}).then(() => self.skipWaiting()));
});
self.addEventListener('activate', (e) => {
  e.waitUntil(caches.keys().then((ks) => Promise.all(ks.filter((k) => k !== V).map((k) => caches.delete(k)))).then(() => self.clients.claim()));
});
self.addEventListener('fetch', (e) => {
  const req = e.request;
  if (req.method !== 'GET') return;
  const url = new URL(req.url);
  if (url.origin !== self.location.origin) return;       // CDN scripts, Paystack, YouTube… handled by the browser
  if (url.pathname.startsWith('/api')) return;            // live data is never cached
  if (url.pathname === '/sw.js') return;
  if (req.mode === 'navigate') {                          // always try the network first so students get the newest version
    e.respondWith(
      fetch(req).then((res) => {
        if (res && res.ok && url.pathname === '/') { const copy = res.clone(); caches.open(V).then((c) => c.put('/', copy)); }
        return res;
      }).catch(() => caches.match('/').then((r) => r || new Response('You are offline. Please reconnect and try again.', { status: 503, headers: { 'content-type': 'text/plain' } })))
    );
    return;
  }
  if (url.pathname.startsWith('/icons/') || url.pathname === '/manifest.json') {
    e.respondWith(caches.match(req).then((r) => r || fetch(req).then((res) => { const copy = res.clone(); caches.open(V).then((c) => c.put(req, copy)); return res; })));
  }
});
