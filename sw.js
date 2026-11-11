const CACHE_NAME = 'rooms-shell-v2';
const APP_SHELL = ['/', '/manifest.json'];

self.addEventListener('install', event => {
    event.waitUntil(
        caches.open(CACHE_NAME)
            .then(cache => cache.addAll(APP_SHELL))
            .then(() => self.skipWaiting())
    );
});

self.addEventListener('activate', event => {
    event.waitUntil(
        caches.keys()
            .then(keys => Promise.all(
                keys.filter(key => key.startsWith('rooms-shell-') && key !== CACHE_NAME)
                    .map(key => caches.delete(key))
            ))
            .then(() => self.clients.claim())
    );
});

self.addEventListener('fetch', event => {
    const request = event.request;
    const url = new URL(request.url);

    if (request.method !== 'GET' || url.origin !== self.location.origin) return;
    if (url.pathname !== '/' && url.pathname !== '/manifest.json') return;

    const responsePromise = fetch(request)
        .then(response => {
            if (response.ok) {
                event.waitUntil(
                    caches.open(CACHE_NAME)
                        .then(cache => cache.put(request, response.clone()))
                );
            }
            return response;
        })
        .catch(async () => {
                const cached = await caches.match(request, { ignoreSearch: true });
                if (cached) return cached;
                if (request.mode === 'navigate') {
                    const appShell = await caches.match('/');
                    if (appShell) return appShell;
                }
                return new Response('İçerik çevrimdışı kullanılamıyor.', {
                    status: 503,
                    headers: { 'Content-Type': 'text/plain; charset=utf-8' }
                });
        });

    event.respondWith(responsePromise);
});
