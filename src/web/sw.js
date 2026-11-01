const CACHE='draw-workspaces-v1';
const FILES=['/workspaces','/workspaces-assets/app.js','/workspaces-assets/app.css','/workspaces-assets/camera.js','/workspaces-assets/zxing.js','/workspaces-assets/icon.svg','/workspaces-assets/driver-state.js'];
self.addEventListener('install',event=>event.waitUntil(caches.open(CACHE).then(cache=>cache.addAll(FILES))));
self.addEventListener('activate',event=>event.waitUntil(Promise.all([caches.keys().then(keys=>Promise.all(keys.filter(k=>k.startsWith('draw-workspaces-')&&k!==CACHE).map(k=>caches.delete(k)))),self.clients.claim()])));
self.addEventListener('fetch',event=>{
 const url=new URL(event.request.url);
 if(event.request.method!=='GET'||url.origin!==location.origin)return;
 // Never cache API responses, private media, or authenticated warehouse pages.
 if(!FILES.includes(url.pathname))return;
 event.respondWith(fetch(event.request).then(response=>{if(response.ok){const copy=response.clone();event.waitUntil(caches.open(CACHE).then(c=>c.put(url.pathname,copy)));}return response;}).catch(()=>caches.match(url.pathname)));
});
