const CACHE='draw-workspaces-v3';
const FILES=['/workspaces','/workspaces-assets/app.js','/workspaces-assets/app.css','/workspaces-assets/camera.js','/workspaces-assets/zxing.js','/workspaces-assets/icon.svg','/workspaces-assets/icon.png','/workspaces-assets/driver-state.js'];
self.addEventListener('install',event=>event.waitUntil(caches.open(CACHE).then(cache=>cache.addAll(FILES))));
self.addEventListener('activate',event=>event.waitUntil(Promise.all([caches.keys().then(keys=>Promise.all(keys.filter(k=>k.startsWith('draw-workspaces-')&&k!==CACHE).map(k=>caches.delete(k)))),self.clients.claim()])));
self.addEventListener('fetch',event=>{
 const url=new URL(event.request.url);
 if(event.request.method!=='GET'||url.origin!==location.origin)return;
 // Never cache API responses, private media, or authenticated warehouse pages.
 if(!FILES.includes(url.pathname))return;
 // Installed application code opens immediately, even with a weak connection.
 // A complete new worker install replaces the shell on the next app launch.
 event.respondWith(caches.open(CACHE).then(async cache=>(await cache.match(url.pathname))||fetch(event.request)));
});
