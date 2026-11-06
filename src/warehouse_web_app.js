import express from 'express';
import path from 'node:path';
import {fileURLToPath} from 'node:url';

// Only public application code is served here. Employee data stays behind the
// warehouse API and is never placed in the service worker's shared cache.
export function mountWarehouseWebApp(app){
  const root=path.dirname(fileURLToPath(import.meta.url));
  app.get('/workspaces-update',(_req,res)=>res.set('Cache-Control','no-store').render('warehouse_update'));
  app.get('/workspaces',(_req,res)=>res.set('Cache-Control','no-store').render('warehouse_mobile'));
  app.get('/warehouse-approvals',(_req,res)=>res.set('Cache-Control','no-store').render('warehouse_approvals'));
  app.get('/warehouse-map-settings',(_req,res)=>res.set('Cache-Control','no-store').render('warehouse_map_settings'));
  app.get('/warehouse-connections',(_req,res)=>res.set('Cache-Control','no-store').render('warehouse_connections'));
  app.get('/warehouse-products',(_req,res)=>res.set('Cache-Control','no-store').render('warehouse_products'));
  app.get('/warehouse-route-invoices',(_req,res)=>res.set('Cache-Control','no-store').render('warehouse_route_invoices'));
  app.get('/warehouse-route-rules',(_req,res)=>res.set('Cache-Control','no-store').render('warehouse_route_rules'));
  app.get('/warehouse-planning',(_req,res)=>res.set('Cache-Control','no-store').render('warehouse_planning'));
  app.get('/warehouse-receiving',(_req,res)=>res.set('Cache-Control','no-store').render('warehouse_receiving'));
  app.get('/workspaces/manifest.webmanifest',(_req,res)=>res.type('application/manifest+json').json({id:'/workspaces',name:'D’ RAW Workspaces',short_name:'D’ RAW',start_url:'/workspaces',scope:'/',display:'standalone',background_color:'#fff4e8',theme_color:'#32115d',icons:[{src:'/workspaces-assets/icon.png',sizes:'216x216',type:'image/png',purpose:'any'},{src:'/workspaces-assets/icon.svg',sizes:'any',type:'image/svg+xml',purpose:'any'}]}));
  app.get('/workspaces-sw.js',(_req,res)=>res.set({'Service-Worker-Allowed':'/','Cache-Control':'no-cache'}).type('text/javascript').sendFile(path.join(root,'web','sw.js')));
  app.get('/workspaces-assets/zxing.js',(_req,res)=>res.type('text/javascript').sendFile(path.join(root,'..','node_modules','@zxing','browser','umd','zxing-browser.min.js')));
  app.use('/workspaces-assets',express.static(path.join(root,'web'),{maxAge:0}));
}
