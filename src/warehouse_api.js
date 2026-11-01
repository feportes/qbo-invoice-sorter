import express from 'express';
import {randomUUID} from 'node:crypto';
import {ensureWarehouseSchema} from './warehouse_schema.js';
import {authenticate,bootstrap,login,addUser,disableUser,safeUser,permit,problem,equal,audit,changePassword,updateUser} from './warehouse_auth.js';
import {createPurchaseOrder,recordReceipt,readPO,readReceipt,suggestPutaway,confirmPutaway,approveReceipt,frozenStockLocations,once} from './warehouse_receiving.js';
import {driverRoute,importRoute,saveDriverEvents} from './warehouse_driver.js';
import {createWarehouseSource} from './warehouse_source.js';

const run=fn=>(req,res,next)=>Promise.resolve().then(()=>fn(req,res)).catch(next);
const viewUser=u=>({id:u.id,username:u.username,displayName:u.displayName,role:u.role,teamId:u.teamId,functions:u.functions,mustChangePassword:u.mustChangePassword});
export function createWarehouseRouter({db,env=process.env,readVendors,readVendor,readPurchaseOrders,readPurchaseOrder,readInvoice,sourceFetch}){
  const r=express.Router(),sql=db.sqlite;ensureWarehouseSchema(sql);
  const source=createWarehouseSource({sql,env,readInvoice,...(sourceFetch?{fetchImpl:sourceFetch}:{})});
  const timer=setInterval(()=>source.kickAll(),30000);timer.unref();r.stopWarehouseSync=()=>clearInterval(timer);
  r.use((req,res,next)=>{
    res.set('Cache-Control','private, no-store');res.set('X-Content-Type-Options','nosniff');
    if(env.WAREHOUSE_ENABLED!=='true')return res.status(503).json({error:'The warehouse pilot has not been enabled.'});
    if(!['GET','HEAD'].includes(req.method)&&req.get('origin')){try{const origin=new URL(req.get('origin'));if(origin.host!==req.get('host'))return res.status(403).json({error:'Cross-origin save denied.'});}catch{return res.status(403).json({error:'Invalid request origin.'});}}
    next();
  });
  r.post('/bootstrap',run(async(req,res)=>res.status(201).json({user:await bootstrap(sql,env,req.body)})));
  r.post('/login',run(async(req,res)=>{const result=await login(sql,req.body,req.ip);res.cookie('__Host-wh_session',result.token,{secure:true,httpOnly:true,sameSite:'strict',path:'/',maxAge:14*86400000});res.json(result);}));

  // Server-to-server bridge only. Its key is never delivered to a handheld or a browser.
  r.use('/bridge',(req,res,next)=>{const secret=env.WAREHOUSE_BRIDGE_KEY,token=/^Bearer (.+)$/.exec(req.get('authorization')||'')?.[1];if(!secret||secret.length<32||!equal(secret,token||''))return res.status(403).json({error:'Bridge access denied.'});req.whUser={id:'warehouse-office-bridge',displayName:'Office bridge',role:'admin'};next();});
  r.post('/bridge/routes',run((req,res)=>res.json(importRoute(sql,req.whUser,req.body))));
  r.get('/bridge/events',run((req,res)=>{const after=Number(req.query.after||0);if(!Number.isSafeInteger(after)||after<0)problem('Review event cursor.');const rows=sql.prepare("SELECT e.*,u.display_name,u.team_id FROM wh_driver_events e JOIN wh_users u ON u.id=e.user_id WHERE e.sequence>? ORDER BY e.sequence LIMIT 200").all(after);res.json({events:rows.map(e=>({...e,payload:JSON.parse(e.payload)})),nextCursor:rows.at(-1)?.sequence||after});}));
  r.post('/bridge/events/ack',run((req,res)=>res.json(once(sql,req.whUser,req.body.requestId,req.body,()=>{if(!Array.isArray(req.body.eventIds)||!req.body.eventIds.length||req.body.eventIds.length>200||!String(req.body.officeReference||'').trim())problem('Review acknowledged events and office reference.');for(const id of req.body.eventIds){const e=sql.prepare('SELECT * FROM wh_driver_events WHERE id=?').get(id);if(!e)problem('Driver event not found.');sql.prepare("UPDATE wh_driver_events SET status='applied-to-office',reviewed_by=?,reviewed_at=? WHERE id=?").run(req.body.officeReference,new Date().toISOString(),id);}audit(sql,req.whUser,'driver-events-applied','bridge',req.body.requestId,{ids:req.body.eventIds,officeReference:req.body.officeReference});return {acknowledged:req.body.eventIds};}))));
  r.get('/bridge/stock',run((req,res)=>res.json(frozenStockLocations(db))));
  r.get('/bridge/media/:id',run((req,res)=>{const m=sql.prepare('SELECT * FROM wh_media WHERE id=?').get(req.params.id);if(!m)problem('Photo not found.',404);res.type(m.mime_type).send(m.bytes);}));

  r.use((req,res,next)=>{try{req.whUser=authenticate(sql,req);next();}catch(e){next(e);}});
  r.get('/session',run((req,res)=>res.json({user:viewUser(req.whUser),csrf:req.whUser.csrf,expiresAt:req.whUser.expiresAt})));
  r.post('/logout',run((req,res)=>{sql.prepare('DELETE FROM wh_sessions WHERE token_hash=?').run(req.whUser.sessionHash);res.clearCookie('__Host-wh_session',{secure:true,httpOnly:true,sameSite:'strict',path:'/'});res.json({signedOut:true});}));
  r.post('/password',run(async(req,res)=>res.json(await changePassword(sql,req.whUser,req.body))));
  r.use((req,res,next)=>{try{
    const feature=req.path.startsWith('/catalog')?'products':req.path.startsWith('/vendors')?'vendors':/^\/(qbo-purchase-orders|purchase-orders|receipts|stock)(\/|$)/.test(req.path)?'receiving':req.path.startsWith('/audit')?'reports':null;
    if(feature)permit(req.whUser,['admin','office','warehouse'],feature);next();
  }catch(e){next(e);}});
  r.get('/users',run((req,res)=>{permit(req.whUser,['admin']);res.json({users:sql.prepare('SELECT * FROM wh_users ORDER BY display_name').all().map(safeUser)});}));
  r.get('/team-identities',run(async(req,res)=>{permit(req.whUser,['admin']);await source.refresh(req.whUser);res.json({team:sql.prepare('SELECT id,name FROM wh_source_team ORDER BY name').all()});}));
  r.post('/users',run(async(req,res)=>{permit(req.whUser,['admin']);res.status(201).json({user:await addUser(sql,req.whUser,req.body)});}));
  r.post('/users/:id/disable',run((req,res)=>res.json(disableUser(sql,req.whUser,req.params.id))));
  r.post('/users/:id',run(async(req,res)=>res.json(await updateUser(sql,req.whUser,req.params.id,req.body))));
  r.get('/driver-catalog',run(async(req,res)=>{permit(req.whUser,['admin','office','driver'],'deliveries');res.json(await source.catalog(req.whUser,String(req.query.version||''),req.query.force==='true'));}));
  r.get('/catalog',run((req,res)=>{permit(req.whUser,['admin','office','warehouse']);res.json({products:sql.prepare('SELECT id,qbo_item_id,qbo_sku,name,unit_type FROM skus WHERE active=1 ORDER BY name').all(),palletConfigs:sql.prepare('SELECT id,sku_id,name,units_per_pallet,is_default FROM pallet_configs ORDER BY sku_id,is_default DESC').all()});}));
  r.get('/vendors',run(async(req,res)=>{permit(req.whUser,['admin','office','warehouse']);if(!readVendors)problem('Vendor lookup is not configured.',503);res.json({vendors:await readVendors()});}));
  r.get('/qbo-purchase-orders',run(async(req,res)=>{permit(req.whUser,['admin','office','warehouse']);if(!readPurchaseOrders)problem('QuickBooks PO lookup is not configured.',503);res.json({purchaseOrders:await readPurchaseOrders(String(req.query.vendorId||''))});}));
  r.post('/purchase-orders/import',run(async(req,res)=>{
    permit(req.whUser,['admin','office']);if(!readPurchaseOrder)problem('QuickBooks PO lookup is not configured.',503);
    const source=await readPurchaseOrder(String(req.body.qboPOId||''));if(!source?.Id||!source.VendorRef?.value)problem('QuickBooks did not return this PO.',502);
    const lines=(source.Line||[]).filter(l=>l.DetailType==='ItemBasedExpenseLineDetail').map(l=>{const q=l.ItemBasedExpenseLineDetail,sku=sql.prepare('SELECT * FROM skus WHERE qbo_item_id=? AND active=1').get(String(q.ItemRef?.value||''));if(!sku)problem('Sync and map each PO product in Product Settings first.');return {id:String(l.Id),skuId:sku.id,orderedQty:Number(q.Qty),area:req.body.areas?.[sku.id]};});
    res.json(createPurchaseOrder(db,req.whUser,{requestId:req.body.requestId,vendorId:String(source.VendorRef.value),vendorName:source.VendorRef.name||'QuickBooks vendor',qboPoId:String(source.Id),poNumber:source.DocNumber||String(source.Id),expectedDate:source.DueDate||null,lines}));
  }));
  r.get('/purchase-orders',run((req,res)=>{permit(req.whUser,['admin','office','warehouse']);res.json({purchaseOrders:sql.prepare('SELECT * FROM wh_purchase_orders ORDER BY updated_at DESC LIMIT 100').all().map(p=>({...p,lines:JSON.parse(p.lines)}))});}));
  r.post('/purchase-orders',run(async(req,res)=>{permit(req.whUser,['admin','office']);if(!readVendor)problem('Vendor lookup is not configured.',503);const vendor=await readVendor(String(req.body.vendorId||''));if(!vendor?.Id||vendor.Active===false)problem('Choose an active QuickBooks vendor.');res.status(201).json(createPurchaseOrder(db,req.whUser,{...req.body,vendorId:String(vendor.Id),vendorName:vendor.DisplayName,qboPoId:null}));}));
  r.get('/receipts',run((req,res)=>{permit(req.whUser,['admin','office','warehouse']);res.json({receipts:sql.prepare('SELECT * FROM wh_receipts ORDER BY received_at DESC LIMIT 100').all().map(x=>({...x,lines:JSON.parse(x.lines)}))});}));
  r.post('/receipts',run((req,res)=>res.status(201).json(recordReceipt(db,req.whUser,req.body))));
  r.get('/receipts/:id',run((req,res)=>{permit(req.whUser,['admin','office','warehouse']);res.json({receipt:readReceipt(sql,req.params.id)});}));
  r.post('/receipts/:id/suggestions',run((req,res)=>{permit(req.whUser,['admin','office','warehouse']);res.json(suggestPutaway(db,req.params.id,req.body.options||{}));}));
  r.post('/receipts/:id/putaway',run((req,res)=>res.json(confirmPutaway(db,req.whUser,{...req.body,receiptId:req.params.id}))));
  r.post('/receipts/:id/approve',run((req,res)=>res.json(approveReceipt(db,req.whUser,{...req.body,receiptId:req.params.id}))));
  r.get('/stock',run((req,res)=>{permit(req.whUser,['admin','office','warehouse']);res.json(frozenStockLocations(db));}));
  r.get('/routes',run(async(req,res)=>{
    permit(req.whUser,['admin','office','driver'],'deliveries');const values=[],where=[];
    await source.refresh(req.whUser);source.kickAll();
    if(!['admin','manager','office'].includes(req.whUser.role)){
      where.push("(driver_id=? OR json_extract(snapshot,'$.helperId')=? OR EXISTS (SELECT 1 FROM json_each(json_extract(snapshot,'$.helperIds')) WHERE value=?))");
      values.push(...Array(3).fill(req.whUser.teamId||'no-assignment'));
      if(sql.prepare('SELECT user_id FROM wh_source_access_cache WHERE user_id=?').get(req.whUser.id)){where.push('EXISTS (SELECT 1 FROM wh_source_access a WHERE a.route_id=wh_routes.id AND a.user_id=?)');values.push(req.whUser.id);}
    }
    if(req.query.date){if(!/^\d{4}-\d{2}-\d{2}$/.test(String(req.query.date)))problem('Choose a route date.');where.push('route_date=?');values.push(req.query.date);}
    if(req.query.includeCompleted!=='true')where.push("status IN ('loaded','delivering')");
    const rows=sql.prepare('SELECT * FROM wh_routes'+(where.length?' WHERE '+where.join(' AND '):'')+' ORDER BY route_date DESC,id LIMIT 100').all(...values);
    const routes=rows.map(r=>{const pending=sql.prepare("SELECT COUNT(*) n FROM wh_driver_events WHERE route_id=? AND status='pending-office'").get(r.id).n,conflicts=sql.prepare("SELECT COUNT(*) n FROM wh_source_jobs WHERE route_id=? AND status='conflict'").get(r.id).n;return {...r,snapshot:JSON.parse(r.snapshot),officeSyncStatus:conflicts?'Office sync needs review':pending?'Saved; office handoff pending':'Office connected',officePending:pending,officeConflicts:conflicts};});
    const assignedRouteIds=source.configured()?sql.prepare('SELECT route_id FROM wh_source_access WHERE user_id=?').all(req.whUser.id).map(r=>r.route_id):undefined;
    res.json({routes,assignedRouteIds,officePending:routes.reduce((n,r)=>n+r.officePending,0),officeConflicts:routes.reduce((n,r)=>n+r.officeConflicts,0),preparedAt:new Date().toISOString(),officeConnection:source.configured()?'automatic':'not-configured'});
  }));
  r.get('/routes/:id',run((req,res)=>res.json({route:driverRoute(sql,req.whUser,req.params.id)})));
  r.get('/routes/:id/activity',run((req,res)=>{
    const route=driverRoute(sql,req.whUser,req.params.id);
    const events=sql.prepare('SELECT e.*,u.display_name FROM wh_driver_events e JOIN wh_users u ON u.id=e.user_id WHERE e.route_id=? ORDER BY e.sequence').all(route.id).map(e=>({id:e.id,kind:e.kind,payload:JSON.parse(e.payload),recordedAt:e.device_at,receivedAt:e.received_at,status:e.status,displayName:e.display_name}));
    const syncIssues=sql.prepare("SELECT event_id,status,error,updated_at FROM wh_source_jobs WHERE route_id=? AND status<>'applied' ORDER BY updated_at").all(route.id);
    res.json({route,events,syncIssues,officeConnection:source.configured()?'automatic':'not-configured'});
  }));
  r.post('/routes/:id/events',run((req,res)=>{const result=saveDriverEvents(sql,req.whUser,{...req.body,routeId:req.params.id});source.kick(req.params.id);res.json(result);}));
  r.post('/media',express.raw({type:['image/jpeg','image/png','image/webp'],limit:'8mb'}),run((req,res)=>{
    const bytes=req.body;if(!Buffer.isBuffer(bytes)||bytes.length<12)problem('Choose a JPG, PNG or WebP photo.');
    const type=bytes[0]===255&&bytes[1]===216&&bytes[2]===255?'image/jpeg':bytes.subarray(0,8).equals(Buffer.from([137,80,78,71,13,10,26,10]))?'image/png':bytes.toString('ascii',0,4)==='RIFF'&&bytes.toString('ascii',8,12)==='WEBP'?'image/webp':null;
    if(!type)problem('Choose a JPG, PNG or WebP photo.');const routeId=String(req.query.routeId||''),receiptId=String(req.query.receiptId||''),stopId=String(req.query.stopId||''),id=String(req.query.id||'');
    if(!/^[A-Za-z0-9_-]{8,100}$/.test(id)||(!!routeId===!!receiptId))problem('Choose one route or receipt for this photo.');
    if(routeId){const route=driverRoute(sql,req.whUser,routeId);if(stopId&&!route.snapshot.stops.some(s=>s.id===stopId))problem('Stop is not on this route.');}else{permit(req.whUser,['admin','office','warehouse']);readReceipt(sql,receiptId);}
    const existing=sql.prepare('SELECT * FROM wh_media WHERE id=?').get(id);if(existing){if(existing.user_id!==req.whUser.id||existing.route_id!==(routeId||null)||existing.receipt_id!==(receiptId||null)||existing.stop_id!==(stopId||null)||!Buffer.from(existing.bytes).equals(bytes))problem('Photo identifier was reused.',409);return res.json({id,duplicate:true});}
    const deviceAt=String(req.query.recordedAt||new Date().toISOString());if(!Number.isFinite(Date.parse(deviceAt)))problem('Review photo timestamp.');
    sql.transaction(()=>{sql.prepare('INSERT INTO wh_media VALUES (?,?,?,?,?,?,?,?,?)').run(id,req.whUser.id,routeId||null,receiptId||null,stopId||null,type,bytes,deviceAt,new Date().toISOString());audit(sql,req.whUser,'photo-saved',routeId?'route':'receipt',routeId||receiptId,{mediaId:id,stopId,deviceAt});})();res.status(201).json({id});
  }));
  r.get('/media/:id',run((req,res)=>{const media=sql.prepare('SELECT * FROM wh_media WHERE id=?').get(req.params.id);if(!media)problem('Photo not found.',404);if(media.route_id)driverRoute(sql,req.whUser,media.route_id);else permit(req.whUser,['admin','office','warehouse']);res.set('Content-Security-Policy',"default-src 'none'").type(media.mime_type).send(media.bytes);}));
  r.get('/audit',run((req,res)=>{permit(req.whUser,['admin','office']);const id=String(req.query.entityId||'');res.json({records:sql.prepare('SELECT * FROM wh_audit'+(id?' WHERE entity_id=?':'')+' ORDER BY at DESC LIMIT 200').all(...(id?[id]:[])).map(x=>({...x,detail:JSON.parse(x.detail)}))});}));
  r.use((error,req,res,next)=>{if(res.headersSent)return next(error);res.status(error.status||500).json({error:error.status?error.message:'The save could not be confirmed. Keep the same save identifier when retrying.'});});
  return r;
}
