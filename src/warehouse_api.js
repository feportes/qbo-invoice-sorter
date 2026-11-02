import {invoiceDate} from './warehouse_planning_invoices.js';
import {sessionPolicy,saveSessionPolicy} from './warehouse_session_policy.js';
import {ensureProductRequests,createProductRequest,productRequests,reviewProductRequest} from './warehouse_product_requests.js';
import {decoratePalletLayout} from './warehouse_pallet_visuals.js';
import {saveBinding,importTelemetry,temperatures,motiveEvidence,fetchMotive,ensureTelemetry,telemetrySources,matchMotiveVehicle} from './warehouse_telemetry.js';
import {importCameraEvents,saveCameraMedia,cameraEvents,cameraMedia} from './warehouse_motive_camera.js';
import {ensureInstructionUpdateSchema,instructionCustomers,createInstructionUpdate,uploadInstructionUpdateMedia,submitInstructionUpdate,readInstructionUpdates,instructionUpdateMedia,reviewInstructionUpdate} from './warehouse_instruction_updates.js';
import {ensureGuideSchema,saveReferenceGuide,saveReferenceMedia,referenceMedia,guideMedia,attachReferenceGuides} from './warehouse_guides.js';
import express from 'express';
import {randomUUID} from 'node:crypto';
import {ensureWarehouseSchema} from './warehouse_schema.js';
import {authenticate,bootstrap,login,addUser,disableUser,safeUser,permit,problem,equal,audit,changePassword,updateUser} from './warehouse_auth.js';
import {createPurchaseOrder,recordReceipt,readPO,readReceipt,suggestPutaway,confirmPutaway,approveReceipt,frozenStockLocations,once} from './warehouse_receiving.js';
import {driverRoute,importRoute,saveDriverEvents} from './warehouse_driver.js';
import {createWarehouseSource} from './warehouse_source.js';
import {stockCatalog,registerStockPallet,moveStockPallet,editStockPallet,copyStockPallet} from './warehouse_stock.js';

const run=fn=>(req,res,next)=>Promise.resolve().then(()=>fn(req,res)).catch(next);

function serveInstructionMedia(req,res,m){const total=m.bytes.length;res.type(m.mime_type).set('Accept-Ranges','bytes');if(req.get('range')){const match=/^bytes=(\d*)-(\d*)$/.exec(req.get('range'));if(!match||!match[1]&&!match[2])return res.status(416).set('Content-Range','bytes */'+total).end();let start=match[1]?Number(match[1]):Math.max(0,total-Number(match[2])),end=match[1]&&match[2]?Math.min(total-1,Number(match[2])):total-1;if(!Number.isSafeInteger(start)||!Number.isSafeInteger(end)||start<0||start>=total||end<start)return res.status(416).set('Content-Range','bytes */'+total).end();res.status(206).set('Content-Range','bytes '+start+'-'+end+'/'+total).send(m.bytes.subarray(start,end+1));}else res.send(m.bytes);}
const viewUser=u=>({id:u.id,username:u.username,displayName:u.displayName,role:u.role,teamId:u.teamId,functions:u.functions,mustChangePassword:u.mustChangePassword});
export function createWarehouseRouter({db,env=process.env,readVendors,readVendor,readPurchaseOrders,readPurchaseOrder,readInvoice,readPlanningInvoices,sourceFetch,readContainerLayout}){
  const r=express.Router(),sql=db.sqlite;ensureWarehouseSchema(sql);ensureGuideSchema(sql);ensureInstructionUpdateSchema(sql);ensureProductRequests(sql);
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
  r.get('/vehicles',run((req,res)=>{permit(req.whUser,['admin','office','driver','warehouse'],req.whUser.functions?.includes('deliveries')?'deliveries':'warehouse');ensureTelemetry(sql);res.json({vehicles:sql.prepare("SELECT target_id,settings FROM wh_telemetry_bindings WHERE provider='motive' ORDER BY target_id").all().map(b=>({id:b.target_id,name:JSON.parse(b.settings).targetName||b.target_id}))});}));
  r.get('/temperatures',run((req,res)=>res.json(temperatures(sql,req.whUser))));
  r.get('/connections/options',run(async(req,res)=>{permit(req.whUser,['admin']);const packet=await source.request('/api/mobile-warehouse',req.whUser);const vehicles=(packet.vehicles||[]).map(v=>({id:v.id,name:v.name}));ensureTelemetry(sql);const sources=telemetrySources(sql,req.whUser).map(s=>{const binding=sql.prepare('SELECT target_id FROM wh_telemetry_bindings WHERE provider=? AND source_id=?').get(s.provider,s.id),target=vehicles.find(v=>v.id===binding?.target_id);return {...s,sourceName:s.name,targetId:binding?.target_id||'',name:target?.name||s.name};});res.json({sources,vehicles,locations:[{id:'WALKIN',name:'Walk-in freezer'},...db.listContainers().map(n=>({id:'CONTAINER-'+n,name:'Container '+n}))]});}));
  r.post('/connections/sources',run(async(req,res)=>{const sources=telemetrySources(sql,req.whUser,req.body);let matched=0;if(req.body.provider==='motive'){const packet=await source.request('/api/mobile-warehouse',req.whUser);for(const entry of sources.filter(s=>s.provider==='motive')){const target=matchMotiveVehicle(entry.name,packet.vehicles||[]);if(target){const current=sql.prepare('SELECT target_id FROM wh_telemetry_bindings WHERE provider=? AND source_id=?').get('motive',entry.id);if(!current||current.target_id===target.id){saveBinding(sql,req.whUser,{provider:'motive',sourceId:entry.id,targetId:target.id,targetName:target.name});}matched++;}}}res.json({sources,matched});}));
  r.post('/container-layout/settings',run((req,res)=>{permit(req.whUser,['admin','office'],'warehouse');res.json(once(sql,req.whUser,req.body.requestId,req.body,()=>{const n=Number(req.body.containerNo);if(!db.listContainers().includes(n))problem('Choose a current container.');const before=db.getContainerDepths(n);if(before.mode!==req.body.expectedMode||before.flip!==req.body.expectedFlip)problem('The container layout changed. Refresh before saving.',409);try{db.saveContainerLayouts([{containerNo:n,mode:req.body.mode,flip:req.body.flip}]);}catch(error){problem(error.message,409);}const after=db.getContainerDepths(n);audit(sql,req.whUser,'container-layout-updated','container',String(n),{before,after,palletSize:'40 × 48 inches',stockMoved:false});return {layout:after,stockMoved:false};}));}));
  r.get('/connections/camera-events',run((req,res)=>res.json(cameraEvents(sql,req.whUser,{date:req.query.date}))));
  r.post('/connections/camera-events',run((req,res)=>res.json(importCameraEvents(sql,req.whUser,req.body))));
  r.post('/connections/camera-events/:id/:position',express.raw({type:['image/jpeg','video/mp4'],limit:'35mb'}),run((req,res)=>res.json(saveCameraMedia(sql,req.whUser,req.params.id,req.params.position,req.body,req.get('content-type')?.split(';')[0]))));
  r.get('/connections/camera-events/:id/:position',run((req,res)=>serveInstructionMedia(req,res,cameraMedia(sql,req.whUser,req.params.id,req.params.position,req.query.type==='video'?'video/mp4':'image/jpeg'))));
  r.get('/connections',run((req,res)=>{permit(req.whUser,['admin','office'],'reports');ensureTelemetry(sql);res.json({motiveAutomaticConfigured:!!env.MOTIVE_API_KEY,temperatureAutomaticConfigured:false,bindings:sql.prepare('SELECT * FROM wh_telemetry_bindings ORDER BY provider,target_id').all().map(b=>({...b,settings:JSON.parse(b.settings)})),records:sql.prepare('SELECT provider,COUNT(*) count,MAX(fetched_at) lastFetchedAt FROM wh_telemetry_records GROUP BY provider').all()});}));
  r.post('/connections/bindings',run(async(req,res)=>{permit(req.whUser,['admin']);let input=req.body;if(input.provider==='motive'){const packet=await source.request('/api/mobile-warehouse',req.whUser),target=(packet.vehicles||[]).find(v=>v.id===input.targetId);if(!target)problem('Choose a current app vehicle.');input={...input,targetName:target.name};}res.json(saveBinding(sql,req.whUser,input));}));
  r.post('/connections/import',run((req,res)=>res.json(importTelemetry(sql,req.whUser,req.body))));
  r.post('/connections/motive-refresh',run(async(req,res)=>res.json(await fetchMotive(sql,req.whUser,env,req.body))));
  r.get('/routes/:id/motive',run((req,res)=>res.json(motiveEvidence(sql,req.whUser,driverRoute(sql,req.whUser,req.params.id)))));
  r.get('/session-policy',run((req,res)=>res.json(sessionPolicy(sql))));
  r.post('/session-policy',run((req,res)=>res.json(saveSessionPolicy(sql,req.whUser,req.body))));
  r.get('/login-directory',run((req,res)=>{permit(req.whUser,['admin','office','warehouse','driver']);res.json({users:sql.prepare('SELECT username,display_name AS displayName FROM wh_users WHERE enabled=1 ORDER BY display_name').all()});}));
  r.get('/products',run(async(req,res)=>{permit(req.whUser,['admin','office','warehouse'],'products');res.json(await source.request('/api/mobile-products',req.whUser));}));
  r.get('/product-requests',run((req,res)=>res.json({requests:productRequests(sql,req.whUser)})));
  r.post('/product-requests',run((req,res)=>res.json(createProductRequest(sql,req.whUser,req.body,driverRoute))));
  r.post('/product-requests/:id/review',run(async(req,res)=>res.json(await reviewProductRequest(sql,req.whUser,req.params.id,req.body,source))));
  r.post('/planning/invoices',run(async(req,res)=>{permit(req.whUser,['admin','office'],'planning');const date=invoiceDate(req.body.date);if(!readPlanningInvoices)problem('Invoice lookup is not configured.',503);res.json(await readPlanningInvoices(date,await source.request('/api/mobile-planning',req.whUser)));}));
  r.get('/planning',run(async(req,res)=>{permit(req.whUser,['admin','office'],'planning');res.json(await source.request('/api/mobile-planning',req.whUser));}));
  r.post('/planning',run(async(req,res)=>{permit(req.whUser,['admin','office'],'planning');res.json(await source.request('/api/mobile-planning',req.whUser,req.body));}));
  r.get('/receiving-lookups',run(async(req,res)=>{permit(req.whUser,['admin','office','warehouse'],'receiving');if(!readVendors)problem('Vendor lookup is not configured.',503);res.json({vendors:await readVendors(),products:sql.prepare('SELECT id,qbo_sku,name,unit_type FROM skus WHERE active=1 ORDER BY name').all(),palletConfigs:sql.prepare('SELECT * FROM pallet_configs').all()});}));
  r.post('/receipts/direct',run(async(req,res)=>{permit(req.whUser,['admin','office','warehouse'],'receiving');if(!readVendor)problem('Vendor lookup is not configured.',503);const vendor=await readVendor(String(req.body.vendorId||''));if(!vendor?.Id||vendor.Active===false)problem('Choose an active QuickBooks vendor.');res.json(once(sql,req.whUser,req.body.requestId,req.body,()=>{const lines=(req.body.lines||[]).map((line,index)=>({...line,id:'DIRECT-'+index,orderedQty:line.receivedQty})),po=createPurchaseOrder(db,req.whUser,{requestId:req.body.requestId+'-po',vendorId:String(vendor.Id),vendorName:vendor.DisplayName,poNumber:String(req.body.poNumber||'').trim()||'DIRECT-'+req.body.requestId.slice(0,12),lines},true).purchaseOrder;return recordReceipt(db,req.whUser,{requestId:req.body.requestId+'-receipt',poId:po.id,expectedPORevision:po.revision,mode:req.body.mode,notes:req.body.notes,lines:lines.map(l=>({...l,poLineId:l.id}))});}));}));
  r.post('/products',run(async(req,res)=>{permit(req.whUser,['admin','office','warehouse'],'products');res.json(await source.request('/api/mobile-products',req.whUser,req.body));}));
  r.post('/logout',run((req,res)=>{sql.prepare('DELETE FROM wh_sessions WHERE token_hash=?').run(req.whUser.sessionHash);res.clearCookie('__Host-wh_session',{secure:true,httpOnly:true,sameSite:'strict',path:'/'});res.json({signedOut:true});}));
  r.post('/password',run(async(req,res)=>res.json(await changePassword(sql,req.whUser,req.body))));
  r.use((req,res,next)=>{try{
    const feature=/^\/stock\/(catalog|pallets)(\/|$)/.test(req.path)?'warehouse':req.path.startsWith('/catalog')?'products':req.path.startsWith('/vendors')?'vendors':/^\/(qbo-purchase-orders|purchase-orders|receipts|stock)(\/|$)/.test(req.path)?'receiving':req.path.startsWith('/audit')?'reports':null;
    if(feature)permit(req.whUser,['admin','office','warehouse'],feature);next();
  }catch(e){next(e);}});
  r.get('/instruction-customers',run(async(req,res)=>{permit(req.whUser,['admin','office'],'deliveries');try{const choices=await source.request('/api/mobile-guide-customers',req.whUser);if(Array.isArray(choices.customers)&&choices.customers.length<=2000)sql.transaction(()=>{const put=sql.prepare('INSERT OR REPLACE INTO wh_instruction_customer_choices VALUES(?,?,?,?)');for(const c of choices.customers)if(typeof c.customerId==='string'&&c.customerId.length<=200&&typeof c.referenceId==='string'&&c.referenceId.length<=100&&typeof c.name==='string'&&c.name.length<=300)put.run(c.customerId,c.referenceId,c.name,String(c.address||'').slice(0,500));})();}catch{}res.json({customers:instructionCustomers(sql)});}));
  r.get('/instruction-updates',run((req,res)=>res.json({updates:readInstructionUpdates(sql,req.whUser)})));
  r.post('/instruction-updates',run((req,res)=>res.json(createInstructionUpdate(sql,req.whUser,req.body,driverRoute))));
  r.post('/instruction-updates/:id/media/:mediaId',express.raw({type:['video/mp4','image/jpeg'],limit:'35mb'}),run((req,res)=>res.json(uploadInstructionUpdateMedia(sql,req.whUser,req.params.id,req.params.mediaId,req.body,req.get('content-type')?.split(';')[0]))));
  r.get('/instruction-updates/:id/media/:mediaId',run((req,res)=>{const m=instructionUpdateMedia(sql,req.whUser,req.params.id,req.params.mediaId);serveInstructionMedia(req,res,m);}));
  r.post('/instruction-updates/:id/submit',run((req,res)=>res.json(submitInstructionUpdate(sql,req.whUser,req.params.id))));
  r.post('/instruction-updates/:id/review',run((req,res)=>res.json(reviewInstructionUpdate(sql,req.whUser,req.params.id,req.body))));
  r.post('/reference-guides',run((req,res)=>res.json(saveReferenceGuide(sql,req.whUser,req.body))));
  r.post('/reference-media/:id',express.raw({type:['video/mp4','image/jpeg','audio/ogg'],limit:'35mb'}),run((req,res)=>res.json(saveReferenceMedia(sql,req.whUser,req.params.id,req.query,req.body,req.get('content-type')?.split(';')[0]))));
  r.get('/reference-media/:id',run((req,res)=>{const m=referenceMedia(sql,req.whUser,req.params.id,req.query.routeId,driverRoute);serveInstructionMedia(req,res,m);}));
  r.get('/reference-guides',run((req,res)=>{permit(req.whUser,['admin','office'],'deliveries');res.json({guides:sql.prepare('SELECT * FROM wh_reference_guides ORDER BY title').all().map(g=>({...g,media:guideMedia(sql,g.id)}))});}));
  r.get('/users',run((req,res)=>{permit(req.whUser,['admin']);res.json({users:sql.prepare('SELECT * FROM wh_users ORDER BY display_name').all().map(safeUser)});}));
  r.get('/team-identities',run(async(req,res)=>{permit(req.whUser,['admin']);await source.refresh(req.whUser);res.json({team:sql.prepare('SELECT id,name FROM wh_source_team ORDER BY name').all()});}));
  r.post('/users',run(async(req,res)=>{permit(req.whUser,['admin']);res.status(201).json({user:await addUser(sql,req.whUser,req.body)});}));
  r.post('/users/:id/disable',run((req,res)=>res.json(disableUser(sql,req.whUser,req.params.id))));
  r.post('/users/:id',run(async(req,res)=>res.json(await updateUser(sql,req.whUser,req.params.id,req.body))));
  r.get('/driver-catalog',run(async(req,res)=>{permit(req.whUser,['admin','office','driver'],'deliveries');res.json(await source.catalog(req.whUser,String(req.query.version||''),req.query.force==='true'));}));
  r.get('/stock/catalog',run(async(req,res)=>{permit(req.whUser,['admin','office','warehouse'],'warehouse');let catalog=null,warning='';try{catalog=await source.request('/api/mobile-warehouse-catalog',req.whUser);}catch{warning='Live barcode lookup is unavailable. Search by product name or warehouse SKU.';}try{res.json({...stockCatalog(db,req.whUser,catalog,db.getConnection?.()?.realm_id),warning});}catch(error){if(error.status!==409)throw error;res.json({...stockCatalog(db,req.whUser),warning:error.message});}}));
  r.post('/stock/pallets',run((req,res)=>res.status(201).json(registerStockPallet(db,req.whUser,req.body))));
  r.post('/stock/pallets/:id/move',run((req,res)=>res.json(moveStockPallet(db,req.whUser,{...req.body,palletId:Number(req.params.id)}))));
  r.post('/stock/pallets/:id/edit',run((req,res)=>res.json(editStockPallet(db,req.whUser,{...req.body,palletId:Number(req.params.id)}))));
  r.post('/stock/pallets/:id/copy',run((req,res)=>res.json(copyStockPallet(db,req.whUser,{...req.body,palletId:Number(req.params.id)}))));
  r.get('/container-layout',run((req,res)=>{permit(req.whUser,['admin','office','warehouse'],'warehouse');if(!readContainerLayout)problem('Container layout is not configured.',503);res.json(decoratePalletLayout(sql,readContainerLayout()));}));
  r.get('/picking',run(async(req,res)=>{permit(req.whUser,['admin','office','warehouse'],'warehouse');res.json(await source.request('/api/mobile-warehouse',req.whUser));}));
  r.post('/picking',run(async(req,res)=>{permit(req.whUser,['admin','office','warehouse'],'warehouse');const result=await source.request('/api/mobile-warehouse',req.whUser,req.body);audit(sql,req.whUser,'warehouse-action','route',String(req.body.command?.args?.[0]||''),{requestId:req.body.requestId,action:req.body.command?.name});res.json(result);}));
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
    const routes=rows.map(r=>{const pending=sql.prepare("SELECT COUNT(*) n FROM wh_driver_events WHERE route_id=? AND status='pending-office'").get(r.id).n,conflicts=sql.prepare("SELECT COUNT(*) n FROM wh_source_jobs WHERE route_id=? AND status='conflict'").get(r.id).n;return {...r,snapshot:driverRoute(sql,req.whUser,r.id).snapshot,officeSyncStatus:conflicts?'Office sync needs review':pending?'Saved; office handoff pending':'Office connected',officePending:pending,officeConflicts:conflicts};});
    const assignedRouteIds=source.configured()?sql.prepare('SELECT route_id FROM wh_source_access WHERE user_id=?').all(req.whUser.id).map(r=>r.route_id):undefined;
    res.json({routes,assignedRouteIds,officePending:routes.reduce((n,r)=>n+r.officePending,0),officeConflicts:routes.reduce((n,r)=>n+r.officeConflicts,0),preparedAt:new Date().toISOString(),officeConnection:source.configured()?'automatic':'not-configured'});
  }));
  r.get('/routes/:id',run((req,res)=>res.json({route:driverRoute(sql,req.whUser,req.params.id)})));
  r.get('/routes/:id/activity',run((req,res)=>{
    const route=driverRoute(sql,req.whUser,req.params.id);
    const events=sql.prepare('SELECT e.*,u.display_name FROM wh_driver_events e JOIN wh_users u ON u.id=e.user_id WHERE e.route_id=? ORDER BY e.sequence').all(route.id).map(e=>({id:e.id,kind:e.kind,payload:JSON.parse(e.payload),recordedAt:e.device_at,receivedAt:e.received_at,status:e.status,displayName:e.display_name}));
    const syncIssues=sql.prepare("SELECT event_id,status,error,updated_at FROM wh_source_jobs WHERE route_id=? AND status<>'applied' ORDER BY updated_at").all(route.id);
    res.json({route,events,motive:motiveEvidence(sql,req.whUser,route),syncIssues,officeConnection:source.configured()?'automatic':'not-configured'});
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
