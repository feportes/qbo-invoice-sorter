import {routeWindow} from './warehouse_telemetry.js';
import {applyDriverEvent} from './web/driver-state.js';
import {attachReferenceGuides,ensureGuideSchema} from './warehouse_guides.js';
import {randomUUID} from 'node:crypto';
import {audit,permit,problem} from './warehouse_auth.js';
import {once} from './warehouse_receiving.js';
import {overlayDriverCatalog,driverBarcodeProof} from './warehouse_mobile_catalog.js';
const clean=v=>String(v??'').trim();
const code=v=>clean(v).toUpperCase();
const qty=(v,min=0)=>{if(typeof v!=='number'||!Number.isFinite(v)||v<min||v>1000000)problem('Review the product quantity.');return Math.round(v*1e6)/1e6;};
export function driverRoute(sql,user,id){
  const row=sql.prepare('SELECT * FROM wh_routes WHERE id=?').get(id);if(!row)problem('Route not found.',404);
  permit(user,['driver','office','admin'],'deliveries');
  if(!['admin','manager','office'].includes(user.role)&&sql.prepare('SELECT user_id FROM wh_source_access_cache WHERE user_id=?').get(user.id)&&!sql.prepare('SELECT route_id FROM wh_source_access WHERE user_id=? AND route_id=?').get(user.id,id))problem('This route is no longer assigned to you.',403);
  const snapshot=routeWindow(sql,{...row,snapshot:JSON.parse(row.snapshot)});
  if(!['admin','manager','office'].includes(user.role)&&(!user.teamId||![row.driver_id,snapshot.helperId,...(snapshot.helperIds||[])].includes(user.teamId)))problem('This route is not assigned to you.',403);
  ensureGuideSchema(sql);return {...row,snapshot:attachReferenceGuides(sql,overlayDriverCatalog(sql,user,snapshot))};
}
export function validateRoute(input){
  if(input.helperId!==undefined&&(typeof input.helperId!=='string'||input.helperId.length>100))problem('Review the assigned route helper.');
  if(input.helperIds!==undefined&&(!Array.isArray(input.helperIds)||input.helperIds.length>20||input.helperIds.some(id=>typeof id!=='string'||!id||id.length>100)))problem('Review assigned route helpers.');
  if(!/^[A-Za-z0-9_-]{1,100}$/.test(input.id||'')||!/^\d{4}-\d{2}-\d{2}$/.test(input.date||'')||!clean(input.driverId)||!['loaded','delivering','delivered'].includes(input.status)||!Array.isArray(input.stops)||input.stops.length>200)problem('Review the route packet.');
  const ids=new Set();for(const stop of input.stops){if(!clean(stop.id)||ids.has(stop.id)||!Array.isArray(stop.products)||stop.products.length>200)problem('Review route stops.');ids.add(stop.id);const products=new Set(),codes=new Map();for(const p of stop.products){if(!clean(p.id)||products.has(p.id)||!Array.isArray(p.barcodes)||p.barcodes.length>100)problem('Review stop products.');products.add(p.id);qty(p.qty,0.000001);for(const c of [...p.barcodes,p.sku].filter(Boolean)){const key=code(c);if(codes.has(key)&&codes.get(key)!==p.id)problem('A barcode identifies two products.');codes.set(key,p.id);}}}
  return structuredClone(input);
}
export function importRoute(sql,actor,input){
  const route=validateRoute(input.route),sourceRevision=clean(input.sourceRevision);if(!sourceRevision||sourceRevision.length>200)problem('Source revision is required.');
  return once(sql,actor,input.requestId,input,()=>{
    const old=sql.prepare('SELECT * FROM wh_routes WHERE id=?').get(route.id);
    if(old?.source_revision===sourceRevision&&!input.refreshContent)return {routeId:route.id,unchanged:true};
    if(old&&sql.prepare("SELECT id FROM wh_driver_events WHERE route_id=? AND status='pending-office' LIMIT 1").get(route.id))problem('Sync the driver events before replacing this route packet.',409);
    const revision=(old?.revision||0)+1,at=new Date().toISOString();
    sql.prepare('INSERT INTO wh_routes(id,route_date,driver_id,status,revision,snapshot,source_revision,updated_at) VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET route_date=excluded.route_date,driver_id=excluded.driver_id,status=excluded.status,revision=excluded.revision,snapshot=excluded.snapshot,source_revision=excluded.source_revision,updated_at=excluded.updated_at').run(route.id,route.date,route.driverId,route.status,revision,JSON.stringify(route),sourceRevision,at);
    audit(sql,actor,'route-imported','route',route.id,{sourceRevision,revision,stops:route.stops.length});return {routeId:route.id,revision};
  });
}
export {applyDriverEvent} from './web/driver-state.js';
export function saveDriverEvents(sql,user,input){
  permit(user,['admin','office','driver'],'deliveries');if(!Array.isArray(input.events)||!input.events.length||input.events.length>100)problem('Send between 1 and 100 saved actions.');
  if(!/^[A-Za-z0-9_-]{8,100}$/.test(input.deviceId||''))problem('A stable device ID is required.');
  return once(sql,user,input.requestId,input,()=>{
    const row=driverRoute(sql,user,input.routeId),route=row.snapshot;
    if(row.revision!==input.expectedRevision&&(!input.baseState||JSON.stringify(driverReplayGuard(row.snapshot,input.events))!==JSON.stringify(driverReplayGuard(input.baseState,input.events))))problem('This stop or route changed. Keep the device work for conflict review.',409);
    const ids=new Set();for(const event of input.events){
      if(!/^[A-Za-z0-9_-]{8,100}$/.test(event.id||'')||ids.has(event.id)||!Number.isFinite(Date.parse(event.recordedAt)))problem('Review saved action identifiers and timestamps.');ids.add(event.id);
      if(sql.prepare('SELECT id FROM wh_driver_events WHERE id=?').get(event.id))problem('This action was already received in another batch. Refresh before syncing.',409);
      if(event.kind==='photo')for(const id of event.payload?.mediaIds||[]){const media=sql.prepare('SELECT * FROM wh_media WHERE id=?').get(id);if(!media||media.route_id!==row.id||media.user_id!==user.id||String(media.stop_id||'')!==String(event.payload.stopId||''))problem('Upload the matching route photos before syncing the log.');}
      if(event.kind==='manual-pick'&&user.role!=='admin')problem('Manual picking requires administrator access.',403);
      const at=new Date().toISOString(),proof=event.kind==='scan'?driverBarcodeProof(sql,user,route,event):null;applyDriverEvent(route,event,at,proof);
      sql.prepare('INSERT INTO wh_driver_events (id,route_id,user_id,device_id,device_at,received_at,kind,payload,status) VALUES (?,?,?,?,?,?,?,?,?)').run(event.id,row.id,user.id,input.deviceId,event.recordedAt,at,event.kind,JSON.stringify(event.payload||{}),'pending-office');
      audit(sql,user,'driver-'+event.kind,'route',row.id,{eventId:event.id,deviceAt:event.recordedAt,stopId:event.payload?.stopId||null,payload:event.payload||{}});
    }
    sql.prepare('UPDATE wh_routes SET revision=revision+1,status=?,snapshot=?,updated_at=? WHERE id=?').run(route.status,JSON.stringify(route),new Date().toISOString(),row.id);
    return {route:driverRoute(sql,user,row.id),acknowledged:input.events.map(e=>e.id),quickbooksChanged:false};
  });
}

// Compare only the work this batch can change. Office approval of another stop
// must not strand a driver's offline scans; changes to this stop still conflict.
export function driverReplayGuard(route,events){
 const whole=events.some(e=>['depart','finalize'].includes(e.kind)),ids=new Set(events.filter(e=>!['note','photo'].includes(e.kind)).map(e=>e.payload?.stopId).filter(Boolean));
 const financial=events.some(e=>e.kind==='edit-line');
 const dictionary=(value,fn)=>Object.keys(value||{}).sort().map(k=>[k,fn(value[k])]);
 const stops=route.stops||[];return {
  route:[route.id,route.date,route.driverId,route.helperId||'',route.status],plan:stops.map(s=>[s.id,s.address||'']),
  stops:stops.filter(s=>whole||ids.has(s.id)).map(s=>({id:s.id,status:s.status||'pending',invoice:s.qboInvoiceId||'',customer:s.qboCustomerId||'',products:(s.products||[]).map(p=>[p.id,p.qty,...(financial?[p.unitPrice??null]:[])]),counts:dictionary(s.counts,n=>n),history:(s.history||[]).slice(-200).map(h=>[h.productId,h.qty,!!h.reversedAt]),shortages:dictionary(s.shortages,x=>[x.missing,x.reason,x.notes||'']),...(!whole?{officeReview:s.officeReview||'draft',proposedChanges:dictionary(s.proposedChanges,x=>[x.qty,x.unitPrice,x.reason||'']),payments:(s.payments||[]).map(x=>[x.id,x.amount,x.method,x.reference||''])}:{})}))
 };
}
