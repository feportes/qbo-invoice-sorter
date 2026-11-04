import {createHash,createHmac,randomUUID} from 'node:crypto';
import {safeUser,problem,audit} from './warehouse_auth.js';
import {importRoute} from './warehouse_driver.js';
import {saveDriverCatalog} from './warehouse_mobile_catalog.js';

const hash=text=>createHash('sha256').update(text).digest('hex');
export function signedSourceRequest(origin,path,user,secret,body,now=Date.now()){
  const url=new URL(path,origin);if(url.origin!==new URL(origin).origin||url.protocol!=='https:')problem('Office service must use its configured HTTPS origin.',503);
  const identity=Buffer.from(JSON.stringify({id:user.id,username:user.username,role:user.role,teamId:user.teamId,functions:user.functions})).toString('base64'),timestamp=String(now),raw=body===undefined?'':JSON.stringify(body),method=body===undefined?'GET':'POST';
  const signature=createHmac('sha256',secret).update([method,url.pathname+url.search,timestamp,identity,hash(raw)].join('\n')).digest('hex');
  return {url:url.href,options:{method,redirect:'error',headers:{'Content-Type':'application/json','x-warehouse-time':timestamp,'x-warehouse-identity':identity,'x-warehouse-signature':signature},...(body===undefined?{}:{body:raw})}};
}
export function createWarehouseSource({sql,env=process.env,fetchImpl=fetch,readInvoice}){
  const configured=()=>!!(env.WAREHOUSE_SOURCE_URL&&env.WAREHOUSE_SOURCE_TOKEN&&env.WAREHOUSE_BRIDGE_KEY?.length>=32),active=new Map();
  const bridgeActor={id:'warehouse-office-bridge',displayName:'Office service',role:'admin'};
  async function request(path,user,body){
    if(!configured())problem('Automatic office connection is not configured.',503);
    const r=signedSourceRequest(env.WAREHOUSE_SOURCE_URL,path,user,env.WAREHOUSE_BRIDGE_KEY,body);r.options.headers['OAI-Sites-Authorization']='Bearer '+env.WAREHOUSE_SOURCE_TOKEN;r.options.signal=AbortSignal.timeout(20000);
    const response=await fetchImpl(r.url,r.options),text=await response.text();if(text.length>12000000)problem('Office returned an oversized response.',502);
    let data;try{data=JSON.parse(text);}catch{problem('Office did not return a service response.',502);}if(!response.ok)problem(data.error||'Office service rejected this request.',response.status);return data;
  }
  async function refresh(user){
    if(!configured())return {configured:false};
    const payload=await request('/api/mobile-routes',user);if(!Array.isArray(payload.routes))problem('Office route response is incomplete.',502);
    sql.transaction(()=>{
      sql.prepare('DELETE FROM wh_source_access WHERE user_id=?').run(user.id);
      for(const entry of payload.routes)sql.prepare('INSERT INTO wh_source_access VALUES (?,?)').run(user.id,entry.route.id);
      sql.prepare('INSERT INTO wh_source_access_cache VALUES (?,?) ON CONFLICT(user_id) DO UPDATE SET refreshed_at=excluded.refreshed_at').run(user.id,new Date().toISOString());
      for(const entry of payload.routes){
        if(sql.prepare("SELECT id FROM wh_driver_events WHERE route_id=? AND status<>'applied-to-office' LIMIT 1").get(entry.route.id))continue;
        const old=sql.prepare('SELECT source_stop_revisions,snapshot FROM wh_routes WHERE id=?').get(entry.route.id),changed=old&&(old.source_stop_revisions!==JSON.stringify(entry.stopRevisions||{})||old.snapshot!==JSON.stringify(entry.route));
        importRoute(sql,bridgeActor,{requestId:randomUUID(),route:entry.route,sourceRevision:entry.sourceRevision,refreshContent:!!changed});
        sql.prepare('UPDATE wh_routes SET source_stop_revisions=? WHERE id=?').run(JSON.stringify(entry.stopRevisions||{}),entry.route.id);
      }
      for(const member of payload.team||[])if(typeof member.id==='string'&&typeof member.name==='string')sql.prepare('INSERT INTO wh_source_team VALUES (?,?) ON CONFLICT(id) DO UPDATE SET name=excluded.name').run(member.id,member.name);
    })();return {configured:true};
  }
  async function catalog(user,knownVersion='',force=false){
    const cached=sql.prepare('SELECT * FROM wh_driver_catalog WHERE user_id=?').get(user.id);
    if(!cached||force||Date.now()-Date.parse(cached.checked_at)>9000){const data=await request('/api/mobile-catalog'+(cached?'?version='+encodeURIComponent(cached.version):''),user);saveDriverCatalog(sql,user,data);}
    const row=sql.prepare('SELECT * FROM wh_driver_catalog WHERE user_id=?').get(user.id);return {version:row.version,barcodeVersion:row.barcode_version,checkedAt:row.checked_at,catalogRevision:row.source_revision,...(row.version===knownVersion?{unchanged:true}:{products:JSON.parse(row.products)})};
  }
  async function media(user,event){
    const refs=[];for(const id of event.payload.mediaIds||[]){const photo=sql.prepare('SELECT * FROM wh_media WHERE id=? AND route_id=? AND user_id=?').get(id,event.route_id,event.user_id);if(!photo)problem('Saved route photo is unavailable.',409);
      const result=await request('/api/mobile-photo',user,{id,routeId:event.route_id,base64:Buffer.from(photo.bytes).toString('base64')});refs.push(result.photo);
    }return refs;
  }
  async function flushRoute(routeId){
    if(!configured())return;
    const pending=sql.prepare("SELECT e.* FROM wh_driver_events e WHERE route_id=? AND status='pending-office' ORDER BY sequence").all(routeId);
    for(const row of pending){
      const stored=sql.prepare('SELECT * FROM wh_users WHERE id=?').get(row.user_id);if(!stored?.enabled)return;
      const user=safeUser(stored),event={id:row.id,kind:row.kind,payload:JSON.parse(row.payload),recordedAt:row.device_at};
      sql.prepare('INSERT OR IGNORE INTO wh_source_jobs(event_id,user_id,route_id,status,updated_at) VALUES (?,?,?,?,?)').run(row.id,row.user_id,routeId,'pending',new Date().toISOString());
      let job=sql.prepare('SELECT * FROM wh_source_jobs WHERE event_id=?').get(row.id);if(job.status==='conflict')return;
      try{
        if(!job.operation){
          const versions=sql.prepare('SELECT source_revision,source_stop_revisions FROM wh_routes WHERE id=?').get(routeId);let sourceRevision=versions?.source_revision,sourceStopRevision=JSON.parse(versions?.source_stop_revisions||'{}')[event.payload.stopId];
          const photos=row.kind==='photo'?await media(user,{...row,payload:event.payload}):[];
          let prepared;
          try{prepared=await request('/api/mobile-prepare',user,{routeId,sourceRevision,sourceStopRevision,event,photos});}
          catch(error){
            if(error.status!==424||!readInvoice)throw error;
            const snapshot=JSON.parse(sql.prepare('SELECT snapshot FROM wh_routes WHERE id=?').get(routeId).snapshot),stop=snapshot.stops.find(s=>s.id===event.payload.stopId);
            if(!stop?.qboInvoiceId)throw error;
            const invoice=await readInvoice(stop);
            const result=await request('/api/mobile-baseline',user,{orderId:stop.id,invoice});sourceRevision=result.sourceRevision;
            sourceStopRevision=result.sourceStopRevision;sql.prepare('UPDATE wh_routes SET source_revision=? WHERE id=?').run(sourceRevision,routeId);
            prepared=await request('/api/mobile-prepare',user,{routeId,sourceRevision,sourceStopRevision,event,photos});
          }
          sql.prepare("UPDATE wh_source_jobs SET operation=?,updated_at=? WHERE event_id=?").run(JSON.stringify(prepared.operation),new Date().toISOString(),row.id);job=sql.prepare('SELECT * FROM wh_source_jobs WHERE event_id=?').get(row.id);
        }
        // Persist the exact operation before sending. Response loss retries the same receipt.
        const result=await request('/api/mobile-operation',user,JSON.parse(job.operation));
        if(typeof result.sourceRevision!=='string'||!result.sourceRevision)problem('Office confirmation is incomplete.',502);
        sql.transaction(()=>{const at=new Date().toISOString();sql.prepare("UPDATE wh_driver_events SET status='applied-to-office',reviewed_by='automatic-office-service',reviewed_at=? WHERE id=?").run(at,row.id);sql.prepare("UPDATE wh_source_jobs SET status='applied',error=NULL,updated_at=? WHERE event_id=?").run(at,row.id);const stopVersions=JSON.parse(sql.prepare('SELECT source_stop_revisions FROM wh_routes WHERE id=?').get(routeId).source_stop_revisions);if(result.stopId&&result.sourceStopRevision)stopVersions[result.stopId]=result.sourceStopRevision;sql.prepare('UPDATE wh_routes SET source_revision=?,source_stop_revisions=? WHERE id=?').run(result.sourceRevision,JSON.stringify(stopVersions),routeId);audit(sql,user,'driver-event-applied-to-office','route',routeId,{eventId:row.id});})();
      }catch(error){
        const permanent=[400,403,409,424].includes(error.status);sql.prepare('UPDATE wh_source_jobs SET status=?,error=?,updated_at=? WHERE event_id=?').run(permanent?'conflict':'pending',permanent?String(error.message).slice(0,1000):'Office handoff '+(error.status||'network')+': '+String(error.message||'Connection unavailable').slice(0,600)+'. Saved work will retry.',new Date().toISOString(),row.id);return;
      }
    }
  }
  function kick(routeId){if(!configured()||active.has(routeId))return active.get(routeId);const task=flushRoute(routeId).catch(()=>{}).finally(()=>active.delete(routeId));active.set(routeId,task);return task;}
  function kickAll(){if(!configured())return;for(const r of sql.prepare("SELECT DISTINCT route_id FROM wh_driver_events WHERE status='pending-office'").all())kick(r.route_id);}
  return {configured,refresh,catalog,kick,kickAll,request,flushRoute};
}
