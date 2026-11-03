import {problem,digest,audit} from './warehouse_auth.js';
export function planningMapRoute(packet,id){
 const route=packet.routes.find(r=>r.id===id);if(!route)problem('Planning route not found.',404);
 if(route.fulfillment==='customer-pickup')problem('Warehouse pickups do not have a driving route.');
 return {id:route.id,name:route.name,stops:packet.orders.filter(o=>o.route===id).sort((a,b)=>a.stop-b.stop).map(o=>({id:o.id,lat:o.lat,lng:o.lng,customerName:o.customerName}))};
}

const normalizeAddress=value=>String(value||'').normalize('NFKC').toLowerCase().replace(/\s+/g,' ').trim();
const point=p=>Number.isFinite(p?.lat)&&Number.isFinite(p?.lng)&&Math.abs(p.lat)<=85&&Math.abs(p.lng)<=180;
export function ensureAddressPins(sql){sql.exec('CREATE TABLE IF NOT EXISTS wh_address_map_pins(address_key TEXT PRIMARY KEY,address TEXT NOT NULL,lat REAL NOT NULL,lng REAL NOT NULL,matched_address TEXT NOT NULL,created_at TEXT NOT NULL,expires_at TEXT NOT NULL)');}
export function attachAddressPins(sql,stops){ensureAddressPins(sql);const read=sql.prepare('SELECT lat,lng,matched_address FROM wh_address_map_pins WHERE address_key=? AND expires_at>?');for(const stop of stops||[]){if(point(stop)||stop.requiresLocation&&!stop.locationId||!stop.address)continue;const cached=read.get(digest(normalizeAddress(stop.address)),new Date().toISOString());if(cached){stop.lat=cached.lat;stop.lng=cached.lng;stop.mapPinSource='Google address match';}}return stops;}
export function attachPlanningPins(sql,packet){attachAddressPins(sql,packet.orders);return packet;}
export function savePlanningAddressPin(sql,user,packet,input){
 const stop=packet.orders.find(o=>o.id===input.orderId);if(!stop)problem('This planning stop no longer exists.',404);
 if(stop.requiresLocation&&!stop.locationId)problem('Choose this order’s delivery location first.',409);
 if(!stop.address||normalizeAddress(stop.address)!==normalizeAddress(input.expectedAddress))problem('The delivery address changed. Locate it again.',409);
 const physicalMatch=['ROOFTOP','RANGE_INTERPOLATED'].includes(input.locationType)||input.locationType==='GEOMETRIC_CENTER'&&Array.isArray(input.matchTypes)&&input.matchTypes.some(t=>['street_address','premise','subpremise'].includes(t));if(!point(input)||!physicalMatch||typeof input.matchedAddress!=='string'||input.matchedAddress.length>500)problem('Choose one complete street-address match.',400);
 ensureAddressPins(sql);const key=digest(normalizeAddress(stop.address)),now=new Date().toISOString(),expires=new Date(Date.now()+29*86400000).toISOString();
 sql.prepare('INSERT OR REPLACE INTO wh_address_map_pins VALUES (?,?,?,?,?,?,?)').run(key,stop.address,input.lat,input.lng,input.matchedAddress,now,expires);
 audit(sql,user,'planning-address-located','map-address',key,{orderId:stop.id,source:'Google street address',matchedAddress:input.matchedAddress,lat:input.lat,lng:input.lng});
 return {lat:input.lat,lng:input.lng,matchedAddress:input.matchedAddress};
}
