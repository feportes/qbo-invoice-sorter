import {problem} from './warehouse_auth.js';
export function planningMapRoute(packet,id){
 const route=packet.routes.find(r=>r.id===id);if(!route)problem('Planning route not found.',404);
 if(route.fulfillment==='customer-pickup')problem('Warehouse pickups do not have a driving route.');
 return {id:route.id,name:route.name,stops:packet.orders.filter(o=>o.route===id).sort((a,b)=>a.stop-b.stop).map(o=>({id:o.id,lat:o.lat,lng:o.lng,customerName:o.customerName}))};
}
