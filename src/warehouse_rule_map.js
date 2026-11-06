import {routeRuleOverview,ruleKey} from './web/route-rule-model.js';
import {problem} from './warehouse_auth.js';
export const ruleMapRouteId=(day,id)=>'RULE-'+day+'-'+id;
export function ruleMapPacket(packet,day){
 if(!Number.isInteger(day)||day<0||day>6)problem('Choose a valid rule weekday.');
 const result=routeRuleOverview(packet,{day}),date='saved-weekday-plan',routes=packet.templates.map(t=>({id:ruleMapRouteId(day,t.id),templateId:t.id,name:t.name,date,status:'draft',fulfillment:'delivery'})),orders=[];
 for(const group of result.groups)for(const [i,s] of group.stops.entries()){const c=s.customer;orders.push({id:ruleKey(c),qboCustomerId:c.qboCustomerId,locationId:c.deliveryLocationId||'',route:ruleMapRouteId(day,group.route.id),stop:i+1,preferredStop:s.rank,customerName:c.customerName,address:c.address||'',...(Number.isFinite(c.lat)&&Number.isFinite(c.lng)?{lat:c.lat,lng:c.lng}:{}),locations:[],products:[],date,manual:false});}
 return {revision:packet.revision,routes,orders,date,day,issues:result.issues};
}
export function ruleMapMove(packet,input){
 const day=input.day,map=ruleMapPacket(packet,day),o=map.orders.find(o=>o.id===input.orderId),source=map.routes.find(r=>r.id===o?.route),target=map.routes.find(r=>r.id===input.targetRouteId);
 if(!o||!source||!target)problem('The rule or saved route changed. Reopen the map.',409);
 const others=map.orders.filter(x=>x.route===target.id&&x.id!==o.id);if(!Number.isInteger(input.position)||input.position<1||input.position>others.length+1)problem('Choose a valid stop position.');
 return {name:'moveCustomerRouteRule',args:[{qboCustomerId:o.qboCustomerId,deliveryLocationId:o.locationId,day,targetTemplateId:target.templateId,position:input.position,expectedSourceTemplateId:source.templateId}]};
}
