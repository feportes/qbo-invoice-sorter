export const ruleKey=c=>JSON.stringify([c.qboCustomerId,c.deliveryLocationId||'']);
export function routingCustomers(packet,query='',onlyRules=false){
 const all=new Map((packet.customerRoutingDirectory||[]).map(c=>[ruleKey(c),c]));
 for(const r of packet.customerRouteDefaults||[])all.set(ruleKey(r),{...all.get(ruleKey(r)),...r});
 const q=query.normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase();
 return [...all.values()].filter(c=>(!onlyRules||packet.customerRouteDefaults.some(r=>ruleKey(r)===ruleKey(c)))&&c.customerName.normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase().includes(q)).sort((a,b)=>a.customerName.localeCompare(b.customerName));
}
export function weekdayAssignment(rule,day){return rule?.dayAssignments?rule.dayAssignments[day]:rule?.days?.includes(day)?rule:null;}
