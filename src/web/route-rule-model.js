export const ruleKey=c=>JSON.stringify([c.qboCustomerId,c.deliveryLocationId||'']);
export function routingCustomers(packet,query='',onlyRules=false){
 const all=new Map((packet.customerRoutingDirectory||[]).map(c=>[ruleKey(c),c]));
 for(const r of packet.customerRouteDefaults||[])all.set(ruleKey(r),{...all.get(ruleKey(r)),...r});
 const q=query.normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase();
 return [...all.values()].filter(c=>(!onlyRules||packet.customerRouteDefaults.some(r=>ruleKey(r)===ruleKey(c)))&&c.customerName.normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase().includes(q)).sort((a,b)=>a.customerName.localeCompare(b.customerName));
}
export function weekdayAssignment(rule,day){return rule?.dayAssignments?rule.dayAssignments[day]:rule?.days?.includes(day)?rule:null;}

// Keep weekday plans separate: the same route may have a different order each day.
export function routeRuleOverview(packet,{query='',day=null,routeId=''}={}){
 const customers=routingCustomers(packet,query),rules=new Map((packet.customerRouteDefaults||[]).map(r=>[ruleKey(r),r])),templates=new Map((packet.templates||[]).map(t=>[t.id,t])),groups=new Map(),unruled=[],issues=[],used=new Set(),assigned=new Set();
 for(const c of customers){const rule=rules.get(ruleKey(c));if(!rule?.days?.length){unruled.push(c);continue;}
  for(const weekday of new Set(rule.days)){const a=weekdayAssignment(rule,weekday),template=templates.get(a?.templateId);
   if(template)used.add(template.id);
   if(day!==null&&weekday!==day||routeId&&a?.templateId!==routeId)continue;
   if(!Number.isInteger(weekday)||weekday<0||weekday>6||!template||!Number.isInteger(a?.rank)||a.rank<1||a.rank>99){issues.push({customer:c,day:weekday,reason:!template?'Saved route is missing or removed':'Weekday or stop order needs review'});continue;}
   const key=JSON.stringify([weekday,template.id]);if(!groups.has(key))groups.set(key,{key,day:weekday,route:template,stops:[]});groups.get(key).stops.push({customer:c,rank:a.rank});assigned.add(ruleKey(c));
  }
 }
 const ordered=[...groups.values()].sort((a,b)=>(a.day+6)%7-(b.day+6)%7||a.route.name.localeCompare(b.route.name));
 for(const g of ordered){g.stops.sort((a,b)=>a.rank-b.rank||a.customer.customerName.localeCompare(b.customer.customerName)||ruleKey(a.customer).localeCompare(ruleKey(b.customer)));const counts=new Map();for(const s of g.stops)counts.set(s.rank,(counts.get(s.rank)||0)+1);for(const s of g.stops)s.sharedRank=counts.get(s.rank)>1;}
 return {groups:ordered,unruled,issues,assignedCustomerCount:assigned.size,unusedRoutes:[...templates.values()].filter(t=>!used.has(t.id)&&(!routeId||t.id===routeId)).sort((a,b)=>a.name.localeCompare(b.name))};
}
