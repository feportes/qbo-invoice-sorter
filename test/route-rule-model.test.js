import test from 'node:test';import assert from 'node:assert/strict';
import {routingCustomers,weekdayAssignment,routeRuleOverview} from '../src/web/route-rule-model.js';
test('customer rules keep delivery locations separate and find customers without current invoices',()=>{
 const p={customerRoutingDirectory:[{qboCustomerId:'1',deliveryLocationId:'A',customerName:'Açaí warehouse'},{qboCustomerId:'1',deliveryLocationId:'B',customerName:'Acai shop'},{qboCustomerId:'2',customerName:'No recent order'}],customerRouteDefaults:[{qboCustomerId:'1',deliveryLocationId:'B',customerName:'Acai shop',days:[5],dayAssignments:{5:{templateId:'Friday',rank:2}}}]};
 assert.equal(routingCustomers(p,'acai').length,2);assert.equal(routingCustomers(p,'',true).length,1);assert.equal(routingCustomers(p,'recent').length,1);assert.deepEqual(weekdayAssignment(p.customerRouteDefaults[0],5),{templateId:'Friday',rank:2});assert.equal(weekdayAssignment(p.customerRouteDefaults[0],1),undefined);
});

test('overview sorts independent weekday routes by preferred rank, keeps gaps and locations, and lists missing rules',()=>{
 const c=(id,name,loc='')=>({qboCustomerId:id,customerName:name,deliveryLocationId:loc}),a=c('1','Occasional Oceanside'),b=c('2','Camp South'),w=c('3','Catering warehouse','W'),n=c('3','Catering North','N');
 const rule=(customer,assignments)=>({...customer,days:Object.keys(assignments).map(Number),dayAssignments:assignments});
 const p={templates:[{id:'A',name:'Alohana'},{id:'B',name:'Oak'},{id:'EMPTY',name:'Unused'}],customerRoutingDirectory:[b,a,w,n,c('4','No rule')],customerRouteDefaults:[rule(b,{1:{templateId:'A',rank:2},5:{templateId:'A',rank:1}}),rule(a,{1:{templateId:'A',rank:1}}),rule(w,{1:{templateId:'A',rank:6}})]},copy=structuredClone(p),result=routeRuleOverview(p);
 assert.deepEqual(result.groups.map(g=>g.day),[1,5]);assert.deepEqual(result.groups[0].stops.map(s=>[s.customer.customerName,s.rank]),[['Occasional Oceanside',1],['Camp South',2],['Catering warehouse',6]]);assert.deepEqual(result.unruled.map(c=>c.customerName),['Catering North','No rule']);assert.deepEqual(result.unusedRoutes.map(t=>t.id),['B','EMPTY']);assert.equal(routeRuleOverview(p,{day:5}).groups[0].stops.length,1);assert.equal(routeRuleOverview(p,{routeId:'B'}).groups.length,0);assert.deepEqual(p,copy);
});
test('overview highlights duplicate ranks and broken rules without treating weekday absence as missing all rules',()=>{
 const p={templates:[{id:'A',name:'North'}],customerRoutingDirectory:[],customerRouteDefaults:[{qboCustomerId:'1',customerName:'One',days:[2],templateId:'A',rank:1},{qboCustomerId:'2',customerName:'Two',days:[2],templateId:'A',rank:1},{qboCustomerId:'3',customerName:'Removed route',days:[3],templateId:'OLD',rank:3}]};
 const result=routeRuleOverview(p);assert.ok(result.groups[0].stops.every(s=>s.sharedRank));assert.equal(result.issues.length,1);assert.equal(result.unruled.length,0);assert.equal(routeRuleOverview(p,{day:5}).unruled.length,0);assert.equal(routeRuleOverview(p,{query:'two'}).groups[0].stops.length,1);
});
