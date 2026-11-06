import test from 'node:test';import assert from 'node:assert/strict';
import {routingCustomers,weekdayAssignment} from '../src/web/route-rule-model.js';
test('customer rules keep delivery locations separate and find customers without current invoices',()=>{
 const p={customerRoutingDirectory:[{qboCustomerId:'1',deliveryLocationId:'A',customerName:'Açaí warehouse'},{qboCustomerId:'1',deliveryLocationId:'B',customerName:'Acai shop'},{qboCustomerId:'2',customerName:'No recent order'}],customerRouteDefaults:[{qboCustomerId:'1',deliveryLocationId:'B',customerName:'Acai shop',days:[5],dayAssignments:{5:{templateId:'Friday',rank:2}}}]};
 assert.equal(routingCustomers(p,'acai').length,2);assert.equal(routingCustomers(p,'',true).length,1);assert.equal(routingCustomers(p,'recent').length,1);assert.deepEqual(weekdayAssignment(p.customerRouteDefaults[0],5),{templateId:'Friday',rank:2});assert.equal(weekdayAssignment(p.customerRouteDefaults[0],1),undefined);
});
