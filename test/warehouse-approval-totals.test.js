import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import express from 'express';
import {createWarehouseRouter} from '../src/warehouse_api.js';
import {approvalQueue} from '../src/warehouse_approvals.js';
import {reviewTotals} from '../src/web/invoice-review-totals.js';
test('saved invoice total and fractional quantity correction retain invoice case rate',()=>{
 const totals=reviewTotals({hasBaseline:true,invoiceTotal:300,invoiceTaxAmount:0,lines:[{qty:2.5,unitRate:25,originalQty:12,originalRate:25,taxable:false}]});assert.equal(totals.proposedTotal,62.5);assert.equal(totals.difference,-237.5);
});
test('administrator archives old tests without deleting snapshots, events or inventory; current routes remain available',async t=>{
 const sql=new Database(':memory:'),today=new Date().toLocaleDateString('en-CA',{timeZone:'America/Los_Angeles'}),env={WAREHOUSE_ENABLED:'true',WAREHOUSE_BOOTSTRAP_TOKEN:'test-setup-'.repeat(8),WAREHOUSE_SOURCE_URL:'https://office.example.test',WAREHOUSE_SOURCE_TOKEN:'synthetic-token',WAREHOUSE_BRIDGE_KEY:'synthetic-bridge-'.repeat(4)},calls=[];
 const sourceFetch=async(url,options)=>{if(options.method==='POST'){calls.push(JSON.parse(options.body));return Response.json({revision:2,orders:[],archivedRoutes:[{id:'OLD',date:'2026-10-28',name:'Test route'}],message:'Archived 1 past test route.'});}return Response.json({revision:1,orders:[],pastTestRoutes:[{id:'OLD',date:'2026-10-28',name:'Test route'}]});};
 const app=express();app.use(express.json());const router=createWarehouseRouter({db:{sqlite:sql},env,sourceFetch});app.use('/api/warehouse',router);const server=app.listen(0,'127.0.0.1');await new Promise(r=>server.once('listening',r));t.after(()=>{router.stopWarehouseSync();server.close();sql.close();});const base='http://127.0.0.1:'+server.address().port+'/api/warehouse',post=async(path,body,token)=>{const r=await fetch(base+path,{method:'POST',headers:{'Content-Type':'application/json',...(token?{Authorization:'Bearer '+token}:{})},body:JSON.stringify(body)});return {status:r.status,data:await r.json()};};
 await post('/bootstrap',{token:env.WAREHOUSE_BOOTSTRAP_TOKEN,username:'test.admin',displayName:'Test Admin',password:'24682468'});const owner=(await post('/login',{username:'test.admin',password:'24682468'})).data;
 for(const [id,date]of [['OLD','2026-10-28'],['TODAY',today],['FUTURE','2099-01-01']])sql.prepare('INSERT INTO wh_routes(id,route_date,driver_id,status,revision,snapshot,source_revision,updated_at) VALUES (?,?,?,?,?,?,?,?)').run(id,date,'D','delivered',1,JSON.stringify({id,name:id,stops:[{id:'S-'+id,status:'completed',products:[{id:'P',qty:10}],counts:{P:8}}]}),'v1',new Date().toISOString());
 const before=sql.prepare('SELECT snapshot FROM wh_routes WHERE id=?').get('TODAY').snapshot;
 assert.equal((await post('/approvals/archive-tests',{requestId:'BAD',beforeDate:'2099-01-01'},owner.token)).status,400);assert.equal(calls.length,0);
 const preview=await fetch(base+'/approvals/cleanup-preview',{headers:{Authorization:'Bearer '+owner.token}});assert.equal((await preview.json()).routes.length,1);
 const result=await post('/approvals/archive-tests',{requestId:'CLEANUP',beforeDate:today},owner.token);assert.equal(result.status,200,JSON.stringify(result.data));assert.equal(result.data.archivedCount,1);assert.equal(sql.prepare('SELECT status FROM wh_routes WHERE id=?').get('OLD').status,'archived');assert.equal(JSON.parse(sql.prepare('SELECT snapshot FROM wh_routes WHERE id=?').get('OLD').snapshot).stops[0].counts.P,8);assert.equal(sql.prepare('SELECT snapshot FROM wh_routes WHERE id=?').get('TODAY').snapshot,before);assert.equal(sql.prepare('SELECT status FROM wh_routes WHERE id=?').get('FUTURE').status,'delivered');assert.equal(sql.prepare("SELECT COUNT(*) n FROM wh_audit WHERE action='past-test-routes-archived'").get().n,1);
 const source={request:async()=>({revision:2,orders:[{id:'S-OLD',routeId:'OLD',status:'pending'},{id:'S-TODAY',routeId:'TODAY',status:'pending'}]})};const queue=await approvalQueue(sql,{id:'ADMIN',role:'admin'},source);assert.deepEqual(queue.orders.map(o=>o.id),['S-TODAY']);
});
