import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';import os from 'node:os';import path from 'node:path';import {randomUUID} from 'node:crypto';
import Database from 'better-sqlite3';
import {ensureWarehouseSchema} from '../src/warehouse_schema.js';
import {bootstrap,login,authenticate,addUser,changePassword,updateUser,permit,defaultFunctions} from '../src/warehouse_auth.js';
import {createPurchaseOrder,recordReceipt,suggestPutaway,confirmPutaway} from '../src/warehouse_receiving.js';
import {importRoute,saveDriverEvents,driverRoute,applyDriverEvent} from '../src/warehouse_driver.js';

const dir=fs.mkdtempSync(path.join(os.tmpdir(),'warehouse-foundation-'));process.env.DB_PATH=path.join(dir,'inventory.db');
const {db}=await import('../src/db.js');const {ensureSchema,seedDefaults,seedLocations}=await import('../src/schema.js');ensureSchema();seedDefaults();seedLocations();ensureWarehouseSchema(db.sqlite);
const admin={id:'test-admin',displayName:'Test owner',role:'admin',functions:defaultFunctions('admin')},receiver={id:'receiver',displayName:'Test receiver',role:'warehouse',functions:defaultFunctions('warehouse')};
const frozen=Number(db.sqlite.prepare("INSERT INTO skus(qbo_item_id,name,unit_type) VALUES ('1001','Test frozen cases','cases')").run().lastInsertRowid),dry=Number(db.sqlite.prepare("INSERT INTO skus(qbo_item_id,name,unit_type) VALUES ('1002','Test dry cases','cases')").run().lastInsertRowid);
const config=Number(db.sqlite.prepare("INSERT INTO pallet_configs(sku_id,name,units_per_pallet,is_default) VALUES (?,'Test 120 cases',120,1)").run(frozen).lastInsertRowid);
function po(lines){return createPurchaseOrder(db,admin,{requestId:randomUUID(),vendorId:'vendor-test',vendorName:'Synthetic vendor',poNumber:'TEST-'+randomUUID(),lines}).purchaseOrder;}
function receipt(p,lines,mode='warehouse'){return recordReceipt(db,receiver,{requestId:randomUUID(),poId:p.id,expectedPORevision:p.revision,mode,lines}).receipt;}

test('bootstrap, individual credentials, mandatory first change and administrator-only security',async()=>{
  const sql=new Database(':memory:');ensureWarehouseSchema(sql);const env={WAREHOUSE_BOOTSTRAP_TOKEN:'x'.repeat(40)};
  const owner=await bootstrap(sql,env,{token:env.WAREHOUSE_BOOTSTRAP_TOKEN,username:'owner.admin',displayName:'Test owner',password:'Owner test password 123!'});assert.equal(owner.role,'admin');assert.equal(owner.mustChangePassword,false);
  await assert.rejects(bootstrap(sql,env,{token:env.WAREHOUSE_BOOTSTRAP_TOKEN,password:'Another password 123!',username:'other.admin',displayName:'Other'}),/already configured/);
  const driver=await addUser(sql,owner,{username:'alex.driver',displayName:'Alex',password:'Temporary driver 123!',role:'driver',teamId:'TEAM-A'});
  const signed=await login(sql,{username:'ALEX.DRIVER',password:'Temporary driver 123!'},'test-ip');assert.ok(!sql.prepare('SELECT token_hash FROM wh_sessions').get().token_hash.includes(signed.token));
  const request={headers:{authorization:'Bearer '+signed.token},method:'GET'},principal=authenticate(sql,request);assert.throws(()=>permit(principal,['driver'],'deliveries'),/temporary password/);
  const changed=await changePassword(sql,principal,{currentPassword:'Temporary driver 123!',newPassword:'Changed driver password 123!'});assert.equal(changed.user.mustChangePassword,false);
  const active=authenticate(sql,request);permit(active,['driver'],'deliveries');assert.throws(()=>permit(active,['admin']),/cannot perform/);
  const manager=await addUser(sql,owner,{username:'ops.manager',displayName:'Manager',password:'Manager password 123!',role:'manager'});
  assert.throws(()=>permit({...manager,mustChangePassword:false},['admin']),/cannot perform/);permit({...manager,mustChangePassword:false},['office']);
  await updateUser(sql,owner,driver.id,{functions:[],newPassword:'Reset driver password 123!'});assert.throws(()=>authenticate(sql,request),/Sign in/);
  assert.ok(!JSON.stringify(sql.prepare('SELECT detail FROM wh_audit').all()).includes('password 123!'));sql.close();
});

test('walk-in restock and container overflow are suggestions until explicitly confirmed, with retry safety',()=>{
  db.createPallet({skuId:frozen,locationCode:'WALKIN',qtyUnits:60,userName:'Test'});
  const p=po([{skuId:frozen,orderedQty:240,area:'frozen'}]);const r=receipt(p,[{poLineId:p.lines[0].id,receivedQty:240}],'vendor-pickup');
  const before=db.sqlite.prepare('SELECT COUNT(*) n FROM pallets').get().n;
  const plan=suggestPutaway(db,r.id,{[r.lines[0].id]:{walkinTargetQty:120}});
  assert.equal(plan.allocations.filter(x=>x.locationCode==='WALKIN').reduce((n,x)=>n+x.qty,0),60);
  assert.equal(plan.allocations.filter(x=>x.locationCode!=='WALKIN').reduce((n,x)=>n+x.qty,0),180);assert.equal(plan.unplaced.length,0);
  assert.equal(db.sqlite.prepare('SELECT COUNT(*) n FROM pallets').get().n,before);
  const input={requestId:randomUUID(),receiptId:r.id,expectedRevision:r.revision,confirmed:true,allocations:plan.allocations};
  const result=confirmPutaway(db,receiver,input);assert.equal(result.palletIds.length,3);assert.equal(result.receipt.mode,'vendor-pickup');
  const again=confirmPutaway(db,receiver,input);assert.equal(again.duplicate,true);assert.deepEqual(again.palletIds,result.palletIds);
  assert.equal(db.sqlite.prepare('SELECT COUNT(*) n FROM pallets').get().n,before+3);
  assert.equal(db.sqlite.prepare("SELECT COUNT(*) n FROM inventory_movements WHERE reference_type='WAREHOUSE_RECEIPT' AND reference_id=?").get(r.id).n,3);
});

test('damage is excluded from frozen stock and dry/refrigerated receiving has no slot requirement',()=>{
  const p=po([{skuId:frozen,orderedQty:20,area:'frozen'},{skuId:dry,orderedQty:10,area:'dry'}]);
  const r=receipt(p,[{poLineId:p.lines[0].id,receivedQty:20,damagedQty:4,exceptionReason:'Four cases arrived damaged'},{poLineId:p.lines[1].id,receivedQty:10}]);
  const plan=suggestPutaway(db,r.id,{[r.lines[0].id]:{walkinQty:16}});assert.equal(plan.allocations[0].qty,16);assert.deepEqual(plan.areaReceipts.map(x=>[x.area,x.qty,x.slotTracked]),[['dry',10,false]]);
  assert.throws(()=>confirmPutaway(db,receiver,{requestId:randomUUID(),receiptId:r.id,expectedRevision:1,confirmed:true,allocations:[{receiptLineId:r.lines[0].id,locationCode:'WALKIN',qty:20,palletConfigId:config}]}),/exactly the accepted/);
  assert.throws(()=>recordReceipt(db,{id:'d',role:'driver',functions:['deliveries']},{}),e=>e.status===403);
});

test('occupied slot or invalid layout rolls back the complete putaway; aliases cannot claim a second physical slot',()=>{
  const p=po([{skuId:frozen,orderedQty:240,area:'frozen'}]),r=receipt(p,[{poLineId:p.lines[0].id,receivedQty:240}]);
  const plan=suggestPutaway(db,r.id);assert.equal(plan.allocations.length,2);
  const occupied=plan.allocations[1].locationCode;db.createPallet({skuId:frozen,locationCode:occupied,qtyUnits:1,userName:'Other receiver'});
  const before=db.sqlite.prepare('SELECT COUNT(*) n FROM pallets').get().n;
  assert.throws(()=>confirmPutaway(db,receiver,{requestId:randomUUID(),receiptId:r.id,expectedRevision:r.revision,confirmed:true,allocations:plan.allocations}),/occupied/);
  assert.equal(db.sqlite.prepare('SELECT COUNT(*) n FROM pallets').get().n,before);assert.equal(db.sqlite.prepare('SELECT putaway_at FROM wh_receipts WHERE id=?').get(r.id).putaway_at,null);
});

const routeFixture=id=>({id,name:'Synthetic route',date:'2026-10-31',driverId:'TEAM-A',status:'loaded',stops:[{id:'STOP-A',customerName:'Synthetic customer',status:'pending',products:[{id:'P1',name:'Test case',sku:'CASE-A',barcodes:['001234567890'],qty:3,scanLimit:10}]}]});
test('real driver scope, ordered offline events, case-insensitive scans, per-item undo and idempotent replay',()=>{
  const route=routeFixture('TEST-ROUTE'),driver={id:'driver-a',displayName:'Driver A',role:'driver',teamId:'TEAM-A',functions:['deliveries']};
  importRoute(db.sqlite,admin,{requestId:randomUUID(),sourceRevision:'test-source-1',route});assert.throws(()=>driverRoute(db.sqlite,{...driver,teamId:'TEAM-B'},route.id),/not assigned/);
  const event=(kind,payload)=>({id:randomUUID(),kind,payload,recordedAt:new Date().toISOString()});
  const events=[event('depart',{confirmedLoad:true}),event('arrive',{stopId:'STOP-A'}),event('start-picking',{stopId:'STOP-A'}),event('scan',{stopId:'STOP-A',barcode:'case-a'}),event('scan',{stopId:'STOP-A',barcode:'CASE-A'}),event('undo',{stopId:'STOP-A',productId:'P1'}),event('shortage',{stopId:'STOP-A',confirmed:true,items:[{productId:'P1',reason:'Not on vehicle'}]}),event('complete-stop',{stopId:'STOP-A'}),event('finalize',{})];
  const input={requestId:randomUUID(),routeId:route.id,deviceId:'DEVICE-TEST-A',expectedRevision:1,events};const result=saveDriverEvents(db.sqlite,driver,input);
  assert.equal(result.route.snapshot.stops[0].counts.P1,1);assert.equal(result.route.snapshot.status,'delivered');assert.equal(result.route.snapshot.stops[0].officeReview,'pending');
  assert.equal(saveDriverEvents(db.sqlite,driver,input).duplicate,true);assert.equal(db.sqlite.prepare('SELECT COUNT(*) n FROM wh_driver_events WHERE route_id=?').get(route.id).n,events.length);
});
test('invalid scans and incomplete deliveries do not partially save a batch',()=>{
  const r=routeFixture('TEST-BAD-BATCH');r.status='delivering';r.stops[0].status='picking';importRoute(db.sqlite,admin,{requestId:randomUUID(),sourceRevision:'v1',route:r});
  const u={id:'driver-a',displayName:'Driver A',role:'driver',teamId:'TEAM-A',functions:['deliveries']};
  const events=[{id:randomUUID(),kind:'scan',payload:{stopId:'STOP-A',barcode:'CASE-A'},recordedAt:new Date().toISOString()},{id:randomUUID(),kind:'complete-stop',payload:{stopId:'STOP-A'},recordedAt:new Date().toISOString()}];
  assert.throws(()=>saveDriverEvents(db.sqlite,u,{requestId:randomUUID(),routeId:r.id,deviceId:'DEVICE-TEST-A',expectedRevision:1,events}),/remaining products/);assert.equal(driverRoute(db.sqlite,u,r.id).snapshot.stops[0].counts,undefined);
  assert.throws(()=>applyDriverEvent(r,{kind:'scan',payload:{stopId:'STOP-A',barcode:'1234567890'}}),/barcode/);
});
