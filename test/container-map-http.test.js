import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import net from 'node:net';
const dir=fs.mkdtempSync(path.join(os.tmpdir(),'container-map-http-'));
process.env.DB_PATH=path.join(dir,'app.db');
const { db }=await import('../src/db.js');
const { ensureSchema, seedDefaults, seedLocations }=await import('../src/schema.js');
ensureSchema();seedDefaults();seedLocations();
db.upsertConnection({realm_id:'test',company_name:'Local test only',access_token:'not-a-real-token',refresh_token:'not-a-real-token',expires_at:Date.now()+3600000,refresh_expires_at:null});
const sku=Number(db.sqlite.prepare("INSERT INTO skus (name,unit_type) VALUES ('D Raw Acai Sorbet','tubs')").run().lastInsertRowid);
const id=db.createPallet({skuId:sku,locationCode:'C2-L-09',qtyUnits:140});
const free=net.createServer();await new Promise(r=>free.listen(0,'127.0.0.1',r));const port=free.address().port;await new Promise(r=>free.close(r));
const child=spawn(process.execPath,['server.js'],{env:{...process.env,PORT:String(port)},stdio:['ignore','pipe','pipe']});
let log='';child.stdout.on('data',b=>log+=b);child.stderr.on('data',b=>log+=b);
const base=`http://127.0.0.1:${port}`;
async function post(url,body,json=false){return fetch(base+url,{method:'POST',redirect:'manual',headers:{'Content-Type':json?'application/json':'application/x-www-form-urlencoded'},body:json?JSON.stringify(body):new URLSearchParams(body)});}

test.before(async()=>{
 for(let i=0;i<100;i++){try{const r=await fetch(base);if(r.ok)return;}catch{}if(child.exitCode!==null)throw new Error(log);await new Promise(r=>setTimeout(r,40));}
 throw new Error('Server did not start: '+log);
});
test('renders shared yard, details, settings, walk-in, add, count and loading notes',async()=>{
 for(const route of ['/inventory/yard','/inventory/map?c=2','/inventory/settings/containers','/inventory/walkin','/inventory/add-pallet?c=2','/inventory/loading-notes',`/inventory/pallet/${id}`]){
  const r=await fetch(base+route);const text=await r.text();assert.equal(r.status,200,route+': '+text.slice(0,150));assert.ok(!text.includes('ReferenceError'));
 }
 const yard=await (await fetch(base+'/inventory/yard')).text();
 assert.ok(yard.indexOf('C2-L-09')<yard.indexOf('C2-L-01'));
 assert.match(yard,/D Raw Acai Sorbet/);
 const add=await(await fetch(base+'/inventory/add-pallet?c=2&slot=C2-R-09&note=110%20tubs&product=Sorbet')).text();
 assert.match(add,/id="location_code" value="C2-R-09"/);
 assert.match(add,/110 tubs/);
});
test('historic duplicate and out-of-layout stock remains visible for review',async()=>{
 const location=db.getLocationByCode('C3-L-11');
 db.sqlite.prepare("INSERT INTO pallets (sku_id,location_id,qty_units) VALUES (?,?,?)").run(sku,location.id,37);
 const r=await fetch(base+'/inventory/yard');const html=await r.text();
 assert.equal(r.status,200);assert.match(html,/Location review needed/);assert.match(html,/37 tubs/);
});
test('legacy move fields and break-to-walkin route work',async()=>{
 const r=await post('/inventory/move-json',{pallet_id:id,to_slot:'C2-R-09'},true);assert.equal(r.status,200);assert.equal((await r.json()).ok,true);
 const r2=await post('/inventory/break-to-walkin',{pallet_id:id,qty:30,container_no:2});assert.equal(r2.status,302);assert.match(r2.headers.get('location'),/^\/inventory\/map\?c=2&msg=/);
 assert.equal(db.sqlite.prepare('SELECT qty_units FROM pallets WHERE id=?').get(id).qty_units,110);
 const r3=await post('/inventory/move',{pallet_id:id,to_slot:'WALKIN',redirect_to:'/inventory/yard'});assert.equal(r3.status,302);assert.match(r3.headers.get('location'),/^\/inventory\/yard\?msg=Pallet/);
});
test('count endpoint records the operator and rejects stale notes',async()=>{
 const p=db.sqlite.prepare('SELECT * FROM pallets WHERE id=?').get(id);
 const body={qty_units:103,expected_qty:p.qty_units,expected_location:p.location_id,expected_notes:p.notes||'',notes:'Warehouse count',user_name:'Murillo'};
 const r=await post('/inventory/pallet/'+id,body);assert.equal(r.status,302);
 assert.match(decodeURIComponent(r.headers.get('location')),/Pallet count saved/);
 const stale=await post('/inventory/pallet/'+id,{...body,expected_qty:103});
 assert.match(decodeURIComponent(stale.headers.get('location')),/This pallet changed/);
 assert.equal(db.sqlite.prepare('SELECT qty_units FROM pallets WHERE id=?').get(id).qty_units,103);
});
test('settings save retains the selected longer side and rejects invalid values',async()=>{
 const r=await post('/inventory/settings/containers',{mode_C6:'20-slot',flip_C6:'R_LONG'});assert.equal(r.status,302);assert.equal(db.getContainerDepths(6).rightMax,11);
 const bad=await post('/inventory/settings/containers',{mode_C6:'bogus'});assert.match(decodeURIComponent(bad.headers.get('location')),/Save failed/);assert.equal(db.getContainerDepths(6).rightMax,11);
});
test('mixed entry and batch-copy endpoints preserve product lines and reject duplicate stock',async()=>{
 const second=Number(db.sqlite.prepare("INSERT INTO skus (name,unit_type) VALUES ('Sorbet second flavor','tubs')").run().lastInsertRowid);
 const received=await post('/inventory/add-pallet',{sku_id:sku,qty_units:20,location_code:'C7-L-09',containerNo:7,'extra_items[0][sku_id]':second,'extra_items[0][qty_units]':30,after_add:'copy'});
 assert.equal(received.status,302);assert.match(received.headers.get('location'),/yard\?copy=/);
 const mixed=db.listPalletsInContainer(7).filter(p=>p.location_code==='C7-L-09');assert.equal(mixed.length,2);
 const {copyFingerprint}=await import('../src/pallet_groups.js');
 const body={source_pallet_id:mixed[0].id,expected_items:copyFingerprint(mixed),location_codes:['C7-L-08','C7-L-07']};
 const pasted=await post('/inventory/copy-pallets',body,true);assert.equal(pasted.status,200);assert.equal((await pasted.json()).count,2);
 const repeat=await post('/inventory/copy-pallets',body,true);assert.equal(repeat.status,400);assert.match((await repeat.json()).error,/occupied/);
 const html=await(await fetch(base+'/inventory/yard')).text();assert.match(html,/Mixed pallet/);assert.match(html,/Copy pallet/);assert.match(html,/Sorbet second flavor/);
 const details=await(await fetch(base+'/inventory/map?c=7')).text();assert.match(details,/Mixed pallet - additional products/);
});
test.after(async()=>{child.kill();await new Promise(r=>child.exitCode!==null?r():child.once('exit',r));db.sqlite.close();fs.rmSync(dir,{recursive:true,force:true});});
