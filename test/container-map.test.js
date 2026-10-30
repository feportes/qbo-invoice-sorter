import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { containerLayout } from '../src/container_layout.js';
import { loadingNotes } from '../src/loading_notes.js';
const dir=fs.mkdtempSync(path.join(os.tmpdir(),'container-map-test-'));
process.env.DB_PATH=path.join(dir,'test.db');
const { db }=await import('../src/db.js');
const { ensureSchema, seedDefaults, seedLocations }=await import('../src/schema.js');
ensureSchema();seedDefaults();seedLocations();
const sku=Number(db.sqlite.prepare("INSERT INTO skus (name,unit_type) VALUES ('D Raw Acai Sorbet','tubs')").run().lastInsertRowid);
const receive=(code,qty=140)=>db.createPallet({skuId:sku,locationCode:code,qtyUnits:qty});
const loc=code=>db.getLocationByCode(code);

test('preserves default layouts and legacy settings',()=>{
 assert.equal(containerLayout(1).leftMax,4);
 assert.equal(containerLayout(1).rightMax,4);
 assert.equal(containerLayout(2,'L','0').mode,'20-slot');
 assert.equal(containerLayout(2,'20-slot','L_LONG').leftMax,11);
 assert.equal(containerLayout(2,'20-slot','R_LONG').rightMax,11);
 assert.equal(containerLayout(1,'9-slot','R_LONG').rightMax,5);
 assert.throws(()=>containerLayout(2,'8-slot'));
});
test('layout changes retain stock and atomically reject hidden pallets',()=>{
 db.saveContainerLayouts([{containerNo:2,mode:'20-slot',flip:'L_LONG'}]);
 const id=receive('C2-L-11');
 assert.equal(db.listValidSlotCodes(2).length,20);
 assert.throws(()=>db.saveContainerLayouts([{containerNo:3,mode:'20-slot',flip:'R_LONG'},{containerNo:2,mode:'18-slot',flip:'L_LONG'}]),/Move pallets/);
 assert.equal(db.getContainerDepths(3).mode,'18-slot');
 assert.throws(()=>db.saveContainerLayouts([{containerNo:2,mode:'20-slot',flip:'R_LONG'}]),/Move pallets/);
 db.movePallet(id,loc('WALKIN').id);
 db.saveContainerLayouts([{containerNo:2,mode:'20-slot',flip:'R_LONG'}]);
 assert.equal(db.getContainerDepths(2).rightMax,11);
 assert.equal(db.sqlite.prepare('SELECT location_id FROM pallets WHERE id=?').get(id).location_id,loc('WALKIN').id);
});
test('receive and move reject occupied or disabled layout slots, including aliases',()=>{
 const a=receive('C3-L01');const b=receive('C3-R-01');
 assert.throws(()=>receive('C3-L-01'),/occupied/);
 assert.throws(()=>db.movePallet(b,loc('C3-L-01').id),/occupied/);
 assert.throws(()=>receive('C3-R-11'),/outside/);
 assert.throws(()=>db.movePallet(a,loc('C3-R-11').id),/outside/);
 assert.equal(db.getLocationByCode('C3-L01').id,loc('C3-L-01').id);
});
test('breaking a pallet preserves loose stock and frees a depleted slot',()=>{
 const id=receive('C4-L-01',110);
 db.breakPalletToWalkin({palletId:id,qty:110,userName:'Murillo'});
 assert.equal(db.listWalkinLoose().find(p=>p.sku_id===sku).qty_units,110);
 assert.equal(db.listPalletsInContainer(4).length,0);
 assert.doesNotThrow(()=>receive('C4-L-01',120));
});
test('physical count audit and stale-screen protection preserve concurrent changes',()=>{
 const id=receive('C5-L-01',56);const location=loc('C5-L-01');
 db.updatePalletCount({palletId:id,qtyUnits:54,expectedQty:56,expectedLocation:location.id,notes:'Short two',userName:'Murillo'});
 const history=db.sqlite.prepare("SELECT * FROM inventory_movements WHERE type='COUNT_ADJUSTMENT' AND to_pallet_id=?").get(id);
 assert.equal(history.qty_units,-2);assert.equal(history.user_name,'Murillo');
 assert.throws(()=>db.updatePalletCount({palletId:id,qtyUnits:55,expectedQty:56,expectedLocation:location.id,userName:'Felipe'}),/changed/);
 assert.throws(()=>db.updatePalletCount({palletId:id,qtyUnits:54,expectedQty:54,expectedLocation:location.id,expectedNotes:'',userName:'Felipe'}),/changed/);
 db.movePallet(id,loc('WALKIN').id);
 assert.throws(()=>db.updatePalletCount({palletId:id,qtyUnits:55,expectedQty:54,expectedLocation:location.id,userName:'Felipe'}),/changed/);
 assert.throws(()=>db.updatePalletCount({palletId:id,qtyUnits:-1,expectedNotes:'Short two',expectedQty:54,expectedLocation:loc('WALKIN').id,userName:'Felipe'}),/zero or more/);
});
test('loading notes preserve partial counts without inventing full-pallet quantities',()=>{
 assert.equal(loadingNotes.filter(p=>p.container===2).reduce((sum,p)=>sum+p.quantity,0),1240);
 assert.equal(loadingNotes.filter(p=>p.container===5).length,17);
 assert.equal(loadingNotes.filter(p=>p.container===6).length,10);
 assert.equal(loadingNotes.find(p=>p.container===5&&p.side==='L'&&p.fromBack===8).quantity,54);
 assert.equal(loadingNotes.find(p=>p.container===6&&p.side==='L'&&p.fromBack===1).quantity,103);
 assert.equal(loadingNotes.filter(p=>p.product==='Unsweetened acai bricks').length,0);
 assert.equal(loadingNotes.find(p=>p.product==='Alohana').quantity,null);
});
test.after(()=>{db.sqlite.close();fs.rmSync(dir,{recursive:true,force:true});});
