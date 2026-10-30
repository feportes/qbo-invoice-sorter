import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { groupPallets,copyFingerprint } from '../src/pallet_groups.js';
const dir=fs.mkdtempSync(path.join(os.tmpdir(),'pallet-copy-test-'));
process.env.DB_PATH=path.join(dir,'test.db');
const {db}=await import('../src/db.js');
const {ensureSchema,seedDefaults,seedLocations}=await import('../src/schema.js');
ensureSchema();seedDefaults();seedLocations();
const sku=Number(db.sqlite.prepare("INSERT INTO skus (name,unit_type) VALUES ('Acai 20 kg','tubs')").run().lastInsertRowid);
const other=Number(db.sqlite.prepare("INSERT INTO skus (name,unit_type) VALUES ('Passion fruit sorbet','tubs')").run().lastInsertRowid);
const lot=Number(db.sqlite.prepare("INSERT INTO lots (sku_id,lot_number) VALUES (?,'EXISTING-LOT')").run(sku).lastInsertRowid);
const source=db.createPallet({skuId:sku,locationCode:'C2-L-09',qtyUnits:140,lotId:lot,notes:'Source-only note'});
const copy=(id,slots,qty)=>db.copyPallets({sourcePalletId:id,locationCodes:slots,qtyUnits:qty,expectedItems:copyFingerprint(db.physicalPalletItems(id)),userName:'Murillo'});

test('copy receives matching product into multiple empty slots without changing the source',()=>{
 const ids=copy(source,['C2-L-08','C2-L-07'],140);
 assert.equal(ids.length,2);assert.notEqual(ids[0],source);
 for(const id of ids){const p=db.physicalPalletItems(id)[0];assert.equal(p.sku_id,sku);assert.equal(p.lot_id,lot);assert.equal(p.qty_units,140);assert.equal(p.pallet_tag,null);assert.equal(p.notes,null);}
 assert.equal(db.physicalPalletItems(source)[0].qty_units,140);
 assert.equal(copy(source,['C2-L-06'],120).length,1);
 assert.equal(db.listPalletsInContainer(2).find(p=>p.location_code==='C2-L-06').qty_units,120);
});
test('copy is atomic and rejects occupied, duplicate, invalid and stale destinations',()=>{
 const before=db.sqlite.prepare('SELECT count(*) AS n FROM pallets').get().n;
 assert.throws(()=>copy(source,['C2-R-09','C2-L-08'],140),/occupied/);
 assert.equal(db.sqlite.prepare('SELECT count(*) AS n FROM pallets').get().n,before);
 assert.throws(()=>copy(source,['C2-R-09','C2-R09'],140),/only once/);
 assert.throws(()=>copy(source,['C2-R-11'],140),/outside/);
 assert.throws(()=>copy(source,['WALKIN'],140),/container slots/);
 assert.throws(()=>copy(source,['C2-R-09'],0),/greater than zero/);
 assert.throws(()=>db.copyPallets({sourcePalletId:source,locationCodes:['C2-R-09'],qtyUnits:140,expectedItems:'stale'}),/changed/);
});
let mixed;
test('mixed products share one physical pallet and keep independent quantities',()=>{
 mixed=db.createMixedPallet({locationCode:'C5-L-09',items:[{sku_id:sku,qty_units:54},{sku_id:other,qty_units:110}]});
 assert.equal(mixed.length,2);
 const rows=db.listPalletsInContainer(5);assert.equal(rows.length,2);
 const groups=groupPallets(rows);assert.equal(groups.length,1);assert.equal(groups[0].items.length,2);
 assert.ok(rows[0].pallet_group);assert.equal(rows[0].pallet_group,rows[1].pallet_group);
 assert.throws(()=>db.createPallet({skuId:sku,locationCode:'C5-L-09',qtyUnits:5}),/occupied/);
});
test('mixed entry rolls back every line if any product or quantity is invalid',()=>{
 const before=db.sqlite.prepare('SELECT count(*) AS n FROM pallets').get().n;
 assert.throws(()=>db.createMixedPallet({locationCode:'C5-R-09',items:[{sku_id:sku,qty_units:10},{sku_id:other,qty_units:-2}]}),/> 0/);
 assert.equal(db.sqlite.prepare('SELECT count(*) AS n FROM pallets').get().n,before);
});
test('copying a mixed pallet copies all products as a new independent physical pallet',()=>{
 const ids=copy(mixed[0],['C5-L-08','C5-L-07']);
 assert.equal(ids.length,2);
 const first=db.physicalPalletItems(ids[0]);assert.equal(first.length,2);
 assert.deepEqual(first.map(p=>p.qty_units),[54,110]);
 assert.notEqual(first[0].pallet_group,db.physicalPalletItems(mixed[0])[0].pallet_group);
 assert.equal(groupPallets(db.listPalletsInContainer(5)).length,3);
});
test('moving one mixed product moves the physical pallet; breaking only changes that product',()=>{
 const dest=db.getLocationByCode('WALKIN');db.movePallet(mixed[1],dest.id);
 assert.ok(db.physicalPalletItems(mixed[0]).every(p=>p.location_id===dest.id));
 db.movePallet(mixed[0],db.getLocationByCode('C5-R-09').id);
 db.breakPalletToWalkin({palletId:mixed[0],qty:54});
 const active=db.physicalPalletItems(mixed[1]);assert.equal(active.length,1);assert.equal(active[0].qty_units,110);
 assert.throws(()=>db.createPallet({skuId:sku,locationCode:'C5-R-09',qtyUnits:20}),/occupied/);
 db.movePallet(mixed[0],dest.id);
 assert.ok(db.physicalPalletItems(mixed[1],true).every(p=>p.location_id===dest.id));
});
test('existing schema migration is idempotent and old single pallets retain their data',()=>{
 ensureSchema();assert.equal(db.sqlite.prepare('PRAGMA table_info(pallets)').all().filter(c=>c.name==='pallet_group').length,1);
 assert.equal(db.physicalPalletItems(source)[0].notes,'Source-only note');
});
test.after(()=>{db.sqlite.close();fs.rmSync(dir,{recursive:true,force:true});});
