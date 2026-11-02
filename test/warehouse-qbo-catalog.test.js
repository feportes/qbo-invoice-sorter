import test from 'node:test';import assert from 'node:assert/strict';import {readProductCatalog,productCatalogRefresher} from '../src/warehouse_qbo_catalog.js';
test('full item refresh retains inactive status, reactivations, SKUs and the new Acerola packs',async()=>{
 const report=await readProductCatalog(async query=>{assert.match(query,/Active in \(true,false\)/);return {QueryResponse:{Item:[{Id:'A',Type:'Inventory',Active:false,FullyQualifiedName:'04. Granolas:Natural Granola (Classic) 24 LB',Sku:'G24'},{Id:'B',Type:'Inventory',Active:true,FullyQualifiedName:'04. Granolas:Natural Granola (Classic) 25 LB',Sku:'G25'},{Id:'C',Type:'Inventory',Active:true,FullyQualifiedName:'03. Frozen Fruits:D Raw Acerola Puree Packs (60 X 3.5 OZ)',Sku:'AC60'},{Id:'D',Type:'Category',Active:true,Name:'Frozen Fruits'}]}};});
 const rows=report.reportData.rows.map(r=>Object.fromEntries(r.cells.map(c=>[c.name,c.value])));assert.equal(rows.length,3);assert.match(rows[0]['Product/Service'],/24 LB \(deleted\)$/);assert.doesNotMatch(rows[1]['Product/Service'],/deleted/);assert.equal(rows[2].SKU,'AC60');
});
test('pagination completes before publishing and rejects incomplete responses',async()=>{
 const page=Array.from({length:1000},(_,i)=>({Id:String(i),Type:'Inventory',Active:true,Name:'P'+i}));let reads=0;const report=await readProductCatalog(async q=>{reads++;return {QueryResponse:{Item:q.includes('startposition 1 ')?page:[{Id:'last',Type:'Inventory',Active:true,Name:'Last'}]}};});assert.equal(reads,2);assert.equal(report.reportData.rows.length,1001);
 await assert.rejects(readProductCatalog(async()=>({Fault:{}})),/incomplete/);await assert.rejects(readProductCatalog(async()=>({QueryResponse:{}})),/empty/);await assert.rejects(readProductCatalog(async()=>({QueryResponse:{Item:page}})),/repeated/);
});
test('refreshes coalesce, respect the shared cache, force a fresh read and retry failures',async()=>{
 let reads=0,saves=0,clock=1000;const refresh=productCatalogRefresher(async()=>{reads++;return {reportData:{rows:[1]}};},async()=>{saves++;},{now:()=>clock});await Promise.all([refresh({id:'A'}),refresh({id:'B'})]);assert.equal(reads,1);assert.equal(saves,1);await refresh({id:'A'});assert.equal(reads,1);await refresh({id:'A'},true);assert.equal(reads,2);clock+=300001;await refresh({id:'A'});assert.equal(reads,3);
 let attempts=0;const retry=productCatalogRefresher(async()=>{if(++attempts===1)throw Error('Offline');return {};},async()=>{});await assert.rejects(retry({}),/Offline/);await retry({});assert.equal(attempts,2);
});
