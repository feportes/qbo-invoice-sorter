import test from 'node:test';import assert from 'node:assert/strict';
import {productSetup,missingSetup,setupSummary,productEditValues} from '../src/web/product-setup.js';
test('picking readiness accepts internal SKUs and keeps optional manufacturer barcodes separate',()=>{
 const p={id:'P',sku:'D1122',zone:'dry',barcodes:[{code:'D1122'}]};
 assert.deepEqual(missingSetup(p),[]);assert.equal(missingSetup(p,'alternateBarcode').length,1);
 assert.equal(setupSummary([p]).ready,1);assert(missingSetup(p,'all').some(x=>x.id==='photo'));
 p.barcodes.push({code:'000123456789'});assert.equal(missingSetup(p,'alternateBarcode').length,0);
});
test('checklist counts recalculate from saved fields without a separate completion flag',()=>{
 const p={id:'P',name:'Acai',sku:' ',zone:'',barcodes:[{code:' '}]};
 assert.deepEqual(missingSetup(p).map(x=>x.id),['scanCode','storage']);
 p.barcodes=[{code:'001234'}];assert.equal(setupSummary([p]).missing.scanCode,0);assert.equal(setupSummary([p]).ready,0);
 p.zone='freezer';assert.equal(setupSummary([p]).ready,1);
 Object.assign(p,{sku:'A1',category:'01. Sorbets',weight:'3 gal',description:'Frozen acai',photo:{id:'photo-1'}});
 assert.equal(setupSummary([p]).complete,1);
 p.photo=null;assert.equal(setupSummary([p]).complete,0);assert.equal(setupSummary([p]).ready,1);
 p.barcodes=[];p.sku='';assert.equal(setupSummary([p]).ready,0);
});
test('empty and uncategorized records remain visible as incomplete',()=>{
 assert.equal(productSetup({photo:{},category:'Uncategorized',barcodes:[]}).filter(x=>x.present).length,0);
 assert.deepEqual(setupSummary([]),{total:0,ready:0,complete:0,missing:{scanCode:0,storage:0,sku:0,photo:0,weight:0,description:0,category:0,alternateBarcode:0}});
});

test('Save and next sends chosen storage and fresh flag with product details while excluding the read-only SKU',()=>{const fields=[['name','Acai'],['sku','READ-ONLY'],['weight','3 gal'],['description','Frozen sorbet']];assert.deepEqual(productEditValues(fields,'freezer',false),{name:'Acai',weight:'3 gal',description:'Frozen sorbet',zone:'freezer',fresh:false});assert.equal(productEditValues(fields,'dry',true).fresh,true);assert.equal(Object.hasOwn(productEditValues(fields,'',false),'zone'),false);});
