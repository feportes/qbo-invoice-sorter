import test from 'node:test';
import assert from 'node:assert/strict';
import {productScanAction} from '../src/web/product-scan.js';
const products=[{id:'P1',name:'Sorbet',sku:'Acai-3gal',barcodes:[{code:'001234567890'}]},{id:'P2',name:'Mango',sku:'F1114',barcodes:[{code:'99887766'}]}];
test('list scans open an exact UPC or SKU, preserving zeros and ignoring letter case',()=>{
 assert.equal(productScanAction(products,'001234567890').products[0].id,'P1');
 assert.equal(productScanAction(products,'acai-3GAL').products[0].id,'P1');
 assert.equal(productScanAction(products,'Sorbet').kind,'unknown');
});
test('unknown scans have an explicit match action and no invented product identity',()=>{
 const a=productScanAction(products,' 000000000001 ');assert.equal(a.kind,'unknown');assert.equal(a.code,'000000000001');assert.deepEqual(a.products,[]);
 assert.equal(productScanAction(products,'').kind,'empty');assert.equal(productScanAction(products,{}).kind,'empty');
});
test('editor scans fill new aliases without navigating or changing catalog data',()=>{
 const before=structuredClone(products),a=productScanAction(products,'NEW-ALIAS','P1');
 assert.equal(a.kind,'fill');assert.equal(a.code,'NEW-ALIAS');assert.deepEqual(products,before);
 assert.equal(productScanAction(products,'ACAI-3GAL','P1').kind,'already');
});
test('an alias owned by another product is blocked in an open editor',()=>{
 const a=productScanAction(products,'99887766','P1');assert.equal(a.kind,'conflict');assert.equal(a.products[0].id,'P2');
});
test('duplicate aliases never silently pick a product',()=>{
 const ambiguous=[...products,{id:'P3',sku:'OTHER',barcodes:[{code:'001234567890'}]}];
 assert.equal(productScanAction(ambiguous,'001234567890').kind,'ambiguous');
 assert.equal(productScanAction(ambiguous,'001234567890','P1').kind,'conflict');
});
