import test from 'node:test';import assert from 'node:assert/strict';import {palletPattern,palletSVG,palletPacking} from '../src/web/pallet-visual.js';import {decoratePalletLayout} from '../src/warehouse_pallet_visuals.js';
test('category layer icons distinguish sorbets, fruit boxes, large puree tubs and mixed stock',()=>{
 const row=(name,category)=>({sku_name:name,category,qty_units:1});
 assert.equal(palletPattern(null),'empty');assert.equal(palletPattern(row('Acai','Sorbet')),'tubs20');assert.equal(palletPattern(row('Acai puree 18 KG','Frozen Fruits')),'tubs12');assert.equal(palletPattern(row('Mango','Frozen Fruits')),'boxes10');assert.equal(palletPattern(row('Vanilla','Gelato')),'boxes10');assert.equal(palletPattern({items:[row('Acai','Sorbet'),row('Mango','Frozen Fruits')]}),'mixed');assert.equal(palletPattern(row('Unknown','')),'stock');
 assert.equal((palletSVG(row('Acai','Sorbet')).match(/fill="#f7f7f2"/g)||[]).length,20);assert.equal((palletSVG(row('Acai 18 kg','Frozen Fruits')).match(/fill="#f7f7f2"/g)||[]).length,12);assert.equal((palletSVG(row('Mango','Frozen Fruits')).match(/fill="#bb8754"/g)||[]).length,10);assert.match(palletSVG(null,'sideways'),/viewBox="0 0 120 100"/);assert.doesNotMatch(palletSVG(row('<script>','Sorbet')),/<script>/);
});

test('frozen fruit case dimensions distinguish 20 lb and 30 lb layer heights without changing other packs',()=>{
 for(const [weight,height] of [[20,7],[30,9]]){const p=palletPacking({sku_name:'Frozen mango dices - '+weight+' lb',category:'03. Frozen Fruits',qty_units:23});assert.equal(p.unitsPerLayer,10);assert.equal(p.caseLengthInches,16);assert.equal(p.caseWidthInches,11);assert.equal(p.caseHeightInches,height);assert.equal(p.layers,3);assert.equal(p.fullLayers,2);assert.equal(p.topLayerUnits,3);assert.equal(p.loadHeightExcludingPalletInches,height*3);}
 assert.equal(palletPacking({sku_name:'Vanilla Gelato 20 lb',category:'Gelato',qty_units:20}).layerHeightInches,null);
 assert.equal(palletPacking({sku_name:'Frozen fruit unknown weight',category:'Frozen Fruits',qty_units:20}).layerHeightInches,null);
});
test('category decoration preserves stock values and fingerprints without mutating the source layout',()=>{const original={yard:[{left:[{pallet:{items:[{sku_id:2,qty_units:73}],copy_fingerprint:'keep'}}],right:[]}],walkin:[]},sql={prepare:()=>({all:()=>[{id:2,category:'Sorbet'}]})},out=decoratePalletLayout(sql,original);assert.equal(out.yard[0].left[0].pallet.items[0].category,'Sorbet');assert.equal(out.yard[0].left[0].pallet.items[0].qty_units,73);assert.equal(out.yard[0].left[0].pallet.copy_fingerprint,'keep');assert.equal(original.yard[0].left[0].pallet.items[0].category,undefined);});
test('compact box icon uses three plus three long cases and four turned cases',()=>{const svg=palletSVG({sku_name:'Frozen mango',category:'Frozen Fruits'});assert.match(svg,/width="48" height="58"/);assert.equal((svg.match(/width="27.5" height="36"/g)||[]).length,6);assert.equal((svg.match(/width="34" height="27"/g)||[]).length,4);assert.match(palletSVG(null,'sideways'),/width="58" height="48"/);});

test('Oakberry uses 28 purple rectangular tubs, including a turned row, without changing round sorbets',()=>{
 const item={sku_name:'Oakberry Organic Acai Sorbet 7 Kg.',category:'02. Sorbets',qty_units:336},p=palletPacking(item),svg=palletSVG(item);
 assert.equal(p.kind,'oakberry28');assert.equal(p.unitsPerLayer,28);assert.equal(p.layers,12);assert.equal(p.topLayerUnits,0);assert.equal(p.layerHeightInches,null);
 assert.equal((svg.match(/data-oakberry-tub="true"/g)||[]).length,28);
 assert.equal((svg.match(/width="14.6" height="22.1"/g)||[]).length,24);
 assert.equal((svg.match(/width="22.1" height="19.6"/g)||[]).length,4);
 assert.match(svg,/28 purple rectangular Oakberry tubs/);assert.equal(palletPacking({...item,qty_units:29}).layers,2);
 assert.equal(palletPattern({sku_name:'D Raw Acai Sorbet 3 Gal',category:'Sorbets'}),'tubs20');
});

test('confirmed case and rectangular-tub profiles calculate complete and partial layers without guessing height',()=>{
 for(const sku_name of ['D’ RAW Acai Packs','Acerola Packs','Pitaya Pack']){const p={sku_name,category:'Frozen Fruits',qty_units:168};assert.equal(palletPattern(p),'whiteBoxes12');assert.equal(palletPacking(p).layers,14);assert.equal(palletPacking(p).topLayerUnits,0);assert.equal((palletSVG(p).match(/fill="#fffdfa"/g)||[]).length,12);}
 for(const sku_name of ['Juice Crafters Acai Sorbet','Brazilian Blend Sorbet']){const p={sku_name,category:'Sorbet',qty_units:105},full=palletPacking(p),partial=palletPacking({...p,qty_units:94});assert.equal(full.unitsPerLayer,15);assert.equal(full.fullPalletUnits,105);assert.equal(full.layers,7);assert.equal(partial.fullLayers,6);assert.equal(partial.topLayerUnits,4);assert.equal(partial.layers,7);assert.equal(partial.layerHeightInches,null);assert.equal((palletSVG(p).match(/fill="#fffdfa"/g)||[]).length,15);}
 assert.equal(palletPattern({sku_name:'Oakberry Acai Packs',category:'Frozen Fruits'}),'boxes10');assert.equal(palletPattern({sku_name:'Acai Puree 18 kg Packs',category:'Frozen Fruits'}),'tubs12');
 const round=palletPacking({sku_name:'D RAW Acai Sorbet 3 Gal',category:'Sorbet',qty_units:48});assert.equal(round.diameterInches,10);assert.equal(round.layerHeightInches,10);assert.equal(round.layers,3);assert.equal(round.loadHeightExcludingPalletInches,30);assert.equal(round.dimensionsApproximate,true);assert.equal(palletPacking({sku_name:'Acai 18 kg',category:'Frozen Fruits',qty_units:12}).layerHeightInches,null);
});
