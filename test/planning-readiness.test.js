import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
const source=fs.readFileSync(new URL('../src/web/planning.js',import.meta.url),'utf8');
const sendSource=source.slice(source.indexOf('async function send(r)'),source.indexOf('function rememberCollapse'));
function screen(route){const fields=[],paragraphs=[],saves=[];let confirm;
 const context={pending:null,busy:false,data:{routes:[]},refresh:async()=>{context.data={revision:42,routes:[route]};},dialog:(title,build)=>{confirm=build({append:n=>paragraphs.push(n.text)});},node:(tag,text)=>({text}),input:(form,label)=>{fields.push(label);return {checked:false,value:''};},choose:()=>({value:'freezer'}),save:(...args)=>saves.push(args)};
 vm.createContext(context);vm.runInContext(sendSource,context);
 return {open:r=>context.send(r),fields,paragraphs,saves,confirm:()=>confirm()};
}
test('send refreshes stale readiness and hides override when products have been completed',async()=>{
 const route={id:'R',name:'Route',status:'draft',missingSetup:[]},ui=screen(route);
 await ui.open({...route,missingSetup:[{id:'P'}]});
 assert.equal(ui.fields.length,0);assert.ok(ui.paragraphs.some(p=>p.includes('Ready to send')));
 ui.confirm();assert.equal(ui.saves[0][0],'release');assert.equal(ui.saves[0][1][0],'R');
});
test('send still offers setup override for a genuine missing product',async()=>{
 const r={id:'R',name:'Route',status:'draft',missingSetup:[{id:'P',name:'Product',zone:'freezer',fresh:false,missingBarcode:true}]},ui=screen(r);
 await ui.open(r);assert.ok(ui.fields.includes('Override missing product setup'));assert.ok(ui.fields.includes('Product - fresh produce'));
});
test('a route released elsewhere cannot open a stale send dialog',async()=>{
 const r={id:'R',name:'Route',status:'released',missingSetup:[]},ui=screen(r);
 await assert.rejects(()=>ui.open({...r,status:'draft'}),/no longer a draft/);assert.equal(ui.saves.length,0);
});
