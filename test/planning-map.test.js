import test from 'node:test';import assert from 'node:assert/strict';
import {createPlanningMap} from '../src/web/planning-map.js';
class Element{constructor(){this.children=[];this.style={};this.events={};this.open=true;this.checked=true;this.textContent='';}append(...nodes){this.children.push(...nodes);}replaceChildren(){this.children=[];}setAttribute(){}addEventListener(name,fn){this.events[name]=fn;}}
const pause=()=>new Promise(resolve=>setTimeout(resolve,450));
test('planning map refreshes moved stops, retains visibility choices and skips incomplete driving routes',async()=>{
 const markers=[],paths=[],reads=[];globalThis.document={createElement:()=>new Element()};globalThis.window={google:{maps:{Map:class{fitBounds(){}},LatLngBounds:class{extend(){}},Marker:class{constructor(options){markers.push(options);}addListener(){}setMap(){}},Polyline:class{constructor(options){paths.push(options);}setMap(){}},InfoWindow:class{},SymbolPath:{CIRCLE:'circle'},event:{trigger(){}}}}};
 const host=new Element(),choices=new Element(),status=new Element(),count=new Element(),section=new Element(),toggle=new Element();
 const ui=createPlanningMap({host,choices,status,count,section,roadToggle:toggle,readRoad:async id=>{reads.push(id);return {status:'ready',geometry:{coordinates:[[-117,33],[-118,34]]}};}});
 const packet={mapProvider:{ready:true,browserKey:'synthetic'},routes:[{id:'A',name:'Alpha',date:'2026-11-02'},{id:'B',name:'Beta',date:'2026-11-02'}],orders:[{id:'O1',route:'A',stop:1,lat:33,lng:-117,customerName:'One'},{id:'O2',route:'B',stop:1,customerName:'Needs pin'}]};
 ui.update(packet,'2026-11-02');await pause();assert.deepEqual(reads,['A']);assert.equal(markers.length,1);assert.match(status.textContent,/1 of 2 stops mapped/);assert.equal(paths.length,1);
 const check=choices.children[0].children[0];check.checked=false;check.onchange();await pause();assert.match(count.textContent,/1 of 2 routes shown/);
 ui.update({...packet,orders:[{...packet.orders[0],route:'B'},packet.orders[1]]},'2026-11-02');await pause();assert.equal(choices.children[0].children[0].checked,false);assert.equal(markers.at(-1).title.startsWith('Beta'),true);assert.deepEqual(reads,['A']);
 ui.hideAll();await pause();assert.match(count.textContent,/0 of 2 routes shown/);
 delete globalThis.window;delete globalThis.document;
});
