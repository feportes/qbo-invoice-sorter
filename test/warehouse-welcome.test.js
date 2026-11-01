import test from 'node:test';
import assert from 'node:assert/strict';
import {cycle} from '../src/web/delivery-welcome.js';
test('native delivery cycle opens side door, loads under mist, delivers and returns empty',()=>{
 assert.equal(cycle(0).load,0);
 assert.ok(cycle(1300).door>.99);
 assert.ok(cycle(2100).mist>.99);
 assert.equal(cycle(2700).load,1);
 assert.equal(cycle(4000).walking,true);
 assert.equal(cycle(7600).progress,1);
 assert.equal(cycle(9600).load,0);
 assert.equal(cycle(11000).returning,true);
 assert.equal(cycle(11000).walking,true);
 assert.deepEqual(cycle(16000),cycle(0));
 assert.deepEqual(cycle(5000,false),cycle(0,false));
 for(let t=0;t<32000;t+=33){const s=cycle(t);for(const key of ['progress','load','door','mist','approach','unloading'])if(s[key]!==undefined)assert.ok(s[key]>=0&&s[key]<=1,key+' at '+t);}
});
