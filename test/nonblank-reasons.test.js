import test from 'node:test';
import assert from 'node:assert/strict';
import {applyDriverEvent} from '../src/web/driver-state.js';
test('synced driver notes accept a single nonblank character and reject whitespace',()=>{
 for(const notes of ['x','OK',' N/A '])assert.doesNotThrow(()=>applyDriverEvent({status:'delivering',stops:[]},{kind:'note',payload:{notes}}));
 for(const notes of ['', '  ', '\t\n','x'.repeat(1001)])assert.throws(()=>applyDriverEvent({status:'delivering',stops:[]},{kind:'note',payload:{notes}}),/Explain/);
});
