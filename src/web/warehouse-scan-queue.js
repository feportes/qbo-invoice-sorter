/** Persist each trigger before accepting it. Server requests stay strictly ordered. */
export class WarehouseScanQueue {
 constructor(storage,key){this.storage=storage;this.key=key;this.items=JSON.parse(storage.getItem(key)||'[]');if(!Array.isArray(this.items))throw Error('Saved scans could not be read.');}
 write(items){this.storage.setItem(this.key,JSON.stringify(items));this.items=items;}
 add(item){if(this.items.length>=500)throw Error('500 scans are waiting. Allow them to sync before continuing.');this.write([...this.items,item]);}
 remove(id){this.write(this.items.filter(item=>item.id!==id));}
 get first(){return this.items[0];}
 get length(){return this.items.length;}
}
