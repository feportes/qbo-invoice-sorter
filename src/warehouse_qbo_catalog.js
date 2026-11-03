import {physicalServiceSupply} from './warehouse-product-policy.js';
export async function readProductCatalog(query){
 const items=[],seen=new Set();let complete=false;
 for(let start=1;start<=20001;start+=1000){
  const response=await query(`select * from Item where Active in (true,false) startposition ${start} maxresults 1000`);
  if(!response?.QueryResponse||response.Fault)throw Error('QuickBooks product refresh was incomplete. Previous catalog retained.');
  const page=response.QueryResponse.Item||[];if(!Array.isArray(page))throw Error('QuickBooks returned an invalid product list.');
  for(const item of page){if(!item.Id||seen.has(String(item.Id)))throw Error('QuickBooks returned repeated product pages. Previous catalog retained.');seen.add(String(item.Id));items.push(item);}
  if(page.length<1000){complete=true;break;}
 }
 if(!complete||items.length>20000)throw Error('QuickBooks product list exceeds the refresh limit. Previous catalog retained.');
 const types={Inventory:'ITEM_INVENTORY',NonInventory:'NONINVENTORY',Service:'SERVICE'};
 const rows=items.filter(p=>types[p.Type]).map(p=>{let name=p.FullyQualifiedName||p.Name;if(!name||typeof p.Active!=='boolean')throw Error('QuickBooks product identity or active status is missing.');if(!p.Active&&!/\(deleted[^)]*\)/i.test(name))name+=' (deleted)';return {cells:Object.entries({'Product/Service':name,Type:types[p.Type],SKU:p.Sku??'', 'Memo/Description':p.Description||'', 'Sales Price':p.UnitPrice??null,'Purchase Price':p.PurchaseCost??null,'Qty on Hand':p.QtyOnHand??null}).map(([name,value])=>({name,value}))};});
 if(!rows.length)throw Error('QuickBooks returned an empty product list. Previous catalog retained.');
 return {reportData:{rows},inventoryItems:items.filter(p=>['Inventory','NonInventory'].includes(p.Type)||physicalServiceSupply(p)).map(p=>({id:String(p.Id),name:p.Name,sku:p.Sku||'',categoryId:p.ParentRef?.value||null,active:p.Active}))};
}
export function syncWarehouseProductCatalog(sql,items){
 if(!Array.isArray(items)||!items.length)throw Error('Warehouse product refresh returned no items.');
 const put=sql.prepare(`INSERT INTO skus(qbo_item_id,name,qbo_sku,qbo_category_id,unit_type,active,is_organic,is_lot_tracked) VALUES(?,?,?,?,'unit',?,0,0) ON CONFLICT(qbo_item_id) DO UPDATE SET name=excluded.name,qbo_sku=excluded.qbo_sku,qbo_category_id=excluded.qbo_category_id,active=excluded.active`);
 sql.transaction(()=>{for(const p of items){if(!p.id||!p.name||typeof p.active!=='boolean')throw Error('Warehouse product identity is incomplete.');put.run(p.id,p.name,p.sku||null,p.categoryId||null,p.active?1:0);}})();
}
export function productCatalogRefresher(read,persist,{now=Date.now,ttl=300000}={}){
 let lastRead=0,inflight;
 return async(user,force=false)=>{if(!read||!force&&lastRead&&now()-lastRead<ttl)return;if(inflight)return inflight;inflight=(async()=>{const report=await read();await persist(user,report);lastRead=now();})();try{return await inflight;}finally{inflight=null;}};
}
