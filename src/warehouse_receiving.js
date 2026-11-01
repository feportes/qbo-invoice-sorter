import {randomUUID} from 'node:crypto';
import {audit,digest,problem,permit} from './warehouse_auth.js';
const now=()=>new Date().toISOString();
const round=n=>Math.round(n*1e6)/1e6;
const quantity=(n,label,allowZero=true)=>{if(typeof n!=='number'||!Number.isFinite(n)||n<(allowZero?0:0.000001)||n>1000000)problem('Review '+label+'.');return round(n);};
const text=(v,label,max=200)=>{v=String(v||'').trim();if(!v||v.length>max)problem('Review '+label+'.');return v;};
export const readPO=(sql,id)=>{const p=sql.prepare('SELECT * FROM wh_purchase_orders WHERE id=?').get(id);if(!p)problem('Purchase order not found.',404);return {...p,lines:JSON.parse(p.lines)};};
export const readReceipt=(sql,id)=>{const r=sql.prepare('SELECT * FROM wh_receipts WHERE id=?').get(id);if(!r)problem('Receipt not found.',404);return {...r,lines:JSON.parse(r.lines)};};
export function once(sql,user,requestId,payload,work){
  if(!/^[A-Za-z0-9_-]{8,100}$/.test(requestId||''))problem('A valid save identifier is required.');
  const signature=digest(JSON.stringify(payload));return sql.transaction(()=>{
    const old=sql.prepare('SELECT * FROM wh_idempotency WHERE id=?').get(requestId);
    if(old){if(old.user_id!==user.id||old.digest!==signature)problem('Save identifier was reused with different contents.',409);return {...JSON.parse(old.result),duplicate:true};}
    const result=work();sql.prepare('INSERT INTO wh_idempotency VALUES (?,?,?,?,?)').run(requestId,user.id,signature,JSON.stringify(result),now());return result;
  })();
}
export function createPurchaseOrder(db,user,input,directReceipt=false){
  permit(user,directReceipt?['admin','office','warehouse']:['admin','office'],'receiving');const sql=db.sqlite;
  const vendorId=text(input.vendorId,'the vendor account',100),vendorName=text(input.vendorName,'the vendor name'),poNumber=text(input.poNumber,'the PO number',100);
  if(!Array.isArray(input.lines)||!input.lines.length||input.lines.length>200)problem('Add between 1 and 200 product lines.');
  const seen=new Set(),lines=input.lines.map((x,index)=>{const id=text(x.id||'LINE-'+(index+1),'the line ID',100);if(seen.has(id))problem('PO line IDs must be unique.');seen.add(id);const sku=sql.prepare('SELECT * FROM skus WHERE id=? AND active=1').get(Number(x.skuId));if(!sku)problem('Choose an active product.');if(!['frozen','dry','refrigerated'].includes(x.area))problem('Choose the product storage area.');return {id,skuId:sku.id,qboItemId:sku.qbo_item_id||null,name:sku.name,unit:sku.unit_type,orderedQty:quantity(x.orderedQty,'ordered quantity',false),area:x.area};});
  return once(sql,user,input.requestId,{vendorId,vendorName,poNumber,lines,qboPoId:input.qboPoId||null,expectedDate:input.expectedDate||null},()=>{const id=randomUUID(),at=now();sql.prepare('INSERT INTO wh_purchase_orders VALUES (?,?,?,?,?,?,?,?,?,?,?,?)').run(id,vendorId,vendorName,input.qboPoId||null,poNumber,input.expectedDate||null,'open',1,JSON.stringify(lines),user.id,at,at);audit(sql,user,'po-created','po',id,{vendorId,poNumber,lines});return {purchaseOrder:readPO(sql,id)};});
}
export function recordReceipt(db,user,input){
  permit(user,['admin','office','warehouse'],'receiving');const sql=db.sqlite;
  if(!['warehouse','vendor-pickup'].includes(input.mode))problem('Choose warehouse receiving or vendor pickup.');
  if(!Array.isArray(input.lines)||!input.lines.length||input.lines.length>200)problem('Review received product lines.');
  return once(sql,user,input.requestId,input,()=>{
    const po=readPO(sql,input.poId);if(po.status==='closed'||po.revision!==input.expectedPORevision)problem('The PO changed. Refresh before receiving.',409);
    const previous=sql.prepare("SELECT lines FROM wh_receipts WHERE po_id=? AND status<>'cancelled'").all(po.id).flatMap(r=>JSON.parse(r.lines)),seen=new Set();
    const lines=input.lines.map(x=>{
      if(seen.has(x.poLineId))problem('Receive each PO line once per receipt.');seen.add(x.poLineId);
      const p=po.lines.find(p=>p.id===x.poLineId);if(!p)problem('Received item is not on this PO.');
      const receivedQty=quantity(x.receivedQty,'received quantity',false),damagedQty=quantity(x.damagedQty??0,'damaged quantity'),rejectedQty=quantity(x.rejectedQty??0,'rejected quantity');
      if(damagedQty+rejectedQty>receivedQty)problem('Damage and rejected quantities exceed what arrived.');
      const goodQty=round(receivedQty-damagedQty-rejectedQty),accepted=previous.filter(l=>l.poLineId===p.id).reduce((s,l)=>s+l.goodQty,0),remaining=Math.max(0,p.orderedQty-accepted);
      const reason=String(x.exceptionReason||'').trim();if((damagedQty||rejectedQty||goodQty>remaining+0.000001)&&(reason.length<5||reason.length>1000))problem('Explain damage, rejected stock or an over-delivery.');
      return {id:randomUUID(),poLineId:p.id,skuId:p.skuId,qboItemId:p.qboItemId,name:p.name,unit:p.unit,area:p.area,receivedQty,damagedQty,rejectedQty,goodQty,exceptionReason:reason,lotNumber:String(x.lotNumber||'').trim().slice(0,100)};
    });
    const id=randomUUID(),at=now();sql.prepare('INSERT INTO wh_receipts VALUES (?,?,?,?,?,?,?,?,?,?,?,?)').run(id,po.id,input.mode,'awaiting-putaway',1,JSON.stringify(lines),String(input.notes||'').trim().slice(0,2000),user.id,at,null,null,null);
    sql.prepare("UPDATE wh_purchase_orders SET revision=revision+1,status='partial',updated_at=? WHERE id=?").run(at,po.id);
    audit(sql,user,'receipt-recorded','receipt',id,{poId:po.id,vendorId:po.vendor_id,mode:input.mode,lines,stockCreated:false,billCreated:false});return {receipt:readReceipt(sql,id)};
  });
}
export function frozenStockLocations(db){
  const slots=[];for(const n of db.listContainers())for(const code of db.listValidSlotCodes(n)){
    const loc=db.getLocationByCode(code);if(!loc?.enabled)continue;
    const stock=db.sqlite.prepare(`SELECT p.id,p.sku_id,p.qty_units,p.pallet_group FROM pallets p JOIN locations l ON l.id=p.location_id WHERE l.type='CONTAINER' AND l.container_no=? AND l.side=? AND l.depth=? AND p.qty_units>0`).all(loc.container_no,loc.side,loc.depth);
    slots.push({id:loc.id,code:loc.code,containerNo:loc.container_no,side:loc.side,depth:loc.depth,occupied:stock.length>0,stock});
  }return {slots,walkin:db.getLocationByCode('WALKIN'),reviewRequired:db.listContainers().flatMap(n=>db.listPalletsInContainer(n)).filter(p=>!slots.some(l=>l.stock.some(s=>s.id===p.id)))};
}
export function suggestPutaway(db,receiptId,options={}){
  const r=readReceipt(db.sqlite,receiptId);if(r.putaway_at)problem('This receipt is already put away.',409);
  const inventory=frozenStockLocations(db),available=inventory.slots.filter(s=>!s.occupied),allocations=[],unplaced=[],areaReceipts=[],walkinPlanned=new Map();
  for(const line of r.lines){
    if(line.area!=='frozen'){areaReceipts.push({receiptLineId:line.id,area:line.area,qty:line.goodQty,slotTracked:false});continue;}
    const preference=options[line.id]||{},config=preference.palletConfigId?db.sqlite.prepare('SELECT * FROM pallet_configs WHERE id=? AND sku_id=?').get(preference.palletConfigId,line.skuId):db.sqlite.prepare('SELECT * FROM pallet_configs WHERE sku_id=? AND is_default=1').get(line.skuId);
    const size=config?.units_per_pallet;if(!size||size<=0){unplaced.push({receiptLineId:line.id,qty:line.goodQty,reason:'Choose the pallet size before suggesting locations.'});continue;}
    const onHand=db.sqlite.prepare(`SELECT COALESCE(SUM(qty_units),0) qty FROM (SELECT p.qty_units FROM pallets p JOIN locations l ON l.id=p.location_id WHERE l.type='WALKIN' AND p.sku_id=? AND p.qty_units>0 UNION ALL SELECT i.qty_units FROM loose_inventory i JOIN locations l ON l.id=i.location_id WHERE l.type='WALKIN' AND i.sku_id=? AND i.qty_units>0)`).get(line.skuId,line.skuId).qty+(walkinPlanned.get(line.skuId)||0);
    if(preference.walkinQty!==undefined&&preference.walkinTargetQty!==undefined)problem('Choose a walk-in quantity or a restock target, not both.');
    const requested=preference.walkinTargetQty!==undefined?Math.max(0,quantity(preference.walkinTargetQty,'walk-in target')-onHand):quantity(preference.walkinQty??0,'walk-in quantity');
    let keep=Math.min(line.goodQty,requested),extra=round(line.goodQty-keep);walkinPlanned.set(line.skuId,(walkinPlanned.get(line.skuId)||0)+keep);
    if(keep&&!inventory.walkin?.enabled)problem('The walk-in freezer receiving area is not enabled.');
    while(keep>0){const qty=Math.min(size,keep);allocations.push({receiptLineId:line.id,locationCode:inventory.walkin.code,qty,palletConfigId:config.id,purpose:'walk-in-restock'});keep=round(keep-qty);}
    // Prefer containers already holding this SKU, then fill from the back. No stock is changed.
    const sameSKU=new Set(inventory.slots.filter(s=>s.stock.some(p=>p.sku_id===line.skuId)).map(s=>s.containerNo));
    available.sort((a,b)=>(Number(sameSKU.has(b.containerNo))-Number(sameSKU.has(a.containerNo)))||a.containerNo-b.containerNo||b.depth-a.depth||a.side.localeCompare(b.side));
    while(extra>0&&available.length){const slot=available.shift(),qty=Math.min(size,extra);allocations.push({receiptLineId:line.id,locationCode:slot.code,locationId:slot.id,qty,palletConfigId:config.id,purpose:'container-overflow'});extra=round(extra-qty);}
    if(extra>0)unplaced.push({receiptLineId:line.id,qty:extra,reason:'No empty slot remains in the configured container layouts.'});
  }
  return {receiptId:r.id,receiptRevision:r.revision,allocations,areaReceipts,unplaced,suggestionOnly:true,stockCreated:false};
}
export function confirmPutaway(db,user,input){
  permit(user,['admin','office','warehouse'],'receiving');const sql=db.sqlite;
  if(input.confirmed!==true||!Array.isArray(input.allocations)||input.allocations.length>500)problem('Confirm the actual placement of the received products.');
  return once(sql,user,input.requestId,input,()=>{
    const r=readReceipt(sql,input.receiptId);if(r.putaway_at||r.revision!==input.expectedRevision)problem('This receipt changed or was already put away.',409);
    const totals=new Map(),slots=new Set(),allocations=input.allocations.map(x=>{
      const l=r.lines.find(l=>l.id===x.receiptLineId);if(!l||l.area!=='frozen')problem('Only frozen products use pallet locations.');
      const loc=db.getLocationByCode(String(x.locationCode||'').toUpperCase());if(!loc?.enabled||!['CONTAINER','WALKIN'].includes(loc.type))problem('Choose an active freezer location.');
      if(loc.type==='CONTAINER'){db.assertAvailableContainerLocation(loc);const key=[loc.container_no,loc.side,loc.depth].join(':');if(slots.has(key))problem('Choose a separate location for each received pallet.');slots.add(key);}
      const qty=quantity(x.qty,'putaway quantity',false);totals.set(l.id,round((totals.get(l.id)||0)+qty));
      if(x.palletConfigId&&!sql.prepare('SELECT id FROM pallet_configs WHERE id=? AND sku_id=?').get(x.palletConfigId,l.skuId))problem('Pallet size belongs to another product.');
      return {...x,qty,locationCode:loc.code,line:l};
    });
    for(const l of r.lines.filter(l=>l.area==='frozen'))if(Math.abs((totals.get(l.id)||0)-l.goodQty)>0.000001)problem('Place exactly the accepted quantity of '+l.name+'. Damaged/rejected stock is excluded.');
    const ids=[];for(const x of allocations){let lotId=null;if(x.line.lotNumber){lotId=sql.prepare('SELECT id FROM lots WHERE sku_id=? AND lot_number=?').get(x.line.skuId,x.line.lotNumber)?.id;if(!lotId)lotId=Number(sql.prepare('INSERT INTO lots(sku_id,lot_number,supplier_lot) VALUES (?,?,?)').run(x.line.skuId,x.line.lotNumber,x.line.lotNumber).lastInsertRowid);}
      const id=db.createPallet({skuId:x.line.skuId,lotId,palletConfigId:x.palletConfigId||null,locationCode:x.locationCode,qtyUnits:x.qty,notes:'Received against PO '+readPO(sql,r.po_id).po_number+'; receipt '+r.id,userName:user.displayName});ids.push(id);
      sql.prepare("UPDATE inventory_movements SET reference_type='WAREHOUSE_RECEIPT',reference_id=? WHERE to_pallet_id=? AND type='RECEIVE'").run(r.id,id);
    }
    const at=now(),plan={allocations:allocations.map(({line,...x})=>x),palletIds:ids,areaReceipts:r.lines.filter(l=>l.area!=='frozen').map(l=>({receiptLineId:l.id,area:l.area,qty:l.goodQty,slotTracked:false}))};
    sql.prepare('INSERT INTO wh_putaway VALUES (?,?,?,?)').run(r.id,JSON.stringify(plan),user.id,at);
    sql.prepare("UPDATE wh_receipts SET status='received',revision=revision+1,putaway_at=? WHERE id=?").run(at,r.id);
    audit(sql,user,'receipt-putaway-confirmed','receipt',r.id,{...plan,billCreated:false});return {receipt:readReceipt(sql,r.id),...plan};
  });
}
export function approveReceipt(db,user,input){permit(user,['admin','office']);return once(db.sqlite,user,input.requestId,input,()=>{const r=readReceipt(db.sqlite,input.receiptId);if(r.approved_at||r.revision!==input.expectedRevision)problem('Refresh this receipt before office review.',409);const at=now();db.sqlite.prepare('UPDATE wh_receipts SET approved_by=?,approved_at=?,revision=revision+1 WHERE id=?').run(user.id,at,r.id);audit(db.sqlite,user,'receipt-office-approved','receipt',r.id,{billCreated:false});return {receipt:readReceipt(db.sqlite,r.id),quickbooksBillCreated:false};});}
