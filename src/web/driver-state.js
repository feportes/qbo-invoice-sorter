import {validateDepartureCounts} from './departure-summary.js';
// Shared validation for the server and offline browser. The server revalidates every event.
const problem=(message,status=400)=>{throw Object.assign(Error(message),{status});};
const clean=v=>String(v??'').trim();
const code=v=>clean(v).toUpperCase();
const round=v=>Math.round(v*1e6)/1e6;
const reasons=new Set(['Not on vehicle','Damaged / unusable','Wrong product loaded','Customer declined','Other']);
const qty=(v,min=0)=>{if(typeof v!=='number'||!Number.isFinite(v)||v<min||v>1000000)problem('Review the product quantity.');return round(v);};
const reason=v=>{v=clean(v);if(v.length<5||v.length>1000)problem('Explain why this change is needed.');return v;};
export function applyDriverEvent(route,event,at=new Date().toISOString(),verifiedProductKey=null){
  const x=event.payload||{};if(route.status==='delivered')problem('This route is finalized. Ask the office to review a correction.',409);
  const stop=route.stops.find(s=>s.id===x.stopId),active=()=>{if(!stop||['completed','skipped','deferred'].includes(stop.status))problem('Choose an open stop.');return stop;};
  if(event.kind==='depart'){
    if(route.status!=='loaded'||x.confirmedLoad!==true)problem('Review the load summary before departure.');
    validateDepartureCounts(route,x);route.departureCounts={...x.loadCounts};route.status='delivering';route.departedAt=at;if(clean(x.vehicleId)){route.vehicleId=clean(x.vehicleId);route.vehicleName=clean(x.vehicleName);}return;
  }
  if(event.kind==='note'||event.kind==='photo'){if(x.stopId&&!stop)problem('Stop is not on this route.');if(event.kind==='note')reason(x.notes);else if(!Array.isArray(x.mediaIds)||!x.mediaIds.length)problem('Attach a saved photo.');return;}
  if(event.kind==='finalize'){
    if(route.status!=='delivering'||route.stops.some(s=>!['completed','skipped'].includes(s.status)))problem('Complete, revisit or explain every stop before finishing.');route.status='delivered';route.completedAt=at;return;
  }
  if(route.status!=='delivering')problem('Start the route before recording a delivery.');
  if(event.kind==='resume'){if(!stop||!['deferred','skipped'].includes(stop.status))problem('Choose a deferred or skipped stop.');stop.status=stop.previousStatus||'pending';stop.resumeReason=reason(x.reason);delete stop.previousStatus;return;}
  active();
  if(event.kind==='defer'||event.kind==='skip'){stop.previousStatus=stop.status||'pending';stop.status=event.kind==='defer'?'deferred':'skipped';stop.visitReason=reason(x.reason);stop.visitAt=at;return;}
  if(event.kind==='arrive'){if(!['pending',undefined].includes(stop.status))problem('This stop is already in progress.');stop.status='arrived';stop.arrivedAt=at;return;}
  if(event.kind==='start-picking'){if(stop.status!=='arrived')problem('Confirm arrival first.');stop.status='picking';stop.pickingAt=at;return;}
  if(event.kind==='manual-pick'){
    if(stop.status!=='picking')problem('Start picking before recording a manual pick.');const p=stop.products.find(p=>p.id===x.productId);if(!p)problem('Choose a product at this stop.');const q=x.qty,picked=stop.counts?.[p.id]||0,left=Math.ceil(round(p.qty))-picked;
    if(!Number.isInteger(q)||q<1||q>left||String(x.reason||'').trim().length<5)problem('Review manual quantity and give a reason.');stop.counts??={};stop.counts[p.id]=picked+q;stop.history??=[];stop.history.push({eventId:event.id,productId:p.id,qty:q,at,source:'admin-manual-pick',reason:x.reason});if(stop.shortages)delete stop.shortages[p.id];return;
  }
  if(event.kind==='scan'){
    if(stop.status!=='picking')problem('Start picking at this stop before scanning.');
    const matches=stop.products.filter(p=>verifiedProductKey?p.id===verifiedProductKey:[...(p.barcodes||[]),p.sku].filter(Boolean).some(c=>code(c)===code(x.barcode)));
    if(matches.length!==1)problem('That barcode does not identify one product at this stop.');
    const p=matches[0];if(p.warehouseWarnings?.length&&!(stop.counts?.[p.id]>0)&&x.warehouseDoubleChecked!==true)problem('This item was picked without a barcode. Double-check its product, size and quantity first.');const count=Math.ceil(p.qty-0.000001),picked=stop.counts?.[p.id]||0,left=count-picked,q=qty(x.qty??1,1);
    if(!Number.isInteger(q)||q>left)problem('Only '+left+' units remain.');if(q>1&&(x.bulk!==true||left<=(p.scanLimit??10)))problem('Scan each remaining unit individually.');
    stop.counts??={};stop.counts[p.id]=picked+q;stop.history??=[];stop.history.push({eventId:event.id,productId:p.id,qty:q,at,...(x.warehouseDoubleChecked?{warehouseDoubleChecked:true}:{})});if(stop.shortages)delete stop.shortages[p.id];return;
  }
  if(event.kind==='undo'){
    if(stop.status!=='picking')problem('Choose a stop that is still being picked.');
    const history=(stop.history||[]).findLast(h=>!h.reversedAt&&h.productId===x.productId);if(!history)problem('No accepted scan remains to undo on this item.');
    history.reversedAt=at;stop.counts[history.productId]-=history.qty;if(stop.shortages)delete stop.shortages[history.productId];return;
  }
  if(event.kind==='shortage'){
    if(stop.status!=='picking'||x.confirmed!==true||!Array.isArray(x.items)||!x.items.length)problem('Confirm missing products before finishing.');
    const seen=new Set();for(const item of x.items){const p=stop.products.find(p=>p.id===item.productId);if(!p||seen.has(p.id))problem('Review the missing products.');seen.add(p.id);const remaining=Math.ceil(p.qty-0.000001)-(stop.counts?.[p.id]||0);if(remaining<1||!reasons.has(item.reason))problem('Review the missing quantity and reason.');if(item.reason==='Other')reason(item.notes);stop.shortages??={};stop.shortages[p.id]={missing:remaining,reason:item.reason,notes:clean(item.notes).slice(0,1000),at};}return;
  }
  if(event.kind==='edit-line'){
    const p=stop.products.find(p=>p.id===x.productId);if(!p)problem('Choose a product from this stop.');const q=qty(x.qty),changedPrice=x.unitPrice!==undefined&&x.unitPrice!==null&&x.unitPrice!=='',price=changedPrice?qty(x.unitPrice):undefined;reason(x.reason);
    stop.proposedChanges??={};stop.proposedChanges[p.id]={qty:q,...(changedPrice?{unitPrice:price}:{}),reason:clean(x.reason),at};return;
  }
  if(event.kind==='payment'){
    if(!['cash','check'].includes(x.method)||qty(x.amount,0.01)<=0)problem('Review cash or check payment details.');if(x.method==='check'&&!clean(x.reference))problem('Enter the check reference.');
    stop.payments??=[];stop.payments.push({id:event.id,method:x.method,amount:x.amount,reference:clean(x.reference).slice(0,100),notes:clean(x.notes).slice(0,1000),at,status:'awaiting-office-approval'});return;
  }
  if(event.kind==='complete-stop'){
    if(stop.status!=='picking')problem('Finish the product checks before completing this stop.');
    for(const p of stop.products){const missing=Math.ceil(p.qty-0.000001)-(stop.counts?.[p.id]||0);if(missing>0&&stop.shortages?.[p.id]?.missing!==missing)problem('Confirm why remaining products were not delivered.');}
    stop.status='completed';stop.completedAt=at;stop.officeReview='pending';return;
  }
  problem('Unsupported driver action.');
}
