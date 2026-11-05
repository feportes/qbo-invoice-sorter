import {productName,storageLabel} from './product-display.js';
import {PickingStock,stockDialog} from './picking-stock.js';
import {WarehouseScanQueue} from './warehouse-scan-queue.js';

import {scanErrorSound} from './scan-error-sound.js';

import {areaPickItems,freezerPalletGroup,palletNames,productKey} from './warehouse-picking-groups.js';

const $=id=>document.getElementById(id),names={freezer:'Freezer',dry:'Dry',refrigerated:'Refrigerated / fresh'},key=l=>l.key||l.order+':'+l.index;

let inventory;
let awaitingCompletion,barcodeRefresh,loadingVehicleScan;
let session,data,current,state={},pending,busy=false,storeKey,requestKey,feedbackTimer,completedPick,advanceTimer,scanQueue,scanQueuePaused=false;

const el=(tag,text)=>{const n=document.createElement(tag);if(text!==undefined)n.textContent=text;return n;};

function remember(){if(storeKey)localStorage.setItem(storeKey,JSON.stringify(state));}

let noteTimer;function note(text,error=false){clearTimeout(noteTimer);$('message').textContent=error?text:'';$('message').className=error?'compact-warning':'';}
function hideScanKeyboard(){document.activeElement?.blur?.();window.DrawKeyboard?.hideKeyboard?.();}

function focusCurrentPick(){requestAnimationFrame(()=>$('active-pick-summary')?.scrollIntoView({block:'center',behavior:'smooth'}));}

const errorSound=scanErrorSound();

function sound(){void errorSound.unlock();}

document.addEventListener('pointerdown',sound,{passive:true});document.addEventListener('keydown',sound,{passive:true});



function feedback(text,error=true){$('pick-feedback')?.remove();clearTimeout(feedbackTimer);const box=el('div');box.id='pick-feedback';box.className='feedback'+(error?'':' success');box.setAttribute('role',error?'alert':'status');const icon=el('span',error?'\u2715':'\u2713');icon.className='symbol';box.append(icon,el('p',text));const close=el('button','Close');close.type='button';close.onclick=()=>box.remove();box.append(close);let y;box.onpointerdown=e=>y=e.clientY;box.onpointerup=e=>{if(y-e.clientY>30)box.remove();};document.body.append(box);feedbackTimer=setTimeout(()=>box.remove(),error?9000:4000);if(error){void errorSound.play();}}

function button(parent,text,run,primary=false){const b=el('button',text);b.type='button';b.className=primary?'primary':'secondary';b.disabled=busy||!!pending||!!scanQueue?.length;b.onclick=()=>Promise.resolve().then(run).catch(e=>feedback(e.message));parent.append(b);return b;}

function field(parent,label,value='',type='text'){const wrap=el('label',label),input=el('input');input.type=type;input.value=value;wrap.append(input);parent.append(wrap);return input;}

async function api(path,body){const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),45000);try{const r=await fetch('/api/warehouse'+path,{method:body?'POST':'GET',signal:controller.signal,cache:'no-store',credentials:'same-origin',headers:body?{'Content-Type':'application/json','X-Warehouse-CSRF':session.csrf}:{},body:body?JSON.stringify(body):undefined});const result=await r.json();if(!r.ok)throw Object.assign(Error(result.error||'Save failed.'),{status:r.status});return result;}finally{clearTimeout(timer);}}

function localDate(){const d=new Date();return [d.getFullYear(),String(d.getMonth()+1).padStart(2,'0'),String(d.getDate()).padStart(2,'0')].join('-');}

async function refresh(){if(session&&(pending||scanQueue?.length)){note('Scans are saved on this device. They will sync automatically; keep picking.',true);return;}if(!session){session=await api('/session');storeKey='draw-warehouse-work:'+session.user.id;requestKey='draw-warehouse-pending:'+session.user.id;inventory=new PickingStock({storage:localStorage,key:'draw-restock:'+session.user.id,api});void inventory.refresh().catch(()=>{});void inventory.send().catch(()=>{});try{state=JSON.parse(localStorage.getItem(storeKey)||'{}');pending=JSON.parse(localStorage.getItem(requestKey)||'null');scanQueue=new WarehouseScanQueue(localStorage,'draw-warehouse-scans:'+session.user.id);}catch{}$('date').value=state.date||localDate();}data=await api('/picking');if(state.routeId&&!data.routes.some(r=>r.id===state.routeId)&&!pending){state.routeId=null;state.date=localDate();$('date').value=state.date;remember();}if(scanQueue?.first&&!pending){state.routeId=scanQueue.first.args[0];state.focus=scanQueue.first.target;state.area=scanQueue.first.area;state.started=true;}if(pending){const r=data.routes.find(r=>r.id===pending.command.args[0]);if(r)state.routeId=r.id;}draw();if(scanQueue?.length&&!pending&&!scanQueuePaused)void drainScans();}

function totals(r){return {total:r.lines.reduce((n,l)=>n+(l.requestedQty??l.qty),0),picked:r.lines.reduce((n,l)=>n+l.picked,0),short:r.lines.reduce((n,l)=>n+(l.shortage?.qty||0),0),remaining:r.lines.reduce((n,l)=>n+l.qty-l.picked,0)};}

function pickPercentage(t){const value=t.total>0?Math.min(100,Math.floor(t.picked/t.total*100)):0,badge=el('div'),label=el('strong',value+'% picked'),bar=el('progress');badge.className='pick-percentage'+(value===100?' complete':'');bar.max=t.total||1;bar.value=t.picked;bar.className='pick-progress'+(value===100?' complete':'');bar.setAttribute('aria-label',value+'% picked');badge.append(label,bar);return badge;}
function chooseRoute(r){sound();clearTimeout(advanceTimer);completedPick=null;awaitingCompletion=null;state={date:r.date,routeId:r.id,area:r.lines.find(l=>l.picked<l.qty)?.zone||'freezer',started:false};$('date').value=r.date;remember();draw();}

function totalMode(){return current?.pickingModes?.[state.area]===true;}

function remainingItems(){if(totalMode())return areaPickItems(current.lines,state.area,true);if(!state.pickStop){const first=areaPickItems(current.lines,state.area,false)[0];state.pickStop=first?.order||null;}return areaPickItems(current.lines,state.area,false,state.pickStop);}

function next(){const all=remainingItems();if(state.area==='freezer'&&!state.pallet)state.pallet=all.find(l=>l.palletGroup==='tubs')?'tubs':'boxes';if(!totalMode()&&state.area==='freezer'&&!all.some(l=>l.palletGroup===state.pallet)&&all.length)state.pallet=all[0].palletGroup;const lines=state.area==='freezer'?all.filter(l=>l.palletGroup===state.pallet):all;return lines.find(l=>key(l)===state.focus)||lines[0];}



async function save(name,args){if(busy||pending||scanQueue?.length)return false;pending={requestId:crypto.randomUUID(),baseRevision:data.revision,command:{name,args}};try{localStorage.setItem(requestKey,JSON.stringify(pending));}catch{pending=null;feedback('Device storage is unavailable. Nothing was submitted.');return false;}return send();}

function scanStatus(){const n=scanQueue?.length||0;const label=$('scan-save-status');if(label)label.textContent=n?n+' scans saving · keep scanning this item':'All scans saved';}

function enqueueScan(code,qty,line,exception=null){
 hideScanKeyboard();

 try{

  if(!scanQueue)throw Error('Refresh the page before scanning.');if(pendingAdditions().length)throw Error('Accept the added order before continuing warehouse picking.');

  if(pending&&!pending.scanId)throw Error('Wait for the current action to finish.');

  if(scanQueuePaused)throw Error('Review the scan warning and resume saved scans first.');

  if(!Number.isInteger(qty)||qty<1)throw Error('Enter a positive whole quantity.');if(qty>line.qty-line.picked)throw Error('Only '+(line.qty-line.picked)+' remain.');if(qty>1&&line.qty-line.picked<=line.scanLimit)throw Error('Scan each unit individually at or below this product threshold.');if(!exception&&!line.scanCodes?.some(c=>String(c).trim().toUpperCase()===code.trim().toUpperCase()))throw Error('Wrong or unregistered barcode. Scan '+line.name+'.');

  scanQueue.add({id:crypto.randomUUID(),recordedAt:new Date().toISOString(),countAfter:productCountAfter(line,qty),target:key(line),area:state.area,deltas:pickDeltas(line,qty),args:exception?[current.id,{...exception,productId:line.productId,packId:line.packId,qty,area:state.area,...(!totalMode()?{order:line.order,index:line.index}:{})}]:[current.id,totalMode()?line.productId:line.order,totalMode()?line.packId:line.index,code,qty,{bulk:qty>1,area:state.area,chooseItem:true}],name:exception?'recordWarehouseUnscannable':totalMode()?'scanWarehouseProduct':'scan',line:structuredClone(line)});

  if(qty===line.qty-line.picked){clearTimeout(advanceTimer);if(navigator.onLine===false){feedback(line.name+' counted offline. Saved on this device.',false);completedPick={...line,picked:line.qty};advanceTimer=setTimeout(()=>{completedPick=null;state.focus=null;draw();focusCurrentPick();},1800);}else {const waiting=awaitingCompletion={...line,picked:line.qty};advanceTimer=setTimeout(()=>{if(awaitingCompletion!==waiting)return;awaitingCompletion=null;state.focus=null;draw();focusCurrentPick();},1800);}}if($('pick-code'))$('pick-code').value='';if($('pick-qty'))$('pick-qty').value=1;draw();scanStatus();void drainScans();return true;

 }catch(e){feedback(e.message);focusCurrentPick();return false;}

}

async function scanBarcode(code,qty,target){
 hideScanKeyboard();
 const same=c=>String(c).trim().toUpperCase()===code.trim().toUpperCase();
 try{
  if(!target.scanCodes?.some(same)&&navigator.onLine!==false){
   const wrong=data.routes.some(r=>r.lines.some(l=>(l.productId!==target.productId||l.packId!==target.packId)&&l.scanCodes?.some(same)));
   if(!wrong){
    if(!barcodeRefresh)barcodeRefresh=api('/picking').finally(()=>{barcodeRefresh=null;});
    const fresh=await barcodeRefresh;
    // Refresh aliases only: never replace counts/revision with a read racing saved scans.
    for(const route of data.routes)for(const line of route.lines){const latest=fresh.routes.find(r=>r.id===route.id)?.lines.find(l=>l.productId===line.productId&&l.packId===line.packId);if(latest)line.scanCodes=latest.scanCodes;}
   }
  }
  current=projectRoute(data.routes.find(r=>r.id===state.routeId));const line=remainingItems().find(l=>key(l)===key(target));
  if(!line)throw Error('This item is already complete. Choose the next product.');
  return enqueueScan(code,qty,line);
 }catch(e){feedback(navigator.onLine===false?'Barcode not verified offline. Scan a saved barcode for '+target.name+'.':e.message);focusCurrentPick();return false;}
}

async function drainScans(){

 if(busy||pending||scanQueuePaused||!scanQueue?.first||navigator.onLine===false)return;

 const read=scanQueue.first;

 pending={requestId:read.id,baseRevision:data.revision,command:{name:read.name,args:read.args},scanId:read.id,scanLine:read.line};

 try{localStorage.setItem(requestKey,JSON.stringify(pending));}catch{pending=null;scanQueuePaused=true;feedback('Scans remain saved on this device, but sending is paused because device storage is unavailable.');draw();return;}

 await send();

}

async function send(){

 if(busy||!pending)return false;busy=true;

 const operation=pending,name=operation.command.name,isScan=['scan','scanWarehouseProduct','recordWarehouseUnscannable'].includes(name),prior=operation.scanLine||(current?next():null),scroll=window.scrollY;

 if(!isScan)draw();else scanStatus();let accepted=false,resumeAfterRejection=false;

 try{

  const result=await api('/picking',operation);

  // Acknowledge queue before clearing exact replay request. Crashes can only replay the same UUID.

  if(operation.scanId)scanQueue.remove(operation.scanId);if(isScan&&awaitingCompletion&&key(awaitingCompletion)===key(prior)&&!scanQueue.items.some(read=>read.target===key(prior)))awaitingCompletion=null;

  localStorage.removeItem(requestKey);pending=null;data=result;note(data.message||'Saved.');

  const route=data.routes.find(r=>r.id===state.routeId),line=prior&&route?((name==='scanWarehouseProduct'||name==='recordWarehouseUnscannable'&&!operation.command.args[1].order)?areaPickItems(route.lines,state.area,true).find(l=>key(l)===key(prior)):route.lines.find(l=>key(l)===key(prior))):null;

  if(isScan&&prior&&!scanQueue?.length&&state.focus===key(prior)&&(!line||line.picked===line.qty)){

   completedPick={...prior,picked:prior.qty};feedback(!totalMode()&&route&&!areaPickItems(route.lines,state.area,false,prior.order).length?prior.customer+' · '+names[state.area]+' complete. Continue to the next customer.':prior.name+' complete. Moving to the next item.',false);clearTimeout(advanceTimer);advanceTimer=setTimeout(()=>{completedPick=null;state.focus=null;draw();focusCurrentPick();},1800);

  }

  accepted=true;return true;

 }catch(e){

  if(e.status===409&&e.message==='Accept the added order before continuing warehouse picking.'){
   if(operation.scanId){localStorage.removeItem(requestKey);pending=null;scanQueuePaused=true;try{data=await api('/picking');}catch{}feedback('New order added. Accept it to continue.');}return false;
  }
  if([400,403,404,409,422].includes(e.status)){

   try{

    if(operation.scanId){const k='draw-warehouse-rejected:'+session.user.id,failed=JSON.parse(localStorage.getItem(k)||'[]');localStorage.setItem(k,JSON.stringify([...failed,{...operation,error:e.message,time:new Date().toISOString()}]));scanQueue.remove(operation.scanId);resumeAfterRejection=[400,422].includes(e.status);scanQueuePaused=!resumeAfterRejection&&scanQueue.length>0;clearTimeout(advanceTimer);completedPick=null;awaitingCompletion=null;state.focus=key(operation.scanLine);state.area=operation.scanLine.zone||state.area;state.pickStop=operation.scanLine.order;}

    localStorage.removeItem(requestKey);pending=null;

    try{data=await api('/picking');}catch{}

    feedback(e.message+(operation.scanId?' This scan was not counted. Check the item and remaining count.':''));

   }catch{feedback('Device storage failed. The exact request is kept for safe retry.');}

  }else{note('Offline · continue picking. Scans are saved on this device and will sync when connected.',true);}

  return false;

 }finally{

  busy=false;const code=$('pick-code')?.value||'',qty=$('pick-qty')?.value||1;

  draw();if($('pick-code'))$('pick-code').value=code;if($('pick-qty'))$('pick-qty').value=qty;window.scrollTo(0,scroll);

  if(isScan||name==='manualPick')focusCurrentPick();

  if((accepted||resumeAfterRejection)&&scanQueue?.length&&!scanQueuePaused)queueMicrotask(drainScans);

 }

}

function projectRoute(route){

 if(!route)return route;const r=structuredClone(route);

 for(const read of scanQueue?.items||[]){if(read.args[0]!==r.id)continue;if(read.deltas){for(const delta of read.deltas){const l=r.lines.find(l=>l.order===delta.order&&l.index===delta.index);if(l)l.picked=Math.max(0,Math.min(l.qty,l.picked+delta.qty));}continue;}let qty=read.args[4];const matches=r.lines.filter(l=>read.name==='scanWarehouseProduct'?l.productId===read.args[1]&&l.packId===read.args[2]&&l.zone===read.area:l.order===read.args[1]&&l.index===read.args[2]);for(const l of matches){const n=Math.min(qty,l.qty-l.picked);l.picked+=n;qty-=n;}}

 return r;

}

function bulkDialog(line){dialog('Bulk pick · '+line.name,(body,actions,d)=>{

 const count=field(body,'Counted units',line.qty-line.picked,'number');count.min=2;count.max=line.qty-line.picked;const barcode=field(body,'Scan or enter this product barcode');barcode.id='bulk-pick-code';

 const label=el('label'),check=el('input');check.type='checkbox';label.append(check,document.createTextNode('I counted and double-checked every unit.'));body.append(label);

 const confirm=button(actions,'Confirm bulk pick',async()=>{if(!check.checked)throw Error('Double-check the physical count and tick the confirmation.');const n=Number(count.value);if(!Number.isInteger(n)||n<2||n>line.qty-line.picked)throw Error('Enter a whole count from 2 to '+(line.qty-line.picked)+'.');if(!barcode.value.trim())throw Error('Scan the product barcode first.');if(await scanBarcode(barcode.value.trim(),n,line))d.close();},true);confirm.disabled=false;

 });}

function dialog(title,build){const d=el('dialog'),heading=el('h2',title),body=el('div'),actions=el('div');heading.tabIndex=-1;heading.autofocus=true;actions.className='dialog-actions';d.append(heading,body,actions);build(body,actions,d);const cancel=el('button','Cancel');cancel.type='button';cancel.onclick=()=>d.close();actions.append(cancel);const hide=el('button','Hide keyboard');hide.type='button';hide.onclick=()=>{document.activeElement?.blur();window.DrawKeyboard?.hideKeyboard?.();};actions.append(hide);d.onclick=e=>{if(e.target!==d)return;const b=d.getBoundingClientRect();if(e.clientX<b.left||e.clientX>b.right||e.clientY<b.top||e.clientY>b.bottom)d.close();};d.onclose=()=>d.remove();document.body.append(d);d.showModal();heading.focus();return d;}

function pickDeltas(line,qty){const out=[];let left=qty;for(const l of line.members||[line]){const n=Math.min(left,l.qty-l.picked);if(n)out.push({order:l.order,index:l.index,productId:l.productId,packId:l.packId,qty:n});left-=n;if(!left)break;}return out;}
function productCountAfter(line,added=0){const same=current.lines.filter(l=>l.productId===line.productId&&l.packId===line.packId);return {picked:same.reduce((n,l)=>n+l.picked,0)+added,remaining:Math.max(0,same.reduce((n,l)=>n+l.qty-l.picked,0)-added)};}
function recentPicks(parent){const d=el('details');d.className='recent-scans';const queued=scanQueue?.items||[],undoIds=new Set(queued.filter(x=>x.name==='undoWarehousePick').map(x=>x.args[1].entryId)),saved=[...(current.recentPicks||[])],seen=new Set(saved.map(x=>x.id));for(const q of queued)if(q.deltas?.some(a=>a.qty>0)&&!seen.has(q.id))saved.push({id:q.id,name:q.line.name,qty:q.deltas.reduce((n,a)=>n+a.qty,0),by:session.user.displayName||session.user.username||'You',at:q.recordedAt,kind:q.name,allocations:q.deltas,pending:true,...(q.countAfter?{pickedAfter:q.countAfter.picked,remainingAfter:q.countAfter.remaining}:{})});const entries=saved.slice(-30).reverse();d.append(el('summary','Recent scans · '+entries.length));if(!entries.length)d.append(el('p','New picks will appear here with their time and quantity.'));for(const h of entries){const row=el('div');row.className='scan-history-row';const when=h.at?new Date(h.at).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit',second:'2-digit'}):'';row.append(el('strong',h.name+' · '+h.qty+' units'),el('small',when+' · '+(h.by||'Picker')+' · '+(h.reversedAt?'Reversed':undoIds.has(h.id)?'Undo saving':h.pending?'Saving':'Saved')+(h.kind==='recordWarehouseUnscannable'?' · No barcode':'')));const allocation=h.allocations?.[0],line=allocation&&current.lines.find(l=>l.productId===allocation.productId&&l.packId===allocation.packId);if(Number.isFinite(h.pickedAfter)&&Number.isFinite(h.remainingAfter)){const total=el('strong',(h.pending?'On this device after scan: ':'Route total after scan: ')+h.pickedAfter+' picked · '+h.remainingAfter+' remaining'+(h.reversedAt?' (before undo)':''));total.className='scan-running-count';row.append(total);}else if(line){const count=productCountAfter(line);row.append(el('small','Current route total: '+count.picked+' picked · '+count.remaining+' remaining'));}if(!h.reversedAt&&!undoIds.has(h.id)&&current.status==='released'){const b=button(row,'Undo / rescan',()=>undoPick(h));b.disabled=scanQueuePaused||!!pending&&!pending.scanId;}d.append(row);}parent.append(d);}
function undoPick(entry){dialog('Undo this pick',(body,actions,d)=>{body.append(el('strong',entry.name),el('p','Rescan this product to reverse this entry: '+entry.qty+' units. Then pick it again if needed.'));const code=field(body,'Rescan product barcode / SKU');code.id='undo-pick-code';code.autocomplete='off';const reason=field(body,'Reason if barcode cannot be scanned'),label=el('label'),check=el('input');check.type='checkbox';label.append(check,document.createTextNode('Barcode unavailable: I checked the item and correction.'));body.append(label);const submit=button(actions,'Confirm undo',()=>{if(!code.value.trim()&&(!check.checked||reason.value.trim().length<5))throw Error('Rescan the item, or give a reason and confirm the physical check.');const first=entry.allocations[0],line=current.lines.find(l=>l.order===first.order&&l.index===first.index);if(!line)throw Error('Refresh this route before correcting the pick.');scanQueue.add({id:crypto.randomUUID(),recordedAt:new Date().toISOString(),target:key(line),area:line.zone,name:'undoWarehousePick',args:[current.id,{entryId:entry.id,barcode:code.value.trim(),reason:reason.value,confirmed:check.checked}],line:structuredClone(line),deltas:entry.allocations.map(a=>({...a,qty:-a.qty}))});clearTimeout(advanceTimer);completedPick=null;awaitingCompletion=null;state.area=line.zone;state.pickStop=line.order;state.focus=totalMode()?productKey(line):key(line);state.started=true;d.close();hideScanKeyboard();draw();focusCurrentPick();void drainScans();},true);submit.disabled=false;d.onBarcode=value=>{code.value=value;submit.click();};});}
function cannotScan(line){dialog('Pick without a scannable barcode',(body,actions,d)=>{body.append(el('strong',line.name),el('p','Check the product label and physical count. The driver will be required to double-check this item.'));const reason=el('select');for(const [v,label]of [['missing-barcode','No barcode on item'],['damaged-label','Damaged label'],['unreadable','Barcode will not read'],['other','Other']])reason.append(new Option(label,v));reason.setAttribute('aria-label','Barcode problem');body.append(reason);const notes=field(body,'Notes (required for Other)'),left=line.qty-line.picked,q=field(body,'Quantity checked',1,'number');q.min=1;q.max=left>line.scanLimit?left:1;q.step=1;const label=el('label'),check=el('input');check.type='checkbox';label.append(check,document.createTextNode('I checked the product, size and quantity against this item.'));body.append(label);const submit=button(actions,'Confirm checked pick',()=>{if(!check.checked||reason.value==='other'&&notes.value.trim().length<5)throw Error('Choose a reason, explain Other and confirm the physical check.');if(enqueueScan('',Number(q.value),line,{reason:reason.value,notes:notes.value,confirmed:true}))d.close();},true);submit.disabled=false;});}

function pickList(parent){const items=remainingItems(),detail=el('details');detail.className='remaining-list';detail.open=true;detail.append(el('summary',(totalMode()?'Remaining in '+names[state.area]:((items[0]?.customer||current.lines.find(l=>l.order===state.pickStop)?.customer||'Current stop')+' · remaining in '+names[state.area]))+' · '+items.length+' '+(totalMode()?(items.length===1?'product':'products'):(items.length===1?'customer item':'customer items'))));const body=el('div');body.className='pick-list';if(!items.length)body.append(el('p',totalMode()?'Everything in this area has been picked.':"This customer's "+names[state.area]+' items are finished.'));const groups=state.area==='freezer'?['tubs','boxes']:[null];for(const group of groups){const rows=group?items.filter(l=>l.palletGroup===group):items;if(group&&rows.length)body.append(el('h3',palletNames[group]));for(const l of rows){const b=button(body,productName(l)+' — '+(l.qty-l.picked)+' remaining',()=>{clearTimeout(advanceTimer);completedPick=null;awaitingCompletion=null;state.focus=key(l);state.pallet=l.palletGroup;state.started=true;remember();draw();focusCurrentPick();});b.disabled=scanQueuePaused||!!pending&&!pending.scanId;b.append(el('small',totalMode()?'Whole route · '+new Set(l.members.map(m=>m.order)).size+' customer orders':'Stop '+l.stop+' · '+l.customer));}}detail.append(body);parent.append(detail);}

function stopComplete(parent){const rows=current.lines.filter(l=>l.order===state.pickStop&&l.zone===state.area),customer=rows[0]?.customer||'Customer',nextLine=areaPickItems(current.lines,state.area,false).find(l=>l.order!==state.pickStop),card=el('section');card.id='active-pick-summary';card.className='card pick-complete';card.setAttribute('role','status');card.append(el('h2',customer+' · '+names[state.area]+' complete'));if(rows.some(l=>l.shortage?.qty>0))card.append(el('p','Finished with reported shortages. The unavailable quantities remain recorded for invoice correction.'));if(scanQueue?.length)card.append(el('p',scanQueue.length+' scans saved on this device and waiting to sync.'));if(nextLine){card.append(el('p','Next: stop '+nextLine.stop+' · '+nextLine.customer));const proceed=button(card,'Next customer · '+nextLine.customer,()=>{clearTimeout(advanceTimer);completedPick=null;awaitingCompletion=null;state.pickStop=nextLine.order;state.focus=null;state.pallet=null;remember();draw();focusCurrentPick();},true);proceed.disabled=scanQueuePaused||!!pending&&!pending.scanId;}else card.append(el('p',names[state.area]+' is finished for this route. Choose another area above.'));parent.append(card);}

function chooseShortage(line){if(!totalMode())return shortage(line);dialog('Choose the customer shortage',(body,actions,d)=>{body.append(el('p','Record the missing quantity for the affected customer order.'));for(const l of line.members.filter(l=>l.picked<l.qty))button(body,l.customer+' · '+(l.qty-l.picked)+' remaining',()=>{d.close();shortage(l);});});}



function shortage(l){dialog('Mark remaining item not picked',(body,actions,d)=>{body.append(el('strong',l.name),el('p',(l.qty-l.picked)+' remaining units for '+l.customer+' will be removed from this shipment.'));const reason=el('select');reason.setAttribute('aria-label','Shortage reason');for(const [id,name]of[['out-of-stock','Out of stock'],['expired','Expired'],['damaged','Damaged'],['not-found','Could not find'],['other','Other']])reason.append(new Option(name,id));const label=el('label','Reason');label.append(reason);body.append(label);const notes=el('textarea');notes.placeholder='Notes (required for Other)';body.append(notes);button(actions,'Confirm not picked',async()=>{if(reason.value==='other'&&notes.value.trim().length<5)throw Error('Explain the shortage reason.');if(await save('recordWarehouseShortage',[current.id,l.order,l.index,{reason:reason.value,notes:notes.value}])){d.close();feedback('Shortage recorded. Continue picking the other items.',false);}},true);});}

async function correctInvoices(){if(busy||pending)return;const id=current.id;busy=true;draw();try{data=await api('/picking/'+encodeURIComponent(id)+'/correct-invoices',{});note('Corrected invoices confirmed in QuickBooks. Reprint the customer copies.');feedback('Invoice quantities and customer notes updated.',false);}catch(e){try{data=await api('/picking');}catch{}feedback(e.message);}finally{busy=false;draw();}}

function corrections(parent){const rows=current.corrections||[];if(!rows.length)return;const card=el('section');card.className='card not-picked';card.append(el('h2','Not-picked items and customer invoices'));for(const row of rows){card.append(el('strong',row.customer+' - Invoice '+row.sourceInvoice));for(const line of row.lines.filter(l=>l.shortage))card.append(el('p',line.shortage.qty+' not picked: '+line.name+' - '+line.shortage.reason.replaceAll('-',' ')));card.append(el('small',row.status==='corrected'?'QuickBooks corrected - office observation recorded':'Correction needed before departure'));}if(['loaded','ready-for-pickup'].includes(current.status)){if(rows.some(r=>r.status!=='corrected'))button(card,'Correct shortage invoices in QuickBooks',correctInvoices,true);else{const a=el('a','Reprint corrected customer invoices (2 copies)');a.href='/api/warehouse/picking/'+encodeURIComponent(current.id)+'/corrected-invoices.pdf';a.target='_blank';a.rel='noopener';card.append(a);}}else card.append(el('p','After loading, correct and reprint the affected customer invoices. No office approval is required.'));parent.append(card);}

function pendingAdditions(){return current?.lateAdditions?.filter(a=>a.requiresAcknowledgement)||[];}
function addedOrders(parent){const additions=pendingAdditions();if(!additions.length)return false;const panel=el('section');panel.className='card warning';panel.setAttribute('role','alert');panel.append(el('h2','New order added to this route'));for(const a of additions){panel.append(el('strong',a.customer),el('p',a.reason));for(const p of a.products)panel.append(el('p',p.qty+' × '+p.name));const print=el('a','Print this invoice · 2 copies');print.href='/api/warehouse/picking/'+encodeURIComponent(current.id)+'/orders/'+encodeURIComponent(a.orderId)+'/invoice.pdf';print.target='_blank';print.rel='noopener';panel.append(print);const accept=button(panel,'Accept added stop',async()=>{if(pending||busy)throw Error('Let the saved request finish first.');pending={requestId:crypto.randomUUID(),baseRevision:data.revision,command:{name:'acceptWarehouseAddition',args:[current.id,{orderId:a.orderId}]}};localStorage.setItem(requestKey,JSON.stringify(pending));if(await send()){scanQueuePaused=false;draw();void drainScans();}},true);accept.disabled=busy||!!pending;}panel.append(el('small','Added items join your pick list after you accept. Saved scans remain on this device.'));parent.append(panel);return true;}
let additionCheck=false;
async function checkAddedOrders(){if(additionCheck||busy||pending||navigator.onLine===false||!data||scanQueue?.length&&!scanQueuePaused)return;additionCheck=true;try{const fresh=await api('/picking');const old=JSON.stringify(data.routes.find(r=>r.id===state.routeId)?.lateAdditions||[]),now=JSON.stringify(fresh.routes.find(r=>r.id===state.routeId)?.lateAdditions||[]);if(old!==now){data=fresh;clearTimeout(advanceTimer);completedPick=null;awaitingCompletion=null;draw();if(pendingAdditions().length)feedback('A new order was added. Review and accept it.');}}catch{}finally{additionCheck=false;}}
setInterval(()=>void checkAddedOrders(),15000);

function draw(){loadingVehicleScan=null;const box=$('work'),scroll=window.scrollY;box.replaceChildren();current=projectRoute(data?.routes.find(r=>r.id===state.routeId));$('route-picker').hidden=!!current;const cards=$('route-cards');cards.replaceChildren();for(const r of data?.routes.filter(r=>r.date===$('date').value)||[]){const t=totals(r),b=button(cards,'',()=>chooseRoute(r));b.className='route-card';b.append(pickPercentage(t),el('strong',r.name),el('span',t.total+' total units - '+t.picked+' picked'+(t.short?' - '+t.short+' unavailable':'')));for(const [area,label]of Object.entries(names))b.append(el('small',label+': '+r.lines.filter(l=>l.zone===area).reduce((n,l)=>n+(l.requestedQty??l.qty),0)));b.append(el('small',r.status==='released'?'Ready to pick':r.status==='loaded'?'Loading confirmed':'Ready for customer pickup'));}if(!current){if(!cards.children.length)cards.append(el('p','No routes sent to picking for this date.'));return;}

 const heading=el('section');heading.className='pick-heading';heading.append(pickPercentage(totals(current)),el('h2',current.name));const tools=el('div');tools.className='pick-tools';button(tools,'Routes',()=>{state.routeId=null;remember();draw();});button(tools,'Refresh',refresh);heading.append(tools);box.append(heading);if(inventory?.pending){const banner=el('section');banner.className='card warning';banner.append(el('strong',inventory.conflict?'Restock needs review':'Restock waiting to sync'));const review=button(banner,'Review saved restock',()=>stockDialog(inventory,{name:'Saved restock',productId:null},current.id,{dialog,feedback}));review.disabled=false;box.append(banner);}const t=totals(current),counts=el('div');counts.className='summary-counts';counts.append(el('strong',t.picked+' picked'),el('strong',t.remaining+' remaining'),el('strong',t.short+' not picked'));box.append(counts);

 if(addedOrders(box))return;
 if((pending&&!busy&&!pending.scanId)||scanQueuePaused){const card=el('section');card.className='card warning';card.append(el('p',scanQueuePaused?'A scan was not counted. Review the warning, then continue saved scans.':'An earlier pick is waiting for confirmation.'));button(card,pending?'Retry saved scans':'Continue saved scans',()=>{scanQueuePaused=false;draw();return pending?send():drainScans();},true).disabled=busy;box.append(card);if(!scanQueuePaused)return;}

 if(scanQueue?.length){const saved=el('p',scanQueue.length+' scans saved on this device · '+(navigator.onLine===false?'offline, will sync automatically':'syncing'));saved.className='card';box.append(saved);}corrections(box);if(current.status==='loaded'){box.append(el('h2','Loading confirmed for this route'));return;}if(current.fulfillment==='customer-pickup'&&current.status==='ready-for-pickup'){box.append(el('h2','Ready for customer pickup'));const who=field(box,'Collected by'),notes=field(box,'Notes');button(box,'Confirm collected',()=>save('confirmCustomerPickup',[current.id,{collector:who.value,notes:notes.value}]),true);return;}

 if(current.hold){note('Office review is holding this route.',true);return;}if(!state.started){button(box,'Start picking',()=>{sound();state.started=true;remember();draw();},true);pickList(box);return;}

 const areaButtons=el('div');areaButtons.className='areas';for(const [area,name]of Object.entries(names)){const left=current.lines.filter(l=>l.zone===area).reduce((n,l)=>n+l.qty-l.picked,0);button(areaButtons,name+' ('+left+' left)',()=>{clearTimeout(advanceTimer);completedPick=null;awaitingCompletion=null;state.area=area;state.pickStop=null;state.focus=null;state.pallet=null;remember();draw();},state.area===area).disabled=scanQueuePaused||!!pending&&!pending.scanId;}box.append(areaButtons);const modeLabel=el('label'),mode=el('input');mode.type='checkbox';mode.checked=totalMode();mode.disabled=busy||!!pending||!!scanQueue?.length;modeLabel.append(mode,document.createTextNode('Pick whole-route product totals in '+names[state.area]));mode.onchange=()=>{state.focus=null;state.pickStop=null;save('setWarehousePickingMode',[current.id,state.area,mode.checked]);};box.append(modeLabel,el('small',totalMode()?'Combine each product across this route. Customer quantities are recorded automatically; individual-scan rules still apply.':'Pick one customer order at a time. Enable totals to build this area’s route pallet together.'));if(state.area==='freezer'){if(!state.pallet)state.pallet=remainingItems().some(l=>l.palletGroup==='tubs')?'tubs':'boxes';const pallets=el('div');pallets.className='areas';for(const group of ['tubs','boxes']){const left=remainingItems().filter(l=>l.palletGroup===group).reduce((n,l)=>n+l.qty-l.picked,0);button(pallets,palletNames[group]+' ('+left+' left)',()=>{clearTimeout(advanceTimer);completedPick=null;awaitingCompletion=null;state.pallet=group;state.focus=null;remember();draw();},state.pallet===group).disabled=scanQueuePaused||!!pending&&!pending.scanId;}box.append(pallets,el('small','Build as many pallets as needed. These are product groups; mixed pallets are allowed.'));}

 const line=completedPick||awaitingCompletion||next();if(line){state.focus=key(line);const card=el('section');card.id='active-pick';card.className='card';const summary=el('div');summary.id='active-pick-summary';card.append(summary);summary.append(el('small',totalMode()?'Whole route · '+new Set(line.members.map(m=>m.order)).size+' customer orders':'Stop '+line.stop+' - '+line.customer),el('h2',productName(line)),el('small',storageLabel(line)));if(state.area==='freezer')summary.append(el('strong',palletNames[line.palletGroup]));const remaining=el('div',(line.qty-line.picked)+' remaining');remaining.className='big-remaining';summary.append(remaining);const scanState=el('small');scanState.id='scan-save-status';scanState.textContent=scanQueue?.length?scanQueue.length+' scans saving · keep scanning this item':'All scans saved';summary.append(scanState);if(awaitingCompletion&&!completedPick){card.append(el('p','Count saved on this device · moving to next item while it syncs…'));box.append(card);recentPicks(box);pickList(box);remember();return;}if(completedPick){card.classList.add('pick-complete');const done=el('div');done.className='product-done';done.append(el('span','✓'),el('strong',scanQueue?.length?'Product counted · scans saving':'Product complete · moving to next item'));card.append(done);box.append(card);recentPicks(box);pickList(box);remember();return;}const form=el('form'),code=field(form,'Scan or type barcode / SKU');code.id='pick-code';code.autocomplete='off';const qty=field(form,'Quantity per scan',1,'number');qty.id='pick-qty';qty.min=1;qty.max=line.qty-line.picked;qty.readOnly=true;qty.parentElement.hidden=true;form.append(el('small','Bulk picking is available while more than '+line.scanLimit+' remain.'));const submit=el('button','Confirm barcode');submit.disabled=!!pending&&!pending.scanId;submit.className='primary';form.append(submit);form.onsubmit=e=>{e.preventDefault();if(code.value.trim())void scanBarcode(code.value.trim(),1,line);};card.append(form);if(line.qty-line.picked>line.scanLimit){const b=button(card,'Bulk pick · count and confirm',()=>bulkDialog(line),true);b.disabled=scanQueuePaused||!!pending&&!pending.scanId;}if(line.manualPickAllowed&&!totalMode())button(card,'Office-approved manual pick',()=>{const reason=prompt('How did you check this product?');if(reason)save('manualPick',[current.id,line.order,line.index,Number(qty.value),reason,{area:state.area,chooseItem:true}]);});if(state.area==='freezer'){const find=button(card,'Find more stock / restock walk-in',()=>stockDialog(inventory,line,current.id,{dialog,feedback}));find.disabled=false;}const cannot=button(card,'Cannot scan barcode',()=>cannotScan(line));cannot.disabled=scanQueuePaused||!!pending&&!pending.scanId;button(card,'Cannot pick remaining item',()=>chooseShortage(line));box.append(card);}else if(!totalMode()&&state.pickStop)stopComplete(box);else box.append(el('h2',state.area==='freezer'?'This product group is complete. Choose another group or area.':'This area is complete. Choose another area or item.'));

 recentPicks(box);pickList(box);

 const missing=current.lines.filter(l=>l.shortage);if(missing.length){const detail=el('details');detail.append(el('summary','Not picked ('+t.short+' units)'));for(const l of missing){const row=el('p',l.name+' - '+l.shortage.qty+' unavailable');button(row,'Restore to pick list',()=>save('clearWarehouseShortage',[current.id,l.order,l.index]));detail.append(row);}box.append(detail);}

 if(!t.remaining){if(current.fulfillment==='customer-pickup')button(box,'Mark ready for customer pickup',()=>save('confirmLoaded',[current.id,{}]),true);else{const card=el('section');card.className='card';const summary=el('div');summary.id='active-pick-summary';card.append(summary);summary.append(el('h2','Ready to load this route'));const vehicle=el('select');vehicle.append(new Option('Vehicle not recorded',''));for(const v of data.vehicles||[])vehicle.append(new Option(v.name,v.id));vehicle.value=current.vehicleId||'';loadingVehicleScan=code=>{const matches=(data.vehicles||[]).filter(v=>v.barcode&&v.barcode.trim().toUpperCase()===String(code).trim().toUpperCase());if(matches.length!==1)return feedback('Vehicle barcode not recognized. Choose the van name or continue without one.');vehicle.value=matches[0].id;feedback('Vehicle selected: '+matches[0].name,false);};card.append(el('strong','Choose vehicle by name or scan its barcode'),el('p','Vehicle tracking is optional.'),vehicle);const label=el('label','All picked pallets, including fresh fruit, are on the vehicle'),check=el('input');check.type='checkbox';label.prepend(check);card.append(label);button(card,'Confirm route loaded',()=>{if(!check.checked)throw Error('Check that all picked pallets are on the vehicle.');return save('confirmLoaded',[current.id,{freshFruit:true,...(vehicle.value?{vehicle:vehicle.value}:{})}]);},true);box.append(card);}}

 remember();window.scrollTo(0,scroll);}

window.addEventListener('zebra-scan',e=>{if((busy||pending)&&!pending?.scanId){feedback('Wait for the current action to finish before scanning.');return;}if(document.querySelector('dialog[open]')){const active=document.querySelector('dialog[open]');if(active.onBarcode){active.onBarcode(typeof e.detail==='string'?e.detail:e.detail?.barcode||'');return;}if($('bulk-pick-code')){$('bulk-pick-code').value=typeof e.detail==='string'?e.detail:e.detail?.barcode||'';return;}feedback('Choose an item or finish the open dialog before scanning.');return;}const code=typeof e.detail==='string'?e.detail:e.detail?.barcode;if(!code)return;if(loadingVehicleScan){loadingVehicleScan(code);return;}if(completedPick||awaitingCompletion){clearTimeout(advanceTimer);completedPick=null;awaitingCompletion=null;state.focus=null;draw();}const form=$('work').querySelector('form');if(!form){feedback('Choose a route and tap Start picking first.');return;}$('pick-code').value=code;form.requestSubmit();});

$('refresh').onclick=()=>refresh().catch(e=>note(e.message,true));$('date').onchange=()=>{if(pending||scanQueue?.length){$('date').value=state.date;feedback('Finish saving scans before changing the date.');return;}state.date=$('date').value;state.routeId=null;remember();draw();};window.addEventListener('online',()=>{void inventory?.send().catch(()=>{});void inventory?.refresh().catch(()=>{});void checkAddedOrders();if(!scanQueuePaused){if(pending)send();else drainScans();}});setInterval(()=>{if(navigator.onLine!==false)void inventory?.send().catch(()=>{});if(!busy&&!scanQueuePaused&&navigator.onLine!==false){if(pending?.scanId)void send();else if(scanQueue?.length)void drainScans();}},5000);refresh().catch(e=>note(e.message,true));

