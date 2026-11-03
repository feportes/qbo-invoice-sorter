import {PickingStock,stockDialog} from './picking-stock.js';
import {WarehouseScanQueue} from './warehouse-scan-queue.js';

import {scanErrorSound} from './scan-error-sound.js';

import {areaPickItems,freezerPalletGroup,palletNames,productKey} from './warehouse-picking-groups.js';

const $=id=>document.getElementById(id),names={freezer:'Freezer',dry:'Dry',refrigerated:'Refrigerated / fresh'},key=l=>l.key||l.order+':'+l.index;

let inventory;
let session,data,current,state={},pending,busy=false,storeKey,requestKey,feedbackTimer,completedPick,advanceTimer,scanQueue,scanQueuePaused=false;

const el=(tag,text)=>{const n=document.createElement(tag);if(text!==undefined)n.textContent=text;return n;};

function remember(){if(storeKey)localStorage.setItem(storeKey,JSON.stringify(state));}

function note(text,error=false){$('message').textContent=text;$('message').className=error?'card warning':'card';}

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
function chooseRoute(r){sound();clearTimeout(advanceTimer);completedPick=null;state={date:r.date,routeId:r.id,area:r.lines.find(l=>l.picked<l.qty)?.zone||'freezer',started:false};$('date').value=r.date;remember();draw();}

function totalMode(){return current?.pickingModes?.[state.area]===true;}

function remainingItems(){return areaPickItems(current.lines,state.area,totalMode());}

function next(){const all=remainingItems();if(state.area==='freezer'&&!state.pallet)state.pallet=all.find(l=>l.palletGroup==='tubs')?'tubs':'boxes';const lines=state.area==='freezer'?all.filter(l=>l.palletGroup===state.pallet):all;return lines.find(l=>key(l)===state.focus)||lines[0];}



async function save(name,args){if(busy||pending||scanQueue?.length)return false;pending={requestId:crypto.randomUUID(),baseRevision:data.revision,command:{name,args}};try{localStorage.setItem(requestKey,JSON.stringify(pending));}catch{pending=null;feedback('Device storage is unavailable. Nothing was submitted.');return false;}return send();}

function scanStatus(){const n=scanQueue?.length||0;const label=$('scan-save-status');if(label)label.textContent=n?n+' scans saving · keep scanning this item':'All scans saved';}

function enqueueScan(code,qty,line){

 try{

  if(!scanQueue)throw Error('Refresh the page before scanning.');

  if(pending&&!pending.scanId)throw Error('Wait for the current action to finish.');

  if(scanQueuePaused)throw Error('Review the scan warning and resume saved scans first.');

  if(!Number.isInteger(qty)||qty<1)throw Error('Enter a positive whole quantity.');if(qty>line.qty-line.picked)throw Error('Only '+(line.qty-line.picked)+' remain.');if(qty>1&&line.qty-line.picked<=line.scanLimit)throw Error('Scan each unit individually at or below this product threshold.');if(Array.isArray(line.scanCodes)&&!line.scanCodes.some(c=>String(c).trim().toUpperCase()===code.trim().toUpperCase())&&(navigator.onLine===false||data.routes.some(r=>r.lines.some(l=>(l.productId!==line.productId||l.packId!==line.packId)&&l.scanCodes?.some(c=>String(c).trim().toUpperCase()===code.trim().toUpperCase())))))throw Error('Wrong or unregistered barcode. Scan '+line.name+'. Reconnect for a new barcode lookup.');

  scanQueue.add({id:crypto.randomUUID(),target:key(line),area:state.area,args:[current.id,totalMode()?line.productId:line.order,totalMode()?line.packId:line.index,code,qty,{bulk:qty>1,area:state.area,chooseItem:true}],name:totalMode()?'scanWarehouseProduct':'scan',line:structuredClone(line)});

  if(qty===line.qty-line.picked){feedback(line.name+' counted. Moving to the next product.',false);completedPick={...line,picked:line.qty};clearTimeout(advanceTimer);advanceTimer=setTimeout(()=>{completedPick=null;state.focus=null;draw();focusCurrentPick();},1800);}if($('pick-code'))$('pick-code').value='';if($('pick-qty'))$('pick-qty').value=1;draw();scanStatus();void drainScans();return true;

 }catch(e){feedback(e.message);focusCurrentPick();return false;}

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

 const operation=pending,name=operation.command.name,isScan=['scan','scanWarehouseProduct'].includes(name),prior=operation.scanLine||(current?next():null),scroll=window.scrollY;

 if(!isScan)draw();else scanStatus();let accepted=false;

 try{

  const result=await api('/picking',operation);

  // Acknowledge queue before clearing exact replay request. Crashes can only replay the same UUID.

  if(operation.scanId)scanQueue.remove(operation.scanId);

  localStorage.removeItem(requestKey);pending=null;data=result;note(data.message||'Saved.');

  const route=data.routes.find(r=>r.id===state.routeId),line=prior&&route?(name==='scanWarehouseProduct'?areaPickItems(route.lines,state.area,true).find(l=>key(l)===key(prior)):route.lines.find(l=>key(l)===key(prior))):null;

  if(isScan&&prior&&!scanQueue?.length&&state.focus===key(prior)&&(!line||line.picked===line.qty)){

   completedPick={...prior,picked:prior.qty};feedback(prior.name+' complete. Moving to the next item.',false);clearTimeout(advanceTimer);advanceTimer=setTimeout(()=>{completedPick=null;state.focus=null;draw();focusCurrentPick();},1800);

  }

  accepted=true;return true;

 }catch(e){

  if([400,403,404,409,422].includes(e.status)){

   try{

    if(operation.scanId){const k='draw-warehouse-rejected:'+session.user.id,failed=JSON.parse(localStorage.getItem(k)||'[]');localStorage.setItem(k,JSON.stringify([...failed,{...operation,error:e.message,time:new Date().toISOString()}]));scanQueue.remove(operation.scanId);scanQueuePaused=true;}

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

  if(accepted&&scanQueue?.length&&!scanQueuePaused)queueMicrotask(drainScans);

 }

}

function projectRoute(route){

 if(!route)return route;const r=structuredClone(route);

 for(const read of scanQueue?.items||[]){if(read.args[0]!==r.id)continue;let qty=read.args[4];const matches=r.lines.filter(l=>read.name==='scanWarehouseProduct'?l.productId===read.args[1]&&l.packId===read.args[2]&&l.zone===read.area:l.order===read.args[1]&&l.index===read.args[2]);for(const l of matches){const n=Math.min(qty,l.qty-l.picked);l.picked+=n;qty-=n;}}

 return r;

}

function bulkDialog(line){dialog('Bulk pick · '+line.name,(body,actions,d)=>{

 const count=field(body,'Counted units',line.qty-line.picked,'number');count.min=2;count.max=line.qty-line.picked;const barcode=field(body,'Scan or enter this product barcode');barcode.id='bulk-pick-code';

 const label=el('label'),check=el('input');check.type='checkbox';label.append(check,document.createTextNode('I counted and double-checked every unit.'));body.append(label);

 const confirm=button(actions,'Confirm bulk pick',()=>{if(!check.checked)throw Error('Double-check the physical count and tick the confirmation.');const n=Number(count.value);if(!Number.isInteger(n)||n<2||n>line.qty-line.picked)throw Error('Enter a whole count from 2 to '+(line.qty-line.picked)+'.');if(!barcode.value.trim())throw Error('Scan the product barcode first.');if(enqueueScan(barcode.value.trim(),n,line))d.close();},true);confirm.disabled=false;

 });}

function dialog(title,build){const d=el('dialog'),heading=el('h2',title),body=el('div'),actions=el('div');heading.tabIndex=-1;heading.autofocus=true;actions.className='dialog-actions';d.append(heading,body,actions);build(body,actions,d);const cancel=el('button','Cancel');cancel.type='button';cancel.onclick=()=>d.close();actions.append(cancel);const hide=el('button','Hide keyboard');hide.type='button';hide.onclick=()=>{document.activeElement?.blur();window.DrawKeyboard?.hideKeyboard?.();};actions.append(hide);d.onclick=e=>{if(e.target!==d)return;const b=d.getBoundingClientRect();if(e.clientX<b.left||e.clientX>b.right||e.clientY<b.top||e.clientY>b.bottom)d.close();};d.onclose=()=>d.remove();document.body.append(d);d.showModal();heading.focus();return d;}

function pickList(parent){const items=remainingItems(),detail=el('details');detail.className='remaining-list';detail.open=true;detail.append(el('summary','Remaining in '+names[state.area]+' · '+items.length+' '+(totalMode()?(items.length===1?'product':'products'):(items.length===1?'customer item':'customer items'))));const body=el('div');body.className='pick-list';if(!items.length)body.append(el('p','Everything in this area has been picked.'));const groups=state.area==='freezer'?['tubs','boxes']:[null];for(const group of groups){const rows=group?items.filter(l=>l.palletGroup===group):items;if(group&&rows.length)body.append(el('h3',palletNames[group]));for(const l of rows){const b=button(body,l.name+' — '+(l.qty-l.picked)+' remaining',()=>{state.focus=key(l);state.pallet=l.palletGroup;state.started=true;remember();draw();focusCurrentPick();});b.disabled=scanQueuePaused||!!pending&&!pending.scanId;b.append(el('small',totalMode()?'Whole route · '+new Set(l.members.map(m=>m.order)).size+' customer orders':'Stop '+l.stop+' · '+l.customer));}}detail.append(body);parent.append(detail);}

function chooseShortage(line){if(!totalMode())return shortage(line);dialog('Choose the customer shortage',(body,actions,d)=>{body.append(el('p','Record the missing quantity for the affected customer order.'));for(const l of line.members.filter(l=>l.picked<l.qty))button(body,l.customer+' · '+(l.qty-l.picked)+' remaining',()=>{d.close();shortage(l);});});}



function shortage(l){dialog('Mark remaining item not picked',(body,actions,d)=>{body.append(el('strong',l.name),el('p',(l.qty-l.picked)+' remaining units for '+l.customer+' will be removed from this shipment.'));const reason=el('select');reason.setAttribute('aria-label','Shortage reason');for(const [id,name]of[['out-of-stock','Out of stock'],['expired','Expired'],['damaged','Damaged'],['not-found','Could not find'],['other','Other']])reason.append(new Option(name,id));const label=el('label','Reason');label.append(reason);body.append(label);const notes=el('textarea');notes.placeholder='Notes (required for Other)';body.append(notes);button(actions,'Confirm not picked',async()=>{if(reason.value==='other'&&notes.value.trim().length<5)throw Error('Explain the shortage reason.');if(await save('recordWarehouseShortage',[current.id,l.order,l.index,{reason:reason.value,notes:notes.value}])){d.close();feedback('Shortage recorded. Continue picking the other items.',false);}},true);});}

async function correctInvoices(){if(busy||pending)return;const id=current.id;busy=true;draw();try{data=await api('/picking/'+encodeURIComponent(id)+'/correct-invoices',{});note('Corrected invoices confirmed in QuickBooks. Reprint the customer copies.');feedback('Invoice quantities and customer notes updated.',false);}catch(e){try{data=await api('/picking');}catch{}feedback(e.message);}finally{busy=false;draw();}}

function corrections(parent){const rows=current.corrections||[];if(!rows.length)return;const card=el('section');card.className='card not-picked';card.append(el('h2','Not-picked items and customer invoices'));for(const row of rows){card.append(el('strong',row.customer+' - Invoice '+row.sourceInvoice));for(const line of row.lines.filter(l=>l.shortage))card.append(el('p',line.shortage.qty+' not picked: '+line.name+' - '+line.shortage.reason.replaceAll('-',' ')));card.append(el('small',row.status==='corrected'?'QuickBooks corrected - office observation recorded':'Correction needed before departure'));}if(['loaded','ready-for-pickup'].includes(current.status)){if(rows.some(r=>r.status!=='corrected'))button(card,'Correct shortage invoices in QuickBooks',correctInvoices,true);else{const a=el('a','Reprint corrected customer invoices (2 copies)');a.href='/api/warehouse/picking/'+encodeURIComponent(current.id)+'/corrected-invoices.pdf';a.target='_blank';a.rel='noopener';card.append(a);}}else card.append(el('p','After loading, correct and reprint the affected customer invoices. No office approval is required.'));parent.append(card);}

function draw(){const box=$('work'),scroll=window.scrollY;box.replaceChildren();current=projectRoute(data?.routes.find(r=>r.id===state.routeId));$('route-picker').hidden=!!current;const cards=$('route-cards');cards.replaceChildren();for(const r of data?.routes.filter(r=>r.date===$('date').value)||[]){const t=totals(r),b=button(cards,'',()=>chooseRoute(r));b.className='route-card';b.append(pickPercentage(t),el('strong',r.name),el('span',t.total+' total units - '+t.picked+' picked'+(t.short?' - '+t.short+' unavailable':'')));for(const [area,label]of Object.entries(names))b.append(el('small',label+': '+r.lines.filter(l=>l.zone===area).reduce((n,l)=>n+(l.requestedQty??l.qty),0)));b.append(el('small',r.status==='released'?'Ready to pick':r.status==='loaded'?'Loading confirmed':'Ready for customer pickup'));}if(!current){if(!cards.children.length)cards.append(el('p','No routes sent to picking for this date.'));return;}

 const heading=el('section');heading.className='pick-heading';heading.append(pickPercentage(totals(current)),el('h2',current.name));button(heading,'Choose another route',()=>{state.routeId=null;remember();draw();});button(heading,'Refresh counts',refresh);box.append(heading);if(inventory?.pending){const banner=el('section');banner.className='card warning';banner.append(el('strong',inventory.conflict?'Restock needs review':'Restock waiting to sync'));const review=button(banner,'Review saved restock',()=>stockDialog(inventory,{name:'Saved restock',productId:null},current.id,{dialog,feedback}));review.disabled=false;box.append(banner);}const t=totals(current),counts=el('div');counts.className='summary-counts';counts.append(el('strong',t.picked+' picked'),el('strong',t.remaining+' remaining'),el('strong',t.short+' not picked'));box.append(counts);

 if((pending&&!busy&&!pending.scanId)||scanQueuePaused){const card=el('section');card.className='card warning';card.append(el('p',scanQueuePaused?'A scan was not counted. Review the warning, then continue saved scans.':'An earlier pick is waiting for confirmation.'));button(card,pending?'Retry saved scans':'Continue saved scans',()=>{scanQueuePaused=false;draw();return pending?send():drainScans();},true).disabled=busy;box.append(card);return;}

 if(scanQueue?.length){const saved=el('p',scanQueue.length+' scans saved on this device · '+(navigator.onLine===false?'offline, will sync automatically':'syncing'));saved.className='card';box.append(saved);}corrections(box);if(current.status==='loaded'){box.append(el('h2','Loading confirmed for this route'));return;}if(current.fulfillment==='customer-pickup'&&current.status==='ready-for-pickup'){box.append(el('h2','Ready for customer pickup'));const who=field(box,'Collected by'),notes=field(box,'Notes');button(box,'Confirm collected',()=>save('confirmCustomerPickup',[current.id,{collector:who.value,notes:notes.value}]),true);return;}

 if(current.hold){note('Office review is holding this route.',true);return;}if(!state.started){button(box,'Start picking',()=>{sound();state.started=true;remember();draw();},true);pickList(box);return;}

 const areaButtons=el('div');areaButtons.className='areas';for(const [area,name]of Object.entries(names)){const left=current.lines.filter(l=>l.zone===area).reduce((n,l)=>n+l.qty-l.picked,0);button(areaButtons,name+' ('+left+' left)',()=>{clearTimeout(advanceTimer);completedPick=null;state.area=area;state.focus=null;state.pallet=null;remember();draw();},state.area===area);}box.append(areaButtons);const modeLabel=el('label'),mode=el('input');mode.type='checkbox';mode.checked=totalMode();mode.disabled=busy||!!pending||!!scanQueue?.length;modeLabel.append(mode,document.createTextNode('Pick whole-route product totals in '+names[state.area]));mode.onchange=()=>{state.focus=null;save('setWarehousePickingMode',[current.id,state.area,mode.checked]);};box.append(modeLabel,el('small',totalMode()?'Combine each product across this route. Customer quantities are recorded automatically; individual-scan rules still apply.':'Pick one customer order at a time. Enable totals to build this area’s route pallet together.'));if(state.area==='freezer'){if(!state.pallet)state.pallet=remainingItems().some(l=>l.palletGroup==='tubs')?'tubs':'boxes';const pallets=el('div');pallets.className='areas';for(const group of ['tubs','boxes']){const left=remainingItems().filter(l=>l.palletGroup===group).reduce((n,l)=>n+l.qty-l.picked,0);button(pallets,palletNames[group]+' ('+left+' left)',()=>{state.pallet=group;state.focus=null;remember();draw();},state.pallet===group);}box.append(pallets,el('small','Build as many pallets as needed. These are product groups; mixed pallets are allowed.'));}

 const line=completedPick||next();if(line){state.focus=key(line);const card=el('section');card.id='active-pick';card.className='card';const summary=el('div');summary.id='active-pick-summary';card.append(summary);summary.append(el('small',totalMode()?'Whole route · '+new Set(line.members.map(m=>m.order)).size+' customer orders':'Stop '+line.stop+' - '+line.customer),el('h2',line.name));if(state.area==='freezer')summary.append(el('strong',palletNames[line.palletGroup]));const remaining=el('div',(line.qty-line.picked)+' remaining');remaining.className='big-remaining';summary.append(remaining);const scanState=el('small');scanState.id='scan-save-status';scanState.textContent=scanQueue?.length?scanQueue.length+' scans saving · keep scanning this item':'All scans saved';summary.append(scanState);if(completedPick){card.classList.add('pick-complete');const done=el('div');done.className='product-done';done.append(el('span','✓'),el('strong',scanQueue?.length?'Product counted · scans saving':'Product complete · moving to next item'));card.append(done);box.append(card);pickList(box);remember();return;}const form=el('form'),code=field(form,'Scan or type barcode / SKU');code.id='pick-code';code.autocomplete='off';const qty=field(form,'Quantity per scan',1,'number');qty.id='pick-qty';qty.min=1;qty.max=line.qty-line.picked;qty.readOnly=true;qty.parentElement.hidden=true;form.append(el('small','Bulk picking is available while more than '+line.scanLimit+' remain.'));const submit=el('button','Confirm barcode');submit.disabled=!!pending&&!pending.scanId;submit.className='primary';form.append(submit);form.onsubmit=e=>{e.preventDefault();if(code.value.trim())enqueueScan(code.value.trim(),1,line);};card.append(form);if(line.qty-line.picked>line.scanLimit){const b=button(card,'Bulk pick · count and confirm',()=>bulkDialog(line),true);b.disabled=scanQueuePaused||!!pending&&!pending.scanId;}if(line.manualPickAllowed&&!totalMode())button(card,'Office-approved manual pick',()=>{const reason=prompt('How did you check this product?');if(reason)save('manualPick',[current.id,line.order,line.index,Number(qty.value),reason,{area:state.area,chooseItem:true}]);});if(state.area==='freezer'){const find=button(card,'Find more stock / restock walk-in',()=>stockDialog(inventory,line,current.id,{dialog,feedback}));find.disabled=false;}button(card,'Cannot pick remaining item',()=>chooseShortage(line));box.append(card);}else box.append(el('h2',state.area==='freezer'?'This product group is complete. Choose another group or area.':'This area is complete. Choose another area or item.'));

 pickList(box);

 const missing=current.lines.filter(l=>l.shortage);if(missing.length){const detail=el('details');detail.append(el('summary','Not picked ('+t.short+' units)'));for(const l of missing){const row=el('p',l.name+' - '+l.shortage.qty+' unavailable');button(row,'Restore to pick list',()=>save('clearWarehouseShortage',[current.id,l.order,l.index]));detail.append(row);}box.append(detail);}

 if(!t.remaining){if(current.fulfillment==='customer-pickup')button(box,'Mark ready for customer pickup',()=>save('confirmLoaded',[current.id,{}]),true);else{const card=el('section');card.className='card';const summary=el('div');summary.id='active-pick-summary';card.append(summary);summary.append(el('h2','Ready to load this route'));const vehicle=el('select');vehicle.append(new Option('Vehicle not recorded',''));for(const v of data.vehicles||[])vehicle.append(new Option(v.name,v.id));card.append(el('p','Vehicle tracking is optional.'),vehicle);const label=el('label','All picked pallets, including fresh fruit, are on the vehicle'),check=el('input');check.type='checkbox';label.prepend(check);card.append(label);button(card,'Confirm route loaded',()=>{if(!check.checked)throw Error('Check that all picked pallets are on the vehicle.');return save('confirmLoaded',[current.id,{freshFruit:true,...(vehicle.value?{vehicle:vehicle.value}:{})}]);},true);box.append(card);}}

 remember();window.scrollTo(0,scroll);}

window.addEventListener('zebra-scan',e=>{if((busy||pending)&&!pending?.scanId){feedback('Wait for the current action to finish before scanning.');return;}if(document.querySelector('dialog[open]')){if($('bulk-pick-code')){$('bulk-pick-code').value=typeof e.detail==='string'?e.detail:e.detail?.barcode||'';return;}feedback('Choose an item or finish the open dialog before scanning.');return;}const code=typeof e.detail==='string'?e.detail:e.detail?.barcode;if(!code)return;if(completedPick){clearTimeout(advanceTimer);completedPick=null;state.focus=null;draw();}const form=$('work').querySelector('form');if(!form){feedback('Choose a route and tap Start picking first.');return;}$('pick-code').value=code;form.requestSubmit();});

$('refresh').onclick=()=>refresh().catch(e=>note(e.message,true));$('date').onchange=()=>{if(pending||scanQueue?.length){$('date').value=state.date;feedback('Finish saving scans before changing the date.');return;}state.date=$('date').value;state.routeId=null;remember();draw();};window.addEventListener('online',()=>{void inventory?.send().catch(()=>{});void inventory?.refresh().catch(()=>{});if(!scanQueuePaused){if(pending)send();else drainScans();}});setInterval(()=>{if(navigator.onLine!==false)void inventory?.send().catch(()=>{});if(!busy&&!scanQueuePaused&&navigator.onLine!==false){if(pending?.scanId)void send();else if(scanQueue?.length)void drainScans();}},5000);refresh().catch(e=>note(e.message,true));

