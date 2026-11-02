// Pointer events work with a mouse and with the TC26/WebView touch screen.
export function installPlanningDrag({getRoutes,getOrder,isBusy,onMove,onError}){
 let drag=null,frame=0;
 const marker=document.createElement('div');marker.className='drop-marker';
 function targetAt(x,y){
  const element=document.elementFromPoint(x,y),zone=element?.closest('[data-drop-route]');if(!zone)return null;
  const route=zone.dataset.dropRoute;
  if(route==='__unassigned')return getOrder(drag.id)?.manual||!getOrder(drag.id)?.route?null:{route,before:null,zone};
  if(!getRoutes().some(r=>r.id===route))return null;
  const card=document.querySelector('.route-card[data-drop-route="'+CSS.escape(route)+'"]');
  const list=card?.querySelector('.route-stop-list');if(!list)return null;
  const collapsed=card.querySelector('.route-body').hidden;
  const stop=collapsed||zone.classList.contains('drag-route-chip')?null:[...list.querySelectorAll('[data-stop-id]')].filter(n=>n.dataset.stopId!==drag.id).find(n=>{const b=n.getBoundingClientRect();return y<b.top+b.height/2;});
  return {route,before:stop?.dataset.stopId||null,zone:card,visual:zone,list,stop,collapsed};
 }
 function update(){
  if(!drag?.started)return;
  drag.ghost.style.transform='translate('+Math.min(innerWidth-220,Math.max(8,drag.x+15))+'px,'+Math.max(8,drag.y-65)+'px)';
  document.querySelectorAll('.drop-active').forEach(n=>n.classList.remove('drop-active'));
  marker.remove();drag.target=targetAt(drag.x,drag.y);
  if(drag.target){const t=drag.target;t.zone.classList.add('drop-active');t.visual?.classList.add('drop-active');if(!t.collapsed&&t.list){if(t.stop)t.list.insertBefore(marker,t.stop);else t.list.append(marker);}}
 }
 function tick(){
  if(!drag?.started)return;
  const bottom=Math.min(innerHeight,drag.tray.getBoundingClientRect().top);
  if(drag.y<80)scrollBy(0,-Math.max(8,(80-drag.y)/4));else if(drag.y>bottom-60&&drag.y<bottom)scrollBy(0,Math.max(8,(drag.y-bottom+60)/4));
  update();frame=requestAnimationFrame(tick);
 }
 function start(){
  drag.started=true;document.activeElement?.blur();drag.card.classList.add('dragging-stop');
  const ghost=document.createElement('div');ghost.className='drag-ghost';ghost.textContent=getOrder(drag.id)?.customerName||'Move stop';document.body.append(ghost);drag.ghost=ghost;
  const tray=document.createElement('div');tray.className='drag-route-tray';const label=document.createElement('strong');label.textContent='Drop onto a route';tray.append(label);
  for(const r of getRoutes())addChip(r.id,r.name);
  if(getOrder(drag.id)?.route&&!getOrder(drag.id)?.manual)addChip('__unassigned','Unassigned');
  function addChip(id,name){const chip=document.createElement('div');chip.className='drag-route-chip';chip.dataset.dropRoute=id;chip.textContent=name;tray.append(chip);}
  document.body.append(tray);drag.tray=tray;update();frame=requestAnimationFrame(tick);
 }
 function cleanup(){
  cancelAnimationFrame(frame);marker.remove();document.querySelectorAll('.drop-active').forEach(n=>n.classList.remove('drop-active'));
  if(drag){drag.card.classList.remove('dragging-stop');drag.ghost?.remove();drag.tray?.remove();try{drag.handle.releasePointerCapture(drag.pointer);}catch{}}
 }
 document.addEventListener('pointerdown',event=>{
  const handle=event.target.closest('[data-drag-stop]');if(!handle||isBusy()||handle.disabled||event.button!==0)return;
  drag={id:handle.dataset.dragStop,handle,card:handle.closest('[data-stop-id]'),pointer:event.pointerId,startX:event.clientX,startY:event.clientY,x:event.clientX,y:event.clientY,started:false};handle.setPointerCapture(event.pointerId);
 });
 document.addEventListener('pointermove',event=>{
  if(!drag||drag.pointer!==event.pointerId)return;drag.x=event.clientX;drag.y=event.clientY;
  if(!drag.started&&Math.hypot(drag.x-drag.startX,drag.y-drag.startY)>8)start();if(drag.started){event.preventDefault();update();}
 },{passive:false});
 document.addEventListener('pointerup',event=>{
  if(!drag||drag.pointer!==event.pointerId)return;const old=drag;if(old.started)update();const target=old.target;cleanup();drag=null;
  if(old.started&&target)Promise.resolve().then(()=>onMove(old.id,target.route,target.before)).catch(error=>onError(error.message));
 });
 document.addEventListener('pointercancel',()=>{cleanup();drag=null;});
 document.addEventListener('keydown',event=>{if(event.key==='Escape'&&drag){cleanup();drag=null;}});
}
