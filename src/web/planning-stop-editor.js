export function mapMoveCommand(packet,date,stopId,targetId,position){
 const stop=packet.orders.find(o=>o.id===stopId),source=packet.routes.find(r=>r.id===stop?.route),target=packet.routes.find(r=>r.id===targetId);
 if(!stop||!source||source.status!=='draft'||source.date!==date||source.fulfillment==='customer-pickup')throw Error('Only draft delivery stops can move on the map.');
 if(!target||target.status!=='draft'||target.date!==date||target.fulfillment==='customer-pickup')throw Error('Choose a draft delivery route on the same date.');
 const others=packet.orders.filter(o=>o.route===targetId&&o.id!==stopId).sort((a,b)=>a.stop-b.stop);
 if(!Number.isInteger(position)||position<1||position>others.length+1)throw Error('Choose a valid delivery position.');
 return {name:'moveBefore',args:[stopId,targetId,others[position-1]?.id||null]};
}
export function stopMapPopup({packet,date,stopId,onMove}){
 const stop=packet.orders.find(o=>o.id===stopId),route=packet.routes.find(r=>r.id===stop?.route),node=(tag,text)=>{const e=document.createElement(tag);if(text!==undefined)e.textContent=text;return e;},root=node('div');root.className='map-stop-editor';
 root.append(node('strong',stop.customerName),node('p',route.name+' · Stop '+stop.stop));if(onMove&&route.status==='draft'){const address=node('details');address.append(node('summary','Delivery address'),node('p',stop.address||'No address saved'));root.append(address);}else root.append(node('p',stop.address||'No address saved'));
 if(!onMove)return root;
 if(route.status!=='draft'){root.append(node('small','Sent to picking · route order is locked.'));return root;}
 const form=node('form'),routeLabel=node('label','Route'),select=node('select'),positionLabel=node('label','Delivery order'),position=node('select'),notice=node('p'),submit=node('button','Move stop');select.setAttribute('aria-label','Route for '+stop.customerName);position.setAttribute('aria-label','Delivery order for '+stop.customerName);submit.type='submit';submit.className='primary';notice.setAttribute('role','status');
 for(const r of packet.routes.filter(r=>r.date===date&&r.status==='draft'&&r.fulfillment!=='customer-pickup')){const option=node('option',r.name);option.value=r.id;select.append(option);}select.value=route.id;
 function positions(){position.replaceChildren();const others=packet.orders.filter(o=>o.route===select.value&&o.id!==stopId).sort((a,b)=>a.stop-b.stop);for(let i=0;i<=others.length;i++){const option=node('option',String(i+1)+(others[i]?' · before '+others[i].customerName:' · last stop'));option.value=String(i+1);position.append(option);}position.value=String(select.value===route.id?Math.min(stop.stop,others.length+1):others.length+1);}
 select.onchange=positions;positions();routeLabel.append(select);positionLabel.append(position);form.append(routeLabel,positionLabel,submit,notice);root.append(form);
 form.onsubmit=async e=>{e.preventDefault();submit.disabled=true;select.disabled=position.disabled=true;notice.textContent='Saving route change…';try{const command=mapMoveCommand(packet,date,stopId,select.value,Number(position.value));await onMove(command,packet.revision);notice.textContent='Saved. Stop numbers and driving routes updated.';}catch(error){notice.textContent=error.message;}finally{submit.disabled=false;select.disabled=position.disabled=false;}};
 return root;
}
