// Only an explicitly rejected saved-name addition can safely use a newer board.
// Uncertain network results always retain their exact request for receipt replay.
export function moveGuard(board,command){
 if(command?.name!=='moveBefore')return null;
 const [id,target]=command.args,order=board.orders.find(o=>o.id===id),routes=[order?.route,target].filter(Boolean);
 if(!order)return null;
 return JSON.stringify({order:[order.id,order.route||'',order.stop,order.locationId||''],routes:[...new Set(routes)].sort().map(id=>{const route=board.routes.find(r=>r.id===id);return {id,status:route?.status,fulfillment:route?.fulfillment,stops:board.orders.filter(o=>o.route===id).sort((a,b)=>a.stop-b.stop).map(o=>[o.id,o.stop,o.locationId||''])};})});
}
export async function sendPlanningChange(request,{post,read,remember,newId=()=>crypto.randomUUID()}){
 try{return await post(request);}catch(error){
  const presetAddition=request.command?.name==='createPresetRoute',moving=request.command?.name==='moveBefore'&&request.moveGuard;
  if(error.status!==409||!presetAddition&&!moving)throw error;
  const latest=await read();
  if(presetAddition){const choice=request.command.args?.[0],preset=latest.templates.find(t=>t.id===choice?.templateId);if(!preset)throw error;if(latest.routes.some(r=>r.date===choice.date&&r.name.trim().toUpperCase()===preset.name.trim().toUpperCase()))return {...latest,message:'This saved route is already on the selected date. Board refreshed.'};}
  else if(moveGuard(latest,request.command)!==request.moveGuard)throw error;
  const replacement={...request,requestId:newId(),baseRevision:latest.revision};
  remember(replacement);
  return post(replacement);
 }
}
