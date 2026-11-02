// Only an explicitly rejected saved-name addition can safely use a newer board.
// Uncertain network results always retain their exact request for receipt replay.
export async function sendPlanningChange(request,{post,read,remember,newId=()=>crypto.randomUUID()}){
 try{return await post(request);}catch(error){
  if(error.status!==409||request.command?.name!=='createPresetRoute')throw error;
  const latest=await read(),choice=request.command.args?.[0],preset=latest.templates.find(t=>t.id===choice?.templateId);
  if(!preset)throw error;
  if(latest.routes.some(r=>r.date===choice.date&&r.name.trim().toUpperCase()===preset.name.trim().toUpperCase()))return {...latest,message:'This saved route is already on the selected date. Board refreshed.'};
  const replacement={...request,requestId:newId(),baseRevision:latest.revision};
  remember(replacement);
  return post(replacement);
 }
}
