const same=(a,b)=>JSON.stringify(a??'')===JSON.stringify(b??'');
export function productSaveGuard(product,command){
 if(command.name==='saveProductStorage')return {zone:product.zone||'',fresh:!!product.fresh};
 if(command.name==='saveWarehouseProduct')return Object.fromEntries(Object.keys(command.args[1]).map(k=>[k,k==='fresh'?!!product[k]:product[k]||'']));
 return {};
}
export async function sendProductChange(request,{post,read,remember,newId=()=>crypto.randomUUID()}){
 try{return await post(request);}catch(error){
  if(error.status!==409)throw error;
  const latest=await read(),product=latest.products.find(p=>p.id===request.command.args[0]);
  if(!product||!request.productGuard||Object.entries(request.productGuard).some(([k,v])=>!same(k==='fresh'?!!product[k]:product[k]||'',v)&&!same(product[k],request.command.args[1]?.[k]))){throw Object.assign(Error('This product changed. Close and reopen it to review the latest details before saving.'),{status:409,latest});}
  const next={...request,baseRevision:latest.revision,requestId:newId()};await remember(next);
  return post(next);
 }
}
