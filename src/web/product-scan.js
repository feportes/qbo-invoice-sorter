const normalized=value=>String(value??'').trim().toUpperCase();

// Scanning inside an editor adds an alias to that product; it never navigates away.
export function productScanAction(products,value,editingId=null){
 const code=typeof value==='string'?value.trim():'';
 if(!code)return {kind:'empty',code};
 const key=normalized(code),matches=products.filter(p=>normalized(p.sku)===key||(p.barcodes||[]).some(b=>normalized(b.code)===key));
 if(editingId){
  const others=matches.filter(p=>p.id!==editingId);
  if(others.length)return {kind:'conflict',code,products:others};
  return {kind:matches.length?'already':'fill',code};
 }
 return {kind:matches.length===1?'open':matches.length?'ambiguous':'unknown',code,products:matches};
}
