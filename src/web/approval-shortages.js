const round=n=>Math.round((n+Number.EPSILON)*1e6)/1e6;
// A suggestion is a review-only copy. Saving still uses the normal versioned,
// audited invoice-line action; a scan count alone is never treated as a shortage.
export function withShortageSuggestions(order){
 if(!order.hasBaseline||order.handoffPending||order.status==='approved')return {...order,suggestionCount:0};
 let suggestionCount=0;
 const lines=order.lines.map(line=>{
  if(line.readOnly||line.added||line.removed||!Number.isFinite(line.originalQty)||line.qty!==line.originalQty)return line;
  const shortages=Object.entries(order.shortages||{}).filter(([key])=>key===line.id||line.productId&&key.split('::')[0]===line.productId);
  // Repeated invoice rows for the same product need office allocation, not a
  // duplicated deduction on each row.
  if(!shortages.length||order.lines.filter(l=>!l.readOnly&&(l.id===line.id||line.productId&&l.productId===line.productId)).length!==1)return line;
  let missing=0;const reasons=[];
  for(const [,x]of shortages){const amount=Number.isFinite(x.plannedQty)&&Number.isFinite(x.scanned)?Math.max(0,x.plannedQty-x.scanned):x.missing;if(!Number.isFinite(amount)||amount<=0||!x.reason)return line;missing+=amount;reasons.push(x.reason+(x.notes?' · '+x.notes:''));}
  if(missing>line.qty+.000001)return line;
  const qty=round(Math.max(0,line.qty-missing));if(qty===line.qty)return line;
  suggestionCount++;
  const reason=[line.reason,'Delivery shortage: '+[...new Set(reasons)].join('; ')].filter(Boolean).join(' · ');
  return {...line,qty,removed:qty===0,reason,shortageSuggestion:{from:line.qty,to:qty,missing:round(missing)}};
 });
 return {...order,lines,suggestionCount};
}
