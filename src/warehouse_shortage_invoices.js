import {createHash} from 'node:crypto';
import {problem} from './warehouse_auth.js';
import {warehouseReference} from './warehouse_invoice.js';
export const shortageNote=correction=>'Warehouse picking adjustment: '+correction.lines.filter(l=>l.shortage).map(l=>(l.shortage.qty)+' '+String(l.name).replace(/^\d+\.\s*[^:]+:/,'')+' not supplied ('+String(l.shortage.reason).replaceAll('-',' ')+').').join(' ')+' Invoice quantities and totals have been adjusted.';
export function shortageInvoicePlan(raw,correction,realm){
 if(String(raw.Id)!==warehouseReference(correction.qboInvoiceId,realm)||String(raw.CustomerRef?.value)!==warehouseReference(correction.qboCustomerId,realm)||String(raw.DocNumber)!==String(correction.sourceInvoice).replace(/^#\s*/,''))problem('Invoice identity changed. Review the customer invoice.',409);
 const changes=correction.lines.filter(l=>l.shortage);if(new Set(changes.map(l=>warehouseReference(l.qboItemId,realm))).size!==changes.length)problem('This product appears on multiple picking lines. Office correction is needed.',409);if(!changes.length)problem('No warehouse shortages to correct.');
 const lines=structuredClone(raw.Line||[]),verifiedQuantities={},adjustments=[];let changed=false;
 for(const change of changes){const item=warehouseReference(change.qboItemId,realm),matches=lines.filter(l=>l.DetailType==='SalesItemLineDetail'&&String(l.SalesItemLineDetail?.ItemRef?.value)===item);if(matches.length>1)problem('This product has multiple invoice lines. Office correction is needed for this invoice.',409);
  if(!Number.isInteger(change.qty)||change.qty<0||change.qty>=change.requestedQty)problem('Only a recorded warehouse shortage can reduce an invoice.');
  if(!matches.length){if(change.qty!==0)problem('A product line changed in QuickBooks. Review the invoice.',409);verifiedQuantities[change.qboItemId]=0;continue;}
  const line=matches[0],detail=line.SalesItemLineDetail,current=Number(detail.Qty);if(current!==change.requestedQty&&current!==change.qty)problem('The invoice quantity changed in QuickBooks. Review it before applying a shortage.',409);
  const rate=Number(detail.UnitPrice);if(!Number.isFinite(rate)||rate<0)problem('Review the current invoice price.',409);const amount=Math.round(change.qty*rate*100)/100;if(current!==change.qty||Number(line.Amount)!==amount){detail.Qty=change.qty;line.Amount=amount;changed=true;}
  verifiedQuantities[change.qboItemId]=change.qty;adjustments.push({name:change.name,from:change.requestedQty,to:change.qty,reason:change.shortage.reason,notes:change.shortage.notes||'',unitPrice:rate});
 }
 // CustomerMemo is the customer-visible message. Existing delivery notes remain verbatim.
 const note=shortageNote(correction),existing=String(raw.CustomerMemo?.value||''),memo=existing.includes(note)?existing:existing+(existing?'\n\n':'')+note;if(memo.length>1000)problem('The customer note is too long to append safely. Review this invoice in the office.',409);
 const memoChanged=memo!==existing;
 // Recalculate explicit subtotal/percentage-discount lines while preserving fixed fees and prices.
 let subtotal=0;for(const line of lines){if(line.DetailType==='SubTotalLineDetail'){line.Amount=Math.round(subtotal*100)/100;}else if(line.DetailType==='DiscountLineDetail'&&line.DiscountLineDetail?.PercentBased){line.Amount=Math.round(subtotal*Number(line.DiscountLineDetail.DiscountPercent||0))/100;subtotal-=line.Amount;}else if(line.DetailType==='SalesItemLineDetail')subtotal+=Number(line.Amount)||0;}
 return {payload:changed||memoChanged?{Id:raw.Id,SyncToken:raw.SyncToken,sparse:true,Line:lines,CustomerMemo:{value:memo}}:null,verifiedQuantities,note,adjustments,signature:createHash('sha256').update(JSON.stringify({order:correction.orderId,revision:correction.revision,changes})).digest('hex')};
}
export function verifyShortageInvoice(raw,correction,realm){const plan=shortageInvoicePlan(raw,correction,realm);if(plan.payload)problem('QuickBooks has not confirmed the shortage quantities and customer note. Retry confirmation.',409);return plan;}
