import {problem} from './warehouse_auth.js';
import {warehouseReference} from './warehouse_invoice.js';
const round=(n,d=6)=>Math.round((n+Number.EPSILON)*10**d)/10**d;
const valid=n=>typeof n==='number'&&Number.isFinite(n)&&n>=0;
export function deliveryInvoicePlan(raw,order,realm){
 if(String(raw.Id)!==warehouseReference(order.qboInvoiceId,realm)||String(raw.CustomerRef?.value)!==warehouseReference(order.qboCustomerId,realm)||String(raw.DocNumber)!==String(order.invoice||order.sourceInvoice).replace(/^#\s*/,''))problem('The QuickBooks invoice or customer identity changed.',409);
 if(order.status!=='approved'||order.approved?.version!==order.version||!order.baseline?.lines)problem('Approve the latest invoice correction first.',409);
 const baseline=order.baseline.lines,target=order.approved.lines.filter(l=>!l.removed&&l.qty>0||l.readOnly&&!l.removed),sales=(raw.Line||[]).filter(l=>l.DetailType==='SalesItemLineDetail');
 if((raw.Line||[]).some(l=>!['SalesItemLineDetail','SubTotalLineDetail'].includes(l.DetailType)))problem('This invoice has an accounting line that needs review in QuickBooks.',409);
 if(!target.length)problem('All invoice items were removed. Review or void this invoice in QuickBooks.',409);
 const canonical=l=>[warehouseReference(l.qboItemId,realm),round(l.qty),round(l.unitRate),l.description||'',l.taxable,l.classId?warehouseReference(l.classId,realm):null];
 const sig=lines=>JSON.stringify(lines.map(canonical).map(JSON.stringify).sort());
 const current=sales.map(l=>{const d=l.SalesItemLineDetail||{},old=baseline.find(x=>x.qboLineId===String(l.Id)),flat=old?.readOnly&&(Number(d.Qty||0)<=0||Math.abs(Number(d.Qty)*Number(d.UnitPrice)-Number(l.Amount))>.005),tax=d.TaxCodeRef?.value;return {qboItemId:String(d.ItemRef?.value||''),qty:flat?1:Number(d.Qty),unitRate:flat?Number(l.Amount):Number(d.UnitPrice),description:l.Description||'',taxable:tax==='TAX'?true:tax==='NON'?false:null,classId:d.ClassRef?.value||null};});
 for(const l of target)if(!valid(l.qty)||!valid(l.unitRate)||typeof l.taxable!=='boolean')problem('Review the approved product, quantity, case price and tax setting.',409);
 if(sig(current)===sig(target))return {payload:null,matched:true};
 if(sig(current)!==sig(baseline))problem('This invoice changed directly in QuickBooks. Review those changes before updating it; nothing was overwritten.',409);
 for(const l of target)if(l.qboLineId&&!sales.some(x=>String(x.Id)===String(l.qboLineId)))problem('An invoice line identity changed in QuickBooks. Review it before updating.',409);
 const byId=new Map(target.filter(l=>l.qboLineId).map(l=>[String(l.qboLineId),l]));if(byId.size!==target.filter(l=>l.qboLineId).length)problem('Repeated invoice line identities need office review.',409);
 const lines=[];for(const rawLine of raw.Line||[]){if(rawLine.DetailType==='SubTotalLineDetail'){lines.push(structuredClone(rawLine));continue;}const l=byId.get(String(rawLine.Id));if(!l)continue;if(l.readOnly){lines.push(structuredClone(rawLine));continue;}const copy=structuredClone(rawLine),d=copy.SalesItemLineDetail;d.Qty=l.qty;d.UnitPrice=l.unitRate;copy.Amount=round(l.qty*l.unitRate,2);copy.Description=l.description||'';d.TaxCodeRef={value:l.taxable?'TAX':'NON'};lines.push(copy);}
 for(const l of target.filter(l=>!l.qboLineId)){if(!l.added)problem('An existing invoice line has no stable identity.',409);lines.push({DetailType:'SalesItemLineDetail',Description:l.description||'',Amount:round(l.qty*l.unitRate,2),SalesItemLineDetail:{ItemRef:{value:warehouseReference(l.qboItemId,realm)},Qty:l.qty,UnitPrice:l.unitRate,TaxCodeRef:{value:l.taxable?'TAX':'NON'},...(l.classId?{ClassRef:{value:warehouseReference(l.classId,realm)}}:{})}});}
 let subtotal=0;for(const l of lines)if(l.DetailType==='SubTotalLineDetail')l.Amount=round(subtotal,2);else subtotal+=l.Amount;
 return {payload:{Id:raw.Id,SyncToken:raw.SyncToken,sparse:true,Line:lines},matched:false};
}
