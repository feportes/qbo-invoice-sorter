import {warehouseReference} from './warehouse_invoice.js';
import {problem} from './warehouse_auth.js';

export function invoiceDate(value){if(typeof value!=='string'||!/^\d{4}-\d{2}-\d{2}$/.test(value)||!Number.isFinite(Date.parse(value+'T12:00:00Z'))||new Date(value+'T12:00:00Z').toISOString().slice(0,10)!==value)problem('Choose a valid invoice date.');return value;}
function referenceEncoder(values,realm){
 const prefixes=new Set();for(const value of values.filter(Boolean)){const text=String(value);if(/^\d{1,30}$/.test(text))continue;const match=/^([^:]+):(\d{1,30})$/.exec(text);if(!match)continue;warehouseReference(text,realm);prefixes.add(match[1]);}
 if(prefixes.size>1)problem('QuickBooks identity formats need office review before import.',409);
 const prefix=[...prefixes][0]||'';return value=>prefix?prefix+':'+String(value):String(value);
}
export function projectPlanningInvoices(invoices,date,realm,planning){
 const orders=planning.orders||[],invoiceId=referenceEncoder(orders.map(o=>o.qboInvoiceId),realm),customerId=referenceEncoder(orders.map(o=>o.qboCustomerId),realm),itemId=referenceEncoder(orders.flatMap(o=>(o.products||[]).map(p=>p.qboItemId)),realm);
 const documents=invoices.filter(i=>i.TxnDate===date).map(i=>{
  const rows=(i.Line||[]).filter(l=>l.DetailType==='SalesItemLineDetail'&&l.SalesItemLineDetail?.ItemRef?.value);
  const name=l=>String(l.SalesItemLineDetail.ItemRef.name||l.Description||''),credit=l=>/(?:^|:)\s*Credit\s*$/i.test(name(l));
  const accountingLines=rows.filter(credit).map(l=>({name:name(l),kind:'credit'}));
  const lines=rows.filter(l=>!credit(l)&&String(l.SalesItemLineDetail.ItemRef.value)!=='520'&&!/operating cost surcharge/i.test(name(l))).map(l=>({qboItemId:itemId(l.SalesItemLineDetail.ItemRef.value),name:name(l),qty:l.SalesItemLineDetail.Qty??null}));
  return {qboInvoiceId:invoiceId(i.Id),qboCustomerId:customerId(i.CustomerRef?.value||''),invoice:String(i.DocNumber||''),date:i.TxnDate,customer:String(i.CustomerRef?.name||''),shipTo:'',lines,...(accountingLines.length?{accountingLines}:{})};
 }).filter(i=>i.lines.length);
 return {status:'success',date,readAt:new Date().toISOString(),documents,possiblyTruncated:invoices.length>=1000};
}
