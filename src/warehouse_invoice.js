import {problem} from './warehouse_auth.js';
// Decode only the connector's documented opaque company/id envelope; never resolve by name.
export function warehouseReference(value,realmId){
 const text=String(value||'');if(/^\d{1,30}$/.test(text))return text;
 const match=/^([^:]+):(\d{1,30})$/.exec(text);if(!match)problem('The invoice identity needs office review.',409);
 let company;try{company=Buffer.from(match[1].replace(/-/g,'+').replace(/_/g,'/'),'base64').toString('utf8').split(':')[1];}catch{}
 if(!company||company!==String(realmId))problem('The invoice belongs to a different QuickBooks company.',409);return match[2];
}
export function nativeInvoiceBaseline(raw,stop,realmId){
 if(String(raw?.Id)!==warehouseReference(stop.qboInvoiceId,realmId)||String(raw.CustomerRef?.value)!==warehouseReference(stop.qboCustomerId,realmId))problem('QuickBooks invoice/customer identity changed. Office review is required.',409);
 if(stop.sourceInvoice&&String(raw.DocNumber)!==String(stop.sourceInvoice).replace(/^#\s*/,''))problem('QuickBooks invoice number changed. Office review is required.',409);
 const items=new Map();for(const p of stop.products||[])if(p.qboItemId)items.set(warehouseReference(p.qboItemId,realmId),p.qboItemId);
 const lines=(raw.Line||[]).map(l=>{
  if(l.DetailType==='SubTotalLineDetail')return {subtotal:Number(l.Amount)};
  if(l.DetailType!=='SalesItemLineDetail'||!l.SalesItemLineDetail?.ItemRef?.value)problem('This invoice contains an accounting line that needs office review.',409);
  const d=l.SalesItemLineDetail,id=String(d.ItemRef.value),tax=String(d.TaxCodeRef?.value||'');
  return {id:String(l.Id||''),item_id:items.get(id)||id,item_name:d.ItemRef.name||l.Description||'',description:l.Description||'',quantity:d.Qty??0,rate:d.UnitPrice??0,amount:l.Amount,taxable:tax==='NON'?false:tax==='TAX'?true:null,class_id:d.ClassRef?.value||null};
 });
 return {id:stop.qboInvoiceId,reference_number:String(raw.DocNumber||''),contact:{id:stop.qboCustomerId,display_name:raw.CustomerRef.name||'',email:raw.BillEmail?.Address||''},lines,amount:raw.TotalAmt,tax_amount:raw.TxnTaxDetail?.TotalTax??null,currency_info:{currency_code:raw.CurrencyRef?.value||'Company currency'}};
}
