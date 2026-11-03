const text=v=>String(v??'').trim();
const normalized=v=>text(v).toUpperCase();
export const setupFields=[
 {id:'scanCode',label:'Scan code',essential:true},
 {id:'storage',label:'Storage area',essential:true},
 {id:'sku',label:'SKU · QuickBooks'},
 {id:'photo',label:'Product photo'},
 {id:'weight',label:'Weight / size'},
 {id:'description',label:'Description'},
 {id:'category',label:'Category · QuickBooks'},
 {id:'alternateBarcode',label:'UPC / alternate barcode',optional:true}
];
export function productSetup(p){
 const codes=(p.barcodes||[]).map(b=>text(b.code)).filter(Boolean),sku=text(p.sku);
 const present={scanCode:!!sku||codes.length>0,storage:['freezer','dry','refrigerated'].includes(p.zone),sku:!!sku,photo:!!(p.photo?.id||p.photo?.key||p.photo?.url),weight:!!text(p.weight),description:!!text(p.description),category:!!text(p.category)&&!/^uncategorized$/i.test(text(p.category)),alternateBarcode:codes.some(c=>normalized(c)!==normalized(sku))};
 return setupFields.map(f=>({...f,present:present[f.id]}));
}
export function missingSetup(p,filter='essential'){
 const fields=productSetup(p).filter(f=>!f.present);
 return filter==='all'?fields.filter(f=>!f.optional):filter==='essential'?fields.filter(f=>f.essential):fields.filter(f=>f.id===filter);
}
export function setupSummary(products){return {total:products.length,ready:products.filter(p=>!missingSetup(p).length).length,complete:products.filter(p=>!missingSetup(p,'all').length).length,missing:Object.fromEntries(setupFields.map(f=>[f.id,products.filter(p=>missingSetup(p,f.id).length).length]))};}

export function productEditValues(entries,zone,fresh){const values=Object.fromEntries(entries);delete values.sku;for(const k of ['warehouseScanLimit','driverScanLimit'])if(k in values)values[k]=Number(values[k]);if(zone){values.zone=zone;values.fresh=!!fresh;}return values;}
