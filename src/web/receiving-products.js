const norm=v=>String(v||'').trim().toUpperCase();
function keys(value){const raw=norm(value).replace(/^\][A-Z][0-9]/,'');const gs=raw.match(/\(01\)(\d{14})/)||raw.match(/^01(\d{14})/);const code=gs?gs[1]:raw;return [raw,code,...(/^\d{12,14}$/.test(code)?[code.padStart(14,'0')]:[])];}
export function matchingReceivingProducts(products,value){const wanted=keys(value);return products.filter(p=>[p.qbo_sku,...(p.barcodes||[])].filter(Boolean).some(code=>keys(code).some(k=>wanted.includes(k))));}
export function searchReceivingProducts(products,value){const q=norm(value);return q?products.filter(p=>norm(p.name).includes(q)||norm(p.qbo_sku).includes(q)||matchingReceivingProducts([p],value).length).slice(0,25):[];}
