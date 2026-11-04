// Presentation only: catalog names and invoice matching remain unchanged.
export function productName(p){let name=String(p.name||''),category=String(p.category||'');if(category&&name.startsWith(category+':'))name=name.slice(category.length+1);return name.replace(/^\s*\d{1,2}\s*[.)-]\s*[^:]+:\s*/,'').trim();}
export function storageLabel(p){return {freezer:'Freezer',dry:'Dry',refrigerated:'Refrigerated'}[p.zone]||'Storage area not recorded';}
