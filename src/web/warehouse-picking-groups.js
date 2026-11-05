// Pure picking presentation shared by the warehouse client and isolated tests.
const text=value=>String(value||'').normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase();
export function freezerPalletGroup(line){
 if(line.zone!=='freezer')return null;
 const name=text(line.name),category=text(line.category);
 if(/sorbets?/.test(category+' '+name)||(/cascade/.test(name)&&/yogurt/.test(name))||(/strawberry/.test(name)&&/gelato/.test(name)&&!/mix/.test(name)&&/3\s*gal|tub/.test(name)))return 'tubs';
 return 'boxes';
}
export const palletNames={tubs:'Tubs',boxes:'Frozen fruit & boxes'};
export const productKey=line=>line.productId+'::'+line.packId;
export function areaPickItems(lines,area,routeTotals=false,orderId=null){
 const areaLines=lines.filter(l=>l.zone===area&&(routeTotals||!orderId||l.order===orderId));
 if(!routeTotals)return areaLines.filter(l=>l.picked<l.qty).map(l=>({...l,members:[l],key:l.order+':'+l.index,palletGroup:freezerPalletGroup(l)}));
 const groups=new Map();for(const l of areaLines){const key=productKey(l);let group=groups.get(key);if(!group){group={...l,key,qty:0,picked:0,requestedQty:0,members:[],palletGroup:freezerPalletGroup(l)};groups.set(key,group);}group.members.push(l);group.qty+=l.qty;group.picked+=l.picked;group.requestedQty+=l.requestedQty??l.qty;}
 return [...groups.values()].filter(g=>g.picked<g.qty);
}
