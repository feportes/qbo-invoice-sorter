const text=value=>String(value||'').normalize('NFD').replace(/[\u0300-\u036f]/g,'').toUpperCase();
export function palletPattern(pallet){
 if(!pallet)return 'empty';
 const rows=(pallet.items||[pallet]).filter(i=>i.qty_units===undefined||Number(i.qty_units)>0);
 const kinds=new Set(rows.map(i=>{const name=text(i.sku_name||i.name),category=text(i.category),all=name+' '+category;
  if(/18\s*(KG|KILO)/.test(name)&&/ACAI/.test(all))return 'tubs12';
  if(/SORBET/.test(all)&&/JUICE\s*CRAFTERS?|BRAZILIAN\s*BLEND/.test(name))return 'rectTubs15';
  if(/PACKS?\b/.test(name)&&/ACAI|ACEROLA|PITAYA/.test(name)&&!/OAKBERRY/.test(name))return 'whiteBoxes12';
  if(/SORBET/.test(all))return 'tubs20';
  if(/FROZEN FRUIT|GELATO/.test(all))return 'boxes10';
  return 'stock';
 }));return kinds.size>1?'mixed':kinds.values().next().value||'empty';
}
// Packing rules confirmed by the operator. Heights stay unknown until measured.
export function palletPacking(item){
 const kind=palletPattern(item),profiles={tubs20:{unitsPerLayer:20,unit:'tubs',shape:'round-tub'},tubs12:{unitsPerLayer:12,unit:'tubs',shape:'large-round-tub'},rectTubs15:{unitsPerLayer:15,fullPalletUnits:105,unit:'tubs',shape:'rectangular-tub'},whiteBoxes12:{unitsPerLayer:12,fullPalletUnits:168,unit:'cases',shape:'white-case'},boxes10:{unitsPerLayer:10,unit:'cases',shape:'brown-case'}};
 const profile=profiles[kind];if(!profile)return null;
 if(kind==='tubs20'&&/ACAI/.test(text(item.sku_name||item.name)))Object.assign(profile,{diameterInches:10,layerHeightInches:10,dimensionsApproximate:true});
 const qty=Number(item.qty_units),known=Number.isFinite(qty)&&qty>=0;
 return {kind,layerHeightInches:null,...profile,...(known?{quantity:qty,fullLayers:Math.floor(qty/profile.unitsPerLayer),topLayerUnits:qty%profile.unitsPerLayer,layers:Math.ceil(qty/profile.unitsPerLayer),...(profile.layerHeightInches?{loadHeightExcludingPalletInches:Math.ceil(qty/profile.unitsPerLayer)*profile.layerHeightInches}:{})}:{})};
}
export function palletSVG(pallet,orientation='straight'){
 const kind=palletPattern(pallet),sideways=orientation==='sideways',labels={empty:'Empty pallet',tubs20:'Layer pattern: 20 sorbet tubs',tubs12:'Layer pattern: 12 large acai tubs',rectTubs15:'Layer pattern: 15 rectangular sorbet tubs, 105 per full pallet',whiteBoxes12:'Layer pattern: 12 white pack cases, 168 per full pallet',boxes10:'Layer pattern: 10 frozen fruit or gelato boxes',mixed:'Mixed product pallet',stock:'Occupied pallet'},label=labels[kind];
 let art='<rect x="3" y="3" width="94" height="114" rx="3" fill="#b48b57" stroke="#8b683f"/>';
 for(let x=7;x<94;x+=18)art+=`<rect x="${x}" y="6" width="13" height="108" rx="1" fill="#dfc498"/>`;
 const tub=(x,y,r)=>`<circle cx="${x}" cy="${y}" r="${r}" fill="#f7f7f2" stroke="#a9b4ba" stroke-width="1.2"/><circle cx="${x}" cy="${y}" r="${r*.66}" fill="none" stroke="#d5dbdd"/><circle cx="${x}" cy="${y}" r="${r*.25}" fill="#58287e"/>`;
 const box=(x,y,w,h)=>`<rect x="${x}" y="${y}" width="${w}" height="${h}" rx="1" fill="#bb8754" stroke="#845b33"/><path d="${w>h?`M${x+1} ${y+h/2}h${w-2}`:`M${x+w/2} ${y+1}v${h-2}`}" stroke="#e7c795" stroke-width="4"/>`;
 if(kind==='tubs20'||kind==='tubs12'){const cols=kind==='tubs20'?4:3,rows=kind==='tubs20'?5:4,dx=90/cols,dy=110/rows;for(let y=0;y<rows;y++)for(let x=0;x<cols;x++)art+=tub(5+(x+.5)*dx,5+(y+.5)*dy,Math.min(dx,dy)*.49);}
 if(kind==='rectTubs15'||kind==='whiteBoxes12'){const rows=kind==='rectTubs15'?5:4,dy=110/rows;for(let y=0;y<rows;y++)for(let x=0;x<3;x++)art+=`<rect x="${5+x*30}" y="${5+y*dy}" width="29" height="${dy-1}" rx="${kind==='rectTubs15'?5:1}" fill="#fffdfa" stroke="#a9b4ba"/>`;}
 // Two columns of three long cases, plus four turned sideways down the edge.
 if(kind==='boxes10'){for(let x=0;x<2;x++)for(let y=0;y<3;y++)art+=box(5+x*28,5+y*36.6,27.5,36);for(let y=0;y<4;y++)art+=box(61,5+y*27.5,34,27);}
 if(kind==='mixed'){for(let y=0;y<3;y++)for(let x=0;x<3;x++)art+=tub(22+x*28,21+y*25,10);art+=box(9,88,39,22)+box(51,88,39,22);}
 if(kind==='stock')art+='<rect x="12" y="17" width="76" height="87" rx="7" fill="#e9e0f2" stroke="#75518e"/><path d="M26 60h48M50 36v48" stroke="#75518e" stroke-width="5"/>';
 return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${sideways?'120 100':'100 120'}" width="${sideways?58:48}" height="${sideways?48:58}" role="img" aria-label="${label}. 40 by 48 inch pallet, ${sideways?'sideways':'straight'}"><title>${label} · illustration, not stock quantity</title><g${sideways?' transform="translate(120 0) rotate(90)"':''}>${art}</g></svg>`;
}
