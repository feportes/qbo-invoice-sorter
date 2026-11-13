import {palletPacking} from './pallet-visual.js';
export function renderPalletVolume(stack,p,n,{onMap=false}={}){
 const ns='http://www.w3.org/2000/svg',svg=document.createElementNS(ns,'svg');
 svg.setAttribute('aria-hidden','true');
 const add=(tag,attrs,parent=svg)=>{const e=document.createElementNS(ns,tag);for(const [k,v] of Object.entries(attrs))e.setAttribute(k,String(v));parent.append(e);return e;};
 const layers=Math.ceil(n/p.perLayer),unitHeight=p.layerHeight||(p.shape==='large'?12:10);
 const height=onMap?Math.max(150,80+layers*unitHeight*2.4):310;svg.setAttribute('viewBox','0 0 '+(onMap?280:400)+' '+height);const baseY=onMap?height-44:260,centerX=onMap?145:205;const scale=onMap?3:Math.min(4,240/(39+layers*unitHeight*.8));
 const pt=(x,y,z)=>[centerX+(x-20-(y-24)*.58)*scale,baseY+((x-20)*.28+(y-24)*.48-z*.8)*scale];
 const points=a=>a.map(v=>pt(...v).join(',')).join(' ');
 const face=(a,fill,stroke='#a8afb0')=>add('polygon',{points:points(a),fill,stroke,'stroke-width':.55,'stroke-linejoin':'round'});
 const defs=add('defs',{});
 for(const [id,colors] of [['fr-tub-body',['#b9c5c9','#f5f7f4','#d5dddb','#aab9bf']],['fr-tub-lid',['#ffffff','#e1e8e5']]]){
  const gradient=add('linearGradient',{id,x1:'0%',y1:'0%',x2:'100%',y2:'0%'},defs);
  colors.forEach((color,i)=>add('stop',{offset:(i/(colors.length-1)*100)+'%','stop-color':color},gradient));
 }
 add('ellipse',{cx:centerX,cy:baseY+12,rx:35*scale,ry:10*scale,fill:'#302238',opacity:.12});
 // Timber deck, open fork pockets, and separate boards.
 face([[0,0,0],[40,0,0],[40,48,0],[0,48,0]],'#bd9867','#8e6c45');
 face([[0,48,0],[40,48,0],[40,48,-3],[0,48,-3]],'#a77a48','#795832');
 face([[40,0,0],[40,48,0],[40,48,-3],[40,0,-3]],'#8e673f','#795832');
 for(let i=0;i<5;i++)face([[i*8+.4,0,.5],[i*8+7.6,0,.5],[i*8+7.6,48,.5],[i*8+.4,48,.5]],i%2?'#d4b27c':'#c9a16a','#aa8452');
 for(const x of [7,23])face([[x,48,-.7],[x+9,48,-.7],[x+9,48,-2.7],[x,48,-2.7]],'#493b2c','#493b2c');
 const units=[];
 for(let layer=0;layer<layers;layer++)for(let u=0;u<Math.min(p.perLayer,n-layer*p.perLayer);u++){
  let x,y,w,d;
  if(p.shape==='oakberry'){
   if(u<24){w=40/6;d=10;x=(u%6)*w;y=Math.floor(u/6)*d;}
   else {w=10;d=8;x=(u-24)*10;y=40;}
  }else if(p.shape==='brown'){
   // Photo-aligned schematic: both strips span the full 48-inch deck.
   // Three longwise cases match four turned cases, with a square outer edge.
   x=u<6?Math.floor(u/3)*11.5:23;y=u<6?(u%3)*16:(u-6)*12;w=u<6?11.5:17;d=u<6?16:12;
  }else{w=40/p.cols;d=48/Math.ceil(p.perLayer/p.cols);x=(u%p.cols)*w;y=Math.floor(u/p.cols)*d;}
  units.push({x,y,w,d,z:layer*unitHeight,h:unitHeight});
 }
 // Paint rear units first and each higher layer above the preceding layer.
 units.sort((a,b)=>a.z-b.z||(a.x*.28+a.y*.48)-(b.x*.28+b.y*.48));
 for(const u of units){
  const {x,y,w,d,z,h}=u;
  if(p.shape==='round'||p.shape==='large'){
   const cx=x+w/2,cy=y+d/2,r=Math.min(w,d)*.485;
   const bottom=pt(cx,cy,z+.6),top=pt(cx,cy,z+h-.3),rx=r*scale*1.12,ry=r*scale*.48;
   add('path',{d:`M ${top[0]-rx} ${top[1]} L ${bottom[0]-rx*.92} ${bottom[1]} A ${rx*.92} ${ry} 0 0 0 ${bottom[0]+rx*.92} ${bottom[1]} L ${top[0]+rx} ${top[1]} Z`,fill:'url(#fr-tub-body)',stroke:'#9baeb3','stroke-width':.65});
   const mid=(top[1]+bottom[1])/2;
   add('rect',{x:top[0]-rx*.42,y:mid-ry*.35,width:rx*.84,height:Math.max(2,(bottom[1]-top[1])*.32),rx:1,fill:'#eaece3',stroke:'#c1c8bd','stroke-width':.4});
   add('path',{d:`M ${top[0]-rx*.2} ${mid} h ${rx*.4}`,stroke:'#6c5085','stroke-width':1.4});
   add('ellipse',{cx:top[0],cy:top[1]+1.4,rx:rx*1.025,ry,fill:'#bbc7c8',stroke:'#93a5a8','stroke-width':.55});
   add('ellipse',{cx:top[0],cy:top[1],rx:rx*1.04,ry,fill:'url(#fr-tub-lid)',stroke:'#a6b6b8','stroke-width':.65});
   add('ellipse',{cx:top[0],cy:top[1]-.3,rx:rx*.86,ry:ry*.78,fill:'none',stroke:'#ced8d4','stroke-width':.6});
  }else if(p.shape==='rect'||p.shape==='oakberry'){
   // Rounded, tapered molded tubs, rather than square carton faces.
   const purple=p.shape==='oakberry',gap=.18,xx=x+gap,yy=y+gap,ww=w-gap*2,dd=d-gap*2;
   function perimeter(a,b,c,d,r){const out=[];for(const [cx,cy,angle] of [[a+c-r,b+r,-90],[a+c-r,b+d-r,0],[a+r,b+d-r,90],[a+r,b+r,180]])for(let i=0;i<=5;i++){const t=(angle+i*18)*Math.PI/180;out.push([cx+Math.cos(t)*r,cy+Math.sin(t)*r]);}return out;}
   const top=perimeter(xx,yy,ww,dd,1.55),bottom=perimeter(xx+.65,yy+.65,ww-1.3,dd-1.3,1.25);
   const sides=top.map((a,i)=>{const j=(i+1)%top.length;return {a,b:top[j],c:bottom[j],d:bottom[i],depth:(a[0]+top[j][0])*.28+(a[1]+top[j][1])*.48};}).sort((a,b)=>a.depth-b.depth);
   for(const side of sides){const {a,b,c,d}=side;const shade=Math.round(216+16*(b[0]-a[0])/Math.max(.01,Math.hypot(b[0]-a[0],b[1]-a[1])));face([[...d,z+.5],[...c,z+.5],[...b,z+h-.65],[...a,z+h-.65]],purple?'#572668':`rgb(${shade-5},${shade+3},${shade+2})`,'none');}
   const lid=perimeter(xx-.18,yy-.18,ww+.36,dd+.36,1.65);
   const ring=pts=>pts.map(v=>[...v,z+h]);
   face(ring(lid),purple?'#623171':'#f5f6f0',purple?'#35123f':'#aab9b9');
   // Thick snap-on lip is visible around the entire lid perimeter.
   for(let i=0;i<lid.length;i++){const a=lid[i],b=lid[(i+1)%lid.length];if((b[0]-a[0])*.48-(b[1]-a[1])*.28>=0)face([[...a,z+h-.65],[...b,z+h-.65],[...b,z+h],[...a,z+h]],'#d1dcd8','#b7c5c1');}
   face(ring(perimeter(xx+.35,yy+.35,ww-.7,dd-.7,1.25)),purple?'#71427e':'#edf2ed',purple?'#9963a1':'#cbd6d0');
   // Printed front panel and drop-down handle identify a plastic food tub.
   face([[xx+ww*.26,yy+dd-.2,z+h*.22],[xx+ww*.74,yy+dd-.2,z+h*.22],[xx+ww*.74,yy+dd-.05,z+h*.58],[xx+ww*.26,yy+dd-.05,z+h*.58]],'#e6ebe5','none');
   const label=pt(xx+ww*.5,yy+dd,z+h*.42);
   add('ellipse',{cx:label[0],cy:label[1],rx:ww*scale*.075,ry:ww*scale*.075,fill:'none',stroke:'#778780','stroke-width':.65});
   for(const offset of [.24,.3]){const a=pt(xx+ww*.35,yy+dd,z+h*offset),b=pt(xx+ww*.65,yy+dd,z+h*offset);add('path',{d:`M ${a} L ${b}`,stroke:'#84928c','stroke-width':.45});}
   const a=pt(xx+ww*.14,yy+dd,z+h*.78),b=pt(xx+ww*.86,yy+dd,z+h*.78),c=pt(xx+ww*.5,yy+dd+.35,z+h*.56);
   add('path',{d:`M ${a} Q ${c[0]} ${c[1]+h*scale*.15} ${b}`,fill:'none',stroke:'#9bafae','stroke-width':2,'stroke-linecap':'round'});
   add('path',{d:`M ${a} Q ${c[0]} ${c[1]+h*scale*.15} ${b}`,fill:'none',stroke:'#eef3ee','stroke-width':1.1,'stroke-linecap':'round'});
  }else{
   const brown=p.shape==='brown',gap=brown ? .08 : .35;
   const xx=x+gap,yy=y+gap,ww=w-gap*2,dd=d-gap*2;
   face([[xx,yy+dd,z],[xx+ww,yy+dd,z],[xx+ww,yy+dd,z+h],[xx,yy+dd,z+h]],brown?'#b88b55':'#dce3df');
   face([[xx+ww,yy,z],[xx+ww,yy+dd,z],[xx+ww,yy+dd,z+h],[xx+ww,yy,z+h]],brown?'#997044':'#b9c9ca');
   face([[xx,yy,z+h],[xx+ww,yy,z+h],[xx+ww,yy+dd,z+h],[xx,yy+dd,z+h]],brown?'#d4b181':'#f4f6ef');
   if(brown){const a=pt(xx+ww/2,yy,z+h+.05),b=pt(xx+ww/2,yy+dd,z+h+.05);add('path',{d:`M ${a} L ${b}`,stroke:'#e5c99b','stroke-width':2});}
   face([[xx+ww*.22,yy+dd+.02,z+h*.3],[xx+ww*.7,yy+dd+.02,z+h*.3],[xx+ww*.7,yy+dd+.02,z+h*.62],[xx+ww*.22,yy+dd+.02,z+h*.62]],'#f5f3e9','#dddacc');
  }
 }
 stack.replaceChildren(svg);
}

export function drawStockVolume(host,item,options={}){const p=item.palletPacking||palletPacking(item);if(!p){host.textContent='No layer pattern configured';return;}const shapes={tubs20:'round',tubs12:'large',rectTubs15:'rect',whiteBoxes12:'white',boxes10:'brown',oakberry28:'oakberry'};renderPalletVolume(host,{shape:shapes[p.kind],perLayer:p.unitsPerLayer,cols:p.kind==='tubs20'?4:3,layerHeight:p.layerHeightInches||null},Number(item.qty_units)||0,options);host.setAttribute('role','img');host.setAttribute('aria-label',(item.sku_name||'Product')+' · '+item.qty_units+' '+(item.unit_type||'units')+' · '+p.layers+' layers');}
