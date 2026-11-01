// The Android workspace scene, drawn with the same artwork and 16-second cycle.
const base='/workspaces-assets/welcome/';
export function cycle(elapsed,animate=true){
 const ease=n=>{n=Math.max(0,Math.min(1,n));return n*n*(3-2*n);},t=((elapsed%16000)+16000)%16000;
 if(!animate)return {progress:.35,load:1};
 if(t<4000)return {progress:0,load:ease((t-2200)/400),returning:t<1100,walking:t>0&&t<500||t>1100&&t<1700,door:t<1100?ease((t-500)/600):1-ease((t-3000)/700),mist:t<2200?ease((t-850)/650):1-ease((t-3000)/900),approach:t<1100?ease(t/500):1-ease((t-1100)/600)};
 if(t<7600)return {progress:(t-4000)/3600,load:1,walking:true};
 if(t<9600)return {progress:1,load:1-(t-7600)/2000,unloading:(t-7600)/2000};
 if(t<10000)return {progress:1,load:0,returning:true};
 if(t<13600)return {progress:1-(t-10000)/3600,load:0,returning:true,walking:true};
 return {progress:0,load:0,returning:true};
}
const names=['push','pull','carts','shop','van'];
let artwork;
function images(){return artwork??=Promise.all(names.map(name=>new Promise((resolve,reject)=>{const image=new Image();image.onload=()=>resolve(image);image.onerror=()=>reject(Error('Welcome artwork unavailable'));image.src=base+name+'.webp';})));}
function legs(c,chest,floor,h,left,moving,elapsed){
 const phase=elapsed%880/880*Math.PI*2,direction=left?-1:1,hip=chest+(left?h*.01:-h*.065);
 for(const back of [true,false]){const a=phase+(back?Math.PI:0),hipX=hip+(back?-1:1)*h*.012,stride=moving?Math.cos(a)*h*.085:(back?-1:1)*h*.018,lift=moving?Math.max(0,-Math.sin(a))*h*.035:0,hipY=floor-h*.475,footX=hipX+direction*stride,footY=floor-lift,ankleY=footY-h*.035,dx=footX-hipX,dy=ankleY-hipY,d=Math.hypot(dx,dy),bend=Math.sqrt(Math.max(0,(h*.225)**2-d*d*.25)),kx=hipX+dx*.5+direction*dy/d*bend,ky=hipY+dy*.5-direction*dx/d*bend;
  c.lineCap='round';for(const [color,thigh,shin] of [[back?'#292b30':'#37393e',.087,.074],[back?'#484a50':'#55575d',.058,.052]]){c.strokeStyle=color;c.lineWidth=h*thigh;c.beginPath();c.moveTo(hipX,hipY);c.lineTo(kx,ky);c.stroke();c.lineWidth=h*shin;c.beginPath();c.moveTo(kx,ky);c.lineTo(footX,ankleY);c.stroke();}
  c.save();c.translate(footX,footY);c.scale(direction,1);c.beginPath();c.moveTo(-h*.038,-h*.059);c.lineTo(h*.019,-h*.059);c.quadraticCurveTo(h*.035,-h*.025,h*.105,-h*.023);c.quadraticCurveTo(h*.132,-h*.018,h*.127,0);c.lineTo(-h*.042,0);c.closePath();c.fillStyle=back?'#181a1e':'#23252a';c.fill();c.restore();
 }
}
function mist(c,left,right,end,floor,h,amount,t){
 if(!amount)return;const smooth=n=>{n=Math.max(0,Math.min(1,n));return n*n*(3-2*n);};
 for(let row=0;row<12;row++)for(let col=0;col<8;col++){const birth=850+row*130+col*9,age=(t%16000-birth)/1000;if(birth>2450||age<0||age>3.2)continue;const seed=.5+.5*Math.sin((row*8+col)*12.9898),falling=Math.min(1,age/.58),ground=Math.max(0,age-.58),x=left+(right-left)*smooth((birth-500)/600)*(col+.35+.3*seed)/8+(end-right)*smooth(ground/1.05)+Math.sin(row+age*2)*h*.018,y=ground>0?floor-h*(.06+Math.min(.13,ground*.12))+Math.sin(col+age*2.2)*h*.025:floor-h*.43+h*.39*falling*falling,r=h*(.055+falling*.04+Math.min(.29,ground*.24))*(.9+seed*.2);
 c.save();c.globalAlpha=205/255*amount*smooth(age/.14)*Math.min(1,(3.2-age)/.7);c.translate(x,y);c.scale(r*(ground>0?1.3:1.05),r);const g=c.createRadialGradient(0,0,0,0,0,1);g.addColorStop(0,'#f5fbff');g.addColorStop(.62,'#e6f3fbec');g.addColorStop(1,'#d6e9f500');c.fillStyle=g;c.beginPath();c.arc(0,0,1,0,Math.PI*2);c.fill();c.restore();}
}
export function welcomeScene(){
 const canvas=document.createElement('canvas');canvas.className='delivery-welcome';canvas.setAttribute('role','img');canvas.setAttribute('aria-label','D’ RAW van, driver and dolly delivering acai tubs and fruit to the shop');
 let assets,raf=0,start,disposed=false;const reduced=matchMedia('(prefers-reduced-motion: reduce)'),context=canvas.getContext('2d');
 function draw(now){raf=0;if(disposed||!assets||document.hidden||!canvas.isConnected)return;start??=now;const rect=canvas.getBoundingClientRect(),w=rect.width,h=rect.height,dpr=Math.min(devicePixelRatio||1,2);if(!w||!h)return;if(canvas.width!==Math.round(w*dpr)||canvas.height!==Math.round(h*dpr)){canvas.width=Math.round(w*dpr);canvas.height=Math.round(h*dpr);}const c=context;c.setTransform(dpr,0,0,dpr,0,0);c.clearRect(0,0,w,h);const [push,pull,carts,shop,van]=assets,floor=h*.93,vw=w*.37,vh=vw*768/1204,t=now-start,s=cycle(t,!reduced.matches);c.fillStyle='#e7dccc';c.fillRect(w*.015,floor,w*.975,Math.max(1,h*.007));c.drawImage(van,28,246,1204,768,0,floor-vh,vw,vh);const sw=w*.34;c.drawImage(shop,19,41,1304,1088,w*.655,floor-sw*1088/1304,sw,sw*1088/1304);
 if(s.door>0){const scale=vw/1204,left=(856-28)*scale,top=floor-vh+(315-246)*scale,width=261*scale,height=465*scale;c.fillStyle='#263449';c.fillRect(left,top,width,height);c.save();c.beginPath();c.rect(left-2*scale,top-2*scale,vw-left+2*scale,height+5*scale);c.clip();c.drawImage(van,856,315,261,465,left+s.door*225*scale,top,width,height);c.restore();}
 const ph=Math.min(w*.17,h*.49),cs=ph*.525/671,from=w*.4,to=w*.94-ph*.3-(838-201)*cs,chest=from+(to-from)*s.progress,cx=chest+ph*.3-201*cs,cy=floor-848*cs,actor=chest-Math.min(w*.07,ph*.55)*(s.approach||0),image=s.returning?pull:push,b=s.returning?[115,8,525,621]:[118,12,491,622],anchor=s.returning?320.02:301.29,scale=ph/(b[3]-b[1]),cellW=image.width/2;
 legs(c,actor,floor,ph,s.returning,s.walking,t);c.drawImage(image,cellW+b[0],b[1],b[2]-b[0],360-b[1],actor-(anchor-b[0])*scale,floor-ph,(b[2]-b[0])*scale,(360-b[1])*scale);
 const cw=carts.width/2;c.drawImage(carts,cw,0,cw,carts.height,cx,cy,cw*cs,carts.height*cs);if(s.load>0){c.globalAlpha=s.load;c.drawImage(carts,165,72,673,614,cx+165*cs+(s.unloading||0)*ph*.08,cy+72*cs-(s.unloading||0)*ph*.12,673*cs,614*cs);c.globalAlpha=1;}
 mist(c,vw*.688,vw*.875,cx+900*cs,floor,ph,s.mist,t);if(!reduced.matches)raf=requestAnimationFrame(draw);
 }
 const resume=()=>{if(!raf&&!disposed)raf=requestAnimationFrame(draw);},resize=new ResizeObserver(resume);resize.observe(canvas);document.addEventListener('visibilitychange',resume);reduced.addEventListener('change',resume);images().then(value=>{assets=value;resume();}).catch(()=>{canvas.hidden=true;});
 // Each dashboard owns one scene; stop drawing when navigation removes it.
 const removal=new MutationObserver(()=>{if(!canvas.isConnected){disposed=true;cancelAnimationFrame(raf);resize.disconnect();removal.disconnect();document.removeEventListener('visibilitychange',resume);reduced.removeEventListener('change',resume);}});queueMicrotask(()=>{if(canvas.isConnected)removal.observe(canvas.parentNode,{childList:true});});
 return canvas;
}
