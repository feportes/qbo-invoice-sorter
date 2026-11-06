import {PDFDocument,degrees,StandardFonts,rgb} from 'pdf-lib';
import {problem} from './warehouse_auth.js';

export function bundleOptions(query={}){
  const copies=Number(query.copies??2),order=query.order??'stop';
  if(![1,2,3].includes(copies)||!['stop','reverse'].includes(order))problem('Choose 1–3 copies and a route order.');
  return {copies,order};
}
export function routeInvoiceIds(value){
 let ids=value;try{if(typeof value==='string')ids=JSON.parse(value);}catch{problem('Choose routes to print.');}
 if(!Array.isArray(ids)||ids.length<1||ids.length>20||ids.some(id=>typeof id!=='string'||!id||id.length>200||/[\x00-\x1f]/.test(id)))problem('Choose between 1 and 20 routes to print.');
 return [...new Set(ids)];
}
export async function routeInvoiceSelection(packets,options,readPdf){
 if(!packets.length||packets.length>20)problem('Choose between 1 and 20 routes to print.');
 if(packets.reduce((n,p)=>n+p.invoices.length,0)>300)problem('Print up to 300 invoices in one selection.',413);
 const output=await PDFDocument.create(),routes=[];let totalBytes=0;
 for(const packet of packets){
  let bundle;try{bundle=await routeInvoiceBundle(packet,options,readPdf);}catch(error){problem(packet.route.name+': '+error.message,error.status||502);}
  totalBytes+=bundle.bytes.length;if(totalBytes>100*1024*1024)problem('This print selection is too large. Select fewer routes.',413);
  const source=await PDFDocument.load(bundle.bytes),startPage=output.getPageCount()+1;
  if(output.getPageCount()+source.getPageCount()>1000)problem('Select fewer routes; this bundle exceeds 1,000 pages.',413);
  for(const page of await output.copyPages(source,source.getPageIndices()))output.addPage(page);
  routes.push({id:packet.route.id,name:packet.route.name,startPage,pages:bundle.pages,summaryPages:bundle.summaryPages,invoiceCount:bundle.invoiceCount});
 }
 output.setTitle('D RAW · Selected route invoice bundles');
 return {bytes:Buffer.from(await output.save()),pages:output.getPageCount(),routes};
}
const printable=value=>String(value??'').normalize('NFKD').replace(/[\u0300-\u036f]/g,'').replace(/[^\x20-\x7e]/g,' ').replace(/\s+/g,' ').trim();
async function loadingChecklist(pdf,packet){
 const font=await pdf.embedFont(StandardFonts.Helvetica),bold=await pdf.embedFont(StandardFonts.HelveticaBold),ink=rgb(.08,.08,.08),line=rgb(.75,.75,.75),page=pdf.addPage([612,792]);
 const text=(s,x,y,size=11,strong=false,target=page)=>target.drawText(printable(s),{x,y,size,font:strong?bold:font,color:ink});
 const wrap=(s,width,size,strong=true)=>{const out=[];let value='';const f=strong?bold:font;for(const word of printable(s).split(' ')){for(const char of (value?' ':'')+word){if(value&&f.widthOfTextAtSize(value+char,size)>width){const cut=value.lastIndexOf(' ');if(cut>0){out.push(value.slice(0,cut));value=value.slice(cut+1);}else{out.push(value);value='';}}value+=char;}}if(value.trim())out.push(value.trim());return out;};
 const fitted=(s,x,y,width,size,strong=false)=>{const f=strong?bold:font;size=Math.min(size,width/Math.max(1,f.widthOfTextAtSize(printable(s),1)));text(s,x,y,size,strong);};
 text("D' RAW | DRIVER LOADING CHECK",36,752,19,true);
 fitted(packet.route.name,36,725,540,17,true);text('Route date: '+packet.route.date,36,705,11);
 fitted('Driver: '+(packet.route.driver||'Not assigned'),36,683,540,13,true);
 fitted('Vehicle: '+(packet.route.vehicle||'Not assigned'),36,663,540,12);
 page.drawLine({start:{x:36,y:650},end:{x:576,y:650},thickness:1,color:line});
 text('Count the cases. Tick when loaded.',36,630,12,true);
 text('Quantities are totals for ALL stops on this route.',36,613,11);
 const reminder=(packet.checklist||[]).filter(i=>['Fresh fruit / produce','Juices'].includes(i.group)).sort((a,b)=>a.group.localeCompare(b.group));
 if(!reminder.length){text('No fresh fruit or juices on this route.',36,570,14,true);return 1;}
 function rows(columns){const width=(540-(columns-1)*12)/columns,size=columns===1?13:12,nameWidth=width-(columns===1?162:102);return reminder.map(item=>{const name=String(item.name).replace(/^\d+\.\s*[^:]+:/,'').trim(),pack=/^(1 invoice unit|case)$/i.test(item.pack||'case')?'':String(item.pack),names=wrap(name,nameWidth,size),packs=pack?wrap(pack,nameWidth,9,false):[];return {item,names,packs,width,size,height:Math.max(54,names.length*(size+2)+packs.length*11+16)};});}
 const height=items=>items.reduce((n,r,i)=>n+r.height+6+(i===0||items[i-1].item.group!==r.item.group?29:0),0);
 let columns=[rows(1)];if(height(columns[0])>548){const all=rows(2);let split=1,best=Infinity;for(let i=1;i<all.length;i++){const h=Math.max(height(all.slice(0,i)),height(all.slice(i)));if(h<best){best=h;split=i;}}columns=all.length===1?[all]:[all.slice(0,split),all.slice(split)];}
 // Render the entire measured table as one object. It can never spill onto
 // another sheet or silently omit a product, even with unusually long lists.
 const tableHeight=Math.max(...columns.map(height)),table=pdf.addPage([540,tableHeight]);
 columns.forEach((items,index)=>{let y=tableHeight,group='';const x=index*(items[0].width+12);for(const r of items){const {item,width,size,names,packs}=r;if(group!==item.group){group=item.group;table.drawRectangle({x,y:y-23,width,height:23,color:rgb(.9,.9,.9)});text(group==='Juices'?'JUICES':'FRESH FRUIT / PRODUCE',x+7,y-16,11,true,table);y-=29;}
 const bottom=y-r.height,qtyWidth=columns.length===1?98:64,qtyX=x+width-qtyWidth;
 table.drawRectangle({x,y:bottom,width,height:r.height,borderColor:line,borderWidth:.7});table.drawRectangle({x:qtyX,y:bottom,width:qtyWidth,height:r.height,color:rgb(.95,.95,.95)});
 table.drawRectangle({x:x+9,y:y-32,width:18,height:18,borderColor:ink,borderWidth:1.4});
 names.forEach((v,i)=>text(v,x+36,y-19-i*(size+2),size,true,table));packs.forEach((v,i)=>text(v,x+36,y-19-names.length*(size+2)-i*11,9,false,table));
 const qty=Number(item.qty.toFixed(3)).toLocaleString('en-US'),qtySize=Math.min(26,(qtyWidth-8)/Math.max(1,bold.widthOfTextAtSize(qty,1))),center=qtyX+qtyWidth/2;
 text(qty,center-bold.widthOfTextAtSize(qty,qtySize)/2,y-29,qtySize,true,table);const unit=/^(each|individual)/i.test(item.pack||'')?'UNITS':/tub|gal/i.test(item.pack||'')?'TUBS':'CASES';text(unit,center-bold.widthOfTextAtSize(unit,9)/2,y-43,9,true,table);y=bottom-6;
 }});
 const embedded=await pdf.embedPage(table),scale=Math.min(1,548/tableHeight);page.drawPage(embedded,{x:36,y:596-tableHeight*scale,width:540*scale,height:tableHeight*scale});pdf.removePage(pdf.getPageCount()-1);
 text('Loading reminder only.',36,30,9);return 1;
}
export async function routeInvoiceBundle(packet,options,readPdf){
  if(!readPdf)problem('QuickBooks invoice printing is not configured.',503);
  const {copies,order}=bundleOptions(options),invoices=[...packet.invoices];
  if(!invoices.length)problem('This route has no QuickBooks invoices to print.',409);
  if(invoices.length>100)problem('Print routes with no more than 100 invoices per bundle.',413);
  if(order==='reverse')invoices.reverse();
  const output=await PDFDocument.create(),summaryPages=options.includeChecklist===false?0:await loadingChecklist(output,packet);let bytesRead=0;
  for(const invoice of invoices){
    if(!invoice.qboInvoiceId)problem('Invoice #'+invoice.invoice+' is missing its QuickBooks link. Nothing was printed.',409);
    try{
      const bytes=await readPdf(invoice);bytesRead+=bytes.length;
      if(bytesRead>50*1024*1024)problem('This bundle is too large. Print a smaller route.',413);
      const source=await PDFDocument.load(bytes);
      if(!source.getPageCount())throw Error('empty document');
      for(const page of source.getPages())if(!page.node.Contents())page.drawRectangle({x:0,y:0,width:0,height:0,opacity:0});
      const pages=await output.embedPages(source.getPages());
      for(let copy=0;copy<copies;copy++)for(let n=0;n<pages.length;n++){
        const embedded=pages[n],rotation=((source.getPage(n).getRotation().angle%360)+360)%360;
        const swap=rotation===90||rotation===270,w=swap?embedded.height:embedded.width,h=swap?embedded.width:embedded.height;
        const scale=Math.min(612/w,792/h),x=(612-w*scale)/2,y=(792-h*scale)/2;
        const offset={0:[0,0],90:[0,h*scale],180:[w*scale,h*scale],270:[w*scale,0]}[rotation];
        if(!offset)throw Error('unsupported page rotation');
        output.addPage([612,792]).drawPage(embedded,{x:x+offset[0],y:y+offset[1],xScale:scale,yScale:scale,rotate:degrees(-rotation)});
      }
      if(output.getPageCount()>1000)problem('This bundle has too many pages.',413);
    }catch(error){if(error.status)throw error;problem('Could not retrieve a valid PDF for invoice #'+invoice.invoice+'. No partial bundle was created. Try again or check it in QuickBooks.',502);}
  }
  output.setTitle(packet.route.name+' · '+packet.route.date+' · route invoices');
  return {bytes:Buffer.from(await output.save()),pages:output.getPageCount(),summaryPages,invoiceCount:invoices.length,copies,order};
}
