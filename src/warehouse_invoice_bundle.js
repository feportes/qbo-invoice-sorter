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
 const font=await pdf.embedFont(StandardFonts.Helvetica),bold=await pdf.embedFont(StandardFonts.HelveticaBold),ink=rgb(.08,.08,.08),line=rgb(.75,.75,.75);let page,y;
 const text=(s,x,at,size=11,strong=false)=>page.drawText(printable(s),{x,y:at,size,font:strong?bold:font,color:ink});
 const wrap=(s,width,size=13)=>{const lines=[];let value='';for(const word of printable(s).split(' ')){if(value&&bold.widthOfTextAtSize(value+' '+word,size)>width){lines.push(value);value=word;}else value+=(value?' ':'')+word;}if(value)lines.push(value);return lines;};
 function sheet(continued=false){
  page=pdf.addPage([612,792]);text("D' RAW | LOADING REMINDER"+(continued?' - continued':''),36,752,19,true);
  text(packet.route.name,36,722,17,true);text('Route date: '+packet.route.date,36,700,11);
  text('Driver: '+(packet.route.driver||'Not assigned'),36,678,13,true);text('Vehicle: '+(packet.route.vehicle||'Not assigned'),36,656,12);
  page.drawLine({start:{x:36,y:640},end:{x:576,y:640},thickness:1,color:line});
  text('Check the product. Count the cases. Tick when loaded.',36,619,12,true);
  text('Each quantity is the TOTAL for all stops on this route.',36,601,11);
  y=570;
 }
 sheet();let group='';
 const reminder=(packet.checklist||[]).filter(item=>['Fresh fruit / produce','Juices'].includes(item.group));
 if(!reminder.length)text('No fresh fruit or juices on this route.',36,y,14,true);
 for(const item of reminder){
  const name=String(item.name).replace(/^\d+\.\s*[^:]+:/,'').trim(),names=wrap(name,330),pack=/^(1 invoice unit|case)$/i.test(item.pack||'case')?'':String(item.pack),packs=pack?wrap(pack,330,10):[];
  const height=Math.max(68,names.length*17+packs.length*12+22),groupSpace=group!==item.group?43:0;
  if(y-height-groupSpace<64){sheet(true);group='';}
  if(group!==item.group){group=item.group;page.drawRectangle({x:36,y:y-28,width:540,height:30,color:rgb(.9,.9,.9)});text(group==='Juices'?'JUICES':'FRESH FRUIT / PRODUCE',46,y-18,13,true);y-=43;}
  const bottom=y-height;
  page.drawRectangle({x:36,y:bottom,width:540,height,borderColor:line,borderWidth:.7});
  page.drawRectangle({x:478,y:bottom,width:98,height,color:rgb(.95,.95,.95)});
  page.drawRectangle({x:48,y:y-39,width:23,height:23,borderColor:ink,borderWidth:1.5});
  names.forEach((v,i)=>text(v,84,y-23-i*17,13,true));packs.forEach((v,i)=>text(v,84,y-23-names.length*17-i*12,10));
  const qty=Number(item.qty.toFixed(3)).toLocaleString('en-US'),size=qty.length>6?20:28;
  text(qty,527-bold.widthOfTextAtSize(qty,size)/2,y-32,size,true);
  const unit=/^(each|individual)/i.test(pack)?'UNITS':/tub|gal/i.test(pack)?'TUBS':'CASES';text(unit,527-bold.widthOfTextAtSize(unit,10)/2,y-49,10,true);
  y=bottom-12;
 }
 text('Loading reminder only.',36,38,9);
 return pdf.getPageCount();
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
