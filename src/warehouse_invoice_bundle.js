import {PDFDocument,degrees,StandardFonts,rgb} from 'pdf-lib';
import {problem} from './warehouse_auth.js';

export function bundleOptions(query={}){
  const copies=Number(query.copies??2),order=query.order??'stop';
  if(![1,2,3].includes(copies)||!['stop','reverse'].includes(order))problem('Choose 1–3 copies and a route order.');
  return {copies,order};
}
const printable=value=>String(value??'').normalize('NFKD').replace(/[\u0300-\u036f]/g,'').replace(/[^\x20-\x7e]/g,' ').replace(/\s+/g,' ').trim();
async function loadingChecklist(pdf,packet){
 const font=await pdf.embedFont(StandardFonts.Helvetica),bold=await pdf.embedFont(StandardFonts.HelveticaBold),ink=rgb(.13,.04,.28);let page,y;
 const text=(s,x,at,size=11,strong=false)=>page.drawText(printable(s),{x,y:at,size,font:strong?bold:font,color:ink});
 const wrap=(s,width,size=11)=>{const lines=[];let line='';for(const word of printable(s).split(' ')){if(line&&font.widthOfTextAtSize(line+' '+word,size)>width){lines.push(line);line=word;}else line+=(line?' ':'')+word;}if(line)lines.push(line);return lines;};
 function sheet(continued=false){page=pdf.addPage([612,792]);text("D' RAW | DRIVER LOADING CHECKLIST"+(continued?' - continued':''),36,754,15,true);text(packet.route.name,36,726,14,true);text('Date: '+packet.route.date,36,704);text('Driver: '+(packet.route.driver||'Not assigned'),36,684);text('Vehicle: '+(packet.route.vehicle||'Not assigned'),36,664);text('Check each product and its total quantity before leaving.',36,636);text('Quantities are combined across all customer invoices on this route.',36,620,10);text('Use the listed case / pack size; do not count individual cans as cases.',36,604,10);text('Total cases / packs',477,584,9,true);y=558;}
 sheet();let group='';
 const reminder=(packet.checklist||[]).filter(item=>['Fresh fruit / produce','Juices'].includes(item.group));
 if(!reminder.length)text('No fresh fruit or juices on this route.',36,y);
 for(const item of reminder){const name=String(item.name).replace(/^\d+\.\s*[^:]+:/,'').trim(),lines=wrap(name,425,10),pack=/^(1 invoice unit|case)$/i.test(item.pack||'case')?[]:wrap(item.pack,425,8),height=lines.length*13+pack.length*10+11+(item.group!==group?25:0);if(y-height<64){sheet(true);group='';}if(group!==item.group){group=item.group;text(group,36,y,12,true);y-=25;}
  page.drawRectangle({x:37,y:y-4,width:13,height:13,borderColor:ink,borderWidth:1});
  lines.forEach((line,i)=>text(line,60,y-i*13,10));text(Number(item.qty.toFixed(3)).toLocaleString('en-US'),510,y,12,true);y-=lines.length*13;pack.forEach((line,i)=>text(line,60,y-i*10,8));y-=pack.length*10+11;
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
  const output=await PDFDocument.create(),summaryPages=await loadingChecklist(output,packet);let bytesRead=0;
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
