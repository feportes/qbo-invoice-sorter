const $=id=>document.getElementById(id),route=new URLSearchParams(location.search).get('route');let blobUrl;
async function read(path){const response=await fetch('/api/warehouse/route-invoices/'+encodeURIComponent(route)+path,{credentials:'same-origin',cache:'no-store'});if(!response.ok){let error;try{error=await response.json();}catch{}throw Error(error?.error||'Could not prepare invoices. Please retry.');}return response;}
try{
 if(!route)throw Error('Choose Print invoices on a route first.');
 const packet=await (await read('')).json();$('title').textContent=packet.route.name;$('date').textContent=packet.route.date+' · '+packet.invoices.length+' invoices';
 for(const invoice of packet.invoices){const li=document.createElement('li');li.textContent='Stop '+invoice.stop+' · #'+invoice.invoice+' · '+invoice.customer;$('invoices').append(li);}
 $('setup').hidden=false;$('prepare').disabled=!packet.invoices.length;$('message').textContent=packet.invoices.length?'Original QuickBooks invoices, grouped for this route.':'This route has no customer invoices. Supplier pickup stops do not have sales invoices.';
}catch(error){$('message').textContent=error.message;const link=document.createElement('a');link.href='/workspaces';link.textContent=' Sign in / workspaces';$('message').append(link);}
$('prepare').onclick=async()=>{
 $('prepare').disabled=true;$('message').textContent='Retrieving original invoices from QuickBooks…';$('result').hidden=true;
 try{const copies=$('copies').value,order=$('order').value,response=await read('/pdf?copies='+copies+'&order='+order);const blob=await response.blob();if(blobUrl)URL.revokeObjectURL(blobUrl);blobUrl=URL.createObjectURL(blob);$('preview').src=blobUrl;$('download').href=blobUrl;$('open').href=blobUrl;$('download').download=response.headers.get('content-disposition')?.match(/filename="([^"]+)"/)?.[1]||'route-invoices.pdf';$('details').textContent=copies+' copies per invoice · US Letter · '+(order==='stop'?'route stop order':'reverse stop order');$('result').hidden=false;$('message').textContent='Bundle ready. Nothing has been sent to the printer yet.';$('result').scrollIntoView({behavior:'smooth',block:'start'});}catch(error){$('message').textContent=error.message;}finally{$('prepare').disabled=false;}
};
$('print').onclick=()=>{try{$('preview').contentWindow.focus();$('preview').contentWindow.print();}catch{$('message').textContent='Open the PDF and use its Print button, or download it to print.';}};
addEventListener('pagehide',()=>{if(blobUrl)URL.revokeObjectURL(blobUrl);});
