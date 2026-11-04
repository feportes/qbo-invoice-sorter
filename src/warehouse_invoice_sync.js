import {randomUUID} from 'node:crypto';
import {permit,audit,problem} from './warehouse_auth.js';
const active=new Set();
export function ensureInvoiceSync(sql){sql.exec('CREATE TABLE IF NOT EXISTS wh_invoice_sync(order_id TEXT PRIMARY KEY,version INTEGER NOT NULL,status TEXT NOT NULL,message TEXT,updated_at TEXT NOT NULL)');}
export function decorateInvoiceSync(sql,packet){ensureInvoiceSync(sql);const orders=packet.orders.map(o=>{const job=sql.prepare('SELECT * FROM wh_invoice_sync WHERE order_id=? AND version=?').get(o.id,o.version),pending=!!o.qboSyncRequired||!!job&&job.status!=='confirmed';return {...o,qboSyncPending:pending,qboSyncStatus:job?.status||(o.synced?'confirmed':null),qboSyncMessage:job?.message||''};});return {...packet,orders,pendingCount:orders.filter(o=>!['approved','not-required'].includes(o.status)||o.qboSyncPending).length};}
export async function syncApprovedInvoice(sql,user,source,writeInvoice,orderId,version){
 permit(user,['admin','office'],'deliveries');if(!Number.isSafeInteger(version)||version<1)problem('Choose the latest approved invoice version.',409);ensureInvoiceSync(sql);if(active.has(orderId))return {ok:false,message:'This invoice update is already running. Refresh for its confirmation.'};active.add(orderId);let ownedJob;
 const record=(status,message)=>sql.prepare('INSERT INTO wh_invoice_sync VALUES (?,?,?,?,?) ON CONFLICT(order_id) DO UPDATE SET version=excluded.version,status=excluded.status,message=excluded.message,updated_at=excluded.updated_at').run(orderId,version,status,message,new Date().toISOString());
 const transition=(type,detail={})=>source.request('/api/mobile-office-review',user,{action:'invoice-transition',type,orderId,version,...detail});
 try{
  const packet=await source.request('/api/mobile-office-review',user),order=packet.orders.find(o=>o.id===orderId);if(!order||order.handoffPending||order.status!=='approved'||order.version!==version||order.approved?.version!==version)throw Error('Refresh and approve the latest invoice version before updating QuickBooks.');
  if(!writeInvoice)throw Error('QuickBooks invoice updating is not configured. The approval remains saved for retry.');
  record('checking','Checking the current QuickBooks invoice…');const current=await writeInvoice(order,{verifyOnly:true});
  if(current.matched){await transition('confirm-update',{jobId:order.job?.kind==='update'?order.job.id:undefined,invoice:current.invoice});record('confirmed','QuickBooks invoice verified.');audit(sql,user,'delivery-invoice-qbo-confirmed','invoice',order.qboInvoiceId,{orderId,version,total:current.total,wroteInvoice:false});return {ok:true,message:'Invoice '+order.invoice+' is confirmed in QuickBooks.'};}
  if(order.job?.status==='in-flight')throw Error('A QuickBooks update is still in progress. Refresh for confirmation before retrying.');
  if(order.job?.status==='uncertain')await transition('verify-update-not-applied',{jobId:order.job.id,invoice:current.invoice});
  ownedJob=randomUUID();await transition('claim-update',{jobId:ownedJob});record('updating','Sending approved invoice corrections to QuickBooks…');
  const result=await writeInvoice(order,{verifyOnly:false});if(!result.matched)throw Error('QuickBooks has not confirmed the approved invoice quantities and prices.');
  await transition('confirm-update',{jobId:ownedJob,invoice:result.invoice});record('confirmed','Approved corrections confirmed in QuickBooks.');audit(sql,user,'delivery-invoice-qbo-confirmed','invoice',order.qboInvoiceId,{orderId,version,total:result.total,wroteInvoice:result.updated===true});return {ok:true,message:'Invoice '+order.invoice+' updated and verified in QuickBooks.'};
 }catch(error){if(ownedJob)try{await transition('job-failed',{jobId:ownedJob,message:error.message,definitive:false});}catch{}record('needs-attention',error.message);audit(sql,user,'delivery-invoice-qbo-needs-attention','invoice',orderId,{version,message:error.message});return {ok:false,message:'Approval saved, but QuickBooks update needs attention: '+error.message};}
 finally{active.delete(orderId);}
}
