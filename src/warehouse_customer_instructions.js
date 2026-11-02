import {createHash,randomUUID} from 'node:crypto';
import {permit,problem,audit} from './warehouse_auth.js';
import {instructionCustomers} from './warehouse_instruction_updates.js';
import {once} from './warehouse_receiving.js';
const digest=value=>createHash('sha256').update(JSON.stringify(value)).digest('hex');
export const isFullInstructionGuide=g=>g?.source?.startsWith('Office customer editor');
export function customerInstructionDirectory(sql,user){
 permit(user,['admin','office'],'deliveries');
 return instructionCustomers(sql).map(c=>{const guide=sql.prepare('SELECT * FROM wh_reference_guides WHERE customer_id=? AND location_ref=? ORDER BY updated_at DESC LIMIT 1').get(c.customerId,c.referenceId||'');const base=c.baseInstructions||'',contacts=base.split('\n').filter(l=>/^(Contact|Phone|Truck key):/.test(l));const instructions=guide?(isFullInstructionGuide(guide)?guide.instructions:[...contacts,guide.instructions].join('\n')):base;return {...c,needsCustomerMatch:!c.customerId,instructions,guideId:guide?.id||null,version:digest({base,guide:guide?{id:guide.id,at:guide.updated_at,text:guide.instructions}:null})};});
}
export function saveCustomerInstructions(sql,user,input){
 permit(user,['admin','office'],'deliveries');
 return once(sql,user,input.requestId,input,()=>{
  const target=customerInstructionDirectory(sql,user).find(c=>c.customerId===input.customerId&&(c.referenceId||'')===(input.referenceId||''));if(!target)problem('Choose a customer and delivery location.',404);if(!target.customerId)problem('Match this delivery reference to its QuickBooks customer before editing.',409);
  if(input.expectedVersion!==target.version)problem('These instructions changed. Refresh and review the latest version before saving.',409);
  if(typeof input.instructions!=='string'||input.instructions.length>14000)problem('Keep delivery instructions under 14,000 characters.');
  const instructions=input.instructions.trim(),id=target.guideId||'GUIDE-'+randomUUID(),at=new Date().toISOString();
  sql.prepare(`INSERT INTO wh_reference_guides(id,customer_id,title,instructions,source,updated_at,location_ref) VALUES(?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET instructions=excluded.instructions,source=excluded.source,updated_at=excluded.updated_at`).run(id,target.customerId,target.name,instructions,'Office customer editor',at,target.referenceId||'');
  audit(sql,user,'customer-delivery-instructions-edited','reference-guide',id,{customerId:target.customerId,referenceId:target.referenceId||'',before:target.instructions,after:instructions});
  return {id,message:'Delivery instructions saved. Connected drivers receive the updated guide.',updatedAt:at};
 });
}
