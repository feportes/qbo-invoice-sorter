// Only an unrelated shared revision may be retried automatically. Any change to
// this route's products, counts, holds or added orders requires a fresh review.
export function loadingFingerprint(route){
 if(!route)return '';
 const {id,status,fulfillment,vehicleId,lateAdditions,holds,hold,missingSetup}=route;
 return JSON.stringify({id,status,fulfillment,vehicleId,lateAdditions,holds,hold,missingSetup,lines:(route.lines||[]).map(l=>({key:l.key,order:l.order,index:l.index,productId:l.productId,packId:l.packId,qty:l.qty,requestedQty:l.requestedQty,picked:l.picked,shortage:l.shortage})).sort((a,b)=>String(a.order).localeCompare(String(b.order))||Number(a.index)-Number(b.index))});
}
export function canRetryLoading(before,after){return !!before&&!!after&&before.status==='released'&&after.status==='released'&&after.lines?.length>0&&after.lines.every(l=>l.qty===l.picked)&&loadingFingerprint(before)===loadingFingerprint(after);}
export function loadingDraft(state,route){const fingerprint=loadingFingerprint(route);if(state.loading?.fingerprint!==fingerprint)state.loading={fingerprint,vehicle:route.vehicleId||'',confirmed:false};return state.loading;}
