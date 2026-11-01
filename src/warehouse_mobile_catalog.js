import {problem} from './warehouse_auth.js';
const normalized=v=>String(v||'').trim().toUpperCase();
export function saveDriverCatalog(sql,user,data){
 if(!/^[a-f0-9]{64}$/.test(data.version||'')||!/^[a-f0-9]{64}$/.test(data.barcodeVersion||''))problem('Product catalog version is incomplete.',502);
 const previous=sql.prepare('SELECT * FROM wh_driver_catalog WHERE user_id=?').get(user.id),revision=data.catalogRevision||0;if(previous&&revision<previous.source_revision)return;if(data.unchanged){if(!previous||previous.version!==data.version)problem('Product catalog cache needs a full refresh.',502);sql.prepare('UPDATE wh_driver_catalog SET checked_at=?,source_revision=? WHERE user_id=?').run(data.checkedAt,revision,user.id);return;}
 if(!Array.isArray(data.products)||data.products.length>20000)problem('Product catalog response is incomplete.',502);
 sql.transaction(()=>{sql.prepare('INSERT INTO wh_driver_catalog(user_id,version,barcode_version,products,checked_at,source_revision) VALUES (?,?,?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET version=excluded.version,barcode_version=excluded.barcode_version,products=excluded.products,checked_at=excluded.checked_at,source_revision=excluded.source_revision').run(user.id,data.version,data.barcodeVersion,JSON.stringify(data.products),data.checkedAt,revision);sql.prepare('INSERT OR IGNORE INTO wh_barcode_versions VALUES (?,?,?)').run(data.barcodeVersion,JSON.stringify(data.products),new Date().toISOString());})();
}
export function overlayDriverCatalog(sql,user,route){
 const row=sql.prepare('SELECT products FROM wh_driver_catalog WHERE user_id=?').get(user.id);if(!row)return route;const products=new Map(JSON.parse(row.products).map(p=>[p.id,p]));
 for(const stop of route.stops)for(const p of stop.products){const item=products.get(p.productId||p.id.split('::')[0]);if(!item)continue;const pack=p.packId||p.id.split('::')[1]||item.defaultPack;p.name=item.name;p.sku=item.sku;p.barcodes=(item.aliases||[]).filter(a=>a.packId===pack).map(a=>a.code);p.unitLabel=item.packs?.find(x=>x.id===pack)?.label||item.weight||'';}
 return route;
}
function matches(products,barcode){const result=new Set(),want=normalized(barcode);for(const p of products){if(p.sku&&normalized(p.sku)===want)result.add(p.id+'::'+p.defaultPack);for(const a of p.aliases||[])if(normalized(a.code)===want)result.add(p.id+'::'+a.packId);}return [...result];}
export function driverBarcodeProof(sql,user,route,event){
 const x=event.payload||{},stop=route.stops.find(s=>s.id===x.stopId);if(!stop)problem('Stop is not on this route.');
 const live=sql.prepare('SELECT products FROM wh_driver_catalog WHERE user_id=?').get(user.id);if(!live)return null;
 const current=matches(JSON.parse(live.products),x.barcode),claimed=x.acceptedProductId;
 if(current.length>1||current.length===1&&claimed&&current[0]!==claimed)problem('This barcode now identifies a different product. Office review is needed.',409);
 if(current.length===1){if(!stop.products.some(p=>p.id===current[0]))problem('That item is not needed at this stop.');return current[0];}
 const historical=sql.prepare('SELECT products FROM wh_barcode_versions WHERE version=?').get(x.catalogVersion||'');
 if(!claimed||!historical||!matches(JSON.parse(historical.products),x.barcode).includes(claimed)||!stop.products.some(p=>p.id===claimed))problem('The saved catalog does not verify this barcode at this stop.',409);
 return claimed;
}
