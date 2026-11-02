const HOUR=60*60*1000;
export function restoreDates(storage,key,today,now=Date.now()){try{const p=JSON.parse(storage.getItem(key)||'null');if(p&&Number.isFinite(p.at)&&now>=p.at&&now-p.at<HOUR&&[p.load,p.route,p.invoice].every(d=>/^\d{4}-\d{2}-\d{2}$/.test(d)))return p;}catch{}return {load:today,route:today,invoice:today};}
export function rememberDates(storage,key,dates,now=Date.now()){try{storage.setItem(key,JSON.stringify({...dates,at:now}));}catch{}}
