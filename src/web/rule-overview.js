import {routeRuleOverview} from './route-rule-model.js';
const days=['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
const el=(tag,text,cls)=>{const n=document.createElement(tag);if(text!==undefined)n.textContent=text;if(cls)n.className=cls;return n;};
export function renderRuleOverview(host,packet,{query,day,routeId,onEdit,onMap,expanded=new Set()}={}){
 const result=routeRuleOverview(packet,{query,day,routeId});host.replaceChildren();
 host.append(el('p',result.groups.length+' route / weekday plans · '+result.assignedCustomerCount+' customers with displayed assignments · '+result.unruled.length+' without any rule','overview-count'));
 const warningCount=result.groups.reduce((n,g)=>n+g.stops.filter(s=>s.sharedRank).length,0);
 if(warningCount)host.append(el('p',warningCount+' entries share a preferred stop number. Highlighted rows let you check their order.','overview-warning'));
 if(result.issues.length){const issues=el('details',undefined,'overview-section');issues.open=true;issues.append(el('summary','Rules needing attention · '+result.issues.length));for(const issue of result.issues){const row=el('div',undefined,'overview-stop');row.append(el('span',issue.customer.customerName+' · '+(days[issue.day]||'Weekday')+' · '+issue.reason));editButton(row,issue.customer);}host.append(issues);}
 const grid=el('div',undefined,'rule-overview-grid');host.append(grid);
 for(const g of result.groups){const card=el('details',undefined,'overview-section route-plan');card.dataset.planKey=g.key;card.open=expanded.has(g.key);const summary=el('summary');summary.append(el('strong',g.route.name),el('span',days[g.day]+' · '+g.stops.length+' stops'));const settings=el('a','Edit saved route','edit-saved-route');settings.href='/warehouse-saved-routes?route='+encodeURIComponent(g.route.id);settings.onclick=e=>e.stopPropagation();summary.append(settings);card.append(summary);if(onMap){const map=el('button','View on map','secondary');map.type='button';map.setAttribute('aria-label','View '+g.route.name+' '+days[g.day]+' on map');map.onclick=()=>onMap(g);card.append(map);}card.addEventListener('toggle',()=>{if(card.open)expanded.add(g.key);else expanded.delete(g.key);});
  for(const s of g.stops){const row=el('div',undefined,'overview-stop'+(s.sharedRank?' overview-warning':'')),rank=el('strong','Stop '+s.rank,'overview-rank'),name=el('span',s.customer.customerName);if(s.sharedRank){name.append(el('small','Shared stop number — review order'));}row.append(rank,name);editButton(row,s.customer);card.append(row);}grid.append(card);
 }
 if(!result.groups.length)grid.append(el('p','No saved route rules match these filters.'));
 const missing=el('details',undefined,'overview-section unruled-section');missing.open=true;missing.append(el('summary','Customers without any rules · '+result.unruled.length),el('p','Across all weekdays and routes. Search above to narrow this list.'));
 for(const c of result.unruled){const row=el('div',undefined,'overview-stop');row.append(el('span',c.customerName));editButton(row,c,'Add rule');missing.append(row);}if(!result.unruled.length)missing.append(el('p','Every customer matching your search has a saved rule.'));host.append(missing);
 if(result.unusedRoutes.length){const empty=el('details',undefined,'overview-section');empty.append(el('summary','Saved routes without matching customer rules · '+result.unusedRoutes.length),el('p',result.unusedRoutes.map(t=>t.name).join(' · ')));host.append(empty);}
 function editButton(parent,c,label='Edit rule'){const b=el('button',label,'secondary');b.type='button';b.setAttribute('aria-label',label+' for '+c.customerName);b.onclick=()=>onEdit(c);parent.append(b);}
 return result;
}
