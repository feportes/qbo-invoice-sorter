// A backdrop tap cancels only the top dialog; clicks inside a form never dismiss it.
let start;
function outside(dialog,event){const r=dialog.getBoundingClientRect();return event.clientX<r.left||event.clientX>r.right||event.clientY<r.top||event.clientY>r.bottom;}
document.addEventListener('pointerdown',e=>{start=e.target?.tagName==='DIALOG'&&outside(e.target,e)?{dialog:e.target,id:e.pointerId}:null;},true);
document.addEventListener('pointerup',e=>{const active=start;start=null;if(!active||active.id!==e.pointerId||e.target!==active.dialog||!outside(active.dialog,e)||active.dialog.dataset.dismissOnBackdrop==='false'||active.dialog.getAttribute('aria-busy')==='true')return;const cancel=new Event('cancel',{cancelable:true});if(active.dialog.dispatchEvent(cancel))active.dialog.close();},true);
