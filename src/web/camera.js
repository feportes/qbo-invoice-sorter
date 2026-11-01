let controls=null,stream=null,generation=0;
export async function scanCamera(onScan){
 const dialog=document.getElementById('camera'),video=dialog.querySelector('video'),status=dialog.querySelector('p'),ticket=++generation;
 const stop=()=>{generation++;controls?.stop();controls=null;stream?.getTracks().forEach(t=>t.stop());stream=null;video.srcObject?.getTracks().forEach(t=>t.stop());video.srcObject=null;if(dialog.open)dialog.close();};
 dialog.querySelector('button').onclick=stop;dialog.oncancel=stop;dialog.showModal();status.textContent='Point the rear camera at the barcode.';
 try{
  if(!navigator.mediaDevices?.getUserMedia)throw Error('Camera requires a secure connection and camera permission. You can still type the barcode.');
  const reader=new window.ZXingBrowser.BrowserMultiFormatReader();
  const result=await reader.decodeFromConstraints({audio:false,video:{facingMode:{ideal:'environment'},width:{ideal:1280},height:{ideal:720}}},video,(value,_error,session)=>{if(value&&ticket===generation){controls=session;const code=value.getText();stop();onScan(code);}});
  if(ticket!==generation)result.stop();else controls=result;
 }catch(error){stop();onScan(null,error.message||'Camera unavailable. Allow camera access in Settings, or type the code.');}
 document.addEventListener('visibilitychange',()=>{if(document.hidden)stop();},{once:true});
}
