/** Native Android cue where available; browser fallback starts only after audio is ready. */
export function scanErrorSound({native=()=>globalThis.DrawFeedback,vibrate=pattern=>globalThis.navigator?.vibrate?.(pattern),context=()=>new(globalThis.AudioContext||globalThis.webkitAudioContext)()}={}){
 let audio,playing=false;
 async function unlock(){try{audio??=context();if(audio.state==='suspended')await audio.resume();return audio.state==='running';}catch{return false;}}
 async function play(){if(playing)return false;playing=true;try{const bridge=native();if(typeof bridge?.errorSound==='function'){try{bridge.errorSound();return true;}catch{}}try{vibrate([180,80,180]);}catch{}if(!await unlock())return false;const start=audio.currentTime+.025;for(let i=0;i<2;i++){const oscillator=audio.createOscillator(),gain=audio.createGain(),at=start+i*.22;oscillator.type='sine';oscillator.frequency.value=i?550:880;gain.gain.setValueAtTime(0,at);gain.gain.linearRampToValueAtTime(.3,at+.012);gain.gain.setValueAtTime(.3,at+.14);gain.gain.linearRampToValueAtTime(0,at+.17);oscillator.connect(gain);gain.connect(audio.destination);oscillator.onended=()=>{oscillator.disconnect();gain.disconnect();};oscillator.start(at);oscillator.stop(at+.18);}return true;}catch{return false;}finally{playing=false;}}
 return {unlock,play};
}
