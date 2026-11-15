async function handle(S,a,d,tok){
const hx=b=>[...new Uint8Array(b)].map(x=>x.toString(16).padStart(2,'0')).join('');
const H=async s=>hx(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(s)));
const rid=()=>hx(crypto.getRandomValues(new Uint8Array(10)));
const env=typeof process!=='undefined'&&process.env?process.env:{};
const AU=env.ADMIN_USER||'lord',AP=env.ADMIN_PASS||'lord@123';
const J=async k=>{const v=await S.get(k);return v?JSON.parse(v):null},W=(k,v)=>S.set(k,JSON.stringify(v));
const SEED=[['Data Communication & Networking','ICTEW 353'],['Ethics',''],['Guidance & Counselling',''],['MOT',''],['Research',''],['Technology Leadership',''],['Trends','']].map((x,i)=>({id:'c'+(i+1),name:x[0],code:x[1],school:'UEW',level:'All levels',materials:[]}));
const who=tok?await S.get('t:'+tok):null,adm=who==='#admin';
const u=(who&&!adm)?await J('u:'+who):null;
const pub=o=>{o={...o};delete o.salt;delete o.hash;return o};
const sess=async v=>{const t=rid();await S.set('t:'+t,v,2592000);return t};
const cl=async(sys,msgs,n)=>{const k=env.ANTHROPIC_API_KEY;if(!k)return{error:'The AI tutor is not switched on yet. Add ANTHROPIC_API_KEY in Vercel.'};const r=await fetch('https://api.anthropic.com/v1/messages',{method:'POST',headers:{'content-type':'application/json','x-api-key':k,'anthropic-version':'2023-06-01'},body:JSON.stringify({model:env.ANTHROPIC_MODEL||'claude-sonnet-4-5',max_tokens:n||900,system:sys,messages:msgs})});const j=await r.json();return j.error?{error:j.error.message}:{text:(j.content||[]).map(c=>c.text||'').join('')}};
if(a==='signup'){const e=(d.email||'').trim().toLowerCase();if(!e||!d.name||!d.password||d.password.length<6)return{error:'Fill every field. Password needs 6+ characters.'};if(await S.get('u:'+e))return{error:'This email already has an account. Log in instead.'};const salt=rid(),n={email:e,name:String(d.name).slice(0,60),school:String(d.school||'').slice(0,80),level:d.level||'',pic:'',xp:0,salt,hash:await H(salt+d.password)};await W('u:'+e,n);await W('ulist',((await J('ulist'))||[]).concat(e));return{token:await sess(e),user:pub(n)}}
if(a==='login'){const id=(d.id||'').trim().toLowerCase();if(id===AU.toLowerCase()&&d.password===AP)return{token:await sess('#admin'),admin:true};const x=await J('u:'+id);if(!x||x.hash!==await H(x.salt+d.password))return{error:'Wrong email or password.'};return{token:await sess(id),user:pub(x)}}
if(!who||(!adm&&!u))return{error:'Please log in.'};
if(a==='me')return adm?{admin:true}:{user:pub(u)};
if(a==='saveme'&&u){const n={...u};['name','school','level','pic'].forEach(k=>{if(typeof d[k]==='string')n[k]=d[k].slice(0,k==='pic'?60000:80)});if(typeof d.xp==='number')n.xp=Math.max(u.xp||0,Math.min(d.xp,1e6));await W('u:'+u.email,n);return{user:pub(n)}}
if(a==='courses')return{courses:(await J('courses'))||SEED};
if(a==='file'){const f=await S.get('f:'+d.id);return f?{data:f}:{error:'File not found.'}}
if(a==='getq')return{qs:(await J('q:'+d.cid))||[]};
if(a==='addq'){const q=((await J('q:'+d.cid))||[]).concat((d.qs||[]).slice(0,100)).slice(-300);await W('q:'+d.cid,q);return{n:q.length}}
if(a==='yt'){const k=env.YOUTUBE_API_KEY;if(!k)return{items:[],note:'Video search needs YOUTUBE_API_KEY (see README).'};const r=await(await fetch('https://www.googleapis.com/youtube/v3/search?part=snippet&type=video&videoEmbeddable=true&maxResults=9&q='+encodeURIComponent(d.q)+'&key='+k)).json();return{items:(r.items||[]).map(i=>({id:i.id.videoId,title:i.snippet.title,ch:i.snippet.channelTitle}))}}
if(a==='ai'){const m=(d.msgs||[]).slice(-10).map(x=>({role:x.role,content:String(x.content).slice(0,4000)}));while(m.length&&m[0].role!=='user')m.shift();if(!m.length)return{error:'Say something first.'};if(d.img){const l=m[m.length-1];if(l.role==='user')l.content=[{type:'image',source:{type:'base64',media_type:'image/jpeg',data:d.img}},{type:'text',text:l.content}]}return cl('You are Adesua Pa, a friendly AI tutor for students in Ghana. Explain step by step in simple English with short examples. If a screenshot is attached, help with what is on screen.',m)}
if(a==='genq'){const r=await cl('Return ONLY a JSON array of 8 multiple-choice questions: [{"q":"","o":["","","",""],"a":0}] where a is the index of the correct option.',[{role:'user',content:'Course: '+String(d.course).slice(0,100)+'. Topic: '+String(d.topic).slice(0,200)}],1800);if(!r.text)return r;try{return{qs:JSON.parse(r.text.replace(/```json|```/g,'').trim())}}catch(e){return{error:'Could not read the AI reply. Try again.'}}}
if(!adm)return{error:'Admins only.'};
const L=(await J('courses'))||SEED;
if(a==='users')return{n:((await J('ulist'))||[]).length};
if(a==='savecourse'){const c=d.course;c.id=c.id||rid();c.materials=c.materials||[];const i=L.findIndex(x=>x.id===c.id);i<0?L.push(c):L[i]=c;await W('courses',L);return{courses:L}}
if(a==='delcourse'){const c=L.find(x=>x.id===d.id),R=L.filter(x=>x.id!==d.id);for(const m of(c?c.materials:[]))await S.del('f:'+m.id);await S.del('q:'+d.id);await W('courses',R);return{courses:R}}
if(a==='upload'){if(!/^data:application\/pdf;base64,/.test(d.data||'')||d.data.length>9e5)return{error:'PDF only, up to about 650 KB per file. Compress it first.'};const c=L.find(x=>x.id===d.cid);if(!c)return{error:'Course not found.'};const id=rid();await S.set('f:'+id,d.data);c.materials.push({id,title:String(d.title||'Untitled').slice(0,80)});await W('courses',L);return{courses:L}}
return{error:'Unknown action.'}}
if(typeof module!=='undefined')module.exports={handle};
