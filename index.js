const {handle}=require('../lib/handler.js');
const U=process.env.UPSTASH_REDIS_REST_URL||process.env.KV_REST_API_URL,T=process.env.UPSTASH_REDIS_REST_TOKEN||process.env.KV_REST_API_TOKEN;
const cmd=async c=>{const r=await fetch(U,{method:'POST',headers:{Authorization:'Bearer '+T},body:JSON.stringify(c)});const j=await r.json();if(j.error)throw new Error(j.error);return j.result};
const S={get:k=>cmd(['GET',k]),set:(k,v,t)=>cmd(t?['SET',k,v,'EX',t]:['SET',k,v]),del:k=>cmd(['DEL',k])};
module.exports=async(req,res)=>{try{if(!U||!T)throw new Error('Upstash environment variables are missing');const b=typeof req.body==='string'?JSON.parse(req.body||'{}'):(req.body||{});res.status(200).json(await handle(S,b.a,b.d||{},b.tok))}catch(e){res.status(200).json({error:'Server error: '+e.message})}};
