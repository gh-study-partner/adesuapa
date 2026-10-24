'use strict';
// Study Partner — the whole server in ONE file (accounts, ads, photos, YouTube, news and the search engine).
const __M = {};
__M.engine = (function () {
const module = { exports: {} }; const exports = module.exports;
// Study Partner search engine — no API keys, no dependencies.
// Pipeline (like a small Google): retrieve from several public sources -> fuse + re-rank -> read the best pages
// -> pick the sentences that actually answer the question -> cite sources. A knowledge base (Upstash Redis)
// remembers what it learned and a daily job (api/cron.js) keeps learning from the internet.
'use strict';
const dns = require('dns').promises;
const net = require('net');
const crypto = require('crypto');

// Finds the Upstash/Vercel KV settings under any name (Vercel lets you choose a custom prefix when connecting).
function findRedisEnv() {
  const e = process.env, names = Object.keys(e);
  let url = e.KV_REST_API_URL || e.UPSTASH_REDIS_REST_URL, token = e.KV_REST_API_TOKEN || e.UPSTASH_REDIS_REST_TOKEN;
  if (!url || !token) { // a custom prefix chosen when connecting (e.g. STORAGE_KV_REST_API_URL)
    for (const k of names) {
      const m = /^(.*?)(?:KV_)?(?:REST_API_URL|REDIS_REST_URL)$/.exec(k); if (!m || !/^https:\/\//.test(e[k] || '') || /READ_ONLY/.test(k)) continue;
      const t = [m[1] + 'KV_REST_API_TOKEN', m[1] + 'REST_API_TOKEN', m[1] + 'REDIS_REST_TOKEN', m[1] + 'KV_REST_TOKEN'].find((n) => e[n]);
      if (t) { url = e[k]; token = e[t]; break; }
    }
  }
  if (!url || !token) { // recognise the pair by what it looks like
    const u = names.find((k) => /^https:\/\/[\w.-]+\.upstash\.io\/?$/i.test(e[k] || ''));
    const tk = names.find((k) => /TOKEN$/i.test(k) && !/READ_?ONLY|_RO_/i.test(k) && /REST|KV|REDIS|UPSTASH|STORAGE/i.test(k) && e[k]);
    if (u && tk) { url = e[u]; token = e[tk]; }
  }
  if (!url || !token) { // only the redis:// address is present: Upstash's REST token is that address's password
    for (const k of names.filter((n) => /(KV_URL|REDIS_URL|REDIS_TLS_URL)$/.test(n))) {
      const m = /^rediss?:\/\/[^:@\/]*:([^@]+)@([^:\/]+)/.exec(e[k] || '');
      if (m && /upstash\.io$/i.test(m[2])) { url = 'https://' + m[2]; token = decodeURIComponent(m[1]); break; }
    }
  }
  return { url: url ? String(url).replace(/\/+$/, '') : url, token };
}
const { url: KV_URL, token: KV_TOKEN } = findRedisEnv();
const REDIS = !!(KV_URL && KV_TOKEN);
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36';
const DAY = 86400000;

/* ================= storage (Upstash Redis, or memory for local tests) ================= */
const MEM = new Map();
function mget(k) { const e = MEM.get(k); if (!e) return undefined; if (e.x && e.x < Date.now()) { MEM.delete(k); return undefined; } return e; }
function memCmd(cmd) {
  const [op, k, ...a] = cmd, O = String(op).toUpperCase(), e = mget(k);
  switch (O) {
    case 'GET': return e ? e.v : null;
    case 'SET': { if (a.some((z) => String(z).toUpperCase() === 'NX') && e) return null; let x = 0; const i = a.findIndex((z) => String(z).toUpperCase() === 'EX'); if (i >= 0) x = Date.now() + (+a[i + 1]) * 1000; MEM.set(k, { v: String(a[0]), x }); return 'OK'; }
    case 'DEL': return MEM.delete(k) ? 1 : 0;
    case 'INCR': { const n = (e ? +e.v : 0) + 1; MEM.set(k, { v: String(n), x: e ? e.x : 0 }); return n; }
    case 'EXPIRE': if (e) e.x = Date.now() + (+a[0]) * 1000; return e ? 1 : 0;
    case 'ZINCRBY': { const m = e ? e.v : new Map(); m.set(a[1], (m.get(a[1]) || 0) + (+a[0])); MEM.set(k, { v: m, x: e ? e.x : 0 }); return String(m.get(a[1])); }
    case 'ZREVRANGE': { if (!e) return []; const s = +a[0], t = +a[1], ws = a.some((z) => String(z).toUpperCase() === 'WITHSCORES'); const r = [...e.v.entries()].sort((x, y) => y[1] - x[1]).slice(s, t < 0 ? undefined : t + 1); return ws ? r.flatMap((x) => [x[0], String(x[1])]) : r.map((x) => x[0]); }
    case 'ZSCORE': return e && e.v.has(a[0]) ? String(e.v.get(a[0])) : null;
    case 'ZREMRANGEBYRANK': { if (!e) return 0; const arr = [...e.v.entries()].sort((x, y) => x[1] - y[1]); const s = +a[0], t = +a[1]; const n = arr.length, from = s < 0 ? Math.max(0, n + s) : s, to = t < 0 ? n + t : t; for (const [m] of arr.slice(from, to + 1)) e.v.delete(m); return 1; }
    case 'RPUSH': { const l = e ? e.v : []; l.push(...a.map(String)); MEM.set(k, { v: l, x: e ? e.x : 0 }); return l.length; }
    case 'LRANGE': { if (!e) return []; const s = +a[0], t = +a[1]; return e.v.slice(s, t < 0 ? undefined : t + 1); }
    case 'LTRIM': { if (e) { const s = +a[0], t = +a[1]; e.v = e.v.slice(s < 0 ? Math.max(0, e.v.length + s) : s, t === -1 ? undefined : t + 1); } return 'OK'; }
    default: throw new Error('unsupported ' + O);
  }
}
async function kv(cmd) {
  if (!REDIS) return memCmd(cmd);
  const r = await fetch(KV_URL, { method: 'POST', headers: { Authorization: 'Bearer ' + KV_TOKEN, 'Content-Type': 'application/json' }, body: JSON.stringify(cmd) });
  const j = await r.json(); if (j.error) throw new Error(j.error); return j.result;
}
async function kvPipe(cmds) {
  if (!REDIS) return cmds.map(memCmd);
  const r = await fetch(KV_URL + '/pipeline', { method: 'POST', headers: { Authorization: 'Bearer ' + KV_TOKEN, 'Content-Type': 'application/json' }, body: JSON.stringify(cmds) });
  const j = await r.json(); return (Array.isArray(j) ? j : []).map((x) => x && x.result);
}
const jget = async (k) => { try { const v = await kv(['GET', k]); return v ? JSON.parse(v) : null; } catch (e) { return null; } };
const jset = (k, o, ex) => kv(ex ? ['SET', k, JSON.stringify(o), 'EX', String(ex)] : ['SET', k, JSON.stringify(o)]).catch(() => null);

/* ================= network safety + fetching ================= */
function privateIp(ip) {
  if (net.isIPv4(ip)) { const [a, b] = ip.split('.').map(Number); return a === 10 || a === 127 || a === 0 || (a === 169 && b === 254) || (a === 172 && b >= 16 && b <= 31) || (a === 192 && b === 168) || (a === 100 && b >= 64 && b <= 127) || a >= 224; }
  if (net.isIPv6(ip)) { const l = ip.toLowerCase(); return l === '::1' || l === '::' || l.startsWith('fc') || l.startsWith('fd') || l.startsWith('fe80') || l.startsWith('::ffff:') ; }
  return true;
}
async function assertPublic(u) {
  const x = new URL(u);
  if (!/^https?:$/.test(x.protocol)) throw new Error('protocol');
  const h = x.hostname.replace(/^\[|\]$/g, '').toLowerCase();
  if (!h || h === 'localhost' || /\.(local|internal|localdomain|lan|home|corp)$/.test(h)) throw new Error('private host');
  if (x.port && !['80', '443', '8080', '8443'].includes(x.port)) throw new Error('port');
  if (process.env.SP_TEST === '1') return;
  if (net.isIP(h)) { if (privateIp(h)) throw new Error('private ip'); return; }
  const addrs = await dns.lookup(h, { all: true });
  if (!addrs.length || addrs.some((a) => privateIp(a.address))) throw new Error('private ip');
}
async function readCapped(res, max) {
  const rd = res.body && res.body.getReader ? res.body.getReader() : null;
  if (!rd) { const b = Buffer.from(await res.arrayBuffer()); return b.subarray(0, max); }
  const parts = []; let n = 0;
  while (n < max) { const { done, value } = await rd.read(); if (done) break; parts.push(Buffer.from(value)); n += value.length; }
  try { await rd.cancel(); } catch (e) { /* done */ }
  return Buffer.concat(parts).subarray(0, max);
}
function decodeBuf(buf, ct) {
  let cs = (/charset=([\w-]+)/i.exec(ct || '') || [])[1];
  if (!cs) { const head = buf.subarray(0, 2048).toString('latin1'); cs = (/<meta[^>]+charset=["']?([\w-]+)/i.exec(head) || [])[1]; }
  try { return new TextDecoder(cs || 'utf-8').decode(buf); } catch (e) { return buf.toString('utf8'); }
}
// Fetch a public web page as text. Redirects are followed by hand so every hop is checked.
async function fetchPage(url, ms, max) {
  const ac = new AbortController(), timer = setTimeout(() => ac.abort(), ms || 4500);
  try {
    let u = url;
    for (let hop = 0; hop < 5; hop++) {
      await assertPublic(u);
      const r = await fetch(u, { redirect: 'manual', signal: ac.signal, headers: { 'user-agent': UA, 'accept-language': 'en-US,en;q=0.9', accept: 'text/html,application/xhtml+xml,text/plain;q=0.9,*/*;q=0.5' } });
      if (r.status >= 300 && r.status < 400 && r.headers.get('location')) { u = new URL(r.headers.get('location'), u).href; continue; }
      if (!r.ok) throw new Error('http ' + r.status);
      const ct = (r.headers.get('content-type') || '').toLowerCase();
      if (!/text\/html|application\/xhtml|text\/plain/.test(ct)) { const e = new Error('type'); e.ctype = ct; throw e; }
      return { url: u, html: decodeBuf(await readCapped(r, max || 900000), ct), plain: /text\/plain/.test(ct) };
    }
    throw new Error('redirects');
  } finally { clearTimeout(timer); }
}
// Trusted public APIs (fixed hosts) — plain fetch with a timeout.
async function getJson(url, ms, opts) {
  const ac = new AbortController(), t = setTimeout(() => ac.abort(), ms || 4500);
  try { const r = await fetch(url, { signal: ac.signal, headers: { 'user-agent': UA, accept: 'application/json', 'api-user-agent': 'StudyPartner/1.0 (student search)' }, ...(opts || {}) }); if (!r.ok) throw new Error('http ' + r.status); return await r.json(); }
  finally { clearTimeout(t); }
}
async function getText(url, ms, opts) {
  const ac = new AbortController(), t = setTimeout(() => ac.abort(), ms || 4500);
  try { const r = await fetch(url, { signal: ac.signal, headers: { 'user-agent': UA, 'accept-language': 'en-US,en;q=0.9', ...((opts && opts.headers) || {}) } }); if (!r.ok) throw new Error('http ' + r.status); return await r.text(); }
  finally { clearTimeout(t); }
}

/* ================= text utilities ================= */
const ENT = { amp: '&', lt: '<', gt: '>', quot: '"', apos: "'", nbsp: ' ', ndash: '–', mdash: '—', hellip: '…', rsquo: '’', lsquo: '‘', rdquo: '”', ldquo: '“', copy: '©', times: '×', deg: '°', middot: '·', bull: '•', eacute: 'é', egrave: 'è', agrave: 'à', ouml: 'ö', uuml: 'ü', euro: '€', pound: '£', laquo: '«', raquo: '»' };
const decodeEnt = (t) => String(t || '').replace(/&(#x?[0-9a-f]+|[a-z]+);/gi, (m, g) => {
  if (g[0] === '#') { const c = g[1].toLowerCase() === 'x' ? parseInt(g.slice(2), 16) : parseInt(g.slice(1), 10); try { return c > 0 && c < 0x110000 ? String.fromCodePoint(c) : ''; } catch (e) { return ''; } }
  return ENT[g.toLowerCase()] !== undefined ? ENT[g.toLowerCase()] : m;
});
const stripTags = (h) => decodeEnt(String(h || '').replace(/<[^>]+>/g, ' ')).replace(/\s+/g, ' ').trim();
const clip = (s, n) => { s = String(s || ''); if (s.length <= n) return s; const c = s.slice(0, n).replace(/\s+\S*$/, ''); return c.replace(/[,;:\s]+$/, '') + '…'; };
const domainOf = (u) => { try { return new URL(u).hostname.replace(/^www\./, ''); } catch (e) { return ''; } };
const uniq = (a) => [...new Set(a)];

const STOP = new Set('a an the of and or to in on at for with by from as is are was were be been being it its this that these those what which who whom whose when where why how do does did can could should would will shall may might must about into than then so such not no yes me my we our you your he she they them their his her i if but also very more most much many some any each other between among over under again further once here there both few own same too just only tell explain describe define give list state name please pls'.split(' '));
const stem = (w) => { w = w.toLowerCase(); if (w.length > 4 && w.endsWith('ies')) return w.slice(0, -3) + 'y'; if (w.length > 5 && w.endsWith('ing')) return w.slice(0, -3); if (w.length > 4 && w.endsWith('ed')) return w.slice(0, -2); if (w.length > 3 && w.endsWith('s') && !w.endsWith('ss') && !w.endsWith('us') && !w.endsWith('is')) return w.slice(0, -1); return w; };
const tokenize = (t) => (String(t || '').toLowerCase().match(/[a-z0-9\u00c0-\u024f]+(?:'[a-z]+)?/g) || []).map((w) => w.replace(/'s$/, ''));
const qTerms = (q) => uniq(tokenize(q).filter((w) => !STOP.has(w) && (w.length > 1 || /\d/.test(w))).map(stem));
const ABBR = /\b(Dr|Mr|Mrs|Ms|Prof|St|Mt|vs|etc|e\.g|i\.e|No|Fig|approx|Inc|Ltd|Jr|Sr|U\.S|U\.K|c|ca)\./g;
function sentences(text) {
  const t = String(text || '').replace(ABBR, (m) => m.replace(/\./g, '\u0001')).replace(/\b([A-Z])\.(?=[A-Z]\.|\s[A-Z]\.)/g, '$1\u0001');
  return (t.match(/[^.!?]+(?:[.!?]+["')\]]*(?=\s|$)|$)/g) || []).map((s) => s.replace(/\u0001/g, '.').trim()).filter(Boolean);
}
const BORING = /(cookie|subscribe|newsletter|sign up|sign in|log in|all rights reserved|privacy policy|terms of (use|service)|click here|javascript|advertis|copyright ©|read more|share this|follow us|whatsapp|facebook|twitter|instagram|©)/i;

/* ---------- HTML -> readable blocks ---------- */
function htmlToBlocks(html, base) {
  html = String(html || '');
  const title = decodeEnt(((/<meta[^>]+property=["']og:title["'][^>]+content=["']([^"']+)/i.exec(html) || /<title[^>]*>([\s\S]*?)<\/title>/i.exec(html) || [])[1]) || '').replace(/\s+/g, ' ').trim();
  const site = decodeEnt((/<meta[^>]+property=["']og:site_name["'][^>]+content=["']([^"']+)/i.exec(html) || [])[1] || '');
  let hero = (/<meta[^>]+property=["']og:image["'][^>]+content=["']([^"']+)/i.exec(html) || [])[1] || '';
  try { if (hero) hero = new URL(decodeEnt(hero), base).href; } catch (e) { hero = ''; }
  if (hero && !/^https?:/i.test(hero)) hero = '';
  let h = html.replace(/<!--[\s\S]*?-->/g, ' ').replace(/<(script|style|noscript|svg|iframe|form|nav|footer|aside|template|button|select|textarea|dialog)\b[\s\S]*?<\/\1>/gi, ' ');
  const mm = /<(article|main)\b[^>]*>([\s\S]*)<\/\1>/i.exec(h); if (mm && mm[2].length > 1500) h = mm[2];
  h = h.replace(/<(div|section)\b[^>]*(?:id|class)=["'][^"']*(?:sidebar|comment|cookie|banner|related|share|social|promo|newsletter|menu|breadcrumb|advert|popup|modal)[^"']*["'][^>]*>[\s\S]*?<\/\1>/gi, ' ');
  const blocks = [], re = /<(h[1-4]|p|li|blockquote|pre|dd|figcaption)\b[^>]*>([\s\S]*?)<\/\1>/gi; let m, seen = new Set();
  while ((m = re.exec(h)) && blocks.length < 400) {
    const tag = m[1].toLowerCase(), x = stripTags(m[2]);
    if (!x) continue;
    if (tag[0] === 'h') { if (x.length > 1 && x.length < 140 && !BORING.test(x)) blocks.push({ t: 'h', x }); continue; }
    if (tag === 'li') { if (x.length < 18 || x.length > 260 || BORING.test(x)) continue; }
    else if (x.length < 40 || BORING.test(x)) continue;
    const key = x.slice(0, 80); if (seen.has(key)) continue; seen.add(key);
    blocks.push({ t: tag === 'li' ? 'li' : tag === 'blockquote' ? 'q' : 'p', x: clip(x, 1500) });
  }
  const imgs = []; const ire = /<img\b[^>]*>/gi; let im;
  while ((im = ire.exec(h)) && imgs.length < 5) {
    const tag = im[0], src = (/\bsrc=["']([^"']+)/i.exec(tag) || /data-src=["']([^"']+)/i.exec(tag) || [])[1]; if (!src || /^data:/.test(src)) continue;
    const w = +((/\bwidth=["']?(\d+)/i.exec(tag) || [])[1] || 0); if ((w && w < 200) || /pixel|sprite|logo|icon|avatar|badge|tracking|spacer|blank|ads?\b/i.test(src)) continue;
    try { const abs = new URL(decodeEnt(src), base).href; if (/^https?:/.test(abs)) imgs.push({ src: abs, alt: decodeEnt((/\balt=["']([^"']*)/i.exec(tag) || [])[1] || '').slice(0, 140) }); } catch (e) { /* skip */ }
  }
  return { title, site, hero, blocks, imgs };
}
function plainToBlocks(text) {
  return { title: '', site: '', hero: '', imgs: [], blocks: String(text).split(/\n{2,}/).map((x) => x.replace(/\s+/g, ' ').trim()).filter((x) => x.length > 30).slice(0, 300).map((x) => ({ t: 'p', x: clip(x, 1500) })) };
}

/* ================= question understanding ================= */
function classify(q) {
  const t = String(q).toLowerCase().trim();
  if (/\b(differentiate|distinguish|difference[s]? between|compare|comparison|contrast)\b/.test(t) || /\s(vs\.?|versus)\s/.test(t)) return 'compare';
  if (/^(who|what)\s+(is|are|was|were)\s+(the\s+)?(current|present|new|latest)\b/.test(t) || /\b(news|latest|today|current affairs|breaking|this week|this month|right now|updates?|exchange rate|price of|score|results? of|202[5-9])\b/.test(t)) return 'current';
  if (/^(list|state|name|outline|enumerate|mention|identify|give (me )?(some|\d+))\b/.test(t) || /\b(types|kinds|examples|advantages|disadvantages|features|characteristics|functions|stages|principles|causes|effects|components|elements|uses|benefits) of\b/.test(t)) return 'list';
  if (/^(how (do|to|can|does|did|should)|steps (to|for|of|in)|guide to|ways to|procedure for)\b/.test(t)) return 'howto';
  if (/\b(solve|calculate|equation|integral|derivative|formula|proof|simplify)\b/.test(t)) return 'math';
  if (/^(what is|what are|what's|define|definition of|meaning of|explain|who is|who was|who are|describe)\b/.test(t)) return 'define';
  return 'general';
}
function cleanQuery(q) {
  return String(q).replace(/\?+\s*$/, '').replace(/^(what\s+(is|are|was|were)( the)?|who\s+(is|are|was|were)( the)?|define|definition of|meaning of|explain|describe|tell me about|how (do|does|to|can)( i| you| we)?)\s+/i, '').trim() || String(q);
}
function parseCompare(q) {
  const t = String(q).replace(/\?+\s*$/, '').replace(/\b(in a table|with examples?|in detail|briefly|tabular form)\b/gi, '').trim();
  const m = /(?:differentiate|distinguish|difference[s]?|compare|comparison|contrast)\s*(?:between|of)?\s*(.+?)\s+(?:and|vs\.?|versus|&|from|with)\s+(.+)$/i.exec(t) || /^(.+?)\s+(?:vs\.?|versus)\s+(.+)$/i.exec(t);
  if (!m) return null;
  const c = (s) => s.replace(/^(a|an|the)\s+/i, '').replace(/[?.,;:]+$/, '').trim();
  const a = c(m[1]), b = c(m[2]); return a && b && a.toLowerCase() !== b.toLowerCase() ? [a, b] : null;
}

/* ================= instant answers (no search needed) ================= */
function calc(expr) { // safe recursive-descent calculator — never uses eval
  const s = expr.toLowerCase().replace(/×|x(?=\s*[\d(])/g, '*').replace(/÷/g, '/').replace(/\^/g, '**').replace(/,(?=\d{3}\b)/g, '').replace(/π/g, 'pi');
  let i = 0; const peek = () => { while (s[i] === ' ') i++; return s[i]; };
  const FN = { sqrt: Math.sqrt, sin: (x) => Math.sin(x * Math.PI / 180), cos: (x) => Math.cos(x * Math.PI / 180), tan: (x) => Math.tan(x * Math.PI / 180), log: Math.log10, ln: Math.log, abs: Math.abs, round: Math.round, floor: Math.floor, ceil: Math.ceil, exp: Math.exp };
  function prim() {
    const c = peek();
    if (c === '(') { i++; const v = add(); if (peek() !== ')') throw new Error('paren'); i++; return v; }
    if (c === '-') { i++; return -pow(); } if (c === '+') { i++; return pow(); }
    const n = /^\d*\.?\d+(?:e[+-]?\d+)?/.exec(s.slice(i));
    if (n) { i += n[0].length; return parseFloat(n[0]); }
    const w = /^[a-z]+/.exec(s.slice(i));
    if (w) { i += w[0].length; if (w[0] === 'pi') return Math.PI; if (w[0] === 'e') return Math.E; if (FN[w[0]]) { if (peek() !== '(') throw new Error('fn'); i++; const v = add(); if (peek() !== ')') throw new Error('paren'); i++; return FN[w[0]](v); } }
    throw new Error('syntax');
  }
  function post() { let v = prim(); while (peek() === '%') { i++; v /= 100; } return v; }
  function pow() { const b = post(); if (s.slice(i, i + 2) === '**' || (peek() === '*' && s[i + 1] === '*')) { while (s[i] !== '*') i++; i += 2; return Math.pow(b, pow()); } return b; }
  function mul() { let v = pow(); for (;;) { const c = peek(); if (c === '*' && s[i + 1] !== '*') { i++; v *= pow(); } else if (c === '/') { i++; v /= pow(); } else return v; } }
  function add() { let v = mul(); for (;;) { const c = peek(); if (c === '+') { i++; v += mul(); } else if (c === '-') { i++; v -= mul(); } else return v; } }
  const v = add(); if (peek() !== undefined) throw new Error('trailing'); return v;
}
const fmtNum = (n) => Number.isInteger(n) && Math.abs(n) < 1e15 ? String(n) : String(+n.toPrecision(10));
const UNITS = { // to base unit
  length: { m: 1, meter: 1, metre: 1, meters: 1, metres: 1, km: 1000, kilometer: 1000, kilometre: 1000, kilometers: 1000, kilometres: 1000, cm: .01, mm: .001, mile: 1609.344, miles: 1609.344, mi: 1609.344, yard: .9144, yards: .9144, yd: .9144, foot: .3048, feet: .3048, ft: .3048, inch: .0254, inches: .0254, in: .0254 },
  mass: { kg: 1, kilogram: 1, kilograms: 1, g: .001, gram: .001, grams: .001, mg: 1e-6, lb: .45359237, lbs: .45359237, pound: .45359237, pounds: .45359237, oz: .0283495, ounce: .0283495, ounces: .0283495, tonne: 1000, tonnes: 1000, ton: 1000 },
  volume: { l: 1, liter: 1, litre: 1, liters: 1, litres: 1, ml: .001, gallon: 3.78541, gallons: 3.78541, cup: .236588, cups: .236588, pint: .473176, pints: .473176 },
  speed: { 'km/h': 1, kph: 1, kmh: 1, 'mph': 1.609344, 'mi/h': 1.609344, 'm/s': 3.6, knots: 1.852, knot: 1.852 },
  area: { 'm2': 1, 'sqm': 1, 'km2': 1e6, 'ha': 1e4, hectare: 1e4, hectares: 1e4, acre: 4046.86, acres: 4046.86, 'ft2': .092903 },
  time: { s: 1, sec: 1, seconds: 1, second: 1, min: 60, minute: 60, minutes: 60, h: 3600, hr: 3600, hour: 3600, hours: 3600, day: 86400, days: 86400, week: 604800, weeks: 604800, year: 31557600, years: 31557600 }
};
const CUR = { cedi: 'GHS', cedis: 'GHS', ghs: 'GHS', usd: 'USD', dollar: 'USD', dollars: 'USD', naira: 'NGN', ngn: 'NGN', gbp: 'GBP', pound: 'GBP', pounds: 'GBP', sterling: 'GBP', eur: 'EUR', euro: 'EUR', euros: 'EUR', rand: 'ZAR', zar: 'ZAR', cfa: 'XOF', xof: 'XOF', yen: 'JPY', jpy: 'JPY', yuan: 'CNY', rmb: 'CNY', cny: 'CNY', inr: 'INR', rupee: 'INR', rupees: 'INR', kes: 'KES', cad: 'CAD', aud: 'AUD', chf: 'CHF', aed: 'AED', sar: 'SAR', egp: 'EGP', mad: 'MAD', xaf: 'XAF', ugx: 'UGX', tzs: 'TZS', rwf: 'RWF', brl: 'BRL' };
const WMO = { 0: 'Clear sky', 1: 'Mostly clear', 2: 'Partly cloudy', 3: 'Overcast', 45: 'Fog', 48: 'Fog', 51: 'Light drizzle', 53: 'Drizzle', 55: 'Heavy drizzle', 61: 'Light rain', 63: 'Rain', 65: 'Heavy rain', 71: 'Light snow', 73: 'Snow', 75: 'Heavy snow', 80: 'Rain showers', 81: 'Rain showers', 82: 'Violent showers', 95: 'Thunderstorm', 96: 'Thunderstorm, hail', 99: 'Thunderstorm, hail' };
async function instant(q) {
  const t = q.trim().replace(/\?+$/, '');
  // 1) calculator
  const pc = /^(?:what(?:'s| is) )?(\d+(?:\.\d+)?)\s*%\s*of\s*(\d+(?:\.\d+)?)$/i.exec(t);
  if (pc) { const v = (+pc[1]) * (+pc[2]) / 100; return { kind: 'calc', title: pc[1] + '% of ' + pc[2], text: fmtNum(v) }; }
  const ex = t.replace(/^(?:what(?:'s| is)|calculate|compute|solve|evaluate)\s+/i, '');
  if (/[\d)]/.test(ex) && /^[\d\s+\-*/^().,%×÷xπa-z]+$/i.test(ex) && /[\d]\s*[+\-*/^%×÷x]|^[a-z]+\(|\)\s*[+\-*/^]/i.test(ex) && !/^[a-z\s]+$/i.test(ex)) {
    try { const v = calc(ex); if (isFinite(v)) return { kind: 'calc', title: ex.replace(/\s+/g, ' '), text: fmtNum(v) }; } catch (e) { /* not maths */ }
  }
  // 2) currency / unit conversion
  const cv = /^(?:convert\s+)?(-?[\d.,]+)\s*([a-z°/²³0-9 ]{1,16}?)\s+(?:to|in|into|=|equals?)\s+([a-z°/²³0-9 ]{1,16})$/i.exec(t);
  if (cv) {
    const amt = parseFloat(cv[1].replace(/,/g, '')), a = cv[2].trim().toLowerCase(), b = cv[3].trim().toLowerCase();
    if (isFinite(amt)) {
      const tmp = { c: 'celsius', celsius: 'celsius', '°c': 'celsius', f: 'fahrenheit', fahrenheit: 'fahrenheit', '°f': 'fahrenheit', k: 'kelvin', kelvin: 'kelvin' };
      if (tmp[a] && tmp[b]) { const c = tmp[a] === 'celsius' ? amt : tmp[a] === 'fahrenheit' ? (amt - 32) * 5 / 9 : amt - 273.15; const r = tmp[b] === 'celsius' ? c : tmp[b] === 'fahrenheit' ? c * 9 / 5 + 32 : c + 273.15; return { kind: 'convert', title: amt + ' ' + tmp[a] + ' to ' + tmp[b], text: (+r.toFixed(2)) + ' ' + tmp[b] }; }
      for (const grp of Object.values(UNITS)) if (grp[a] && grp[b]) { const r = amt * grp[a] / grp[b]; return { kind: 'convert', title: amt + ' ' + a + ' to ' + b, text: fmtNum(+r.toPrecision(8)) + ' ' + b }; }
      if (CUR[a] && CUR[b]) {
        try {
          const j = await getJson('https://open.er-api.com/v6/latest/' + CUR[a], 4000), rate = j && j.rates && j.rates[CUR[b]];
          if (rate) return { kind: 'currency', title: amt + ' ' + CUR[a] + ' to ' + CUR[b], text: (+(amt * rate).toFixed(2)).toLocaleString('en-US') + ' ' + CUR[b], note: '1 ' + CUR[a] + ' = ' + (+rate.toPrecision(5)) + ' ' + CUR[b] + ' · rates update daily · source: open.er-api.com', url: 'https://www.exchangerate-api.com' };
        } catch (e) { /* fall through to normal search */ }
      }
    }
  }
  // 3) weather / local time (Open-Meteo, no key)
  const wm = /^(?:what(?:'s| is) the )?weather(?: like)?(?: today)?(?: in| at| for)?\s+(.{2,60})$/i.exec(t) || /^(.{2,60}?)\s+weather(?: today| now| forecast)?$/i.exec(t);
  const tm = /^(?:what(?:'s| is) the )?(?:current )?(?:local )?time(?: right now)?(?: in| at| for)\s+(.{2,60})$/i.exec(t);
  const place = (wm || tm || [])[1];
  if (place) {
    try {
      const g = await getJson('https://geocoding-api.open-meteo.com/v1/search?count=1&language=en&name=' + encodeURIComponent(place.trim()), 4000), p = g.results && g.results[0];
      if (p) {
        const label = [p.name, p.admin1, p.country].filter(Boolean).join(', ');
        if (tm) { const now = new Date(); const s = new Intl.DateTimeFormat('en-GB', { timeZone: p.timezone, weekday: 'long', day: 'numeric', month: 'long', hour: '2-digit', minute: '2-digit', hour12: true }).format(now); return { kind: 'time', title: 'Time in ' + label, text: s }; }
        const w = await getJson('https://api.open-meteo.com/v1/forecast?latitude=' + p.latitude + '&longitude=' + p.longitude + '&current=temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code&daily=temperature_2m_max,temperature_2m_min,precipitation_probability_max&timezone=auto&forecast_days=3', 4500);
        const c = w.current, d = w.daily;
        const days = d.time.map((day, i) => new Date(day + 'T12:00:00Z').toLocaleDateString('en-GB', { weekday: 'short', timeZone: 'UTC' }) + ' ' + Math.round(d.temperature_2m_min[i]) + '–' + Math.round(d.temperature_2m_max[i]) + '°C, rain ' + (d.precipitation_probability_max[i] == null ? '?' : d.precipitation_probability_max[i]) + '%');
        return { kind: 'weather', title: 'Weather in ' + label, text: Math.round(c.temperature_2m) + '°C · ' + (WMO[c.weather_code] || 'Weather'), note: 'Humidity ' + c.relative_humidity_2m + '% · wind ' + Math.round(c.wind_speed_10m) + ' km/h', rows: days, url: 'https://open-meteo.com' };
      }
    } catch (e) { /* fall through */ }
  }
  // 4) dictionary
  const dm = /^(?:define|definition of|meaning of|what(?:'s| is) the (?:meaning|definition) of|what does)\s+([a-z][a-z'-]{1,29})(?:\s+mean)?$/i.exec(t);
  if (dm) {
    try {
      const j = await getJson('https://api.dictionaryapi.dev/api/v2/entries/en/' + encodeURIComponent(dm[1].toLowerCase()), 4000), e = j && j[0];
      if (e) { const ph = e.phonetic || (e.phonetics || []).map((x) => x.text).find(Boolean) || '';
        const defs = []; for (const m of (e.meanings || []).slice(0, 3)) for (const d of (m.definitions || []).slice(0, 2)) defs.push({ pos: m.partOfSpeech, def: d.definition, ex: d.example || '' });
        if (defs.length) return { kind: 'define', title: e.word, text: ph, defs: defs.slice(0, 5), url: 'https://en.wiktionary.org/wiki/' + encodeURIComponent(e.word) }; }
    } catch (e) { /* fall through */ }
  }
  return null;
}

/* ================= retrieval sources ================= */
const wikiBase = 'https://en.wikipedia.org/w/api.php?format=json&formatversion=2&origin=*&';
async function wikiSearch(q, n, intro) {
  const j = await getJson(wikiBase + 'action=query&generator=search&gsrlimit=' + (n || 5) + '&gsrsearch=' + encodeURIComponent(q) + '&prop=extracts%7Cpageimages%7Cdescription%7Cinfo&exintro=1&explaintext=1&exlimit=max&exsentences=' + (intro || 5) + '&piprop=thumbnail&pithumbsize=420&inprop=url&redirects=1', 5000);
  const pages = ((j.query && j.query.pages) || []).slice().sort((a, b) => (a.index || 99) - (b.index || 99));
  return pages.filter((p) => p.extract && !/may (also )?refer to:?/i.test(p.extract.slice(0, 220))).map((p) => ({ title: p.title, extract: p.extract.replace(/\s*\n+\s*/g, ' ').trim(), desc: p.description || '', thumb: p.thumbnail ? p.thumbnail.source : '', url: p.fullurl || 'https://en.wikipedia.org/wiki/' + encodeURIComponent(p.title.replace(/ /g, '_')), idx: p.index || 99 }));
}
function ddgUnwrap(h) { try { const u = new URL(h.startsWith('//') ? 'https:' + h : h, 'https://duckduckgo.com'); const g = u.searchParams.get('uddg'); return g ? decodeURIComponent(g) : u.href; } catch (e) { return ''; } }
function splitByClass(html, cls) { // returns chunks that each start at an element whose class contains cls
  const re = new RegExp('<a\\b[^>]*class="[^"]*\\b' + cls + '\\b[^"]*"[^>]*>', 'g'), idx = []; let m;
  while ((m = re.exec(html))) idx.push({ i: m.index, tag: m[0] });
  return idx.map((x, k) => ({ tag: x.tag, body: html.slice(x.i + x.tag.length, k + 1 < idx.length ? idx[k + 1].i : x.i + 6000) }));
}
const hrefOf = (tag) => decodeEnt((/\bhref="([^"]+)"/i.exec(tag) || [])[1] || '');
async function srcDdg(q) {
  const html = await getText('https://html.duckduckgo.com/html/?kl=wt-wt&q=' + encodeURIComponent(q), 4200, { headers: { referer: 'https://html.duckduckgo.com/' } });
  if (/anomaly|unusual traffic|captcha/i.test(html.slice(0, 20000)) && !/result__a/.test(html)) throw new Error('blocked');
  const out = [];
  for (const c of splitByClass(html, 'result__a')) {
    const url = ddgUnwrap(hrefOf(c.tag)); if (!/^https?:/.test(url) || /duckduckgo\.com\/y\.js|ad_provider|\/aclk/.test(url)) continue;
    const title = stripTags(c.body.slice(0, c.body.indexOf('</a>') < 0 ? 300 : c.body.indexOf('</a>')));
    const sm = /class="[^"]*result__snippet[^"]*"[^>]*>([\s\S]*?)<\/(?:a|div|td)>/i.exec(c.body);
    if (title) out.push({ title, url, snippet: sm ? stripTags(sm[1]) : '' });
  }
  if (!out.length) throw new Error('no results'); return out;
}
function bingUnwrap(h) {
  try { const u = new URL(h, 'https://www.bing.com'); if (/bing\.com$/.test(u.hostname) && u.pathname.startsWith('/ck/')) { const v = u.searchParams.get('u'); if (v && v.startsWith('a1')) { const d = Buffer.from(v.slice(2), 'base64url').toString('utf8'); if (/^https?:/.test(d)) return d; } return ''; } return u.href; } catch (e) { return ''; }
}
async function srcBing(q) {
  const html = await getText('https://www.bing.com/search?setlang=en&cc=gh&count=12&q=' + encodeURIComponent(q), 4200);
  const out = [];
  for (const chunk of html.split(/<li[^>]+class="[^"]*\bb_algo\b[^"]*"/).slice(1)) {
    const a = /<h2[^>]*>\s*<a\b([^>]*)>([\s\S]*?)<\/a>/i.exec(chunk); if (!a) continue;
    const url = bingUnwrap(hrefOf('<a ' + a[1] + '>')); if (!/^https?:/.test(url)) continue;
    const sm = /<p[^>]*>([\s\S]*?)<\/p>/i.exec(chunk) || /class="[^"]*b_lineclamp\d[^"]*"[^>]*>([\s\S]*?)<\/(?:div|p)>/i.exec(chunk);
    out.push({ title: stripTags(a[2]), url, snippet: sm ? stripTags(sm[1]).replace(/^[A-Z][a-z]{2} \d{1,2}, \d{4}\s*[·\-–]\s*/, '') : '' });
  }
  if (!out.length) throw new Error('no results'); return out;
}
async function srcMojeek(q) {
  const html = await getText('https://www.mojeek.com/search?fmt=html&q=' + encodeURIComponent(q), 5000), out = [];
  for (const c of splitByClass(html, 'title')) {
    const url = hrefOf(c.tag); if (!/^https?:/.test(url)) continue;
    const title = stripTags(c.body.slice(0, Math.max(0, c.body.indexOf('</a>')) || 200)), sm = /<p class="s">([\s\S]*?)<\/p>/i.exec(c.body);
    if (title) out.push({ title, url, snippet: sm ? stripTags(sm[1]) : '' });
  }
  if (!out.length) throw new Error('no results'); return out;
}
async function srcMarginalia(q) {
  const j = await getJson('https://api.marginalia.nu/public/search/' + encodeURIComponent(q) + '?count=8', 5000);
  const out = (j.results || []).map((r) => ({ title: String(r.title || r.url), url: r.url, snippet: stripTags(r.description || '') })).filter((r) => /^https?:/.test(r.url));
  if (!out.length) throw new Error('no results'); return out;
}
async function srcStack(q) {
  const j = await getJson('https://api.stackexchange.com/2.3/search/advanced?order=desc&sort=relevance&accepted=True&pagesize=3&site=stackoverflow&q=' + encodeURIComponent(q), 4500);
  return (j.items || []).map((i) => ({ title: decodeEnt(i.title), url: i.link, snippet: 'Stack Overflow · ' + (i.answer_count || 0) + ' answers · accepted answer' }));
}
const SOURCES = { bing: { fn: srcBing, w: 1.0 }, ddg: { fn: srcDdg, w: 1.0 }, mojeek: { fn: srcMojeek, w: 0.8 }, marginalia: { fn: srcMarginalia, w: 0.55 } };

/* ---------- fuse + re-rank (reciprocal rank fusion + our own quality signals) ---------- */
const GOOD = /(\.edu|\.gov|\.ac\.[a-z]{2}|\.edu\.[a-z]{2}|wikipedia\.org|britannica\.com|khanacademy\.org|bbc\.(com|co\.uk)|nature\.com|nih\.gov|who\.int|un\.org|worldbank\.org|reuters\.com|ghanaweb\.com|myjoyonline\.com|citinewsroom\.com|graphic\.com\.gh|coursera\.org|ck12\.org|geeksforgeeks\.org|w3schools\.com|mathsisfun\.com|byjus\.com|investopedia\.com|mayoclinic\.org|sciencedirect\.com|oxfordreference\.com)$/i;
const POOR = /(pinterest\.|facebook\.com|instagram\.com|tiktok\.com|twitter\.com|x\.com$|linkedin\.com|amazon\.|ebay\.|aliexpress\.|alibaba\.|etsy\.|temu\.|youtube\.com|youtu\.be|vimeo\.com|scribd\.com|chegg\.com|coursehero\.com|slideshare\.net)$/i;
const NOREAD = /(youtube\.com|youtu\.be|facebook\.com|instagram\.com|tiktok\.com|twitter\.com|x\.com|pinterest\.|linkedin\.com|reddit\.com|quora\.com|medium\.com\/m\/|scribd\.com|chegg\.com|coursehero\.com)/i;
const canon = (u) => { try { const x = new URL(u); return x.hostname.replace(/^www\./, '') + x.pathname.replace(/\/+$/, '').toLowerCase(); } catch (e) { return u; } };
function fuse(lists, qt) {
  const map = new Map();
  for (const { name, w, items } of lists) items.forEach((it, rank) => {
    const k = canon(it.url); let e = map.get(k);
    if (!e) { e = { ...it, score: 0, from: [] }; map.set(k, e); }
    e.score += w / (60 + rank); e.from.push(name);
    if ((it.snippet || '').length > (e.snippet || '').length) e.snippet = it.snippet;
    if (!e.title || e.title.length < 4) e.title = it.title;
  });
  const out = [...map.values()];
  for (const e of out) {
    const host = domainOf(e.url), tt = new Set(tokenize(e.title + ' ' + e.snippet).map(stem));
    const cov = qt.length ? qt.filter((t) => tt.has(t)).length / qt.length : 0;
    e.score += cov * 0.012;
    if (GOOD.test(host)) e.score += 0.006; if (POOR.test(host)) e.score -= 0.008;
    if (e.from.length > 1) e.score += 0.004 * (e.from.length - 1);
    e.domain = host;
  }
  return out.sort((a, b) => b.score - a.score);
}

/* ================= sentence extraction ================= */
function candidates(pages, qt, onlyTypes) {
  const out = [];
  pages.forEach((pg, pi) => {
    let pos = 0;
    for (const b of pg.blocks) {
      if (!onlyTypes.includes(b.t)) continue;
      for (const s of sentences(b.x)) {
        if (s.length < 35 || s.length > 600 || BORING.test(s)) continue;
        const stems = tokenize(s).map(stem); if (stems.length < 5) continue;
        out.push({ s, page: pi, pos: pos++, stems, joined: stems.join(' '), pageBoost: Math.max(0, 0.05 - pi * 0.015) });
      }
    }
  });
  return out;
}
function scoreCands(c, qt, define) {
  const N = c.length || 1, df = new Map();
  for (const x of c) for (const t of new Set(x.stems)) df.set(t, (df.get(t) || 0) + 1);
  const idf = (t) => Math.log(1 + N / (1 + (df.get(t) || 0)));
  const qi = qt.map(idf), total = qi.reduce((a, b) => a + b, 0) || 1;
  for (const x of c) {
    const set = new Set(x.stems); let m = 0, hit = 0; qt.forEach((t, i) => { if (set.has(t)) { m += qi[i]; hit++; } });
    let bi = 0; for (let i = 0; i < qt.length - 1; i++) if (x.joined.includes(qt[i] + ' ' + qt[i + 1])) bi++;
    const len = x.s.length, lenF = len < 55 ? 0.65 : len > 340 ? 0.75 : 1;
    const def = define && /\b(is|are|was|were|refers? to|means?|defined as|known as|consists? of)\b/i.test(x.s.slice(0, 170)) ? 0.12 : 0;
    x.cov = m / total; x.hit = hit; x.score = (x.cov * 0.75 + Math.min(0.15, bi * 0.07) + def + x.pageBoost + Math.max(0, 0.04 - x.pos * 0.004)) * lenF;
  }
}
function pickTop(c, qt, n, perPage, minCov, avoid) {
  const need = qt.length <= 2 ? 0.99 : minCov;
  const pool = c.filter((x) => x.cov >= need || (qt.length > 2 && x.hit >= 2 && x.cov >= minCov * 0.8)).sort((a, b) => b.score - a.score);
  const pick = [], perP = {};
  const jac = (a, b) => { const A = new Set(a.stems), B = new Set(b.stems); let i = 0; for (const t of A) if (B.has(t)) i++; return i / (A.size + B.size - i || 1); };
  for (const x of pool) {
    if (pick.length >= n) break; if ((perP[x.page] || 0) >= perPage) continue;
    if (pick.some((p) => jac(p, x) > 0.55) || (avoid && avoid.some((p) => jac(p, x) > 0.55))) continue;
    pick.push(x); perP[x.page] = (perP[x.page] || 0) + 1;
  }
  return pick;
}
// Runs of consecutive list items (steps, types, examples…) that sit under a heading/paragraph about the query.
function bestListRun(pages, qt) {
  let best = null; const tset = (s) => new Set(tokenize(s).map(stem)), cov = (s) => { const T = tset(s); return qt.length ? qt.filter((t) => T.has(t)).length / qt.length : 0; };
  pages.forEach((pg, pi) => {
    const b = pg.blocks;
    for (let i = 0; i < b.length;) {
      if (b[i].t !== 'li') { i++; continue; }
      let j = i; while (j < b.length && b[j].t === 'li') j++;
      const items = b.slice(i, j); if (items.length >= 3) {
        const ctx = Math.max(i > 0 ? cov(b[i - 1].x) : 0, i > 1 ? cov(b[i - 2].x) : 0), self = items.reduce((a, x) => a + cov(x.x), 0) / items.length;
        const sc = Math.max(ctx, self) * 0.8 + Math.min(items.length, 10) * 0.02 - pi * 0.03 + (ctx > 0 && self > 0 ? 0.1 : 0);
        if (!best || sc > best.sc) best = { sc, pi, items: items.slice(0, 12).map((x) => x.x), ctx, self };
      }
      i = j;
    }
  });
  return best && Math.max(best.ctx, best.self) >= 0.34 ? best : null;
}

/* ================= knowledge base (what the engine has learned) ================= */
const KB_KEY = 'kb:index', KB_PEND = 'kb:pending', KB_MAX = 900;
const docId = (u) => crypto.createHash('sha1').update(canon(u)).digest('hex').slice(0, 10);
function kbEntry(title, url, text, extra) {
  const tf = new Map(); const add = (t, w) => { for (const x of qTerms(t)) tf.set(x, (tf.get(x) || 0) + w); };
  add(title, 4); add(text, 1);
  return { id: docId(url), t: clip(title, 90), u: url, s: clip(text, 320), k: [...tf.entries()].sort((a, b) => b[1] - a[1]).slice(0, 14).map((x) => x[0]), d: Date.now(), ...(extra || {}) };
}
async function kbLoad() { return (await jget(KB_INDEX_KEY())) || []; }
const KB_INDEX_KEY = () => KB_KEY;
function kbSearch(index, qt, n) {
  if (!qt.length || !index.length) return [];
  const df = new Map(); for (const d of index) for (const k of d.k) df.set(k, (df.get(k) || 0) + 1);
  const N = index.length, idf = (t) => Math.log(1 + N / (1 + (df.get(t) || 0))), total = qt.reduce((a, t) => a + idf(t), 0) || 1;
  return index.map((d) => { const S = new Set(d.k); const tt = new Set(qTerms(d.t)); let m = 0; for (const t of qt) if (S.has(t)) m += idf(t) * (tt.has(t) ? 1.4 : 1); return { d, sc: m / total }; })
    .filter((x) => x.sc >= (qt.length <= 2 ? 0.95 : 0.6)).sort((a, b) => b.sc - a.sc).slice(0, n).map((x) => x.d);
}
async function kbMerge(entries) { // used by the daily job (and by pending searches)
  const cur = await kbLoad(), map = new Map(cur.map((d) => [d.id, d]));
  for (const e of entries) map.set(e.id, { ...map.get(e.id), ...e });
  const all = [...map.values()].sort((a, b) => b.d - a.d).slice(0, KB_MAX);
  await jset(KB_KEY, all); return all.length;
}
async function kbFlushPending() {
  const raw = (await kv(['LRANGE', KB_PEND, '0', '-1']).catch(() => [])) || []; if (!raw.length) return 0;
  const entries = raw.map((r) => { try { return JSON.parse(r); } catch (e) { return null; } }).filter(Boolean);
  await kbMerge(entries); await kv(['DEL', KB_PEND]).catch(() => null); return entries.length;
}

/* ================= the main pipeline ================= */
function relatedFor(q, type, wiki, main) {
  const core = cleanQuery(main ? main.title : q), out = [];
  for (const w of wiki.slice(0, 6)) { if (main && w.title === main.title) continue; out.push('What is ' + w.title + '?'); }
  const T = { define: ['Examples of ' + core, 'Advantages and disadvantages of ' + core, 'History of ' + core], general: ['Examples of ' + core, 'Importance of ' + core], list: ['Explain each type of ' + core], howto: ['Common mistakes when ' + core], compare: [], current: ['Latest news on ' + core], math: ['Worked examples on ' + core] }[type] || [];
  return uniq(T.concat(out)).slice(0, 5);
}
async function wikiSections(title) {
  const j = await getJson(wikiBase + 'action=query&prop=extracts&explaintext=1&exsectionformat=wiki&redirects=1&titles=' + encodeURIComponent(title), 6000);
  const p = j.query && j.query.pages && j.query.pages[0]; if (!p || !p.extract) return [];
  const parts = p.extract.split(/\n(?===+\s*[^=\n]+?\s*==+\s*\n)/), out = [];
  for (const part of parts) {
    const m = /^(=+)\s*([^=\n]+?)\s*=+\s*\n([\s\S]*)$/.exec(part.trim()); if (!m) continue;
    if (/^(see also|references|external links|further reading|notes|sources|bibliography|footnotes|citations)$/i.test(m[2])) continue;
    const body = m[3].replace(/=+\s*[^=\n]+?\s*=+/g, ' ').replace(/\s+/g, ' ').trim(), ss = sentences(body).slice(0, 2).join(' ');
    if (ss.length > 40) out.push({ h: m[2], text: clip(ss, 380) });
  }
  return out.slice(0, 8);
}

async function search(q, opt) {
  opt = opt || {}; const t0 = Date.now(), deep = !!opt.deep;
  q = String(q || '').replace(/\s+/g, ' ').trim().slice(0, 200);
  const type = classify(q), qt = qTerms(cleanQuery(q)), core = cleanQuery(q), timing = {}, status = {};
  const ckey = 'se:v2:' + (deep ? 'd:' : '') + q.toLowerCase();
  if (!opt.fresh) { const c = await jget(ckey); if (c) return { ...c, cached: true }; }

  const isTech = /\b(code|coding|javascript|python|java|html|css|sql|error|exception|function|array|api|react|node|linux|bash|git|compile|programming|algorithm)\b/i.test(q);
  const cmp = type === 'compare' ? parseCompare(q) : null;
  const run = async (name, fn) => { const s = Date.now(); try { const r = await fn(); status[name] = { ok: true, n: Array.isArray(r) ? r.length : 1, ms: Date.now() - s }; return r; } catch (e) { status[name] = { ok: false, err: String(e && e.message || e).slice(0, 40), ms: Date.now() - s }; return null; } };

  // ---- Stage A: ask every source at once ----
  const [inst, wiki, ...web] = await Promise.all([
    run('instant', () => instant(q)),
    run('wikipedia', () => wikiSearch(core, 5, deep ? 8 : 5)),
    ...Object.entries(SOURCES).map(([name, s]) => run(name, () => s.fn(q).then((items) => ({ name, w: s.w, items })))),
    isTech ? run('stack', () => srcStack(q).then((items) => ({ name: 'stack', w: 0.7, items }))) : null
  ].filter(Boolean));
  const wikis = wiki || [];
  let kbIndex = []; try { kbIndex = await kbLoad(); } catch (e) { /* optional */ }
  const kbHits = kbSearch(kbIndex, qt, 4).filter((d) => !wikis.some((w) => canon(w.url) === canon(d.u)));
  const lists = web.filter(Boolean).filter((x) => x.items && x.items.length);
  lists.push({ name: 'wikipedia', w: 1.0, items: wikis.map((w) => ({ title: w.title + ' – Wikipedia', url: w.url, snippet: clip(w.extract, 220) })) });
  const results = fuse(lists, qt);
  timing.search = Date.now() - t0;

  // ---- main Wikipedia article (only if it is really about the question) ----
  const qset = new Set(qt); let main = null;
  for (const w of wikis) { const T = qTerms(w.title); const ov = T.filter((t) => qset.has(t)).length; if (T.length && (ov / T.length >= 0.6 || (w.idx === 1 && qt.length && ov / qt.length >= 0.34))) { main = w; break; } }

  // ---- Stage B: read the best pages ----
  const want = deep ? 6 : 4, readable = results.filter((r) => !NOREAD.test(r.domain) && !/\.(pdf|docx?|pptx?|xlsx?|zip)(\?|$)/i.test(r.url) && !(main && canon(r.url) === canon(main.url))).slice(0, want);
  const pages = (await Promise.all(readable.map(async (r, i) => {
    try { const f = await fetchPage(r.url, deep ? 4500 : 3500, 800000); const p = f.plain ? plainToBlocks(f.html) : htmlToBlocks(f.html, f.url); if (p.blocks.length < 2) return null; return { ...p, url: f.url, rank: i, domain: r.domain, title: p.title || r.title, snippet: r.snippet }; }
    catch (e) { return null; }
  }))).filter(Boolean);
  status.pages = { ok: pages.length > 0, n: pages.length, of: readable.length };
  timing.read = Date.now() - t0;

  // ---- Stage C: compose the answer ----
  const sources = []; const cite = (title, url, kind) => { let s = sources.find((x) => canon(x.url) === canon(url)); if (!s) { s = { n: sources.length + 1, title: clip(title, 90), url, domain: domainOf(url), kind }; sources.push(s); } return s.n; };
  const ans = { headline: null, points: [], list: null, table: null, sections: null };
  const pageDocs = pages.map((p) => ({ blocks: p.blocks }));
  const prose = candidates(pageDocs, qt, ['p', 'q']); scoreCands(prose, qt, type === 'define' || type === 'general');
  const asPoint = (x) => ({ text: clip(x.s, 330), src: cite(pages[x.page].title, pages[x.page].url, 'web') });

  if (cmp) {
    const ents = await Promise.all(cmp.map((e) => run('wiki:' + e, () => wikiSearch(e, 2, 6)).then((r) => (r && r[0]) || null)));
    const [A, B] = ents; const rows = [];
    const first = (w) => w ? clip(sentences(w.extract)[0] || w.extract, 240) : '—', rest = (w) => w ? clip(sentences(w.extract).slice(1, 3).join(' '), 300) : '—';
    if (A || B) {
      rows.push(['What it is', first(A), first(B)]); rows.push(['In more detail', rest(A), rest(B)]);
      if ((A && A.desc) || (B && B.desc)) rows.splice(1, 0, ['Category', (A && A.desc) || '—', (B && B.desc) || '—']);
      const sent = (name) => { const nt = qTerms(name); const c = prose.filter((x) => nt.every((t) => x.stems.includes(t)) && x.s.length < 280).sort((a, b) => b.score - a.score)[0]; return c ? c : null; };
      const sa = sent(cmp[0]), sb = sent(cmp[1]); if (sa || sb) rows.push(['Described on the web', sa ? clip(sa.s, 240) + ' [' + cite(pages[sa.page].title, pages[sa.page].url, 'web') + ']' : '—', sb ? clip(sb.s, 240) + ' [' + cite(pages[sb.page].title, pages[sb.page].url, 'web') + ']' : '—']);
      ans.table = { head: ['', cmp[0], cmp[1]], rows, src: [A && cite(A.title, A.url, 'wiki'), B && cite(B.title, B.url, 'wiki')].filter(Boolean) };
    }
    const both = prose.filter((x) => cmp.every((e) => qTerms(e).every((t) => x.stems.includes(t))) || (/\b(differ|unlike|whereas|while|compared|contrast|than)\b/i.test(x.s) && cmp.some((e) => qTerms(e).every((t) => x.stems.includes(t))))).sort((a, b) => b.score - a.score);
    const top = pickTop(both.length ? both : prose, qTerms(cmp.join(' ')), 4, 2, 0.4); ans.points = top.map(asPoint);
    if (!ans.table && !ans.points.length && wikis[0]) ans.headline = { text: clip(wikis[0].extract, 420), src: cite(wikis[0].title, wikis[0].url, 'wiki') };
  } else {
    if (main && type !== 'howto') { ans.headline = { text: clip(sentences(main.extract).slice(0, 3).join(' '), 440), src: cite(main.title + ' – Wikipedia', main.url, 'wiki') }; }
    if (type === 'list' || type === 'howto') {
      const run1 = bestListRun(pages, qt);
      if (run1) ans.list = { ordered: type === 'howto', items: run1.items.map((x) => clip(x, 220)), src: cite(pages[run1.pi].title, pages[run1.pi].url, 'web') };
    }
    const avoid = ans.headline ? [{ stems: tokenize(ans.headline.text).map(stem) }] : [];
    const top = pickTop(prose, qt, deep ? 8 : 4, 2, 0.5, avoid); ans.points = top.map(asPoint);
    if (!ans.headline && ans.points.length) ans.headline = ans.points.shift();
  }
  if (deep && main) { try { const sec = await wikiSections(main.title); if (sec.length) { ans.sections = sec; cite(main.title + ' – Wikipedia', main.url, 'wiki'); } } catch (e) { /* optional */ } }
  if (!ans.headline && !ans.points.length && !ans.table && !ans.list && results[0] && results[0].snippet) ans.headline = { text: clip(results[0].snippet, 300), src: cite(results[0].title, results[0].url, 'web') };

  // ---- results shown to the student ----
  const shown = results.filter((r) => !(main && canon(r.url) === canon(main.url))).slice(0, deep ? 12 : 8).map((r) => ({ title: clip(r.title, 110), url: r.url, domain: r.domain, snippet: clip(r.snippet, 260), score: +r.score.toFixed(4), via: r.from }));
  const out = {
    q, type, deep, instant: inst || null,
    answer: (ans.headline || ans.points.length || ans.table || ans.list || ans.sections) ? ans : null,
    wiki: main ? { title: main.title, desc: main.desc, extract: clip(main.extract, 700), thumb: main.thumb, url: main.url } : null,
    sources, results: shown,
    learned: kbHits.map((d) => ({ title: d.t, url: d.u, text: d.s, when: d.d })),
    related: relatedFor(q, type, wikis, main),
    googleUrl: 'https://www.google.com/search?q=' + encodeURIComponent(q),
    meta: { pagesRead: pages.length, sourcesAsked: Object.keys(status).filter((k) => status[k].ok && !/^(instant|pages|wiki:)/.test(k)).length, ms: Date.now() - t0, status: opt.debug ? status : undefined, builtAt: Date.now() }
  };
  const good = out.answer || out.instant || out.results.length;
  if (good) {
    await jset(ckey, out, type === 'current' ? 1800 : deep ? 43200 : 21600);
    try { // remember this search: popularity + a few learned documents for the knowledge base
      const cmds = [['ZINCRBY', 'q:pop', '1', q.toLowerCase().slice(0, 100)]];
      if (!deep) for (const p of pages.slice(0, 2)) { const first = (prose.filter((x) => x.page === pages.indexOf(p)).sort((a, b) => b.score - a.score)[0] || {}).s; if (first) cmds.push(['RPUSH', KB_PEND, JSON.stringify(kbEntry(p.title, p.url, first))]); }
      if (main) cmds.push(['RPUSH', KB_PEND, JSON.stringify(kbEntry(main.title + ' – Wikipedia', main.url, main.extract))]);
      cmds.push(['LTRIM', KB_PEND, '-300', '-1']);
      await kvPipe(cmds);
    } catch (e) { /* learning is best-effort */ }
  }
  return out;
}

/* ================= reader: show any web page inside the app ================= */
async function readPage(url) {
  const key = 'rd:v1:' + canon(url), c = await jget(key); if (c) return c;
  let f; try { f = await fetchPage(url, 7000, 1500000); } catch (e) { if (e && e.ctype) { const err = new Error('file'); err.kind = /pdf/.test(e.ctype) ? 'pdf' : 'file'; throw err; } throw e; }
  const p = f.plain ? plainToBlocks(f.html) : htmlToBlocks(f.html, f.url);
  if (p.blocks.length < 2) { const err = new Error('empty'); err.kind = 'empty'; throw err; }
  const out = { url: f.url, domain: domainOf(f.url), title: p.title || domainOf(f.url), site: p.site, hero: p.hero, images: p.imgs, blocks: p.blocks.slice(0, 220) };
  await jset(key, out, 3600); return out;
}

/* ================= suggestions + home feed ================= */
async function suggest(q) {
  q = String(q || '').trim().slice(0, 80); if (q.length < 2) return [];
  try { const j = await getJson('https://duckduckgo.com/ac/?kl=wt-wt&type=list&q=' + encodeURIComponent(q), 2500); const out = (Array.isArray(j) ? j.map((x) => x.phrase) : []).filter(Boolean); if (out.length) return out.slice(0, 7); } catch (e) { /* try Wikipedia */ }
  try { const j = await getJson('https://en.wikipedia.org/w/api.php?action=opensearch&format=json&origin=*&limit=7&search=' + encodeURIComponent(q), 2500); return (j[1] || []).slice(0, 7); } catch (e) { return []; }
}
async function newsHeadlines(edition) {
  const xml = await getText('https://news.google.com/rss?' + edition, 5000);
  return [...xml.matchAll(/<item>([\s\S]*?)<\/item>/g)].slice(0, 8).map((m) => {
    const b = m[1], tag = (n) => { const x = new RegExp('<' + n + '(?:\\s[^>]*)?>([\\s\\S]*?)</' + n + '>').exec(b); return x ? decodeEnt(x[1].replace(/<!\[CDATA\[|\]\]>/g, '')).trim() : ''; };
    const source = tag('source'); let title = tag('title'); if (source && title.endsWith(' - ' + source)) title = title.slice(0, -(source.length + 3));
    const t = Date.parse(tag('pubDate')); return { title, source, url: tag('link'), published: isNaN(t) ? '' : new Date(t).toISOString() };
  }).filter((x) => x.title && x.url);
}
const SKIP_TITLE = /^(Main_Page|Special:|Wikipedia:|Portal:|File:|Help:|Category:|Talk:|Template:|-)/;
async function wikiFeed() {
  const tryDay = async (off) => { const d = new Date(Date.now() - off * DAY), p = [d.getUTCFullYear(), String(d.getUTCMonth() + 1).padStart(2, '0'), String(d.getUTCDate()).padStart(2, '0')].join('/'); return getJson('https://api.wikimedia.org/feed/v1/wikipedia/en/featured/' + p, 6000); };
  let j; try { j = await tryDay(0); if (!j.mostread) j = await tryDay(1); } catch (e) { j = await tryDay(1); }
  const trending = ((j.mostread && j.mostread.articles) || []).filter((a) => !SKIP_TITLE.test(a.title)).slice(0, 12).map((a) => ({ title: String(a.normalizedtitle || a.title).replace(/_/g, ' '), desc: a.description || '', extract: a.extract || '', views: a.views || 0 }));
  const tfa = j.tfa ? { title: String(j.tfa.normalizedtitle || j.tfa.title).replace(/_/g, ' '), extract: j.tfa.extract || '', desc: j.tfa.description || '' } : null;
  const inNews = (j.news || []).slice(0, 4).map((n) => ({ text: stripTags(n.story || ''), links: (n.links || []).slice(0, 2).map((l) => String(l.normalizedtitle || l.title).replace(/_/g, ' ')) })).filter((n) => n.text);
  return { trending, tfa, inNews };
}

/* ================= daily learning ================= */
const CURRICULUM = ['Photosynthesis', 'Cell division', 'Osmosis', 'Natural selection', 'Human digestive system', 'Ecosystem', 'Genetics', 'Respiration', 'Newton\'s laws of motion', 'Electromagnetic induction', 'Ohm\'s law', 'Refraction', 'Radioactivity', 'Thermodynamics', 'Atomic structure', 'Periodic table', 'Chemical bonding', 'Acid–base reaction', 'Electrolysis', 'Organic chemistry', 'Rate of reaction', 'Quadratic equation', 'Trigonometry', 'Probability', 'Statistics', 'Calculus', 'Matrix (mathematics)', 'Pythagorean theorem', 'Logarithm', 'Demand and supply', 'Inflation', 'Gross domestic product', 'Monetary policy', 'Opportunity cost', 'Market structure', 'Accounting', 'Double-entry bookkeeping', 'Balance sheet', 'Entrepreneurship', 'Marketing', 'Management', 'History of Ghana', 'Kwame Nkrumah', 'Ashanti Empire', 'Atlantic slave trade', 'Scramble for Africa', 'Pan-Africanism', 'Cold War', 'French Revolution', 'Industrial Revolution', 'Constitution of Ghana', 'Separation of powers', 'Democracy', 'Human rights', 'United Nations', 'African Union', 'ECOWAS', 'Federalism', 'Climate change', 'Weather and climate', 'Plate tectonics', 'Rock cycle', 'Rivers and drainage', 'Population growth', 'Urbanization', 'Soil', 'Agriculture in Ghana', 'Crop rotation', 'Artificial intelligence', 'Machine learning', 'Computer network', 'Database', 'Operating system', 'Algorithm', 'Cybersecurity', 'Internet of things', 'Cloud computing', 'Programming language', 'Data structure', 'HTML', 'Python (programming language)', 'Software engineering', 'Literary devices', 'Shakespeare', 'Poetry', 'Novel', 'Drama', 'Grammar', 'Essay', 'Malaria', 'HIV/AIDS', 'Nutrition', 'Vaccination', 'First aid', 'Public health', 'Mental health', 'Sustainable Development Goals', 'Renewable energy', 'Solar power', 'Water cycle', 'Globalization', 'Research methodology', 'Scientific method', 'Critical thinking', 'Time management', 'Study skills', 'Financial literacy', 'Taxation', 'Banking', 'Stock market', 'Insurance', 'Law of contract', 'Criminal law', 'Social studies', 'Culture', 'Sociology', 'Psychology', 'Philosophy', 'Ethics', 'Logic', 'Education'];
async function learn(opts) {
  opts = opts || {}; const t0 = Date.now(), budget = opts.budgetMs || 40000, left = () => budget - (Date.now() - t0);
  const log = { added: 0, steps: {} }, entries = [];
  const step = async (name, fn) => { const s = Date.now(); try { const r = await fn(); log.steps[name] = { ok: true, ms: Date.now() - s, n: r }; } catch (e) { log.steps[name] = { ok: false, ms: Date.now() - s, err: String(e && e.message || e).slice(0, 60) }; } };
  const home = { builtAt: Date.now(), trending: [], news: [], world: [], inNews: [], tfa: null, topics: [] };

  await step('wikipedia-feed', async () => {
    const f = await wikiFeed(); home.trending = f.trending.map((x) => ({ title: x.title, desc: x.desc })); home.tfa = f.tfa; home.inNews = f.inNews;
    for (const x of f.trending) if (x.extract) entries.push(kbEntry(x.title + ' – Wikipedia', 'https://en.wikipedia.org/wiki/' + encodeURIComponent(x.title.replace(/ /g, '_')), x.extract, { cat: 'trending' }));
    if (f.tfa && f.tfa.extract) entries.push(kbEntry(f.tfa.title + ' – Wikipedia', 'https://en.wikipedia.org/wiki/' + encodeURIComponent(f.tfa.title.replace(/ /g, '_')), f.tfa.extract, { cat: 'featured' }));
    return f.trending.length;
  });
  await step('news', async () => {
    const [gh, wd] = await Promise.all([newsHeadlines('hl=en-GH&gl=GH&ceid=GH:en').catch(() => []), newsHeadlines('hl=en-US&gl=US&ceid=US:en').catch(() => [])]);
    home.news = gh; home.world = wd; for (const n of gh.concat(wd)) entries.push(kbEntry(n.title, n.url, n.title + (n.source ? ' — ' + n.source : ''), { cat: 'news' }));
    if (!gh.length && !wd.length) throw new Error('no news'); return gh.length + wd.length;
  });
  await step('curriculum', async () => { // 6 study topics a day, rotating through the whole syllabus list
    const day = Math.floor(Date.now() / DAY), picks = []; for (let i = 0; i < 6; i++) picks.push(CURRICULUM[(day * 6 + i) % CURRICULUM.length]);
    let n = 0; home.topics = picks;
    await Promise.all(picks.map(async (tpc) => { if (left() < 8000) return; try { const r = await wikiSearch(tpc, 1, 6); if (r[0]) { entries.push(kbEntry(r[0].title + ' – Wikipedia', r[0].url, r[0].extract, { cat: 'study' })); n++; if (left() > 12000) { const sec = await wikiSections(r[0].title).catch(() => []); for (const s of sec.slice(0, 4)) entries.push(kbEntry(r[0].title + ': ' + s.h, r[0].url + '#' + encodeURIComponent(s.h.replace(/ /g, '_')), s.text, { cat: 'study' })); } } } catch (e) { /* skip */ } }));
    return n;
  });
  await step('merge', async () => { const f = await kbFlushPending().catch(() => 0); const total = await kbMerge(entries); log.added = entries.length + f; return total; });
  await step('refresh-popular', async () => { // re-learn the answers students ask about most, so they are fresh tomorrow
    const top = (await kv(['ZREVRANGE', 'q:pop', '0', '7']).catch(() => [])) || []; let n = 0;
    for (const q of top) { if (left() < 14000) break; try { await search(q, { fresh: true }); n++; } catch (e) { /* skip */ } }
    return n;
  });
  await step('popular-list', async () => { const flat = (await kv(['ZREVRANGE', 'q:pop', '0', '11', 'WITHSCORES']).catch(() => [])) || []; const pop = []; for (let i = 0; i < flat.length; i += 2) if (+flat[i + 1] >= 2) pop.push(flat[i]); home.popular = pop.slice(0, 8); return pop.length; });
  home.stats = { docs: ((await kbLoad()).length), lastRun: Date.now(), added: log.added };
  await jset('learn:home', home, 3 * DAY); await jset('learn:log', { at: Date.now(), ms: Date.now() - t0, ...log }, 14 * DAY);
  return { ms: Date.now() - t0, ...log, docs: home.stats.docs };
}
// Runs the daily learning job at most once a day, triggered by the first visitor of the day (no scheduler needed).
async function maybeLearn() {
  const h = await jget('learn:home'); if (h && Date.now() - h.builtAt < 22 * 3600000) return false;
  const got = await kv(['SET', 'learn:lock', '1', 'EX', '900', 'NX']).catch(() => null); if (got !== 'OK') return false;
  await learn({ budgetMs: 40000 }); return true;
}
async function homeFeed() {
  let h = await jget('learn:home');
  if (!h || Date.now() - h.builtAt > 18 * 3600000) { // first visit before the daily job has run, or it is stale: learn now (cheap version)
    try { h = await Promise.race([learn({ budgetMs: 12000 }).then(() => jget('learn:home')), new Promise((_, rej) => setTimeout(() => rej(new Error('slow')), 11000))]) || h; } catch (e) { /* use what we have */ }
  }
  return h || { trending: [], news: [], world: [], inNews: [], topics: [], popular: [], stats: { docs: 0 } };
}

/* ================= self-test: which sources work from this server? ================= */
async function diagnose() {
  const t = async (name, fn) => { const s = Date.now(); try { const r = await fn(); return { name, ok: true, ms: Date.now() - s, n: Array.isArray(r) ? r.length : 1 }; } catch (e) { return { name, ok: false, ms: Date.now() - s, err: String(e && e.message || e).slice(0, 60) }; } };
  const q = 'what is photosynthesis';
  const checks = await Promise.all([
    t('Wikipedia search', () => wikiSearch('photosynthesis', 3)), t('Bing results', () => srcBing(q)), t('DuckDuckGo results', () => srcDdg(q)), t('Mojeek results', () => srcMojeek(q)), t('Marginalia results', () => srcMarginalia('photosynthesis')),
    t('Page reader (en.wikipedia.org)', async () => { const f = await fetchPage('https://en.wikipedia.org/wiki/Photosynthesis', 6000, 600000); const p = htmlToBlocks(f.html, f.url); if (p.blocks.length < 5) throw new Error('no text'); return p.blocks; }),
    t('Suggestions', () => suggest('photosyn')), t('News feed', () => newsHeadlines('hl=en-GH&gl=GH&ceid=GH:en')), t('Wikipedia daily feed', () => wikiFeed().then((f) => f.trending)),
    t('Weather (Open-Meteo)', () => instant('weather in Accra').then((r) => { if (!r) throw new Error('none'); return [r]; })), t('Currency rates', () => instant('100 usd to ghs').then((r) => { if (!r) throw new Error('none'); return [r]; }))
  ]);
  const idx = await kbLoad().catch(() => []); const lg = await jget('learn:log');
  return { checks, redis: REDIS, knowledgeDocs: idx.length, lastLearn: lg ? { at: lg.at, ms: lg.ms, added: lg.added, steps: lg.steps } : null, now: Date.now() };
}


/* ================= PASCO: find the answer to an exam question on the web ================= */
const STATEMENT_OPTS = /^(true|false|yes|no)$/i;
async function answerQuestion(question, options, budgetMs) {
  const t0 = Date.now(), cap = budgetMs || 9000;
  const work = async () => {
    const q = String(question || '').replace(/\s+/g, ' ').replace(/^\s*(?:q(?:uestion)?\s*)?\d{1,3}\s*[.):\-]\s*/i, '').trim().slice(0, 300);
    const core = q.replace(/[?:]+\s*$/, ''), qt = qTerms(core), query = clip(core, 150);
    const opts = Array.isArray(options) ? options.map((o) => String(o || '').slice(0, 200)).filter(Boolean).slice(0, 6) : [];
    if (!qt.length) return { answer: null, conf: 0, why: '', src: null, found: false };
    const [wiki, ...lists] = await Promise.all([wikiSearch(query, 3, 6).catch(() => []), srcBing(query).catch(() => []), srcDdg(query).catch(() => [])]);
    const results = fuse(lists.filter((l) => l && l.length).map((items, i) => ({ name: 'w' + i, w: 1, items })), qt);
    const readable = results.filter((r) => !NOREAD.test(r.domain) && !/\.(pdf|docx?|pptx?|xlsx?|zip)(\?|$)/i.test(r.url)).slice(0, 2);
    const web = (await Promise.all(readable.map(async (r) => { try { const f = await fetchPage(r.url, 3200, 600000), p = f.plain ? plainToBlocks(f.html) : htmlToBlocks(f.html, f.url); return p.blocks.length >= 2 ? { blocks: p.blocks, title: p.title || r.title, url: f.url } : null; } catch (e) { return null; } }))).filter(Boolean);
    const pages = [];
    for (const w of wiki.slice(0, 2)) pages.push({ blocks: [{ t: 'p', x: w.extract }], title: w.title + ' – Wikipedia', url: w.url });
    for (const p of web) pages.push(p);
    const snip = results.filter((r) => (r.snippet || '').length > 50).slice(0, 8);
    if (snip.length) pages.push({ blocks: snip.map((r) => ({ t: 'p', x: r.snippet })), title: snip[0].title, url: snip[0].url });
    const cands = candidates(pages, qt, ['p']); scoreCands(cands, qt, true);
    const evidence = cands.filter((x) => x.cov >= (qt.length <= 2 ? 0.99 : 0.34) || (x.hit >= 2 && x.cov >= 0.25)).sort((a, b) => b.score - a.score).slice(0, 14);
    const srcOf = (e) => { const p = pages[e.page]; return p ? { title: clip(p.title, 90), url: p.url } : null; };
    if (!evidence.length) return { answer: null, conf: 0, why: '', src: null, found: false };
    // multiple choice
    if (opts.length >= 2 && !(opts.length === 2 && opts.every((o) => STATEMENT_OPTS.test(o.trim())))) {
      const stems = opts.map((o) => qTerms(o)), common = stems[0].filter((t) => stems.every((s) => s.includes(t)));
      const dist = stems.map((s) => { const d = s.filter((t) => !common.includes(t)); return d.length ? d : s; });
      const scores = dist.map((d, i) => {
        if (!d.length) return 0; let sc = 0; const phrase = stems[i].join(' ');
        for (const e of evidence) { const set = new Set(e.stems), frac = d.filter((t) => set.has(t)).length / d.length; if (frac > 0) sc += e.score * frac * frac; if (phrase.length > 3 && e.joined.includes(phrase)) sc += e.score * 0.6; }
        return sc;
      });
      opts.forEach((o, i) => { if (/^(all|both|none)\b.*\b(above|these)\b/i.test(o)) scores[i] = Math.min(scores[i], 0.12 * Math.max(...scores)); });
      const order = scores.map((v, i) => [v, i]).sort((a, b) => b[0] - a[0]), best = order[0][0], second = order[1] ? order[1][0] : 0;
      if (best < 0.08) return { answer: null, conf: 0, why: evidence[0].s, src: srcOf(evidence[0]), found: false };
      const bi = order[0][1], d = dist[bi];
      const why = evidence.map((e) => ({ e, f: d.filter((t) => new Set(e.stems).has(t)).length / (d.length || 1) })).sort((a, b) => b.f * b.e.score - a.f * a.e.score)[0].e;
      const conf = Math.round((0.5 * Math.min(1, best / 0.6) + 0.5 * (best > 0 ? (best - second) / best : 0)) * 100) / 100;
      return { answer: bi, conf, why: clip(why.s, 320), src: srcOf(why), found: true };
    }
    // true / false
    if (opts.length === 2) {
      const top = evidence[0], yes = top.cov >= 0.85 && !/\b(not|never|no|cannot|isn't|aren't|false)\b/i.test(top.s.slice(0, 200)) ? 0 : -1;
      return { answer: yes === 0 ? opts.findIndex((o) => /^(true|yes)$/i.test(o.trim())) : null, conf: yes === 0 ? 0.45 : 0, why: clip(top.s, 320), src: srcOf(top), found: yes === 0 };
    }
    // open question: the best sentence is the answer
    const top = evidence[0];
    return { answer: null, text: clip(top.s, 360), conf: Math.round(Math.min(1, top.cov) * 100) / 100, why: clip(top.s, 320), src: srcOf(top), found: true };
  };
  try { return await Promise.race([work(), new Promise((r) => setTimeout(() => r({ answer: null, conf: 0, why: '', src: null, found: false, timeout: true }), cap))]); }
  catch (e) { return { answer: null, conf: 0, why: '', src: null, found: false, error: true }; }
}

module.exports = { answerQuestion, maybeLearn, search, readPage, suggest, homeFeed, learn, diagnose, instant, calc, classify, parseCompare, htmlToBlocks, sentences, qTerms, fuse, bingUnwrap, srcDdg, srcBing, srcMojeek, kbEntry, kbSearch, kv, REDIS, assertPublic, privateIp };

return module.exports;
})();
__M.search = (function () {
const module = { exports: {} }; const exports = module.exports;
// Study Partner search API — /api/search?a=web|read|suggest|home|diag
const E = __M.engine;
const hits = new Map();
function limited(ip, bucket, max) {
  const k = bucket + ':' + ip, now = Date.now(), h = (hits.get(k) || []).filter((t) => now - t < 600000);
  if (h.length >= max) return true; h.push(now); hits.set(k, h); if (hits.size > 8000) hits.clear(); return false;
}
const send = (res, code, obj, cache) => { res.statusCode = code; res.setHeader('X-Content-Type-Options', 'nosniff'); res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin'); res.setHeader('Content-Type', 'application/json; charset=utf-8'); res.setHeader('Cache-Control', cache || 'no-store'); res.end(JSON.stringify(obj)); };
module.exports = async (req, res, override) => {
  try {
    const q = override || req.query || Object.fromEntries(new URL(req.url, 'http://x').searchParams);
    const ip = String((req.headers && req.headers['x-forwarded-for']) || '').split(',')[0].trim() || 'x';
    const a = String(q.a || 'web');
    if (a === 'suggest') {
      if (limited(ip, 's', 150)) return send(res, 429, { items: [] });
      return send(res, 200, { items: await E.suggest(q.q) }, 'public, s-maxage=3600, stale-while-revalidate=86400');
    }
    if (a === 'home') return send(res, 200, await E.homeFeed(), 'public, s-maxage=300, stale-while-revalidate=3600');
    if (a === 'diag') {
      if (limited(ip, 'd', 6)) return send(res, 429, { error: 'Test limit reached — try again in a few minutes.' });
      return send(res, 200, await E.diagnose());
    }
    if (a === 'read') {
      if (limited(ip, 'r', 40)) return send(res, 429, { error: 'You are opening pages very fast — please wait a few minutes.', code: 'rate_limited' });
      const u = String(q.url || '').slice(0, 1000);
      if (!/^https?:\/\//i.test(u)) return send(res, 400, { error: 'That is not a web address.' });
      try { return send(res, 200, await E.readPage(u), 'public, s-maxage=600, stale-while-revalidate=3600'); }
      catch (e) { return send(res, 422, { error: 'This page cannot be shown here.', code: (e && e.kind) || 'blocked' }); }
    }
    // default: the answer engine
    if (limited(ip, 'w', 40)) return send(res, 429, { error: 'You are searching very fast — please wait a few minutes.', code: 'rate_limited' });
    const text = String(q.q || '').trim();
    if (text.length < 2) return send(res, 400, { error: 'Type something to search for.' });
    const out = await E.search(text, { deep: String(q.deep || '') === '1', debug: String(q.debug || '') === '1' });
    send(res, 200, out, out.cached ? 'public, s-maxage=600, stale-while-revalidate=3600' : 'public, s-maxage=120, stale-while-revalidate=1800');
    E.maybeLearn().catch(() => null); return;
  } catch (e) {
    console.error(e); return send(res, 500, { error: 'The search engine had a problem. Please try again.' });
  }
};

return module.exports;
})();
__M.features = (function () {
const module = { exports: {} }; const exports = module.exports;
'use strict';
// Study Partner — newer features, kept apart from the older server code:
//   reels (feed, likes, comments, who may post) · online courses (admin-built, optional certificate)
//   account data sync (courses, notes, progress — not the uploaded files) · PASCO answers found on the web
// index.js passes in its helpers and calls handle(a, body, q, t). Returns null if the action is not ours,
// otherwise { code, obj, cache } for index.js to send.
module.exports = function make(C) {
  const { kv, verify, needAdmin, needUser, bad, nameKey, cleanName, putImg, IMG_RE, crypto, rateCheck, saveUser, engine } = C;
  // Everything new lives under its own "sp2:" key names, so it can never collide with (or be broken by) other data in a shared database.
  const K = { reels: 'sp2:reels', oc: 'sp2:oc', ud: 'sp2:ud', rlu: (k) => 'sp2:rlu:' + k, rcm: (i) => 'sp2:rcm:' + i, rv: (i) => 'sp2:rv:' + i };
  async function readAll(key) { const flat = (await kv(['HGETALL', key])) || [], out = []; for (let i = 0; i + 1 < flat.length; i += 2) { try { const v = JSON.parse(flat[i + 1]); if (v && typeof v === 'object') out.push(v); } catch (e) { /* skip anything that is not ours */ } } return out; }
  async function readOne(key, field) { try { const v = await kv(['HGET', key, field]); const o = v ? JSON.parse(v) : null; return o && typeof o === 'object' ? o : null; } catch (e) { return null; } }
  const s = (v, n) => String(v == null ? '' : v).slice(0, n);
  const rid = (p) => p + Date.now().toString(36) + crypto.randomBytes(3).toString('hex');
  const ok = (obj, cache) => ({ code: 200, obj, cache });
  const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36';
  const MAXCH = 26, CH = 375000, CAP = 2600000, VTYPES = ['video/mp4', 'video/webm', 'video/quicktime'];
  const ttId = (u) => { const m = String(u || '').match(/tiktok\.com\/(?:@[\w.\-]+\/)?(?:video|embed\/v2|embed)\/(\d{8,25})/i); return m ? m[1] : ''; };
  const isTikTok = (u) => { try { return /(^|\.)tiktok\.com$/i.test(new URL(u).hostname); } catch (e) { return false; } };
  async function resolveTikTok(link) { // vm.tiktok.com / vt.tiktok.com short links: follow the redirects (TikTok hosts only) to find the video number
    let u = link;
    for (let i = 0; i < 4; i++) {
      const id = ttId(u); if (id) return id; if (!isTikTok(u)) return '';
      const r = await fetch(u, { redirect: 'manual', headers: { 'user-agent': UA }, signal: AbortSignal.timeout(4500) }), loc = r.headers.get('location');
      if (!loc) { const html = await r.text().catch(() => ''), m = /tiktok\.com\/@[\w.\-]+\/video\/(\d{8,25})/.exec(html); return m ? m[1] : ''; }
      u = new URL(loc, u).href;
    }
    return ttId(u);
  }
  const ytId = (u) => { const m = String(u || '').match(/(?:youtu\.be\/|v=|embed\/|shorts\/|live\/)([\w-]{11})/); return m ? m[1] : ''; };

  // admin or a signed-in student -> { key, name, isAdmin, u }
  async function who(t) {
    const p = verify(t); if (!p) bad(401, 'Please log in first.');
    if (p.role === 'admin') return { key: '__admin', name: 'Admin', isAdmin: true, u: null };
    const { key, u } = await needUser(t); return { key, name: u.name, isAdmin: false, u };
  }
  const maybeWho = async (t) => { try { return t ? await who(t) : null; } catch (e) { return null; } };
  const canPost = (me) => !!(me && (me.isAdmin || (me.u && me.u.canReel)));

  /* ---------------- reels ---------------- */
  const pubReel = (r, me, liked) => ({
    id: r.id, by: r.byName, byAdmin: !!r.admin, kind: r.kind, yt: r.yt || '', tt: r.tt || '', link: r.link || '', vid: r.vid ? '/api?a=reelvid&id=' + r.id : '', vtype: r.vtype || '', caption: r.caption || '', ts: r.ts,
    img: r.img ? '/api?a=img&id=reel_' + r.id + '&v=' + r.ts : '', pics: r.img && r.pics ? Array.from({ length: r.pics + 1 }, (_, i) => '/api?a=img&id=reel_' + r.id + (i ? '_' + i : '') + '&v=' + r.ts) : [], likes: r.likes || 0, comments: r.comments || 0, liked: !!liked,
    mine: !!(me && (me.isAdmin || me.key === r.by))
  });

  /* ---------------- online courses ---------------- */
  const LTYPES = ['video', 'slides', 'reading', 'quiz', 'link'];
  function cleanCourse(c, old) {
    const id = old ? old.id : rid('oc');
    const lessons = (Array.isArray(c.lessons) ? c.lessons : []).slice(0, 80).map((l) => {
      const type = LTYPES.includes(l.type) ? l.type : 'reading';
      const o = { id: /^[\w-]{3,30}$/.test(l.id || '') ? l.id : rid('l'), type, title: s(l.title, 120) || 'Untitled lesson', mins: Math.max(0, Math.min(600, +l.mins || 0)) };
      if (type === 'video' || type === 'link') o.url = s(l.url, 500);
      if (type === 'slides') { o.url = s(l.url, 500); o.pages = Math.max(0, Math.min(60, +l.pages || 0)); }
      if (type === 'reading') o.text = s(l.text, 20000);
      if (type === 'quiz') {
        o.pass = Math.max(0, Math.min(100, +l.pass || 60));
        o.questions = (Array.isArray(l.questions) ? l.questions : []).slice(0, 40).map((x) => {
          const opts = (Array.isArray(x.opts) ? x.opts : []).map((v) => s(v, 160)).filter(Boolean).slice(0, 6);
          return { q: s(x.q, 400), opts, answer: Math.max(0, Math.min(opts.length - 1, +x.answer || 0)), why: s(x.why, 300) };
        }).filter((x) => x.q && x.opts.length >= 2);
      }
      return o;
    });
    return { id, title: s(c.title, 120).trim(), desc: s(c.desc, 600), cat: s(c.cat, 60), level: s(c.level, 30), dur: s(c.dur, 40), color: /^#[0-9a-f]{6}$/i.test(c.color || '') ? c.color : '#6d28d9',
      cert: c.cert === true || c.cert === 'true', published: c.published !== false, cover: old ? !!old.cover : false, lessons, ts: Date.now(), created: old ? old.created : Date.now() };
  }
  const ocSummary = (c) => ({ id: c.id, title: c.title, desc: c.desc, cat: c.cat, level: c.level, dur: c.dur, color: c.color, cert: !!c.cert, published: c.published !== false,
    cover: c.cover ? '/api?a=img&id=occ_' + c.id + '&v=' + c.ts : '', lessons: c.lessons.length, ts: c.ts });

  /* ---------------- account data sync ---------------- */
  const NT = 'sp2:nt', NTR = (k) => 'sp2:ntr:' + k, UX = 'sp2:ux', QZ = 'sp2:qz', QZR = (i) => 'sp2:qzr:' + i;
  const SYNC_KEYS = ['sp_courses', 'sp_pasco', 'sp_progress', 'sp_ocprog', 'sp_notebooks'];
  const SYNC_MAX_KEY = 700000, SYNC_MAX_ALL = 1500000;

  async function run(a, body, q, t, req) {
    switch (a) {
      /* ===== reels ===== */

      /* ===== notifications (sent by the admin) ===== */
      case 'notifSend': {
        needAdmin(t); const title = s(body.title, 80).trim(), text = s(body.body, 600).trim(); if (!title) bad(400, 'Add a title.');
        let to = 'all', toName = 'Everyone';
        if (body.to && String(body.to).trim() && String(body.to).trim().toLowerCase() !== 'all') { const k = nameKey(cleanName(body.to)), u = await readOne('users', k); if (!u) bad(404, 'No student with that name.'); to = k; toName = u.name; }
        if ((await readAll(NT)).length >= 300) bad(400, 'Too many notifications saved — delete some old ones first.');
        const n = { id: rid('n'), ts: Date.now(), title, body: text, to, toName }; await kv(['HSET', NT, n.id, JSON.stringify(n)]); return ok({ n });
      }
      case 'notifAdminList': { needAdmin(t); return ok({ list: (await readAll(NT)).sort((a, b) => b.ts - a.ts).slice(0, 100) }); }
      case 'notifDel': { needAdmin(t); await kv(['HDEL', NT, s(body.id, 40)]); return ok({ ok: true }); }
      case 'notifList': {
        const p = verify(t); if (!p || p.role !== 'student') return ok({ list: [], unread: 0 });
        const { key, u } = await needUser(t); const since = u.createdAt || 0;
        const read = new Set(); const flat = (await kv(['HGETALL', NTR(key)])) || []; for (let i = 0; i < flat.length; i += 2) read.add(flat[i]);
        const list = (await readAll(NT)).filter((n) => (n.to === 'all' || n.to === key) && n.ts >= since).sort((a, b) => b.ts - a.ts).slice(0, 50).map((n) => ({ id: n.id, ts: n.ts, title: n.title, body: n.body, read: read.has(n.id) }));
        return ok({ list, unread: list.filter((n) => !n.read).length });
      }
      case 'notifRead': {
        const { key } = await needUser(t); const list = (await readAll(NT)).filter((n) => n.to === 'all' || n.to === key).slice(0, 100);
        for (const n of list) await kv(['HSET', NTR(key), n.id, '1']); return ok({ ok: true });
      }
      /* ===== favourites and who I follow ===== */
      case 'uxGet': {
        const p = verify(t); if (!p || p.role !== 'student') return ok({ favs: [], following: [] });
        const { key } = await needUser(t), d = (await readOne(UX, key)) || {}; return ok({ favs: d.favs || [], following: d.following || [] });
      }
      case 'uxSet': {
        const { key } = await needUser(t);
        const favs = (Array.isArray(body.favs) ? body.favs : []).slice(0, 150).map((f) => ({ id: s(f.id, 40), t: f.t === 'course' ? 'course' : 'reel', title: s(f.title, 120), by: s(f.by, 40), kind: s(f.kind, 10), img: s(f.img, 200), yt: s(f.yt, 20), tt: s(f.tt, 30), link: s(f.link, 300), vid: s(f.vid, 120), vtype: s(f.vtype, 30), ts: +f.ts || 0 })).filter((f) => f.id);
        const following = [...new Set((Array.isArray(body.following) ? body.following : []).map((n) => s(n, 40)).filter(Boolean))].slice(0, 300);
        await kv(['HSET', UX, key, JSON.stringify({ favs, following })]); return ok({ ok: true });
      }
      case 'myReels': {
        const me = await who(t); const mine = (await readAll(K.reels)).filter((r) => r.by === me.key).sort((x, y) => y.ts - x.ts).slice(0, 60);
        return ok({ reels: mine.map((r) => pubReel(r, me, false)), can: canPost(me) });
      }
      /* ===== online quizzes posted by the admin (some give a gift) ===== */
      case 'qzSave': {
        needAdmin(t); const old = body.id ? await readOne(QZ, s(body.id, 40)) : null;
        const qs = (Array.isArray(body.questions) ? body.questions : []).slice(0, 60).map((x) => { const opts = (Array.isArray(x.opts) ? x.opts : []).map((v) => s(v, 160)).filter(Boolean).slice(0, 5); return { q: s(x.q, 400), opts, answer: Math.max(0, Math.min(opts.length - 1, +x.answer || 0)) }; }).filter((x) => x.q && x.opts.length >= 2);
        const title = s(body.title, 100).trim(); if (!title) bad(400, 'Add a title.'); if (!qs.length) bad(400, 'Add at least one question with an Answer line.');
        const rec = { id: old ? old.id : rid('z'), title, desc: s(body.desc, 300), gift: s(body.gift, 120).trim(), pass: Math.max(1, Math.min(100, +body.pass || 50)), qs, ts: old ? old.ts : Date.now(), open: body.open !== false };
        await kv(['HSET', QZ, rec.id, JSON.stringify(rec)]); return ok({ ok: true, id: rec.id });
      }
      case 'qzDelete': { needAdmin(t); const id = s(body.id, 40); await kv(['HDEL', QZ, id]); await kv(['DEL', QZR(id)]); return ok({ ok: true }); }
      case 'qzAdminList': {
        needAdmin(t); const list = []; for (const q of (await readAll(QZ)).sort((a, b) => b.ts - a.ts)) { const res = await readAll(QZR(q.id)); list.push({ id: q.id, title: q.title, desc: q.desc, gift: q.gift, pass: q.pass, open: q.open !== false, n: q.qs.length, tries: res.length, winners: res.filter((r) => r.passed).sort((a, b) => a.ts - b.ts).map((r) => ({ name: r.name, score: r.score, total: r.total, ts: r.ts })) }); }
        return ok({ list });
      }
      case 'qzList': {
        const me = await maybeWho(t); const out = [];
        for (const q of (await readAll(QZ)).filter((x) => x.open !== false).sort((a, b) => b.ts - a.ts)) { const mine = me && !me.isAdmin ? await readOne(QZR(q.id), me.key) : null; out.push({ id: q.id, title: q.title, desc: q.desc, gift: q.gift, pass: q.pass, n: q.qs.length, ts: q.ts, mine: mine ? { score: mine.score, total: mine.total, passed: !!mine.passed } : null }); }
        return ok({ list: out });
      }
      case 'qzGet': {
        const q = await readOne(QZ, s(body.id, 40)); if (!q || q.open === false) bad(404, 'That quiz is not available.');
        return ok({ quiz: { id: q.id, title: q.title, desc: q.desc, gift: q.gift, pass: q.pass, qs: q.qs.map((x) => ({ q: x.q, opts: x.opts })) } });
      }
      case 'qzSubmit': {
        const me = await who(t); if (me.isAdmin) bad(400, 'Admins can preview quizzes but not submit them.');
        const q = await readOne(QZ, s(body.id, 40)); if (!q || q.open === false) bad(404, 'That quiz is not available.');
        if (await rateCheck('rl:qz:' + me.key, 60, 3600)) bad(429, 'Too many tries. Please wait a while.');
        const ans = Array.isArray(body.answers) ? body.answers : []; let score = 0; const key = q.qs.map((x, i) => { if (+ans[i] === x.answer) score++; return x.answer; });
        const total = q.qs.length, passed = Math.round(score / total * 100) >= q.pass, prev = await readOne(QZR(q.id), me.key);
        if (!prev || score > prev.score || (passed && !prev.passed)) await kv(['HSET', QZR(q.id), me.key, JSON.stringify({ name: me.name, score, total, passed: passed || !!(prev && prev.passed), ts: prev && prev.passed ? prev.ts : Date.now() })]);
        return ok({ score, total, passed, gift: passed ? q.gift : '', answers: key });
      }
      case 'reelsList': {
        const me = await maybeWho(t), before = +body.before || Infinity;
        const all = (await readAll(K.reels)).sort((x, y) => y.ts - x.ts).filter((r) => r.ts < before);
        const page = all.slice(0, Math.max(1, Math.min(+body.limit || 12, 30)));
        if (body.only) { const one = await readOne(K.reels, s(body.only, 40)); if (one && !page.some((r) => r.id === one.id)) page.unshift(one); }
        const liked = new Set();
        if (me) { const flat = (await kv(['HGETALL', K.rlu(me.key)])) || []; for (let i = 0; i < flat.length; i += 2) liked.add(flat[i]); }
        return ok({ reels: page.map((r) => pubReel(r, me, liked.has(r.id))), more: all.length > page.length, can: canPost(me), me: me ? { name: me.name, admin: me.isAdmin } : null });
      }
      case 'reelVidChunk': { // a short video from the phone, sent in pieces
        const me = await who(t); if (!canPost(me)) bad(403, 'You have not been given permission to post reels. Ask the admin.');
        if (await rateCheck('rl:rvc:' + me.key, 300, 3600)) bad(429, 'Too many uploads. Please try again later.');
        const id = s(body.id, 40), n = Math.floor(+body.n), d = body.data;
        if (!/^r[a-z0-9]{8,30}$/.test(id)) bad(400, 'Invalid upload.'); if (!(n >= 0 && n < MAXCH)) bad(413, 'That video is still too large after shrinking — choose a shorter part, or paste a YouTube or TikTok link.');
        if (d && d.length !== 500000 && !body.last) bad(400, 'That video piece has the wrong size.');
        if (typeof d !== 'string' || d.length > 520000 || !/^[A-Za-z0-9+/=]+$/.test(d)) bad(400, 'That video piece could not be read.');
        if (await readOne(K.reels, id)) bad(409, 'That reel is already posted.');
        if (n === 0) { await kv(['DEL', K.rv(id)]); await kv(['HSET', K.rv(id), 'meta', JSON.stringify({ by: me.key })]); }
        else { const m = await readOne(K.rv(id), 'meta'); if (!m || m.by !== me.key) bad(403, 'Upload not found — start again.'); }
        await kv(['HSET', K.rv(id), String(n), d]); await kv(['EXPIRE', K.rv(id), '7200']);
        return ok({ ok: true });
      }
      case 'reelPost': {
        const me = await who(t);
        if (!canPost(me)) bad(403, 'You have not been given permission to post reels. Ask the admin.');
        if (await rateCheck('rl:reel:' + me.key, 40, 3600)) bad(429, 'You are posting too fast. Please try again later.');
        const caption = s(body.caption, 500).trim(), link = s(body.link, 500).trim();
        const img = typeof body.img === 'string' && IMG_RE.test(body.img) && body.img.length <= 700000 ? body.img : '';
        const vid = body.vid && typeof body.vid === 'object' ? body.vid : null;
        let vsize = 0, yt = '', tt = '', direct = '', id = rid('r').toLowerCase().replace(/[^a-z0-9]/g, ''), vn = 0, vtype = '';
        if (vid) {
          id = s(vid.id, 40); vn = Math.floor(+vid.n); vtype = VTYPES.includes(vid.type) ? vid.type : 'video/mp4';
          if (!/^r[a-z0-9]{8,30}$/.test(id) || !(vn >= 1 && vn <= MAXCH)) bad(400, 'Invalid video.');
          const m = await readOne(K.rv(id), 'meta'), first = await kv(['HGET', K.rv(id), '0']), last = await kv(['HGET', K.rv(id), String(vn - 1)]);
          if (!m || m.by !== me.key || !first || !last) bad(400, 'The video did not finish uploading — please try again.');
          await kv(['EXPIRE', K.rv(id), '315360000']);
          vsize = (vn - 1) * CH + Math.floor(String(last).length * 3 / 4) - (String(last).endsWith('==') ? 2 : String(last).endsWith('=') ? 1 : 0);
        } else if (!img) {
          yt = ytId(link);
          if (!yt && isTikTok(link)) { tt = ttId(link); if (!tt) { try { tt = await resolveTikTok(link); } catch (e) { tt = ''; } } if (!tt) bad(400, 'Could not read that TikTok link. Open the video, tap Share → Copy link, and paste it here.'); }
          if (!yt && !tt) direct = /^https?:\/\/\S+\.(mp4|webm)(\?\S*)?$/i.test(link) ? link : '';
          if (!yt && !tt && !direct) bad(400, 'Add a picture or a short video, or paste a YouTube, TikTok or .mp4 link.');
        }
        if (await readOne(K.reels, id)) bad(409, 'That reel is already posted.');
        const rec = { id, by: me.key, byName: me.name, admin: me.isAdmin, kind: img ? 'image' : 'video', img: !!img, yt, tt, link: direct, vid: vid ? vn : 0, vsize, vtype, caption, ts: Date.now(), likes: 0, comments: 0 };
        if (img) await putImg('reel_' + id, img);
        const extra = img && Array.isArray(body.more) ? body.more.filter((x) => typeof x === 'string' && IMG_RE.test(x) && x.length <= 700000).slice(0, 9) : [];
        for (let i = 0; i < extra.length; i++) await putImg('reel_' + id + '_' + (i + 1), extra[i]);
        rec.pics = extra.length;
        await kv(['HSET', K.reels, id, JSON.stringify(rec)]);
        return ok({ reel: pubReel(rec, me, false) });
      }
      case 'reelvid': { // streams an uploaded video in slices (what phones and browsers ask for with Range requests)
        const id = s(q.id || body.id, 40); if (!/^r[a-z0-9]{8,30}$/.test(id)) bad(404, 'Not found.');
        const rec = await readOne(K.reels, id); if (!rec || !rec.vid) bad(404, 'Not found.');
        let size = rec.vsize;
        if (!size) { const last = await kv(['HGET', K.rv(id), String(rec.vid - 1)]) || ''; size = (rec.vid - 1) * CH + Math.floor(last.length * 3 / 4) - (last.endsWith('==') ? 2 : last.endsWith('=') ? 1 : 0); }
        const rg = /bytes=(\d*)-(\d*)/.exec((req && req.headers && req.headers.range) || '');
        let a0 = 0, b0 = size - 1;
        if (rg && (rg[1] || rg[2])) { if (rg[1]) { a0 = +rg[1]; if (rg[2]) b0 = Math.min(+rg[2], size - 1); } else { a0 = Math.max(0, size - +rg[2]); } }
        if (a0 > b0 || a0 >= size) return { raw: true, code: 416, headers: { 'Content-Range': 'bytes */' + size }, buf: Buffer.alloc(0) };
        const partial = !!rg || size > CAP; if (b0 - a0 + 1 > CAP) b0 = a0 + CAP - 1;
        const from = Math.floor(a0 / CH), to = Math.floor(b0 / CH), idx = []; for (let i = from; i <= to; i++) idx.push(i);
        const pieces = await Promise.all(idx.map(async (i) => Buffer.from((await kv(['HGET', K.rv(id), String(i)])) || '', 'base64')));
        const buf = Buffer.concat(pieces).subarray(a0 - from * CH, a0 - from * CH + (b0 - a0 + 1));
        const h = { 'Content-Type': rec.vtype || 'video/mp4', 'Accept-Ranges': 'bytes', 'Cache-Control': 'public, max-age=31536000, immutable', 'Content-Length': String(buf.length) };
        if (partial) return { raw: true, code: 206, headers: { ...h, 'Content-Range': 'bytes ' + a0 + '-' + (a0 + buf.length - 1) + '/' + size }, buf };
        return { raw: true, code: 200, headers: h, buf };
      }
      case 'reelDelete': {
        const me = await who(t), id = s(body.id, 40), r = await readOne(K.reels, id); if (!r) bad(404, 'That reel is already gone.');
        if (!me.isAdmin && r.by !== me.key) bad(403, 'You can only delete your own reels.');
        await kv(['HDEL', K.reels, id]); await kv(['HDEL', 'imgs', 'reel_' + id]); for (let i = 1; i <= (r.pics || 0); i++) await kv(['HDEL', 'imgs', 'reel_' + id + '_' + i]); await kv(['DEL', K.rv(id)]); await kv(['DEL', K.rcm(id)]);
        return ok({ ok: true });
      }
      case 'reelLike': {
        const me = await who(t), id = s(body.id, 40), r = await readOne(K.reels, id); if (!r) bad(404, 'That reel is gone.');
        if (await rateCheck('rl:like:' + me.key, 240, 600)) bad(429, 'Slow down a little.');
        const had = await kv(['HGET', K.rlu(me.key), id]);
        if (had) { await kv(['HDEL', K.rlu(me.key), id]); r.likes = Math.max(0, (r.likes || 0) - 1); } else { await kv(['HSET', K.rlu(me.key), id, '1']); r.likes = (r.likes || 0) + 1; }
        await kv(['HSET', K.reels, id, JSON.stringify(r)]);
        return ok({ liked: !had, likes: r.likes });
      }
      case 'reelComments': {
        const me = await maybeWho(t), id = s(body.id, 40);
        const list = (await readAll(K.rcm(id))).sort((x, y) => x.ts - y.ts).slice(-80);
        return ok({ comments: list.map((c) => ({ id: c.id, name: c.name, admin: !!c.admin, text: c.text, ts: c.ts, mine: !!(me && (me.isAdmin || me.key === c.by)) })) });
      }
      case 'reelComment': {
        const me = await who(t), id = s(body.id, 40), text = s(body.text, 300).trim(); if (!text) bad(400, 'Write something first.');
        const r = await readOne(K.reels, id); if (!r) bad(404, 'That reel is gone.');
        if (await rateCheck('rl:cm:' + me.key, 60, 3600)) bad(429, 'You are commenting too fast. Please wait a bit.');
        const c = { id: rid('c'), by: me.key, name: me.name, admin: me.isAdmin, text, ts: Date.now() };
        await kv(['HSET', K.rcm(id), c.id, JSON.stringify(c)]);
        r.comments = (r.comments || 0) + 1; await kv(['HSET', K.reels, id, JSON.stringify(r)]);
        return ok({ comment: { id: c.id, name: c.name, admin: c.admin, text: c.text, ts: c.ts, mine: true }, comments: r.comments });
      }
      case 'reelCommentDel': {
        const me = await who(t), id = s(body.id, 40), cid = s(body.cid, 40), c = await readOne(K.rcm(id), cid); if (!c) bad(404, 'That comment is already gone.');
        if (!me.isAdmin && c.by !== me.key) bad(403, 'You can only delete your own comments.');
        await kv(['HDEL', K.rcm(id), cid]);
        const r = await readOne(K.reels, id); if (r) { r.comments = Math.max(0, (r.comments || 0) - 1); await kv(['HSET', K.reels, id, JSON.stringify(r)]); }
        return ok({ ok: true });
      }
      case 'userReel': { // admin switches a student's permission to post reels on or off
        needAdmin(t); const key = nameKey(cleanName(body.name)), u = await readOne('users', key); if (!u) bad(404, 'Account not found.');
        u.canReel = !!body.allow; await saveUser(key, u); return ok({ ok: true, canReel: u.canReel });
      }

      /* ===== online courses ===== */
      case 'ocList': {
        const admin = (() => { const p = verify(t); return !!(p && p.role === 'admin'); })();
        const list = (await readAll(K.oc)).filter((c) => admin || c.published !== false).sort((x, y) => (y.created || 0) - (x.created || 0));
        return ok({ courses: list.map(ocSummary) }, admin ? undefined : 'public, s-maxage=15, stale-while-revalidate=60');
      }
      case 'ocGet': {
        const c = await readOne(K.oc, s(body.id || q.id, 40)); const p = verify(t), admin = !!(p && p.role === 'admin');
        if (!c || (c.published === false && !admin)) bad(404, 'Course not found.');
        return ok({ course: { ...ocSummary(c), lessons: c.lessons.map((l) => ({ ...l, pageUrls: l.type === 'slides' && l.pages ? Array.from({ length: l.pages }, (_, i) => '/api?a=img&id=ocp_' + c.id + '_' + l.id + '_' + i + '&v=' + c.ts) : undefined })) } });
      }
      case 'ocSave': {
        needAdmin(t); const inc = body.course || {}, old = inc.id ? await readOne(K.oc, s(inc.id, 40)) : null;
        const c = cleanCourse(inc, old); if (!c.title) bad(400, 'Give the course a title.');
        const json = JSON.stringify(c); if (json.length > 400000) bad(413, 'This course has too much text — split it into two courses.');
        await kv(['HSET', K.oc, c.id, json]); return ok({ course: ocSummary(c), id: c.id, lessons: c.lessons.map((l) => l.id) });
      }
      case 'ocPage': { // one slide picture (page of a PDF, or an image the admin chose)
        needAdmin(t); const cid = s(body.courseId, 40), lid = s(body.lessonId, 40), i = Math.floor(+body.index);
        if (!/^[\w-]+$/.test(cid) || !/^[\w-]+$/.test(lid) || !(i >= 0 && i < 60)) bad(400, 'Invalid slide.');
        if (typeof body.dataUrl !== 'string' || !IMG_RE.test(body.dataUrl) || body.dataUrl.length > 800000) bad(400, 'That slide picture is too large — try a smaller file.');
        const c = await readOne(K.oc, cid); if (!c) bad(404, 'Save the course first.');
        await putImg('ocp_' + cid + '_' + lid + '_' + i, body.dataUrl);
        const l = c.lessons.find((x) => x.id === lid); if (l && l.type === 'slides') { l.pages = Math.max(l.pages || 0, i + 1); c.ts = Date.now(); await kv(['HSET', K.oc, c.id, JSON.stringify(c)]); }
        return ok({ ok: true });
      }
      case 'ocCover': {
        needAdmin(t); const cid = s(body.courseId, 40), c = await readOne(K.oc, cid); if (!c) bad(404, 'Save the course first.');
        if (typeof body.dataUrl !== 'string' || !IMG_RE.test(body.dataUrl) || body.dataUrl.length > 500000) bad(400, 'That picture is too large.');
        await putImg('occ_' + cid, body.dataUrl); c.cover = true; c.ts = Date.now(); await kv(['HSET', K.oc, cid, JSON.stringify(c)]); return ok({ ok: true });
      }
      case 'ocDelete': {
        needAdmin(t); const cid = s(body.id, 40), c = await readOne(K.oc, cid);
        if (c) { for (const l of c.lessons) if (l.type === 'slides') for (let i = 0; i < (l.pages || 0); i++) await kv(['HDEL', 'imgs', 'ocp_' + cid + '_' + l.id + '_' + i]); await kv(['HDEL', 'imgs', 'occ_' + cid]); }
        await kv(['HDEL', K.oc, cid]); return ok({ ok: true });
      }

      /* ===== account data sync (everything except uploaded files) ===== */
      case 'udGet': {
        const { key } = await needUser(t); const raw = await readOne(K.ud, key); return ok({ data: raw || {}, now: Date.now() });
      }
      case 'udPut': {
        const { key } = await needUser(t), k = String(body.key || '');
        if (!SYNC_KEYS.includes(k)) bad(400, 'Unknown data.');
        const val = JSON.stringify(body.value === undefined ? null : body.value); if (val.length > SYNC_MAX_KEY) bad(413, 'That is too large to sync.');
        const cur = (await readOne(K.ud, key)) || {}; cur[k] = { ts: Date.now(), v: body.value };
        const total = JSON.stringify(cur).length; if (total > SYNC_MAX_ALL) bad(413, 'Your synced data is too large.');
        await kv(['HSET', K.ud, key, JSON.stringify(cur)]); return ok({ ts: cur[k].ts });
      }

      /* ===== PASCO: answers found on the web ===== */
      case 'pqAnswer': {
        const p = verify(t); if (!p) bad(401, 'Please log in to find answers.');
        const items = (Array.isArray(body.items) ? body.items : []).slice(0, 3);
        if (!items.length) bad(400, 'No questions.');
        if (await rateCheck('rl:pq:' + (p.u || 'admin'), 240, 600)) bad(429, 'You have asked for many answers just now. Please wait a few minutes.');
        const out = await Promise.all(items.map((it) => engine.answerQuestion(it.q, it.opts, 7000)));
        return ok({ answers: out });
      }
      default: return null;
    }
  }
  // Any unexpected problem is reported with its reason (instead of a vague "something went wrong"), so it can be fixed quickly.
  return async function handle(a, body, q, t, req) {
    try { return await run(a, body, q, t, req); }
    catch (e) { if (e && typeof e.code === 'number') throw e; console.error('feature error', a, e); return { code: 500, obj: { error: 'Could not complete that (' + String((e && e.message) || e).slice(0, 90) + '). Please try again.' } }; }
  };
};

return module.exports;
})();

// Study Partner backend — one Vercel serverless function, no dependencies.
// Storage: Upstash Redis (add it free from Vercel → Storage / Marketplace).
const crypto = require('crypto');

// Finds the Upstash/Vercel KV settings under any name (Vercel lets you choose a custom prefix when connecting).
function findRedisEnv() {
  const e = process.env, names = Object.keys(e);
  let url = e.KV_REST_API_URL || e.UPSTASH_REDIS_REST_URL, token = e.KV_REST_API_TOKEN || e.UPSTASH_REDIS_REST_TOKEN;
  if (!url || !token) { // a custom prefix chosen when connecting (e.g. STORAGE_KV_REST_API_URL)
    for (const k of names) {
      const m = /^(.*?)(?:KV_)?(?:REST_API_URL|REDIS_REST_URL)$/.exec(k); if (!m || !/^https:\/\//.test(e[k] || '') || /READ_ONLY/.test(k)) continue;
      const t = [m[1] + 'KV_REST_API_TOKEN', m[1] + 'REST_API_TOKEN', m[1] + 'REDIS_REST_TOKEN', m[1] + 'KV_REST_TOKEN'].find((n) => e[n]);
      if (t) { url = e[k]; token = e[t]; break; }
    }
  }
  if (!url || !token) { // recognise the pair by what it looks like
    const u = names.find((k) => /^https:\/\/[\w.-]+\.upstash\.io\/?$/i.test(e[k] || ''));
    const tk = names.find((k) => /TOKEN$/i.test(k) && !/READ_?ONLY|_RO_/i.test(k) && /REST|KV|REDIS|UPSTASH|STORAGE/i.test(k) && e[k]);
    if (u && tk) { url = e[u]; token = e[tk]; }
  }
  if (!url || !token) { // only the redis:// address is present: Upstash's REST token is that address's password
    for (const k of names.filter((n) => /(KV_URL|REDIS_URL|REDIS_TLS_URL)$/.test(n))) {
      const m = /^rediss?:\/\/[^:@\/]*:([^@]+)@([^:\/]+)/.exec(e[k] || '');
      if (m && /upstash\.io$/i.test(m[2])) { url = 'https://' + m[2]; token = decodeURIComponent(m[1]); break; }
    }
  }
  return { url: url ? String(url).replace(/\/+$/, '') : url, token };
}
const { url: KV_URL, token: KV_TOKEN } = findRedisEnv();
const ADMIN_USER = process.env.ADMIN_USER || 'Yo Lord!';
const ADMIN_PASS = process.env.ADMIN_PASS || '#124#';
const SECRET = process.env.AUTH_SECRET || crypto.createHash('sha256').update('sp-secret|' + (KV_TOKEN || 'dev')).digest('hex');
const REDIS = !!(KV_URL && KV_TOKEN);
const READY = REDIS || process.env.SP_ALLOW_MEMORY === '1'; // memory mode is for local testing only

let FEAT = null; // newer features live in _features.js
/* ---------- storage ---------- */
const mem = new Map();
async function kv(cmd) {
  if (REDIS) {
    const r = await fetch(KV_URL, {
      method: 'POST',
      headers: { Authorization: 'Bearer ' + KV_TOKEN, 'Content-Type': 'application/json' },
      body: JSON.stringify(cmd),
    });
    const j = await r.json();
    if (j.error) throw new Error(j.error);
    return j.result;
  }
  const [op, key, a, b] = cmd;
  switch (String(op).toUpperCase()) {
    case 'GET': return mem.has(key) ? mem.get(key) : null;
    case 'SET': mem.set(key, a); return 'OK';
    case 'HGET': { const m = mem.get(key); return m && m.has(a) ? m.get(a) : null; }
    case 'HSET': { let m = mem.get(key); if (!m) { m = new Map(); mem.set(key, m); } m.set(a, b); return 1; }
    case 'HDEL': { const m = mem.get(key); return m && m.delete(a) ? 1 : 0; }
    case 'HGETALL': { const m = mem.get(key), out = []; if (m) for (const [k, v] of m) out.push(k, v); return out; }
    case 'INCR': { const n = (+mem.get(key) || 0) + 1; mem.set(key, String(n)); return n; }
    case 'EXPIRE': return 1;
    case 'DEL': return mem.delete(key) ? 1 : 0;
    case 'HSTRLEN': { const m = mem.get(key); const v = m && m.get(a); return v == null ? 0 : String(v).length; }
    default: throw new Error('unsupported');
  }
}
const jget = async (key, f) => { const v = f === undefined ? await kv(['GET', key]) : await kv(['HGET', key, f]); return v ? JSON.parse(v) : null; };
async function hall(key) {
  const flat = (await kv(['HGETALL', key])) || [], out = [];
  for (let i = 0; i < flat.length; i += 2) out.push(JSON.parse(flat[i + 1]));
  return out;
}

/* ---------- crypto ---------- */
const hashPw = (pw, salt) => crypto.scryptSync(pw, salt, 32).toString('hex');
const safeEq = (a, b) => { const x = Buffer.from(String(a)), y = Buffer.from(String(b)); return x.length === y.length && crypto.timingSafeEqual(x, y); };
function sign(p) {
  const body = Buffer.from(JSON.stringify(p)).toString('base64url');
  return body + '.' + crypto.createHmac('sha256', SECRET).update(body).digest('base64url');
}
function verify(t) {
  if (typeof t !== 'string') return null;
  const [body, sig] = t.split('.');
  if (!body || !sig) return null;
  const good = crypto.createHmac('sha256', SECRET).update(body).digest('base64url');
  if (!safeEq(sig, good)) return null;
  try { const p = JSON.parse(Buffer.from(body, 'base64url').toString()); return p.exp > Date.now() ? p : null; } catch { return null; }
}
const DAY = 86400000;
const userToken = (key) => sign({ u: key, role: 'student', exp: Date.now() + 30 * DAY });
const adminToken = () => sign({ u: '@admin', role: 'admin', exp: Date.now() + DAY / 2 });

/* ---------- helpers ---------- */
const send = (res, code, obj, cache) => { res.statusCode = code; res.setHeader('X-Content-Type-Options', 'nosniff'); res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin'); res.setHeader('Content-Type', 'application/json'); res.setHeader('Cache-Control', cache || 'no-store'); res.end(JSON.stringify(obj)); };
const cleanName = (n) => String(n || '').replace(/\s+/g, ' ').trim();
const nameKey = (n) => n.toLowerCase();
const pubUser = (u) => ({ name: u.name, createdAt: u.createdAt, lastLogin: u.lastLogin || null, active: u.active !== false, canReel: !!u.canReel, timetable: u.timetable || {}, profile: u.profile || {} });
const IMG_RE = /^data:image\/(jpeg|png|webp);base64,[A-Za-z0-9+\/=]+$/;
async function fullUser(key, u) {
  const o = pubUser(u);
  if (!o.profile.avatar) { const av = await kv(['HGET', 'avatars', key]); if (av) o.profile = { ...o.profile, avatar: av }; }
  return o;
}
async function migrateAvatar(key, u) { // older accounts kept the picture inside the user record
  if (u.profile && u.profile.avatar) { await kv(['HSET', 'avatars', key, u.profile.avatar]); u.profile = { ...u.profile, avatar: '' }; }
}
const listUser = (u) => ({ name: u.name, createdAt: u.createdAt, lastLogin: u.lastLogin || null, active: u.active !== false, canReel: !!u.canReel });
class HttpError extends Error { constructor(code, msg) { super(msg); this.code = code; } }
const bad = (code, msg) => { throw new HttpError(code, msg); };
const MAX = 950000;

const clientIp = (req) => String((req.headers && req.headers['x-forwarded-for']) || '').split(',')[0].trim() || 'x';
async function rateCheck(key, limit, ttl) {
  const n = await kv(['INCR', key]);
  if (n === 1) await kv(['EXPIRE', key, ttl || 600]);
  return n > limit;
}
const bump = (key) => rateCheck(key, 1e12, 600);
const adImgUrl = (x) => (x.hasImg || x.img) ? '/api?a=img&id=ad_' + x.id + '&v=' + (x.createdAt || 0) : '';
async function putImg(id, dataUrl) { await kv(['HSET', 'imgs', id, JSON.stringify({ d: dataUrl, v: Date.now() })]); }
const DAYKEYS = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'];
async function dayImageUrls() {
  const meta = (await jget('content:dayImages')) || {}; let changed = false;
  for (const d of Object.keys(meta)) { // older versions saved the pictures inline — move them out once
    if (typeof meta[d] === 'string') { if (DAYKEYS.includes(d) && IMG_RE.test(meta[d])) { await putImg('day_' + d, meta[d]); meta[d] = { v: Date.now() }; } else delete meta[d]; changed = true; }
  }
  if (changed) await kv(['SET', 'content:dayImages', JSON.stringify(meta)]);
  const out = {};
  for (const d of DAYKEYS) if (meta[d]) out[d] = '/api?a=img&id=day_' + d + '&v=' + meta[d].v;
  return out;
}
function needAdmin(t) { const p = verify(t); if (!p || p.role !== 'admin') bad(401, 'Admin sign-in required.'); }
const PLAN_MS = { '24 hours': 86400000, '48 hours': 172800000, '3 days': 259200000, '1 week': 604800000, '1 month': 2592000000 };
function planMs(plan) { return PLAN_MS[plan] || 86400000; }
async function sweepAds(list) {
  const now = Date.now(), out = [];
  for (const ad of list) {
    if (ad.status === 'live' && ad.expiresAt && ad.expiresAt <= now) {
      ad.status = 'ended'; ad.endedAt = now; ad.endedBy = 'expired';
      await kv(['HSET', 'ads', ad.id, JSON.stringify(ad)]);
    }
    out.push(ad);
  }
  return out;
}
async function adminCreds() {
  const o = await jget('admin:creds');
  return o && o.name && o.salt && o.hash ? o : { name: ADMIN_USER, salt: null, hash: null };
}
async function adminName() { return (await adminCreds()).name; }
async function checkAdminLogin(name, pass) {
  const c = await adminCreds();
  if (c.salt) return safeEq(nameKey(name), nameKey(c.name)) && safeEq(hashPw(String(pass), c.salt), c.hash);
  return safeEq(nameKey(name), nameKey(ADMIN_USER)) && safeEq(name, ADMIN_USER) && safeEq(pass, ADMIN_PASS);
}
async function needUser(t) {
  const p = verify(t);
  if (!p || p.role !== 'student') bad(401, 'Please log in again.');
  const u = await jget('users', p.u);
  if (!u) bad(401, 'Please log in again.');
  if (u.active === false) bad(403, 'This account has been deactivated. Please contact the admin.');
  return { key: p.u, u };
}
const saveUser = (key, u) => kv(['HSET', 'users', key, JSON.stringify(u)]);

async function createUser(name, pass) {
  name = cleanName(name);
  if (name.length < 2 || name.length > 40) bad(400, 'Name must be 2–40 characters.');
  if (String(pass || '').length < 6 || String(pass).length > 100) bad(400, 'Password must be at least 6 characters.');
  const key = nameKey(name);
  if (key === nameKey(await adminName()) || (await kv(['HGET', 'users', key]))) bad(409, 'That name is already taken. Tap “Log in” if it’s yours, or add your surname to make it unique.');
  const salt = crypto.randomBytes(16).toString('hex');
  const u = { name, salt, hash: hashPw(String(pass), salt), createdAt: Date.now(), lastLogin: Date.now(), active: true, timetable: {} };
  await saveUser(key, u);
  return { key, u };
}



class ApiError extends Error { constructor(code, msg) { super(msg); this.apiCode = code; } }

async function timedFetch(url, opts, ms) {
  const ac = new AbortController(), timer = setTimeout(() => ac.abort(), Math.max(1500, ms));
  try { return await fetch(url, { ...opts, signal: ac.signal }); }
  catch (e) { throw new ApiError(e && e.name === 'AbortError' ? 'timeout' : 'network', 'The AI service did not respond.'); }
  finally { clearTimeout(timer); }
}






/* ---------- YouTube search for the in-app Search page ----------
   Optional: YOUTUBE_API_KEY (free, Google Cloud -> YouTube Data API v3). Without it, a best-effort
   fallback reads YouTube's public results page. Results are cached so a crowd shares quota. */
const YT_KEY = process.env.YOUTUBE_API_KEY || '';
const ytHits = new Map();
const decodeEnt = (t) => String(t || '').replace(/&amp;/g, '&').replace(/&#39;/g, "'").replace(/&quot;/g, '"').replace(/&lt;/g, '<').replace(/&gt;/g, '>');
const ytFmtDur = (iso) => { const m = /^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$/.exec(iso || ''); if (!m) return ''; const h = +m[1] || 0, mi = +m[2] || 0, se = +m[3] || 0; return (h ? h + ':' + String(mi).padStart(2, '0') : String(mi)) + ':' + String(se).padStart(2, '0'); };
const ytFmtViews = (n) => { n = +n; if (!n) return ''; return (n >= 1e6 ? (n / 1e6).toFixed(1).replace(/\.0$/, '') + 'M' : n >= 1e3 ? (n / 1e3).toFixed(n >= 1e4 ? 0 : 1).replace(/\.0$/, '') + 'K' : String(n)) + ' views'; };
async function ytApi(q, ms) {
  const r = await timedFetch('https://www.googleapis.com/youtube/v3/search?part=snippet&type=video&maxResults=12&videoEmbeddable=true&safeSearch=moderate&q=' + encodeURIComponent(q) + '&key=' + encodeURIComponent(YT_KEY), {}, ms);
  if (!r.ok) throw new ApiError(r.status === 403 || r.status === 429 ? 'quota' : 'yt_error', 'YouTube API error');
  const items = ((await r.json()).items || []).filter((i) => i.id && i.id.videoId).map((i) => ({ id: i.id.videoId, title: decodeEnt(i.snippet.title), channel: decodeEnt(i.snippet.channelTitle), thumb: (i.snippet.thumbnails && (i.snippet.thumbnails.high || i.snippet.thumbnails.medium || i.snippet.thumbnails.default) || {}).url || 'https://i.ytimg.com/vi/' + i.id.videoId + '/hqdefault.jpg', duration: '', views: '', published: String(i.snippet.publishedAt || '').slice(0, 10) }));
  if (!items.length) return items;
  try { // durations + view counts cost 1 extra quota unit; nice to have, never required
    const d = await timedFetch('https://www.googleapis.com/youtube/v3/videos?part=contentDetails,statistics&id=' + items.map((i) => i.id).join(',') + '&key=' + encodeURIComponent(YT_KEY), {}, 3000);
    if (d.ok) for (const v of ((await d.json()).items || [])) { const it = items.find((x) => x.id === v.id); if (it) { it.duration = ytFmtDur(v.contentDetails && v.contentDetails.duration); it.views = ytFmtViews(v.statistics && v.statistics.viewCount); } }
  } catch (e) { /* optional */ }
  return items;
}
function ytCollect(node, out, depth) {
  if (!node || typeof node !== 'object' || depth > 40 || out.length >= 40) return;
  if (node.videoRenderer && node.videoRenderer.videoId) { out.push(node.videoRenderer); return; }
  for (const k of Object.keys(node)) ytCollect(node[k], out, depth + 1);
}
function ytMap(v) {
  const txt = (x) => (x && (x.simpleText || (x.runs && x.runs.map((r) => r.text).join('')))) || '';
  return { id: v.videoId, title: txt(v.title), channel: txt(v.ownerText) || txt(v.longBylineText), thumb: 'https://i.ytimg.com/vi/' + v.videoId + '/hqdefault.jpg', duration: txt(v.lengthText), views: txt(v.shortViewCountText) || txt(v.viewCountText), published: txt(v.publishedTimeText) };
}
async function ytScrape(q, ms) {
  const r = await timedFetch('https://www.youtube.com/results?search_query=' + encodeURIComponent(q) + '&sp=EgIQAQ%253D%253D', { headers: { 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36', 'accept-language': 'en-US,en;q=0.9', cookie: 'CONSENT=YES+1; SOCS=CAI' } }, ms);
  if (!r.ok) throw new ApiError('yt_error', 'YouTube page error');
  const m = /var ytInitialData\s*=\s*(\{[\s\S]*?\});\s*<\/script>/.exec(await r.text());
  if (!m) throw new ApiError('yt_blocked', 'YouTube page unreadable');
  let data; try { data = JSON.parse(m[1]); } catch { throw new ApiError('yt_blocked', 'YouTube data unreadable'); }
  const found = []; ytCollect(data, found, 0); const seen = new Set();
  const items = found.map(ytMap).filter((x) => x.id && x.title && !seen.has(x.id) && seen.add(x.id));
  if (!items.length) throw new ApiError('yt_blocked', 'no videos found');
  return items.slice(0, 12);
}
async function ytSearch(q) {
  const key = 'yt:' + q.toLowerCase();
  if (READY) { try { const c = await kv(['GET', key]); if (c) return { ...JSON.parse(c), cached: true }; } catch (e) { /* cache is optional */ } }
  let items = null, source = '', last = null; const deadline = Date.now() + 8500;
  if (YT_KEY) { try { items = await ytApi(q, Math.min(5000, deadline - Date.now())); source = 'api'; } catch (e) { last = e; } }
  if (!items) { try { items = await ytScrape(q, Math.max(2500, deadline - Date.now())); source = 'web'; } catch (e) { last = e; } }
  if (!items) throw last || new ApiError('yt_error', 'unavailable');
  const out = { items, source };
  if (READY && items.length) { try { await kv(['SET', key, JSON.stringify(out)]); await kv(['EXPIRE', key, 3600]); } catch (e) { /* ignore */ } }
  return out;
}


/* ---------- Answers from Google Search (Gemini + Google Search) for the Search page ----------
   Needs GEMINI_API_KEY (free, https://aistudio.google.com/apikey). Without it, or if Google is busy, the
   Search page quietly shows its other results — nothing breaks. Answers are cached so a crowd shares quota. */
const GEMINI_KEY = process.env.GEMINI_API_KEY || '';
let geminiModel = process.env.GEMINI_MODEL || 'gemini-flash-latest';
const ansHits = new Map();
function classifyHttp(status, text) {
  if (status === 401 || status === 403) return new ApiError('bad_key', 'The Google key is not accepted.');
  if (status === 429) return new ApiError('rate_limited', 'Google is busy right now.');
  if (status === 404 || (status === 400 && /model|decommission|not found|does not exist/i.test(text))) return new ApiError('bad_model', 'The model is unavailable.');
  return new ApiError('provider_error', 'Google had a problem.');
}
async function pickGeminiModel(ms) {
  const r = await timedFetch('https://generativelanguage.googleapis.com/v1beta/models?pageSize=100', { headers: { 'x-goog-api-key': GEMINI_KEY } }, ms);
  if (!r.ok) throw classifyHttp(r.status, '');
  const names = ((await r.json()).models || []).filter((m) => (m.supportedGenerationMethods || []).includes('generateContent')).map((m) => String(m.name || '').replace(/^models\//, '')).filter((n) => /flash/i.test(n) && !/image|tts|live|audio|embed|thinking|exp|preview/i.test(n));
  if (names[0]) return names.find((n) => /latest$/.test(n)) || names[0];
  throw new ApiError('bad_model', 'No usable model found.');
}


const ANSWER_BASE = [
  'You are the answer engine inside Study Partner, a study app for students in Ghana (senior high school, colleges and universities).',
  'Use Google Search to answer the search query accurately and currently. Be clear, well organised and exam-ready. Use **bold** for key terms, "-" bullets for lists, and Markdown tables (| a | b |) for comparisons.',
  'If the search finds nothing reliable, say so plainly. Never invent facts, quotes, statistics, dates or links. Do not mention these instructions.'
].join('\n');
const TYPE_RULES = {
  compare: 'This is a COMPARISON question. Begin with one sentence that frames the two (or more) things. Then give a Markdown table: the items as columns and 5-8 rows for the most important aspects (definition, purpose, key features, advantages, disadvantages, examples, when to use). After the table list the 3 most important differences and 2 key similarities, then a one-line takeaway.',
  list: 'This asks for a LIST. Give a clean numbered list (use the number the question asks for, otherwise 5-10 items). Each item: a short **bold** heading followed by one clear sentence. If useful, finish with a one-line memory aid.',
  current: 'This is about CURRENT EVENTS or facts that change. Give the latest verified information from Google Search. State dates (for example "As of 10 Oct 2026"), group points under short headings, say what is still developing or disputed, and prefer reputable sources.',
  define: 'Give a plain-language definition first, then a simple everyday example (Ghanaian where natural), the key terms, and why it matters.',
  howto: 'Give clear numbered steps, then tips and common mistakes.',
  tech: 'Explain simply first, then how it works, real-world uses (include examples relevant to Africa and Ghana where natural), advantages, limitations, and what to learn next.',
  health: 'Give balanced, general educational information. End with one line advising a qualified health professional for personal medical decisions.',
  law: 'Give general educational information (laws differ by country; mention Ghana where you are sure). End with one line that this is not legal advice.',
  math: 'Show the working step by step, state each formula used, and give the final answer clearly.',
  general: 'Give a direct answer first, then the key facts and examples a student should know.'
};
const DEEP_RULE = 'The student wants to GO DEEPER. Write a detailed study note (about 450-700 words) using these headings: Overview, How it works / Key points, Examples, Common mistakes or misconceptions, Quick revision points, What to learn next. Use tables or lists wherever they help.';
function classifyQuery(q) {
  const t = String(q).toLowerCase().trim();
  if (/\b(differenti\w*|distinguish|difference[s]? between|compare|comparison|contrast|vs\.?|versus)\b/.test(t)) return 'compare';
  if (/^(list|state|name|outline|enumerate|mention|identify|give (me )?(some|\d+))\b/.test(t) || /\b(types|kinds|examples|advantages|disadvantages|features|characteristics|functions|stages|principles|causes|effects|components|elements) of\b/.test(t)) return 'list';
  if (/\b(news|latest|today|current affairs|current|recent|breaking|this week|this month|this year|right now|updates?)\b/.test(t) || /\b(election|budget|exchange rate|price of|score|weather|who is the (current )?(president|minister|ceo|governor)|202[4-9])\b/.test(t)) return 'current';
  if (/^(how (do|to|can|does|did)|steps to|guide to|ways to)\b/.test(t)) return 'howto';
  if (/\b(solve|calculate|equation|integral|derivative|formula|proof|simplify)\b/.test(t) || /\d\s*[+\-*\/^]\s*\d/.test(t)) return 'math';
  if (/\b(ai|artificial intelligence|software|programming|network(ing)?|cyber\w*|blockchain|cloud|database|machine learning|internet|technology|computer|robot|5g|iot|algorithm|hardware|operating system|web)\b/.test(t)) return 'tech';
  if (/\b(symptom\w*|treatment|medicine|disease|drug|health|diet|vaccine|malaria|infection)\b/.test(t)) return 'health';
  if (/\b(law|legal|constitution|court|rights|contract|tax|act of)\b/.test(t)) return 'law';
  if (/^(what is|what are|what's|define|meaning of|explain|who is|who was)\b/.test(t)) return 'define';
  return 'general';
}
function answerSystem(type, deep) {
  return ANSWER_BASE + '\n' + (TYPE_RULES[type] || TYPE_RULES.general) + (deep ? '\n' + DEEP_RULE : '') +
    "\nToday's date is " + new Date().toUTCString().slice(0, 16) + '.' +
    '\nAfter your answer, on a new final line write exactly "RELATED:" followed by 4 short follow-up questions a student might ask next, separated by " | ". Put nothing after that line.';
}
function splitRelated(text) {
  const m = /\n?\s*RELATED:\s*([^\n]+)\s*$/i.exec(text);
  if (!m) return { answer: text.trim(), related: [] };
  const related = m[1].split('|').map((x) => x.replace(/^[\s\-*\d.)]+/, '').trim()).filter((x) => x.length >= 3 && x.length <= 120).slice(0, 5);
  return { answer: text.slice(0, m.index).trim(), related };
}
async function geminiAnswer(q, deadline, deep) {
  const type = classifyQuery(q), st = { search: true, noThink: true };
  for (let attempt = 0; attempt < 5; attempt++) {
    const r = await timedFetch(`https://generativelanguage.googleapis.com/v1beta/models/${geminiModel}:generateContent`, {
      method: 'POST', headers: { 'content-type': 'application/json', 'x-goog-api-key': GEMINI_KEY },
      body: JSON.stringify({ system_instruction: { parts: [{ text: answerSystem(type, deep) }] }, contents: [{ role: 'user', parts: [{ text: q }] }], ...(st.search ? { tools: [{ google_search: {} }] } : {}), generationConfig: { temperature: 0.3, maxOutputTokens: deep ? 2800 : 1600, ...(st.noThink ? { thinkingConfig: { thinkingBudget: 0 } } : {}) } })
    }, deadline - Date.now());
    if (r.ok) {
      const cand = ((await r.json()).candidates || [])[0] || {};
      const raw = ((cand.content && cand.content.parts) || []).map((x) => x.text || '').join('').trim();
      if (!raw) throw new ApiError('provider_error', 'Empty answer.');
      const { answer, related } = splitRelated(raw);
      const seen = new Set(), sources = [];
      for (const c of ((cand.groundingMetadata && cand.groundingMetadata.groundingChunks) || [])) { const w = c.web; if (w && w.uri && !seen.has(w.uri)) { seen.add(w.uri); sources.push({ title: String(w.title || w.uri).slice(0, 60), url: w.uri }); } }
      return { answer, related, type, deep: !!deep, sources: sources.slice(0, 6), searched: st.search };
    }
    const text = await r.text().catch(() => '');
    if (r.status === 400 && st.noThink && /thinking/i.test(text)) { st.noThink = false; continue; }
    if (r.status === 400 && st.search && /google_search|grounding|search/i.test(text)) { st.search = false; continue; }
    const err = classifyHttp(r.status, text);
    if (err.apiCode === 'bad_model' && attempt < 4) { geminiModel = await pickGeminiModel(deadline - Date.now()); continue; }
    throw err;
  }
  throw new ApiError('bad_model', 'The model is unavailable.');
}
async function answerFor(q, deep) {
  const key = 'ans:' + (deep ? 'd:' : '') + q.toLowerCase();
  if (READY) { try { const c = await kv(['GET', key]); if (c) return { ...JSON.parse(c), cached: true }; } catch (e) { /* cache optional */ } }
  const out = await geminiAnswer(q, Date.now() + (deep ? 9500 : 9000), deep);
  if (READY) { try { await kv(['SET', key, JSON.stringify(out)]); await kv(['EXPIRE', key, deep ? 43200 : 21600]); } catch (e) { /* ignore */ } }
  return out;
}
// ---- latest news headlines (Google News feed — public, no key) ----
const newsHits = new Map();
function parseNewsRss(xml) {
  return [...String(xml).matchAll(/<item>([\s\S]*?)<\/item>/g)].slice(0, 8).map((m) => {
    const b = m[1], tag = (n) => { const x = new RegExp('<' + n + '(?:\\s[^>]*)?>([\\s\\S]*?)</' + n + '>').exec(b); return x ? decodeEnt(x[1].replace(/<!\[CDATA\[|\]\]>/g, '')).trim() : ''; };
    const source = tag('source'); let title = tag('title');
    if (source && title.endsWith(' - ' + source)) title = title.slice(0, -(source.length + 3));
    const pub = tag('pubDate'), t = Date.parse(pub);
    return { title, source, url: tag('link'), published: isNaN(t) ? '' : new Date(t).toISOString() };
  }).filter((x) => x.title && x.url);
}
async function newsFor(q) {
  const key = 'news:' + q.toLowerCase();
  if (READY) { try { const c = await kv(['GET', key]); if (c) return { ...JSON.parse(c), cached: true }; } catch (e) { /* optional */ } }
  let items = [];
  for (const ed of ['hl=en-GH&gl=GH&ceid=GH:en', 'hl=en-US&gl=US&ceid=US:en']) {
    try {
      const r = await timedFetch('https://news.google.com/rss/search?q=' + encodeURIComponent(q) + '&' + ed, { headers: { 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36' } }, 5000);
      if (!r.ok) continue; items = parseNewsRss(await r.text()); if (items.length) break;
    } catch (e) { /* try the next edition */ }
  }
  if (!items.length) throw new ApiError('news_unavailable', 'No news found.');
  const out = { items };
  if (READY) { try { await kv(['SET', key, JSON.stringify(out)]); await kv(['EXPIRE', key, 900]); } catch (e) { /* ignore */ } }
  return out;
}

/* ---------- handler ---------- */
module.exports = async (req, res) => {
  try {
    let body = req.body;
    if (typeof body === 'string') { try { body = JSON.parse(body); } catch { body = {}; } }
    body = body || {};
    const q = req.query || Object.fromEntries(new URL(req.url, 'http://x').searchParams);
    const a = req.method === 'GET' ? q.a : body.a;

    if (a === 'sx') return __M.search(req, res, { ...q, a: String(q.sa || 'web') }); // the search engine, through this one function

    if (a === 'health') {
      const out = { ok: true, ready: READY, redis: REDIS, envNames: Object.keys(process.env).filter((k) => /(KV_|REDIS|UPSTASH|STORAGE|REST_)/i.test(k)).sort() };
      if (REDIS) { try { await kv(['SET', 'health:ping', String(Date.now())]); out.works = (await kv(['GET', 'health:ping'])) != null; } catch (e) { out.works = false; out.error = String(e && e.message || e).slice(0, 120); } }
      return send(res, 200, out);
    }
    if (a === 'answer') {
      if (!GEMINI_KEY) return send(res, 503, { error: 'not_configured', code: 'not_configured' });
      const ip = clientIp(req), now = Date.now(), hh = (ansHits.get(ip) || []).filter((t) => now - t < 600000);
      if (hh.length >= 25) return send(res, 429, { error: 'You are searching very fast — please wait a few minutes.', code: 'rate_limited' });
      hh.push(now); ansHits.set(ip, hh); if (ansHits.size > 5000) ansHits.clear();
      const qq = String(q.q || '').trim().slice(0, 200);
      if (qq.length < 2) return send(res, 400, { error: 'Type something to search for.' });
      const deep = String(q.deep || '') === '1';
      try { return send(res, 200, await answerFor(qq, deep), 'public, s-maxage=600, stale-while-revalidate=1800'); }
      catch (e) { return send(res, e && e.apiCode === 'rate_limited' ? 429 : 502, { error: 'No answer available right now.', code: (e && e.apiCode) || 'provider_error' }); }
    }
    if (a === 'news') {
      const ip = clientIp(req), now = Date.now(), hh = (newsHits.get(ip) || []).filter((t) => now - t < 600000);
      if (hh.length >= 30) return send(res, 429, { error: 'Too many requests — please wait a few minutes.', code: 'rate_limited' });
      hh.push(now); newsHits.set(ip, hh); if (newsHits.size > 5000) newsHits.clear();
      const qq = String(q.q || '').trim().slice(0, 120);
      if (qq.length < 2) return send(res, 400, { error: 'Type something to search for.' });
      try { return send(res, 200, await newsFor(qq), 'public, s-maxage=300, stale-while-revalidate=600'); }
      catch (e) { return send(res, 502, { error: 'News is not available right now.', code: (e && e.apiCode) || 'news_unavailable' }); }
    }
    if (a === 'answertest') { // shows in seconds whether the Google key really works
      const ip = clientIp(req), now = Date.now(), hh = (ansHits.get('t:' + ip) || []).filter((t) => now - t < 600000);
      if (hh.length >= 6) return send(res, 429, { error: 'Test limit reached — try again in a few minutes.' });
      hh.push(now); ansHits.set('t:' + ip, hh);
      if (!GEMINI_KEY) return send(res, 200, { configured: false, ok: false, code: 'not_configured' });
      const t0 = Date.now();
      try { const r = await geminiAnswer('What is the capital of Ghana?', Date.now() + 9000, false); return send(res, 200, { configured: true, ok: true, ms: Date.now() - t0, model: geminiModel, sources: r.sources.length, searched: r.searched, sample: r.answer.slice(0, 60) }); }
      catch (e) { return send(res, 200, { configured: true, ok: false, ms: Date.now() - t0, code: (e && e.apiCode) || 'error' }); }
    }
    if (a === 'yt') {
      const ip = clientIp(req), now = Date.now(), hh = (ytHits.get(ip) || []).filter((t) => now - t < 600000);
      if (hh.length >= 40) return send(res, 429, { error: 'You are searching very fast — please wait a few minutes.', code: 'rate_limited' });
      hh.push(now); ytHits.set(ip, hh); if (ytHits.size > 5000) ytHits.clear();
      const qq = String(q.q || '').trim().slice(0, 100);
      if (qq.length < 2) return send(res, 400, { error: 'Type something to search for.' });
      try { const out = await ytSearch(qq); return send(res, 200, out, 'public, s-maxage=300, stale-while-revalidate=600'); }
      catch (e) { return send(res, 502, { error: 'YouTube search is not available right now.', code: (e && e.apiCode) || 'yt_error' }); }
    }
    if (!READY) return send(res, 503, { error: 'Online storage is not connected yet.' });

    if (a === 'img') {
      const id = String(q.id || '');
      let data = null;
      if (/^[\w-]{1,70}$/.test(id)) {
        const rec = await jget('imgs', id); data = rec && rec.d;
        if (!data && id.startsWith('ad_')) { const ad = await jget('ads', id.slice(3)); data = ad && ad.img; } // older ads kept the flyer inline
      }
      const m = data && /^data:(image\/(?:jpeg|png|webp));base64,(.+)$/.exec(data);
      if (!m) { res.statusCode = 404; res.setHeader('Content-Type', 'text/plain'); res.setHeader('Cache-Control', 'no-store'); return res.end('Not found'); }
      const buf = Buffer.from(m[2], 'base64');
      res.statusCode = 200; res.setHeader('Content-Type', m[1]); res.setHeader('Content-Length', buf.length);
      res.setHeader('Cache-Control', 'public, max-age=31536000, immutable'); // URL changes whenever the picture changes
      return res.end(buf);
    }
    if (a === 'content') {
      const settings = (await jget('content:settings')) || {};
      const ads = (await sweepAds(await hall('ads'))).filter((x) => x.status === 'live').map((x) => ({ id: x.id, biz: x.biz, kind: x.kind || 'video', link: x.link || '', img: adImgUrl(x), wa: x.wa, status: 'live' }));
      const dayImages = await dayImageUrls();
      // Short shared cache: a launch-day crowd opening the app reads from the edge, not from the database.
      return send(res, 200, { online: (await jget('content:online')) || [], dayImages, ads, googleCx: settings.googleCx || process.env.GOOGLE_CSE_ID || '' }, 'public, s-maxage=10, stale-while-revalidate=30');
    }

    const t = body.t;
    if (!FEAT) FEAT = __M.features({ kv, jget, hall, verify, needAdmin, needUser, bad, nameKey, cleanName, putImg, IMG_RE, crypto, rateCheck, saveUser, engine: __M.engine });
    { const r = await FEAT(a, body, q, t, req); if (r) { if (r.raw) { res.statusCode = r.code; for (const [k, v] of Object.entries(r.headers || {})) res.setHeader(k, v); return res.end(r.buf); } return send(res, r.code, r.obj, r.cache); } }
    switch (a) {
      case 'signup': case 'login': {
        // Network-wide limits are generous on purpose: a whole class often shares ONE public IP.
        // The strict limit is per (network + account name), so one student's typos never lock out the room.
        const ip = clientIp(req);
        if (await rateCheck('rl:ip:' + a + ':' + ip, a === 'signup' ? 800 : 3000)) bad(429, 'A lot of people are signing in from this network right now. Please wait a minute and try again.');
        const name = cleanName(body.name), pass = String(body.pass || '');
        const nkey = nameKey(name), failKey = 'rl:fail:' + ip + ':' + nkey, isAdminName = nkey === nameKey(await adminName());
        if ((+(await kv(['GET', failKey])) || 0) >= (isAdminName ? 8 : 15)) bad(429, 'Too many wrong passwords for this name. Please wait about 10 minutes, or ask the admin to reset it.');
        if ((+(await kv(['GET', 'rl:ipfail:' + ip])) || 0) >= 80) bad(429, 'Too many failed attempts from this network. Please wait about 10 minutes and try again.');
        if (await checkAdminLogin(name, pass)) {
          await kv(['DEL', failKey]);
          return send(res, 200, { token: adminToken(), user: { name: 'Admin', isAdmin: true }, usingDefault: !(await adminCreds()).salt });
        }
        if (isAdminName) { await bump(failKey); await bump('rl:ipfail:' + ip); }
        if (a === 'signup') {
          const { key, u } = await createUser(name, pass);
          return send(res, 200, { token: userToken(key), user: await fullUser(key, u) });
        }
        const key = nkey, u = await jget('users', key);
        if (!u) bad(404, 'No account found with that name. Check the spelling, or tap “Sign up” to create one.');
        if (u.active === false) bad(403, 'This account has been deactivated. Please contact the admin.');
        if (!safeEq(hashPw(pass, u.salt), u.hash)) { await bump(failKey); await bump('rl:ipfail:' + ip); bad(401, 'Wrong password. Try again — or ask the admin to reset it.'); }
        await kv(['DEL', failKey]);
        u.lastLogin = Date.now(); await migrateAvatar(key, u); await saveUser(key, u);
        return send(res, 200, { token: userToken(key), user: await fullUser(key, u) });
      }
      case 'me': {
        const p = verify(t);
        if (p && p.role === 'admin') return send(res, 200, { user: { name: 'Admin', isAdmin: true }, usingDefault: !(await adminCreds()).salt });
        const { key, u } = await needUser(t);
        return send(res, 200, { user: await fullUser(key, u) });
      }
      case 'adminCreds': {
        needAdmin(t);
        const curName = (await adminCreds()).name;
        const newName = body.newName !== undefined ? cleanName(body.newName) : curName;
        const newPass = body.newPass !== undefined ? String(body.newPass) : '';
        const curPass = String(body.curPass || '');
        if (newName.length < 2 || newName.length > 40) bad(400, 'Admin name must be 2–40 characters.');
        if (!(await checkAdminLogin(curName, curPass))) bad(401, 'Current admin password is wrong.');
        if (newPass && newPass.length < 4) bad(400, 'New password must be at least 4 characters.');
        if (nameKey(newName) !== nameKey(curName) && (await kv(['HGET', 'users', nameKey(newName)]))) bad(409, 'A student account already uses that name.');
        const c = await adminCreds();
        const salt = crypto.randomBytes(16).toString('hex');
        const pass = newPass || curPass; // keep current password if a new one wasn't given, but re-hash under the (possibly new) salt/name
        await kv(['SET', 'admin:creds', JSON.stringify({ name: newName, salt, hash: hashPw(pass, salt) })]);
        return send(res, 200, { ok: true, name: newName, token: adminToken() });
      }
      case 'profile': {
        let { key, u } = await needUser(t);
        if (body.timetable !== undefined) {
          if (typeof body.timetable !== 'object' || JSON.stringify(body.timetable).length > 60000) bad(400, 'Timetable is too large.');
          u.timetable = body.timetable;
        }
        if (body.profile !== undefined) {
          const p = body.profile || {}, s = (v, n) => String(v || '').slice(0, n);
          const av = typeof p.avatar === 'string' && IMG_RE.test(p.avatar) && p.avatar.length <= 120000 ? p.avatar : '';
          u.profile = { school: s(p.school, 80), prog: s(p.prog, 80), level: s(p.level, 30), phone: s(p.phone, 30), bio: s(p.bio, 160), theme: ['auto', 'light', 'dark'].includes(p.theme) ? p.theme : 'auto', avatar: '' };
          if (av) await kv(['HSET', 'avatars', key, av]); else await kv(['HDEL', 'avatars', key]);
        }
        if (body.newPass) {
          if (!safeEq(hashPw(String(body.curPass || ''), u.salt), u.hash)) bad(401, 'Your current password is wrong.');
          if (String(body.newPass).length < 6) bad(400, 'Password must be at least 6 characters.');
          u.salt = crypto.randomBytes(16).toString('hex'); u.hash = hashPw(String(body.newPass), u.salt);
        }
        let oldKey = key;
        if (body.newName) {
          const nn = cleanName(body.newName), nk = nameKey(nn);
          if (nn.length < 2 || nn.length > 40) bad(400, 'Name must be 2–40 characters.');
          if (nk !== key && (nk === nameKey(await adminName()) || (await kv(['HGET', 'users', nk])))) bad(409, 'That name is already taken.');
          u.name = nn; key = nk;
        }
        await saveUser(key, u);
        if (key !== oldKey) { const ud0 = await kv(['HGET', 'ud', oldKey]); if (ud0) { await kv(['HSET', 'ud', key, ud0]); await kv(['HDEL', 'ud', oldKey]); } await kv(['HDEL', 'users', oldKey]); const av0 = await kv(['HGET', 'avatars', oldKey]); if (av0) { await kv(['HSET', 'avatars', key, av0]); await kv(['HDEL', 'avatars', oldKey]); } }
        return send(res, 200, { user: await fullUser(key, u), token: userToken(key) });
      }
      case 'adSubmit': {
        const ad = body.ad || {};
        const s = (v, n) => String(v || '').slice(0, n);
        const kind = ad.kind === 'flyer' ? 'flyer' : 'video';
        const img = kind === 'flyer' && typeof ad.img === 'string' && IMG_RE.test(ad.img) && ad.img.length <= 700000 ? ad.img : '';
        const rec = { id: 'a' + Date.now() + crypto.randomBytes(3).toString('hex'), biz: s(ad.biz, 80), kind, link: kind === 'video' ? s(ad.link, 500) : '', img: '', hasImg: !!img, wa: s(ad.wa, 200), plan: s(ad.plan, 40), durationMs: planMs(ad.plan), price: Number(ad.price) || 0, ref: s(ad.ref, 80), by: (() => { const vp = verify(body.t); return vp && vp.role === 'student' ? vp.u : ''; })(), status: 'pending', createdAt: Date.now() };
        if (!rec.biz || (kind === 'video' && !rec.link) || (kind === 'flyer' && !img)) bad(400, 'Please add the business name and either a video link or a flyer image.');
        if (img) await putImg('ad_' + rec.id, img);
        await kv(['HSET', 'ads', rec.id, JSON.stringify(rec)]);
        return send(res, 200, { ok: true });
      }
      case 'myAds': {
        const { key } = await needUser(t); const mine = (await sweepAds(await hall('ads'))).filter((x) => x.by === key).sort((x, y) => y.createdAt - x.createdAt).slice(0, 50);
        return send(res, 200, { ads: mine.map((x) => ({ id: x.id, biz: x.biz, kind: x.kind || 'video', status: x.status, plan: x.plan, createdAt: x.createdAt, expiresAt: x.expiresAt || 0, img: adImgUrl(x) })) });
      }
      /* ---- admin only ---- */
      case 'users': { needAdmin(t); return send(res, 200, { users: (await hall('users')).map(listUser) }); }
      case 'userSet': {
        needAdmin(t); const key = nameKey(cleanName(body.name)), u = await jget('users', key);
        if (!u) bad(404, 'Account not found.'); u.active = !!body.active; await saveUser(key, u); return send(res, 200, { ok: true });
      }
      case 'userDel': { needAdmin(t); const dk = nameKey(cleanName(body.name)); await kv(['HDEL', 'users', dk]); await kv(['HDEL', 'avatars', dk]); await kv(['HDEL', 'ud', dk]); return send(res, 200, { ok: true }); }
      case 'userPass': {
        needAdmin(t); const key = nameKey(cleanName(body.name)), u = await jget('users', key);
        if (!u) bad(404, 'Account not found.');
        if (String(body.pass || '').length < 4) bad(400, 'Password must be at least 4 characters.');
        u.salt = crypto.randomBytes(16).toString('hex'); u.hash = hashPw(String(body.pass), u.salt); await saveUser(key, u);
        return send(res, 200, { ok: true });
      }
      case 'userCreate': { needAdmin(t); const { u } = await createUser(body.name, body.pass); return send(res, 200, { user: listUser(u) }); }
      case 'status': {
        needAdmin(t);
        const meta = (await jget('content:dayImages')) || {}, days = [];
        for (const d of DAYKEYS) days.push({ day: d, bytes: meta[d] ? Number(await kv(['HSTRLEN', 'imgs', 'day_' + d])) || 0 : 0, url: meta[d] ? '/api?a=img&id=day_' + d + '&v=' + (meta[d].v || 0) : '' });
        const all = await hall('ads'), adsOut = [];
        for (const x of all) adsOut.push({ id: x.id, biz: x.biz, status: x.status, kind: x.kind, expiresAt: x.expiresAt || 0, bytes: (x.hasImg || x.img) ? (Number(await kv(['HSTRLEN', 'imgs', 'ad_' + x.id])) || (x.img ? x.img.length : 0)) : 0, url: adImgUrl(x) });
        const settings = (await jget('content:settings')) || {};
        return send(res, 200, { redis: REDIS, days, ads: adsOut, users: (await hall('users')).length, googleCx: !!(settings.googleCx || process.env.GOOGLE_CSE_ID), youtubeKey: !!YT_KEY, googleAnswers: !!GEMINI_KEY, now: Date.now() });
      }
      case 'settingsSet': {
        needAdmin(t);
        const cx = String(body.googleCx == null ? '' : body.googleCx).trim();
        if (cx && !/^[\w:.-]{6,100}$/.test(cx)) bad(400, 'That does not look like a Search engine ID (it is a short code like 0123abc456def:xyz).');
        const settings = (await jget('content:settings')) || {}; settings.googleCx = cx;
        await kv(['SET', 'content:settings', JSON.stringify(settings)]);
        return send(res, 200, { ok: true, googleCx: cx });
      }
      case 'dayImageSet': {
        needAdmin(t);
        const d = String(body.day || ''), img = body.dataUrl;
        if (!DAYKEYS.includes(d)) bad(400, 'Unknown day.');
        if (typeof img !== 'string' || !IMG_RE.test(img)) bad(400, 'That does not look like a picture (use a JPG or PNG).');
        if (img.length > 900000) bad(413, 'That picture is too big — please choose a smaller one.');
        await putImg('day_' + d, img);
        const meta = (await jget('content:dayImages')) || {}; meta[d] = { v: Date.now() };
        await kv(['SET', 'content:dayImages', JSON.stringify(meta)]);
        return send(res, 200, { ok: true, dayImages: await dayImageUrls() });
      }
      case 'dayImageDel': {
        needAdmin(t);
        const d = String(body.day || ''); if (!DAYKEYS.includes(d)) bad(400, 'Unknown day.');
        const meta = (await jget('content:dayImages')) || {}; delete meta[d];
        await kv(['SET', 'content:dayImages', JSON.stringify(meta)]); await kv(['HDEL', 'imgs', 'day_' + d]);
        return send(res, 200, { ok: true, dayImages: await dayImageUrls() });
      }
      case 'setContent': {
        needAdmin(t);
        const k = body.key, v = body.value;
        if (k !== 'online' || !Array.isArray(v)) bad(400, 'Invalid content.');
        const s = JSON.stringify(v);
        if (s.length > MAX) bad(413, 'That is too large to save online — use smaller images or links instead of uploaded files.');
        await kv(['SET', 'content:' + k, s]);
        return send(res, 200, { ok: true });
      }
      case 'learnNow': { needAdmin(t); return send(res, 200, await __M.engine.learn({ budgetMs: 25000 })); }
      case 'adList': { needAdmin(t); const ads = (await sweepAds(await hall('ads'))).map((x) => ({ ...x, img: adImgUrl(x) })); return send(res, 200, { ads: ads.sort((x, y) => y.createdAt - x.createdAt) }); }
      case 'adSet': {
        needAdmin(t); const ad = await jget('ads', body.id);
        if (!ad || !['live', 'rejected', 'pending', 'ended'].includes(body.status)) bad(400, 'Invalid request.');
        ad.status = body.status;
        if (body.status === 'live') { ad.liveAt = Date.now(); ad.expiresAt = ad.liveAt + (ad.durationMs || planMs(ad.plan)); delete ad.endedAt; delete ad.endedBy; }
        if (body.status === 'ended') { ad.endedAt = Date.now(); ad.endedBy = 'admin'; }
        await kv(['HSET', 'ads', ad.id, JSON.stringify(ad)]); return send(res, 200, { ok: true });
      }
      case 'adCreate': {
        needAdmin(t);
        const ad = body.ad || {}, s = (v, n) => String(v || '').slice(0, n);
        const kind = ad.kind === 'flyer' ? 'flyer' : 'video';
        const img = kind === 'flyer' && typeof ad.img === 'string' && IMG_RE.test(ad.img) && ad.img.length <= 700000 ? ad.img : '';
        if (!s(ad.biz, 80) || (kind === 'video' && !s(ad.link, 500)) || (kind === 'flyer' && !img)) bad(400, 'Add a business name and either a video link or a flyer image.');
        const now = Date.now(), durationMs = planMs(ad.plan);
        const rec = { id: 'a' + now + crypto.randomBytes(3).toString('hex'), biz: s(ad.biz, 80), kind, link: kind === 'video' ? s(ad.link, 500) : '', img: '', hasImg: !!img, wa: s(ad.wa, 200), plan: s(ad.plan, 40) || '24 hours', durationMs, price: 0, ref: 'admin', status: 'live', createdAt: now, liveAt: now, expiresAt: now + durationMs };
        if (img) await putImg('ad_' + rec.id, img);
        await kv(['HSET', 'ads', rec.id, JSON.stringify(rec)]);
        return send(res, 200, { ok: true, id: rec.id });
      }
      case 'adDelete': { needAdmin(t); await kv(['HDEL', 'ads', body.id]); await kv(['HDEL', 'imgs', 'ad_' + body.id]); return send(res, 200, { ok: true }); }
      default: bad(400, 'Unknown request.');
    }
  } catch (e) {
    if (e instanceof HttpError) return send(res, e.code, { error: e.message });
    console.error(e);
    return send(res, 500, { error: 'Something went wrong on the server. Please try again.' });
  }
};
