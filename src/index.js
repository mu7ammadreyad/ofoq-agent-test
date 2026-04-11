// ================================================================
//  OFOQ Publisher Agent v5.0 — بسم الله الرحمن الرحيم
//  ─────────────────────────────────────────────────────────────
//  التفكير : Gemini 2.5 Flash Native Thinking (SSE streaming)
//  التنفيذ : Function Calling — لا eval() لا code generation
//  الذاكرة : Workers KV
//  الـ Cron  : Cloudflare Workflows + step.sleepUntil()
// ================================================================

import { WorkflowEntrypoint } from 'cloudflare:workers';

// ================================================================
// SECTION 0 — DEFAULTS
// ================================================================
const DEFAULT_MEM = {
  github:   { repo_owner:null, repo_name:null, token:null, status:'not_configured', last_verified:null, pending_count:0 },
  youtube:  { client_id:null, client_secret:null, refresh_token:null, access_token:null, channel_id:null, status:'not_configured', last_verified:null },
  instagram:{ access_token:null, account_id:null, account_name:null, status:'not_configured', last_verified:null },
  facebook: { access_token:null, page_id:null, page_name:null, status:'not_configured', last_verified:null },
  tiktok:   { access_token:null, open_id:null, status:'not_configured', last_verified:null },
  settings: { timezone:'Africa/Cairo', location_lat:'30.0444', location_lng:'31.2357', fajr_offset_minutes:'30', posts_per_day:'4' },
};
const DEFAULT_DOCS = {
  plan:   '# خطة النشر اليومية\n## الحالة: idle\n## الجدول\n| الوقت | المنصة | الفيديو | الحالة | رابط |\n|-------|--------|---------|--------|------|\n\n## إحصائيات اليوم\n- منشور: 0\n- فاشل: 0\n- متبقي: 0\n',
  log:    '# سجل العمليات\n## آخر تحديث: null\n\n## العمليات (آخر 50)\n',
  queue:  '# قائمة الفيديوهات\n## آخر تحديث: null\n## إجمالي المعلق: 0\n\n## آخر منشور\n- الاسم: null\n- المنصة: null\n- التاريخ: null\n- الرابط: null\n',
  health: '# تقرير الصحة\n## آخر فحص: null\n\n## حالة التوكنز\n- GitHub: null\n\n## تحذيرات\n- لا تحذيرات\n',
};

const PERSONA = `أنت أفق — OFOQ Publishing Agent v5.0
مهمتك: مساعدة المستخدم في نشر المحتوى الإسلامي تلقائياً على منصات التواصل.
لغتك: عربي مصري، ودود، مباشر، مختصر.
عندك tools جاهزة — استخدمها دون تردد بدل الكلام.
ردودك النصية بعد الـ tool: مختصرة ومفيدة، لا تعيد ما قالته الـ tool.
القيم: لا نشر محتوى غير إسلامي | لا حفظ كلمات مرور | الشفافية الكاملة.`;

// ================================================================
// SECTION 1 — KV MEMORY ENGINE
// ================================================================
async function getMemory(env) {
  const raw = await env.OFOQ_KV.get('memory');
  if (!raw) return JSON.parse(JSON.stringify(DEFAULT_MEM));
  try { return JSON.parse(raw); } catch { return JSON.parse(JSON.stringify(DEFAULT_MEM)); }
}
async function saveMemory(env, mem) { await env.OFOQ_KV.put('memory', JSON.stringify(mem)); }
async function memWrite(env, section, data) {
  const mem = await getMemory(env);
  if (!mem[section]) mem[section] = {};
  Object.assign(mem[section], data);
  await saveMemory(env, mem);
}
async function getDoc(env, key) { return (await env.OFOQ_KV.get(`doc_${key}`)) || DEFAULT_DOCS[key] || ''; }
async function saveDoc(env, key, content) { await env.OFOQ_KV.put(`doc_${key}`, content); }
async function appendLog(env, time, platform, video, status, detail) {
  let log = await getDoc(env, 'log');
  const ts    = new Date().toLocaleString('ar-EG', { timeZone:'Africa/Cairo' });
  const lines = (log + `\n- [${ts}] | ${time} | ${platform} | ${video} | ${status} | ${detail}`)
    .split('\n').filter(l => l.startsWith('- '));
  await saveDoc(env, 'log', `# سجل العمليات\n## آخر تحديث: ${new Date().toISOString()}\n\n## العمليات (آخر 50)\n` + lines.slice(-50).join('\n'));
}

// ================================================================
// SECTION 2 — GITHUB API
// ================================================================
function ghHeaders(token) {
  return { Authorization:`token ${token}`, Accept:'application/vnd.github.v3+json', 'Content-Type':'application/json', 'User-Agent':'OFOQ-Agent/5.0' };
}
async function ghFetch(method, path, token, body=null) {
  const r = await fetch(`https://api.github.com${path}`, { method, headers:ghHeaders(token), body:body?JSON.stringify(body):null });
  let data; try { data=await r.json(); } catch { data={}; }
  return { ok:r.ok, status:r.status, data };
}
async function getPendingVideos(owner, repo, token) {
  const { ok, data:releases } = await ghFetch('GET', `/repos/${owner}/${repo}/releases`, token);
  if (!ok || !Array.isArray(releases)) return [];
  const rel = releases.find(r => r.tag_name==='pending'); if (!rel) return [];
  const { data:assets } = await ghFetch('GET', `/repos/${owner}/${repo}/releases/${rel.id}/assets`, token);
  if (!Array.isArray(assets)) return [];
  return assets.filter(a => /\.(mp4|mov|avi|mkv|webm)$/i.test(a.name)).map(a => {
    const base = a.name.replace(/\.[^.]+$/, '');
    const md   = assets.find(x => x.name===`${base}.md`);
    return { id:a.id, name:a.name, base, url:a.browser_download_url, size:a.size, mdId:md?.id||null, mdUrl:md?.browser_download_url||null };
  });
}
async function readVideoMeta(mdUrl, token) {
  if (!mdUrl) return { title:'', description:'', tags:[] };
  const r = await fetch(mdUrl, { headers:ghHeaders(token) });
  if (!r.ok) return { title:'', description:'', tags:[] };
  const text=await r.text(), meta={ title:'', description:'', tags:[] }; let inDesc=false;
  for (const line of text.split('\n')) {
    if      (line.startsWith('# '))                       { meta.title=line.slice(2).trim(); inDesc=false; }
    else if (/^## (وصف|description)/i.test(line))        { inDesc=true; }
    else if (/^## /.test(line))                           { inDesc=false; }
    else if (/^(tags|وسوم|هاشتاقات)\s*:/i.test(line))    { meta.tags=line.split(':')[1]?.split(/[,،]/).map(t=>t.trim().replace(/^#/,'')).filter(Boolean)||[]; }
    else if (inDesc && line.trim())                       { meta.description+=(meta.description?'\n':'')+line.trim(); }
  }
  return meta;
}
async function deleteReleaseAsset(owner, repo, token, assetId) {
  return (await fetch(`https://api.github.com/repos/${owner}/${repo}/releases/assets/${assetId}`, { method:'DELETE', headers:ghHeaders(token) })).ok;
}
async function removeFromPending(owner, repo, token, video) {
  for (const id of [video.id, video.mdId].filter(Boolean)) await deleteReleaseAsset(owner, repo, token, id);
}

// ================================================================
// SECTION 3 — TOOL IMPLEMENTATIONS
// بديل كامل لـ eval() وcode generation
// ================================================================
async function toolSaveCredentials(env, { platform, data }) {
  if (!platform || !data) return { success:false, error:'platform و data مطلوبان' };
  await memWrite(env, platform, data);
  return { success:true, saved:Object.keys(data).join(', '), message:`تم حفظ بيانات ${platform}` };
}

async function toolVerifyConnection(env, { platform }) {
  const mem = await getMemory(env);
  switch (platform) {
    case 'github': {
      if (!mem.github.token) return { success:false, error:'GitHub token غير موجود — أضفه أولاً' };
      const r = await fetch('https://api.github.com/user', { headers:ghHeaders(mem.github.token) });
      if (!r.ok) return { success:false, error:`GitHub API فشل: ${r.status}` };
      const d = await r.json();
      await memWrite(env, 'github', { status:'verified', last_verified:new Date().toISOString() });
      return { success:true, data:{ login:d.login, name:d.name, public_repos:d.public_repos } };
    }
    case 'youtube': {
      if (!mem.youtube.client_id || !mem.youtube.refresh_token) return { success:false, error:'YouTube: client_id + client_secret + refresh_token مطلوبان' };
      const token = await ytGetAccessToken(mem.youtube);
      if (!token) return { success:false, error:'فشل تجديد YouTube token — تحقق من client_id/secret/refresh_token' };
      await memWrite(env, 'youtube', { status:'verified', last_verified:new Date().toISOString(), access_token:token });
      return { success:true, data:{ message:'YouTube token صالح وتم تجديده' } };
    }
    case 'facebook':
    case 'instagram': {
      if (!mem.facebook.access_token) return { success:false, error:'Facebook access_token غير موجود' };
      const r = await fetch(`https://graph.facebook.com/me?fields=name,id&access_token=${mem.facebook.access_token}`);
      const d = await r.json();
      if (!r.ok) return { success:false, error:`Facebook فشل: ${d.error?.message||r.status}` };
      await memWrite(env, 'facebook', { status:'verified', last_verified:new Date().toISOString() });
      if (mem.instagram.access_token) await memWrite(env, 'instagram', { status:'verified', last_verified:new Date().toISOString() });
      return { success:true, data:{ name:d.name, id:d.id } };
    }
    case 'tiktok': {
      if (!mem.tiktok.access_token) return { success:false, error:'TikTok access_token غير موجود' };
      await memWrite(env, 'tiktok', { status:'verified', last_verified:new Date().toISOString() });
      return { success:true, data:{ note:'تم قبول الـ token — TikTok لا يدعم التحقق التلقائي' } };
    }
    default: return { success:false, error:`منصة غير معروفة: ${platform}` };
  }
}

async function toolListPendingVideos(env) {
  const mem = await getMemory(env);
  if (!mem.github.token) return { success:false, error:'GitHub غير مُهيأ — أضف token أولاً' };
  const videos = await getPendingVideos(mem.github.repo_owner, mem.github.repo_name, mem.github.token);
  if (!videos.length) return { success:true, data:{ count:0, message:'لا يوجد فيديوهات في pending release' } };
  return { success:true, data:{ count:videos.length, videos:videos.slice(0,10).map(v=>({ name:v.base, size_mb:(v.size/1024/1024).toFixed(1), has_meta:!!v.mdUrl })) } };
}

async function toolBuildDailyPlan(env) {
  const result = await buildDailyPlanFromChat(env, async()=>{});
  return { success:!result.message?.startsWith('❌'), message:result.message };
}

async function toolHealthCheck(env) {
  const h = await runHealthCheck(env, null);
  return { success:true, data:{ results:h.results, warnings:h.warnings, all_ok:h.warnings.length===0 } };
}

async function toolGetStatus(env) {
  const mem  = await getMemory(env);
  const fajr = calcFajr(parseFloat(mem.settings.location_lat)||30.0444, parseFloat(mem.settings.location_lng)||31.2357);
  const plan = await getDoc(env, 'plan');
  let wf = 'unknown';
  try { const today=new Date().toLocaleDateString('en-CA',{timeZone:'Africa/Cairo'}); const inst=await env.PUBLISH_WORKFLOW.get(`daily-${today}`); wf=(await inst.status()).status; } catch { wf='no_instance_today'; }
  return { success:true, data:{ fajr:fajr?.formatted||'?', workflow:wf, plan_active:plan.includes('active'), published_today:(plan.match(/✅ منشور/g)||[]).length, github:mem.github.status, youtube:mem.youtube.status, instagram:mem.instagram.status, facebook:mem.facebook.status, tiktok:mem.tiktok.status } };
}

async function toolFetchGitHub(env, { path }) {
  const mem = await getMemory(env);
  if (!mem.github.token) return { success:false, error:'GitHub غير مُهيأ' };
  const { ok, data } = await ghFetch('GET', path, mem.github.token);
  if (!ok) return { success:false, error:`GitHub ${path}: فشل` };
  if (Array.isArray(data)) return { success:true, data:{ count:data.length, items:data.slice(0,20).map(i=>i.name||i.tag_name||i.id) } };
  return { success:true, data };
}

async function toolUpdateSettings(env, args) {
  const updates = {};
  const fields = ['location_lat','location_lng','posts_per_day','fajr_offset_minutes'];
  for (const f of fields) { if (args[f] != null) updates[f] = String(args[f]); }
  if (!Object.keys(updates).length) return { success:false, error:'لم تُرسَل أي إعدادات' };
  await memWrite(env, 'settings', updates);
  return { success:true, updated:updates };
}

async function executeTool(env, name, args={}) {
  try {
    switch (name) {
      case 'save_credentials':    return await toolSaveCredentials(env, args);
      case 'verify_connection':   return await toolVerifyConnection(env, args);
      case 'list_pending_videos': return await toolListPendingVideos(env);
      case 'build_daily_plan':    return await toolBuildDailyPlan(env);
      case 'health_check':        return await toolHealthCheck(env);
      case 'get_status':          return await toolGetStatus(env);
      case 'fetch_github':        return await toolFetchGitHub(env, args);
      case 'update_settings':     return await toolUpdateSettings(env, args);
      default:                    return { success:false, error:`tool غير معروف: ${name}` };
    }
  } catch (e) { return { success:false, error:`${name}: ${e.message}` }; }
}

const TOOL_LABELS = {
  save_credentials:'💾 حفظ البيانات', verify_connection:'🔍 التحقق',
  list_pending_videos:'📹 جلب الفيديوهات', build_daily_plan:'📅 بناء الخطة',
  health_check:'🏥 فحص الصحة', get_status:'📊 حالة النظام',
  fetch_github:'🔗 GitHub', update_settings:'⚙️ تحديث الإعدادات',
};

// ================================================================
// SECTION 4 — TOOL DECLARATIONS (Gemini Function Calling Schema)
// ================================================================
const TOOL_DECLARATIONS = [
  {
    name:'save_credentials',
    description:'حفظ بيانات تسجيل دخول منصة في الذاكرة. استخدمها فوراً عندما يعطيك المستخدم أي token أو ID أو secret.',
    parameters:{ type:'OBJECT', properties:{ platform:{ type:'STRING', description:'github | youtube | instagram | facebook | tiktok | settings' }, data:{ type:'OBJECT', description:'البيانات: token, repo_owner, repo_name, client_id, client_secret, refresh_token, access_token, account_id, page_id, open_id', properties:{ token:{type:'STRING'}, repo_owner:{type:'STRING'}, repo_name:{type:'STRING'}, client_id:{type:'STRING'}, client_secret:{type:'STRING'}, refresh_token:{type:'STRING'}, access_token:{type:'STRING'}, account_id:{type:'STRING'}, page_id:{type:'STRING'}, open_id:{type:'STRING'} } } }, required:['platform','data'] }
  },
  {
    name:'verify_connection',
    description:'التحقق من صحة الـ token لمنصة معينة واختبار الاتصال. استخدمها بعد حفظ أي credentials.',
    parameters:{ type:'OBJECT', properties:{ platform:{ type:'STRING', description:'github | youtube | facebook | instagram | tiktok' } }, required:['platform'] }
  },
  {
    name:'list_pending_videos',
    description:'عرض قائمة الفيديوهات المعلقة في GitHub Release pending وعدها.',
    parameters:{ type:'OBJECT', properties:{} }
  },
  {
    name:'build_daily_plan',
    description:'بناء خطة النشر اليومية الذكية وإطلاق Cloudflare Workflow للنشر التلقائي.',
    parameters:{ type:'OBJECT', properties:{} }
  },
  {
    name:'health_check',
    description:'فحص صحة جميع التوكنز والاتصالات وعرض التقرير الكامل.',
    parameters:{ type:'OBJECT', properties:{} }
  },
  {
    name:'get_status',
    description:'عرض الحالة الكاملة للنظام: Workflow، المنصات، وقت الفجر، إحصائيات اليوم.',
    parameters:{ type:'OBJECT', properties:{} }
  },
  {
    name:'fetch_github',
    description:'استعلام GitHub API لجلب أي بيانات: releases, repos, contents, إلخ.',
    parameters:{ type:'OBJECT', properties:{ path:{ type:'STRING', description:'مثل: /repos/owner/repo/releases أو /repos/owner/repo/contents/' } }, required:['path'] }
  },
  {
    name:'update_settings',
    description:'تحديث إعدادات الموقع وجدول النشر.',
    parameters:{ type:'OBJECT', properties:{ location_lat:{type:'STRING'}, location_lng:{type:'STRING'}, posts_per_day:{type:'STRING'}, fajr_offset_minutes:{type:'STRING'} } }
  },
];

// ================================================================
// SECTION 5 — GEMINI STREAMING ENGINE
// streamGenerateContent + alt=sse
// → thinking tokens بـ real-time
// → function calls تُكتشف وتُنفَّذ فوراً
// ================================================================
async function streamGemini(env, messages, onThinkChunk, onUpdate) {
  const apiKey = env.GEMINI_API_KEY;
  if (!apiKey) throw new Error('GEMINI_API_KEY غير مُهيأ في Worker Secrets — أضفه من Dashboard');

  const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-3.0-flash:streamGenerateContent?alt=sse&key=${apiKey}`;

  // Build context from KV in parallel
  const [mem, plan, log] = await Promise.all([ getMemory(env), getDoc(env,'plan'), getDoc(env,'log') ]);

  const memSummary = Object.entries(mem).map(([s,d]) => {
    const active = Object.entries(d).filter(([,v]) => v && v!=='null' && v!=='not_configured');
    return active.length ? `${s}: ${active.map(([k,v])=>`${k}=${v}`).join(', ')}` : `${s}: غير مُهيأ`;
  }).join('\n');

  // inject context as first turn
  const contextMsg = `[Context - ${new Date().toLocaleString('ar-EG',{timeZone:'Africa/Cairo'})}]\nالذاكرة:\n${memSummary}\nآخر الخطة: ${plan.slice(0,200)}\nآخر السجل: ${log.slice(-150)}`;
  const fullMessages = [
    { role:'user',  parts:[{ text:contextMsg }] },
    { role:'model', parts:[{ text:'فهمت. كيف أساعدك؟' }] },
    ...messages,
  ];

  const body = {
    contents:        fullMessages,
    tools:           [{ functionDeclarations:TOOL_DECLARATIONS }],
    toolConfig:      { functionCallingConfig:{ mode:'AUTO' } },
    systemInstruction:{ parts:[{ text:PERSONA }] },
    generationConfig:{
      temperature:    0.3,
      maxOutputTokens:2048,
      thinkingConfig: { thinkingBudget:512 }, // ← Native Gemini 2.5 thinking
    },
  };

  const controller = new AbortController();
  const tmId       = setTimeout(() => controller.abort(), 28_000);

  let resp;
  try {
    resp = await fetch(url, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body), signal:controller.signal });
  } catch (e) {
    clearTimeout(tmId);
    if (e.name==='AbortError') throw new Error('انتهت مهلة الـ AI (28s) — جرب مرة أخرى');
    throw e;
  }
  clearTimeout(tmId);

  if (!resp.ok) {
    if (resp.status===503 || resp.status===429) return fallbackGemma(env, messages);
    const err = await resp.json().catch(()=>({}));
    throw new Error(`Gemini ${resp.status}: ${JSON.stringify(err).slice(0,100)}`);
  }

  const reader  = resp.body.getReader();
  const decoder = new TextDecoder();
  let buf = '', thinking = '', text = '', funcCall = null;

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buf += decoder.decode(value, { stream:true });
    const lines = buf.split('\n'); buf = lines.pop() ?? '';

    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed.startsWith('data:')) continue;
      const json = trimmed.slice(5).trim();
      if (!json || json==='[DONE]') continue;
      let chunk; try { chunk=JSON.parse(json); } catch { continue; }

      for (const part of (chunk.candidates?.[0]?.content?.parts ?? [])) {
        if (part.thought===true && part.text) {
          // ← Native thinking token — stream فوراً للـ frontend
          thinking += part.text;
          await onThinkChunk(part.text);
        } else if (part.functionCall) {
          funcCall = part.functionCall;
        } else if (part.text) {
          text += part.text;
        }
      }
    }
  }

  return { thinking, text, funcCall };
}

// Gemma 3 fallback — نص فقط بدون function calling
async function fallbackGemma(env, messages) {
  const apiKey = env.GEMMA_API_KEY || env.GEMINI_API_KEY;
  const url    = `https://generativelanguage.googleapis.com/v1beta/models/gemma-3-27b-it:generateContent?key=${apiKey}`;
  const simple = messages.filter(m=>!m.parts?.some(p=>p.functionCall||p.functionResponse)).map(m=>({ role:m.role, parts:m.parts.filter(p=>p.text&&!p.thought) })).filter(m=>m.parts.length);
  const resp = await fetch(url, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({ contents:simple, systemInstruction:{parts:[{text:PERSONA}]}, generationConfig:{temperature:0.4,maxOutputTokens:800} }) });
  if (!resp.ok) throw new Error(`Gemma ${resp.status}`);
  const data = await resp.json();
  return { thinking:'', text:data.candidates?.[0]?.content?.parts?.[0]?.text||'', funcCall:null };
}

// ================================================================
// SECTION 6 — AGENT LOOP
// يستبدل reactLoop + evalCode + parseAI تماماً
// ================================================================
async function agentLoop(env, userMsg, history, sendUpd, sendThink) {
  // Convert OpenAI-style history to Gemini format
  const messages = history.filter(m=>m.content).map(m=>({ role:m.role==='user'?'user':'model', parts:[{ text:m.content }] }));
  messages.push({ role:'user', parts:[{ text:userMsg }] });

  let finalText = '';

  for (let round = 0; round < 6; round++) {
    let result;
    try { result = await streamGemini(env, messages, sendThink, sendUpd); }
    catch (e) { return { text:`❌ ${e.message}`, history:[...history,{role:'user',content:userMsg},{role:'assistant',content:`❌ ${e.message}`}] }; }

    const { thinking, text, funcCall } = result;

    // أضف رد الـ model للـ history (بما فيه thinking وfunction call)
    const modelParts = [];
    if (thinking) modelParts.push({ thought:true, text:thinking });
    if (funcCall)  modelParts.push({ functionCall:funcCall });
    if (text)      modelParts.push({ text });
    if (modelParts.length) messages.push({ role:'model', parts:modelParts });

    if (funcCall) {
      const label = TOOL_LABELS[funcCall.name] || funcCall.name;
      await sendUpd(`${label}...`);

      const toolResult = await executeTool(env, funcCall.name, funcCall.args ?? {});

      messages.push({ role:'user', parts:[{ functionResponse:{ name:funcCall.name, response:toolResult } }] });
      continue;
    }

    // لا function call = الرد النهائي
    if (text) { finalText = text; break; }
  }

  if (!finalText) finalText = '❌ لم أتمكن من إتمام الطلب. حاول مرة أخرى.';

  return {
    text: finalText,
    history: [...history, { role:'user', content:userMsg }, { role:'assistant', content:finalText }],
  };
}

// ================================================================
// SECTION 7 — CONTENT OPTIMIZER
// ================================================================
async function optimizeContent(env, meta, platform) {
  const guides = { youtube:'عنوان SEO (60 حرف) + وصف + هاشتاقات', instagram:'كاشن + إيموجي + 30 هاشتاق', facebook:'عنوان عاطفي + وصف + 10 هاشتاقات', tiktok:'عنوان 30 حرف + هاشتاقات TikTok' };
  const prompt = `تسويق إسلامي. المنصة: ${platform}\n${guides[platform]||'عام'}\nعنوان: ${meta.title}\nوصف: ${meta.description}\nرد بـ JSON فقط: {"title":"...","description":"...","tags":["..."]}`;
  try {
    const resp = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-04-17:generateContent?key=${env.GEMINI_API_KEY}`, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({ contents:[{role:'user',parts:[{text:prompt}]}], generationConfig:{temperature:0.3,maxOutputTokens:512,responseMimeType:'application/json'} }) });
    if (!resp.ok) return meta;
    return JSON.parse((await resp.json()).candidates?.[0]?.content?.parts?.[0]?.text||'{}');
  } catch { return meta; }
}

// ================================================================
// SECTION 8 — FAJR CALCULATOR
// ================================================================
function calcFajr(lat, lng, date=new Date()) {
  const D2R=Math.PI/180, y=date.getFullYear(), mo=date.getMonth()+1, d=date.getDate();
  const JD=Math.floor(365.25*(y+4716))+Math.floor(30.6001*(mo+1))+d-1524.5, n=JD-2451545;
  const L=((280.460+0.9856474*n)%360+360)%360, g=((357.528+0.9856003*n)%360)*D2R;
  const lam=(L+1.915*Math.sin(g)+0.020*Math.sin(2*g))*D2R, eps=23.439*D2R;
  const dec=Math.asin(Math.sin(eps)*Math.sin(lam)), RA=Math.atan2(Math.cos(eps)*Math.sin(lam),Math.cos(lam));
  const noon=12-(lng/15)-((L*D2R-RA)*12/Math.PI)+2;
  const cosH=(Math.sin(-18*D2R)-Math.sin(lat*D2R)*Math.sin(dec))/(Math.cos(lat*D2R)*Math.cos(dec));
  if (Math.abs(cosH)>1) return null;
  const fTime=(((noon-Math.acos(cosH)*12/Math.PI)%24)+24)%24;
  const hh=Math.floor(fTime), mm=Math.floor((fTime-hh)*60);
  return { hours:hh, minutes:mm, formatted:`${pad(hh)}:${pad(mm)}` };
}
function buildSlotTimestamp(dateStr, timeStr) { return new Date(`${dateStr}T${timeStr}:00+02:00`).getTime(); }

// ================================================================
// SECTION 9 — HEALTH CHECK
// ================================================================
async function runHealthCheck(env, sendUpd) {
  const mem=await getMemory(env); const results={}, warns=[];
  if (mem.github.token) { const r=await fetch('https://api.github.com/user',{headers:ghHeaders(mem.github.token)}); results.github=r.ok?'✅ سليم':'❌ فشل'; if(!r.ok) warns.push('⚠️ GitHub token منتهي'); } else { results.github='❌ غير مُهيأ'; }
  if (mem.youtube.refresh_token&&mem.youtube.client_id) { const r=await fetch('https://oauth2.googleapis.com/token',{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded'},body:new URLSearchParams({client_id:mem.youtube.client_id,client_secret:mem.youtube.client_secret,refresh_token:mem.youtube.refresh_token,grant_type:'refresh_token'})}); results.youtube=r.ok?'✅ سليم':'❌ منتهي'; if(!r.ok) warns.push('⚠️ YouTube token منتهي'); } else { results.youtube='⚪ غير مُهيأ'; }
  if (mem.facebook.access_token) { const r=await fetch(`https://graph.facebook.com/me?access_token=${mem.facebook.access_token}`); results.facebook=r.ok?'✅ سليم':'❌ مشكلة'; results.instagram=mem.instagram.access_token?(r.ok?'✅ سليم':'⚠️ راجع Facebook'):'⚪ غير مُهيأ'; if(!r.ok) warns.push('⚠️ Facebook token تحتاج تجديد'); } else { results.facebook='⚪ غير مُهيأ'; results.instagram='⚪ غير مُهيأ'; }
  results.tiktok=mem.tiktok.access_token?'⚪ يحتاج فحص':'⚪ غير مُهيأ';
  await saveDoc(env,'health',`# تقرير الصحة\n## آخر فحص: ${new Date().toISOString()}\n\n## حالة التوكنز\n${Object.entries(results).map(([k,v])=>`- ${k}: ${v}`).join('\n')}\n\n## تحذيرات\n${warns.length?warns.map(w=>`- ${w}`).join('\n'):'- لا تحذيرات ✅'}\n`);
  if(sendUpd) await sendUpd('🏥 اكتمل فحص صحة التوكنز');
  return { results, warnings:warns };
}

// ================================================================
// SECTION 10 — PLATFORM PUBLISHERS
// ================================================================
async function ytGetAccessToken(yt) {
  const r=await fetch('https://oauth2.googleapis.com/token',{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded'},body:new URLSearchParams({client_id:yt.client_id,client_secret:yt.client_secret,refresh_token:yt.refresh_token,grant_type:'refresh_token'})});
  return r.ok?(await r.json()).access_token||null:null;
}
async function publishYouTube(env,mem,videoUrl,meta){const yt=mem.youtube;if(!yt.refresh_token)return{success:false,error:'YouTube refresh_token غير موجود'};const access=await ytGetAccessToken(yt);if(!access)return{success:false,error:'فشل تجديد YouTube token'};const optimized=await optimizeContent(env,meta,'youtube');const init=await fetch('https://www.googleapis.com/upload/youtube/v3/videos?uploadType=resumable&part=snippet,status',{method:'POST',headers:{Authorization:`Bearer ${access}`,'Content-Type':'application/json','X-Upload-Content-Type':'video/mp4'},body:JSON.stringify({snippet:{title:optimized.title||meta.title||'فيديو',description:optimized.description||'',tags:optimized.tags||[],categoryId:'27'},status:{privacyStatus:'public'}})});if(!init.ok)return{success:false,error:`YouTube init فشل: ${init.status}`};const uploadUrl=init.headers.get('Location');if(!uploadUrl)return{success:false,error:'YouTube لم يُرجع upload URL'};const vid=await fetch(videoUrl,{headers:ghHeaders(mem.github.token)});if(!vid.ok)return{success:false,error:'فشل جلب الفيديو'};const up=await fetch(uploadUrl,{method:'PUT',headers:{'Content-Type':'video/mp4','Content-Length':vid.headers.get('content-length')||''},body:vid.body,duplex:'half'});if(!up.ok)return{success:false,error:`YouTube upload فشل: ${up.status}`};const d=await up.json();return{success:true,videoId:d.id,url:`https://youtu.be/${d.id}`};}
async function publishFacebook(env,mem,videoUrl,meta){const fb=mem.facebook;if(!fb.access_token)return{success:false,error:'Facebook token غير موجود'};const o=await optimizeContent(env,meta,'facebook');const r=await fetch(`https://graph-video.facebook.com/v19.0/${fb.page_id}/videos`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({file_url:videoUrl,title:o.title||'',description:o.description||'',access_token:fb.access_token})});if(!r.ok){const e=await r.json().catch(()=>({}));return{success:false,error:e.error?.message||'Facebook فشل'};}return{success:true,videoId:(await r.json()).id};}
async function publishInstagram(env,mem,videoUrl,meta){const ig=mem.instagram;if(!ig.access_token)return{success:false,error:'Instagram token غير موجود'};const o=await optimizeContent(env,meta,'instagram');const caption=[o.title,o.description,o.tags?.map(t=>`#${t}`).join(' ')].filter(Boolean).join('\n\n');const cr=await fetch(`https://graph.facebook.com/v19.0/${ig.account_id}/media`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({media_type:'REELS',video_url:videoUrl,caption,access_token:ig.access_token})});if(!cr.ok)return{success:false,error:'Instagram container فشل'};const{id:cid}=await cr.json();await sleep(13_000);const pr=await fetch(`https://graph.facebook.com/v19.0/${ig.account_id}/media_publish`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({creation_id:cid,access_token:ig.access_token})});if(!pr.ok)return{success:false,error:'Instagram نشر فشل'};return{success:true,mediaId:(await pr.json()).id};}
async function publishTikTok(env,mem,videoUrl,meta){const tt=mem.tiktok;if(!tt.access_token)return{success:false,error:'TikTok يحتاج موافقة خاصة'};const o=await optimizeContent(env,meta,'tiktok');const r=await fetch('https://open.tiktokapis.com/v2/post/publish/video/init/',{method:'POST',headers:{Authorization:`Bearer ${tt.access_token}`,'Content-Type':'application/json'},body:JSON.stringify({post_info:{title:o.title||'',privacy_level:'PUBLIC_TO_EVERYONE',disable_duet:false,disable_stitch:false},source_info:{source:'PULL_FROM_URL',video_url:videoUrl}})});if(!r.ok){const e=await r.json().catch(()=>({}));return{success:false,error:e.error?.message||'TikTok فشل'};}return{success:true};}
async function publishVideo(env,platform,videoUrl,meta){const mem=await getMemory(env);switch(platform){case'youtube':return publishYouTube(env,mem,videoUrl,meta);case'facebook':return publishFacebook(env,mem,videoUrl,meta);case'instagram':return publishInstagram(env,mem,videoUrl,meta);case'tiktok':return publishTikTok(env,mem,videoUrl,meta);default:return{success:false,error:`منصة غير معروفة: ${platform}`};}}

// ================================================================
// SECTION 11 — DAILY PLAN BUILDER
// ================================================================
function makeDefaultSlots(sH,sM,count){let s=sH*60+sM;const e=23*60;if(s>=e)s=6*60+30;const base=Math.floor((e-s)/count),out=[];for(let i=0;i<count;i++){const t=Math.min(e-1,Math.max(s,s+i*base+(Math.floor(Math.random()*18)-9)));out.push(`${pad(Math.floor(t/60))}:${pad(t%60)}`);}return out.sort();}
async function getBestTimes(env,fH,fM,count,platforms){
  const prompt=`اقترح ${count} مواعيد نشر بعد ${pad(fH)}:${pad(fM)} (فجر القاهرة) للمنصات: ${platforms.join(', ')}\nرد بـ JSON فقط: {"slots":[{"time":"HH:MM","platform":"youtube","reason":"..."}]}`;
  try{const resp=await fetch(`https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-04-17:generateContent?key=${env.GEMINI_API_KEY}`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({contents:[{role:'user',parts:[{text:prompt}]}],generationConfig:{temperature:0.3,maxOutputTokens:512,responseMimeType:'application/json'}})});if(!resp.ok)return null;return JSON.parse((await resp.json()).candidates?.[0]?.content?.parts?.[0]?.text||'{}').slots||null;}catch{return null;}
}
async function buildDailyPlanCore(env){
  const mem=await getMemory(env),cfg=mem.settings;
  const lat=parseFloat(cfg.location_lat)||30.0444,lng=parseFloat(cfg.location_lng)||31.2357;
  const off=parseInt(cfg.fajr_offset_minutes)||30,ppd=parseInt(cfg.posts_per_day)||4;
  const fajr=calcFajr(lat,lng);if(!fajr)return{error:'تعذّر حساب وقت الفجر'};
  const platforms=['youtube','instagram','facebook','tiktok'].filter(p=>{const s=mem[p];if(!s)return false;if(s.status==='verified'||s.status==='active')return true;if(p==='youtube')return!!(s.refresh_token&&s.client_id);if(p==='instagram')return!!(s.access_token&&s.account_id);if(p==='facebook')return!!(s.access_token&&s.page_id);if(p==='tiktok')return!!(s.access_token);return false;});
  if(!platforms.length)return{error:'لا توجد منصات مُهيأة'};
  const{repo_owner,repo_name,token}=mem.github;if(!token)return{error:'GitHub token غير موجود'};
  const videos=await getPendingVideos(repo_owner,repo_name,token);if(!videos.length)return{error:'لا يوجد فيديوهات في Release pending'};
  const aiSlots=await getBestTimes(env,fajr.hours,fajr.minutes+off,ppd,platforms);
  let schedule;
  if(aiSlots?.length>=ppd){schedule=aiSlots.slice(0,ppd).map((s,i)=>({time:s.time,platform:s.platform||platforms[i%platforms.length],video:videos[i]?.base||`video_${i+1}`,videoUrl:videos[i]?.url||null,mdUrl:videos[i]?.mdUrl||null,assetId:videos[i]?.id||null,mdAssetId:videos[i]?.mdId||null,status:'pending'}));}
  else{const slots=makeDefaultSlots(fajr.hours,fajr.minutes+off,ppd);schedule=slots.map((t,i)=>({time:t,platform:platforms[i%platforms.length],video:videos[i]?.base||`video_${i+1}`,videoUrl:videos[i]?.url||null,mdUrl:videos[i]?.mdUrl||null,assetId:videos[i]?.id||null,mdAssetId:videos[i]?.mdId||null,status:'pending'}));}
  const today=new Date().toLocaleDateString('en-CA',{timeZone:'Africa/Cairo'});
  const rows=schedule.map(s=>`| ${s.time} | ${s.platform} | ${s.video} | pending | — |`).join('\n');
  await saveDoc(env,'plan',`# خطة النشر اليومية\n## التاريخ: ${today}\n## وقت الفجر: ${fajr.formatted}\n## الحالة: active\n\n## الجدول\n| الوقت | المنصة | الفيديو | الحالة | رابط |\n|-------|--------|---------|--------|------|\n${rows}\n\n## إحصائيات اليوم\n- منشور: 0\n- فاشل: 0\n- متبقي: ${schedule.length}\n`);
  await saveDoc(env,'queue',`# قائمة الفيديوهات\n## آخر تحديث: ${new Date().toISOString()}\n## إجمالي المعلق: ${videos.length}\n## إجمالي المنشور: 0\n\n## آخر منشور\n- الاسم: null\n- المنصة: null\n- التاريخ: null\n- الرابط: null\n`);
  return{schedule,today,fajr,platforms,videoCount:videos.length};
}
async function buildDailyPlanFromChat(env,sendUpd){
  await sendUpd('🏥 فحص التوكنز...');const health=await runHealthCheck(env,null);if(health.warnings.length)await sendUpd(`⚠️ ${health.warnings.join(' | ')}`);
  await sendUpd('📅 بناء الخطة...');const result=await buildDailyPlanCore(env);if(result.error)return{message:`❌ ${result.error}`};
  const{schedule,today,fajr,platforms,videoCount}=result;
  await sendUpd('🚀 إطلاق Workflow...');
  try{await env.PUBLISH_WORKFLOW.create({id:`daily-${today}`,params:{date:today,autoSchedule:false,schedule}});await sendUpd('✅ Workflow شغّال!');}catch(e){if(!e.message?.includes('already exists'))await sendUpd(`⚠️ ${e.message}`);else await sendUpd('ℹ️ Workflow موجود بالفعل');}
  const lines=schedule.map(s=>`  • ${s.time} → ${s.platform.padEnd(10)} | ${s.video}`).join('\n');
  return{message:`✅ بسم الله! خطة اليوم (${today}):\n\n${lines}\n\n🌅 الفجر: ${fajr.formatted}\n🔔 Workflow يراقب الـ slots تلقائياً\n📊 فيديوهات: ${videoCount} | منصات: ${platforms.join(', ')}`};
}

// ================================================================
// SECTION 12 — ROUTE HANDLERS
// ================================================================
async function routeChat(req, env) {
  const { message, history=[] } = await req.json();
  const { readable, writable }  = new TransformStream();
  const writer = writable.getWriter();
  const enc    = new TextEncoder();
  const push   = obj => writer.write(enc.encode(JSON.stringify(obj) + '\n'));

  (async () => {
    try {
      push({ type:'thinking_start' }); // ← bubble نابضة فوراً

      const sendUpd   = t => push({ type:'update', text:t });
      const sendThink = t => push({ type:'thinking_chunk', text:t }); // ← real-time thinking

      const result = await agentLoop(env, message, history, sendUpd, sendThink);
      push({ type:'message', text:result.text, history:result.history });
    } catch (e) {
      push({ type:'message', text:`❌ خطأ: ${e.message}` });
    } finally {
      await writer.close();
    }
  })();

  return new Response(readable, { headers:{ 'Content-Type':'text/event-stream', 'Cache-Control':'no-cache', 'Access-Control-Allow-Origin':'*' } });
}

async function routeStatus(env) {
  const mem=await getMemory(env),planDoc=await getDoc(env,'plan'),fajr=calcFajr(parseFloat(mem.settings.location_lat)||30.0444,parseFloat(mem.settings.location_lng)||31.2357);
  let wf='unknown';try{const today=new Date().toLocaleDateString('en-CA',{timeZone:'Africa/Cairo'});const inst=await env.PUBLISH_WORKFLOW.get(`daily-${today}`);wf=(await inst.status()).status;}catch{wf='no_instance_today';}
  return jsonRes({version:'5.0',executor:'Function Calling — no eval()',scheduler:'Cloudflare Workflows',memory:'Workers KV',github:mem.github?.status,platforms:{youtube:mem.youtube?.status,instagram:mem.instagram?.status,facebook:mem.facebook?.status,tiktok:mem.tiktok?.status},workflow:wf,plan_status:planDoc.includes('active')?'active':'idle',published_today:(planDoc.match(/✅ منشور/g)||[]).length,fajr:fajr?.formatted||'—'});
}

// ================================================================
// SECTION 13 — OfoqDailyWorkflow
// ================================================================
export class OfoqDailyWorkflow extends WorkflowEntrypoint {
  async run(event, step) {
    const{date,autoSchedule,fajr}=event.payload; let schedule=event.payload.schedule;
    if(autoSchedule&&fajr?.formatted){const ts=buildSlotTimestamp(date,fajr.formatted);if(ts>Date.now())await step.sleepUntil('await_fajr',new Date(ts));}
    if(!schedule?.length){schedule=await step.do('build_daily_plan',async()=>{const r=await buildDailyPlanCore(this.env);if(r.error)throw new Error(r.error);return r.schedule;});}
    if(!schedule?.length){await appendLog(this.env,'—','—','—','⚠️','لا schedule');return;}
    for(let i=0;i<schedule.length;i++){
      const slot=schedule[i],slotTs=buildSlotTimestamp(date,slot.time);
      if(slotTs>Date.now())await step.sleepUntil(`await_slot_${i}_${slot.time.replace(':','')}`,new Date(slotTs));
      await step.do(`pub_${i}_${slot.platform}_${slot.video.slice(0,15)}`,{retries:{limit:3,delay:'10 minutes',backoff:'exponential'},timeout:'10 minutes'},async()=>{
        const mem=await getMemory(this.env),{repo_owner,repo_name,token}=mem.github;
        const videos=await getPendingVideos(repo_owner,repo_name,token);
        const video=videos.find(v=>v.base===slot.video)||(slot.videoUrl?{base:slot.video,url:slot.videoUrl,mdUrl:slot.mdUrl,id:slot.assetId,mdId:slot.mdAssetId}:null);
        if(!video)throw new Error(`فيديو ${slot.video} غير موجود`);
        const meta=await readVideoMeta(video.mdUrl||slot.mdUrl,token);
        const result=await publishVideo(this.env,slot.platform,video.url||slot.videoUrl,meta);
        if(!result.success)throw new Error(result.error||'فشل النشر');
        if(video.id)await removeFromPending(repo_owner,repo_name,token,video);
        let p=await getDoc(this.env,'plan');p=p.replace(new RegExp(`\\| ${slot.video.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')} \\| pending \\|`),`| ${slot.video} | ✅ منشور |`).replace(/- منشور: (\d+)/,(_,n)=>`- منشور: ${parseInt(n)+1}`).replace(/- متبقي: (\d+)/,(_,n)=>`- متبقي: ${Math.max(0,parseInt(n)-1)}`);await saveDoc(this.env,'plan',p);
        let q=await getDoc(this.env,'queue');q=q.replace('- الاسم: null',`- الاسم: ${slot.video}`).replace('- المنصة: null',`- المنصة: ${slot.platform}`).replace('- التاريخ: null',`- التاريخ: ${new Date().toISOString()}`).replace('- الرابط: null',`- الرابط: ${result.url||result.videoId||'ok'}`);await saveDoc(this.env,'queue',q);
        await appendLog(this.env,slot.time,slot.platform,slot.video,'✅',result.url||result.videoId||'ok');
        return{url:result.url,videoId:result.videoId};
      });
    }
    await step.do('schedule_tomorrow',async()=>{
      const tom=new Date();tom.setDate(tom.getDate()+1);const td=tom.toLocaleDateString('en-CA',{timeZone:'Africa/Cairo'});const mem=await getMemory(this.env),cfg=mem.settings;const tf=calcFajr(parseFloat(cfg.location_lat)||30.0444,parseFloat(cfg.location_lng)||31.2357,tom);
      try{await this.env.PUBLISH_WORKFLOW.create({id:`daily-${td}`,params:{date:td,autoSchedule:true,fajr:tf}});await appendLog(this.env,'—','—','—','📅',`Workflow الغد (${td}) مُجدول`);}catch(e){if(!e.message?.includes('already exists'))throw e;}
    });
  }
}

// ================================================================
// SECTION 14 — MAIN EXPORT
// ================================================================
export default {
  async fetch(req, env) {
    if(req.method==='OPTIONS')return new Response(null,{headers:{'Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'GET,POST,OPTIONS','Access-Control-Allow-Headers':'Content-Type'}});
    const url=new URL(req.url),path=url.pathname;
    if(path==='/chat'   &&req.method==='POST')return routeChat(req,env);
    if(path==='/status')                      return routeStatus(env);
    if(path==='/debug/memory')                return jsonRes({memory:await getMemory(env)});
    if(path==='/workflow-status'){const today=new Date().toLocaleDateString('en-CA',{timeZone:'Africa/Cairo'});try{const inst=await env.PUBLISH_WORKFLOW.get(`daily-${today}`);return jsonRes({instanceId:`daily-${today}`,status:await inst.status()});}catch(e){return jsonRes({error:`لا يوجد Workflow: ${e.message}`},404);}}
    if(path==='/trigger'&&req.method==='POST'){const{date,force}=await req.json().catch(()=>({}));const today=date||new Date().toLocaleDateString('en-CA',{timeZone:'Africa/Cairo'});const result=await buildDailyPlanCore(env);if(result.error)return jsonRes({ok:false,error:result.error},400);try{if(force){try{await(await env.PUBLISH_WORKFLOW.get(`daily-${today}`)).terminate();}catch{}}const inst=await env.PUBLISH_WORKFLOW.create({id:`daily-${today}`,params:{date:today,autoSchedule:false,schedule:result.schedule}});return jsonRes({ok:true,instanceId:inst.id,slots:result.schedule.length});}catch(e){return jsonRes({ok:false,error:e.message},400);}}
    return new Response('Not Found',{status:404});
  },
  async scheduled(controller, env) {
    const today=new Date().toLocaleDateString('en-CA',{timeZone:'Africa/Cairo'});
    const mem=await getMemory(env),cfg=mem.settings;
    const fajr=calcFajr(parseFloat(cfg.location_lat)||30.0444,parseFloat(cfg.location_lng)||31.2357);
    try{const inst=await env.PUBLISH_WORKFLOW.create({id:`daily-${today}`,params:{date:today,autoSchedule:false,fajr}});console.log(`[OFOQ Cron] Created: ${inst.id}`);await appendLog(env,'—','cron',today,'📅','Workflow created by cron');}
    catch(e){if(!e.message?.includes('already exists')){console.error('[OFOQ Cron]',e.message);await appendLog(env,'—','cron',today,'❌',`cron error: ${e.message}`);}}
  },
};

// UTILITIES
function pad(n){return String(n).padStart(2,'0');}
function sleep(ms){return new Promise(r=>setTimeout(r,ms));}
function jsonRes(obj,status=200){return new Response(JSON.stringify(obj),{status,headers:{'Content-Type':'application/json','Access-Control-Allow-Origin':'*'}});}
