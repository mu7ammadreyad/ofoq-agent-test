// ================================================================
//   ██████╗ ███████╗ ██████╗  ██████╗     ███████╗ ██████╗ ██╗
//  ██╔═══██╗██╔════╝██╔═══██╗██╔═══██╗   ██╔════╝██╔════╝ ██║
//  ██║   ██║█████╗  ██║   ██║██║   ██║   █████╗  ██║  ███╗██║
//  ██║   ██║██╔══╝  ██║   ██║██║▄▄ ██║   ██╔══╝  ██║   ██║██║
//  ╚██████╔╝██║     ╚██████╔╝╚██████╔╝   ███████╗╚██████╔╝███████╗
//   ╚═════╝ ╚═╝      ╚═════╝  ╚══▀▀═╝   ╚══════╝ ╚═════╝ ╚══════╝
//
//  OFOQ Publisher Agent v4.0 — بسم الله الرحمن الرحيم
//  ─────────────────────────────────────────────────────────────
//  العقل    : Cloudflare Worker + Gemini 2.5 Flash
//  Fallback  : Gemma 3 (عند الخطأ أو Rate Limit)
//  التنفيذ  : eval() محلي داخل الـ Worker (<100ms)
//  الذاكرة  : Workers KV (tokens, settings, logs)
//  الـ Cron  : Cloudflare Workflows + step.sleepUntil()
//  الـ Static: Workers Assets (index.html)
//  الفيديوهات: GitHub Releases (pending / published)
// ================================================================

import { WorkflowEntrypoint } from 'cloudflare:workers';

// ================================================================
// SECTION 0 — DEFAULTS
// ================================================================
const DEFAULT_MEM = {
  github:   { repo_owner: null, repo_name: null, token: null, status: 'not_configured', last_verified: null, pending_count: 0 },
  youtube:  { client_id: null, client_secret: null, refresh_token: null, access_token: null, channel_id: null, channel_name: null, status: 'not_configured', last_verified: null, token_expires: null },
  instagram:{ access_token: null, account_id: null, account_name: null, status: 'not_configured', last_verified: null, token_expires: null },
  facebook: { access_token: null, page_id: null, page_name: null, status: 'not_configured', last_verified: null, token_expires: null },
  tiktok:   { access_token: null, open_id: null, status: 'not_configured', last_verified: null },
  settings: { timezone: 'Africa/Cairo', location_lat: '30.0444', location_lng: '31.2357', fajr_offset_minutes: '30', posts_per_day: '4' },
};

const DEFAULT_DOCS = {
  plan:   '# خطة النشر اليومية\n## الحالة: idle\n## الجدول\n| الوقت | المنصة | الفيديو | الحالة | رابط |\n|-------|--------|---------|--------|------|\n\n## إحصائيات اليوم\n- منشور: 0\n- فاشل: 0\n- متبقي: 0\n',
  log:    '# سجل العمليات\n## آخر تحديث: null\n\n## العمليات (آخر 50)\n',
  queue:  '# قائمة الفيديوهات\n## آخر تحديث: null\n## إجمالي المعلق: 0\n\n## آخر منشور\n- الاسم: null\n- المنصة: null\n- التاريخ: null\n- الرابط: null\n',
  health: '# تقرير الصحة\n## آخر فحص: null\n\n## حالة التوكنز\n- GitHub: null\n- YouTube: null\n- Instagram: null\n- Facebook: null\n- TikTok: null\n\n## تحذيرات\n- لا تحذيرات حالياً\n',
};

// persona مدمجة في الكود (ثابتة — لا تحتاج KV)
const PERSONA = `
# أفق — OFOQ Publishing Agent v4.0
## الهوية
- الاسم: أفق | المهمة: نشر المحتوى الإسلامي تلقائياً | اللغة: عربي (مصري ودود)

## الشخصية
- أتكلم بوضوح، أشرح كل خطوة، أطلب تأكيداً قبل العمليات الحساسة
- أبدأ المهام المهمة بـ "بسم الله" | ردودي مختصرة وعملية

## منهج التفكير — ReAct + Plan-and-Solve
1. PERCEIVE  → اقرأ الذاكرة وافهم الحالة
2. REASON    → فكر بصوت عالٍ
3. PLAN      → ضع خطة مرتبة
4. ACT       → نفّذ (CODE أو MEMORY_UPDATE أو PLAN)
5. OBSERVE   → استقبل النتيجة
6. REFLECT   → هل نجحنا؟
7. UPDATE    → حدّث الذاكرة

## شكل الـ JSON Response (دائماً JSON نظيف فقط)
{"action":"SPEAK|CODE|MEMORY_UPDATE|PLAN|HEALTH_CHECK|PENDING_PREVIEW","thinking":"...","message":"...","code":"...","code_purpose":"...","memory_updates":[{"section":"...","data":{}}],"needs_followup":false,"retry_on_fail":false}

## بيئة تنفيذ الكود (eval محلي في Worker — v4.0)
- الكود يُنفَّذ داخل Cloudflare Worker مباشرة
- استخدم __mem للوصول للذاكرة (tokens, settings)
- fetch() متاح عالمياً
- لا require/import | لا process.env | لا setTimeout طويل
- النتيجة: return { success: bool, data: any, error: string|null }
- مثال: const r = await fetch('https://api.github.com/user', {headers:{Authorization:'token '+__mem.github.token}}); return {success:r.ok, data:(await r.json()).login};

## قواعد JSON صارمة
- JSON صالح فقط — لا نص قبله ولا بعده | لا markdown | لا تعليقات
- message بدون newlines خام — استخدم \\n

## القيم: لا نشر محتوى غير إسلامي | لا كلمات مرور | شفافية كاملة
`;

// ================================================================
// SECTION 1 — KV MEMORY ENGINE
// ================================================================
async function getMemory(env) {
  const raw = await env.OFOQ_KV.get('memory');
  if (!raw) return JSON.parse(JSON.stringify(DEFAULT_MEM));
  try { return JSON.parse(raw); } catch { return JSON.parse(JSON.stringify(DEFAULT_MEM)); }
}

async function saveMemory(env, mem) {
  await env.OFOQ_KV.put('memory', JSON.stringify(mem));
}

async function memWrite(env, section, data) {
  const mem = await getMemory(env);
  if (!mem[section]) mem[section] = {};
  Object.assign(mem[section], data);
  await saveMemory(env, mem);
}

async function getDoc(env, key) {
  const raw = await env.OFOQ_KV.get(`doc_${key}`);
  return raw || DEFAULT_DOCS[key] || '';
}

async function saveDoc(env, key, content) {
  await env.OFOQ_KV.put(`doc_${key}`, content);
}

async function appendLog(env, time, platform, video, status, detail) {
  let log = await getDoc(env, 'log');
  const ts  = new Date().toLocaleString('ar-EG', { timeZone: 'Africa/Cairo' });
  const row = `\n- [${ts}] | ${time} | ${platform} | ${video} | ${status} | ${detail}`;
  const lines = (log + row).split('\n').filter(l => l.startsWith('- '));
  const header = `# سجل العمليات\n## آخر تحديث: ${new Date().toISOString()}\n\n## العمليات (آخر 50)`;
  await saveDoc(env, 'log', header + '\n' + lines.slice(-50).join('\n'));
}

async function buildSystemPrompt(env) {
  const mem    = await getMemory(env);
  const plan   = await getDoc(env, 'plan');
  const log    = await getDoc(env, 'log');
  const health = await getDoc(env, 'health');
  const queue  = await getDoc(env, 'queue');
  const now    = new Date().toLocaleString('ar-EG', { timeZone: 'Africa/Cairo' });
  const memText = Object.entries(mem).map(([s, d]) =>
    `## ${s}\n\`\`\`\n${Object.entries(d).map(([k, v]) => `${k}: ${v ?? 'null'}`).join('\n')}\n\`\`\``
  ).join('\n\n');
  return [
    PERSONA,
    `\n## الذاكرة الحالية:\n${memText}`,
    `\n## خطة اليوم:\n${plan}`,
    `\n## آخر السجل:\n${log.slice(-500)}`,
    `\n## صحة التوكنز:\n${health}`,
    `\n## قائمة الفيديوهات:\n${queue}`,
    `\nالوقت الآن (القاهرة): ${now}`,
  ].join('\n');
}

// ================================================================
// SECTION 2 — GITHUB API (للفيديوهات فقط)
// ================================================================
function ghHeaders(token) {
  return { Authorization: `token ${token}`, Accept: 'application/vnd.github.v3+json', 'Content-Type': 'application/json', 'User-Agent': 'OFOQ-Agent/4.0' };
}

async function ghFetch(method, path, token, body = null) {
  const r = await fetch(`https://api.github.com${path}`, {
    method, headers: ghHeaders(token), body: body ? JSON.stringify(body) : null,
  });
  let data; try { data = await r.json(); } catch { data = {}; }
  return { ok: r.ok, status: r.status, data };
}

async function getPendingVideos(owner, repo, token) {
  const { ok, data: releases } = await ghFetch('GET', `/repos/${owner}/${repo}/releases`, token);
  if (!ok) return [];
  const rel = releases.find(r => r.tag_name === 'pending');
  if (!rel) return [];
  const { data: assets } = await ghFetch('GET', `/repos/${owner}/${repo}/releases/${rel.id}/assets`, token);
  if (!Array.isArray(assets)) return [];
  const videoRe = /\.(mp4|mov|avi|mkv|webm)$/i;
  return assets
    .filter(a => videoRe.test(a.name))
    .map(a => {
      const base = a.name.replace(/\.[^.]+$/, '');
      const md   = assets.find(x => x.name === `${base}.md`);
      return { id: a.id, name: a.name, base, url: a.browser_download_url, size: a.size, mdId: md?.id || null, mdUrl: md?.browser_download_url || null, releaseId: rel.id };
    });
}

async function readVideoMeta(mdUrl, token) {
  if (!mdUrl) return { title: '', description: '', tags: [] };
  const r = await fetch(mdUrl, { headers: ghHeaders(token) });
  if (!r.ok) return { title: '', description: '', tags: [] };
  const text = await r.text();
  const meta = { title: '', description: '', tags: [] };
  let inDesc = false;
  for (const line of text.split('\n')) {
    if      (line.startsWith('# '))                         { meta.title = line.slice(2).trim(); inDesc = false; }
    else if (/^## (وصف|description)/i.test(line))          { inDesc = true; }
    else if (/^## /.test(line))                             { inDesc = false; }
    else if (/^(tags|وسوم|هاشتاقات)\s*:/i.test(line))      { meta.tags = line.split(':')[1]?.split(/[,،]/).map(t => t.trim().replace(/^#/, '')).filter(Boolean) || []; }
    else if (inDesc && line.trim())                         { meta.description += (meta.description ? '\n' : '') + line.trim(); }
  }
  return meta;
}

async function deleteReleaseAsset(owner, repo, token, assetId) {
  const r = await fetch(`https://api.github.com/repos/${owner}/${repo}/releases/assets/${assetId}`, {
    method: 'DELETE', headers: ghHeaders(token),
  });
  return r.ok;
}

async function removeFromPending(owner, repo, token, video) {
  for (const id of [video.id, video.mdId].filter(Boolean)) {
    await deleteReleaseAsset(owner, repo, token, id);
  }
}

// ================================================================
// SECTION 3 — eval() CODE EXECUTOR
// ================================================================
async function evalCode(env, code, purposeLabel, sendUpd) {
  await sendUpd(`⚙️ تنفيذ: ${purposeLabel}`);
  const mem = await getMemory(env);
  try {
    const wrapped = `
      const __mem  = arguments[0];
      const fetch  = arguments[1];
      return (async () => { ${code} })();
    `;
    const fn = new Function(wrapped); // eslint-disable-line no-new-func
    const result = await Promise.race([
      fn(mem, globalThis.fetch),
      new Promise((_, rej) => setTimeout(() => rej(new Error('انتهت مهلة التنفيذ (25s)')), 25_000)),
    ]);
    return { success: true, data: result ?? null };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

// ================================================================
// SECTION 4 — AI ENGINE (Gemini + Gemma Fallback)
// ================================================================
async function callAI(env, messages, systemPrompt, useGemma = false) {
  const model  = useGemma ? (env.GEMMA_MODEL  || 'gemma-3-27b-it')               : (env.GEMINI_MODEL || 'gemini-2.5-flash-preview-04-17');
  const apiKey = useGemma ? (env.GEMMA_API_KEY || env.GEMINI_API_KEY)             : env.GEMINI_API_KEY;
  const base   = env.GEMINI_BASE || 'https://generativelanguage.googleapis.com/v1beta';
  const url    = `${base}/models/${model}:generateContent?key=${apiKey}`;

  const body = {
    contents: messages.map(m => ({ role: m.role === 'assistant' ? 'model' : 'user', parts: [{ text: m.content }] })),
    generationConfig: { temperature: 0.7, maxOutputTokens: 4096, topP: 0.9 },
  };
  if (systemPrompt) body.systemInstruction = { parts: [{ text: systemPrompt }] };

  let resp;
  try {
    resp = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  } catch (e) {
    if (!useGemma) { await sleep(800); return callAI(env, messages, systemPrompt, true); }
    throw e;
  }

  if (!resp.ok) {
    if ((resp.status === 429 || resp.status === 503 || resp.status === 500) && !useGemma) {
      return callAI(env, messages, systemPrompt, true);
    }
    const err = await resp.json().catch(() => ({}));
    throw new Error(`AI ${resp.status}: ${JSON.stringify(err).slice(0, 200)}`);
  }

  const data = await resp.json();
  return data.candidates?.[0]?.content?.parts?.[0]?.text || '';
}

function parseAI(text) {
  if (!text) return { action: 'SPEAK', message: '' };
  const m1 = text.match(/\{[\s\S]*\}/);
  if (m1) { try { const p = JSON.parse(m1[0]); if (p?.message) return p; } catch {} }
  const m2 = text.match(/```(?:json)?([\s\S]*?)```/);
  if (m2) { try { const p = JSON.parse(m2[1].trim()); if (p?.message) return p; } catch {} }
  const mMsg = text.match(/"message"\s*:\s*"([\s\S]*?)(?:"\s*[,}]|"\s*$)/m);
  if (mMsg) {
    return {
      action:  text.match(/"action"\s*:\s*"([^"]+)"/)?.[1] || 'SPEAK',
      thinking:text.match(/"thinking"\s*:\s*"([^"]+)"/)?.[1] || '',
      message: mMsg[1].replace(/\\n/g, '\n'),
    };
  }
  return { action: 'SPEAK', message: text.replace(/```json[\s\S]*?```/g, '').replace(/[{}]/g, '').trim() || text };
}

// ================================================================
// SECTION 5 — CONTENT OPTIMIZER
// ================================================================
async function optimizeContent(env, meta, platform) {
  const guides = {
    youtube:   'عنوان SEO (60 حرف) + وصف مفصل + هاشتاقات YouTube',
    instagram: 'كاشن جذاب + إيموجي + 30 هاشتاق + CTA',
    facebook:  'عنوان عاطفي + وصف + 10 هاشتاقات',
    tiktok:    'عنوان 30 حرف + هاشتاقات TikTok',
  };
  const prompt = `أنت خبير تسويق محتوى إسلامي. المنصة: ${platform}\nالإرشادات: ${guides[platform]||'عام'}\nالعنوان: ${meta.title}\nالوصف: ${meta.description}\nالهاشتاقات: ${meta.tags?.join(', ')}\nأنشئ نسخة مُحسَّنة. رد بـ JSON فقط: {"title":"...","description":"...","tags":["..."]}`;
  try {
    const raw = await callAI(env, [{ role: 'user', content: prompt }], null);
    return JSON.parse(raw.replace(/```json|```/g, '').trim());
  } catch { return meta; }
}

// ================================================================
// SECTION 6 — FAJR CALCULATOR
// ================================================================
function calcFajr(lat, lng, date = new Date()) {
  const D2R = Math.PI / 180;
  const y = date.getFullYear(), mo = date.getMonth() + 1, d = date.getDate();
  const JD  = Math.floor(365.25 * (y + 4716)) + Math.floor(30.6001 * (mo + 1)) + d - 1524.5;
  const n   = JD - 2451545.0;
  const L   = ((280.460 + 0.9856474 * n) % 360 + 360) % 360;
  const g   = ((357.528 + 0.9856003 * n) % 360) * D2R;
  const lam = (L + 1.915 * Math.sin(g) + 0.020 * Math.sin(2 * g)) * D2R;
  const eps = 23.439 * D2R;
  const dec = Math.asin(Math.sin(eps) * Math.sin(lam));
  const RA  = Math.atan2(Math.cos(eps) * Math.sin(lam), Math.cos(lam));
  const EqT = (L * D2R - RA) * 12 / Math.PI;
  const noon = 12 - lng / 15 - EqT + 2; // UTC+2
  const fAng = 18 * D2R;
  const cosH = (Math.sin(-fAng) - Math.sin(lat * D2R) * Math.sin(dec)) / (Math.cos(lat * D2R) * Math.cos(dec));
  if (Math.abs(cosH) > 1) return null;
  const H     = Math.acos(cosH) * 12 / Math.PI;
  const fTime = (((noon - H) % 24) + 24) % 24;
  const hh    = Math.floor(fTime);
  const mm    = Math.floor((fTime - hh) * 60);
  return { hours: hh, minutes: mm, formatted: `${pad(hh)}:${pad(mm)}` };
}

function buildSlotTimestamp(dateStr, timeStr) {
  // Egypt = UTC+2 ثابت (لا DST منذ 2011)
  return new Date(`${dateStr}T${timeStr}:00+02:00`).getTime();
}

// ================================================================
// SECTION 7 — BEST TIME AI
// ================================================================
async function getBestTimes(env, fajrHour, fajrMin, count, platforms) {
  const logSample = (await getDoc(env, 'log')).slice(-1200);
  const prompt = `أنت خبير منصات التواصل. وقت الفجر: ${pad(fajrHour)}:${pad(fajrMin)} (القاهرة)\nالمنصات: ${platforms.join(', ')} | عدد المنشورات: ${count}\nسجل الأداء:\n${logSample || 'لا يوجد — استخدم أفضل المواعيد العامة'}\n\nاقترح ${count} مواعيد ذكية بعد الفجر. المواعيد تبدأ بعد الفجر بـ 30 دقيقة وموزعة على اليوم.\nرد بـ JSON فقط: {"slots":[{"time":"HH:MM","platform":"youtube","reason":"..."}]}`;
  try {
    const raw  = await callAI(env, [{ role: 'user', content: prompt }], null);
    const json = JSON.parse(raw.replace(/```json|```/g, '').trim());
    return json.slots || null;
  } catch { return null; }
}

// ================================================================
// SECTION 8 — HEALTH CHECK
// ================================================================
async function runHealthCheck(env, sendUpd) {
  const mem = await getMemory(env); const results = {}; const warns = [];

  if (mem.github.token) {
    const r = await fetch('https://api.github.com/user', { headers: ghHeaders(mem.github.token) });
    results.github = r.ok ? '✅ سليم' : '❌ فشل';
    if (!r.ok) warns.push('⚠️ GitHub token منتهي أو خاطئ');
  } else { results.github = '❌ غير مُهيأ'; }

  if (mem.youtube.refresh_token && mem.youtube.client_id) {
    const r = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({ client_id: mem.youtube.client_id, client_secret: mem.youtube.client_secret, refresh_token: mem.youtube.refresh_token, grant_type: 'refresh_token' }),
    });
    results.youtube = r.ok ? '✅ سليم' : '❌ Token منتهي';
    if (!r.ok) warns.push('⚠️ YouTube refresh_token منتهي');
  } else { results.youtube = '⚪ غير مُهيأ'; }

  if (mem.facebook.access_token) {
    const r = await fetch(`https://graph.facebook.com/me?access_token=${mem.facebook.access_token}`);
    results.facebook  = r.ok ? '✅ سليم' : '❌ Token مشكلة';
    results.instagram = mem.instagram.access_token ? (r.ok ? '✅ سليم' : '⚠️ راجع Facebook') : '⚪ غير مُهيأ';
    if (!r.ok) warns.push('⚠️ Facebook token تحتاج تجديد');
  } else { results.facebook = '⚪ غير مُهيأ'; results.instagram = '⚪ غير مُهيأ'; }

  results.tiktok = mem.tiktok.access_token ? '⚪ يحتاج فحص يدوي' : '⚪ غير مُهيأ';

  const healthDoc = `# تقرير الصحة\n## آخر فحص: ${new Date().toISOString()}\n\n## حالة التوكنز\n- GitHub: ${results.github}\n- YouTube: ${results.youtube}\n- Instagram: ${results.instagram}\n- Facebook: ${results.facebook}\n- TikTok: ${results.tiktok}\n\n## تحذيرات\n${warns.length ? warns.map(w => `- ${w}`).join('\n') : '- لا تحذيرات ✅'}\n`;
  await saveDoc(env, 'health', healthDoc);
  if (sendUpd) await sendUpd('🏥 اكتمل فحص صحة التوكنز');
  return { results, warnings: warns };
}

// ================================================================
// SECTION 9 — PLATFORM PUBLISHERS
// ================================================================
async function ytGetAccessToken(yt) {
  const r = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ client_id: yt.client_id, client_secret: yt.client_secret, refresh_token: yt.refresh_token, grant_type: 'refresh_token' }),
  });
  if (!r.ok) return null;
  return (await r.json()).access_token || null;
}

async function publishYouTube(env, mem, videoUrl, meta) {
  const yt = mem.youtube;
  if (!yt.refresh_token) return { success: false, error: 'YouTube refresh_token غير موجود' };
  const access = await ytGetAccessToken(yt);
  if (!access) return { success: false, error: 'فشل تجديد YouTube token' };
  const optimized = await optimizeContent(env, meta, 'youtube');
  const init = await fetch('https://www.googleapis.com/upload/youtube/v3/videos?uploadType=resumable&part=snippet,status', {
    method: 'POST',
    headers: { Authorization: `Bearer ${access}`, 'Content-Type': 'application/json', 'X-Upload-Content-Type': 'video/mp4' },
    body: JSON.stringify({ snippet: { title: optimized.title || meta.title || 'فيديو', description: optimized.description || '', tags: optimized.tags || [], categoryId: '27' }, status: { privacyStatus: 'public' } }),
  });
  if (!init.ok) return { success: false, error: `YouTube init فشل: ${init.status}` };
  const uploadUrl = init.headers.get('Location');
  if (!uploadUrl) return { success: false, error: 'YouTube لم يُرجع upload URL' };
  const vidResp = await fetch(videoUrl, { headers: ghHeaders(mem.github.token) });
  if (!vidResp.ok) return { success: false, error: 'فشل جلب الفيديو من GitHub' };
  const up = await fetch(uploadUrl, { method: 'PUT', headers: { 'Content-Type': 'video/mp4', 'Content-Length': vidResp.headers.get('content-length') || '' }, body: vidResp.body, duplex: 'half' });
  if (!up.ok) return { success: false, error: `YouTube upload فشل: ${up.status}` };
  const d = await up.json();
  return { success: true, videoId: d.id, url: `https://youtu.be/${d.id}` };
}

async function publishFacebook(env, mem, videoUrl, meta) {
  const fb = mem.facebook;
  if (!fb.access_token) return { success: false, error: 'Facebook token غير موجود' };
  const optimized = await optimizeContent(env, meta, 'facebook');
  const r = await fetch(`https://graph-video.facebook.com/v19.0/${fb.page_id}/videos`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ file_url: videoUrl, title: optimized.title || '', description: optimized.description || '', access_token: fb.access_token }),
  });
  if (!r.ok) { const e = await r.json().catch(() => ({})); return { success: false, error: e.error?.message || 'Facebook فشل' }; }
  const d = await r.json();
  return { success: true, videoId: d.id };
}

async function publishInstagram(env, mem, videoUrl, meta) {
  const ig = mem.instagram;
  if (!ig.access_token) return { success: false, error: 'Instagram token غير موجود' };
  const optimized = await optimizeContent(env, meta, 'instagram');
  const caption   = [optimized.title, optimized.description, optimized.tags?.map(t => `#${t}`).join(' ')].filter(Boolean).join('\n\n');
  const cr = await fetch(`https://graph.facebook.com/v19.0/${ig.account_id}/media`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ media_type: 'REELS', video_url: videoUrl, caption, access_token: ig.access_token }),
  });
  if (!cr.ok) return { success: false, error: 'Instagram container فشل' };
  const { id: cid } = await cr.json();
  await sleep(13_000); // Instagram processing time
  const pr = await fetch(`https://graph.facebook.com/v19.0/${ig.account_id}/media_publish`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ creation_id: cid, access_token: ig.access_token }),
  });
  if (!pr.ok) return { success: false, error: 'Instagram نشر فشل' };
  const d = await pr.json();
  return { success: true, mediaId: d.id };
}

async function publishTikTok(env, mem, videoUrl, meta) {
  const tt = mem.tiktok;
  if (!tt.access_token) return { success: false, error: 'TikTok يحتاج موافقة خاصة' };
  const optimized = await optimizeContent(env, meta, 'tiktok');
  const r = await fetch('https://open.tiktokapis.com/v2/post/publish/video/init/', {
    method: 'POST',
    headers: { Authorization: `Bearer ${tt.access_token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ post_info: { title: optimized.title || '', privacy_level: 'PUBLIC_TO_EVERYONE', disable_duet: false, disable_stitch: false }, source_info: { source: 'PULL_FROM_URL', video_url: videoUrl } }),
  });
  if (!r.ok) { const e = await r.json().catch(() => ({})); return { success: false, error: e.error?.message || 'TikTok فشل' }; }
  return { success: true };
}

async function publishVideo(env, platform, videoUrl, meta) {
  const mem = await getMemory(env);
  switch (platform) {
    case 'youtube':   return publishYouTube(env, mem, videoUrl, meta);
    case 'facebook':  return publishFacebook(env, mem, videoUrl, meta);
    case 'instagram': return publishInstagram(env, mem, videoUrl, meta);
    case 'tiktok':    return publishTikTok(env, mem, videoUrl, meta);
    default:          return { success: false, error: `منصة غير معروفة: ${platform}` };
  }
}

// ================================================================
// SECTION 10 — DAILY PLAN BUILDER (للـ Chat + الـ Workflow)
// ================================================================
function makeDefaultSlots(startH, startM, count) {
  let totalStart = startH * 60 + startM;
  const totalEnd = 23 * 60;
  if (totalStart >= totalEnd) totalStart = 6 * 60 + 30;
  const span = totalEnd - totalStart;
  const base = Math.floor(span / count);
  const out  = [];
  for (let i = 0; i < count; i++) {
    const jitter = Math.floor(Math.random() * 18) - 9;
    const total  = Math.min(totalEnd - 1, Math.max(totalStart, totalStart + i * base + jitter));
    out.push(`${pad(Math.floor(total / 60))}:${pad(total % 60)}`);
  }
  return out.sort();
}

// هذه الدالة تُستخدم في Chat و في الـ Workflow على حدٍّ سواء
async function buildDailyPlanCore(env) {
  const mem      = await getMemory(env);
  const cfg      = mem.settings;
  const lat      = parseFloat(cfg.location_lat)      || 30.0444;
  const lng      = parseFloat(cfg.location_lng)      || 31.2357;
  const off      = parseInt(cfg.fajr_offset_minutes) || 30;
  const ppd      = parseInt(cfg.posts_per_day)       || 4;
  const fajr     = calcFajr(lat, lng);
  if (!fajr) return { error: 'تعذّر حساب وقت الفجر' };

  const platforms = ['youtube', 'instagram', 'facebook', 'tiktok'].filter(p => mem[p]?.status === 'verified');
  if (!platforms.length) return { error: 'لا توجد منصات مُفعّلة' };

  const { repo_owner, repo_name, token } = mem.github;
  if (!token) return { error: 'مفتاح GitHub غير موجود' };

  const videos = await getPendingVideos(repo_owner, repo_name, token);
  if (!videos.length) return { error: 'لا يوجد فيديوهات في Release pending' };

  const aiSlots = await getBestTimes(env, fajr.hours, fajr.minutes + off, ppd, platforms);
  let schedule;

  if (aiSlots?.length >= ppd) {
    schedule = aiSlots.slice(0, ppd).map((s, i) => ({
      time: s.time, platform: s.platform || platforms[i % platforms.length],
      video: videos[i]?.base || `video_${i+1}`, videoUrl: videos[i]?.url || null,
      mdUrl: videos[i]?.mdUrl || null, assetId: videos[i]?.id || null,
      mdAssetId: videos[i]?.mdId || null, status: 'pending',
    }));
  } else {
    const slots = makeDefaultSlots(fajr.hours, fajr.minutes + off, ppd);
    schedule = slots.map((t, i) => ({
      time: t, platform: platforms[i % platforms.length],
      video: videos[i]?.base || `video_${i+1}`, videoUrl: videos[i]?.url || null,
      mdUrl: videos[i]?.mdUrl || null, assetId: videos[i]?.id || null,
      mdAssetId: videos[i]?.mdId || null, status: 'pending',
    }));
  }

  const today   = new Date().toLocaleDateString('en-CA', { timeZone: 'Africa/Cairo' });
  const rows    = schedule.map(s => `| ${s.time} | ${s.platform} | ${s.video} | pending | — |`).join('\n');
  const planDoc = `# خطة النشر اليومية\n## التاريخ: ${today}\n## وقت الفجر: ${fajr.formatted}\n## تاريخ الإنشاء: ${new Date().toISOString()}\n## الحالة: active\n\n## الجدول\n| الوقت | المنصة | الفيديو | الحالة | رابط |\n|-------|--------|---------|--------|------|\n${rows}\n\n## إحصائيات اليوم\n- منشور: 0\n- فاشل: 0\n- متبقي: ${schedule.length}\n`;

  await saveDoc(env, 'plan', planDoc);
  await saveDoc(env, 'queue', `# قائمة الفيديوهات\n## آخر تحديث: ${new Date().toISOString()}\n## إجمالي المعلق: ${videos.length}\n## إجمالي المنشور: 0\n\n## آخر منشور\n- الاسم: null\n- المنصة: null\n- التاريخ: null\n- الرابط: null\n`);

  return { schedule, today, fajr, platforms, videoCount: videos.length };
}

// نسخة Chat: تُعيد رسالة مقروءة + تُطلق الـ Workflow
async function buildDailyPlanFromChat(env, sendUpd) {
  await sendUpd('🏥 جارٍ فحص التوكنز...');
  const health = await runHealthCheck(env, null);
  if (health.warnings.length) await sendUpd(`⚠️ ${health.warnings.join(' | ')}`);

  await sendUpd('📅 جارٍ بناء الخطة...');
  const result = await buildDailyPlanCore(env);
  if (result.error) return { message: `❌ ${result.error}` };

  const { schedule, today, fajr, platforms, videoCount } = result;

  // أطلق الـ Workflow instance
  await sendUpd('🚀 جارٍ إطلاق Workflow instance...');
  try {
    await env.PUBLISH_WORKFLOW.create({
      id:     `daily-${today}`,
      params: { date: today, autoSchedule: false, schedule },
    });
    await sendUpd('✅ Workflow شغّال — سينشر كل slot تلقائياً!');
  } catch (e) {
    if (e.message?.includes('already exists')) {
      await sendUpd('ℹ️ Workflow لهذا اليوم موجود بالفعل');
    } else {
      await sendUpd(`⚠️ فشل إطلاق Workflow: ${e.message}`);
    }
  }

  const lines = schedule.map(s => `  • ${s.time} → ${s.platform.padEnd(10)} | ${s.video}`).join('\n');
  return {
    message: `✅ بسم الله! خطة اليوم (${today}):\n\n${lines}\n\n🌅 الفجر: ${fajr.formatted}\n🔔 Workflow يراقب الـ slots تلقائياً\n📊 فيديوهات: ${videoCount} | منصات: ${platforms.join(', ')}`,
  };
}

// ================================================================
// SECTION 11 — PENDING PREVIEW
// ================================================================
async function getPendingPreview(env, count = 10) {
  const mem = await getMemory(env);
  if (!mem.github.token) return '❌ GitHub غير مُهيأ';
  const videos = await getPendingVideos(mem.github.repo_owner, mem.github.repo_name, mem.github.token);
  if (!videos.length) return '📭 لا يوجد فيديوهات معلقة';
  const list = videos.slice(0, count).map((v, i) =>
    `${i+1}. **${v.base}** — ${(v.size/1024/1024).toFixed(1)} MB`
  ).join('\n');
  return `📹 **الفيديوهات المعلقة (${videos.length} إجمالاً):**\n\n${list}${videos.length > count ? `\n\n...و ${videos.length-count} فيديو آخر` : ''}`;
}

// ================================================================
// SECTION 12 — ReAct LOOP ENGINE
// ================================================================
async function reactLoop(env, userMsg, history, sendUpd) {
  const sysPrompt = await buildSystemPrompt(env);
  const conv      = [...history, { role: 'user', content: userMsg }];
  let   execResult = null;

  for (let loop = 0; loop < 7; loop++) {
    if (execResult) {
      conv.push({ role: 'user', content: `نتيجة التنفيذ:\n${JSON.stringify(execResult, null, 2)}\nماذا تفعل الآن؟` });
      execResult = null;
    }

    let raw;
    try { raw = await callAI(env, conv, sysPrompt); }
    catch (e) { return { message: `❌ خطأ في الـ AI: ${e.message}`, history: conv }; }

    const parsed = parseAI(raw);
    const { action, thinking, message, code, code_purpose, memory_updates, needs_followup, retry_on_fail } = parsed;

    if (thinking) await sendUpd(`🤔 ${thinking}`);

    if (!action || action === 'SPEAK') {
      conv.push({ role: 'assistant', content: message || raw });
      return { message: message || raw, thinking: thinking || null, history: conv };
    }

    if (action === 'MEMORY_UPDATE') {
      await sendUpd('💾 جارٍ الحفظ في Workers KV...');
      for (const u of (memory_updates || [])) {
        if (u.section && u.data) await memWrite(env, u.section, u.data);
      }
      await sendUpd('✅ تم الحفظ!');
      if (!needs_followup) {
        conv.push({ role: 'assistant', content: message || '✅ تم الحفظ' });
        return { message: message || '✅ تم الحفظ', history: conv };
      }
      execResult = { success: true, saved: true };
      conv.push({ role: 'assistant', content: message || 'تم الحفظ' });
      continue;
    }

    if (action === 'CODE') {
      const result = await evalCode(env, code, code_purpose || 'تنفيذ', sendUpd);
      execResult   = result;
      if (result.success) await sendUpd('✅ نجح التنفيذ!');
      else { await sendUpd(`❌ فشل: ${result.error}`); if (retry_on_fail) await sendUpd('🔄 سيُعيد المحاولة...'); }
      conv.push({ role: 'assistant', content: `نتيجة التنفيذ: ${JSON.stringify(result)}` });
      if (!needs_followup) {
        conv.push({ role: 'user', content: 'أخبر المستخدم بما حدث بشكل ودي ومختصر.' });
        const fr = await callAI(env, conv, sysPrompt);
        const fm = parseAI(fr).message || fr;
        conv.push({ role: 'assistant', content: fm });
        return { message: fm, history: conv };
      }
      continue;
    }

    if (action === 'PLAN') {
      const res = await buildDailyPlanFromChat(env, sendUpd);
      conv.push({ role: 'assistant', content: res.message });
      return { message: res.message, history: conv };
    }

    if (action === 'HEALTH_CHECK') {
      const h   = await runHealthCheck(env, sendUpd);
      const msg = `🏥 **تقرير صحة التوكنز:**\n${Object.entries(h.results).map(([k,v]) => `- ${k}: ${v}`).join('\n')}${h.warnings.length ? '\n\n⚠️ **تحذيرات:**\n' + h.warnings.join('\n') : '\n\n✅ كل شيء سليم!'}`;
      conv.push({ role: 'assistant', content: msg });
      return { message: msg, history: conv };
    }

    if (action === 'PENDING_PREVIEW') {
      const preview = await getPendingPreview(env, 12);
      conv.push({ role: 'assistant', content: preview });
      return { message: preview, history: conv };
    }

    conv.push({ role: 'assistant', content: message || raw });
    return { message: message || raw, thinking: thinking || null, history: conv };
  }

  return { message: '❌ وصلت للحد الأقصى من الدورات. جرب مرة أخرى.', history: conv };
}

// ================================================================
// SECTION 13 — ROUTE HANDLERS
// ================================================================
async function routeChat(req, env) {
  const { message, history = [] } = await req.json();
  const { readable, writable }    = new TransformStream();
  const writer  = writable.getWriter();
  const enc     = new TextEncoder();
  const push    = obj => writer.write(enc.encode(JSON.stringify(obj) + '\n'));

  (async () => {
    try {
      const sendUpd = t => push({ type: 'update', text: t });
      const result  = await reactLoop(env, message, history, sendUpd);
      push({ type: 'message', text: result.message, thinking: result.thinking || null, history: result.history });
    } catch (e) {
      push({ type: 'message', text: `❌ خطأ داخلي: ${e.message}` });
    } finally {
      await writer.close();
    }
  })();

  return new Response(readable, {
    headers: { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*' },
  });
}

async function routeStatus(env) {
  const mem        = await getMemory(env);
  const planDoc    = await getDoc(env, 'plan');
  const fajr       = calcFajr(parseFloat(mem.settings.location_lat) || 30.0444, parseFloat(mem.settings.location_lng) || 31.2357);
  const published  = (planDoc.match(/✅ منشور/g) || []).length;

  // جلب حالة الـ Workflow instance الحالي
  let workflowStatus = 'unknown';
  try {
    const today    = new Date().toLocaleDateString('en-CA', { timeZone: 'Africa/Cairo' });
    const instance = await env.PUBLISH_WORKFLOW.get(`daily-${today}`);
    const status   = await instance.status();
    workflowStatus = status.status;
  } catch { workflowStatus = 'no_instance_today'; }

  return jsonRes({
    version:    '4.0',
    executor:   'eval() — Cloudflare Worker',
    scheduler:  'Cloudflare Workflows + step.sleepUntil()',
    memory:     'Workers KV',
    github:     mem.github?.status,
    platforms:  { youtube: mem.youtube?.status, instagram: mem.instagram?.status, facebook: mem.facebook?.status, tiktok: mem.tiktok?.status },
    workflow:   workflowStatus,
    plan_status: planDoc.includes('active') ? 'active' : 'idle',
    published_today: published,
    fajr:       fajr?.formatted || '—',
  });
}

async function routeDebugMemory(env) {
  const mem    = await getMemory(env);
  const plan   = await getDoc(env, 'plan');
  const health = await getDoc(env, 'health');
  return jsonRes({ memory: mem, plan_preview: plan.slice(0, 400), health_preview: health.slice(0, 300) });
}

async function routeTrigger(req, env) {
  const { date, force } = await req.json().catch(() => ({}));
  const today = date || new Date().toLocaleDateString('en-CA', { timeZone: 'Africa/Cairo' });

  // بناء الخطة أولاً
  const result = await buildDailyPlanCore(env);
  if (result.error) return jsonRes({ ok: false, error: result.error }, 400);

  const { schedule } = result;
  try {
    if (force) {
      // لو force=true: احذف الـ instance القديم أولاً لو موجود
      try {
        const old = await env.PUBLISH_WORKFLOW.get(`daily-${today}`);
        await old.terminate();
      } catch { /* لم يكن موجوداً */ }
    }
    const instance = await env.PUBLISH_WORKFLOW.create({
      id:     `daily-${today}`,
      params: { date: today, autoSchedule: false, schedule },
    });
    return jsonRes({ ok: true, instanceId: instance.id, slots: schedule.length });
  } catch (e) {
    return jsonRes({ ok: false, error: e.message }, 400);
  }
}

// ================================================================
// SECTION 14 — OfoqDailyWorkflow (قلب النشر التلقائي)
// ================================================================
export class OfoqDailyWorkflow extends WorkflowEntrypoint {

  async run(event, step) {
    // event.payload: { date: "2025-04-10", autoSchedule: bool, schedule?: [...], fajr?: {...} }
    const { date, autoSchedule, fajr } = event.payload;
    let schedule = event.payload.schedule;

    // ── إذا auto-schedule: انتظر حتى وقت الفجر أولاً ───────────
    if (autoSchedule && fajr?.formatted) {
      const fajrTs = buildSlotTimestamp(date, fajr.formatted);
      if (fajrTs > Date.now()) {
        await step.sleepUntil('await_fajr', new Date(fajrTs));
      }
    }

    // ── بناء الخطة (أو استخدام الخطة المرسلة من Chat) ─────────
    if (!schedule?.length) {
      schedule = await step.do('build_daily_plan', async () => {
        const result = await buildDailyPlanCore(this.env);
        if (result.error) throw new Error(result.error);
        return result.schedule;
      });
    }

    if (!schedule?.length) {
      await appendLog(this.env, '—', '—', '—', '⚠️', 'لا schedule — انتهى الـ Workflow');
      return;
    }

    // ── تنفيذ كل slot ──────────────────────────────────────────
    for (let i = 0; i < schedule.length; i++) {
      const slot   = schedule[i];
      const slotTs = buildSlotTimestamp(date, slot.time);

      // انتظر حتى موعد الـ slot (step.sleepUntil لا تُحسب ضمن الـ steps!)
      if (slotTs > Date.now()) {
        await step.sleepUntil(`await_slot_${i}_${slot.time.replace(':', '')}`, new Date(slotTs));
      }

      // نشر مع retry تلقائي 3 مرات
      await step.do(
        `publish_${i}_${slot.platform}_${slot.video.slice(0, 20)}`,
        {
          retries: { limit: 3, delay: '10 minutes', backoff: 'exponential' },
          timeout: '10 minutes',
        },
        async () => {
          const mem    = await getMemory(this.env);
          const { repo_owner, repo_name, token } = mem.github;

          // جلب أحدث قائمة للفيديوهات (في حالة تغيّرت)
          const videos  = await getPendingVideos(repo_owner, repo_name, token);
          const video   = videos.find(v => v.base === slot.video) || (slot.videoUrl ? { base: slot.video, url: slot.videoUrl, mdUrl: slot.mdUrl, id: slot.assetId, mdId: slot.mdAssetId } : null);

          if (!video) throw new Error(`فيديو ${slot.video} غير موجود في pending`);

          const meta   = await readVideoMeta(video.mdUrl || slot.mdUrl, token);
          const result = await publishVideo(this.env, slot.platform, video.url || slot.videoUrl, meta);

          if (!result.success) throw new Error(result.error || 'فشل النشر');

          // أزل من pending بعد النجاح
          if (video.id) await removeFromPending(repo_owner, repo_name, token, video);

          // حدّث plan doc في KV
          let planDoc = await getDoc(this.env, 'plan');
          planDoc = planDoc
            .replace(new RegExp(`\\| ${slot.video.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')} \\| pending \\|`), `| ${slot.video} | ✅ منشور |`)
            .replace(/- منشور: (\d+)/, (_, n) => `- منشور: ${parseInt(n) + 1}`)
            .replace(/- متبقي: (\d+)/,  (_, n) => `- متبقي: ${Math.max(0, parseInt(n) - 1)}`);
          await saveDoc(this.env, 'plan', planDoc);

          // حدّث queue doc
          let queueDoc = await getDoc(this.env, 'queue');
          queueDoc = queueDoc
            .replace('- الاسم: null',   `- الاسم: ${slot.video}`)
            .replace('- المنصة: null',  `- المنصة: ${slot.platform}`)
            .replace('- التاريخ: null', `- التاريخ: ${new Date().toISOString()}`)
            .replace('- الرابط: null',  `- الرابط: ${result.url || result.videoId || 'ok'}`);
          await saveDoc(this.env, 'queue', queueDoc);

          await appendLog(this.env, slot.time, slot.platform, slot.video, '✅', result.url || result.videoId || 'ok');

          return { url: result.url, videoId: result.videoId };
        }
      );
    }

    // ── جدولة الـ Workflow لليوم التالي ────────────────────────
    await step.do('schedule_tomorrow', async () => {
      const tomorrow     = new Date();
      tomorrow.setDate(tomorrow.getDate() + 1);
      const tomorrowDate = tomorrow.toLocaleDateString('en-CA', { timeZone: 'Africa/Cairo' });
      const mem          = await getMemory(this.env);
      const cfg          = mem.settings;
      const tomorrowFajr = calcFajr(
        parseFloat(cfg.location_lat) || 30.0444,
        parseFloat(cfg.location_lng) || 31.2357,
        tomorrow
      );

      try {
        await this.env.PUBLISH_WORKFLOW.create({
          id:     `daily-${tomorrowDate}`,
          params: { date: tomorrowDate, autoSchedule: true, fajr: tomorrowFajr },
        });
        await appendLog(this.env, '—', '—', '—', '📅', `Workflow الغد (${tomorrowDate}) مُجدول`);
      } catch (e) {
        // الـ instance موجود بالفعل (من الـ cron) — مقبول تماماً
        if (!e.message?.includes('already exists')) throw e;
      }
    });
  }
}

// ================================================================
// SECTION 15 — MAIN EXPORT (Worker Entry Point)
// ================================================================
export default {

  // ── HTTP Requests ──────────────────────────────────────────────
  async fetch(req, env) {
    if (req.method === 'OPTIONS') {
      return new Response(null, {
        headers: {
          'Access-Control-Allow-Origin':  '*',
          'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type',
        },
      });
    }

    const url  = new URL(req.url);
    const path = url.pathname;

    // ── Chat (streaming) ───────────────────────────────────────
    if (path === '/chat' && req.method === 'POST') return routeChat(req, env);

    // ── Status ─────────────────────────────────────────────────
    if (path === '/status') return routeStatus(env);

    // ── Trigger Workflow manually ──────────────────────────────
    if (path === '/trigger' && req.method === 'POST') return routeTrigger(req, env);

    // ── Debug Memory (dev only) ────────────────────────────────
    if (path === '/debug/memory') return routeDebugMemory(env);

    // ── Workflow Instance Status ───────────────────────────────
    if (path === '/workflow-status') {
      const today = new Date().toLocaleDateString('en-CA', { timeZone: 'Africa/Cairo' });
      try {
        const instance = await env.PUBLISH_WORKFLOW.get(`daily-${today}`);
        return jsonRes({ instanceId: `daily-${today}`, status: await instance.status() });
      } catch (e) {
        return jsonRes({ error: `لا يوجد Workflow لهذا اليوم: ${e.message}` }, 404);
      }
    }

    // ── كل طلب آخر → Workers Assets (index.html) ──────────────
    return new Response('Not Found', { status: 404 });
  },

  // ── Cron Trigger (safety net يومي من wrangler.toml) ───────────
  async scheduled(controller, env, ctx) {
    const today = new Date().toLocaleDateString('en-CA', { timeZone: 'Africa/Cairo' });
    const mem   = await getMemory(env);
    const cfg   = mem.settings;
    const fajr  = calcFajr(
      parseFloat(cfg.location_lat) || 30.0444,
      parseFloat(cfg.location_lng) || 31.2357
    );

    console.log(`[OFOQ Cron] ${today} | Fajr: ${fajr?.formatted || '?'}`);

    try {
      const instance = await env.PUBLISH_WORKFLOW.create({
        id:     `daily-${today}`,
        params: { date: today, autoSchedule: false, fajr },
      });
      console.log(`[OFOQ Cron] Created workflow: ${instance.id}`);
      await appendLog(env, '—', 'cron', today, '📅', 'Workflow created by cron');
    } catch (e) {
      // الـ Workflow لهذا اليوم موجود بالفعل (من الأمس) — طبيعي
      if (e.message?.includes('already exists')) {
        console.log(`[OFOQ Cron] Workflow for ${today} already exists — skipping`);
      } else {
        console.error('[OFOQ Cron] Error:', e.message);
        await appendLog(env, '—', 'cron', today, '❌', `cron error: ${e.message}`);
      }
    }
  },
};

// ================================================================
// UTILITIES
// ================================================================
function pad(n)    { return String(n).padStart(2, '0'); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function jsonRes(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
  });
}
