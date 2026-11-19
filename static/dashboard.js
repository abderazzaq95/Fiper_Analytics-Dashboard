/* App shell and "Waiting now" for the Fiper dashboard (loaded after the main script). */
const PAGE_META = {
  'sec-overview': ['Overview', 'نظرة عامة'], 'sec-waiting': ['Waiting now', 'بانتظار الرد'],
  'sec-alerts': ['Alerts', 'التنبيهات'], 'sec-agents': ['Agents', 'الموظفون'],
  'sec-channels': ['Channels', 'القنوات'], 'sec-insights': ['AI insights', 'رؤى الذكاء الاصطناعي'],
  'sec-pipeline': ['Lead pipeline', 'مسار العملاء'], 'sec-geo': ['Leads & countries', 'العملاء والدول'],
  'sec-journey': ['Lead journey', 'رحلة العميل'],
};
let currentPage = 'sec-overview';
const T = (en, ar) => lang === 'ar' ? ar : en;

// ── Pages ────────────────────────────────────────────────────────────────────
function buildPages() {
  const main = document.querySelector('main');
  [...main.children].filter(el => el.classList.contains('modal-backdrop')).forEach(el => document.body.appendChild(el));
  const pages = {};
  const beforeFirst = [];
  let current = null;
  [...main.children].forEach(el => {
    if (el.id && PAGE_META[el.id]) {
      current = document.createElement('section');
      current.className = 'page';
      current.dataset.page = el.id;
      pages[el.id] = current;
      main.appendChild(current);
    }
    if (current) current.appendChild(el); else beforeFirst.push(el);
  });
  const overview = pages['sec-overview'];
  beforeFirst.forEach(el => overview.appendChild(el));   // hot leads below the KPIs
}
function showPage(id, ev) {
  if (ev) ev.preventDefault();
  if (!PAGE_META[id]) id = 'sec-overview';
  currentPage = id;
  document.querySelectorAll('.page').forEach(p => p.classList.toggle('active', p.dataset.page === id));
  document.querySelectorAll('.nav-item[data-page]').forEach(a => {
    const on = a.dataset.page === id;
    a.classList.toggle('active', on);
    if (on) a.setAttribute('aria-current', 'page'); else a.removeAttribute('aria-current');
  });
  // Waiting now is always live, so the date controls do not apply there.
  const usesDates = id !== 'sec-waiting';
  document.getElementById('rangeTabs').hidden = !usesDates;
  document.getElementById('customRange').hidden = !usesDates || document.querySelector('#rangeTabs .active')?.dataset.range !== 'custom';
  document.body.classList.remove('nav-open');
  updatePageTitle();
  if (location.hash !== '#' + id.replace('sec-', '')) history.replaceState(null, '', '#' + id.replace('sec-', ''));
  window.scrollTo(0, 0);
  requestAnimationFrame(() => Object.values(charts).forEach(c => c && c.resize && c.resize()));
}
function smoothScrollTo(id, ev) { showPage(id, ev); }
function toggleNavMenu() { document.body.classList.toggle('nav-open'); }
function updatePageTitle() {
  const meta = PAGE_META[currentPage];
  document.getElementById('pageTitle').textContent = T(meta[0], meta[1]);
  document.title = `${meta[0]} · Fiper`;
}

// ── Theme ────────────────────────────────────────────────────────────────────
function chartLine() { return document.documentElement.dataset.theme === 'dark' ? '#1D2A45' : '#E8EDF5'; }
function applyChartTheme() {
  if (!window.Chart) return;
  const dark = document.documentElement.dataset.theme === 'dark';
  Chart.defaults.color = dark ? '#94A3B8' : '#56657C';
  Chart.defaults.borderColor = chartLine();
  Chart.defaults.font.family = '"Fira Sans",system-ui,sans-serif';
}
function toggleTheme() {
  const next = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark';
  document.documentElement.dataset.theme = next;
  try { localStorage.setItem('fiper_theme', next); } catch (e) {}
  applyChartTheme();
  if (_cache) renderAll(..._cache);
}

// ── Date range ───────────────────────────────────────────────────────────────
function isoDay(offset = 0) {
  const d = new Date(Date.now() + 2 * 3600e3 + offset * 864e5);
  return d.toISOString().slice(0, 10);
}
function chooseRange(value) {
  document.getElementById('customRange').hidden = value !== 'custom';
  if (value === 'custom') {
    const from = document.getElementById('rangeFrom'), to = document.getElementById('rangeTo');
    from.max = to.max = isoDay();
    if (!from.value) from.value = isoDay(-6);
    if (!to.value) to.value = isoDay();
    markRange('custom');
    return;
  }
  setRange(value);
}
function markRange(value) {
  document.querySelectorAll('#rangeTabs button').forEach(b => {
    const on = b.dataset.range === value;
    b.classList.toggle('active', on);
    b.setAttribute('aria-pressed', on ? 'true' : 'false');
  });
}
function applyCustomRange() {
  const from = document.getElementById('rangeFrom').value, to = document.getElementById('rangeTo').value;
  if (from && to) setRange(`custom:${from <= to ? from : to}:${from <= to ? to : from}`);
}

// ── Team filter ──────────────────────────────────────────────────────────────
let _teams = [], _teamSet = null;
function inTeam(name) { return !_teamSet || _teamSet.has(String(name || '').trim().toLowerCase()); }
function applyTeam() {
  const team = _teams.find(t => t.name === _teamName);
  _teamSet = team ? new Set(team.agents.map(a => a.toLowerCase())) : null;
  if (!team) _teamName = '';
}
function setTeam(name) {
  _teamName = name;
  try { localStorage.setItem('fiper_team', name); } catch (e) {}
  applyTeam();
  renderWaiting();
  showCurrentView();
}
async function loadTeams() {
  try {
    const r = await fetch(`${API}/api/teams`);
    if (!r.ok) return;
    _teams = (await r.json()).teams || [];
    const saved = _teamName;
    applyTeam();
    renderTeamSelect();
    renderWaiting();
    if (saved && !_teamName) {
      try { localStorage.removeItem('fiper_team'); } catch (e) {}
      showCurrentView();
    }
  } catch (e) { console.warn('teams:', e); }
}
function renderTeamSelect() {
  const sel = document.getElementById('teamSelect');
  sel.hidden = !_teams.length;
  sel.innerHTML = `<option value="">${esc(T('All teams', 'كل الفرق'))}</option>` +
    _teams.map(t => `<option value="${esc(t.name)}">${esc(t.name)} (${t.agents.length})</option>`).join('');
  sel.value = _teamName;
}

// ── Waiting now ──────────────────────────────────────────────────────────────
let _waiting = null, _waitingAt = 0, _waitingFilter,  _waitingShowAll = false, _waitingSearch = '';
let _waitSort = {key: 'live', desc: true};
const WAIT_COLS = [
  {key: 'customer', en: 'Customer', ar: 'العميل', val: i => i.name || i.phone || ''},
  {key: 'phone', en: 'Phone', ar: 'الرقم', val: i => i.phone || ''},
  {key: 'agent', en: 'Agent', ar: 'الموظف', val: i => i.agent || ''},
  {key: 'line', en: 'Line', ar: 'الخط', val: i => i.line || ''},
  {key: 'live', en: 'Waiting', ar: 'الانتظار', val: i => i.live},
  {key: 'priority', en: 'Priority', ar: 'الأولوية', val: i => i.needs_reply === false ? 0 : ({urgent: 3, normal: 2, low: 1}[i.priority] || '')},
  {key: 'since', en: 'Since', ar: 'منذ', val: i => i.since ? Date.parse(i.since) : 0},
  {key: 'body', en: 'Last message', ar: 'آخر رسالة', val: i => i.body || ''},
  {key: 'status', en: 'Status', ar: 'الحالة', val: i => i.escalations * 100 + i.reminders},
];
function waitMins(m) {
  m = Math.max(0, Math.round(m));
  if (m < 60) return T(`${m} min`, `${m} د`);
  const h = Math.floor(m / 60), r = m % 60;
  return r ? T(`${h}h ${r}m`, `${h} س ${r} د`) : T(`${h}h`, `${h} س`);
}
function fmtLocal(iso, withDate = true) {
  if (!iso) return '—';
  const opts = {timeZone: 'Etc/GMT-2', hour: '2-digit', minute: '2-digit', hour12: false};
  if (withDate) Object.assign(opts, {day: '2-digit', month: '2-digit'});
  return new Date(iso).toLocaleString('en-GB', opts);
}
async function loadWaiting() {
  try {
    const r = await fetch(`${API}/api/waiting?_=${Date.now()}`, {cache: 'no-store'});
    if (!r.ok) return;
    _waiting = await r.json();
    _waitingAt = Date.now();
    renderWaiting();
  } catch (e) { console.warn('waiting:', e); }
}
function inLine(item) {
  return !waLine || waLine === 'all' || String(item.line_number || '').replace(/\D/g, '') === String(waLine).replace(/\D/g, '');
}
function setWaitingFilter(agent) { _waitingFilter = _waitingFilter === agent ? undefined : agent; renderWaiting(); }
function setWaitingSearch(value) { _waitingSearch = value.trim().toLowerCase(); renderWaiting(); }
function sortWaiting(key) {
  if (_waitSort.key === key) _waitSort.desc = !_waitSort.desc;
  else _waitSort = {key, desc: true};   // first click: high to low
  renderWaiting();
}
function compareWaiting(a, b) {
  const col = WAIT_COLS.find(c => c.key === _waitSort.key) || WAIT_COLS.find(c => c.key === 'live');
  const va = col.val(a), vb = col.val(b);
  if (va === '' && vb !== '') return 1;       // empty values stay last
  if (vb === '' && va !== '') return -1;
  const result = typeof va === 'number'
    ? va - vb
    : String(va).localeCompare(String(vb), lang === 'ar' ? 'ar' : 'en', {numeric: true, sensitivity: 'base'});
  return (_waitSort.desc ? -result : result) || b.live - a.live;
}
function waitStatus(i) {
  if (i.escalations > 1) return `<span class="pill pill-red">${T(`Follow-up ×${i.escalations - 1}`, `متابعة ×${i.escalations - 1}`)}</span>`;
  if (i.escalations) return `<span class="pill pill-red">${T('Escalated', 'تم التصعيد')}</span>`;
  if (i.reminders) return `<span class="pill pill-warn">${T(`Reminded ×${i.reminders}`, `تذكير ×${i.reminders}`)}</span>`;
  return `<span class="pill pill-gray">${T('New', 'جديد')}</span>`;
}
function waitPriority(i) {
  // The AI decision (or the word rules while the AI is unavailable); the reason shows on hover.
  const why = [i.ai_reason, i.ai_source === 'rules' ? T('word rules, AI not checked yet', 'قواعد الكلمات، لم يفحصها الذكاء الاصطناعي بعد') : (i.ai_source ? `AI: ${i.ai_source}` : '')]
    .filter(Boolean).join(' · ');
  const pill = (cls, en, ar) => `<span class="pill ${cls}" title="${esc(why)}">${esc(T(en, ar))}</span>`;
  if (i.needs_reply === false) return pill('pill-green', 'No reply needed', 'لا يحتاج رداً');
  if (i.priority === 'urgent') return pill('pill-red', 'Urgent', 'عاجل');
  if (i.priority === 'low') return pill('pill-gray', 'Low', 'منخفضة');
  if (i.priority === 'normal') return pill('pill-blue', 'Normal', 'عادية');
  return '—';
}
function renderWaiting() {
  const list = document.getElementById('waitingList');
  if (!list || !_waiting) return;
  const d = _waiting;
  const extra = d.working_time ? (Date.now() - _waitingAt) / 60000 : 0;
  const all = d.items.map(i => ({...i, live: i.elapsed_min + extra})).filter(i => inTeam(i.agent) && inLine(i));
  document.getElementById('navWaitingBadge').textContent = all.length || '';
  const over2 = all.filter(i => i.live >= 120).length;
  const oldest = all.reduce((m, i) => Math.max(m, i.live), 0);
  const unassigned = all.filter(i => !i.agent).length;
  const escalated = all.filter(i => i.escalations).length;
  const urgent = all.filter(i => i.priority === 'urgent' && i.needs_reply !== false).length;
  const noReply = all.filter(i => i.needs_reply === false).length;
  const kpi = (en, ar, value, tone = '', sub = '') =>
    `<div class="kpi"><div class="kpi-label">${esc(T(en, ar))}</div><div class="kpi-value ${tone}">${esc(value)}</div>${sub ? `<div class="kpi-foot">${esc(sub)}</div>` : ''}</div>`;
  document.getElementById('waitingKpis').innerHTML =
    kpi('Waiting now', 'بانتظار الرد', all.length, all.length ? 'mid' : 'good') +
    kpi('Over 2 hours', 'أكثر من ساعتين', over2, over2 ? 'bad' : 'good') +
    kpi('Longest wait', 'أطول انتظار', all.length ? waitMins(oldest) : '—', oldest >= 120 ? 'bad' : '') +
    kpi('Urgent', 'عاجل', urgent, urgent ? 'bad' : 'good', T(`${noReply} need no reply (AI)`, `${noReply} لا يحتاج رداً (AI)`)) +
    kpi('Escalated', 'تم التصعيد', escalated, escalated ? 'bad' : 'good') +
    kpi('Unassigned', 'غير مُسنَد', unassigned, unassigned ? 'mid' : 'good');
  const modeTxt = d.mode === 'live' ? T('Reminders live', 'التذكيرات مفعّلة') : d.mode === 'shadow' ? T('Reminders in test mode', 'التذكيرات في وضع التجربة') : T('Reminders off', 'التذكيرات متوقفة');
  const aiTxt = d.ai_mode === 'live' ? T('AI check live', 'فحص الذكاء الاصطناعي مفعّل')
    : d.ai_mode === 'shadow' ? T('AI check in test mode (shown, not applied)', 'فحص الذكاء الاصطناعي تجريبي (يُعرض ولا يُطبَّق)') : T('AI check off', 'فحص الذكاء الاصطناعي متوقف');
  document.getElementById('waitingInfo').textContent =
    `${modeTxt} · ${aiTxt} · ${T(`working time ${d.work_hours}`, `وقت العمل ${d.work_hours}`)} · ${T(`reminders at ${d.ladder.first} and ${d.ladder.second} min, supervisor at ${d.ladder.escalate / 60}h`, `تذكير بعد ${d.ladder.first} و${d.ladder.second} د، المشرف بعد ${d.ladder.escalate / 60} س`)}`;

  const counts = new Map();
  all.forEach(i => counts.set(i.agent || null, (counts.get(i.agent || null) || 0) + 1));
  _waitingChips = [...counts.entries()].sort((a, b) => b[1] - a[1]);
  const chips = document.getElementById('waitingAgents');
  chips.hidden = !_waitingChips.length;
  chips.innerHTML = _waitingChips.map(([agent, n], idx) =>
    `<button type="button" class="chip ${_waitingFilter === agent ? 'active' : ''}" aria-pressed="${_waitingFilter === agent}" onclick="setWaitingFilter(_waitingChips[${idx}][0])">${esc(agent || T('Unassigned', 'غير مُسنَد'))} <b>${n}</b></button>`).join('');

  let rows = all;
  if (_waitingFilter !== undefined && counts.has(_waitingFilter)) rows = rows.filter(i => (i.agent || null) === _waitingFilter);
  if (_waitingSearch) rows = rows.filter(i => [i.name, i.phone, i.agent, i.body, i.line].some(v => String(v || '').toLowerCase().includes(_waitingSearch)));
  rows = rows.slice().sort(compareWaiting);
  if (!rows.length) {
    list.innerHTML = `<div class="empty-state"><div class="empty-icon">✓</div><div class="empty-text">${esc(all.length ? T('No customer matches this filter.', 'لا يوجد عميل مطابق.') : T('No customer is waiting right now.', 'لا يوجد عملاء بانتظار الرد الآن.'))}</div></div>`;
    return;
  }
  const limit = _waitingShowAll ? rows.length : 50;
  const head = WAIT_COLS.map(c => {
    const on = _waitSort.key === c.key;
    const label = T(c.en, c.ar);
    return `<th class="${on ? 'sorted' : ''}" aria-sort="${on ? (_waitSort.desc ? 'descending' : 'ascending') : 'none'}" tabindex="0" title="${esc(T('Click to sort', 'اضغط للترتيب'))}" onclick="sortWaiting('${c.key}')" onkeydown="if(event.key==='Enter'||event.key===' '){event.preventDefault();sortWaiting('${c.key}')}">${esc(label)}<span class="sort-mark">${on ? (_waitSort.desc ? '▼' : '▲') : '↕'}</span></th>`;
  }).join('');
  const body = rows.slice(0, limit).map(i => {
    const tone = i.live >= 120 ? 'pill-red' : i.live >= 45 ? 'pill-warn' : 'pill-blue';
    return `<tr class="row-link" tabindex="0" data-phone="${esc(i.phone || '')}" onclick="openConversation(this.dataset.phone)" onkeydown="if(event.key==='Enter')openConversation(this.dataset.phone)">
      <td class="agent-name-cell">${esc(i.name || T('Customer', 'عميل'))}</td>
      <td dir="ltr" class="num">${esc(i.phone || '—')}</td>
      <td>${esc(i.agent || T('Unassigned', 'غير مُسنَد'))}</td>
      <td>${esc(i.line || '—')}</td>
      <td><span class="pill ${tone}">${esc(waitMins(i.live))}</span></td>
      <td>${waitPriority(i)}</td>
      <td class="num">${esc(fmtLocal(i.since))}</td>
      <td class="t-msg" dir="auto" title="${esc(i.body || '')}">${esc(i.body || '')}</td>
      <td>${waitStatus(i)}</td></tr>`;
  }).join('');
  const more = rows.length > limit
    ? `<div style="padding:12px;text-align:center"><button class="btn-icon" onclick="_waitingShowAll=true;renderWaiting()">${esc(T(`Show all ${rows.length}`, `عرض الكل (${rows.length})`))}</button></div>` : '';
  list.innerHTML = `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>${more}`;
}
let _waitingChips = [];

// ── Conversation ─────────────────────────────────────────────────────────────
function closeConversation() { document.getElementById('conversationModal').classList.remove('open'); }
async function openConversation(phone) {
  if (!phone) return;
  const modal = document.getElementById('conversationModal');
  const body = document.getElementById('convBody');
  document.getElementById('convTitle').textContent = phone;
  document.getElementById('convMeta').textContent = '';
  body.innerHTML = `<div class="conv-state">${esc(T('Loading conversation…', 'جار تحميل المحادثة…'))}</div>`;
  modal.classList.add('open');
  try {
    const r = await fetch(`${API}/api/conversation?phone=${encodeURIComponent(phone)}`);
    if (!r.ok) throw new Error(r.status);
    const d = await r.json();
    document.getElementById('convTitle').textContent = d.name || phone;
    document.getElementById('convMeta').textContent = [d.name ? phone : '', d.agent ? `${T('Agent', 'الموظف')}: ${d.agent}` : ''].filter(Boolean).join(' · ');
    let day = '';
    body.innerHTML = (d.events || []).map(e => {
      const stamp = new Date(e.at).toLocaleDateString('en-GB', {timeZone: 'Etc/GMT-2', day: '2-digit', month: 'short', year: 'numeric'});
      const sep = stamp !== day ? `<div class="conv-day">${esc(stamp)}</div>` : '';
      day = stamp;
      if (e.type === 'call') {
        const secs = e.duration_seconds ? ` · ${dur(e.duration_seconds)}` : '';
        return `${sep}<div class="conv-call">📞 ${esc(e.agent || T('Call', 'مكالمة'))} · ${esc(String(e.outcome || '').replace(/_/g, ' '))}${esc(secs)} · ${esc(fmtLocal(e.at, false))}</div>`;
      }
      const out = e.direction === 'outbound';
      return `${sep}<div class="bubble ${out ? 'out' : ''}" dir="auto">${esc(e.body)}<small>${esc([out ? e.agent : '', fmtLocal(e.at, false)].filter(Boolean).join(' · '))}</small></div>`;
    }).join('') || `<div class="conv-state">${esc(T('No stored messages.', 'لا توجد رسائل محفوظة.'))}</div>`;
    body.scrollTop = body.scrollHeight;
  } catch (e) {
    body.innerHTML = `<div class="conv-state">${esc(T('Could not load the conversation.', 'تعذر تحميل المحادثة.'))}</div>`;
  }
}

// ── Start ────────────────────────────────────────────────────────────────────
function togglePassword() {
  const field = document.getElementById('loginPass');
  field.type = field.type === 'password' ? 'text' : 'password';
}
const _kpiNumbers = new Map();
function animateKpis() {
  if (matchMedia('(prefers-reduced-motion: reduce)').matches) return;
  document.querySelectorAll('#kpiGrid .kpi-card').forEach(card => {
    const label = card.querySelector('.kpi-label')?.textContent, value = card.querySelector('.kpi-value');
    const match = value?.textContent.trim().match(/^([\d,]+(?:\.\d+)?)(.*)$/);
    if (!match) return;
    const target = Number(match[1].replace(/,/g, '')), digits = (match[1].split('.')[1] || '').length;
    const from = _kpiNumbers.get(label) ?? 0;
    _kpiNumbers.set(label, target);
    if (from === target) return;
    const started = performance.now();
    const tick = now => {
      const p = Math.min(1, (now - started) / 450), eased = 1 - Math.pow(1 - p, 3);
      value.textContent = (from + (target - from) * eased).toLocaleString('en', {minimumFractionDigits: digits, maximumFractionDigits: digits}) + match[2];
      if (p < 1) requestAnimationFrame(tick);
    };
    requestAnimationFrame(tick);
  });
}
function startDashboardShell() { applyTeam(); loadTeams(); loadWaiting(); }
buildPages();
applyChartTheme();
showPage('sec-' + (location.hash.slice(1) || 'overview'));
window.addEventListener('hashchange', () => showPage('sec-' + (location.hash.slice(1) || 'overview')));
document.addEventListener('keydown', e => { if (e.key === 'Escape') closeConversation(); });
setInterval(() => { if (_dashboardStarted && !document.hidden) loadWaiting(); }, 60 * 1000);
setInterval(() => { if (_waiting) renderWaiting(); }, 30 * 1000);
