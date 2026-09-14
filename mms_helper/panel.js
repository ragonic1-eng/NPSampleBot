/* NP Sample Request Helper — content script for MMS
   http://www.npsin.com/mms3/master/sampleRequestUpdate.do?code=S-XXXX

   Alex (14-Sep-2026): "generate a default template to assist user to raise
   sample request … project the input needed to raise the request … provide
   any information that the database also has (customer address, name,
   contact)."

   What it does, in order:
     1. reads this SR page: the customer label, every saved item's note
        (the strongest ship-to source — the same one the Telegram bot
        uses: the SR's own request logs), the MMS dropdowns;
     2. shows a side panel with the standard request form, pre-filled with
        what the SR already knows, each value labelled with WHERE it came
        from, and a live "Still missing" list — what is needed changes with
        the request (hand-carry needs no address; Repeat/Modify needs a
        base code);
     3. on "Write into MMS" fills the new item (request type, base code,
        note, next-action-by, until-date). It NEVER presses Save — you do.

   House rules carried over from the bot (memory: sr-note-format,
   helpful-not-assuming): the note copies the rep's words; budget is never
   derived; compliance is PROPOSED from the country in the address;
   anything proposed is labelled and one click to change. */
(() => {
  'use strict';
  const SR = new URL(location.href).searchParams.get('code') || '';
  if (!SR || document.querySelector('input[type="password"]')) return;   // login page
  if (document.getElementById('nph-root')) return;

  const MARKETS = ['Bangladesh', 'Mexico', 'Singapore', 'Malaysia', 'Indonesia', 'Thailand', 'Vietnam', 'Japan',
    'Korea', 'China', 'India', 'Philippines', 'Australia', 'New Zealand', 'USA', 'Canada', 'EU', 'UK', 'Middle East',
    'Dubai', 'UAE', 'Taiwan', 'Hong Kong', 'Nepal', 'Myanmar', 'Sri Lanka', 'Pakistan', 'Saudi Arabia', 'Cambodia',
    'Jordan', 'Kuwait', 'Qatar', 'Bahrain', 'Oman', 'Syria', 'Lebanon', 'Turkey', 'Egypt', 'Nigeria', 'South Africa',
    'CODEX', 'FDA', 'FSANZ'];
  const BASES = ['Potato chips', 'Corn puff', 'Corn curl (Twisties)', 'Wheat flour base pellets', 'Wheat flour biscuit',
    'Potato crackers', 'Prawn crackers', 'Extruded snack', 'Nuts', 'Instant noodle', 'Popcorn'];
  const METHODS = ['DHL', 'FedEx', 'UPS', 'Courier', 'Lalamove', 'Self collect', 'Hand carry'];
  const DEFAULT_ASSIGNEE = 'Jessie';      // Singapore R&D — TERRITORY_ASSIGNEE in the bot
  const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  const KEY = `nph:${SR}`;

  /* ── read the page ───────────────────────────────────────── */
  const form = [...document.forms].find((f) => f.querySelector('[name^="sreq1["]')) || document.forms[0];
  const notes = () => [...document.querySelectorAll('textarea[name^="sreq1["][name$=".reqnote"]')];
  const idxOf = (el) => Number((el.name.match(/sreq1\[(\d+)\]/) || [])[1]);
  const emptySection = () => notes().find((t) => !t.value.trim()) || null;

  const flat = document.body.innerText.replace(/\s+/g, ' ');
  let customer = '';
  {
    const m = flat.match(/Customer ID\s+(.+?)\s+Customer List/);
    if (m) {
      let raw = m[1].trim();
      if (raw.startsWith('(') && raw.endsWith(')')) raw = raw.slice(1, -1);
      const m2 = raw.match(/^[SJBC]-[A-Z0-9]+\s*:?\s*(.+)$/i);
      customer = (m2 ? m2[1] : raw).trim();
    }
  }

  /* Previous notes on this SR, oldest → newest. Each label we emit is a
     label we can read back (the bot's rule of thumb), so a note the bot
     or this panel wrote round-trips exactly. */
  const LABELS = {
    attn: /^(?:receiver(?:\s*name)?|attn|attention|recipient)\s*[:\-]?\s*(.+)$/i,
    contact: /^(?:contact(?:\s*no\.?)?|mobile|phone|tel|whatsapp|hp)\s*[:\-]?\s*(.+)$/i,
    addr: /^(?:delivery\s*address|address|ship\s*to)\s*[:\-]?\s*(.+)$/i,
    compliance: /^complian(?:ce|t)\s*[:\-]?\s*(.+)$/i,
    bag: /^bag\s*[:\-]?\s*(.+)$/i,
    budget: /^budget\s*[:\-]?\s*(.+)$/i,
    base: /^target\s*base\s*[:\-]?\s*(.+)$/i,
    method: /^(?:delivery|send|shipping)\s*method\s*[:\-]?\s*(.+)$/i,
    qty: /^qty\s*[:\-]?\s*(.+)$/i,
  };
  const history = { attn: '', contact: '', addr: '', compliance: '', bag: '', budget: '', base: '', method: '', qty: '',
                    src: {}, names: [] };
  const saved = notes().filter((t) => t.value.trim());
  saved.forEach((t) => {
    const n = idxOf(t) + 1;
    const lines = t.value.split(/\r?\n/).map((l) => l.trim());
    let inNames = false;
    for (const ln of lines) {
      if (/^seasoning\s*names?\s*:?\s*$/i.test(ln)) { inNames = true; continue; }
      if (!ln || /^[A-Za-z][A-Za-z .]{1,24}:/.test(ln) || /^comment/i.test(ln)) inNames = false;
      if (inNames && ln && ln.length < 70 && !/^[SJBC]-[A-Z0-9-]+$/i.test(ln)) {
        const nm = ln.replace(/\s*\b[SJBC]-[A-Z0-9]+(?:-[A-Z0-9]+)*\b/gi, '').trim();
        if (nm && !history.names.includes(nm)) history.names.push(nm);
        continue;
      }
      for (const [k, re] of Object.entries(LABELS)) {
        const m = ln.match(re);
        if (m && m[1].trim()) {
          const v = m[1].trim();
          if (k === 'addr' && !/\d|road|rd\.|street|st\.|jalan|blk|block|ave|lane|district|city|building|floor|\bplot\b/i.test(v)) break;
          history[k] = v; history.src[k] = `item ${n}`;
          break;
        }
      }
    }
  });
  // Compliance is a regulatory field: never an old note's value, propose
  // the customer's country from the address instead (bot rule, 09-Sep).
  const country = MARKETS.find((c) => new RegExp(`\\b${c}\\b`, 'i').test(`${history.addr} ${customer}`)) || '';

  const assigneeOpts = (() => {
    const sel = document.querySelector('select[name$=".nextActUserId"]');
    return sel ? [...sel.options].map((o) => ({ v: o.value, t: o.textContent.trim() })).filter((o) => o.t) : [];
  })();

  /* ── state ───────────────────────────────────────────────── */
  const plus7 = () => {
    const d = new Date(); d.setDate(d.getDate() + 7);
    while (d.getDay() === 0 || d.getDay() === 6) d.setDate(d.getDate() + 1);
    return d.toISOString().slice(0, 10);
  };
  const state = {
    rtype: 'new', base_code: '', names: '', comment: '', base: '', qty: '',
    bag: history.bag ? (/empty/i.test(history.bag) ? 'Empty bag' : 'NP bag') : '',
    budget: history.budget || '', compliance: country || '',
    need_by: plus7(), method: history.method || '',
    addr: history.addr || '', attn: history.attn || '', contact: history.contact || '',
    assignee: (assigneeOpts.find((o) => o.t.toLowerCase() === DEFAULT_ASSIGNEE.toLowerCase()) || assigneeOpts[0] || {}).v || '',
    src: {
      bag: history.bag ? history.src.bag : '', budget: history.budget ? history.src.budget : '',
      compliance: country ? `their address says ${country}` : '', need_by: 'proposed: 1 week',
      method: history.method ? history.src.method : '', addr: history.src.addr || '',
      attn: history.src.attn || '', contact: history.src.contact || '',
    },
    open: true, pendingWrite: false,
  };
  const store = (chrome && chrome.storage && chrome.storage.local) || null;
  const save = () => { try { store ? store.set({ [KEY]: state }) : localStorage.setItem(KEY, JSON.stringify(state)); } catch (e) { /* ignore */ } };
  const load = () => new Promise((res) => {
    try {
      if (store) store.get(KEY, (r) => res(r && r[KEY]));
      else res(JSON.parse(localStorage.getItem(KEY) || 'null'));
    } catch (e) { res(null); }
  });
  const forget = () => { try { store ? store.remove(KEY) : localStorage.removeItem(KEY); } catch (e) { /* ignore */ } };

  /* ── the note (mirrors the bot's render_reqnote layout) ──── */
  const handCarry = () => /hand/i.test(state.method);
  const names = () => state.names.split(/\r?\n/).map((s) => s.trim()).filter(Boolean);
  const needByText = () => {
    if (!state.need_by) return '';
    const d = new Date(state.need_by + 'T00:00:00');
    return `BY ${String(d.getDate()).padStart(2, '0')} ${MONTHS[d.getMonth()].toUpperCase()} ${d.getFullYear()}`;
  };
  const prepdate = () => {
    if (!state.need_by) return '';
    const d = new Date(state.need_by + 'T00:00:00');
    return `${String(d.getDate()).padStart(2, '0')}/${MONTHS[d.getMonth()]}/${d.getFullYear()}`;
  };
  function renderNote() {
    const L = [];
    L.push('Seasoning name:'); names().forEach((n) => L.push(n)); L.push('');
    const c = state.comment.trim();
    if (c) { L.push('Comment:'); c.split(/\r?\n/).forEach((l) => L.push(l)); } else L.push('Comment: -');
    L.push('');
    if (state.base) L.push(`TARGET BASE: ${state.base}`);
    if (state.bag) L.push(`BAG: ${state.bag.toUpperCase()}`);
    if (state.budget) L.push(`BUDGET: ${state.budget}`);
    if (state.compliance) L.push(`COMPLIANCE: ${state.compliance}`);
    if (state.qty) L.push(`QTY: ${state.qty}`);
    if (state.need_by) L.push(`NEED BY: ${needByText()}`);
    if (state.method || (!handCarry() && state.addr)) {
      L.push('');
      if (state.method) L.push(`Delivery method: ${state.method}`);
      if (!handCarry() && state.addr) L.push(`Delivery address: ${state.addr}`);
    }
    if (!handCarry() && (state.attn || state.contact)) {
      L.push('');
      if (state.attn) L.push(`RECEIVER NAME: ${state.attn}`);
      if (state.contact) L.push(`CONTACT NO.: ${state.contact}`);
    }
    return L.join('\n').replace(/\n{3,}/g, '\n\n').trim();
  }

  /* What this request still needs — the point of the panel. */
  function gaps() {
    const g = [];
    if (!names().length) g.push('Seasoning name');
    if (!state.base) g.push('Target base');
    if (!state.qty) g.push('Qty');
    if (!state.method) g.push('Send method');
    if ((state.rtype === 'rep' || state.rtype === 'mod') && !/^[SJBC]-[A-Z0-9-]{3,}$/i.test(state.base_code)) g.push('Base code');
    if (!handCarry() && state.method) {
      if (!state.addr) g.push('Address');
      if (!state.attn) g.push('Receiver name');
      if (!state.contact) g.push('Contact');
    }
    if (!state.compliance) g.push('Compliance');
    if (!state.assignee && assigneeOpts.length) g.push('Next action by');
    return g;
  }

  /* ── UI ──────────────────────────────────────────────────── */
  const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const root = document.createElement('aside');
  root.id = 'nph-root';
  document.body.appendChild(root);
  const src = (k) => (state.src[k] && state[k] ? `<i class="nph-src">${esc(state.src[k])}</i>` : '');
  const opt = (list, cur) => list.map((v) => `<option value="${esc(v)}"${v === cur ? ' selected' : ''}>${esc(v)}</option>`).join('');

  function render() {
    const g = gaps();
    const empty = emptySection();
    root.className = state.open ? '' : 'is-min';
    root.innerHTML = `
      <div class="nph-head">
        <b>NP Sample Request Helper</b>
        <span>${esc(SR)}${customer ? ` · ${esc(customer)}` : ''}</span>
        <button type="button" class="nph-min" data-act="min" title="${state.open ? 'Minimise' : 'Open'}">${state.open ? '–' : '+'}</button>
      </div>
      <div class="nph-body">
        ${history.names.length ? `<div class="nph-prev"><span>Previously on this SR</span>${history.names.slice(-8).map((n) => `<button type="button" class="nph-chip" data-name="${esc(n)}">${esc(n)}</button>`).join('')}</div>` : ''}
        <div class="nph-row nph-seg" role="group" aria-label="Request type">
          ${['new', 'rep', 'mod'].map((t) => `<button type="button" data-rtype="${t}" aria-pressed="${state.rtype === t}">${{ new: 'New', rep: 'Repeat', mod: 'Modify' }[t]}</button>`).join('')}
          ${state.rtype !== 'new' ? `<input data-k="base_code" placeholder="Base code, e.g. S-18CS43-002" value="${esc(state.base_code)}" class="nph-code">` : ''}
        </div>
        <label>Seasoning name <em>required · one per line, as you'd say it</em>
          <textarea data-k="names" rows="3" placeholder="Cheese seasoning&#10;BBQ seasoning">${esc(state.names)}</textarea></label>
        <label>Comment <em>your words for R&amp;D; one paragraph per seasoning</em>
          <textarea data-k="comment" rows="3" placeholder="Cheese seasoning - more cheesy, less salty">${esc(state.comment)}</textarea></label>
        <div class="nph-grid">
          <label>Target base <em>required</em><input data-k="base" list="nph-bases" value="${esc(state.base)}" placeholder="Potato chips"></label>
          <label>Qty <em>required</em><input data-k="qty" value="${esc(state.qty)}" placeholder="200g each, no application"></label>
          <label>Bag ${src('bag')}<select data-k="bag"><option value="">—</option>${opt(['NP bag', 'Empty bag'], state.bag)}</select></label>
          <label>Budget ${src('budget')}<input data-k="budget" value="${esc(state.budget)}" placeholder="never guessed — type it"></label>
          <label>Compliance ${src('compliance')}<input data-k="compliance" list="nph-markets" value="${esc(state.compliance)}" placeholder="country / market"></label>
          <label>Need by ${src('need_by')}<input data-k="need_by" type="date" value="${esc(state.need_by)}"></label>
          <label>Send method <em>required</em> ${src('method')}<select data-k="method"><option value="">—</option>${opt(METHODS, state.method)}${state.method && !METHODS.includes(state.method) ? `<option selected>${esc(state.method)}</option>` : ''}</select></label>
          ${assigneeOpts.length ? `<label>Next action by<select data-k="assignee">${assigneeOpts.map((o) => `<option value="${esc(o.v)}"${o.v === state.assignee ? ' selected' : ''}>${esc(o.t)}</option>`).join('')}</select></label>` : ''}
        </div>
        ${handCarry() ? '<p class="nph-note">Hand carry — no address or contact needed.</p>' : `
        <label>Delivery address ${src('addr')}<textarea data-k="addr" rows="2">${esc(state.addr)}</textarea></label>
        <div class="nph-grid">
          <label>Receiver name ${src('attn')}<input data-k="attn" value="${esc(state.attn)}"></label>
          <label>Contact no. ${src('contact')}<input data-k="contact" value="${esc(state.contact)}"></label>
        </div>`}
        <p class="nph-gaps ${g.length ? 'has' : 'ok'}">${g.length ? `<b>Still missing:</b> ${g.map(esc).join(', ')}` : '<b>Everything R&amp;D needs is here.</b>'}</p>
        <details class="nph-preview"><summary>Preview the note</summary><pre>${esc(renderNote())}</pre></details>
        <div class="nph-actions">
          <button type="button" class="nph-btn" data-act="copy">Copy note</button>
          <button type="button" class="nph-btn nph-primary" data-act="write" ${g.length ? 'disabled title="Fill the missing fields first"' : ''}>${empty ? `Write into item ${idxOf(empty) + 1}` : 'Add item + write'}</button>
        </div>
        <p class="nph-foot">Nothing is saved until <b>you</b> press Save in MMS.</p>
        <datalist id="nph-bases">${BASES.map((b) => `<option value="${esc(b)}">`).join('')}</datalist>
        <datalist id="nph-markets">${MARKETS.map((b) => `<option value="${esc(b)}">`).join('')}</datalist>
      </div>`;
  }

  /* ── writing into MMS ────────────────────────────────────── */
  function setCommand(value) {
    // MMS (Struts) reads one 'command' parameter; the page's own buttons
    // set it the same way the bot's payload does.
    let cmd = form.querySelector('input[name="command"]');
    if (cmd && cmd.type === 'submit') {
      const btn = [...form.querySelectorAll('input[name="command"],button[name="command"]')].find((b) => b.value === value);
      if (btn) { btn.click(); return true; }
    }
    if (!cmd || cmd.type !== 'hidden') {
      cmd = document.createElement('input'); cmd.type = 'hidden'; cmd.name = 'command'; form.appendChild(cmd);
    }
    cmd.value = value;
    form.submit();
    return true;
  }

  function fill(section) {
    const n = idxOf(section);
    const q = (name) => form.querySelector(`[name="${name}"]`);
    const radio = form.querySelector(`input[name="sreq1[${n}].rtype"][value="${state.rtype}"]`);
    if (radio) radio.checked = true;
    const bc = q(`reqProductCode[${n}]`);
    if (bc) bc.value = state.rtype === 'new' ? '' : state.base_code.toUpperCase();
    section.value = renderNote().replace(/\n/g, '\r\n');
    const asg = q(`sreq1[${n}].nextActUserId`);
    if (asg && state.assignee) asg.value = state.assignee;
    const pd = q(`sreq1[${n}].prepdateString`);
    let dateNote = '';
    if (pd) {
      const want = prepdate();
      if ([...pd.options].some((o) => o.value === want)) pd.value = want;
      else dateNote = ` MMS's date list doesn't offer ${want} — pick the nearest in the dropdown.`;
    }
    section.scrollIntoView({ behavior: 'smooth', block: 'center' });
    section.classList.add('nph-flash');
    setTimeout(() => section.classList.remove('nph-flash'), 2400);
    toast(`Item ${n + 1} is filled in. Check it, then press Save in MMS.${dateNote}`);
  }

  let toastEl = null;
  function toast(msg) {
    if (!toastEl) { toastEl = document.createElement('div'); toastEl.id = 'nph-toast'; document.body.appendChild(toastEl); }
    toastEl.textContent = msg; toastEl.classList.add('on');
    clearTimeout(toastEl._t); toastEl._t = setTimeout(() => toastEl.classList.remove('on'), 6000);
  }

  root.addEventListener('input', (e) => {
    const k = e.target.dataset.k;
    if (!k) return;
    state[k] = e.target.value;
    if (k === 'need_by') state.src.need_by = 'you';
    else if (k in state.src) state.src[k] = e.target.value ? 'you' : '';
    save();
    // re-render only the parts that depend on other fields; keep focus
    const focus = e.target.dataset.k, pos = e.target.selectionStart;
    if (k === 'method' || k === 'rtype') { render(); return; }
    root.querySelector('.nph-gaps').outerHTML = (() => { const g = gaps(); return `<p class="nph-gaps ${g.length ? 'has' : 'ok'}">${g.length ? `<b>Still missing:</b> ${g.map(esc).join(', ')}` : '<b>Everything R&amp;D needs is here.</b>'}</p>`; })();
    root.querySelector('.nph-preview pre').textContent = renderNote();
    const w = root.querySelector('[data-act="write"]'); const g = gaps();
    w.disabled = !!g.length; w.title = g.length ? 'Fill the missing fields first' : '';
    void focus; void pos;
  });
  root.addEventListener('change', (e) => { if (e.target.dataset.k === 'method' || e.target.dataset.k === 'bag' || e.target.dataset.k === 'assignee') { state[e.target.dataset.k] = e.target.value; save(); render(); } });
  root.addEventListener('click', (e) => {
    const t = e.target.closest('[data-act],[data-rtype],[data-name]');
    if (!t) return;
    if (t.dataset.rtype) { state.rtype = t.dataset.rtype; save(); render(); return; }
    if (t.dataset.name) { state.names = (state.names.trim() ? state.names.trim() + '\n' : '') + t.dataset.name; save(); render(); return; }
    const act = t.dataset.act;
    if (act === 'min') { state.open = !state.open; save(); render(); return; }
    if (act === 'copy') { navigator.clipboard.writeText(renderNote()).then(() => toast('Note copied.')); return; }
    if (act === 'write') {
      if (gaps().length) return;
      const empty = emptySection();
      if (empty) { fill(empty); return; }
      // No empty item yet: add one through MMS's own Add Item, then fill it
      // after the page comes back (the page reloads on every command).
      state.pendingWrite = true; save();
      toast('Adding an item to the SR…');
      setCommand('additem');
    }
  });

  load().then((prev) => {
    if (prev && typeof prev === 'object') {
      const keep = { ...prev }; delete keep.src;
      Object.assign(state, keep);
      state.src = { ...state.src, ...(prev.src || {}) };
    }
    render();
    if (state.pendingWrite) {
      state.pendingWrite = false; save();
      const empty = emptySection();
      if (empty) fill(empty); else toast('MMS did not add an empty item — press Add Item in MMS, then Write again.');
    }
  });
})();
