"use strict";

/* GymBot app — plain JS, no build step.
   Inside Telegram it signs in with Telegram initData; as a standalone (home screen)
   app it uses a session token obtained with a one-time /login code from the bot. */

const tg = window.Telegram && window.Telegram.WebApp;
const inTelegram = !!(tg && tg.initData);
if (inTelegram) {
  document.documentElement.dataset.tg = "1";
  tg.ready();
  tg.expand();
}

const SESSION_KEY = "gymbot.session";
const session = {
  get() { try { return localStorage.getItem(SESSION_KEY); } catch (_) { return null; } },
  set(token) { try { localStorage.setItem(SESSION_KEY, token); } catch (_) { /* private mode */ } },
  clear() { try { localStorage.removeItem(SESSION_KEY); } catch (_) { /* private mode */ } },
};

const $ = (sel, root = document) => root.querySelector(sel);
const view = $("#view");
const state = { me: null, workout: null, groups: [], exercises: [], rest: null, tab: "workout" };

// ------------------------------------------------------------------ helpers

const esc = (v) =>
  String(v ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

const num = (v, d = 2) => (v == null ? "-" : String(+Number(v).toFixed(d)));
const clock = (s) => {
  if (s == null) return "-";
  s = Math.round(s);
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), r = s % 60;
  return h ? `${h}:${String(m).padStart(2, "0")}:${String(r).padStart(2, "0")}` : `${m}:${String(r).padStart(2, "0")}`;
};
const span = (s) => {
  const m = Math.round(s / 60);
  return m < 60 ? `${m} min` : `${Math.floor(m / 60)} h ${String(m % 60).padStart(2, "0")} min`;
};
const kg = (v) => (v >= 10000 ? `${num(v / 1000, 1)} t` : `${num(v, 0)} kg`);
const fmtDate = (iso, opts = { weekday: "short", day: "numeric", month: "short" }) =>
  new Date(iso).toLocaleDateString(undefined, opts);
const fmtTime = (iso) => new Date(iso).toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" });
const DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

function parseDuration(text) {
  const t = String(text).trim().toLowerCase().replace(/\s+/g, "");
  if (!t) return null;
  if (/^\d+(:\d{1,2}){1,2}$/.test(t)) {
    const parts = t.split(":").map(Number);
    if (parts.slice(1).some((p) => p >= 60)) return null;
    return parts.reduce((acc, p) => acc * 60 + p, 0) || null;
  }
  const m = t.match(/^(?:(\d+)h)?(?:(\d+)(?:m|min))?(?:(\d+)(?:s|sec)?)?$/);
  if (!m || !(m[1] || m[2] || m[3])) return null;
  return (+(m[1] || 0)) * 3600 + (+(m[2] || 0)) * 60 + (+(m[3] || 0)) || null;
}

function haptic(kind) {
  try {
    if (!tg || !tg.HapticFeedback) return;
    if (kind === "success" || kind === "warning" || kind === "error") tg.HapticFeedback.notificationOccurred(kind);
    else tg.HapticFeedback.impactOccurred(kind || "light");
  } catch (_) { /* older clients */ }
}

let toastTimer;
function toast(text, cls = "") {
  const el = $("#toast");
  el.className = `toast ${cls}`;
  el.textContent = text;
  el.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => (el.hidden = true), cls === "pr" ? 4500 : 2500);
}

function confirmAsk(text) {
  return new Promise((resolve) => {
    if (tg && tg.showConfirm && tg.initData) tg.showConfirm(text, (ok) => resolve(!!ok));
    else resolve(window.confirm(text));
  });
}

async function api(path, { method = "GET", body } = {}) {
  const headers = { "Content-Type": "application/json" };
  if (inTelegram) headers.Authorization = `tma ${tg.initData}`;
  else if (session.get()) headers.Authorization = `Bearer ${session.get()}`;
  const res = await fetch(`/api${path}`, { method, headers, body: body ? JSON.stringify(body) : undefined });
  let data = null;
  try { data = await res.json(); } catch (_) { /* empty */ }
  if (!res.ok) {
    const err = new Error((data && data.error) || `Request failed (${res.status})`);
    err.status = res.status;
    throw err;
  }
  return data;
}

async function guard(fn) {
  try {
    return await fn();
  } catch (e) {
    haptic("error");
    if (e.status === 401) {
      if (inTelegram) renderAuthError(e.message);
      else { session.clear(); renderLogin(); }
    } else {
      toast(e.message);
    }
    return undefined;
  }
}

function renderAuthError(msg) {
  view.innerHTML = `<div class="card empty"><p><b>Can't sign you in.</b></p><p>${esc(msg)}</p><p>Open this app from the GymBot chat in Telegram.</p></div>`;
}

// ------------------------------------------------------------------ standalone sign-in

let loginShown = false;
async function renderLogin(message = "") {
  loginShown = true;
  document.body.classList.add("signed-out");
  $("#title").textContent = "GymBot";
  view.onclick = view.onchange = null;
  let botUrl = null;
  try { botUrl = (await (await fetch("/api/public/config")).json()).bot_url; } catch (_) { /* offline */ }
  view.innerHTML = `<div class="login">
      <img src="icon-192.png" alt="">
      <h2>Sign in to GymBot</h2>
      <div class="card" style="margin-top:14px;text-align:left">
        <ol>
          <li>Open GymBot in Telegram and send <b>/login</b>.</li>
          <li>Type the 6-character code it sends you here.</li>
        </ol>
        ${botUrl ? `<a class="btn ghost block" href="${esc(botUrl)}" target="_blank" rel="noopener">Open GymBot in Telegram</a>` : ""}
        <input class="input code-input" style="margin-top:12px" maxlength="7" placeholder="ABC-123"
          autocomplete="one-time-code" autocapitalize="characters" spellcheck="false" data-code>
        <button class="btn block" style="margin-top:10px" data-signin>Sign in</button>
        <p class="muted" data-msg style="min-height:1.4em;margin:10px 0 0">${esc(message)}</p>
      </div>
    </div>`;
  const input = $("[data-code]", view);
  const submit = async () => {
    const code = input.value.trim();
    if (code.replace(/[^a-z0-9]/gi, "").length < 6) { $("[data-msg]", view).textContent = "Enter the 6-character code."; return; }
    $("[data-signin]", view).disabled = true;
    const ok = await redeem(code);
    if (!ok) $("[data-signin]", view).disabled = false;
  };
  $("[data-signin]", view).addEventListener("click", submit);
  input.addEventListener("keydown", (e) => { if (e.key === "Enter") submit(); });
}

async function redeem(code) {
  try {
    const res = await fetch("/api/auth/redeem", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ code }) });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || "Sign-in failed");
    session.set(data.token);
    loginShown = false;
    document.body.classList.remove("signed-out");
    toast(`Welcome, ${data.user.first_name || "athlete"}! 💪`);
    refreshMe();
    switchTab("workout");
    return true;
  } catch (e) {
    if (loginShown) { const msg = $("[data-msg]", view); if (msg) msg.textContent = e.message; }
    else renderLogin(e.message);
    return false;
  }
}

// ------------------------------------------------------------------ sheet (modal)

function openSheet(title, html, onMount) {
  $("#sheet-title").textContent = title;
  $("#sheet-content").innerHTML = html;
  $("#sheet").hidden = false;
  document.body.style.overflow = "hidden";
  if (onMount) onMount($("#sheet-content"));
}
function closeSheet() {
  $("#sheet").hidden = true;
  document.body.style.overflow = "";
}
$("#sheet").addEventListener("click", (e) => { if (e.target.closest("[data-close]")) closeSheet(); });

// ------------------------------------------------------------------ charts (inline SVG)

function lineChart(points, { yLabel = (v) => num(v, 1), height = 170 } = {}) {
  if (!points.length) return `<div class="empty">No data yet</div>`;
  const W = 320, H = height, L = 46, R = 10, T = 12, B = 22;
  const ys = points.map((p) => p.y);
  let lo = Math.min(...ys), hi = Math.max(...ys);
  if (lo === hi) { lo -= 1; hi += 1; }
  const pad = (hi - lo) * 0.1; lo -= pad; hi += pad;
  const x = (i) => (points.length === 1 ? (L + W - R) / 2 : L + (i * (W - L - R)) / (points.length - 1));
  const y = (v) => T + (1 - (v - lo) / (hi - lo)) * (H - T - B);
  const path = points.map((p, i) => `${i ? "L" : "M"}${x(i).toFixed(1)},${y(p.y).toFixed(1)}`).join("");
  const area = `${path}L${x(points.length - 1).toFixed(1)},${H - B}L${x(0).toFixed(1)},${H - B}Z`;
  const ticks = [lo + pad, (lo + hi) / 2, hi - pad];
  const labelEvery = Math.max(1, Math.ceil(points.length / 5));
  return `<svg class="chart" viewBox="0 0 ${W} ${H}" role="img">
    ${ticks.map((t) => `<line class="axis" x1="${L}" x2="${W - R}" y1="${y(t)}" y2="${y(t)}" stroke-dasharray="2 3"/>
      <text x="${L - 4}" y="${y(t) + 3}" text-anchor="end">${esc(yLabel(t))}</text>`).join("")}
    <path class="area" d="${area}"/><path class="line" d="${path}"/>
    ${points.map((p, i) => `<circle class="dot" cx="${x(i)}" cy="${y(p.y)}" r="${points.length > 30 ? 1.8 : 3}"><title>${esc(p.label)}: ${esc(yLabel(p.y))}</title></circle>`).join("")}
    ${points.map((p, i) => (i % labelEvery === 0 || i === points.length - 1) && (i === points.length - 1 || points.length - 1 - i >= labelEvery / 2)
      ? `<text x="${x(i)}" y="${H - 6}" text-anchor="middle">${esc(p.label)}</text>` : "").join("")}
  </svg>`;
}

function barChart(points, { yLabel = (v) => num(v, 0), height = 150 } = {}) {
  if (!points.length) return `<div class="empty">No data yet</div>`;
  const W = 320, H = height, L = 46, R = 6, T = 10, B = 22;
  const hi = Math.max(1, ...points.map((p) => p.y));
  const bw = (W - L - R) / points.length;
  const y = (v) => T + (1 - v / hi) * (H - T - B);
  const labelEvery = Math.max(1, Math.ceil(points.length / 6));
  return `<svg class="chart" viewBox="0 0 ${W} ${H}" role="img">
    <line class="axis" x1="${L}" x2="${W - R}" y1="${H - B}" y2="${H - B}"/>
    <text x="${L - 4}" y="${T + 4}" text-anchor="end">${esc(yLabel(hi))}</text>
    <text x="${L - 4}" y="${H - B}" text-anchor="end">0</text>
    ${points.map((p, i) => {
      const h = Math.max(p.y ? 2 : 1, H - B - y(p.y));
      return `<rect class="bar ${p.y ? "" : "zero"}" x="${L + i * bw + bw * 0.15}" y="${H - B - h}" width="${bw * 0.7}" height="${h}" rx="2"><title>${esc(p.label)}: ${esc(yLabel(p.y))}</title></rect>
        ${i % labelEvery === 0 ? `<text x="${L + i * bw + bw / 2}" y="${H - 6}" text-anchor="middle">${esc(p.label)}</text>` : ""}`;
    }).join("")}
  </svg>`;
}

// ------------------------------------------------------------------ exercise picker

async function loadCatalog() {
  if (state.exercises.length) return;
  const [g, e] = await Promise.all([api("/groups"), api("/exercises")]);
  state.groups = g.groups;
  state.exercises = e.exercises;
}

const thumb = (ex) =>
  ex.image ? `<img class="thumb" src="${esc(ex.image)}" alt="" loading="lazy">` : `<span class="thumb">${ex.kind === "cardio" ? "🏃" : ex.kind === "time" ? "⏱" : "🏋️"}</span>`;

function pickExercise(title = "Pick exercise") {
  return new Promise(async (resolve) => {
    await guard(loadCatalog);
    let group = state.groups[0] || "";
    let query = "";
    const recentIds = new Set((state.workout?.exercises || []).map((b) => b.exercise.id));
    const render = (root) => {
      const q = query.trim().toLowerCase();
      const items = state.exercises.filter((x) => (q ? x.name.toLowerCase().includes(q) : x.group === group));
      root.querySelector(".chips").innerHTML = state.groups
        .map((g) => `<button class="chip ${g === group && !q ? "on" : ""}" data-group="${esc(g)}">${esc(g)}</button>`).join("");
      root.querySelector(".list").innerHTML = items.length
        ? items.map((x) => `<button class="list-item" data-id="${x.id}">${thumb(x)}<span class="grow"><span class="ellipsis">${esc(x.name)}</span>
            <span class="muted">${esc(x.group)}${recentIds.has(x.id) ? " · in this workout" : ""}${x.custom ? " · custom" : ""}</span></span></button>`).join("")
        : `<div class="empty">Nothing found.</div>`;
    };
    openSheet(title, `
      <input class="input" type="search" placeholder="Search exercises" autocomplete="off">
      <div class="chips" style="margin-top:10px"></div>
      <div class="card tight list"></div>
      <button class="btn ghost block" data-custom>+ Custom exercise</button>`, (root) => {
      render(root);
      root.querySelector("input").addEventListener("input", (e) => { query = e.target.value; render(root); });
      root.addEventListener("click", async (e) => {
        const chip = e.target.closest("[data-group]");
        if (chip) { group = chip.dataset.group; query = ""; root.querySelector("input").value = ""; render(root); return; }
        const item = e.target.closest("[data-id]");
        if (item) { closeSheet(); resolve(state.exercises.find((x) => x.id === +item.dataset.id)); return; }
        if (e.target.closest("[data-custom]")) {
          const ex = await customExerciseForm(group, query);
          resolve(ex);
        }
      });
    });
  });
}

function customExerciseForm(group, name) {
  return new Promise((resolve) => {
    openSheet("Custom exercise", `
      <label class="field"><span>Name</span><input class="input" name="name" maxlength="120" value="${esc(name)}"></label>
      <label class="field"><span>Muscle group</span><input class="input" name="group" maxlength="40" value="${esc(group)}" list="groups-dl">
        <datalist id="groups-dl">${state.groups.map((g) => `<option value="${esc(g)}">`).join("")}</datalist></label>
      <label class="field"><span>Type</span><select class="input" name="kind">
        <option value="strength">Weight × reps</option><option value="time">Hold time</option><option value="cardio">Distance + time</option></select></label>
      <button class="btn block" data-save>Add exercise</button>`, (root) => {
      root.querySelector("[data-save]").addEventListener("click", async () => {
        const body = { name: root.querySelector("[name=name]").value, group: root.querySelector("[name=group]").value, kind: root.querySelector("[name=kind]").value };
        const ex = await guard(() => api("/exercises", { method: "POST", body }));
        if (!ex) return;
        state.exercises = [];
        closeSheet();
        resolve(ex);
      });
    });
  });
}

// ------------------------------------------------------------------ rest timer

let restTick;
function startRest(seconds) {
  state.rest = { end: Date.now() + seconds * 1000, total: seconds };
  drawRest();
  clearInterval(restTick);
  restTick = setInterval(drawRest, 250);
}
function stopRest() {
  state.rest = null;
  clearInterval(restTick);
  const el = $("#rest");
  if (el) el.remove();
}
function drawRest() {
  if (!state.rest) return;
  const left = Math.max(0, (state.rest.end - Date.now()) / 1000);
  let el = $("#rest");
  if (!el && state.tab === "workout") {
    el = document.createElement("div");
    el.id = "rest";
    el.className = "rest";
    el.innerHTML = `<span>⏱</span><span class="time"></span><span class="bar"><i></i></span><button data-rest="30">+30s</button><button data-rest="skip">Skip</button>`;
    view.prepend(el);
    el.addEventListener("click", (e) => {
      const b = e.target.closest("[data-rest]");
      if (!b) return;
      if (b.dataset.rest === "skip") stopRest();
      else { state.rest.end += 30000; state.rest.total += 30; drawRest(); }
    });
  }
  if (el) {
    el.querySelector(".time").textContent = clock(Math.ceil(left));
    el.querySelector(".bar > i").style.width = `${(left / state.rest.total) * 100}%`;
  }
  if (left <= 0) {
    stopRest();
    haptic("success");
    if (navigator.vibrate) navigator.vibrate([200, 100, 200]);
    toast("Rest over — next set! 💪");
  }
}

// ------------------------------------------------------------------ workout tab

let currentExercise = null;

async function renderWorkout() {
  const data = await guard(() => api("/workout"));
  if (data === undefined) return;
  state.workout = data.workout;
  if (!state.workout) return renderStart();

  const w = state.workout;
  const started = fmtTime(w.started_at);
  let html = `<div class="card"><div class="row between"><div><div class="muted">Started ${esc(started)}</div>
      <div class="big">${w.set_count} set${w.set_count === 1 ? "" : "s"}</div></div><div class="muted" style="text-align:right">${w.volume_kg ? kg(w.volume_kg) : ""}
      ${w.pr_count ? `<div>🏆 ${w.pr_count} PR${w.pr_count > 1 ? "s" : ""}</div>` : ""}</div></div></div>`;

  if (w.plan) {
    html += `<h3>${esc(w.plan.name)}</h3><div class="card">${w.plan.items.map((it) => {
      const done = it.done >= it.target_sets;
      return `<div class="plan-step"><span class="check ${done ? "done" : ""}">${done ? "✓" : ""}</span>
        <button class="list-item" style="padding:0" data-open="${it.exercise.id}"><span class="grow"><span class="ellipsis">${esc(it.exercise.name)}</span>
        <span class="muted">${it.done}/${it.target_sets} sets${it.target_reps ? ` × ${it.target_reps}` : ""}</span></span></button></div>`;
    }).join("")}</div>`;
  }

  html += `<div id="logger"></div>`;

  if (w.exercises.length) {
    html += `<h3>This workout</h3>` + w.exercises.map((b) => `<div class="card" data-open="${b.exercise.id}" style="cursor:pointer">
      <div class="row">${thumb(b.exercise)}<div class="grow"><b>${esc(b.exercise.name)}</b><div class="muted">${b.sets.length} set${b.sets.length === 1 ? "" : "s"}</div></div></div>
      <div class="sets">${b.sets.map((s) => `<span class="set ${s.is_pr ? "pr" : ""}">${esc(s.text)}</span>`).join("")}</div></div>`).join("");
  }

  html += `<div class="btns"><button class="btn ghost" data-act="pick">+ Exercise</button><button class="btn" data-act="finish">Finish</button></div>
    <div class="btns"><button class="btn danger small" data-act="undo">Undo last set</button><button class="btn danger small" data-act="cancel">Discard workout</button></div>`;
  view.innerHTML = html;
  if (state.rest) drawRest();

  const nextPlan = w.plan && w.plan.items.find((it) => it.done < it.target_sets);
  if (currentExercise) renderLogger(currentExercise);
  else if (nextPlan) renderLogger(nextPlan.exercise);

  view.onclick = async (e) => {
    const open = e.target.closest("[data-open]");
    if (open) {
      await guard(loadCatalog);
      const ex = state.exercises.find((x) => x.id === +open.dataset.open);
      if (ex) { renderLogger(ex); $("#logger").scrollIntoView({ behavior: "smooth", block: "center" }); }
      return;
    }
    const act = e.target.closest("[data-act]")?.dataset.act;
    if (act === "pick") {
      const ex = await pickExercise();
      if (ex) { renderLogger(ex); $("#logger")?.scrollIntoView({ behavior: "smooth", block: "center" }); }
    } else if (act === "finish") {
      if (!w.set_count && !(await confirmAsk("No sets logged — finish and discard this workout?"))) return;
      const res = await guard(() => api("/workout/finish", { method: "POST" }));
      if (!res) return;
      stopRest(); currentExercise = null; haptic("success");
      if (res.summary) showSummary(res.summary, true);
      renderWorkout(); refreshMe();
    } else if (act === "undo") {
      const res = await guard(() => api("/workout/sets/last", { method: "DELETE" }));
      if (res) { toast("Last set removed"); renderWorkout(); }
    } else if (act === "cancel") {
      if (!(await confirmAsk("Discard this workout and all its sets?"))) return;
      if (await guard(() => api("/workout/cancel", { method: "POST" }))) { stopRest(); currentExercise = null; renderWorkout(); }
    }
  };
}

async function renderStart() {
  const plans = (await guard(() => api("/plans")))?.plans || [];
  view.innerHTML = `<div class="card"><h2>Ready to train?</h2><p class="muted">Start an empty workout and add exercises as you go, or follow one of your plans.</p>
      <button class="btn block" data-start>🏋️ Start workout</button></div>
    ${plans.filter((p) => p.items.length).length ? `<h3>Start from a plan</h3><div class="card tight">${plans.filter((p) => p.items.length).map((p) =>
      `<button class="list-item" data-plan="${p.id}"><span class="grow"><b>${esc(p.name)}</b><span class="muted ellipsis" style="display:block">${p.items.map((i) => esc(i.exercise.name)).join(" · ")}</span></span><span>▶️</span></button>`).join("")}</div>` : ""}`;
  view.onclick = async (e) => {
    const plan = e.target.closest("[data-plan]");
    if (!plan && !e.target.closest("[data-start]")) return;
    const body = plan ? { plan_id: +plan.dataset.plan } : {};
    if (await guard(() => api("/workout/start", { method: "POST", body }))) { haptic("medium"); currentExercise = null; renderWorkout(); }
  };
}

async function renderLogger(ex) {
  currentExercise = ex;
  const box = $("#logger");
  if (!box || !ex) return;
  const done = (state.workout.exercises.find((b) => b.exercise.id === ex.id) || { sets: [] }).sets;
  const lastRes = await guard(() => api(`/exercises/${ex.id}/last`));
  const last = lastRes?.last_time || [];
  const ref = done[done.length - 1] || last[done.length] || last[last.length - 1] || null;
  const planItem = state.workout.plan?.items.find((it) => it.exercise.id === ex.id);

  let fields;
  if (ex.kind === "strength") {
    fields = `<div class="log-grid">
      <div><div class="stepper"><button data-step="weight:-2.5">−</button><input name="weight" inputmode="decimal" value="${ref ? num(ref.weight_kg || 0) : ""}" placeholder="0"><button data-step="weight:2.5">+</button></div><div class="stepper-label">kg (0 = bodyweight)</div></div>
      <div><div class="stepper"><button data-step="reps:-1">−</button><input name="reps" inputmode="numeric" value="${ref?.reps ?? planItem?.target_reps ?? ""}" placeholder="0"><button data-step="reps:1">+</button></div><div class="stepper-label">reps</div></div></div>`;
  } else if (ex.kind === "time") {
    fields = `<div class="stepper"><button data-step="dur:-5">−</button><input name="duration" inputmode="text" value="${ref ? clock(ref.duration_s) : "0:30"}"><button data-step="dur:5">+</button></div><div class="stepper-label">hold time (m:ss)</div>`;
  } else {
    fields = `<div class="log-grid">
      <div><div class="stepper"><input name="distance" inputmode="decimal" value="${ref ? num(ref.distance_km) : ""}" placeholder="0"></div><div class="stepper-label">km</div></div>
      <div><div class="stepper"><input name="duration" inputmode="text" value="${ref ? clock(ref.duration_s) : ""}" placeholder="28:30"></div><div class="stepper-label">time (h:mm:ss)</div></div></div>`;
  }
  box.innerHTML = `<h3>Log a set</h3><div class="card">
      <div class="row" style="margin-bottom:10px">${thumb(ex)}<div class="grow"><b>${esc(ex.name)}</b>
        <div class="muted">${planItem ? `Target ${planItem.target_sets} sets${planItem.target_reps ? ` × ${planItem.target_reps}` : ""} · ` : ""}${last.length ? `Last time: ${last.map((s) => esc(s.text)).join(", ")}` : "First time — no history yet"}</div></div></div>
      ${fields}
      <button class="btn block" style="margin-top:12px" data-log>Log set ${done.length + 1}</button></div>`;

  box.onclick = async (e) => {
    const step = e.target.closest("[data-step]");
    if (step) {
      const [field, delta] = step.dataset.step.split(":");
      if (field === "dur") {
        const input = box.querySelector("[name=duration]");
        input.value = clock(Math.max(5, (parseDuration(input.value) || 0) + +delta));
      } else {
        const input = box.querySelector(`[name=${field}]`);
        const v = Math.max(0, (parseFloat(String(input.value).replace(",", ".")) || 0) + +delta);
        input.value = field === "reps" ? Math.round(v) : num(v);
      }
      haptic("light");
      return;
    }
    if (!e.target.closest("[data-log]")) return;
    const val = (n) => box.querySelector(`[name=${n}]`)?.value.replace(",", ".").trim();
    const body = { exercise_id: ex.id };
    if (ex.kind === "strength") { body.weight_kg = val("weight") === "" ? 0 : +val("weight"); body.reps = +val("reps"); }
    else if (ex.kind === "time") body.duration_s = parseDuration(val("duration"));
    else { body.distance_km = +val("distance"); body.duration_s = parseDuration(val("duration")); }
    if (Object.values(body).some((v) => v === null || Number.isNaN(v))) { toast("Check the numbers"); haptic("error"); return; }
    const btn = e.target.closest("[data-log]");
    btn.disabled = true;
    const res = await guard(() => api("/workout/sets", { method: "POST", body }));
    btn.disabled = false;
    if (!res) return;
    state.workout = res.workout;
    if (res.prs.length) { haptic("success"); toast(`🏆 New PR! ${res.prs[0]}`, "pr"); } else haptic("medium");
    startRest(res.rest_seconds);
    await renderWorkout();
  };
}

function showSummary(s, fresh = false) {
  openSheet(fresh ? "Workout complete 💪" : fmtDate(s.started_at, { weekday: "long", day: "numeric", month: "long" }), `
    <div class="stats"><div class="stat"><b>${span(s.duration_s)}</b><span>duration</span></div>
      <div class="stat"><b>${s.set_count}</b><span>sets</span></div>
      <div class="stat"><b>${s.volume_kg ? kg(s.volume_kg) : s.cardio_km ? `${num(s.cardio_km)} km` : "-"}</b><span>${s.volume_kg ? "volume" : "distance"}</span></div></div>
    ${s.pr_count ? `<p>🏆 <b>${s.pr_count} new PR${s.pr_count > 1 ? "s" : ""}</b></p>` : ""}
    ${s.exercises.map((b) => `<div class="card"><b>${esc(b.exercise.name)}</b><div class="sets">${b.sets.map((x) => `<span class="set ${x.is_pr ? "pr" : ""}">${esc(x.text)}</span>`).join("")}</div></div>`).join("")}
    <div class="btns"><button class="btn ghost" data-saveplan>💾 Save as plan</button>${fresh ? "" : `<button class="btn danger" data-del>Delete</button>`}</div>`, (root) => {
    root.querySelector("[data-saveplan]").addEventListener("click", async () => {
      const name = window.prompt("Plan name", s.groups.slice(0, 2).join(" + ") || "My plan");
      if (!name) return;
      if (await guard(() => api(`/workouts/${s.id}/plan`, { method: "POST", body: { name } }))) { toast("Plan saved"); closeSheet(); }
    });
    root.querySelector("[data-del]")?.addEventListener("click", async () => {
      if (!(await confirmAsk("Delete this workout permanently?"))) return;
      if (await guard(() => api(`/workouts/${s.id}`, { method: "DELETE" }))) { closeSheet(); toast("Workout deleted"); renderHistory(); refreshMe(); }
    });
  });
}

// ------------------------------------------------------------------ history tab

async function renderHistory(offset = 0, append = false) {
  const data = await guard(() => api(`/history?limit=15&offset=${offset}`));
  if (!data) return;
  const items = data.workouts.map((w) => `<button class="list-item" data-w="${w.id}">
      <span class="thumb">${fmtDate(w.started_at, { day: "numeric" })}</span>
      <span class="grow"><b class="ellipsis" style="display:block">${esc(w.groups.slice(0, 3).join(", ") || "Workout")}</b>
      <span class="muted">${fmtDate(w.started_at)} · ${w.set_count} sets · ${span(w.duration_s)}${w.pr_count ? ` · ${w.pr_count}🏆` : ""}</span></span></button>`).join("");
  const more = offset + data.workouts.length < data.total ? `<button class="btn ghost block" data-more="${offset + 15}">Load more</button>` : "";
  if (append) {
    $(".card.tight", view).insertAdjacentHTML("beforeend", items);
    view.querySelector("[data-more]")?.remove();
    view.insertAdjacentHTML("beforeend", more);
  } else {
    view.innerHTML = data.total
      ? `<div class="muted" style="margin:4px 4px 8px">${data.total} workouts</div><div class="card tight">${items}</div>${more}`
      : `<div class="card empty">No workouts yet. Your finished workouts show up here.</div>`;
  }
  const cache = (state.history = append ? [...(state.history || []), ...data.workouts] : data.workouts);
  view.onclick = (e) => {
    const m = e.target.closest("[data-more]");
    if (m) return renderHistory(+m.dataset.more, true);
    const w = e.target.closest("[data-w]");
    if (w) showSummary(cache.find((x) => x.id === +w.dataset.w));
  };
}

// ------------------------------------------------------------------ progress tab

let period = "week";
async function renderProgress() {
  const [sum, weekly, rec] = await Promise.all([
    guard(() => api(`/stats/summary?period=${period}`)),
    guard(() => api("/stats/weekly?weeks=12")),
    guard(() => api("/stats/records")),
  ]);
  if (!sum || !weekly || !rec) return;
  const periods = [["today", "Today"], ["week", "Week"], ["month", "Month"]];
  view.innerHTML = `
    <div class="chips">${periods.map(([k, l]) => `<button class="chip ${k === period ? "on" : ""}" data-period="${k}">${l}</button>`).join("")}</div>
    <div class="stats">
      <div class="stat"><b>${sum.workouts}</b><span>workouts</span></div>
      <div class="stat"><b>${sum.sets}</b><span>sets</span></div>
      <div class="stat"><b>${kg(sum.volume_kg)}</b><span>volume</span></div>
      <div class="stat"><b>${span(sum.training_s)}</b><span>training</span></div>
      <div class="stat"><b>${num(sum.cardio_km)} km</b><span>cardio</span></div>
      <div class="stat"><b>${sum.prs}</b><span>PRs 🏆</span></div>
    </div>
    ${sum.groups.length ? `<div class="card" style="margin-top:10px">${sum.groups.map((g) => `<div class="row between"><span>${esc(g.group)}</span><span class="muted">${g.sets} sets${g.volume_kg ? ` · ${kg(g.volume_kg)}` : ""}</span></div>`).join("")}</div>` : ""}
    <h3>Volume per week</h3>
    <div class="card">${barChart(weekly.weeks.map((w) => ({ label: fmtDate(w.week_start, { day: "numeric", month: "numeric" }), y: w.volume_kg })), { yLabel: kg })}</div>
    <h3>Workouts per week</h3>
    <div class="card">${barChart(weekly.weeks.map((w) => ({ label: fmtDate(w.week_start, { day: "numeric", month: "numeric" }), y: w.workouts })), { height: 110 })}</div>
    <h3>Personal records · tap for chart</h3>
    ${rec.records.length ? `<div class="card tight">${rec.records.map((r) => `<button class="list-item" data-ex="${r.exercise.id}">${thumb(r.exercise)}
      <span class="grow"><span class="ellipsis" style="display:block">${esc(r.exercise.name)}</span><span class="muted">${esc(recordText(r))}</span></span><span>📈</span></button>`).join("")}</div>`
      : `<div class="card empty">Log some sets to see your records.</div>`}`;
  view.onclick = (e) => {
    const p = e.target.closest("[data-period]");
    if (p) { period = p.dataset.period; return renderProgress(); }
    const ex = e.target.closest("[data-ex]");
    if (ex) showExerciseChart(+ex.dataset.ex);
  };
}

function recordText(r) {
  const k = r.exercise.kind;
  if (k === "cardio") return [r.best_distance_km && `${num(r.best_distance_km)} km`, r.best_pace_s && `${clock(r.best_pace_s)} /km`].filter(Boolean).join(" · ");
  if (k === "time") return `Longest hold ${clock(r.best_hold_s)}`;
  if (r.best_weight_kg) return `${num(r.best_weight_kg)} kg × ${r.best_weight_reps} · e1RM ${num(r.best_e1rm_kg, 1)} kg`;
  return `${r.best_reps} reps`;
}

async function showExerciseChart(id) {
  const p = await guard(() => api(`/exercises/${id}/progress`));
  if (!p) return;
  const pts = p.points.map((x) => ({ label: fmtDate(x.day, { day: "numeric", month: "numeric" }), y: x.value }));
  const fmt = p.metric === "hold" ? clock : p.metric === "distance" ? (v) => `${num(v, 1)}` : (v) => num(v, 0);
  const extra = p.metric === "e1rm"
    ? `<h3>Volume per session</h3><div class="card">${barChart(p.points.map((x) => ({ label: fmtDate(x.day, { day: "numeric", month: "numeric" }), y: x.extra || 0 })), { yLabel: kg, height: 120 })}</div>`
    : p.metric === "distance" && p.points.some((x) => x.extra)
      ? `<h3>Best pace (s/km, lower is better)</h3><div class="card">${lineChart(p.points.filter((x) => x.extra).map((x) => ({ label: fmtDate(x.day, { day: "numeric", month: "numeric" }), y: x.extra })), { yLabel: clock, height: 130 })}</div>` : "";
  openSheet(p.exercise.name, `<div class="muted" style="margin-bottom:6px">${esc(p.label)} · ${p.points.length} sessions</div>
    <div class="card">${lineChart(pts, { yLabel: fmt })}</div>${extra}`);
}

// ------------------------------------------------------------------ plans tab

async function renderPlans() {
  const data = await guard(() => api("/plans"));
  if (!data) return;
  view.innerHTML = `${data.plans.length ? data.plans.map((p) => `<div class="card">
      <div class="row between"><b>${esc(p.name)}</b><button class="btn small ghost" data-edit="${p.id}">Edit</button></div>
      <div class="muted" style="margin:6px 0 10px">${p.items.length ? p.items.map((i) => `${esc(i.exercise.name)} ${i.target_sets}×${i.target_reps || "–"}`).join(" · ") : "No exercises yet"}</div>
      ${p.items.length ? `<button class="btn block small" data-go="${p.id}">▶️ Start this plan</button>` : ""}</div>`).join("")
      : `<div class="card empty">No plans yet. Create one, or save a finished workout as a plan.</div>`}
    <button class="btn block" data-new>+ New plan</button>`;
  view.onclick = async (e) => {
    const go = e.target.closest("[data-go]");
    if (go) {
      const res = await guard(() => api("/workout/start", { method: "POST", body: { plan_id: +go.dataset.go } }));
      if (res) {
        if (!res.created) toast("You already have a workout running");
        currentExercise = null; switchTab("workout");
      }
      return;
    }
    const edit = e.target.closest("[data-edit]");
    if (edit) return editPlan(+edit.dataset.edit);
    if (e.target.closest("[data-new]")) {
      const name = window.prompt("Plan name", "Push day");
      if (!name) return;
      const p = await guard(() => api("/plans", { method: "POST", body: { name } }));
      if (p) { renderPlans(); editPlan(p.id); }
    }
  };
}

async function editPlan(id) {
  const p = await guard(() => api(`/plans/${id}`));
  if (!p) return;
  openSheet(p.name, `
    <div class="card tight">${p.items.length ? p.items.map((it, i) => `<div class="list-item" data-item="${it.id}">
      ${thumb(it.exercise)}<span class="grow"><span class="ellipsis" style="display:block">${esc(it.exercise.name)}</span>
      <span class="row" style="gap:6px;margin-top:4px">
        <select class="input" style="min-height:32px;padding:4px 6px;width:auto" data-f="target_sets">${[1, 2, 3, 4, 5, 6, 8, 10].map((n) => `<option ${n === it.target_sets ? "selected" : ""}>${n}</option>`).join("")}</select> sets ×
        <input class="input" style="min-height:32px;padding:4px 6px;width:64px" data-f="target_reps" inputmode="numeric" value="${it.target_reps || ""}" placeholder="reps"></span></span>
      <span style="display:flex;flex-direction:column">
        <button class="icon-btn" data-move="-1" ${i === 0 ? "disabled" : ""} aria-label="Move up">▲</button>
        <button class="icon-btn" data-move="1" ${i === p.items.length - 1 ? "disabled" : ""} aria-label="Move down">▼</button></span>
      <button class="icon-btn" data-remove aria-label="Remove">🗑</button></div>`).join("") : `<div class="empty">Add exercises to this plan.</div>`}</div>
    <button class="btn block" data-add>+ Add exercise</button>
    <div class="btns"><button class="btn ghost" data-rename>Rename</button><button class="btn danger" data-delplan>Delete plan</button></div>`, (root) => {
    const refresh = () => { editPlan(id); renderPlans(); };
    root.addEventListener("change", async (e) => {
      const f = e.target.closest("[data-f]");
      if (!f) return;
      const itemId = f.closest("[data-item]").dataset.item;
      const body = { [f.dataset.f]: f.value === "" ? 0 : +f.value };
      if (await guard(() => api(`/plans/${id}/items/${itemId}`, { method: "PATCH", body }))) renderPlans();
    });
    root.addEventListener("click", async (e) => {
      const item = e.target.closest("[data-item]");
      const mv = e.target.closest("[data-move]");
      if (mv && item) {
        if (await guard(() => api(`/plans/${id}/items/${item.dataset.item}`, { method: "PATCH", body: { move: +mv.dataset.move } }))) refresh();
      } else if (e.target.closest("[data-remove]") && item) {
        if (await guard(() => api(`/plans/${id}/items/${item.dataset.item}`, { method: "DELETE" }))) refresh();
      } else if (e.target.closest("[data-add]")) {
        const ex = await pickExercise("Add to plan");
        if (ex && (await guard(() => api(`/plans/${id}/items`, { method: "POST", body: { exercise_id: ex.id, target_sets: 3 } })))) refresh();
      } else if (e.target.closest("[data-rename]")) {
        const name = window.prompt("Plan name", p.name);
        if (name && (await guard(() => api(`/plans/${id}`, { method: "PATCH", body: { name } })))) refresh();
      } else if (e.target.closest("[data-delplan]")) {
        if (!(await confirmAsk(`Delete plan "${p.name}"?`))) return;
        if (await guard(() => api(`/plans/${id}`, { method: "DELETE" }))) { closeSheet(); renderPlans(); }
      }
    });
  });
}

// ------------------------------------------------------------------ settings tab

async function renderSettings() {
  const [me, rem, bw] = await Promise.all([guard(() => api("/me")), guard(() => api("/reminders")), guard(() => api("/bodyweight"))]);
  if (!me || !rem || !bw) return;
  const u = me.user;
  const browserTz = Intl.DateTimeFormat().resolvedOptions().timeZone;
  view.innerHTML = `
    <h3>Training</h3>
    <div class="card">
      <label class="field"><span>Rest timer</span><select class="input" data-rest>${[30, 45, 60, 75, 90, 120, 150, 180, 240, 300].map((s) => `<option value="${s}" ${s === u.rest_seconds ? "selected" : ""}>${clock(s)}</option>`).join("")}
        ${[30, 45, 60, 75, 90, 120, 150, 180, 240, 300].includes(u.rest_seconds) ? "" : `<option selected value="${u.rest_seconds}">${clock(u.rest_seconds)}</option>`}</select></label>
      <label class="field" style="margin:0"><span>Timezone — ${esc(u.timezone_label)}</span>
        <div class="row"><input class="input grow" data-tz value="${esc(u.timezone)}" placeholder="Europe/Berlin or +3">
        <button class="btn small" data-savetz>Save</button></div></label>
      ${browserTz && browserTz !== u.timezone ? `<button class="btn ghost small" style="margin-top:8px" data-usetz="${esc(browserTz)}">Use this device's timezone (${esc(browserTz)})</button>` : ""}
    </div>

    <h3>Reminders</h3>
    <div class="card">
      ${rem.reminders.length ? rem.reminders.map((r) => `<div class="row between" style="padding:6px 0" data-r="${r.id}">
        <span><b>${esc(r.at_time)}</b> <span class="muted">${esc(r.label.replace(/ at .*$/, ""))}</span></span>
        <span class="row"><label class="switch"><input type="checkbox" data-toggle ${r.enabled ? "checked" : ""}><i></i></label>
        <button class="icon-btn" data-delr aria-label="Delete">🗑</button></span></div>`).join("") : `<div class="muted">No reminders. You'll get a message at the chosen time unless you've already trained that day.</div>`}
      <div style="border-top:1px solid var(--line);margin-top:10px;padding-top:10px">
        <div class="days" data-days>${DAYS.map((d, i) => `<button data-day="${i}" class="${i < 5 ? "on" : ""}">${d}</button>`).join("")}</div>
        <div class="row" style="margin-top:8px"><input class="input grow" type="time" value="18:00" data-time><button class="btn small" data-addr>Add</button></div>
      </div>
    </div>

    <h3>Body weight</h3>
    <div class="card">
      ${bw.entries.length > 1 ? lineChart(bw.entries.map((x) => ({ label: fmtDate(x.measured_at, { day: "numeric", month: "numeric" }), y: x.weight_kg })), { height: 140 }) : ""}
      ${bw.entries.length ? `<div class="muted" style="margin:6px 0">Latest: <b>${num(bw.entries[bw.entries.length - 1].weight_kg)} kg</b> · ${fmtDate(bw.entries[bw.entries.length - 1].measured_at)}</div>` : ""}
      <div class="row"><input class="input grow" inputmode="decimal" placeholder="e.g. 82.5" data-bw><button class="btn small" data-addbw>Log kg</button></div>
    </div>

    <h3>App</h3>
    <div class="card" data-home><span class="muted">Checking…</span></div>
    <p class="muted" style="text-align:center">GymBot · ${esc(u.first_name || "")} · ${me.total_workouts} workout${me.total_workouts === 1 ? "" : "s"} logged</p>`;

  renderHomeScreen($("[data-home]", view));

  const saveTz = async (tz) => {
    const res = await guard(() => api("/me", { method: "PATCH", body: { timezone: tz } }));
    if (res) { toast(`Timezone: ${res.timezone_label}`); renderSettings(); }
  };
  view.onchange = async (e) => {
    if (e.target.matches("[data-rest]")) {
      if (await guard(() => api("/me", { method: "PATCH", body: { rest_seconds: +e.target.value } }))) toast(`Rest timer: ${clock(+e.target.value)}`);
    } else if (e.target.matches("[data-toggle]")) {
      const id = e.target.closest("[data-r]").dataset.r;
      await guard(() => api(`/reminders/${id}`, { method: "PATCH", body: { enabled: e.target.checked } }));
    }
  };
  view.onclick = async (e) => {
    const t = e.target;
    if (t.closest("[data-day]")) { t.closest("[data-day]").classList.toggle("on"); return; }
    if (t.closest("[data-savetz]")) return saveTz($("[data-tz]", view).value);
    if (t.closest("[data-usetz]")) return saveTz(t.closest("[data-usetz]").dataset.usetz);
    if (t.closest("[data-delr]")) {
      const id = t.closest("[data-r]").dataset.r;
      if (await guard(() => api(`/reminders/${id}`, { method: "DELETE" }))) renderSettings();
      return;
    }
    if (t.closest("[data-addr]")) {
      const weekdays = [...view.querySelectorAll("[data-day].on")].map((b) => +b.dataset.day);
      const at_time = $("[data-time]", view).value;
      if (await guard(() => api("/reminders", { method: "POST", body: { weekdays, at_time } }))) { toast("Reminder added"); renderSettings(); }
      return;
    }
    if (t.closest("[data-addbw]")) {
      const v = parseFloat($("[data-bw]", view).value.replace(",", "."));
      if (!v) return toast("Enter your weight in kg");
      if (await guard(() => api("/bodyweight", { method: "POST", body: { weight_kg: v } }))) { toast("Logged"); renderSettings(); }
    }
  };
}

// ------------------------------------------------------------------ home screen shortcut

const isStandalone = () =>
  window.matchMedia("(display-mode: standalone)").matches || window.navigator.standalone === true;

function renderHomeScreen(box) {
  if (!box) return;
  if (!inTelegram) return renderAccountBox(box);
  box.innerHTML = `<div class="muted" style="margin-bottom:8px"><b>Install GymBot as its own app</b> — full screen, own icon, no Telegram around it. The bot sends you a sign-in link and code.</div>
    <button class="btn block" data-install>📲 Install as app</button>
    <div data-tgshortcut style="margin-top:12px"></div>`;
  box.querySelector("[data-install]").addEventListener("click", async () => {
    let url = null;
    try { url = (await (await fetch("/api/public/config")).json()).bot_url; } catch (_) { /* offline */ }
    if (url && tg.openTelegramLink) { tg.openTelegramLink(url); tg.close(); }
    else toast("Send /login to the bot in Telegram");
  });
  renderTelegramShortcut(box.querySelector("[data-tgshortcut]"));
}

function renderAccountBox(box) {
  const installHint = isStandalone()
    ? ""
    : `<div class="muted" style="margin-bottom:10px">Add this app to your home screen: on iPhone tap Share → <i>Add to Home Screen</i>; on Android tap ⋮ → <i>Add to Home screen</i> / <i>Install app</i>.</div>`;
  box.innerHTML = `${installHint}
    <div class="btns" style="margin-top:0"><button class="btn ghost" data-signout>Sign out</button><button class="btn danger" data-signoutall>Sign out everywhere</button></div>`;
  box.querySelector("[data-signout]").addEventListener("click", () => { session.clear(); renderLogin("Signed out."); });
  box.querySelector("[data-signoutall]").addEventListener("click", async () => {
    if (!(await confirmAsk("Sign out of GymBot on all your phones and browsers? (Telegram stays signed in.)"))) return;
    if (await guard(() => api("/auth/signout-all", { method: "POST" }))) { session.clear(); renderLogin("Signed out everywhere."); }
  });
}

function renderTelegramShortcut(box) {
  const supported = typeof tg.addToHomeScreen === "function" && tg.isVersionAtLeast && tg.isVersionAtLeast("8.0");
  if (!supported) { box.remove(); return; }
  const show = (status) => {
    if (status === "added") {
      box.innerHTML = `<span class="muted">✅ Telegram shortcut is on your home screen.</span>`;
    } else if (status === "unsupported") {
      box.remove();
    } else {
      box.innerHTML = `<div class="muted" style="margin-bottom:8px">Or add a shortcut that opens GymBot inside Telegram:</div>
        <button class="btn ghost block" data-addhome>Add Telegram shortcut</button>`;
      box.querySelector("[data-addhome]").addEventListener("click", () => {
        try { tg.addToHomeScreen(); } catch (_) { toast("Couldn't open the prompt — update Telegram and try again"); }
      });
    }
  };
  show("unknown");
  try { tg.checkHomeScreenStatus((status) => show(status)); } catch (_) { /* keep the button */ }
}

if (tg && typeof tg.onEvent === "function") {
  tg.onEvent("homeScreenAdded", () => {
    toast("Added to your home screen 🎉");
    const box = $("[data-tgshortcut]");
    if (box) box.innerHTML = `<span class="muted">✅ Telegram shortcut is on your home screen.</span>`;
  });
}

// ------------------------------------------------------------------ navigation

const renderers = { workout: renderWorkout, history: renderHistory, progress: renderProgress, plans: renderPlans, settings: renderSettings };
const titles = { workout: "Workout", history: "History", progress: "Progress", plans: "Plans", settings: "Settings" };

function switchTab(tab) {
  if (!renderers[tab]) tab = "workout";
  state.tab = tab;
  document.querySelectorAll(".tabs button").forEach((b) => b.classList.toggle("on", b.dataset.tab === tab));
  $("#title").textContent = titles[tab];
  if (location.hash !== `#${tab}`) history.replaceState(null, "", `#${tab}`);
  view.onclick = view.onchange = null;
  view.innerHTML = `<div class="empty">Loading…</div>`;
  window.scrollTo(0, 0);
  renderers[tab]();
}

document.querySelector(".tabs").addEventListener("click", (e) => {
  const b = e.target.closest("[data-tab]");
  if (b) { haptic("light"); switchTab(b.dataset.tab); }
});
window.addEventListener("hashchange", () => { if (!loginShown) switchTab(location.hash.slice(1)); });

async function refreshMe() {
  const me = await guard(() => api("/me"));
  if (!me) return;
  state.me = me;
  const el = $("#streak");
  el.hidden = me.streak_weeks < 1;
  el.textContent = `🔥 ${me.streak_weeks} week${me.streak_weeks > 1 ? "s" : ""}`;
}

async function boot() {
  if (!inTelegram) {
    if ("serviceWorker" in navigator) navigator.serviceWorker.register("sw.js").catch(() => { /* not critical */ });
    const code = new URLSearchParams(location.search).get("login");
    if (code) {
      // Drop the code from the address bar so it isn't bookmarked or installed with the app.
      history.replaceState(null, "", location.pathname + location.hash);
      await redeem(code);
      return;
    }
    if (!session.get()) {
      // No session: the server may still allow a local dev user; otherwise show sign-in.
      const res = await fetch("/api/me").catch(() => null);
      if (!res || !res.ok) { renderLogin(); return; }
    }
  }
  refreshMe();
  switchTab(location.hash.slice(1) || "workout");
}

boot();
