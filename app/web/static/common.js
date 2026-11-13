// Shared page code: API helper, identity dialog, banner, live SSE client with watchdog.
"use strict";

const MGX = (() => {
  const $ = (s, el) => (el || document).querySelector(s);
  const esc = (s) => String(s == null ? "" : s).replace(/[&<>"']/g, (c) => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"}[c]));

  async function api(method, url, body) {
    const opt = {method, headers: {"Accept": "application/json"}, credentials: "same-origin"};
    if (body !== undefined) { opt.headers["Content-Type"] = "application/json"; opt.body = JSON.stringify(body); }
    let r;
    try { r = await fetch(url, opt); } catch (e) { return {status: 0, data: {error: "NETWORK", detail: String(e)}}; }
    let data = null;
    try { data = await r.json(); } catch (e) { data = {error: "BAD_RESPONSE"}; }
    return {status: r.status, data, instance: r.headers.get("X-Instance"), headers: r.headers};
  }

  let bannerTimer = null;
  function banner(msg, kind, ms) {
    const b = $("#banner");
    b.textContent = msg; b.dataset.kind = kind || "warn"; b.hidden = false;
    clearTimeout(bannerTimer);
    if (ms !== 0) bannerTimer = setTimeout(() => { b.hidden = true; }, ms || 6000);
  }

  let metaP = null;
  function meta() { if (!metaP) metaP = api("GET", "/api/meta").then((r) => r.data); return metaP; }

  // ---------------------------------------------------------------- identity dialog
  function initIdentity() {
    const dlg = $("#ident-dialog");
    $("#ident-btn").addEventListener("click", async () => {
      const m = await meta();
      const sel = $("#ident-hosp");
      sel.innerHTML = m.hospitals.map((h) => `<option value="${esc(h.hospital_id)}">${esc(h.hospital_id)} ${esc(h.name)}</option>`).join("");
      if (window.MG.ident && window.MG.ident.role) {
        $("#ident-role").value = window.MG.ident.role;
        if (window.MG.ident.hospital_id) sel.value = window.MG.ident.hospital_id;
      }
      dlg.showModal();
    });
    $("#ident-cancel").addEventListener("click", () => dlg.close());
    $("#ident-form").addEventListener("submit", async (ev) => {
      ev.preventDefault();
      const r = await api("POST", "/api/identity", {role: $("#ident-role").value, user_id: $("#ident-user").value.trim(),
        hospital_id: $("#ident-hosp").value});
      if (r.status === 200) { dlg.close(); location.reload(); } else banner("Identity rejected: " + (r.data.error || r.status), "error");
    });
  }

  // ---------------------------------------------------------------- live stream (fetch-based SSE parser)
  // EventSource hides comment heartbeats, so the watchdog would not see them; this reader sees every line.
  const live = {handlers: [], state: "Connecting", instance: null, retryMs: 500, lastByte: 0, ctrl: null, queue: [], raf: 0};

  function setConn(state) {
    live.state = state;
    const c = $("#conn");
    c.dataset.state = state;
    $("#conn-text").textContent = state + (live.instance && state === "Connected" ? " to " + live.instance : "");
  }

  function setMode(m) {
    if (!m) return;
    const b = $("#mode-badge");
    const label = m.mode === "READY" ? "Fast path (Redis READY, gen " + m.generation + ")"
      : m.mode === "REBUILDING" ? "Degraded safe mode: Redis REBUILDING, PostgreSQL only"
      : "Degraded safe mode: PostgreSQL only";
    b.textContent = "Mode: " + label;
    b.dataset.mode = m.mode;
  }

  function dispatch(kind, data) {
    if (kind === "instance") { live.instance = data.instance; $("#inst").textContent = data.instance; setConn("Connected"); return; }
    if (kind === "snapshot") { setMode(data.mode); live.handlers.forEach((h) => h.snapshot && h.snapshot(data)); return; }
    if (kind === "batch") {
      live.queue.push(data);
      if (!live.raf) live.raf = requestAnimationFrame(() => {
        const q = live.queue; live.queue = []; live.raf = 0;
        for (const d of q) { if (d.mode) setMode(d.mode); live.handlers.forEach((h) => h.batch && h.batch(d)); }
      });
    }
  }

  async function connect() {
    setConn(live.instance ? "Reconnecting" : "Connecting");
    const ctrl = new AbortController();
    live.ctrl = ctrl;
    live.lastByte = Date.now();
    try {
      const r = await fetch("/api/stream", {signal: ctrl.signal, cache: "no-store", credentials: "same-origin"});
      if (!r.ok || !r.body) throw new Error("status " + r.status);
      const reader = r.body.getReader();
      const dec = new TextDecoder();
      let buf = "", ev = "message", data = [];
      for (;;) {
        const {value, done} = await reader.read();
        if (done) break;
        live.lastByte = Date.now();
        buf += dec.decode(value, {stream: true});
        let i;
        while ((i = buf.indexOf("\n")) >= 0) {
          const line = buf.slice(0, i).replace(/\r$/, ""); buf = buf.slice(i + 1);
          if (line === "") {
            if (data.length) { try { dispatch(ev, JSON.parse(data.join("\n"))); } catch (e) { /* ignore bad frame */ } }
            ev = "message"; data = [];
          } else if (line.startsWith(":")) { /* heartbeat */ }
          else if (line.startsWith("retry:")) live.retryMs = parseInt(line.slice(6), 10) || 500;
          else if (line.startsWith("event:")) ev = line.slice(6).trim();
          else if (line.startsWith("data:")) data.push(line.slice(5).trim());
        }
      }
    } catch (e) { /* fall through to reconnect */ }
    if (live.ctrl !== ctrl) return;
    setConn(navigator.onLine === false ? "Offline" : "Reconnecting");
    setTimeout(connect, live.retryMs);
  }

  function startLive(handler) {
    live.handlers.push(handler);
    if (live.started) return;
    live.started = true;
    connect();
    // watchdog: nothing (heartbeats included) for 5 s -> close and reopen
    setInterval(() => {
      if (Date.now() - live.lastByte > 5000 && live.ctrl) {
        const old = live.ctrl; live.ctrl = null; old.abort();
        setConn(navigator.onLine === false ? "Offline" : "Reconnecting");
        setTimeout(connect, live.retryMs);
        live.lastByte = Date.now();
      }
    }, 1000);
    window.addEventListener("offline", () => setConn("Offline"));
  }

  function statusHtml(s) { return `<span class="st st-${esc(s)}">${esc(s)}</span>`; }
  function uuid() { return (crypto.randomUUID && crypto.randomUUID()) || (Date.now() + "-" + Math.random().toString(16).slice(2)); }
  const typeLabel = {ICU: "ICU", VENTILATOR: "VENT", GENERAL: "GEN"};

  function markNav() {
    document.querySelectorAll(".top nav a").forEach((a) => {
      const href = a.getAttribute("href");
      if (location.pathname === href || location.pathname.startsWith(href + "/")) a.setAttribute("aria-current", "page");
    });
  }
  document.addEventListener("DOMContentLoaded", () => { initIdentity(); markNav(); });
  return {$, esc, api, banner, meta, startLive, statusHtml, uuid, typeLabel, live};
})();
