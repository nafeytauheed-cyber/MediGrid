"use strict";
(async () => {
  const {$, esc, api, banner, meta, startLive, statusHtml, uuid, typeLabel} = MGX;
  const HID = window.HOSPITAL_ID;
  const m = await meta();
  const me = m.hospitals.find((h) => h.hospital_id === HID);
  if (!me) { $("#h-title").textContent = "Unknown hospital " + HID; return; }
  $("#h-title").textContent = `${HID} ${me.name}`;
  $("#caps").innerHTML = "Verified capabilities: " + me.capabilities.map((c) => `<span class="cap">${esc(c)}</span>`).join(" ");

  let beds = [];               // authoritative list from the server
  const pending = {};          // bed_id -> optimistic overlay
  let sel = 0;
  let skew = 0;                // local clock minus server clock (seconds)
  const filt = {type: "ALL", status: "ALL"};

  function view() {
    return beds.map((b) => Object.assign({}, b, pending[b.bed_id] || {}))
      .filter((b) => (filt.type === "ALL" || b.bed_type === filt.type) && (filt.status === "ALL" || b.status === filt.status));
  }

  function render() {
    const v = view();
    if (sel >= v.length) sel = Math.max(0, v.length - 1);
    const cnt = {FREE: 0, RESERVED: 0, OCCUPIED: 0};
    beds.forEach((b) => { cnt[(pending[b.bed_id] || b).status]++; });
    $("#summary").innerHTML = `${statusHtml("FREE")} ${cnt.FREE} &nbsp; ${statusHtml("RESERVED")} ${cnt.RESERVED} &nbsp; ${statusHtml("OCCUPIED")} ${cnt.OCCUPIED}`;
    $("#grid").innerHTML = v.map((b, i) => `<button type="button" class="bed${pending[b.bed_id] ? " pending" : ""}" role="option" id="bed-${esc(b.bed_id)}"
        aria-selected="${i === sel}" data-i="${i}" tabindex="-1">
        <div class="id">${esc(b.bed_id)}</div><div>${statusHtml(b.status)} <span class="small">${typeLabel[b.bed_type]}</span></div>
        <div class="pt">${b.patient_id ? esc(b.patient_id) : "&nbsp;"}${pending[b.bed_id] ? " (saving)" : ""}</div></button>`).join("");
    const cur = v[sel];
    if (cur) $("#grid").setAttribute("aria-activedescendant", "bed-" + cur.bed_id);
    renderActions(cur);
  }

  function renderActions(b) {
    if (!b) { $("#act").innerHTML = "<h2>Selected bed</h2><p class='muted'>No bed.</p>"; return; }
    let html = `<h2>Bed <span class="mono">${esc(b.bed_id)}</span></h2><p>${statusHtml(b.status)} ${typeLabel[b.bed_type]} &middot; version ${b.version}</p>`;
    if (b.patient_id) html += `<p>Patient <span class="mono">${esc(b.patient_id)}</span></p>`;
    if (b.status === "FREE") html += `<label>Patient id <input id="adm-pid" maxlength="48" value="WALK-${Math.random().toString(36).slice(2, 7).toUpperCase()}"></label>
      <button class="btn-primary" id="b-admit">Admit (A)</button>`;
    if (b.status === "OCCUPIED") html += `<div class="row"><button id="b-discharge">Discharge (D)</button><button id="b-transfer">Transfer (T)</button></div>`;
    if (b.status === "RESERVED" && b.purpose === "DISPATCH") {
      const left = b.expires_epoch ? Math.max(0, Math.round(b.expires_epoch - (Date.now() / 1000 - skew))) : null;
      html += `<p>Ambulance hold${left != null ? `, expires in <span class="countdown">${Math.floor(left / 60)}:${String(left % 60).padStart(2, "0")}</span>` : ""}</p>
        <button class="btn-primary" id="b-confirm">Confirm arrival (C)</button>`;
    }
    if (b.status === "RESERVED" && b.purpose === "TRANSFER") html += `<p>Held for an incoming transfer.</p>`;
    $("#act").innerHTML = html;
    const on = (id, fn) => { const e = $("#" + id); if (e) e.onclick = fn; };
    on("b-admit", () => admit(b)); on("b-discharge", () => discharge(b)); on("b-transfer", () => openTransfer(b)); on("b-confirm", () => confirmArrival(b));
  }

  async function load() {
    const r = await api("GET", `/api/hospitals/${HID}/beds`);
    if (r.status !== 200) { banner("Could not load beds: " + (r.data.error || r.status), "error"); return; }
    beds = r.data.beds;
    skew = Date.now() / 1000 - r.data.server_epoch;
    render();
  }

  // optimistic update with clear rollback on failure
  async function mutate(b, optimistic, call, okMsg) {
    pending[b.bed_id] = optimistic;
    render();
    const r = await call();
    delete pending[b.bed_id];
    if (r.status === 200) {
      const nb = r.data.bed;
      if (nb) beds = beds.map((x) => (x.bed_id === nb.bed_id ? Object.assign({}, x, nb) : x));
      banner(okMsg, "ok", 3000);
    } else {
      const cur = r.data.current && r.data.current.bed;
      if (cur) beds = beds.map((x) => (x.bed_id === cur.bed_id ? Object.assign({}, x, cur) : x));
      banner(`Change rolled back (${r.status} ${r.data.error || ""}): ${r.data.detail || ""}. Showing the server state.`, "error", 0);
    }
    render();
    load();
  }
  function admit(b) {
    const pid = ($("#adm-pid") || {}).value || "WALK-" + Math.random().toString(36).slice(2, 7).toUpperCase();
    mutate(b, {status: "OCCUPIED", patient_id: pid},
      () => api("POST", `/api/beds/${b.bed_id}/admit`, {patient_id: pid, expected_version: b.version, idempotency_key: uuid()}),
      `Admitted ${pid} to ${b.bed_id}.`);
  }
  function discharge(b) {
    mutate(b, {status: "FREE", patient_id: null}, () => api("POST", `/api/beds/${b.bed_id}/discharge`, {expected_version: b.version}),
      `Discharged from ${b.bed_id}.`);
  }
  async function confirmArrival(b) {
    mutate(b, {status: "OCCUPIED"}, () => api("POST", `/api/reservations/${b.reservation_id}/confirm`), `Arrival confirmed in ${b.bed_id}.`);
  }

  // transfer dialog
  let txBed = null;
  function fillTx() {
    const c = m.conditions[$("#tx-cond").value];
    $("#tx-type").innerHTML = c.bed_types.map((t) => `<option value="${t}">${t}</option>`).join("");
    const capable = m.hospitals.filter((h) => h.hospital_id !== HID && c.required.every((r) => h.capabilities.includes(r)));
    $("#tx-to").innerHTML = capable.map((h) => `<option value="${h.hospital_id}">${esc(h.hospital_id)} ${esc(h.name)}</option>`).join("") || "<option value=''>No capable hospital</option>";
  }
  $("#tx-cond").innerHTML = Object.entries(m.conditions).map(([k, c]) => `<option value="${k}">${esc(c.label)}</option>`).join("");
  $("#tx-cond").value = "GENERAL_ACUTE";
  $("#tx-cond").addEventListener("change", fillTx);
  fillTx();
  function openTransfer(b) {
    txBed = b;
    $("#tx-from").textContent = `From ${b.bed_id}, patient ${b.patient_id}`;
    $("#tx-dialog").showModal();
  }
  $("#tx-cancel").addEventListener("click", () => $("#tx-dialog").close());
  $("#tx-form").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    $("#tx-dialog").close();
    const r = await api("POST", "/api/transfers", {patient_id: txBed.patient_id, from_bed_id: txBed.bed_id, to_hospital_id: $("#tx-to").value,
      condition_code: $("#tx-cond").value, bed_type: $("#tx-type").value, priority: +$("#tx-prio").value, idempotency_key: uuid()});
    if (r.status === 201) banner(`Transfer requested to ${r.data.to_hosp_id}, bed ${r.data.to_bed_id}. Waiting for acceptance.`, "ok");
    else banner(`Transfer refused (${r.status}): ${r.data.error || ""} ${r.data.detail || ""}`, "error", 0);
    loadTransfers();
  });

  // transfers: incoming alerts with server-derived countdown
  let transfers = [];
  async function loadTransfers() {
    const r = await api("GET", `/api/hospitals/${HID}/transfers`);
    if (r.status !== 200) return;
    skew = Date.now() / 1000 - r.data.server_epoch;
    transfers = r.data.items;
    renderTransfers();
  }
  function renderTransfers() {
    const now = Date.now() / 1000 - skew;
    const inc = transfers.filter((t) => t.to_hosp_id === HID && t.saga_state === "AWAITING_ACCEPTANCE");
    $("#incoming").innerHTML = inc.map((t) => {
      const left = Math.max(0, Math.round(t.deadline_epoch - now));
      return `<div class="alert-transfer" role="alert"><strong>Incoming transfer</strong> from ${esc(t.from_hosp_id)}: patient <span class="mono">${esc(t.patient_id)}</span>,
        bed <span class="mono">${esc(t.to_bed_id)}</span>, priority ${t.priority}. Accept within <span class="countdown">${left} s</span>
        <div class="row"><button class="btn-primary" data-acc="${t.transfer_id}" data-v="${t.version}">Accept</button>
        <button data-rej="${t.transfer_id}" data-v="${t.version}">Reject</button></div></div>`;
    }).join("");
    $("#transfers").innerHTML = transfers.length ? `<table><thead><tr><th>Patient</th><th>From</th><th>To</th><th>State</th></tr></thead><tbody>${
      transfers.map((t) => `<tr><td class="mono">${esc(t.patient_id)}</td><td>${esc(t.from_hosp_id)}</td><td>${esc(t.to_hosp_id)}</td><td>${esc(t.saga_state)}${t.outcome_reason ? " (" + esc(t.outcome_reason) + ")" : ""}</td></tr>`).join("")}</tbody></table>` : "<p class='muted'>None.</p>";
  }
  $("#incoming").addEventListener("click", async (e) => {
    const b = e.target.closest("button");
    if (!b) return;
    const id = b.dataset.acc || b.dataset.rej;
    const kind = b.dataset.acc ? "accept" : "reject";
    b.disabled = true;
    const r = await api("POST", `/api/transfers/${id}/${kind}`, {expected_version: +b.dataset.v});
    if (r.status === 200) banner(`Transfer ${kind}ed.`, "ok"); else banner(`${kind} refused (${r.status}): ${r.data.error} ${r.data.detail || ""}`, "error", 0);
    loadTransfers(); load();
  });

  async function loadNotes() {
    const r = await api("GET", `/api/hospitals/${HID}/notifications`);
    if (r.status === 200) $("#notes").innerHTML = r.data.items.map((n) => `<li>${new Date(n.created_at).toLocaleTimeString()} ${esc(n.message)}</li>`).join("") || "<li class='muted'>None.</li>";
  }

  // keyboard navigation
  const grid = $("#grid");
  grid.addEventListener("click", (e) => { const b = e.target.closest(".bed"); if (b) { sel = +b.dataset.i; render(); grid.focus(); } });
  grid.addEventListener("keydown", (e) => {
    const v = view();
    const cols = Math.max(1, Math.round(grid.clientWidth / ((grid.firstElementChild || {}).offsetWidth || 150)));
    const b = v[sel];
    const k = e.key.toLowerCase();
    if (e.key === "ArrowRight") sel = Math.min(v.length - 1, sel + 1);
    else if (e.key === "ArrowLeft") sel = Math.max(0, sel - 1);
    else if (e.key === "ArrowDown") sel = Math.min(v.length - 1, sel + cols);
    else if (e.key === "ArrowUp") sel = Math.max(0, sel - cols);
    else if (e.key === "Enter") { const f = $("#act button"); if (f) f.focus(); e.preventDefault(); return; }
    else if (k === "a" && b && b.status === "FREE") { admit(b); e.preventDefault(); return; }
    else if (k === "d" && b && b.status === "OCCUPIED") { discharge(b); e.preventDefault(); return; }
    else if (k === "t" && b && b.status === "OCCUPIED") { openTransfer(b); e.preventDefault(); return; }
    else if (k === "c" && b && b.status === "RESERVED" && b.purpose === "DISPATCH") { confirmArrival(b); e.preventDefault(); return; }
    else return;
    e.preventDefault();
    render();
    const el = $("#grid [aria-selected='true']"); if (el) el.scrollIntoView({block: "nearest"});
  });
  ["f-type", "f-status"].forEach((id) => $("#" + id).addEventListener("change", (e) => { filt[id === "f-type" ? "type" : "status"] = e.target.value; sel = 0; render(); }));

  await load(); loadTransfers(); loadNotes();
  setInterval(() => { renderTransfers(); const b = view()[sel]; if (b && b.status === "RESERVED") renderActions(b); }, 1000);
  setInterval(loadTransfers, 5000);
  let t = 0;
  startLive({
    snapshot() { load(); loadTransfers(); },
    batch(d) {
      if (d.hospitals && d.hospitals[HID]) { clearTimeout(t); t = setTimeout(() => { load(); loadTransfers(); loadNotes(); }, 150); }
    },
  });
})();
