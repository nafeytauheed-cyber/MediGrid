"use strict";
(async () => {
  const {$, esc, api, banner, meta, startLive, uuid} = MGX;
  const m = await meta();
  const conds = m.conditions;
  $("#condition").innerHTML = Object.entries(conds).map(([k, c]) => `<option value="${k}">${esc(c.label)} (${k})</option>`).join("");
  $("#condition").value = "CARDIAC_EMERGENCY";
  $("#lat").value = m.demo_point.lat; $("#lon").value = m.demo_point.lon;
  $("#patient_id").value = "PT-" + Math.random().toString(36).slice(2, 8).toUpperCase();
  function syncTypes() {
    const c = conds[$("#condition").value];
    $("#bed_type").innerHTML = c.bed_types.map((t) => `<option value="${t}">${t}</option>`).join("");
    $("#req-caps").innerHTML = "Mandatory capabilities: " + (c.required.length ? c.required.map((x) => `<span class="cap req">${esc(x)}</span>`).join(" ") : "none") +
      ` &middot; permitted bed types: ${c.bed_types.join(", ")}`;
  }
  $("#condition").addEventListener("change", syncTypes);
  syncTypes();
  startLive({});

  const form = () => ({condition_code: $("#condition").value, bed_type: $("#bed_type").value, priority: +$("#priority").value,
    patient_id: $("#patient_id").value.trim(), ambulance_id: $("#ambulance_id").value.trim(),
    lat: +$("#lat").value, lon: +$("#lon").value, radius_km: +$("#radius").value});

  function hospRow(h, excluded) {
    return `<tr class="${excluded ? "excluded" : ""}"><td>${esc(h.hospital_id)} ${esc(h.name)}</td><td class="num">${h.distance_km.toFixed(1)} km</td>
      <td class="num">${h.free}</td><td><span class="caps">${h.capabilities.map((c) => `<span class="cap">${esc(c)}</span>`).join("")}</span></td><td>${esc(h.reason)}</td></tr>`;
  }
  function showRationale(s) {
    const head = `<tr><th>Hospital</th><th class="num">Distance</th><th class="num">Free ${esc(s.bed_type)}</th><th>Capabilities</th><th>Reason</th></tr>`;
    const plain = s.plain_nearest ? `Plain nearest-bed choice (ignoring capability): <strong>${esc(s.plain_nearest.hospital_id)}</strong>
      ${s.plain_nearest.distance_km.toFixed(1)} km, ${s.plain_nearest.eligible ? "also capable" : "NOT capable for this condition, so SmartMatch skips it"}.` : "Plain nearest-bed choice: none in radius.";
    $("#rationale").innerHTML = `
      <p><strong>${esc(s.condition_label)}</strong>. Required: ${s.required_capabilities.map((x) => `<span class="cap req">${esc(x)}</span>`).join(" ") || "none"}.
      Search path: ${esc(s.path)} (${s.server_ms} ms on ${esc(s.instance)}).</p>
      ${s.result ? `<div class="banner" data-kind="error" style="display:block">NO_SUITABLE_HOSPITAL_OR_BED. ${esc(s.explanation)}</div>` : ""}
      <p>${plain}</p>
      <h3>Matched (${s.matched_total}), nearest first</h3>
      <table><thead>${head}</thead><tbody>${s.matched.map((h) => hospRow(h, false)).join("") || "<tr><td colspan=5>none</td></tr>"}</tbody></table>
      <h3>Excluded in radius (${s.excluded_total}), never offered as fallback</h3>
      <table><thead>${head}</thead><tbody>${s.excluded.map((h) => hospRow(h, true)).join("") || "<tr><td colspan=5>none</td></tr>"}</tbody></table>`;
  }
  async function search() {
    const f = form();
    const r = await api("GET", "/api/dispatch/search?" + new URLSearchParams(f));
    if (r.status !== 200) { banner(`Search failed: ${r.data.error || r.status} ${r.data.detail || ""}`, "error"); return; }
    showRationale(r.data);
  }
  $("#btn-search").addEventListener("click", search);
  setInterval(() => { if (hold && hold.status === "HELD") refreshHold(); }, 3000);

  let hold = null, tick = 0;
  function renderHold() {
    if (!hold) return;
    const left = Math.max(0, Math.round(new Date(hold.expires_at).getTime() / 1000 - (Date.now() / 1000 + hold.skew)));
    const live = hold.status === "HELD";
    const sm = hold.smartmatch || {};
    $("#result").innerHTML = `<h2>Hold</h2>
      <dl class="kv"><dt>Status</dt><dd>${MGX.statusHtml(hold.status === "HELD" ? "RESERVED" : hold.status === "CONFIRMED" ? "OCCUPIED" : "FREE")} ${esc(hold.status)}</dd>
      <dt>Hospital</dt><dd>${esc(hold.hospital_id)} ${esc(hold.hospital_name || "")}</dd>
      <dt>Bed</dt><dd class="mono">${esc(hold.bed_id)} (${esc(hold.bed_type)})</dd>
      <dt>Reservation</dt><dd class="mono small">${esc(hold.reservation_id)}</dd>
      <dt>Decided by</dt><dd>PostgreSQL (candidate from ${esc(hold.alloc_path || "")})${hold.replay ? ", replayed" : ""}</dd>
      ${sm.reason ? `<dt>Why</dt><dd>${esc(sm.reason)}</dd>` : ""}
      ${live ? `<dt>Expires in</dt><dd class="countdown" aria-live="off">${Math.floor(left / 60)}:${String(left % 60).padStart(2, "0")}</dd>` : ""}</dl>
      ${live ? `<div class="row"><button class="btn-primary" id="btn-confirm">Confirm arrival</button><button id="btn-cancel">Cancel hold</button></div>` : ""}`;
    if (live) {
      $("#btn-confirm").onclick = () => act("confirm");
      $("#btn-cancel").onclick = () => act("cancel");
    }
  }
  async function refreshHold() {
    if (!hold) return;
    const r = await api("GET", `/api/reservations/${hold.reservation_id}`);
    if (r.status === 200) { hold = Object.assign(hold, r.data, {skew: hold.skew}); renderHold(); }
  }
  async function act(kind) {
    const r = await api("POST", `/api/reservations/${hold.reservation_id}/${kind}`);
    if (r.status === 200) banner(kind === "confirm" ? "Arrival confirmed; patient admitted." : "Hold cancelled; bed released.", "ok");
    else banner(`${kind} refused (${r.status}): ${r.data.error || ""} ${r.data.detail || ""}`, "error", 0);
    refreshHold();
  }
  $("#req").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const f = form();
    f.idempotency_key = uuid();
    $("#btn-reserve").disabled = true;
    const r = await api("POST", "/api/dispatch/reserve", f);
    $("#btn-reserve").disabled = false;
    if (r.status === 201 || r.status === 200) {
      hold = Object.assign(r.data, {skew: Date.now() / 1000 - r.data.server_now});
      renderHold();
      clearInterval(tick); tick = setInterval(() => { renderHold(); }, 1000);
      $("#patient_id").value = "PT-" + Math.random().toString(36).slice(2, 8).toUpperCase();
      search();
    } else if (r.status === 429) {
      banner(`Rate limited: retry after ${r.headers.get("Retry-After")} s (${r.data.limit}).`, "error");
    } else if (r.data.result === "NO_SUITABLE_HOSPITAL_OR_BED") {
      banner("NO_SUITABLE_HOSPITAL_OR_BED: " + (r.data.explanation || ""), "error", 0);
      search();
    } else {
      banner(`Reserve failed (${r.status}): ${r.data.error || ""} ${r.data.detail || ""}`, "error", 0);
    }
  });
})();
