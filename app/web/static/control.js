"use strict";
(async () => {
  const {$, esc, startLive, meta, typeLabel} = MGX;
  const m = await meta();
  const svg = $("#map");
  const NS = "http://www.w3.org/2000/svg";
  const hosp = {};
  m.hospitals.forEach((h) => { hosp[h.hospital_id] = h; });
  const counts = {};
  let selected = null;
  const filt = {type: "ALL", status: "ALL", cap: "ALL"};

  const caps = [...new Set(m.hospitals.flatMap((h) => h.capabilities))].sort();
  $("#f-cap").innerHTML += caps.map((c) => `<option value="${esc(c)}">${esc(c)}</option>`).join("");

  // projection: main city box, plus a strip for far satellites (not to scale)
  const SAT_LAT = 18.9;
  const city = m.hospitals.filter((h) => h.lat < SAT_LAT);
  const pad = 0.01;
  const minLat = Math.min(...city.map((h) => h.lat)) - pad, maxLat = Math.max(...city.map((h) => h.lat)) + pad;
  const minLon = Math.min(...m.hospitals.map((h) => h.lon)) - pad, maxLon = Math.max(...m.hospitals.map((h) => h.lon)) + pad;
  const top = 90, W = 900, H = 640;
  function proj(lat, lon) {
    const x = 30 + (lon - minLon) / (maxLon - minLon) * (W - 60);
    if (lat >= SAT_LAT) return [x, 45];
    return [x, top + (maxLat - lat) / (maxLat - minLat) * (H - top - 20)];
  }
  function el(name, attrs, parent) {
    const e = document.createElementNS(NS, name);
    for (const k in attrs) e.setAttribute(k, attrs[k]);
    if (parent) parent.appendChild(e);
    return e;
  }
  el("rect", {x: 0, y: 0, width: W, height: 70, fill: "#eef0f3"}, svg);
  el("text", {x: 10, y: 16}, svg).textContent = "Northern satellite area, about 70 km away (not to scale)";
  el("line", {x1: 0, y1: 70, x2: W, y2: 70, stroke: "#c9ced4"}, svg);
  if (m.demo_point) {
    const [dx, dy] = proj(m.demo_point.lat, m.demo_point.lon);
    el("path", {d: `M${dx - 7} ${dy}h14M${dx} ${dy - 7}v14`, stroke: "#1f3a5f", "stroke-width": 2}, svg);
    el("text", {x: dx + 9, y: dy + 14}, svg).textContent = "demo point";
  }
  const ambLayer = el("g", {id: "amb-layer"}, svg);
  const hospLayer = el("g", {}, svg);

  const nodes = {};
  m.hospitals.forEach((h) => {
    const [x, y] = proj(h.lat, h.lon);
    const g = el("g", {class: "hosp", tabindex: 0, role: "button", transform: `translate(${x},${y})`}, hospLayer);
    const shape = el("g", {}, g);
    const label = el("text", {x: 11, y: 4}, g);
    g.addEventListener("click", () => select(h.hospital_id));
    g.addEventListener("keydown", (e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); select(h.hospital_id); } });
    nodes[h.hospital_id] = {g, shape, label, sig: null};
  });

  function freeOf(hid) {
    const c = counts[hid] || {};
    const types = filt.type === "ALL" ? ["ICU", "VENTILATOR", "GENERAL"] : [filt.type];
    return types.reduce((s, t) => s + ((c[t] || {}).free || 0), 0);
  }
  function load(free) { return free === 0 ? "FULL" : free <= 3 ? "LOW" : "OK"; }

  function drawHosp(hid) {
    const n = nodes[hid];
    const free = freeOf(hid);
    const st = load(free);
    const visible = (filt.status === "ALL" || filt.status === st) && (filt.cap === "ALL" || hosp[hid].capabilities.includes(filt.cap));
    const sig = `${st}|${free}|${visible}|${selected === hid}`;
    if (sig === n.sig) return; // touch only changed elements
    n.sig = sig;
    n.g.style.display = visible ? "" : "none";
    n.g.classList.toggle("sel", selected === hid);
    n.shape.textContent = "";
    if (st === "OK") el("circle", {r: 7, fill: "#2f6b3a"}, n.shape);
    else if (st === "LOW") el("polygon", {points: "0,-8 8,7 -8,7", fill: "#8a5a00"}, n.shape);
    else el("rect", {x: -7, y: -7, width: 14, height: 14, fill: "#7a2630"}, n.shape);
    el("circle", {class: "ring", r: 11, fill: "none", stroke: "transparent"}, n.shape);
    n.label.textContent = `${hid} ${st} ${free}`;
    n.g.setAttribute("aria-label", `${hosp[hid].name}: ${st}, ${free} free ${filt.type === "ALL" ? "beds" : typeLabel[filt.type] + " beds"}`);
  }

  function drawTable() {
    $("#htable").innerHTML = m.hospitals.map((h) => {
      const c = counts[h.hospital_id] || {};
      const f = (t) => ((c[t] || {}).free ?? "N/A");
      const st = load(freeOf(h.hospital_id));
      return `<tr><td><a href="#" data-h="${h.hospital_id}">${esc(h.hospital_id)}</a> ${esc(h.name)}</td><td>${st}</td>
        <td class="num">${f("ICU")}</td><td class="num">${f("VENTILATOR")}</td><td class="num">${f("GENERAL")}</td>
        <td><span class="caps">${h.capabilities.map((x) => `<span class="cap">${esc(x)}</span>`).join("")}</span></td></tr>`;
    }).join("");
  }
  $("#htable").addEventListener("click", (e) => { const a = e.target.closest("a[data-h]"); if (a) { e.preventDefault(); select(a.dataset.h); } });

  function drawTotals() {
    const tot = {ICU: [0, 0, 0], VENTILATOR: [0, 0, 0], GENERAL: [0, 0, 0]};
    Object.values(counts).forEach((c) => Object.entries(c).forEach(([t, v]) => { tot[t][0] += v.free; tot[t][1] += v.reserved; tot[t][2] += v.occupied; }));
    $("#totals").innerHTML = Object.entries(tot).map(([t, v]) => `<tr><td>${typeLabel[t]}</td><td class="num">${v[0]}</td><td class="num">${v[1]}</td><td class="num">${v[2]}</td></tr>`).join("");
    $("#last-update").textContent = "Last update " + new Date().toLocaleTimeString();
  }

  function select(hid) {
    const prev = selected; selected = hid;
    if (prev) drawHosp(prev);
    drawHosp(hid);
    drawDetail();
  }
  function drawDetail() {
    if (!selected) return;
    const h = hosp[selected], c = counts[selected] || {};
    $("#detail").innerHTML = `<h2>${esc(h.hospital_id)} ${esc(h.name)}</h2>
      <table><thead><tr><th>Type</th><th class="num"><span class="st st-FREE">FREE</span></th><th class="num"><span class="st st-RESERVED">RESERVED</span></th><th class="num"><span class="st st-OCCUPIED">OCCUPIED</span></th></tr></thead><tbody>
      ${["ICU", "VENTILATOR", "GENERAL"].map((t) => { const v = c[t] || {}; return `<tr><td>${typeLabel[t]}</td><td class="num">${v.free ?? "N/A"}</td><td class="num">${v.reserved ?? "N/A"}</td><td class="num">${v.occupied ?? "N/A"}</td></tr>`; }).join("")}
      </tbody></table>
      <h3>Verified capabilities (synthetic)</h3><div class="caps">${h.capabilities.map((x) => `<span class="cap">${esc(x)}</span>`).join("") || "none"}</div>
      <p><a href="/hospital/${esc(h.hospital_id)}">Open hospital console</a></p>`;
  }

  const ambNodes = {};
  function drawAmb(a) {
    if (!a) return;
    $("#amb-status").textContent = a.available ? `${a.items.length} ambulances reported by the simulator process.` : `Ambulance positions: N/A (${a.reason}).`;
    const seen = new Set();
    (a.items || []).forEach((it) => {
      seen.add(it.id);
      const [x, y] = proj(it.lat, it.lon);
      let n = ambNodes[it.id];
      if (!n) { n = el("rect", {class: "amb", width: 6, height: 6, x: -3, y: -3}, ambLayer); ambNodes[it.id] = n; }
      n.setAttribute("transform", `translate(${x.toFixed(1)},${y.toFixed(1)}) rotate(45)`);
    });
    Object.keys(ambNodes).forEach((id) => { if (!seen.has(id)) { ambNodes[id].remove(); delete ambNodes[id]; } });
  }

  function redrawAll() { Object.keys(nodes).forEach(drawHosp); drawTable(); drawTotals(); drawDetail(); }
  [["f-type", "type"], ["f-status", "status"], ["f-cap", "cap"]].forEach(([id, k]) => $("#" + id).addEventListener("change", (e) => {
    filt[k] = e.target.value; Object.values(nodes).forEach((n) => { n.sig = null; }); redrawAll();
  }));
  $("#f-amb").addEventListener("change", () => { ambLayer.style.display = $("#f-amb").checked ? "" : "none"; });

  let tableTimer = 0;
  startLive({
    snapshot(s) { Object.assign(counts, s.hospitals); redrawAll(); drawAmb(s.ambulances); },
    batch(b) {
      const changed = Object.keys(b.hospitals || {});
      changed.forEach((h) => { counts[h] = b.hospitals[h]; drawHosp(h); });
      if (changed.length) { drawTotals(); if (changed.includes(selected)) drawDetail(); clearTimeout(tableTimer); tableTimer = setTimeout(drawTable, 300); }
      if (b.ambulances) drawAmb(b.ambulances);
    },
  });
})();
