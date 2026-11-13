"use strict";
(async () => {
  const {$, esc, startLive, meta, typeLabel} = MGX;
  const m = await meta();
  const hosp = {};
  m.hospitals.forEach((h) => { hosp[h.hospital_id] = h; });
  const counts = {};
  let selected = null;
  const filt = {type: "ALL", status: "ALL", cap: "ALL"};

  const caps = [...new Set(m.hospitals.flatMap((h) => h.capabilities))].sort();
  $("#f-cap").innerHTML += caps.map((c) => `<option value="${esc(c)}">${esc(c)}</option>`).join("");

  // ---------------------------------------------------------------- map
  const map = L.map("map", {zoomControl: true, preferCanvas: false});
  const tiles = L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 19, attribution: "&copy; OpenStreetMap contributors",
  }).addTo(map);
  // fit once to the city cluster; live updates never touch the view again
  const city = m.hospitals.filter((h) => h.lat < 18.9);
  map.fitBounds(L.latLngBounds(city.map((h) => [h.lat, h.lon])).pad(0.08));

  let tileOk = 0, tileErr = 0;
  const msg = $("#tile-msg");
  function tileNotice() {
    if (tileOk === 0 && tileErr > 0) {
      msg.textContent = "Map tiles could not be loaded (no internet or tile server blocked). Hospital markers, live counts and the table below still work.";
      msg.hidden = false;
    }
  }
  tiles.on("tileload", () => { tileOk++; msg.hidden = true; });
  tiles.on("tileerror", () => { tileErr++; tileNotice(); });
  setTimeout(() => { if (tileOk === 0) { tileErr = Math.max(tileErr, 1); tileNotice(); } }, 8000);

  if (m.demo_point) {
    L.marker([m.demo_point.lat, m.demo_point.lon], {
      icon: L.divIcon({className: "demo-pt", html: "<span aria-hidden='true'>+</span><em>demo point</em>", iconSize: [14, 14], iconAnchor: [7, 7]}),
      keyboard: false, interactive: false,
    }).addTo(map);
  }

  function freeOf(hid) {
    const c = counts[hid] || {};
    const types = filt.type === "ALL" ? ["ICU", "VENTILATOR", "GENERAL"] : [filt.type];
    return types.reduce((s, t) => s + ((c[t] || {}).free || 0), 0);
  }
  function load(free) { return free === 0 ? "FULL" : free <= 3 ? "LOW" : "OK"; }
  function shortName(h) { return h.name.replace(/\s*\(H-\d+\)$/, ""); }

  const markers = {};
  m.hospitals.forEach((h) => {
    const mk = L.marker([h.lat, h.lon], {keyboard: true, title: h.name, riseOnHover: true});
    mk.on("click", () => select(h.hospital_id));
    mk.addTo(map);
    markers[h.hospital_id] = {mk, sig: null, shown: true};
  });

  function drawHosp(hid) {
    const n = markers[hid];
    const h = hosp[hid];
    const free = freeOf(hid);
    const st = load(free);
    const visible = (filt.status === "ALL" || filt.status === st) && (filt.cap === "ALL" || h.capabilities.includes(filt.cap));
    const known = !!counts[hid];
    const sig = `${st}|${free}|${visible}|${selected === hid}|${known}|${filt.type}`;
    if (sig === n.sig) return; // touch only changed markers; the map view is never moved here
    n.sig = sig;
    if (visible !== n.shown) { visible ? n.mk.addTo(map) : n.mk.remove(); n.shown = visible; }
    if (!visible) return;
    const tl = filt.type === "ALL" ? "free" : typeLabel[filt.type] + " free";
    n.mk.setIcon(L.divIcon({
      className: "hmk-wrap",
      iconSize: null, iconAnchor: [9, 9],
      html: `<div class="hmk${selected === hid ? " sel" : ""}"><span class="mk mk-${known ? st : "OK"}"></span>
        <span class="hmk-text"><b>${esc(hid)}</b> ${known ? `${st} &middot; ${free} ${tl}` : "N/A"}<br><span class="hmk-name">${esc(shortName(h))}</span></span></div>`,
    }));
    const el = n.mk.getElement();
    if (el) el.setAttribute("aria-label", `${h.name}: ${known ? `${st}, ${free} ${tl}` : "no data"}`);
  }

  function drawTable() {
    $("#htable").innerHTML = m.hospitals.map((h) => {
      const c = counts[h.hospital_id] || {};
      const f = (t) => ((c[t] || {}).free ?? "N/A");
      const st = load(freeOf(h.hospital_id));
      return `<tr class="${selected === h.hospital_id ? "row-sel" : ""}"><td><a href="#" data-h="${h.hospital_id}">${esc(h.hospital_id)}</a> ${esc(shortName(h))}</td>
        <td><span class="load"><span class="mk mk-${st}" aria-hidden="true"></span>${st}</span></td>
        <td class="num">${f("ICU")}</td><td class="num">${f("VENTILATOR")}</td><td class="num">${f("GENERAL")}</td>
        <td><span class="caps">${h.capabilities.map((x) => `<span class="cap">${esc(x)}</span>`).join("")}</span></td></tr>`;
    }).join("");
  }
  $("#htable").addEventListener("click", (e) => {
    const a = e.target.closest("a[data-h]");
    if (a) { e.preventDefault(); select(a.dataset.h, true); }
  });

  function drawTotals() {
    const tot = {ICU: [0, 0, 0], VENTILATOR: [0, 0, 0], GENERAL: [0, 0, 0]};
    Object.values(counts).forEach((c) => Object.entries(c).forEach(([t, v]) => { tot[t][0] += v.free; tot[t][1] += v.reserved; tot[t][2] += v.occupied; }));
    $("#totals").innerHTML = Object.entries(tot).map(([t, v]) => `<tr><td>${typeLabel[t]}</td><td class="num">${v[0]}</td><td class="num">${v[1]}</td><td class="num">${v[2]}</td></tr>`).join("");
    $("#last-update").textContent = "Live. Last update " + new Date().toLocaleTimeString();
  }

  function select(hid, pan) {
    const prev = selected; selected = hid;
    if (prev) drawHosp(prev);
    drawHosp(hid);
    drawDetail();
    drawTable();
    if (pan) map.panTo([hosp[hid].lat, hosp[hid].lon]); // only on explicit user action
  }
  function drawDetail() {
    if (!selected) return;
    const h = hosp[selected], c = counts[selected] || {};
    const st = load(freeOf(selected));
    $("#detail").innerHTML = `<h2>${esc(h.hospital_id)} ${esc(shortName(h))}</h2>
      <p><span class="load"><span class="mk mk-${st}" aria-hidden="true"></span>${st}</span></p>
      <table><thead><tr><th>Type</th><th class="num"><span class="st st-FREE">FREE</span></th><th class="num"><span class="st st-RESERVED">RESERVED</span></th><th class="num"><span class="st st-OCCUPIED">OCCUPIED</span></th></tr></thead><tbody>
      ${["ICU", "VENTILATOR", "GENERAL"].map((t) => { const v = c[t] || {}; return `<tr><td>${typeLabel[t]}</td><td class="num">${v.free ?? "N/A"}</td><td class="num">${v.reserved ?? "N/A"}</td><td class="num">${v.occupied ?? "N/A"}</td></tr>`; }).join("")}
      </tbody></table>
      <h3>Verified capabilities (synthetic)</h3><div class="caps">${h.capabilities.map((x) => `<span class="cap">${esc(x)}</span>`).join("") || "none"}</div>
      <p><a class="btn-link" href="/hospital/${esc(h.hospital_id)}">Open hospital console</a></p>`;
  }

  // ambulances: one marker per id, moved in place
  const ambLayer = L.layerGroup().addTo(map);
  const ambIcon = L.divIcon({className: "amb-wrap", html: "<span class='amb-dot'></span>", iconSize: [10, 10], iconAnchor: [5, 5]});
  const amb = {};
  function drawAmb(a) {
    if (!a) return;
    $("#amb-status").textContent = a.available ? `${a.items.length} ambulances reported by the simulator process.` : `Ambulance positions: N/A (${a.reason}).`;
    const seen = new Set();
    (a.items || []).forEach((it) => {
      seen.add(it.id);
      if (amb[it.id]) amb[it.id].setLatLng([it.lat, it.lon]);
      else amb[it.id] = L.marker([it.lat, it.lon], {icon: ambIcon, keyboard: false, title: `${it.id} ${it.status || ""}`}).addTo(ambLayer);
    });
    Object.keys(amb).forEach((id) => { if (!seen.has(id)) { ambLayer.removeLayer(amb[id]); delete amb[id]; } });
  }
  $("#f-amb").addEventListener("change", () => { $("#f-amb").checked ? ambLayer.addTo(map) : ambLayer.remove(); });

  function redrawAll() { Object.keys(markers).forEach(drawHosp); drawTable(); drawTotals(); drawDetail(); }
  [["f-type", "type"], ["f-status", "status"], ["f-cap", "cap"]].forEach(([id, k]) => $("#" + id).addEventListener("change", (e) => {
    filt[k] = e.target.value; redrawAll();
  }));

  redrawAll();
  let tableTimer = 0;
  startLive({
    snapshot(s) { Object.assign(counts, s.hospitals); redrawAll(); drawAmb(s.ambulances); },
    batch(b) {
      const changed = Object.keys(b.hospitals || {});
      changed.forEach((h) => { counts[h] = b.hospitals[h]; drawHosp(h); });
      if (changed.length) {
        drawTotals();
        if (changed.includes(selected)) drawDetail();
        clearTimeout(tableTimer); tableTimer = setTimeout(drawTable, 300);
      }
      if (b.ambulances) drawAmb(b.ambulances);
    },
  });
})();
