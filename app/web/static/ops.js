"use strict";
(async () => {
  const {$, esc, api, banner, startLive} = MGX;
  startLive({});
  const na = (o) => `N/A (${esc(o.reason)})`;
  const v = (x, unit) => (x === null || x === undefined ? "N/A (no samples)" : x + (unit || ""));

  async function refresh() {
    const r = await api("GET", "/api/ops/metrics");
    if (r.status === 401 || r.status === 403) { $("#ops-note").textContent = "Choose an operator identity to see operations data."; return; }
    if (r.status !== 200) { $("#ops-note").textContent = "Metrics unavailable: " + (r.data.error || r.status); return; }
    const m = r.data;
    const t = (label, val) => `<div class="tile"><div class="v">${val}</div><div class="l">${esc(label)}</div></div>`;
    const inv = m.invariants;
    $("#tiles").innerHTML = [
      t("Events per second (10 s avg, audit consumer)", v(m.events_per_second.last_10s_avg)),
      t("Bed search p95, last 60 s, both instances", v(m.search_latency.p95_ms, " ms") + `<div class="l">${m.search_latency.samples} samples</div>`),
      t("Reserve p95, last 60 s", v(m.reserve_latency.p95_ms, " ms") + `<div class="l">${m.reserve_latency.samples} samples</div>`),
      t("Consumer lag p95 (event age at audit)", v(m.consumer_lag.p95_ms, " ms")),
      t("DLQ depth", m.dlq_depth.available ? m.dlq_depth.total : na(m.dlq_depth)),
      t("Retries published", m.counters.retry_published),
      t("Outbox pending", m.outbox_pending.count + (m.outbox_pending.oldest_age_s != null ? `<div class="l">oldest ${m.outbox_pending.oldest_age_s} s</div>` : "")),
      t("Double allocations", `<span class="${inv.double_allocation.violations === 0 ? "pass" : "fail"}">${inv.double_allocation.violations}</span>`),
      t("Free ICU / VENT / GEN (PostgreSQL)", ["ICU", "VENTILATOR", "GENERAL"].map((k) => m.free_beds_pg.by_type[k]).join(" / ")),
      t("Free ICU / VENT / GEN (Redis cache)", m.free_beds_redis.available ? ["ICU", "VENTILATOR", "GENERAL"].map((k) => m.free_beds_redis.by_type[k]).join(" / ") : na(m.free_beds_redis)),
      t("Cache state", `${esc(m.cache.state)} gen ${m.cache.generation}`),
      t("SSE clients on this instance", `${m.sse.clients} <div class="l">${esc(m.instance)}</div>`),
    ].join("");
    $("#queues").innerHTML = m.queues.available ? `<table><thead><tr><th>Queue</th><th class="num">Messages</th><th class="num">Ready</th><th class="num">Unacked</th><th class="num">Consumers</th></tr></thead><tbody>${
      m.queues.items.map((q) => `<tr><td class="mono">${esc(q.name)}</td><td class="num">${q.messages ?? "N/A"}</td><td class="num">${q.ready ?? "N/A"}</td><td class="num">${q.unacked ?? "N/A"}</td><td class="num">${q.consumers ?? "N/A"}</td></tr>`).join("")}</tbody></table>
      <p class="small muted">Management API statistics refresh about every 5 s.</p>` : na(m.queues);
    $("#inv").innerHTML = `<table><tbody>${Object.entries(inv).filter(([k, x]) => typeof x === "object" && x.violations !== undefined)
      .map(([k, x]) => `<tr><td>${esc(k)}</td><td class="${x.violations === 0 ? "pass" : "fail"}">${x.violations} rows</td></tr>`).join("")}</tbody></table>`;
    $("#cache").innerHTML = `<p>State <strong>${esc(m.cache.state)}</strong>, generation ${m.cache.generation}. Breaker open on ${esc(m.instance)}: ${m.cache.breaker_open_here ? "yes" : "no"}.</p>
      <table class="small"><thead><tr><th>At</th><th>To</th><th>Gen</th><th>By</th><th>Detail</th></tr></thead><tbody>${
      m.cache.transitions.map((x) => `<tr><td>${esc(String(x.at).slice(11, 23))}</td><td>${["DEGRADED", "REBUILDING", "READY"][x.to_state]}</td><td>${x.generation}</td><td>${esc(x.actor)}</td><td>${esc(x.detail)}</td></tr>`).join("")}</tbody></table>`;
    $("#blocked").innerHTML = m.blocked_streams.length ? `<table class="small"><thead><tr><th>Group</th><th>Patient</th><th>Waiting seq</th><th>Missing seq</th></tr></thead><tbody>${
      m.blocked_streams.map((b) => `<tr><td>${esc(b.consumer_group)}</td><td class="mono">${esc(b.patient_id)}</td><td>${esc(b.waiting_seq)}</td><td>${esc(b.expected_seq)}</td></tr>`).join("")}</tbody></table>` : "<p class='muted'>None.</p>";
  }

  async function refreshDlq() {
    const r = await api("GET", "/api/ops/dlq");
    if (r.status !== 200) return;
    $("#dlq").innerHTML = Object.entries(r.data).map(([q, d]) => `<h3 class="mono">${esc(q)}</h3>` + (!d.available ? na(d) :
      d.items.length ? `<table class="small"><thead><tr><th>Event</th><th>Type</th><th>Error</th><th></th></tr></thead><tbody>${d.items.map((it) =>
        `<tr><td class="mono">${esc((it.event_id || "").slice(0, 8))}</td><td>${esc(it.type)}</td><td>${esc(it.error)}</td>
         <td><button class="btn-small" data-q="${esc(q)}" data-e="${esc(it.event_id)}">Replay</button></td></tr>`).join("")}</tbody></table>
         <button class="btn-small" data-q="${esc(q)}">Replay all</button>` : "<p class='muted small'>Empty.</p>")).join("");
    const f = await api("GET", "/api/chaos/flags");
    if (f.status === 200) {
      const e = Object.entries(f.data);
      $("#flags").innerHTML = e.length ? `<ul class="small">${e.map(([k, x]) => `<li class="mono">${esc(k)} = ${esc(x)}</li>`).join("")}</ul>` : "<p class='muted'>No fault or pause flags set.</p>";
    }
  }
  $("#dlq").addEventListener("click", async (e) => {
    const b = e.target.closest("button[data-q]"); if (!b) return;
    b.disabled = true;
    const r = await api("POST", "/api/ops/dlq/replay", {queue: b.dataset.q, event_id: b.dataset.e || null});
    banner(r.status === 200 ? `Replayed ${r.data.replayed.length} message(s) from ${r.data.queue}.` : "Replay failed: " + (r.data.error || r.status), r.status === 200 ? "ok" : "error");
    refreshDlq();
  });

  // ---------------------------------------------------------------- chaos bar
  const SCEN = [
    {id: "stampede", n: 1, title: "Last-bed stampede", text: "H-01 set to one free ICU bed; 50 users across both instances; separate per-user 429 check."},
    {id: "priority", n: 2, title: "Flood then critical", text: "Stop all q.transfer_saga consumers, confirm 10,000 routine + 1 critical in that queue, resume one consumer (prefetch 1)."},
    {id: "expiry", n: 3, title: "Reservation expiry", text: "TTL override 10 s, hold not confirmed; measure DB expiry to free and to UI batch."},
    {id: "poison", n: 4, title: "Poison and crash", text: "(A) handler error with 1/3/9 s confirmed retries then DLQ; (B) crash before ack, redelivery, crash-loop guard; replay after fault removed."},
    {id: "broker", n: 5, title: "Broker restart", cmd: "docker compose run --rm tools python -m sim.broker_restart\n# while it runs, in another terminal:\ndocker compose restart rabbitmq", verify: "parity"},
    {id: "failover", n: 6, title: "Instance stop and Redis pause", cmd: "docker compose run --rm tools python -m sim.failover\n# it prints when to run each of:\ndocker compose stop api-1\ndocker compose pause redis   (10 s)  then  docker compose unpause redis\ndocker compose restart redis\ndocker compose start api-1"},
  ];
  $("#scen").innerHTML = SCEN.map((s) => `<div class="card"><strong>${s.n}. ${esc(s.title)}</strong><p class="small">${esc(s.text || "")}</p>
    ${s.cmd ? `<pre class="cmd">${esc(s.cmd)}</pre><button class="btn-small" data-copy="${esc(s.cmd)}">Copy commands</button>` : ""}
    ${s.verify ? `<button class="btn-small" data-verify="${s.verify}">Verify event parity (last 30 min)</button>` : ""}
    ${!s.cmd ? (window.MG.chaos ? `<button class="btn-primary btn-small" data-run="${s.id}">Run</button>` : "<p class='muted small'>CHAOS_ENABLED is off.</p>") : ""}
    <div class="small" id="res-${s.id}"></div></div>`).join("");
  $("#scen").addEventListener("click", async (e) => {
    const b = e.target.closest("button"); if (!b) return;
    if (b.dataset.copy) { await navigator.clipboard.writeText(b.dataset.copy).catch(() => {}); banner("Commands copied.", "info", 2000); return; }
    if (b.dataset.verify) {
      const r = await api("POST", "/api/chaos/verify/parity", {since_minutes: 30});
      $("#res-broker").innerHTML = r.status === 200 ? parityHtml(r.data) : "Error " + (r.data.error || r.status);
      return;
    }
    if (b.dataset.run) {
      b.disabled = true;
      const r = await api("POST", `/api/chaos/${b.dataset.run}/run`);
      if (r.status !== 202) { banner("Run refused: " + (r.data.error || r.status), "error"); b.disabled = false; return; }
      $("#res-" + b.dataset.run).textContent = "Running...";
      pollRun(r.data.run_id, b);
    }
  });
  function parityHtml(d) {
    return `<p class="${d.pass ? "pass" : "fail"}">committed ${d.committed_events}, outbox pending ${d.outbox_pending}</p><table class="small"><thead><tr><th>Group</th><th class="num">Expected</th><th class="num">Processed</th><th class="num">DLQ</th><th class="num">Unresolved</th></tr></thead><tbody>${
      Object.entries(d.groups).map(([g, x]) => `<tr><td>${g}</td><td class="num">${x.expected}</td><td class="num">${x.processed}</td><td class="num">${x.dead_lettered}</td><td class="num">${x.unresolved}</td></tr>`).join("")}</tbody></table>`;
  }
  async function pollRun(id, btn) {
    for (;;) {
      await new Promise((res) => setTimeout(res, 1000));
      const r = await api("GET", "/api/chaos/runs");
      const run = (r.data || []).find((x) => x.run_id === id);
      if (!run || run.status === "RUNNING") continue;
      btn.disabled = false;
      const res = run.result || {};
      const checks = res.checks || {};
      $("#res-" + run.scenario).innerHTML = `<p class="${run.status === "PASS" ? "pass" : "fail"}">${run.status === "PASS" ? "" : esc(run.status)}</p>
        <ul>${Object.entries(checks).map(([k, ok]) => `<li class="${ok ? "pass" : "fail"}">${esc(k)}</li>`).join("")}</ul>
        ${res.error ? `<p>${esc(res.error)}</p>` : ""}
        <details><summary>Measured values</summary><pre class="cmd">${esc(JSON.stringify(res, null, 1))}</pre></details>`;
      return;
    }
  }

  refresh(); refreshDlq();
  setInterval(refresh, 2000);
  setInterval(refreshDlq, 5000);
})();
