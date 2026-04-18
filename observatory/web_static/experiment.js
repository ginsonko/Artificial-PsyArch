const S = {
  datasets: null,
  runs: [],
  selectedDatasetKey: "",
  selectedRunId: "",
  activeJobId: "",
  jobPollTimer: null,
  lastJob: null,
  lastManifest: null,
  lastMetricsRows: [],
};

const E = {};

function B(id, fn) {
  const el = document.getElementById(id);
  if (el) el.addEventListener("click", fn);
}

function a(value) {
  return Array.isArray(value) ? value : [];
}

function n(value) {
  const num = +value || 0;
  return Number.isFinite(num) ? num.toFixed(4) : "0.0000";
}

function y(value) {
  return value ? "是" : "否";
}

function esc(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function tm(value) {
  const num = +value || 0;
  if (!num) return "-";
  try {
    return new Date(num).toLocaleString("zh-CN", { hour12: false });
  } catch {
    return String(value);
  }
}

function empty(text) {
  return `<div class="empty-state">${esc(text)}</div>`;
}

function rows(items, emptyText = "当前没有数据。") {
  return items && items.length
    ? items
        .map((item) => `<article class="mini-row"><div class="title">${esc(item.title || "-")}</div><div class="desc">${esc(item.desc || "-").replace(/\n/g, "<br>")}</div></article>`)
        .join("")
    : empty(emptyText);
}

async function G(url) {
  const response = await fetch(url);
  const data = await response.json();
  if (!response.ok || data.success === false) throw new Error(data.message || url);
  return data;
}

async function P(url, body) {
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
  const data = await response.json();
  if (!response.ok || data.success === false) throw new Error(data.message || url);
  return data;
}

function fbTo(el, message, kind) {
  if (!el) return;
  const k = String(kind || "ok");
  const stamp = tm(Date.now());
  el.textContent = `${stamp} | ${message}`;
  el.classList.remove("ok", "err", "busy");
  if (k === "err" || k === "busy" || k === "ok") el.classList.add(k);
}

function expFb(message, kind) {
  fbTo(E.expJobFeedback, message, kind);
}

function expImportFb(message, kind) {
  fbTo(E.expImportFeedback, message, kind);
}

function expClearFb(message, kind) {
  fbTo(E.expClearFeedback, message, kind);
}

function datasetKey(ref) {
  const source = String(ref?.source || "").trim();
  const rel = String(ref?.rel_path || "").trim();
  return source && rel ? `${source}::${rel}` : "";
}

function parseDatasetKey(key) {
  const raw = String(key || "");
  const parts = raw.split("::");
  if (parts.length < 2) return null;
  return { source: parts[0], rel_path: parts.slice(1).join("::") };
}

function getSelectedDatasetRef() {
  const key = String(E.expDatasetSelect?.value || S.selectedDatasetKey || "");
  const ref = parseDatasetKey(key);
  return ref && ref.source && ref.rel_path ? ref : null;
}

function findDatasetByKey(key) {
  const list = a(S.datasets?.datasets);
  return list.find((d) => datasetKey(d) === key) || null;
}

function renderDatasetMeta() {
  if (!E.expDatasetMeta) return;
  const key = String(E.expDatasetSelect?.value || S.selectedDatasetKey || "");
  const dsItem = findDatasetByKey(key);
  if (!dsItem) {
    E.expDatasetMeta.textContent = "尚未选择数据集。";
    return;
  }
  const meta = dsItem.meta || {};
  const did = meta.dataset_id || dsItem.rel_path || "-";
  const tb = meta.time_basis || "-";
  const ticks = meta.estimated_ticks ?? "-";
  const src = dsItem.source || "-";
  E.expDatasetMeta.textContent = `source=${src} | dataset_id=${did} | time_basis=${tb} | estimated_ticks=${ticks}`;
}

function renderDatasetPreview(previewTicks, totalTicks) {
  if (!E.expDatasetPreview) return;
  const items = a(previewTicks).map((t) => {
    const idx = t?.tick_index ?? "-";
    const ep = t?.episode_id || "-";
    const inText = String(t?.input_text || "");
    const isEmpty = Boolean(t?.input_is_empty) || !inText;
    const shown = isEmpty ? "（空 tick）" : inText;
    return {
      title: `tick ${idx} · ep ${ep}`,
      desc: `输入 / Input: ${shown}\n标签 / tags: ${(a(t?.tags).join(", ") || "-")}`,
    };
  });
  const extra = totalTicks != null ? `预览 ${items.length}/${totalTicks} tick` : `预览 ${items.length} tick`;
  E.expDatasetPreview.innerHTML = rows(items, "暂无预览数据。") + `<div class="meta">${esc(extra)}</div>`;
}

async function refreshDatasets(silent = false) {
  try {
    const res = await G("/api/experiment/datasets");
    S.datasets = res.data || null;
    renderDatasets();
    if (!silent) expFb("已刷新数据集列表。", "ok");
  } catch (error) {
    if (!silent) expFb(`刷新数据集失败: ${error.message}`, "err");
  }
}

function renderDatasets() {
  const list = a(S.datasets?.datasets);
  if (!E.expDatasetSelect) return;
  if (!list.length) {
    E.expDatasetSelect.innerHTML = `<option value="">（暂无数据集）</option>`;
    renderDatasetMeta();
    return;
  }
  const currentKey = String(E.expDatasetSelect.value || S.selectedDatasetKey || "");
  E.expDatasetSelect.innerHTML = list
    .map((d) => {
      const key = datasetKey(d);
      const meta = d.meta || {};
      const labelId = meta.dataset_id || String(d.rel_path || "").split("/").pop() || key;
      const ticks = meta.estimated_ticks != null ? `${meta.estimated_ticks}t` : "?t";
      const tb = meta.time_basis ? String(meta.time_basis) : "-";
      const tag = d.source === "imported" ? "Imported" : "Built-in";
      return `<option value="${esc(key)}">[${esc(tag)}] ${esc(labelId)} · ${esc(tb)} · ${esc(ticks)} · ${esc(d.rel_path || "")}</option>`;
    })
    .join("");
  const keep = list.some((d) => datasetKey(d) === currentKey) ? currentKey : datasetKey(list[0]);
  E.expDatasetSelect.value = keep;
  S.selectedDatasetKey = keep;
  renderDatasetMeta();
}

async function previewDataset() {
  const ref = getSelectedDatasetRef();
  if (!ref) return expFb("请选择数据集。", "err");
  expFb("正在获取数据集预览…", "busy");
  try {
    const res = await P("/api/experiment/datasets/preview", { dataset_ref: ref, limit: 24 });
    const data = res.data || {};
    renderDatasetPreview(data.preview_ticks || [], data.total_ticks);
    expFb(`预览成功：${data.dataset_id || "-"}（total=${data.total_ticks ?? "-"}）`, "ok");
  } catch (error) {
    expFb(`预览失败: ${error.message}`, "err");
  }
}

async function expandDataset() {
  const ref = getSelectedDatasetRef();
  if (!ref) return expFb("请选择数据集。", "err");
  expFb("正在展开数据集为 JSONL（expanded ticks）…", "busy");
  try {
    const res = await P("/api/experiment/datasets/expand", { dataset_ref: ref });
    const data = res.data || {};
    expFb(`展开完成：tick_count=${data.tick_count ?? "-"} | out=${data.out_path || "-"}`, "ok");
  } catch (error) {
    expFb(`展开失败: ${error.message}`, "err");
  }
}

async function importDataset() {
  const content = String(E.expImportContent?.value || "");
  const filename = String(E.expImportFilename?.value || "").trim();
  const format = String(E.expImportFormat?.value || "yaml").trim();
  if (!content.trim()) return expImportFb("请先粘贴数据集内容。", "err");
  expImportFb("正在导入并保存…", "busy");
  try {
    const res = await P("/api/experiment/datasets/import", { content, filename, format });
    const ref = res.data || {};
    expImportFb(`导入成功：${ref.rel_path || "-"}`, "ok");
    await refreshDatasets(true);
    const key = datasetKey(ref);
    if (key && E.expDatasetSelect) {
      E.expDatasetSelect.value = key;
      S.selectedDatasetKey = key;
      renderDatasetMeta();
    }
  } catch (error) {
    expImportFb(`导入失败: ${error.message}`, "err");
  }
}

async function clearRuntime() {
  expClearFb("正在清空运行态…", "busy");
  try {
    await P("/api/clear_runtime", {});
    expClearFb("已清空运行态（保留 HDB）。", "ok");
  } catch (error) {
    expClearFb(`清空失败: ${error.message}`, "err");
  }
}

async function clearHdb() {
  expClearFb("正在清空 HDB…", "busy");
  try {
    await P("/api/clear_hdb", {});
    expClearFb("HDB 已清空。", "ok");
  } catch (error) {
    expClearFb(`清空失败: ${error.message}`, "err");
  }
}

async function clearAll() {
  expClearFb("正在清空全部运行态…", "busy");
  try {
    await P("/api/clear_all", {});
    expClearFb("已清空全部（含 HDB）。", "ok");
  } catch (error) {
    expClearFb(`清空失败: ${error.message}`, "err");
  }
}

async function refreshRuns(silent = false) {
  try {
    const res = await G("/api/experiment/runs?limit=32");
    S.runs = a(res.data?.runs);
    renderRuns();
    if (!silent) expFb("已刷新运行列表。", "ok");
  } catch (error) {
    if (!silent) expFb(`刷新运行列表失败: ${error.message}`, "err");
  }
}

function renderRuns() {
  if (!E.expRunsList) return;
  const runs = a(S.runs);
  if (!runs.length) {
    E.expRunsList.innerHTML = empty("暂无实验运行。");
    return;
  }
  const selected = String(S.selectedRunId || "");
  E.expRunsList.innerHTML = runs
    .map((rid) => {
      const active = rid === selected ? "active" : "";
      return `<article class="mini-row rule-row ${active}" data-run-id="${esc(rid)}"><div class="title">${esc(rid)}</div><div class="desc">点击加载 manifest + metrics，并渲染图表。</div></article>`;
    })
    .join("");
  E.expRunsList.querySelectorAll("[data-run-id]").forEach((node) => {
    node.addEventListener("click", () => {
      const rid = String(node.getAttribute("data-run-id") || "").trim();
      if (rid) selectRun(rid, { reloadMetrics: true });
    });
  });
}

async function selectRun(runId, opts) {
  const options = opts && typeof opts === "object" ? opts : {};
  const rid = String(runId || "").trim();
  if (!rid) return;
  S.selectedRunId = rid;
  renderRuns();
  if (E.expRunMeta) E.expRunMeta.textContent = `正在加载 ${rid} …`;

  try {
    const manifestRes = await G(`/api/experiment/run/manifest?run_id=${encodeURIComponent(rid)}`);
    S.lastManifest = manifestRes.data || null;
  } catch (error) {
    S.lastManifest = null;
    expFb(`加载 manifest 失败: ${error.message}`, "err");
  }

  if (options.reloadMetrics) {
    await loadMetrics(rid);
  }
  renderRunSummary();
  renderCharts();
}

async function loadMetrics(runId) {
  const rid = String(runId || "").trim();
  if (!rid) return;
  const every = Math.max(1, Number(E.expDownsampleEvery?.value || 1) || 1);
  const limit = 5000;
  try {
    const res = await G(`/api/experiment/run/metrics?run_id=${encodeURIComponent(rid)}&every=${every}&limit=${limit}`);
    S.lastMetricsRows = a(res.data?.rows);
  } catch (error) {
    S.lastMetricsRows = [];
    expFb(`加载 metrics 失败: ${error.message}`, "err");
  }
}

function renderRunSummary() {
  if (!E.expRunSummary) return;
  const man = S.lastManifest || null;
  const rowsList = a(S.lastMetricsRows);
  if (!man) {
    E.expRunSummary.innerHTML = empty("尚未加载 manifest。");
    if (E.expRunMeta) E.expRunMeta.textContent = "";
    return;
  }
  const st = man.status || "-";
  const did = man.dataset?.dataset_id || "-";
  const total = man.dataset?.total_ticks ?? "-";
  const done = man.tick_done ?? rowsList.length;
  const exportNote = `export_json=${y(man.options?.export_json)} / export_html=${y(man.options?.export_html)}`;
  if (E.expRunMeta) E.expRunMeta.textContent = `status=${st} | dataset=${did} | done=${done}/${total} | ${exportNote}`;

  const last = rowsList.length ? rowsList[rowsList.length - 1] : {};
  E.expRunSummary.innerHTML = rowsList.length
    ? [
        {
          title: "最后一条 tick 指标（下采样后）",
          desc:
            `tick_index=${last.tick_index ?? "-"} | trace_id=${last.trace_id ?? "-"}\n` +
            `pool ER=${n(last.pool_total_er)} / EV=${n(last.pool_total_ev)} / CP=${n(last.pool_total_cp)}\n` +
            `CFS count=${last.cfs_signal_count ?? "-"} | NT COR=${n(last.nt_COR)} ADR=${n(last.nt_ADR)} SER=${n(last.nt_SER)}\n` +
            `Action executed=${last.action_executed_count ?? "-"} (focus=${last.action_executed_attention_focus ?? 0}, recall=${last.action_executed_recall ?? 0})`,
        },
        {
          title: "运行配置（关键项）",
          desc:
            `reset_mode=${man.options?.reset_mode || "-"}\n` +
            `time_sensor_time_basis=${man.options?.time_sensor_time_basis ?? "(no override)"}\n` +
            `max_ticks=${man.options?.max_ticks ?? "(none)"}\n` +
            `metrics_rows_loaded=${rowsList.length} (every=${Math.max(1, Number(E.expDownsampleEvery?.value || 1) || 1)})`,
        },
      ]
        .map((x) => `<article class="mini-row"><div class="title">${esc(x.title)}</div><div class="desc">${esc(x.desc).replace(/\n/g, "<br>")}</div></article>`)
        .join("")
    : empty("该 run 暂无 metrics（可能仍在运行，或被关闭导出/失败）。");
}

function _seriesFromRows(rowsList, key) {
  return a(rowsList).map((r) => Number(r?.[key] ?? 0) || 0);
}

function _tickFromRows(rowsList) {
  return a(rowsList).map((r) => Number(r?.tick_index ?? 0) || 0);
}

function renderLineChart(container, cfg) {
  if (!container) return;
  const rowsList = a(cfg?.rows);
  const series = a(cfg?.series);
  if (!rowsList.length || !series.length) {
    container.innerHTML = empty("暂无可绘制数据。");
    return;
  }

  const w = 920;
  const h = 260;
  const padL = 46;
  const padR = 16;
  const padT = 12;
  const padB = 28;

  const xs = _tickFromRows(rowsList);
  const xMin = xs[0] ?? 0;
  const xMax = xs[xs.length - 1] ?? xs.length - 1;
  const xSpan = Math.max(1e-9, xMax - xMin);

  let yMin = Infinity;
  let yMax = -Infinity;
  series.forEach((s) => {
    const vals = _seriesFromRows(rowsList, s.key);
    vals.forEach((v) => {
      if (!Number.isFinite(v)) return;
      if (v < yMin) yMin = v;
      if (v > yMax) yMax = v;
    });
  });
  if (!Number.isFinite(yMin) || !Number.isFinite(yMax)) {
    container.innerHTML = empty("图表数据无效。");
    return;
  }
  if (Math.abs(yMax - yMin) < 1e-9) yMax = yMin + 1.0;
  const ySpan = yMax - yMin;

  const X = (x) => padL + ((x - xMin) / xSpan) * (w - padL - padR);
  const Y = (y) => padT + (1 - (y - yMin) / ySpan) * (h - padT - padB);

  const gridLines = 4;
  const grid = [];
  for (let i = 0; i <= gridLines; i++) {
    const yy = padT + (i / gridLines) * (h - padT - padB);
    grid.push(`<line x1="${padL}" y1="${yy.toFixed(2)}" x2="${w - padR}" y2="${yy.toFixed(2)}" stroke="rgba(21,55,45,0.10)" stroke-width="1" />`);
  }

  const paths = series
    .map((s) => {
      const vals = _seriesFromRows(rowsList, s.key);
      let d = "";
      for (let i = 0; i < vals.length; i++) {
        const x = X(xs[i] ?? i);
        const yv = Y(vals[i]);
        d += i === 0 ? `M ${x.toFixed(2)} ${yv.toFixed(2)}` : ` L ${x.toFixed(2)} ${yv.toFixed(2)}`;
      }
      const color = s.color || "var(--accent)";
      return `<path d="${d}" fill="none" stroke="${color}" stroke-width="2.6" stroke-linecap="round" stroke-linejoin="round" opacity="0.92" />`;
    })
    .join("");

  const yLabelMax = `<text x="${padL}" y="${padT + 10}" fill="rgba(21,55,45,0.64)" font-size="11">${esc(String(yMax.toFixed(3)))}</text>`;
  const yLabelMin = `<text x="${padL}" y="${h - padB + 18}" fill="rgba(21,55,45,0.64)" font-size="11">${esc(String(yMin.toFixed(3)))}</text>`;
  const xLabelMin = `<text x="${padL}" y="${h - 8}" fill="rgba(21,55,45,0.64)" font-size="11">tick ${esc(String(xMin))}</text>`;
  const xLabelMax = `<text x="${w - padR - 60}" y="${h - 8}" fill="rgba(21,55,45,0.64)" font-size="11">tick ${esc(String(xMax))}</text>`;

  const tooltipId = `chart_tip_${Math.floor(Math.random() * 1e9)}`;
  const svg = `
    <svg class="chart-svg" viewBox="0 0 ${w} ${h}" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="${esc(cfg?.ariaLabel || "chart")}">
      ${grid.join("")}
      <rect x="${padL}" y="${padT}" width="${w - padL - padR}" height="${h - padT - padB}" fill="transparent" />
      ${paths}
      <line x1="${padL}" y1="${h - padB}" x2="${w - padR}" y2="${h - padB}" stroke="rgba(21,55,45,0.18)" stroke-width="1" />
      <line x1="${padL}" y1="${padT}" x2="${padL}" y2="${h - padB}" stroke="rgba(21,55,45,0.18)" stroke-width="1" />
      ${yLabelMax}
      ${yLabelMin}
      ${xLabelMin}
      ${xLabelMax}
      <rect x="${padL}" y="${padT}" width="${w - padL - padR}" height="${h - padT - padB}" fill="transparent" data-tooltip="${tooltipId}" style="cursor: crosshair;" />
    </svg>
  `;

  const legend = `<div class="chart-legend">${series
    .map((s) => {
      const color = s.color || "var(--accent)";
      return `<span class="chart-chip"><span class="chart-swatch" style="background:${esc(color)}"></span>${esc(s.name || s.key || "-")}</span>`;
    })
    .join("")}</div>`;

  container.innerHTML = `${svg}<div id="${tooltipId}" class="chart-tooltip"></div>${legend}`;

  const tip = document.getElementById(tooltipId);
  const rect = container.querySelector(`[data-tooltip="${tooltipId}"]`);
  if (!tip || !rect) return;

  const renderTip = (clientX) => {
    const bbox = rect.getBoundingClientRect();
    const px = Math.max(0, Math.min(1, (clientX - bbox.left) / Math.max(1, bbox.width)));
    const idx = Math.max(0, Math.min(rowsList.length - 1, Math.round(px * (rowsList.length - 1))));
    const row = rowsList[idx] || {};
    const lines = [`tick ${row.tick_index ?? idx}`].concat(series.map((s) => `${s.name || s.key}: ${n(row?.[s.key])}`));
    tip.textContent = lines.join(" | ");
    tip.classList.add("show");
  };
  rect.addEventListener("mousemove", (ev) => renderTip(ev.clientX));
  rect.addEventListener("mouseenter", () => tip.classList.add("show"));
  rect.addEventListener("mouseleave", () => tip.classList.remove("show"));
}

function renderCharts() {
  renderLineChart(E.expChartEnergy, {
    ariaLabel: "pool energy chart",
    rows: S.lastMetricsRows,
    series: [
      { key: "pool_total_er", name: "ER", color: "rgba(21,55,45,0.92)" },
      { key: "pool_total_ev", name: "EV", color: "rgba(190,116,65,0.95)" },
      { key: "pool_total_cp", name: "CP", color: "rgba(140,59,46,0.82)" },
    ],
  });
  renderLineChart(E.expChartCfs, {
    ariaLabel: "cfs chart",
    rows: S.lastMetricsRows,
    series: [
      { key: "cfs_dissonance_max", name: "dissonance", color: "rgba(140,59,46,0.88)" },
      { key: "cfs_pressure_max", name: "pressure", color: "rgba(190,116,65,0.92)" },
      { key: "cfs_complexity_max", name: "complexity", color: "rgba(21,55,45,0.84)" },
    ],
  });
  renderLineChart(E.expChartNt, {
    ariaLabel: "nt chart",
    rows: S.lastMetricsRows,
    series: [
      { key: "nt_COR", name: "COR", color: "rgba(140,59,46,0.86)" },
      { key: "nt_ADR", name: "ADR", color: "rgba(190,116,65,0.92)" },
      { key: "nt_SER", name: "SER", color: "rgba(21,55,45,0.84)" },
      { key: "nt_END", name: "END", color: "rgba(82,120,102,0.86)" },
    ],
  });
  renderLineChart(E.expChartAction, {
    ariaLabel: "action chart",
    rows: S.lastMetricsRows,
    series: [
      { key: "action_executed_count", name: "executed", color: "rgba(21,55,45,0.92)" },
      { key: "action_executed_attention_focus", name: "focus", color: "rgba(190,116,65,0.92)" },
      { key: "action_executed_recall", name: "recall", color: "rgba(140,59,46,0.86)" },
    ],
  });
}

function stopJobPolling() {
  if (S.jobPollTimer) {
    clearInterval(S.jobPollTimer);
    S.jobPollTimer = null;
  }
}

function startJobPolling(jobId, runId) {
  stopJobPolling();
  const jid = String(jobId || "").trim();
  if (!jid) return;
  S.activeJobId = jid;
  const rid = String(runId || "").trim();
  S.jobPollTimer = setInterval(async () => {
    await refreshJob(jid, rid);
  }, 900);
  refreshJob(jid, rid);
}

async function refreshJob(jobId, runId) {
  const jid = String(jobId || "").trim();
  if (!jid) return;
  try {
    const res = await G(`/api/experiment/jobs?job_id=${encodeURIComponent(jid)}`);
    const job = res.data || null;
    S.lastJob = job;
    if (E.expJobMeta && job) {
      const done = job.tick_done ?? 0;
      const planned = job.tick_planned ?? 0;
      const status = job.status || "-";
      E.expJobMeta.textContent = `job=${jid} | status=${status} | ${done}/${planned || "?"}`;
      if (E.expProgressBar) {
        const pct = planned ? Math.max(0, Math.min(1, done / Math.max(1, planned))) : 0;
        E.expProgressBar.style.width = `${Math.round(pct * 100)}%`;
      }
    }
    const status = String(job?.status || "");
    if (["completed", "failed", "cancelled", "stopped_max_ticks"].includes(status)) {
      stopJobPolling();
      expFb(`任务结束：status=${status}。`, status === "failed" ? "err" : "ok");
      await refreshRuns(true);
      const rid = String(runId || job?.run_id || "");
      if (rid) selectRun(rid, { reloadMetrics: true });
    }
  } catch (error) {
    expFb(`刷新任务进度失败: ${error.message}`, "err");
  }
}

async function startRun() {
  const ref = getSelectedDatasetRef();
  if (!ref) return expFb("请选择数据集。", "err");

  const resetMode = String(E.expResetMode?.value || "keep").trim() || "keep";
  const maxTicksRaw = String(E.expMaxTicks?.value || "").trim();
  const maxTicks = maxTicksRaw ? Math.max(1, Number(maxTicksRaw) || 0) : null;
  const tb = String(E.expTimeBasisOverride?.value || "").trim();
  const exportJson = Boolean(E.expExportJsonChk?.checked);
  const exportHtml = Boolean(E.expExportHtmlChk?.checked);

  const options = {
    reset_mode: resetMode,
    export_json: exportJson,
    export_html: exportHtml,
    time_sensor_time_basis: tb ? tb : null,
    max_ticks: maxTicks,
  };

  expFb("正在启动实验任务…", "busy");
  try {
    const res = await P("/api/experiment/run/start", { dataset_ref: ref, options });
    const jobId = res.data?.job_id;
    const runId = res.data?.run_id;
    if (!jobId) throw new Error("job_id missing in response");
    expFb(`任务已启动：job=${jobId} run=${runId || "-"}`, "ok");
    startJobPolling(jobId, runId);
  } catch (error) {
    expFb(`启动失败: ${error.message}`, "err");
  }
}

async function stopRun() {
  const jid = String(S.activeJobId || "").trim();
  if (!jid) return expFb("当前没有 active job。", "err");
  expFb(`正在请求停止任务 ${jid} …`, "busy");
  try {
    await P("/api/experiment/run/stop", { job_id: jid });
    expFb(`已请求停止：${jid}（等待后台响应）`, "ok");
  } catch (error) {
    expFb(`停止失败: ${error.message}`, "err");
  }
}

document.addEventListener("DOMContentLoaded", async () => {
  [
    "expBackBtn",
    "expRefreshDatasetsBtn",
    "expDatasetSelect",
    "expDatasetMeta",
    "expPreviewBtn",
    "expExpandBtn",
    "expDatasetPreview",
    "expImportFilename",
    "expImportFormat",
    "expImportContent",
    "expImportBtn",
    "expImportFeedback",
    "expClearRuntimeBtn",
    "expClearHdbBtn",
    "expClearAllBtn",
    "expClearFeedback",
    "expRefreshRunsBtn",
    "expResetMode",
    "expMaxTicks",
    "expTimeBasisOverride",
    "expExportJsonChk",
    "expExportHtmlChk",
    "expRunStartBtn",
    "expRunStopBtn",
    "expProgressBar",
    "expJobMeta",
    "expJobFeedback",
    "expRunsList",
    "expDownsampleEvery",
    "expRunSummary",
    "expRunMeta",
    "expChartEnergy",
    "expChartCfs",
    "expChartNt",
    "expChartAction",
  ].forEach((id) => {
    E[id] = document.getElementById(id);
  });

  if (E.expBackBtn) {
    E.expBackBtn.addEventListener("click", () => {
      try {
        window.history.back();
      } catch {
        window.location.href = "/";
      }
    });
  }
  B("expRefreshDatasetsBtn", () => refreshDatasets());
  B("expPreviewBtn", () => previewDataset());
  B("expExpandBtn", () => expandDataset());
  B("expImportBtn", () => importDataset());
  B("expClearRuntimeBtn", () => clearRuntime());
  B("expClearHdbBtn", () => clearHdb());
  B("expClearAllBtn", () => clearAll());
  B("expRefreshRunsBtn", () => refreshRuns());
  B("expRunStartBtn", () => startRun());
  B("expRunStopBtn", () => stopRun());

  if (E.expDatasetSelect) {
    E.expDatasetSelect.addEventListener("change", () => {
      S.selectedDatasetKey = String(E.expDatasetSelect.value || "");
      renderDatasetMeta();
    });
  }
  if (E.expDownsampleEvery) {
    E.expDownsampleEvery.addEventListener("change", () => {
      if (S.selectedRunId) selectRun(S.selectedRunId, { reloadMetrics: true });
    });
  }

  await refreshDatasets(true);
  await refreshRuns(true);
});

