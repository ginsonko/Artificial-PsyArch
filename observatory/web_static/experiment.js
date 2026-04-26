const STATE = {
  protocol: null,
  datasets: [],
  datasetPreview: null,
  runs: [],
  selectedDatasetKey: "",
  selectedRunId: "",
  activeJobId: "",
  activeRunId: "",
  jobPollTimer: null,
  lastJob: null,
  lastManifest: null,
  lastMetricsRows: [],
  lastMetricsEvery: 1,
  lastMetricsFetchMs: 0,
  autoTunerDefaults: null,
  livePaused: false,
  liveDashboard: null,
  liveLastFetchMs: 0,
  liveActionLog: [],
  liveAutoTuneLog: [],
  llmPollTimer: null,
};
window.STATE = STATE;

const DOM = {};
window.DOM = DOM;
let LIVE_TIMER = null;

function byId(id) { return document.getElementById(id); }
function asArray(v) { return Array.isArray(v) ? v : []; }
function asNumber(v, d = 0) { const n = Number(v); return Number.isFinite(n) ? n : d; }
function formatNumber(v, digits = 4) { return asNumber(v, 0).toFixed(digits); }
function formatMaybe(v, digits = 4) { return asNumber(v, 0).toFixed(digits); }
function formatPercent(v, digits = 2) { return `${formatNumber(asNumber(v, 0) * 100, digits)}%`; }
function formatSigned(v, digits = 4) { const n = asNumber(v, 0); return `${n >= 0 ? '+' : ''}${formatNumber(n, digits)}`; }
function formatDelta(v, digits = 4) { return formatSigned(v, digits); }
function formatRange(minV, maxV, digits = 4) { return `${formatNumber(minV, digits)} ~ ${formatNumber(maxV, digits)}`; }
function formatDuration(ms) { const n = asNumber(ms, 0); return n >= 1000 ? `${formatNumber(n / 1000, 2)} s` : `${formatNumber(n, 0)} ms`; }
function formatCount(v) { return String(Math.round(asNumber(v, 0))); }
function formatBool(v) { return v ? '是' : '否'; }
function formatTime(v) { const n = asNumber(v, 0); if (!n) return '-'; try { return new Date(n).toLocaleString('zh-CN', { hour12: false }); } catch { return String(v); } }
function esc(v) { return String(v ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;'); }
function emptyState(text) { return `<div class="empty-state">${esc(text)}</div>`; }
function truncateText(value, maxLen = 180) { const text = String(value ?? ''); return text.length <= maxLen ? text : `${text.slice(0, maxLen)}…`; }
function miniRow(title, desc) { return `<article class="mini-row"><div class="title">${esc(title || '-')}</div><div class="desc">${esc(desc || '-').replace(/\n/g,'<br>')}</div></article>`; }
function metricCard(label, value, note = '') { return `<article class="metric-card"><div class="label">${esc(label)}</div><div class="value">${esc(value)}</div><div class="note">${esc(note)}</div></article>`; }
function setFeedback(el, message, kind='ok') { if(!el) return; el.textContent = `${formatTime(Date.now())} | ${message}`; el.classList.remove('ok','err','busy'); el.classList.add(kind); }
function pushBounded(list, item, maxSize = 80) { const arr = asArray(list).slice(); arr.push(item); return arr.slice(-Math.max(1, maxSize)); }
function actionKindLabel(kind) {
  const k = String(kind || '').trim();
  const mapping = {
    attention_focus: '注意聚焦（attention_focus）',
    attention_focus_mode: '注意聚焦模式（attention_focus_mode）',
    attention_diverge_mode: '注意发散模式（attention_diverge_mode）',
    recall: '回忆（recall）',
  };
  return mapping[k] || (k || '未知行动');
}
function metricLabel(key) {
  const mapping = {
    pool_total_er: '实能量（ER）',
    pool_total_ev: '虚能量（EV）',
    pool_total_cp: '认知压（CP）',
    pool_active_item_count: '状态池活跃条目数',
    pool_high_cp_item_count: '高认知压条目数',
    attention_memory_item_count: '注意力记忆条目数',
    attention_cam_item_count: '注意力当前工作集条目数',
    cam_item_count: '当前工作集条目数',
    attention_cam_item_cap: '注意力 CAM 上限',
    attention_state_pool_candidate_count: '状态池候选条目数',
    attention_skipped_memory_item_count: '记忆跳过条目数',
    attention_consumed_total_energy: '注意力消耗总能量',

    external_sa_count: '外源 SA 数',
    internal_sa_count: '内源 SA 数',
    merged_flat_token_count: '合流后 flat token 数',
    cache_input_flat_token_count: '输入 flat token 数',
    cache_residual_flat_token_count: '中和后 flat token 数',
    landed_flat_token_count: '落地 flat token 数',
    internal_flat_token_count: '内源 flat token 数',
    internal_minus_external_sa_count: '内外源 SA 差值',
    internal_to_external_sa_ratio: '内外源 SA 比值',
    input_len: '原始输入字符数',

    internal_candidate_structure_count: '内源候选结构数',
    internal_selected_structure_count: '内源入选结构数',
    internal_fragment_count: '内源片段数',
    internal_source_structure_count: '内源来源结构数',
    internal_resolution_raw_sa_count: '内源原始 SA 数',
    internal_resolution_selected_sa_count: '内源入选 SA 数',
    internal_resolution_budget_sa_cap: '内源预算 SA 上限',
    internal_resolution_max_structures_per_tick: '内源结构预算上限',
    internal_resolution_detail_budget: '内源细节分辨率预算',
    internal_resolution_detail_budget_base: '内源基础细节预算',
    internal_resolution_detail_budget_adr_gain: '内源细节预算肾上腺素增益',
    internal_resolution_raw_unit_count: '内源原始细节单元数',
    internal_resolution_raw_unit_count_total: '内源原始细节总单元数',
    internal_resolution_raw_unit_count_total_candidates: '内源候选细节总单元数',
    internal_resolution_selected_unit_count: '内源已选细节单元数',
    internal_resolution_structure_count_selected: '内源已选结构数量',
    internal_resolution_cursor_count: '内源分辨率游标数',
    internal_resolution_history_count: '内源分辨率历史数',
    internal_resolution_history_bucket_count: '内源分辨率疲劳桶数',
    internal_resolution_focus_credit_count: '内源聚焦信用条目数',
    internal_csa_count: '内源结构片段数',
    structure_round_count: '结构级查存轮次',
    stimulus_round_count: '刺激级查存轮次',
    stimulus_new_structure_count: '刺激级新建结构数',
    cs_action_count: '认知拼接动作次数',
    cs_candidate_count: '认知拼接候选数',
    cs_created_count: '认知拼接新建数',
    cs_extended_count: '认知拼接扩展数',
    cs_merged_count: '认知拼接合并数',
    cs_reinforced_count: '认知拼接强化数',
    cs_seed_event_count: '认知拼接事件种子数',
    cs_seed_structure_count: '认知拼接结构种子数',
    cs_enabled: '认知拼接启用状态',
    cs_narrative_top_total_energy: '认知拼接叙事总能量',
    cs_narrative_top_grasp: '认知拼接叙事把握感',

    cfs_dissonance_max: '违和感峰值',
    cfs_pressure_max: '压力峰值',
    cfs_grasp_max: '把握感峰值',
    cfs_complexity_max: '复杂度峰值',
    cfs_surprise_max: '惊讶感峰值',
    cfs_repetition_max: '重复感峰值',
    cfs_expectation_max: '期待感峰值',
    cfs_correctness_max: '正确感峰值',

    cfs_dissonance_live_total_energy: '违和感总量',
    cfs_correctness_live_total_energy: '正确感总量',
    cfs_expectation_live_total_energy: '期待总量',
    cfs_pressure_live_total_energy: '压力总量',
    cfs_grasp_live_total_energy: '把握感总量',
    cfs_complexity_live_total_energy: '复杂度总量',
    cfs_surprise_live_total_energy: '惊讶感总量',
    cfs_repetition_live_total_energy: '重复感总量',
    cfs_total_strength: '认知感受总强度',

    cfs_signal_count: '感受信号总数',
    cfs_dissonance_count: '违和触发次数',
    cfs_surprise_count: '惊讶触发次数',
    cfs_repetition_count: '重复感触发次数',
    cfs_expectation_count: '期待感触发次数',
    cfs_pressure_count: '压力触发次数',
    cfs_grasp_count: '把握感触发次数',
    cfs_pressure_unverified_count: '未证实压力次数',
    cfs_pressure_unverified_max: '未证实压力峰值',

    cfs_dissonance_live_item_count: '违和感覆盖对象数',
    cfs_dissonance_live_attribute_count: '违和感属性条目数',
    cfs_dissonance_live_total_ev: '违和感总虚能量',
    cfs_correctness_live_item_count: '正确感覆盖对象数',
    cfs_correctness_live_attribute_count: '正确感属性条目数',
    cfs_correctness_live_total_er: '正确感总实能量',
    cfs_expectation_live_item_count: '期待覆盖对象数',
    cfs_expectation_live_attribute_count: '期待属性条目数',
    cfs_pressure_live_item_count: '压力覆盖对象数',
    cfs_pressure_live_attribute_count: '压力属性条目数',
    cfs_grasp_live_item_count: '把握感覆盖对象数',
    cfs_grasp_live_attribute_count: '把握感属性条目数',
    cfs_surprise_live_item_count: '惊讶感覆盖对象数',
    cfs_surprise_live_attribute_count: '惊讶感属性条目数',
    cfs_repetition_live_item_count: '重复感覆盖对象数',
    cfs_repetition_live_attribute_count: '重复感属性条目数',
    cfs_complexity_count: '复杂度触发次数',
    cfs_complexity_live_item_count: '复杂度覆盖对象数',
    cfs_complexity_live_attribute_count: '复杂度属性条目数',
    cfs_complexity_live_total_er: '复杂度总实能量',
    cfs_complexity_live_total_ev: '复杂度总虚能量',
    cfs_correct_event_count: '正确事件触发次数',
    cfs_correct_event_max: '正确事件峰值',
    cfs_correct_event_live_item_count: '正确事件覆盖对象数',
    cfs_correct_event_live_attribute_count: '正确事件属性条目数',
    cfs_correct_event_live_total_energy: '正确事件总能量',
    cfs_correct_event_live_total_er: '正确事件总实能量',

    rwd_pun_rwd: '系统奖励信号',
    rwd_pun_pun: '系统惩罚信号',
    teacher_rwd: '教师奖励',
    teacher_pun: '教师惩罚',
    teacher_applied_count: '教师信号应用次数',
    label_teacher_rwd: '教师奖励标签',
    label_teacher_pun: '教师惩罚标签',
    label_should_call_weather: '天气调用标签',

    nt_COR: '皮质醇（COR）',
    nt_ADR: '肾上腺素（ADR）',
    nt_SER: '血清素（SER）',
    nt_END: '内啡肽（END）',
    nt_DA: '多巴胺（DA）',
    nt_OXY: '催产素（OXY）',

    action_executed_count: '行动执行总数',
    action_attempted_count: '行动尝试总数',
    action_scheduled_weather_stub: '天气查询调度次数',
    action_executed_attention_focus: '注意聚焦执行次数',
    action_executed_recall: '回忆执行次数',
    action_executed_weather_stub: '天气查询执行次数',
    action_attempted_attention_diverge_mode: '注意发散尝试次数',
    action_attempted_attention_focus: '注意聚焦尝试次数',
    action_attempted_diverge_mode: '发散模式尝试次数',
    action_attempted_focus_mode: '聚焦模式尝试次数',
    action_attempted_recall: '回忆尝试次数',
    action_attempted_weather_stub: '天气查询尝试次数',
    action_executed_attention_diverge_mode: '注意发散执行次数',
    action_executed_diverge_mode: '发散模式执行次数',
    action_executed_focus_mode: '聚焦模式执行次数',
    action_drive_max: '最大行动驱动力',
    action_drive_mean: '平均行动驱动力',
    action_drive_active_count: '活跃行动节点数',
    action_node_count: '行动节点总数',

    time_sensor_bucket_update_count: '时间桶更新数',
    time_sensor_attribute_binding_count: '时间属性绑定数',
    time_sensor_memory_sample_count: '时间感受记忆样本数',
    time_sensor_delayed_task_registered_count: '延迟任务注册次数',
    time_sensor_delayed_task_updated_count: '延迟任务更新次数',
    time_sensor_delayed_task_executed_count: '延迟任务执行次数',
    time_sensor_delayed_task_pruned_count: '延迟任务清理次数',
    time_sensor_delayed_task_capacity_skip_count: '延迟任务容量跳过次数',
    time_sensor_delayed_task_table_size: '延迟任务表大小',
    time_sensor_bucket_energy_max: '时间桶能量峰值',
    time_sensor_bucket_energy_sum: '时间桶能量总和',
    time_sensor_delayed_task_skipped_capacity_count: '延迟任务容量跳过次数',
    time_sensor_memory_used_count: '时间感受取样记忆数',

    map_count: '记忆赋能条目数',
    map_feedback_count: '记忆反馈条目数',
    map_apply_count: '记忆赋能应用次数',
    map_total_er: '记忆赋能总实能量',
    map_total_ev: '记忆赋能总虚能量',
    map_feedback_total_ev: '记忆反馈总虚能量',

    hdb_structure_count: 'HDB 结构总数',
    hdb_group_count: 'HDB 结构组总数',
    hdb_episodic_count: '情节记忆总数',

    timing_total_logic_ms: '总逻辑耗时',
    timing_structure_level_ms: '结构级耗时',
    timing_stimulus_level_ms: '刺激级耗时',
    timing_cache_neutralization_ms: '缓存中和耗时',
    timing_induction_and_memory_ms: '归纳与记忆耗时',
    timing_attention_ms: '注意力耗时',
    timing_cognitive_stitching_ms: '认知拼接耗时',
    timing_iesm_ms: 'IESM 耗时',
    timing_action_ms: '行动耗时',
    timing_emotion_ms: '情绪耗时',
    timing_cfs_ms: '认知感受耗时',
    timing_time_sensor_ms: '时间感受器耗时',

    sensor_feature_sa_count: '基础刺激元数量',
    sensor_attribute_sa_count: '属性刺激元数量',
    sensor_csa_bundle_count: '结构包数量',
    sensor_echo_frames_used_count: '参与的残响帧数',
    sensor_echo_current_round: '当前轮残响数',
    sensor_echo_pool_size: '残响池大小',

    maintenance_before_active_item_count: '维护前活跃条目数',
    maintenance_after_active_item_count: '维护后活跃条目数',
    maintenance_delta_active_item_count: '维护活跃条目变化',
    maintenance_before_high_cp_item_count: '维护前高压条目数',
    maintenance_after_high_cp_item_count: '维护后高压条目数',
    maintenance_delta_high_cp_item_count: '维护高压条目变化',
    maintenance_event_count: '维护事件数',

    pool_apply_merged_item_count: '状态池合并条目数',
    pool_apply_new_item_count: '状态池新增条目数',
    pool_apply_updated_item_count: '状态池更新条目数',
    pool_apply_total_delta_cp: '状态池认知压增量',
    pool_apply_total_delta_er: '状态池实能量增量',
    pool_apply_total_delta_ev: '状态池虚能量增量',

    episode_repeat_index: '情节重复索引',
    tick_in_episode_index: '情节内 Tick 序号',
  };
  if (mapping[key]) return mapping[key];
  return String(key || '')
    .replace(/^cfs_/, '认知感受_')
    .replace(/^nt_/, '递质_')
    .replace(/^timing_/, '耗时_')
    .replace(/^action_/, '行动_')
    .replace(/^time_sensor_/, '时间感受器_')
    .replace(/^sensor_/, '感受器_')
    .replace(/^pool_/, '状态池_')
    .replace(/^map_/, '记忆赋能_')
    .replace(/^maintenance_/, '维护_')
    .replace(/^internal_/, '内源_')
    .replace(/^external_/, '外源_')
    .replace(/_/g, ' ');
}

function getSeriesValues(rows, key) {
  return asArray(rows).map((row)=> Number(row?.[key])).filter((v)=> Number.isFinite(v));
}

function isMeaninglessSeries(rows, key) {
  if (!key || /(?:^|_)(enabled|disabled)$/.test(key)) return true;
  if (['cs_narrative_top_grasp','cs_narrative_top_total_energy'].includes(String(key))) {
    const vals = getSeriesValues(rows, key);
    if (!vals.length || vals.every((v)=> v === 0)) return true;
  }
  const values = getSeriesValues(rows, key);
  if (!values.length) return true;
  const min = Math.min(...values);
  const max = Math.max(...values);
  if (min === 0 && max === 0) return true;
  if (min === max) return true;
  return false;
}
async function apiGet(url, timeoutMs = 12000){
  const controller = new AbortController();
  const timer = setTimeout(()=> controller.abort(), timeoutMs);
  try {
    const r = await fetch(url, { signal: controller.signal });
    const data = await r.json();
    if(!r.ok || data.success===false) throw new Error(data.message || url);
    return data;
  } catch (error) {
    if (error?.name === 'AbortError') throw new Error(`请求超时：${url}`);
    throw error;
  } finally {
    clearTimeout(timer);
  }
}
async function apiPost(url, body, timeoutMs = 12000){
  const controller = new AbortController();
  const timer = setTimeout(()=> controller.abort(), timeoutMs);
  try {
    const r = await fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body||{}),signal: controller.signal});
    const data = await r.json();
    if(!r.ok || data.success===false) throw new Error(data.message || url);
    return data;
  } catch (error) {
    if (error?.name === 'AbortError') throw new Error(`请求超时：${url}`);
    throw error;
  } finally {
    clearTimeout(timer);
  }
}
window.apiGet = apiGet;
window.apiPost = apiPost;
window.metricCard = metricCard;
window.miniRow = miniRow;
window.emptyState = emptyState;
window.formatCount = formatCount;
window.formatBool = formatBool;
window.formatNumber = formatNumber;
window.formatMaybe = formatMaybe;
window.formatPercent = formatPercent;
window.formatSigned = formatSigned;
window.formatDelta = formatDelta;
window.formatRange = formatRange;
window.formatDuration = formatDuration;
window.setFeedback = setFeedback;

function datasetKey(ref) {
  const source = String(ref?.source || '').trim();
  const rel = String(ref?.rel_path || '').trim();
  return source && rel ? `${source}::${rel}` : '';
}
function parseDatasetKey(key) {
  const raw = String(key || '');
  const parts = raw.split('::');
  if (parts.length < 2) return null;
  return { source: parts[0], rel_path: parts.slice(1).join('::') };
}
function getSelectedDatasetRef() {
  return parseDatasetKey(String(DOM.expDatasetSelect?.value || STATE.selectedDatasetKey || ''));
}
window.getSelectedDatasetRef = getSelectedDatasetRef;

function bindDom() {
  [
    'expBackBtn','expRefreshProtocolBtn','expProtocolCards','expProtocolYamlFields','expProtocolJsonlFields','expProtocolYamlExample','expProtocolJsonlExample',
    'expRefreshDatasetsBtn','expDatasetSelect','expDatasetMeta','expDatasetOverviewCards','expPreviewBtn','expExpandBtn','expDatasetPreviewMeta','expDatasetPreview',
    'expImportFilename','expImportFormat','expImportContent','expImportBtn','expImportFeedback',
    'expClearRuntimeBtn','expClearHdbBtn','expClearAllBtn','expClearFeedback',
    'expResetMode','expMaxTicks','expTimeBasisOverride','expExportJsonChk','expExportHtmlChk','expRunStartBtn','expRunStopBtn','expProgressBar','expJobMeta','expJobFeedback','expJobOverviewCards','expJobSummary','expJobAutoTuner',
    'expRefreshRunsBtn','expDeleteRunBtn','expClearRunsBtn','expRunsList','expDownsampleEvery','expRunMeta','expRunOverviewCards','expRunSummary',
    'expMetricsOverviewCards','expMetricsNarrative','expChartDeck',
    'expLivePauseBtn','expLiveClearBtn','expLiveMeta','expLiveStateTop','expLiveCsTop','expLiveCfsTotals','expLiveAutoTuneLog','expLiveActionLog',
    'expChartModal','expChartModalScrim','expChartModalTitle','expChartModalSubtitle','expChartModalDesc','expChartModalChart','expChartModalStats','expChartModalFactors','expChartModalCloseBtn','expChartModalFullscreenBtn',
    'expLlmConfigMeta','expLlmEnabledChk','expLlmAutoChk','expLlmBaseUrl','expLlmModel','expLlmApiKey','expLlmMaxPromptChars','expLlmRefreshBtn','expLlmSaveBtn','expLlmSaveFeedback','expLlmStartBtn','expLlmStartForceBtn','expLlmStatusRefreshBtn','expLlmStatusMeta','expLlmStatusFeedback','expLlmReport','expLlmCopyReportBtn','expLlmDownloadReportBtn'
  ].forEach((id)=> DOM[id]=byId(id));

  DOM.expRefreshRunsInlineBtn = byId('expRefreshRunsInlineBtn');
  DOM.expRefreshRunSummaryBtn = byId('expRefreshRunSummaryBtn');

  if (!DOM.expRefreshRunsInlineBtn && DOM.expDownsampleEvery?.parentElement) {
    const btn = document.createElement('button');
    btn.id = 'expRefreshRunsInlineBtn';
    btn.className = 'ghost';
    btn.type = 'button';
    btn.textContent = '刷新运行记录';
    DOM.expDownsampleEvery.parentElement.parentElement?.insertBefore(btn, DOM.expDownsampleEvery.parentElement);
    DOM.expRefreshRunsInlineBtn = btn;
  }
  if (!DOM.expRefreshRunSummaryBtn && DOM.expRunMeta?.parentElement) {
    const toolbar = DOM.expRunMeta.parentElement;
    const btn = document.createElement('button');
    btn.id = 'expRefreshRunSummaryBtn';
    btn.className = 'ghost';
    btn.type = 'button';
    btn.textContent = '刷新摘要';
    toolbar.appendChild(btn);
    DOM.expRefreshRunSummaryBtn = btn;
  }
  if (DOM.expChartModal) {
    DOM.expChartModal.hidden = true;
    DOM.expChartModal.setAttribute('aria-hidden', 'true');
  }
}

function normalizeRowsForChart(rowsList, series) {
  const src = asArray(rowsList).filter((r)=>r && typeof r === 'object');
  if (!src.length) return [];
  const tickMap = new Map();
  src.forEach((row, idx) => {
    const tick = asNumber(row.tick_index, idx);
    tickMap.set(tick, row);
  });
  const ticks = Array.from(tickMap.keys()).sort((a,b)=>a-b);
  const minTick = ticks[0], maxTick = ticks[ticks.length-1];
  const prev = Object.create(null);
  const keys = asArray(series).map((s)=>String(s?.key || '')).filter(Boolean);
  const out = [];
  for(let tick=minTick; tick<=maxTick; tick+=1){
    const row = tickMap.get(tick);
    const next = { tick_index: tick, __synthetic_gap__: !row };
    if (row) Object.assign(next, row);
    keys.forEach((key)=>{
      const raw = row?.[key];
      const num = Number(raw);
      if (Number.isFinite(num)) { next[key] = num; prev[key] = num; }
      else if (Object.prototype.hasOwnProperty.call(prev, key)) next[key] = prev[key];
      else next[key] = 0;
    });
    out.push(next);
  }
  return out;
}

function renderLineChart(container, cfg) {
  if (!container) return;
  const sourceSeries = asArray(cfg?.series);
  const rows0 = normalizeRowsForChart(cfg?.rows, sourceSeries);
  const series = sourceSeries.filter((s)=> rows0.some((r)=> Number.isFinite(Number(r?.[s.key])) && Number(r?.[s.key]) !== 0));
  const rows = normalizeRowsForChart(cfg?.rows, series);
  if (!rows.length || !series.length) { container.innerHTML = emptyState('暂无可绘制数据。'); return; }
  const xs = rows.map((r, i) => asNumber(r.tick_index, i));
  let yMin = Infinity, yMax = -Infinity;
  series.forEach((s)=> rows.forEach((r)=>{ const v=asNumber(r[s.key],0); if(v<yMin) yMin=v; if(v>yMax) yMax=v; }));
  if (!Number.isFinite(yMin) || !Number.isFinite(yMax)) { yMin = 0; yMax = 1; }
  if (Math.abs(yMax - yMin) < 1e-9) yMax = yMin + 1;
  const w = 980, h = 360, padL = 54, padR = 20, padT = 14, padB = 36;
  const xMin = xs[0], xMax = xs[xs.length-1], xSpan = Math.max(1e-9, xMax - xMin), ySpan = Math.max(1e-9, yMax - yMin);
  const X = (x) => padL + ((x - xMin) / xSpan) * (w - padL - padR);
  const Y = (y) => padT + (1 - (y - yMin) / ySpan) * (h - padT - padB);
  const grid = Array.from({length:5}, (_,i)=>{ const yy = padT + (i/4)*(h-padT-padB); return `<line x1="${padL}" y1="${yy.toFixed(2)}" x2="${w-padR}" y2="${yy.toFixed(2)}" stroke="rgba(21,55,45,0.10)" stroke-width="1" />`; }).join('');
  const paths = series.map((s)=>{
    let d='';
    rows.forEach((r, i)=>{ const x = X(xs[i]); const y = Y(asNumber(r[s.key],0)); d += i===0 ? `M ${x.toFixed(2)} ${y.toFixed(2)}` : ` L ${x.toFixed(2)} ${y.toFixed(2)}`; });
    return `<path d="${d}" fill="none" stroke="${esc(s.color || '#18453b')}" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round" opacity="0.94" />`;
  }).join('');
  const points = series.map((s)=> rows.map((r, i)=> { const x = X(xs[i]); const y = Y(asNumber(r[s.key], 0)); return `<circle cx="${x.toFixed(2)}" cy="${y.toFixed(2)}" r="8" fill="transparent" data-tip="${esc((r.__tick_label || ('tick ' + r.tick_index)) + ' | ' + (s.name || s.key) + '：' + formatMaybe(r[s.key], 3))}"></circle>`; }).join('')).join('');
  const legend = `<div class="chart-legend">${series.map((s)=>`<span class="chart-chip"><span class="chart-swatch" style="background:${esc(s.color||'#18453b')}"></span>${esc(s.name||s.key)}</span>`).join('')}</div>`;
  container.innerHTML = `<div class="chart-hover-tip" hidden></div><svg class="chart-svg" viewBox="0 0 ${w} ${h}" xmlns="http://www.w3.org/2000/svg">${grid}${paths}${points}<line x1="${padL}" y1="${h-padB}" x2="${w-padR}" y2="${h-padB}" stroke="rgba(21,55,45,0.18)"/><line x1="${padL}" y1="${padT}" x2="${padL}" y2="${h-padB}" stroke="rgba(21,55,45,0.18)"/><text x="${padL}" y="${padT+10}" fill="rgba(21,55,45,0.64)" font-size="11">${esc(yMax.toFixed(3))}</text><text x="${padL}" y="${h-padB+18}" fill="rgba(21,55,45,0.64)" font-size="11">${esc(yMin.toFixed(3))}</text><text x="${padL}" y="${h-8}" fill="rgba(21,55,45,0.64)" font-size="11">tick ${esc(String(xMin))}</text><text x="${w-padR-60}" y="${h-8}" fill="rgba(21,55,45,0.64)" font-size="11">tick ${esc(String(xMax))}</text></svg>${legend}`;
  bindChartHover(container);
}
function renderBarChart(container, cfg) {
  if (!container) return;
  const sourceSeries = asArray(cfg?.series);
  const rows0 = normalizeRowsForChart(cfg?.rows, sourceSeries);
  const series = sourceSeries.filter((s)=> rows0.some((r)=> Number.isFinite(Number(r?.[s.key])) && Number(r?.[s.key]) !== 0));
  const rows = normalizeRowsForChart(cfg?.rows, series);
  if (!rows.length || !series.length) { container.innerHTML = emptyState('暂无可绘制数据。'); return; }
  const w = 980, h = 360, padL = 44, padR = 18, padT = 14, padB = 34;
  const xs = rows.map((_, i) => i);
  let yMax = 0;
  series.forEach((s)=> rows.forEach((r)=> { yMax = Math.max(yMax, asNumber(r[s.key], 0)); }));
  yMax = Math.max(1, yMax);
  const barGroupWidth = (w - padL - padR) / Math.max(1, rows.length);
  const barWidth = Math.max(1, (barGroupWidth * 0.82) / Math.max(1, series.length));
  const Y = (v)=> padT + (1 - (v / yMax)) * (h - padT - padB);
  const bars = [];
  rows.forEach((r, i)=> {
    series.forEach((s, si)=> {
      const v = asNumber(r[s.key], 0);
      const x = padL + i * barGroupWidth + si * barWidth;
      const y = Y(v);
      const bh = Math.max(1, (h - padB) - y);
      bars.push(`<rect x="${x.toFixed(2)}" y="${y.toFixed(2)}" width="${Math.max(1, barWidth - 1).toFixed(2)}" height="${bh.toFixed(2)}" fill="${esc(s.color || '#18453b')}" rx="2" data-tip="${esc((r.__tick_label || ('tick ' + r.tick_index)) + ' | ' + (s.name || s.key) + '：' + formatMaybe(v, 3))}" />`);
    });
  });
  const legend = `<div class="chart-legend">${series.map((s)=>`<span class="chart-chip"><span class="chart-swatch" style="background:${esc(s.color || '#18453b')}"></span>${esc(s.name || s.key)}</span>`).join('')}</div>`;
  container.innerHTML = `<div class="chart-hover-tip" hidden></div><svg class="chart-svg" viewBox="0 0 ${w} ${h}" xmlns="http://www.w3.org/2000/svg">${bars.join('')}<line x1="${padL}" y1="${h-padB}" x2="${w-padR}" y2="${h-padB}" stroke="rgba(21,55,45,0.18)"/><line x1="${padL}" y1="${padT}" x2="${padL}" y2="${h-padB}" stroke="rgba(21,55,45,0.18)"/></svg>${legend}`;
  bindChartHover(container);
}
function renderAreaChart(container, cfg) {
  if (!container) return;
  const sourceSeries = asArray(cfg?.series);
  const rows0 = normalizeRowsForChart(cfg?.rows, sourceSeries);
  const series = sourceSeries.filter((s)=> rows0.some((r)=> Number.isFinite(Number(r?.[s.key])) && Number(r?.[s.key]) !== 0));
  const rows = normalizeRowsForChart(cfg?.rows, series);
  if (!rows.length || !series.length) { container.innerHTML = emptyState('暂无可绘制数据。'); return; }
  const xs = rows.map((r, i) => asNumber(r.tick_index, i));
  let yMin = 0, yMax = -Infinity;
  series.forEach((s)=> rows.forEach((r)=>{ const v=asNumber(r[s.key],0); if(v>yMax) yMax=v; }));
  if (!Number.isFinite(yMax) || yMax <= 0) yMax = 1;
  const w = 980, h = 360, padL = 54, padR = 20, padT = 14, padB = 36;
  const xMin = xs[0], xMax = xs[xs.length-1], xSpan = Math.max(1e-9, xMax - xMin), ySpan = Math.max(1e-9, yMax - yMin);
  const X = (x) => padL + ((x - xMin) / xSpan) * (w - padL - padR);
  const Y = (y) => padT + (1 - (y - yMin) / ySpan) * (h - padT - padB);
  const baseY = h - padB;
  const areas = series.map((s)=> {
    let line = '';
    rows.forEach((r, i)=> { const x = X(xs[i]); const y = Y(asNumber(r[s.key],0)); line += i===0 ? `M ${x.toFixed(2)} ${y.toFixed(2)}` : ` L ${x.toFixed(2)} ${y.toFixed(2)}`; });
    const firstX = X(xs[0]);
    const lastX = X(xs[xs.length - 1]);
    const area = `${line} L ${lastX.toFixed(2)} ${baseY.toFixed(2)} L ${firstX.toFixed(2)} ${baseY.toFixed(2)} Z`;
    return `<path d="${area}" fill="${esc(s.color || '#18453b')}" opacity="0.16" /><path d="${line}" fill="none" stroke="${esc(s.color || '#18453b')}" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round" opacity="0.96" />`;
  }).join('');
  const points = series.map((s)=> rows.map((r, i)=> { const x = X(xs[i]); const y = Y(asNumber(r[s.key], 0)); return `<circle cx="${x.toFixed(2)}" cy="${y.toFixed(2)}" r="8" fill="transparent" data-tip="${esc((r.__tick_label || ('tick ' + r.tick_index)) + ' | ' + (s.name || s.key) + '：' + formatMaybe(r[s.key], 3))}"></circle>`; }).join('')).join('');
  const legend = `<div class="chart-legend">${series.map((s)=>`<span class="chart-chip"><span class="chart-swatch" style="background:${esc(s.color||'#18453b')}"></span>${esc(s.name||s.key)}</span>`).join('')}</div>`;
  container.innerHTML = `<div class="chart-hover-tip" hidden></div><svg class="chart-svg" viewBox="0 0 ${w} ${h}" xmlns="http://www.w3.org/2000/svg">${areas}${points}<line x1="${padL}" y1="${baseY}" x2="${w-padR}" y2="${baseY}" stroke="rgba(21,55,45,0.18)"/><line x1="${padL}" y1="${padT}" x2="${padL}" y2="${baseY}" stroke="rgba(21,55,45,0.18)"/></svg>${legend}`;
  container.classList.add('chart-area-mode');
  bindChartHover(container);
}
function renderStackedBarChart(container, cfg) {
  if (!container) return;
  const sourceSeries = asArray(cfg?.series);
  const rows0 = normalizeRowsForChart(cfg?.rows, sourceSeries);
  const series = sourceSeries.filter((s)=> rows0.some((r)=> Number.isFinite(Number(r?.[s.key])) && Number(r?.[s.key]) !== 0));
  const rows = normalizeRowsForChart(cfg?.rows, series);
  if (!rows.length || !series.length) { container.innerHTML = emptyState('暂无可绘制数据。'); return; }
  const w = 980, h = 360, padL = 44, padR = 18, padT = 14, padB = 34;
  const totals = rows.map((r)=> series.reduce((sum, s)=> sum + Math.max(0, asNumber(r[s.key], 0)), 0));
  const yMax = Math.max(1, ...totals);
  const barGroupWidth = (w - padL - padR) / Math.max(1, rows.length);
  const barWidth = Math.max(8, barGroupWidth * 0.52);
  const Y = (v)=> padT + (1 - (v / yMax)) * (h - padT - padB);
  const bars = [];
  rows.forEach((r, i)=> {
    let acc = 0;
    series.forEach((s)=> {
      const v = Math.max(0, asNumber(r[s.key], 0));
      const nextAcc = acc + v;
      const y = Y(nextAcc);
      const bh = Math.max(1, Y(acc) - y);
      const x = padL + i * barGroupWidth + (barGroupWidth - barWidth) / 2;
      bars.push(`<rect x="${x.toFixed(2)}" y="${y.toFixed(2)}" width="${barWidth.toFixed(2)}" height="${bh.toFixed(2)}" fill="${esc(s.color || '#18453b')}" rx="2" data-tip="${esc((r.__tick_label || ('tick ' + r.tick_index)) + ' | ' + (s.name || s.key) + '：' + formatMaybe(v, 3))}" />`);
      acc = nextAcc;
    });
  });
  const legend = `<div class="chart-legend">${series.map((s)=>`<span class="chart-chip"><span class="chart-swatch" style="background:${esc(s.color || '#18453b')}"></span>${esc(s.name || s.key)}</span>`).join('')}</div>`;
  container.innerHTML = `<div class="chart-hover-tip" hidden></div><svg class="chart-svg" viewBox="0 0 ${w} ${h}" xmlns="http://www.w3.org/2000/svg">${bars.join('')}<line x1="${padL}" y1="${h-padB}" x2="${w-padR}" y2="${h-padB}" stroke="rgba(21,55,45,0.18)"/><line x1="${padL}" y1="${padT}" x2="${padL}" y2="${h-padB}" stroke="rgba(21,55,45,0.18)"/></svg>${legend}`;
  bindChartHover(container);
}
function renderChart(container, cfg) {
  const type = String(cfg?.chartType || 'line');
  if (type === 'area') return renderAreaChart(container, cfg);
  if (type === 'bar_stacked') return renderStackedBarChart(container, cfg);
  if (type === 'bar' || type === 'bar_grouped' || type === 'bar_stacked') return renderBarChart(container, cfg);
  return renderLineChart(container, cfg);
}

const CHART_COLORS = ['#1f6f5f','#c17c4a','#5b8def','#b05d8f','#2f8f83','#d1a545','#5d6cc1','#7a9e47','#c45f43','#3b7ea6','#8c6dd7','#9b774a'];
function buildSeries(keys, startIndex = 0) { return asArray(keys).map((key, idx)=> ({ key, name: metricLabel(key), color: CHART_COLORS[(startIndex + idx) % CHART_COLORS.length] })); }
function chartConfig(id, section, title, subtitle, description, chartType, keys, extra = {}) { return { id, section, title, subtitle, description, chartType, series: buildSeries(keys, extra.colorOffset || 0), ...extra }; }
function getRenderableSeries(rows, series) { return asArray(series).filter((s)=> !isMeaninglessSeries(rows, s?.key)); }

const CHART_SECTIONS = [
  { id: 'overview', title: '运行总览', description: '先看整体稳态：能量、负载与状态池规模是否健康。', diagnostic: false },
  { id: 'sensor', title: '感受器', description: '专门观察文本感受器输入文本、输入长度、外源构成与残响参与情况。', diagnostic: false },
  { id: 'stimulus', title: '刺激链路', description: '观察外源输入、内源补充、合流中和与落地是否连贯。', diagnostic: false },
  { id: 'internal', title: '内源解析与查存', description: '检查内源刺激来源、预算约束、查存轮次与认知拼接活动。', diagnostic: false },
  { id: 'stitching', title: '认知拼接', description: '单独观察认知拼接的种子、候选、动作、产出与叙事成熟度。', diagnostic: false },
  { id: 'cfs', title: '认知感受', description: '把峰值、运行态维持量与触发频次拆开看，避免语义混杂。', diagnostic: false },
  { id: 'reward', title: '奖惩与监督', description: '观察系统奖惩、教师信号与期望契约监督是否生效。', diagnostic: false },
  { id: 'neuro', title: '情绪递质', description: '将应激稳定与奖励趋近两类递质分开观察。', diagnostic: false },
  { id: 'action', title: '行动链路', description: '拆开执行结果、尝试调度、驱动力与节点规模。', diagnostic: false },
  { id: 'time', title: '时间感受器', description: '区分时间绑定活动与延迟任务活动。', diagnostic: false },
  { id: 'map', title: '记忆赋能池', description: '区分记忆赋能规模与能量变化。', diagnostic: false },
  { id: 'performance', title: '性能分析', description: '主性能与细分性能分开显示，方便定位真正大头。', diagnostic: false },
  { id: 'diagnostic', title: '诊断图表', description: '保留调试所需的补充指标，但不干扰主视图。', diagnostic: true },
];

const CHART_CONFIGS = [
  chartConfig('pool_energy','overview','状态池总能量','实能量、虚能量与认知压的整体趋势','用于判断系统是否稳定维持活跃运行。','line',['pool_total_er','pool_total_ev','pool_total_cp']),
  chartConfig('pool_load','overview','状态池规模与负载','活跃条目、高压条目与注意力负载','用于判断状态池是否过载、过空或被某类负载占满。','line',['pool_active_item_count','pool_high_cp_item_count','attention_memory_item_count','attention_cam_item_count']),
  chartConfig('overview_hdb','overview','长期积累规模','结构、结构组与情节记忆的增长','用于观察长期积累是否在稳定形成。','area',['hdb_structure_count','hdb_group_count','hdb_episodic_count']),

  chartConfig('sensor_text','sensor','输入文本与外源长度','输入字符数、外源 SA 与输入 token 规模','用于直接观察文本感受器当前输入规模与分段后的外源量。','line',['input_len','external_sa_count','cache_input_flat_token_count']),
  chartConfig('sensor_compose','sensor','文本感受器输出构成','基础刺激元、属性刺激元、结构包与残响参与情况','用于判断文本感受器输出是否完整，以及残响是否正常介入。','bar_grouped',['sensor_feature_sa_count','sensor_attribute_sa_count','sensor_csa_bundle_count','sensor_echo_frames_used_count']),

  chartConfig('stimulus_size','stimulus','刺激规模','从外源输入到合流、中和、落地的规模变化','如果某一段长期为 0 或骤降，通常意味着链路缺数或中和异常。','line',['external_sa_count','merged_flat_token_count','cache_residual_flat_token_count','landed_flat_token_count']),
  chartConfig('stimulus_balance','stimulus','内外源平衡','观察内源刺激与外源刺激的相对规模','用于判断内源刺激是否过弱、过强或失去约束。','line',['external_sa_count','internal_sa_count','internal_minus_external_sa_count','internal_to_external_sa_ratio']),
  chartConfig('internal_source','internal','内源解析与预算','候选结构、入选结构、片段数与细节预算的合并视图','用于同时判断上游候选是否足够，以及内源分辨率预算是否真的压缩了当前内源刺激。','bar_grouped',['internal_candidate_structure_count','internal_selected_structure_count','internal_fragment_count','internal_resolution_raw_sa_count','internal_resolution_selected_sa_count','internal_resolution_budget_sa_cap']),
  chartConfig('internal_resolution_detail','internal','内源分辨率细项','细节预算、原始细节单元、已选细节单元与已选结构数','用于更细地查看注意力滤波和内源分辨率预算到底压掉了哪些细节。','bar_grouped',['internal_resolution_detail_budget','internal_resolution_raw_unit_count','internal_resolution_selected_unit_count','internal_resolution_structure_count_selected']),
  chartConfig('internal_resolution_pool','internal','内源来源与工作集关系','当前工作集、来源结构、候选结构与最终片段','用于观察注意力工作集经过内源解析后，最终有多少结构真的吐出了片段。','bar_grouped',['cam_item_count','internal_source_structure_count','internal_candidate_structure_count','internal_fragment_count']),
  chartConfig('retrieval_rounds','internal','查存轮次','结构级、刺激级与认知拼接活动','用于判断系统是否真正经历了多轮解析，而不是在前面被短路。','bar_grouped',['structure_round_count','stimulus_round_count','cs_action_count']),

  chartConfig('stitching_flow','stitching','认知拼接流程','种子、候选、动作与强化','用于观察认知拼接从种子到动作的转化率。','bar_grouped',['cs_seed_event_count','cs_seed_structure_count','cs_candidate_count','cs_action_count','cs_reinforced_count']),
  chartConfig('stitching_output','stitching','认知拼接产出','动作、强化、刺激级新建结构、新建、扩展与合并','用于判断系统当前是只有认知拼接候选/动作，还是已经通过刺激级字符串关系或认知拼接本体开始形成新结构。','bar_grouped',['cs_action_count','cs_reinforced_count','stimulus_new_structure_count','cs_created_count','cs_extended_count','cs_merged_count']),
  chartConfig('cfs_peak','cfs','认知感受峰值','关注本轮最强的主观感受峰值','峰值适合看“有没有触发”，不适合代替总量。','line',['cfs_dissonance_max','cfs_pressure_max','cfs_grasp_max','cfs_complexity_max','cfs_surprise_max']),
  chartConfig('cfs_live','cfs','认知感受运行态总量','关注感受在运行态中的维持强度','用于判断感受是否只是一闪而过，还是持续维持。','line',['cfs_dissonance_live_total_energy','cfs_correctness_live_total_energy','cfs_expectation_live_total_energy','cfs_pressure_live_total_energy','cfs_grasp_live_total_energy','cfs_complexity_live_total_energy']),
  chartConfig('cfs_count','cfs','认知感受触发频次','看不同感受通道到底有没有被触发','适合发现某条感受通道长期为 0 的问题。','bar_grouped',['cfs_signal_count','cfs_dissonance_count','cfs_surprise_count','cfs_repetition_count','cfs_expectation_count','cfs_pressure_count']),
  chartConfig('reward_system','reward','系统奖惩','系统自身的奖励与惩罚信号构成','用于观察单个 tick 内部奖励与惩罚是如何共同组成的。','bar_stacked',['rwd_pun_rwd','rwd_pun_pun']),
  chartConfig('reward_teacher','reward','教师监督与期望契约','教师信号、监督标签与应用次数','用于核对监督链路有没有真正打到运行中。','bar_grouped',['teacher_rwd','teacher_pun','teacher_applied_count','label_teacher_rwd','label_teacher_pun','label_should_call_weather']),
  chartConfig('neuro_stress','neuro','应激与稳定递质','应激、稳定与恢复相关递质','用于观察系统是否长期过度紧绷或持续低活性。','line',['nt_COR','nt_ADR','nt_SER','nt_END']),
  chartConfig('neuro_reward','neuro','奖励与趋近递质','趋近、社会联结与奖赏倾向','用于观察奖励驱动是否与任务阶段一致。','line',['nt_DA','nt_OXY','nt_END']),
  chartConfig('action_result','action','行动执行结果','单个 tick 内执行结果的组成','适合观察 weather_stub、回忆、注意聚焦链路是否真的跑通。','bar_stacked',['action_executed_attention_focus','action_executed_recall','action_executed_weather_stub','action_executed_count']),
  chartConfig('action_schedule','action','行动尝试与调度','执行前的尝试与调度活动','用于区分“没有想做”还是“想做但没执行成功”。','bar_grouped',['action_attempted_count','action_scheduled_weather_stub']),
  chartConfig('action_drive','action','行动驱动力','行动系统的最大与平均驱动力','驱动力是连续值，应与节点规模分开观察。','line',['action_drive_max','action_drive_mean']),
  chartConfig('action_nodes','action','行动节点规模','行动节点总数与活跃行动节点数','用于判断行动网络是否在增长或僵死。','bar_grouped',['action_node_count','action_drive_active_count']),
  chartConfig('time_binding','time','时间绑定活动','时间桶与时间属性的绑定活动','用于判断时间感受器是否在正常生成绑定。','bar_grouped',['time_sensor_bucket_update_count','time_sensor_attribute_binding_count','time_sensor_memory_sample_count']),
  chartConfig('time_delayed','time','延迟任务活动','延迟任务的注册、更新、执行与清理','用于观察延迟反馈、期望契约与时间任务的活动负载。','bar_grouped',['time_sensor_delayed_task_registered_count','time_sensor_delayed_task_updated_count','time_sensor_delayed_task_executed_count','time_sensor_delayed_task_pruned_count','time_sensor_delayed_task_capacity_skip_count','time_sensor_delayed_task_table_size']),
  chartConfig('map_scale','map','记忆赋能规模','赋能、反馈与应用次数','用于判断记忆赋能是缺少条目，还是有条目但没真正应用。','bar_grouped',['map_count','map_feedback_count','map_apply_count']),
  chartConfig('map_energy','map','记忆赋能能量','MAP 与反馈链路的能量变化','用于判断反馈偏虚能量还是能真正转成实能量。','line',['map_total_er','map_total_ev','map_feedback_total_ev']),
  chartConfig('timing_main','performance','主性能耗时','最主要的四个耗时大头','适合先看哪条链路在拖慢整体 Tick。','line',['timing_total_logic_ms','timing_structure_level_ms','timing_stimulus_level_ms','timing_cache_neutralization_ms']),
  chartConfig('timing_detail','performance','细分性能耗时','其余主要模块耗时','适合进一步判断是归纳、注意力、IESM 还是情绪链路偏慢。','line',['timing_induction_and_memory_ms','timing_attention_ms','timing_cognitive_stitching_ms','timing_iesm_ms','timing_action_ms','timing_emotion_ms','timing_cfs_ms','timing_time_sensor_ms']),
  chartConfig('diag_pool_apply','diagnostic','状态池落地应用','新增、更新、合并与能量增量','用于检查刺激包落地到状态池时，增量结构是否合理。','bar_grouped',['pool_apply_merged_item_count','pool_apply_new_item_count','pool_apply_updated_item_count','pool_apply_total_delta_cp','pool_apply_total_delta_er','pool_apply_total_delta_ev']),
  chartConfig('diag_attention','diagnostic','注意力诊断','候选量、预算上限、跳过量与消耗能量','用于分析注意力为什么没有选中更多记忆，或为什么耗能异常。','bar_grouped',['attention_state_pool_candidate_count','attention_cam_item_cap','attention_skipped_memory_item_count','attention_consumed_total_energy']),
  chartConfig('diag_maintenance','diagnostic','维护阶段诊断','维护前后状态与维护事件数','用于检查维护模块是否在实际清理，而不是形同虚设。','bar_grouped',['maintenance_before_active_item_count','maintenance_after_active_item_count','maintenance_delta_active_item_count','maintenance_before_high_cp_item_count','maintenance_after_high_cp_item_count','maintenance_delta_high_cp_item_count','maintenance_event_count']),
  chartConfig('diag_map_detail','diagnostic','记忆赋能补充诊断','MAP 条目、反馈与能量细项','用于进一步查看 MAP 的应用次数、反馈次数与能量变化。','bar_grouped',['map_count','map_feedback_count','map_apply_count','map_total_er','map_total_ev','map_feedback_total_ev']),
  chartConfig('diag_cfs_coverage','diagnostic','认知感受覆盖诊断','认知感受覆盖对象数与属性条目数','用于判断某种感受是没触发，还是触发了但绑定覆盖太窄。','bar_grouped',['cfs_dissonance_live_item_count','cfs_dissonance_live_attribute_count','cfs_correctness_live_item_count','cfs_correctness_live_attribute_count','cfs_expectation_live_item_count','cfs_expectation_live_attribute_count','cfs_pressure_live_item_count','cfs_pressure_live_attribute_count']),
  chartConfig('diag_echo_and_input','diagnostic','输入与残响诊断','原始输入长度、残响池与情节节奏','用于判断输入分段、残响池与情节节奏是否异常。','line',['input_len','sensor_echo_current_round','sensor_echo_pool_size','tick_in_episode_index','episode_repeat_index']),
  chartConfig('diag_cs_detail','diagnostic','认知拼接诊断','候选、新建、扩展、合并与强化','用于查看认知拼接到底是没候选，还是候选很多但落地太少。','bar_grouped',['cs_candidate_count','cs_created_count','cs_extended_count','cs_merged_count','cs_reinforced_count','cs_seed_event_count','cs_seed_structure_count']),
];

function sortedNumericMetricKeys(rows) {
  const keys = new Set();
  asArray(rows).forEach((row)=> {
    Object.entries(row || {}).forEach(([k,v])=> {
      if (k.startsWith('__')) return;
      if (typeof v === 'number' && Number.isFinite(v)) keys.add(k);
    });
  });
  return Array.from(keys).sort();
}

function getChartMetricKeys(cfg) { return asArray(cfg?.series).map((s)=> String(s?.key || '')).filter(Boolean); }
function buildDiagnosticChartConfigs(rows) {
  const existing = new Set(CHART_CONFIGS.flatMap((cfg)=> getChartMetricKeys(cfg)));
  const leftovers = sortedNumericMetricKeys(rows).filter((k)=> !existing.has(k) && !['tick_index','dataset_tick_index','source_dataset_tick_index','started_at_ms','finished_at_ms'].includes(k) && !isMeaninglessSeries(rows, k));
  const groups = [
    { id: 'diag_reward_tail', title: '奖惩补充诊断', subtitle: '主图未覆盖的奖惩与标签指标', description: '用于联调教师信号、奖惩标签与执行契约。', prefixes: ['rwd_pun_','teacher_','label_'], chartType: 'bar_grouped' },
    { id: 'diag_time_tail', title: '时间能量与延迟任务诊断', subtitle: '时间感受器的补充技术指标', description: '主要看时间桶能量、记忆抽样与延迟任务容量是否异常。', prefixes: ['time_sensor_'], chartType: 'bar_grouped' },
    { id: 'diag_cfs_tail', title: '认知感受细粒度诊断', subtitle: '主图未覆盖的认知感受细项', description: '用于查看复杂度、正确事件等更细粒度感受通道。', prefixes: ['cfs_'], chartType: 'bar_grouped' },
    { id: 'diag_action_tail', title: '行动细项诊断', subtitle: '动作尝试与执行的细分类', description: '用于看具体是哪一类动作在尝试、执行或被抑制。', prefixes: ['action_attempted_','action_executed_'], chartType: 'bar_grouped' },
    { id: 'diag_sensor_tail', title: '感受器补充诊断', subtitle: '主图未覆盖的感受器与输入指标', description: '用于查看感受器残余指标。', prefixes: ['sensor_','external_'], chartType: 'bar_grouped' },
    { id: 'diag_cache_tail', title: '缓存与输入诊断', subtitle: '输入长度与 flat token 相关补充项', description: '用于查看输入长度、flat token 规模与缓存残余指标。', prefixes: ['cache_','input_'], chartType: 'bar_grouped' },
    { id: 'diag_internal_tail', title: '内源分辨率补充诊断', subtitle: '预算、候选与丢弃细项', description: '用于进一步查看内源解析中哪些结构被保留、丢弃或压缩。', prefixes: ['internal_resolution_','internal_'], chartType: 'bar_grouped' },
    { id: 'diag_timing_tail', title: '细分耗时补充诊断', subtitle: '主图未覆盖的 timing 细项', description: '用于继续追查真正的性能耗时尾部。', prefixes: ['timing_'], chartType: 'line' },
    { id: 'diag_cs_tail', title: '认知拼接补充诊断', subtitle: '拼接运行态与叙事细项', description: '用于查看认知拼接是否只是启用，还是已经形成叙事输出。', prefixes: ['cs_'], chartType: 'bar_grouped' },
    { id: 'diag_misc_tail', title: '其余补充诊断', subtitle: '少量剩余技术指标', description: '只作为调试兜底，不建议直接据此调参。', prefixes: [], chartType: 'bar_grouped' },
  ];
  const out = [];
  let remaining = leftovers.slice();
  groups.forEach((group, groupIndex)=> {
    const matched = group.prefixes.length
      ? remaining.filter((k)=> group.prefixes.some((prefix)=> k.startsWith(prefix)) && !isMeaninglessSeries(rows, k)).slice(0, 10)
      : remaining.filter((k)=> !isMeaninglessSeries(rows, k)).slice(0, 10);
    if (!matched.length) return;
    remaining = remaining.filter((k)=> !matched.includes(k));
    out.push(chartConfig(group.id, 'diagnostic', group.title, group.subtitle, group.description, group.chartType, matched, { colorOffset: groupIndex * 2 }));
  });
  return out;
}

function normalizeMetricRows(rows) {
  const src = asArray(rows).slice().sort((a,b)=> asNumber(a?.tick_index, 0) - asNumber(b?.tick_index, 0));
  if (!src.length) return [];
  const numericKeys = sortedNumericMetricKeys(src);
  const byTick = new Map(src.map((row)=> [asNumber(row?.tick_index, 0), row]));
  const maxTick = Math.max(...src.map((row)=> asNumber(row?.tick_index, 0)));
  const lastValues = Object.create(null);
  const normalized = [];
  for (let tick = 0; tick <= maxTick; tick += 1) {
    const base = byTick.get(tick) || { tick_index: tick };
    const row = { ...base, __tick_label: 'tick ' + tick };
    numericKeys.forEach((key)=> {
      const raw = row[key];
      if (typeof raw === 'number' && Number.isFinite(raw)) lastValues[key] = raw;
      else if (Object.prototype.hasOwnProperty.call(lastValues, key)) row[key] = lastValues[key];
      else row[key] = 0;
    });
    normalized.push(row);
  }
  return normalized;
}

function renderChartFactors(cfg) {
  const keys = getChartMetricKeys(cfg);
  const rowsForFilter = asArray(STATE.lastMetricsRows);
  const visibleKeys = asArray(rowsForFilter.length ? getRenderableSeries(rowsForFilter, cfg?.series || []) : []).map((s)=> s.key);
  const explainMap = {
    diag_time_tail: '这张图主要回答两个问题：时间桶里的能量是不是在异常堆积；延迟任务是不是因为容量或采样策略而失真。若时间桶能量长期过高，通常说明时间感受绑定或衰减不平衡。',
    diag_cfs_tail: '这张图不是看主通道峰值，而是看更细的感受通道是否真的被绑定并维持。若事件计数有了，但 live item / live attribute 长期为 0，说明绑定链路可能过严。',
    diag_action_tail: '这张图用于区分“想做但没做成”和“根本没想做”。如果 attempted 高而 executed 低，问题多半在驱动力阈值、竞争或执行条件。',
    diag_cache_tail: '这张图主要看输入进入缓存后，flat token 与中和残余是否异常膨胀或异常归零。若输入长度正常但 token 长期极端，优先回看分段与缓存中和策略。',
    diag_internal_tail: '这张图是内源解析的细账本。适合判断到底是 rich candidate 太少、selected 太少，还是 dropped 太多。',
    diag_timing_tail: '这张图用于继续追查主性能图没有单独展示的耗时尾部。看到某一项升高后，应回到对应模块主链路排查。',
    diag_cs_tail: '这张图用于看认知拼接是否真的从“启用”走到了“形成叙事或结构输出”。如果 enabled 一直是 1，但 narrative / created 长期接近 0，说明拼接阈值或候选质量存在问题。',
    diag_map_detail: '这张图用于判断记忆赋能池是“没条目”“没反馈”，还是“有反馈但没真正转成稳定能量”。',
  };
  const factorMap = {
    overview_hdb: [
      { title: '真实影响因素：长期结构沉积速率', desc: '这张图由 HDB 新建、切割、合并、认知拼接持久化、情节记忆写入共同决定。放到运行总览里，是为了把长期沉积和当前活跃态放在同一视角下观察。' },
    ],
    sensor_text: [
      { title: '真实影响因素：文本感受器输入规模', desc: '输入字符数、外源 SA、输入 flat token 数共同反映文本感受器把原始文本切成了多少可处理对象。若字符数正常但外源 SA 很低，优先查文本切分和标点分段链。' },
    ],
    sensor_compose: [
      { title: '真实影响因素：感受器构件输出', desc: '基础刺激元、属性刺激元、结构包、残响帧数反映文本感受器输出的“层次完整度”。如果长期只有基础刺激元而缺属性或结构包，应优先查属性抽取与打包链路。' },
    ],
    pool_energy: [
      { title: '真实影响因素：状态池注入与衰减', desc: '这张图主要受状态池落地增量、维护衰减、记忆反馈注入、情绪与认知感受绑定的综合影响。若总能量持续单边上涨或过快掉空，应先回查状态池落地和维护链。' },
    ],
    pool_load: [
      { title: '真实影响因素：注意力候选规模与维护阈值', desc: '`attention_state_pool_candidate_count`、`attention_cam_item_cap` 与状态池维护阈值会共同决定活跃条目数和高认知压条目数。若活跃条目异常膨胀，先查注意力候选与维护链。' },
    ],
    stimulus_size: [
      { title: '真实影响因素：输入分段、中和与落地', desc: '外源 SA、合流 token、中和后 token、落地 token 分别对应文本感受器分段、缓存中和、状态池落地几个阶段。若中间某一段长期掉 0，应直接回查对应链路是否断数。' },
    ],
    stimulus_balance: [
      { title: '真实影响因素：内源预算与外源分段规模', desc: '这张图同时受外源输入分段长度、内源结构候选规模、内源细节预算约束影响。若内源长期远低于外源，优先看内源候选与预算；若长期远高于外源，则看疲劳与回忆约束是否失效。' },
    ],
    stimulus_sensor: [
      { title: '真实影响因素：文本感受器切分与残响参与', desc: '基础刺激元、属性刺激元、结构包、残响帧数由文本切分器、属性抽取和 echo 参与共同决定。若基础量正常但结构包长期为 0，先查结构封装与输入解析链。' },
    ],
    internal_source: [
      { title: '真实影响因素：内源结构数量上限', desc: '`hdb.main.py` 中的 `internal_resolution_max_structures_per_tick`。当“内源候选结构数”持续高、但“内源入选结构数”长期卡住时，应优先看这个上限。' },
      { title: '真实影响因素：内源细节预算', desc: '`hdb.main.py` 中的 `internal_resolution_detail_budget_base` 与 `internal_resolution_detail_budget_adr_gain`。如果结构入选了，但“内源原始 SA 数”到“内源入选 SA 数”压缩很重，主要受这组预算控制。' },
      { title: '真实影响因素：每结构细节上限', desc: '`hdb.main.py` 中的 `internal_resolution_min_detail_per_structure`、`internal_resolution_max_detail_per_structure`、`internal_resolution_flat_unit_cap_per_structure`。它们决定单个来源结构最多能吐出多少字符级细节。' },
      { title: '真实影响因素：注意力工作集规模', desc: '`attention.main.py` 输出的 `cam_item_count` 会直接影响内源来源候选。若候选本身就少，问题更可能在注意力筛选链，而不是内源预算链。' },
    ],
    internal_resolution_detail: [
      { title: '真实影响因素：内源细节预算与实际入选量', desc: '这张图直接回答“预算给了多少”“原始细节有多少”“最后真正入选了多少”。它最适合观察注意力滤波和内源分辨率机制到底压缩了多少 SA/细节单元。' },
      { title: '真实影响因素：结构数与细节数的双重门控', desc: '`internal_resolution_structure_count_selected` 反映选中了多少来源结构，`internal_resolution_selected_unit_count` 反映这些结构最终吐出了多少细节。结构选得上来但细节仍少，说明压缩发生在细节预算而不是候选结构阶段。' },
    ],
    internal_resolution_pool: [
      { title: '真实影响因素：注意力工作集到内源片段的转化', desc: '`cam_item_count` 是当前注意力工作集规模，`internal_source_structure_count` / `internal_candidate_structure_count` / `internal_fragment_count` 则表示其中多少对象真正进入了内源解析并产出片段。这个视图能直接看出注意力滤波效果。' },
    ],
    retrieval_rounds: [
      { title: '如何读这张图', desc: '这张图展示的是“结构级查存轮次 / 刺激级查存轮次 / 认知拼接动作次数”。如果你当前只看到一个有效系列，通常不是图坏了，而是另外两项在这段运行里恒定为 0 或恒定不变，已被无意义系列过滤自动隐藏。' },
      { title: '真实影响因素：上游是否真的进入对应阶段', desc: '结构级轮次取决于结构级查存是否实际发生；刺激级轮次取决于刺激级贪婪匹配是否进入循环；认知拼接动作次数则取决于候选事件、权重阈值与动作上限。某项长期消失，优先说明那条链根本没被跑起来。' },
    ],
    stitching_flow: [
      { title: '真实影响因素：认知拼接前段转化率', desc: '从事件种子、结构种子到候选、动作、强化，反映的是认知拼接前段是否真的跑起来。如果种子有、候选有，但动作始终低，问题多半在权重阈值或动作限额。' },
    ],
    stitching_output: [
      { title: '真实影响因素：字符串关系产出与认知拼接后段产出', desc: '你当前看到的许多长字符串对象，未必来自 `cognitive_stitching.main` 的 `created/extended/merged` 计数，也可能来自刺激级 `goal_b_string_relation_seed` 等字符串关系产出。因此这张图现在把 `stimulus_new_structure_count` 一并纳入，避免“明明有新字符串对象，图却空白”的错位。' },
    ],
    cfs_peak: [
      { title: '真实影响因素：触发阈值与当前事件强度', desc: '峰值图回答的是“有没有触发到”。它主要受当前 tick 的事件冲突、期待落空、惊讶输入、复杂度与把握感计算影响。若某通道长期为 0，优先排查触发条件而不是总量维持。' },
    ],
    cfs_live: [
      { title: '真实影响因素：绑定维持与衰减', desc: '运行态总量不是瞬时触发，而是绑定到状态池后的维持结果。若峰值存在但总量长期很低，通常说明绑定后维持链太弱、衰减过强，或绑定对象数太少。' },
    ],
    cfs_count: [
      { title: '真实影响因素：通道触发频次', desc: '这张图适合看“哪些主观通道根本没出现”。若某通道频次始终为 0，而理论上应当能出现，应直接排查该通道的触发条件和上游输入。' },
    ],
    reward_system: [
      { title: '真实影响因素：系统内部奖惩结算', desc: '系统奖励与惩罚由期望契约、行动成功/失败、违和与正确性链路共同驱动。若奖励和惩罚都长期为 0，应先查是否根本没有结算事件进入奖惩模块。' },
    ],
    reward_teacher: [
      { title: '真实影响因素：教师标签与实际应用次数', desc: '教师奖励/惩罚、标签值、实际应用次数分别表示“数据集给了什么监督”“系统读到了什么标签”“最后有没有真正落地到当前 tick”。三者不一致时，应优先查标签读取和应用链。' },
    ],
    neuro_stress: [
      { title: '真实影响因素：应激链与稳定链平衡', desc: '皮质醇、肾上腺素、血清素、内啡肽分别反映紧张、驱动、稳定、恢复。若应激递质持续高位，应回查惊讶/违和/压力链和恢复衰减链。' },
    ],
    neuro_reward: [
      { title: '真实影响因素：奖赏驱动与社会趋近', desc: '多巴胺、催产素、内啡肽主要受奖励、成功感、互动契合度与恢复机制影响。若行动成功但奖励递质始终不动，应回查奖惩到递质的传递链。' },
    ],
    action_result: [
      { title: '真实影响因素：执行链是否真正落地', desc: '这张图看的是已执行结果，而不是尝试。若尝试存在但执行为 0，优先排查驱动力阈值、行动竞争、执行条件与行动器返回结果。' },
    ],
    action_schedule: [
      { title: '真实影响因素：想做什么 与 被调度什么', desc: '行动尝试总数代表当前产生了多少行动意图，调度次数代表其中有多少真的被送入行动器。两者差得很大时，问题多半在调度与门控。' },
    ],
    action_drive: [
      { title: '真实影响因素：驱动力生成公式', desc: '最大/平均驱动力来自行动节点竞争后的驱动力分布。若尝试很多但驱动力始终过低，应回查行动触发条件、奖励预期和当前注意焦点。' },
    ],
    action_nodes: [
      { title: '真实影响因素：行动网络是否活化', desc: '行动节点总数与活跃节点数反映当前可行动作库和当前实际被激活的子集。若节点数存在但活跃数长期过低，问题在驱动力或门控，不在动作库缺失。' },
    ],
    time_binding: [
      { title: '真实影响因素：时间绑定是否正常发生', desc: '时间桶更新数、时间属性绑定数、时间记忆样本数共同反映时间感受器是否在运作。若记忆样本有而绑定数很低，应优先查时间属性绑定条件。' },
    ],
    time_delayed: [
      { title: '真实影响因素：延迟任务表与容量限制', desc: '注册、更新、执行、清理、容量跳过、表大小一起反映延迟任务系统负载。若容量跳过非 0 或表大小持续堆高，应先回查任务容量和清理策略。' },
    ],
    map_energy: [
      { title: '真实影响因素：反馈能量是虚转实还是停留在虚能量', desc: '`map_total_er`、`map_total_ev`、`map_feedback_total_ev` 用来区分记忆赋能最后有没有真正沉到实能量。若反馈虚能量持续高但实能量始终不抬头，问题在反馈落地或状态池吸收。' },
    ],
    hdb_growth: [
      { title: '可调参数：长期积累速率', desc: '优先结合 HDB 新建、切割、合并相关阈值观察，重点看是否“几乎不增长”或“增长过快导致噪声累积”。' },
    ],
    performance_main: [
      { title: '真实影响因素：主链大头模块', desc: '总逻辑耗时、结构级耗时、刺激级耗时、缓存中和耗时对应最主要的主链模块。应先看哪一条曲线抬头，再回到对应模块参数，而不要直接全局乱调。' },
    ],
    performance_detail: [
      { title: '真实影响因素：尾部耗时归因', desc: '细分性能图用于定位注意力、归纳记忆、认知拼接、IESM、情绪、时间感受器等尾部模块。若主图有抬头但主链四项解释不了，就看这里。' },
    ],
    diag_pool_apply: [
      { title: '真实影响因素：状态池落地方式', desc: '新增、更新、合并和能量增量共同说明刺激包是以“创建新对象”为主，还是以“更新已有对象”为主。若新增长期过高，可能存在融合不足；若合并长期过高，可能表示对象过度集中。' },
    ],
    diag_attention: [
      { title: '真实影响因素：候选规模、上限与跳过', desc: '状态池候选条目数、CAM 上限、跳过记忆条目数、消耗能量一起决定注意力为什么“看不到”更多对象。候选多但上限小，问题在容量；候选少，则问题在上游筛选。' },
    ],
    diag_maintenance: [
      { title: '真实影响因素：维护前后净变化', desc: '维护前后活跃条目、高压条目和维护事件数共同反映维护是否真的在工作。若前后几乎不变但事件数很高，说明维护规则可能过软；若删得过猛，则可能过硬。' },
    ],
    diag_cfs_coverage: [
      { title: '真实影响因素：触发后覆盖面', desc: '这张图不是看感受强度，而是看每类感受触发后覆盖了多少对象、绑定了多少属性。若峰值存在但覆盖面很窄，说明绑定条件过严或对象分布过碎。' },
    ],
    diag_echo_and_input: [
      { title: '真实影响因素：输入长度与残响介入', desc: '输入 flat token、原始输入长度、残响当前轮数、残响池大小一起反映文本输入与 echo 机制的整体负载。若残响长期不参与，可回查 echo 策略；若残响池过大，则回查清理与衰减。' },
    ],
    diag_cs_detail: [
      { title: '真实影响因素：认知拼接细部账本', desc: '这里看的是认知拼接在更细粒度上的运行账本，不是单纯结果图。适合排查为什么种子能来、候选能来，但最终产出和强化之间断层。' },
    ],
    diag_time_tail: [
      { title: '可调参数：延迟任务容量', desc: '`time_sensor.delayed_task_capacity`。若“容量跳过次数”非零，应优先调这里。' },
      { title: '可调参数：时间记忆采样数', desc: '`time_sensor.memory_sample_limit`。若时间取样记忆数长期过小或过大，可优先调这里。' },
    ],
    diag_internal_tail: [
      { title: '真实影响因素：细节疲劳窗口', desc: '`hdb.main.py` 中的 `internal_resolution_detail_fatigue_window`、`internal_resolution_detail_fatigue_start`、`internal_resolution_detail_fatigue_full`、`internal_resolution_detail_fatigue_min_scale`、`internal_resolution_detail_fatigue_beta`。若同类细节被反复抽到后迅速变瘦，这组参数直接参与限制。' },
      { title: '真实影响因素：锚点与丰富度偏置', desc: '`internal_resolution_stable_anchor_count`、`internal_resolution_anchor_ratio`、`internal_resolution_rich_structure_ratio`、`internal_resolution_rich_structure_min_units`、`internal_resolution_structure_richness_power`。它们决定细节预算更偏向锚点，还是偏向更“丰富”的结构。' },
    ],
    diag_timing_tail: [
      { title: '可调参数：按慢模块回溯', desc: '该图用于继续追踪尾部耗时，看到某个 timing 项异常后，应回到该模块的主参数而不是在这里直接拍脑袋调。' },
    ],
    diag_cs_tail: [
      { title: '真实影响因素：是否只是启用但未形成输出', desc: '`cs_enabled` 只表示功能开关打开，不表示真的发生拼接。因此它不应进图；真正该看的是 `cs_candidate_count`、`cs_created_count`、`cs_extended_count`、`cs_merged_count`。' },
      { title: '真实影响因素：拼接阈值与动作限额', desc: '优先回查认知拼接配置中的最低权重阈值、每 tick 动作上限、候选筛选阈值。若候选存在但新建/扩展长期为 0，问题通常出在这些真实阈值，而不是开关本身。' },
    ],
    diag_map_detail: [
      { title: '真实影响因素：MAP 条目来源', desc: '`map_apply_count`、`map_feedback_count`、`map_count` 先受回忆链路与记忆激活条目数影响。若条目数低，先查 recall / memory activation，不是先调能量。' },
      { title: '真实影响因素：反馈能量链路', desc: '`map_feedback_total_ev`、`map_total_er`、`map_total_ev` 用来区分“有反馈但只停留在虚能量”还是“已经沉到实能量”。如果反馈数有了但总实能量上不来，问题在反馈落地或状态池吸收链。' },
    ],
    map_scale: [
      { title: '真实影响因素：回忆命中与激活规模', desc: '这张图里的“记忆赋能条目数 / 记忆反馈条目数 / 记忆赋能应用次数”不是孤立参数，它们先受回忆是否命中、记忆激活池是否形成、以及当前 tick 是否真的发生赋能应用影响。' },
      { title: '如何读这张图', desc: '如果“条目数”高但“应用次数”低，说明有记忆候选却没真正打进状态池；如果“反馈条目数”低，优先排查记忆反馈链是否被上游回忆命中率限制。' },
    ],
    diag_cfs_tail: [
      { title: '真实影响因素：绑定对象数量', desc: '`...live_item_count` 与 `...live_attribute_count` 先回答“有没有真正绑定到状态池对象”。若事件计数有、但绑定对象数长期为 0，优先排查绑定条件而不是只看峰值。' },
      { title: '真实影响因素：ER / EV 落点', desc: '`cfs_correctness_live_total_er`、`cfs_dissonance_live_total_ev` 这类指标分别表示对应感受当前主要沉在实能量还是虚能量。若峰值高但总量始终起不来，通常是绑定后维持链或衰减链过强。' },
    ],
    diag_cs_detail: [
      { title: '真实影响因素：种子与动作转化率', desc: '`cs_seed_event_count`、`cs_seed_structure_count` 对照 `cs_candidate_count`、`cs_action_count`、`cs_created_count` 看的是“种子 -> 候选 -> 动作 -> 新结构”的转化率。真正异常是转化断层，不是单看某个点值。' },
      { title: '真实影响因素：叙事输出是否尚未成熟', desc: '`cs_narrative_top_total_energy`、`cs_narrative_top_grasp` 长期为 0，通常表示当前阶段尚未形成稳定叙事对象，不应把它和是否启用混为一谈。' },
    ],
    diag_misc_tail: [
      { title: '说明', desc: '这是混合兜底图，不建议直接据此调参。应先按系列名称回到对应模块，再查看该模块的主图和参数。' },
    ],
  };
  const rows = [
    { title: '图表覆盖指标', desc: '当前图覆盖：' + ((visibleKeys.length ? visibleKeys : keys).map(metricLabel).join('、') || '-') },
    ...(explainMap[cfg?.id] ? [{ title: '图表意义', desc: explainMap[cfg.id] }] : []),
    ...asArray(factorMap[cfg?.id]),
  ];
  return `<details class="details-panel exp-chart-factors"><summary><h4>主要影响因素与调参线索</h4></summary><div class="details-body">${rows.map((r)=>miniRow(r.title, r.desc)).join('')}</div></details>`;
}

function bindChartHover(container) {
  const tip = container.querySelector('.chart-hover-tip');
  if (!tip) return;
  container.querySelectorAll('[data-tip]').forEach((node)=> {
    node.addEventListener('mouseenter', ()=> {
      tip.textContent = node.getAttribute('data-tip') || '';
      tip.hidden = false;
    });
    node.addEventListener('mouseleave', ()=> {
      tip.hidden = true;
    });
  });
}

function openChartModal(cfg) {
  if (!DOM.expChartModal) return;
  const rows = STATE.lastMetricsRows;
  DOM.expChartModal.hidden = false;
  DOM.expChartModal.classList.remove('hidden');
  DOM.expChartModal.setAttribute('aria-hidden', 'false');
  if (DOM.expChartModalTitle) DOM.expChartModalTitle.textContent = String(cfg?.title || '图表放大查看');
  if (DOM.expChartModalSubtitle) DOM.expChartModalSubtitle.textContent = String(cfg?.subtitle || '');
  if (DOM.expChartModalDesc) DOM.expChartModalDesc.textContent = String(cfg?.description || '');
  if (DOM.expChartModalChart) renderChart(DOM.expChartModalChart, { rows, series: cfg?.series || [], chartType: cfg?.chartType || 'line', title: cfg?.title || '' });
  if (DOM.expChartModalStats) {
    DOM.expChartModalStats.innerHTML = asArray(cfg?.series).map((s)=>{
      const st = summarizeMetric(rows, s.key);
      return st ? miniRow(s.name || s.key, '最小 ' + formatMaybe(st.min,3) + ' | 最大 ' + formatMaybe(st.max,3) + ' | 平均 ' + formatMaybe(st.mean,3) + ' | 中位 ' + formatMaybe(st.median,3) + ' | 最新 ' + formatMaybe(st.latest,3) + ' | 首末差值 ' + formatMaybe(st.delta,3)) : '';
    }).join('');
  }
  if (DOM.expChartModalFactors) DOM.expChartModalFactors.innerHTML = renderChartFactors(cfg);
}
function closeChartModal() {
  if (!DOM.expChartModal) return;
  DOM.expChartModal.hidden = true;
  DOM.expChartModal.classList.add('hidden');
  DOM.expChartModal.setAttribute('aria-hidden', 'true');
  if (DOM.expChartModalChart) DOM.expChartModalChart.innerHTML = '';
}
function renderChartDeck(){
  if (!DOM.expChartDeck) return;
  const rows = normalizeMetricRows(STATE.lastMetricsRows);
  STATE.lastMetricsRows = rows;
  if (!rows.length) {
    DOM.expChartDeck.innerHTML = emptyState('当前运行还没有统计数据，请先选择一条运行记录。');
    return;
  }
  const allConfigs = CHART_CONFIGS.concat(buildDiagnosticChartConfigs(rows));
  DOM.expChartDeck.innerHTML = CHART_SECTIONS.map((section)=> {
    const items = allConfigs.filter((cfg)=> cfg.section === section.id);
    if (!items.length) return '';
    const cards = items.map((cfg)=> {
      const visibleSeries = getRenderableSeries(rows, cfg.series);
      const stats = visibleSeries.map((s)=> { const st = summarizeMetric(rows, s.key); return st ? miniRow(s.name, '最小 ' + formatMaybe(st.min,3) + ' | 最大 ' + formatMaybe(st.max,3) + ' | 平均 ' + formatMaybe(st.mean,3) + ' | 中位 ' + formatMaybe(st.median,3) + ' | 最新 ' + formatMaybe(st.latest,3) + ' | 首末差值 ' + formatMaybe(st.delta,3)) : ''; }).join('');
      return '<article class="subpanel exp-chart-card"><div class="exp-chart-head"><div class="section-head"><div><h4>' + esc(cfg.title) + '</h4><div class="meta">' + esc(cfg.subtitle || '') + '</div></div></div><div class="exp-chart-actions"><button type="button" class="ghost exp-chart-open-btn" data-chart-open="' + esc(cfg.id) + '">放大查看</button></div></div><p class="exp-chart-description">' + esc(cfg.description || '') + '</p><div id="chart_' + esc(cfg.id) + '" class="chart-wrap clickable exp-spaced-top"></div><div class="stack exp-spaced-top exp-chart-stats-scroll">' + stats + '</div>' + renderChartFactors(cfg) + '</article>';
    }).join('');
    if (section.diagnostic) {
      return '<details class="details-panel exp-chart-group-card" open><summary><div><h3>' + esc(section.title) + '</h3><div class="meta">' + esc(section.description || '') + '</div></div></summary><div class="details-body"><div class="exp-chart-grid exp-chart-grid-diagnostic">' + cards + '</div></div></details>';
    }
    return '<section class="subpanel exp-chart-group-card"><div class="section-head"><div><h3>' + esc(section.title) + '</h3><div class="meta">' + esc(section.description || '') + '</div></div></div><div class="exp-chart-grid">' + cards + '</div></section>';
  }).join('');
  allConfigs.forEach((cfg)=> renderChart(byId('chart_' + cfg.id), { rows, series: getRenderableSeries(rows, cfg.series), chartType: cfg.chartType || 'line', title: cfg.title }));
  DOM.expChartDeck.querySelectorAll('[data-chart-open]').forEach((el)=> el.addEventListener('click', ()=> {
    const cfg = allConfigs.find((item)=> item.id === el.getAttribute('data-chart-open'));
    if (cfg) openChartModal(cfg);
  }));
  DOM.expChartDeck.querySelectorAll('.chart-wrap.clickable').forEach((el)=> el.addEventListener('click', ()=> {
    const id = String(el.id || '').replace(/^chart_/, '');
    const cfg = allConfigs.find((item)=> item.id === id);
    if (cfg) openChartModal(cfg);
  }));
}

function renderMetricNarrative() {
  if (!DOM.expMetricsNarrative) return;
  const rows = asArray(STATE.lastMetricsRows);
  if (!rows.length) {
    DOM.expMetricsNarrative.innerHTML = emptyState('当前还没有可叙述的 metrics 数据。');
    return;
  }
  const last = rows.at(-1) || {};
  const timing = summarizeMetric(rows, 'timing_total_logic_ms');
  const ext = summarizeMetric(rows, 'external_sa_count');
  const internal = summarizeMetric(rows, 'internal_sa_count');
  const cfs = summarizeMetric(rows, 'cfs_dissonance_max');
  DOM.expMetricsNarrative.innerHTML = [
    miniRow('最新运行摘要', `tick ${last.tick_index ?? '-'} | 外源 SA ${formatCount(last.external_sa_count || 0)} | 内源 SA ${formatCount(last.internal_sa_count || 0)} | 总逻辑耗时 ${formatMaybe(last.timing_total_logic_ms || 0, 1)} ms`),
    timing ? miniRow('性能概览', `总逻辑耗时：最小 ${formatMaybe(timing.min,1)} | 最大 ${formatMaybe(timing.max,1)} | 平均 ${formatMaybe(timing.mean,1)} | 最新 ${formatMaybe(timing.latest,1)}`) : '',
    ext && internal ? miniRow('刺激规模概览', `外源 SA：最新 ${formatMaybe(ext.latest,0)} | 平均 ${formatMaybe(ext.mean,1)}；内源 SA：最新 ${formatMaybe(internal.latest,0)} | 平均 ${formatMaybe(internal.mean,1)}`) : '',
    cfs ? miniRow('认知感受概览', `违和感峰值：最小 ${formatMaybe(cfs.min,4)} | 最大 ${formatMaybe(cfs.max,4)} | 平均 ${formatMaybe(cfs.mean,4)} | 最新 ${formatMaybe(cfs.latest,4)}`) : '',
  ].filter(Boolean).join('');
}

function summarizeMetric(rows, key) {
  const vals = asArray(rows).map((row)=> Number(row?.[key])).filter((v)=> Number.isFinite(v));
  if (!vals.length) return null;
  const sorted = vals.slice().sort((a,b)=> a-b);
  const sum = vals.reduce((s,v)=> s+v, 0);
  const mean = sum / vals.length;
  const median = sorted.length % 2 ? sorted[Math.floor(sorted.length/2)] : (sorted[sorted.length/2 - 1] + sorted[sorted.length/2]) / 2;
  return {
    min: sorted[0] || 0,
    max: sorted[sorted.length - 1] || 0,
    mean,
    median,
    latest: vals[vals.length - 1] || 0,
    delta: (vals[vals.length - 1] || 0) - (vals[0] || 0),
  };
}

function renderProtocol(data) {
  const doc = data || {};
  if (DOM.expProtocolCards) {
    DOM.expProtocolCards.innerHTML = [
      metricCard('协议版本', doc.version || '-', '实验数据集的当前公开协议说明'),
      metricCard('推荐格式', doc.recommended_format || 'YAML / JSONL', '默认建议 YAML 模板'),
      metricCard('导入格式数', formatCount(asArray(doc.formats || ['yaml','jsonl']).length), '支持 YAML / JSONL 等'),
    ].join('');
  }
  if (DOM.expProtocolYamlFields) {
    const rows = asArray(doc.yaml_required_fields).concat(asArray(doc.yaml_optional_fields)).map((x)=> miniRow(x.field || x.name || '-', x.meaning || x.desc || '-'));
    DOM.expProtocolYamlFields.innerHTML = rows.join('') || emptyState('暂无 YAML 标准字段。');
  }
  if (DOM.expProtocolJsonlFields) {
    DOM.expProtocolJsonlFields.innerHTML = asArray(doc.jsonl_fields).map((x)=> miniRow(x.field || x.name || '-', x.meaning || x.desc || '-')).join('') || emptyState('暂无 JSONL 标准字段。');
  }
  if (DOM.expProtocolYamlExample) DOM.expProtocolYamlExample.textContent = String(doc.yaml_example || '');
  if (DOM.expProtocolJsonlExample) DOM.expProtocolJsonlExample.textContent = String(doc.jsonl_example || '');
}

async function refreshProtocol(silent=false){
  try {
    const res = await apiGet('/api/experiment/dataset_protocol');
    STATE.protocol = res.data || null;
    renderProtocol(STATE.protocol);
    if(!silent) setFeedback(DOM.expJobFeedback, '已刷新标准说明。', 'ok');
  } catch(error){
    if (DOM.expProtocolCards) DOM.expProtocolCards.innerHTML = emptyState(`标准说明加载失败：${error.message}`);
    if(!silent) setFeedback(DOM.expJobFeedback, `刷新标准说明失败：${error.message}`, 'err');
    throw error;
  }
}

function renderDatasets() {
  const items = asArray(STATE.datasets?.datasets);
  if (DOM.expDatasetSelect) {
    DOM.expDatasetSelect.innerHTML = items.map((d)=> `<option value="${esc(datasetKey(d))}">${esc(d.meta?.dataset_id || d.rel_path || datasetKey(d))}</option>`).join('');
    if (!STATE.selectedDatasetKey && items.length) STATE.selectedDatasetKey = datasetKey(items[0]);
    DOM.expDatasetSelect.value = STATE.selectedDatasetKey;
  }
  const ds = items.find((d)=> datasetKey(d) === STATE.selectedDatasetKey) || items[0] || null;
  if (DOM.expDatasetMeta) DOM.expDatasetMeta.textContent = ds ? `source=${ds.source || '-'} | dataset_id=${ds.meta?.dataset_id || '-'} | estimated_ticks=${ds.meta?.estimated_ticks ?? '-'}` : '尚未选择数据集。';
  if (DOM.expDatasetOverviewCards) {
    DOM.expDatasetOverviewCards.innerHTML = ds ? [
      metricCard('数据集 ID', ds.meta?.dataset_id || '-', ds.rel_path || '-'),
      metricCard('时间基准', ds.meta?.time_basis || '-', `来源：${ds.source || '-'}`),
      metricCard('估计 Tick', formatCount(ds.meta?.estimated_ticks || 0), `标签 Tick：${formatCount(ds.meta?.labeled_ticks || 0)}`),
      metricCard('用途 / 标题', ds.meta?.title || '-', ds.meta?.description || '暂无说明'),
      metricCard('实验目标', ds.meta?.experiment_goal || '-', asArray(ds.meta?.evaluation_dimensions).slice(0,2).join('；') || '暂无目标说明'),
    ].join('') : emptyState('暂无可用数据集。');
  }
}

async function refreshDatasets(silent=false){
  try {
    const res = await apiGet('/api/experiment/datasets');
    STATE.datasets = res.data || null;
    renderDatasets();
    if(!silent) setFeedback(DOM.expJobFeedback, '已刷新数据集列表。', 'ok');
  } catch(error){
    if (DOM.expDatasetOverviewCards) DOM.expDatasetOverviewCards.innerHTML = emptyState(`数据集加载失败：${error.message}`);
    if(!silent) setFeedback(DOM.expJobFeedback, `刷新数据集失败：${error.message}`, 'err');
    throw error;
  }
}

async function previewDataset(){
  const ref = getSelectedDatasetRef();
  if(!ref) return setFeedback(DOM.expJobFeedback, '请先选择数据集。', 'err');
  try {
    const res = await apiPost('/api/experiment/datasets/preview', { dataset_ref: ref, limit: 24 });
    const data = res.data || {};
    if(DOM.expDatasetPreviewMeta) DOM.expDatasetPreviewMeta.textContent = `预览 ${formatCount(asArray(data.preview_ticks).length)}/${formatCount(data.total_ticks || 0)} tick`;
    if(DOM.expDatasetPreview) DOM.expDatasetPreview.innerHTML = asArray(data.preview_ticks).map((t)=> miniRow(`tick ${t.tick_index ?? '-'} · ep ${t.episode_id || '-'}`, `输入：${t.input_text || '（空 tick）'}\n标签：${asArray(t.tags).join(', ') || '-'}`)).join('') || emptyState('暂无预览数据。');
  } catch(error){
    setFeedback(DOM.expJobFeedback, `预览失败：${error.message}`, 'err');
  }
}

async function expandDataset(){
  const ref = getSelectedDatasetRef();
  if(!ref) return setFeedback(DOM.expJobFeedback, '请先选择数据集。', 'err');
  try {
    const res = await apiPost('/api/experiment/datasets/expand', { dataset_ref: ref, limit: 120 });
    const data = res.data || {};
    if(DOM.expDatasetPreviewMeta) DOM.expDatasetPreviewMeta.textContent = `展开 ${formatCount(asArray(data.expanded_ticks).length)}/${formatCount(data.total_ticks || 0)} tick`;
    if(DOM.expDatasetPreview) DOM.expDatasetPreview.innerHTML = asArray(data.expanded_ticks).map((t)=> miniRow(`tick ${t.tick_index ?? '-'} · ep ${t.episode_id || '-'}`, `输入：${t.input_text || '（空 tick）'}\n标签：${asArray(t.tags).join(', ') || '-'}`)).join('') || emptyState('暂无展开数据。');
  } catch(error){
    setFeedback(DOM.expJobFeedback, `展开失败：${error.message}`, 'err');
  }
}

async function importDataset(){
  const filename = String(DOM.expImportFilename?.value || '').trim();
  const format = String(DOM.expImportFormat?.value || '').trim();
  const content = String(DOM.expImportContent?.value || '');
  if (!filename || !format || !content.trim()) return setFeedback(DOM.expImportFeedback || DOM.expJobFeedback, '请填写文件名、格式与内容。', 'err');
  try {
    await apiPost('/api/experiment/datasets/import', { filename, format, content });
    setFeedback(DOM.expImportFeedback || DOM.expJobFeedback, '已导入数据集。', 'ok');
    await refreshDatasets(true);
  } catch(error){
    setFeedback(DOM.expImportFeedback || DOM.expJobFeedback, `导入失败：${error.message}`, 'err');
  }
}

async function clearRuntime(){ try { await apiPost('/api/experiment/runtime/clear', {}); setFeedback(DOM.expClearFeedback || DOM.expJobFeedback, '已清空运行态。', 'ok'); } catch(error){ setFeedback(DOM.expClearFeedback || DOM.expJobFeedback, `清空运行态失败：${error.message}`, 'err'); } }
async function clearHdb(){ try { await apiPost('/api/experiment/hdb/clear', {}); setFeedback(DOM.expClearFeedback || DOM.expJobFeedback, '已清空 HDB。', 'ok'); } catch(error){ setFeedback(DOM.expClearFeedback || DOM.expJobFeedback, `清空 HDB 失败：${error.message}`, 'err'); } }
async function clearAll(){ try { await apiPost('/api/experiment/clear_all', {}); setFeedback(DOM.expClearFeedback || DOM.expJobFeedback, '已执行全清理。', 'ok'); } catch(error){ setFeedback(DOM.expClearFeedback || DOM.expJobFeedback, `全清理失败：${error.message}`, 'err'); } }

function renderMetricsOverview(){
  if (DOM.expMetricsOverviewCards) {
    const last = STATE.lastMetricsRows.at(-1) || {};
    DOM.expMetricsOverviewCards.innerHTML = [
      metricCard('已加载行数', formatCount(STATE.lastMetricsRows.length), `下采样步长 ${formatCount(STATE.lastMetricsEvery)}`),
      metricCard('最新 tick', formatCount(STATE.lastMetricsRows.at(-1)?.tick_index || 0), '来自 metrics.jsonl'),
      metricCard('最新刷新', formatTime(STATE.lastMetricsFetchMs), '图表与摘要使用同一份 rows'),
      metricCard('最新外源 SA', formatCount(last.external_sa_count || 0), `合流 flat token：${formatCount(last.merged_flat_token_count || 0)}`),
      metricCard('最新总耗时', `${formatMaybe(last.timing_total_logic_ms || 0, 1)} ms`, `刺激级：${formatMaybe(last.timing_stimulus_level_ms || 0, 1)} | 中和：${formatMaybe(last.timing_cache_neutralization_ms || 0, 1)}`),
      metricCard('最新 live CFS', `违和EV ${formatMaybe(last.cfs_dissonance_live_total_ev || 0, 3)}`, `正确ER ${formatMaybe(last.cfs_correctness_live_total_er || 0, 3)}`),
    ].join('');
  }
  renderMetricNarrative();
}
function renderRunSummary(){
  const man = STATE.lastManifest || {};
  if (DOM.expRunMeta) DOM.expRunMeta.textContent = `status=${man.status || '-'} | dataset=${man.dataset?.dataset_id || '-'} | done=${man.tick_done ?? 0}/${man.dataset?.total_ticks ?? '-'}`;
  if (DOM.expRunOverviewCards) {
    const expc = man.expectation_contracts || {};
    const at = man.auto_tuner || {};
    DOM.expRunOverviewCards.innerHTML = [
      metricCard('运行状态', man.status || '-', `run_id=${man.run_id || '-'}`),
      metricCard('已执行 Tick', formatCount(man.tick_done || 0), `source=${formatCount(man.source_tick_done || 0)} | synthetic=${formatCount(man.synthetic_tick_done || 0)}`),
      metricCard('数据集', man.dataset?.dataset_id || '-', man.dataset?.dataset_ref?.rel_path || '-'),
      metricCard('期待契约', formatCount(expc.registered_count || 0), `success=${formatCount(expc.success_count || 0)} | failure=${formatCount(expc.failure_count || 0)}`),
      metricCard('调参器', formatBool(Boolean(at.enabled)), `短期=${formatBool(Boolean(at.short_term?.enabled ?? true))} | 长期=${formatBool(Boolean(at.long_term?.enabled ?? true))}`),
      metricCard('时间感受器', man.time_sensor_runtime_override?.time_basis || 'tick', `tick_interval_sec=${man.time_sensor_runtime_override?.tick_interval_sec ?? '-'}`),
    ].join('');
  }
  if (DOM.expRunSummary) {
    const last = STATE.lastMetricsRows.at(-1) || {};
    DOM.expRunSummary.innerHTML = [
      miniRow('最后一条 tick 指标', `tick ${last.tick_index ?? '-'} | 外源SA ${last.external_sa_count ?? 0} | 合流 flat token ${last.merged_flat_token_count ?? 0} | 总耗时 ${last.timing_total_logic_ms ?? 0}ms`),
      miniRow('运行选项', `reset_mode=${man.options?.reset_mode || '-'} | export_json=${formatBool(Boolean(man.options?.export_json))} | export_html=${formatBool(Boolean(man.options?.export_html))} | time_basis=${man.options?.time_sensor_time_basis || '(default)'}`),
      miniRow('执行进度解释', `source_tick_done=${formatCount(man.source_tick_done || 0)} | synthetic_tick_done=${formatCount(man.synthetic_tick_done || 0)} | executed_tick_done_total=${formatCount(man.executed_tick_done_total || 0)} | tick_planned=${formatCount(man.tick_planned || 0)}`),
    ].join('');
  }
}

async function refreshRuns(silent=false){ try { const res = await apiGet('/api/experiment/runs?limit=48'); STATE.runs = asArray(res.data?.items || res.data?.runs); if (DOM.expRunsList) { DOM.expRunsList.innerHTML = STATE.runs.map((r)=> `<button class="list-row-btn ${STATE.selectedRunId===r.run_id?'active':''}" data-run-id="${esc(r.run_id)}"><span>${esc(r.run_id)}</span><span class="meta">${esc(r.status || '-')} | ${esc(r.dataset_id || '-')} | ${formatCount(r.tick_done || 0)}/${formatCount(r.tick_planned || 0)}</span></button>`).join('') || emptyState('当前还没有实验运行记录。'); DOM.expRunsList.querySelectorAll('[data-run-id]').forEach((el)=> el.addEventListener('click', ()=> selectRun(el.getAttribute('data-run-id'), { reloadMetrics: true }))); } if(!STATE.selectedRunId && STATE.runs.length) STATE.selectedRunId = STATE.runs[0].run_id; if(STATE.selectedRunId) await selectRun(STATE.selectedRunId, { reloadMetrics: true, silent: true }); if(!silent) setFeedback(DOM.expJobFeedback, '已刷新运行记录。', 'ok'); } catch(error){ if(!silent) setFeedback(DOM.expJobFeedback, `刷新运行记录失败：${error.message}`, 'err'); } }
async function deleteSelectedRun(){ const rid = String(STATE.selectedRunId || '').trim(); if(!rid) return setFeedback(DOM.expJobFeedback, '当前没有选中的运行记录。', 'err'); try { await apiPost('/api/experiment/run/delete', { run_id: rid }); setFeedback(DOM.expJobFeedback, `已删除运行 ${rid}`, 'ok'); if (STATE.selectedRunId === rid) { STATE.selectedRunId = ''; STATE.lastManifest = null; STATE.lastMetricsRows = []; renderRunSummary(); renderMetricsOverview(); renderChartDeck(); } await refreshRuns(true); } catch(error){ setFeedback(DOM.expJobFeedback, `删除运行失败：${error.message}`, 'err'); } }
async function clearRuns(){ try { await apiPost('/api/experiment/runs/clear', {}); STATE.selectedRunId = ''; STATE.lastManifest = null; STATE.lastMetricsRows = []; renderRunSummary(); renderMetricsOverview(); renderChartDeck(); await refreshRuns(true); setFeedback(DOM.expJobFeedback, '已清空运行记录。', 'ok'); } catch(error){ setFeedback(DOM.expJobFeedback, `清空运行记录失败：${error.message}`, 'err'); } }
async function selectRun(runId, { reloadMetrics = true, silent = false } = {}) { const rid = String(runId || '').trim(); if(!rid) return; STATE.selectedRunId = rid; try { const [m1, m2] = await Promise.all([ apiGet(`/api/experiment/run/manifest?run_id=${encodeURIComponent(rid)}`), reloadMetrics ? apiGet(`/api/experiment/run/metrics?run_id=${encodeURIComponent(rid)}&downsample_every=${Math.max(1, asNumber(DOM.expDownsampleEvery?.value, 1))}`) : Promise.resolve({data:{rows:STATE.lastMetricsRows}}) ]); STATE.lastManifest = m1.data || null; STATE.lastMetricsRows = asArray(m2.data?.rows); STATE.lastMetricsEvery = Math.max(1, asNumber(m2.data?.downsample_every, asNumber(DOM.expDownsampleEvery?.value, 1))); STATE.lastMetricsFetchMs = Date.now(); renderRunSummary(); renderMetricsOverview(); renderChartDeck(); await refreshLlmStatus(rid, true).catch(()=>{}); if(!silent) setFeedback(DOM.expJobFeedback, `已加载运行 ${rid}`, 'ok'); } catch(error){ if(!silent) setFeedback(DOM.expJobFeedback, `加载运行失败：${error.message}`, 'err'); } }

function renderJobPanel(job){
  if (!DOM.expJobMeta || !job) return;
  const done = asNumber(job.tick_done, 0), planned = asNumber(job.tick_planned, 0);
  DOM.expJobMeta.textContent = `job=${job.job_id || '-'} | status=${job.status || '-'} | ${done}/${planned || '?'}`;
  if (DOM.expProgressBar) DOM.expProgressBar.style.width = `${planned ? Math.round(Math.max(0, Math.min(1, done / Math.max(1, planned))) * 100) : 0}%`;
  if (DOM.expJobOverviewCards) DOM.expJobOverviewCards.innerHTML = [
    metricCard('当前状态', job.status || '-', job.error || '状态自动刷新'),
    metricCard('当前进度', `${formatCount(done)}/${formatCount(planned)}`, `executed=${formatCount(job.executed_tick_done_total || done)}`),
    metricCard('运行记录', job.run_id || '-', `dataset=${job.dataset_id || '-'}`),
  ].join('');
  if (DOM.expJobSummary) DOM.expJobSummary.innerHTML = miniRow('任务说明', `source_tick_done=${formatCount(job.source_tick_done || 0)} | synthetic_tick_done=${formatCount(job.synthetic_tick_done || 0)} | executed_total=${formatCount(job.executed_tick_done_total || 0)}`);
}
window.renderJobPanel = renderJobPanel;

function stopJobPolling(){ if(STATE.jobPollTimer){ clearInterval(STATE.jobPollTimer); STATE.jobPollTimer = null; } }
async function pollJob(jobId, runId){ const jid = String(jobId || '').trim(); if(!jid) return; try { const res = await apiGet(`/api/experiment/jobs?job_id=${encodeURIComponent(jid)}`); const job = res.data || null; STATE.lastJob = job; renderJobPanel(job); const status = String(job?.status || ''); if (['completed','failed','cancelled','stopped_max_ticks'].includes(status)) { stopJobPolling(); await refreshRuns(true); const rid = String(runId || job?.run_id || ''); if (rid) await selectRun(rid, { reloadMetrics: true, silent: true }); } } catch(error){ setFeedback(DOM.expJobFeedback, `刷新任务进度失败：${error.message}`, 'err'); } }
function startJobPolling(jobId, runId){ stopJobPolling(); STATE.activeJobId = String(jobId || ''); STATE.activeRunId = String(runId || ''); pollJob(jobId, runId); STATE.jobPollTimer = setInterval(()=> pollJob(jobId, runId), 2500); }
window.startRun = async function startRun(){ const ref = getSelectedDatasetRef(); if(!ref) return setFeedback(DOM.expJobFeedback, '请先选择数据集。', 'err'); const options = { reset_mode: String(DOM.expResetMode?.value || 'keep').trim() || 'keep', export_json: Boolean(DOM.expExportJsonChk?.checked), export_html: Boolean(DOM.expExportHtmlChk?.checked), time_sensor_time_basis: String(DOM.expTimeBasisOverride?.value || '').trim() || null, max_ticks: (String(DOM.expMaxTicks?.value || '').trim() ? Math.max(1, asNumber(DOM.expMaxTicks?.value, 0)) : null) }; setFeedback(DOM.expJobFeedback, '正在启动实验任务…', 'busy'); try { const res = await apiPost('/api/experiment/run/start', { dataset_ref: ref, options }); const jobId = String(res.data?.job_id || '').trim(); const runId = String(res.data?.run_id || '').trim(); if(!jobId) throw new Error('后端没有返回 job_id。'); startJobPolling(jobId, runId); setFeedback(DOM.expJobFeedback, `任务已启动：${jobId} | run=${runId || '-'}`, 'ok'); } catch(error){ setFeedback(DOM.expJobFeedback, `启动失败：${error.message}`, 'err'); } };
window.stopRun = async function stopRun(){ const jid = String(STATE.activeJobId || '').trim(); if(!jid) return setFeedback(DOM.expJobFeedback, '当前没有运行中的任务。', 'err'); try { await apiPost('/api/experiment/run/stop', { job_id: jid }); setFeedback(DOM.expJobFeedback, `已请求停止 ${jid}。`, 'ok'); } catch(error){ setFeedback(DOM.expJobFeedback, `停止失败：${error.message}`, 'err'); } };

async function refreshLiveMonitor(){ try { const res = await apiGet('/api/dashboard'); STATE.liveDashboard = res.data || null; STATE.liveLastFetchMs = Date.now(); const dash = STATE.liveDashboard || {}; if (DOM.expLiveMeta) DOM.expLiveMeta.textContent = `最后刷新：${formatTime(STATE.liveLastFetchMs)}`; const top = asArray(dash?.state_snapshot?.top_items); if (DOM.expLiveStateTop) DOM.expLiveStateTop.innerHTML = top.slice(0,18).map((it)=>miniRow(`${it.display || '-'} · ${it.ref_object_type || '-'}`, `ER ${formatMaybe(it.er)} | EV ${formatMaybe(it.ev)} | CP ${formatMaybe(it.cp_abs)} | id ${it.ref_object_id || '-'}`)).join('') || emptyState('当前没有状态池 Top。'); const rep = dash?.last_report || {}; const items = asArray(rep?.cognitive_stitching?.narrative_top_items); if (DOM.expLiveCsTop) DOM.expLiveCsTop.innerHTML = items.length ? items.slice(0,12).map((it, idx)=>miniRow(`Top${idx+1} · 总能量 ${formatMaybe(it.total_energy)} · 把握感 ${formatMaybe(it.event_grasp)}`, `${truncateText(it.visible_text || it.display_text || it.event_text || '-', 220)}\nref ${it.event_ref_id || '-'} | st ${it.structure_id || '-'} | 组分 ${it.component_count ?? '-'}`)).join('') : emptyState('当前没有认知拼接叙事 Top。通常表示本轮尚未形成可稳定叙事化的事件对象，而不一定是前端取数失败。'); const totals = dash?.state_snapshot?.summary?.bound_attribute_energy_totals || {}; const rows = Object.values(totals).filter((r)=> String(r?.attribute_name || '').startsWith('cfs_')).sort((l,r)=> asNumber(r?.total_energy,0)-asNumber(l?.total_energy,0)); if (DOM.expLiveCfsTotals) DOM.expLiveCfsTotals.innerHTML = rows.length ? rows.slice(0,18).map((r)=>miniRow(`${r.attribute_name || '-'} · 总 ${formatMaybe(r.total_energy)}`, `ER ${formatMaybe(r.total_er)} | EV ${formatMaybe(r.total_ev)} | 覆盖对象 ${formatCount(r.item_count || 0)} | 属性条目 ${formatCount(r.attribute_count || 0)}`)).join('') : emptyState('当前没有 cfs_* 的 bound attributes 聚合数据。'); } catch(error){} }
async function refreshLiveMonitor(){
  try {
    const res = await apiGet('/api/dashboard');
    STATE.liveDashboard = res.data || null;
    STATE.liveLastFetchMs = Date.now();
    const dash = STATE.liveDashboard || {};
    if (DOM.expLiveMeta) DOM.expLiveMeta.textContent = `最后刷新：${formatTime(STATE.liveLastFetchMs)}${STATE.livePaused ? ' | 已暂停自动刷新' : ''}`;
    const top = asArray(dash?.state_snapshot?.top_items);
    if (DOM.expLiveStateTop) DOM.expLiveStateTop.innerHTML = top.slice(0,18).map((it)=>miniRow(`${truncateText(it.display || '-', 72)} · ${it.ref_object_type || '-'}`, `ER ${formatMaybe(it.er)} | EV ${formatMaybe(it.ev)} | CP ${formatMaybe(it.cp_abs)} | id ${it.ref_object_id || '-'}`)).join('') || emptyState('当前没有状态池 Top。');
    const rep = dash?.last_report || {};
    const items = asArray(rep?.cognitive_stitching?.narrative_top_items);
    if (DOM.expLiveCsTop) {
      const fallbackTop = top.filter((it)=> String(it.ref_object_type || '') === 'st' && String(it.display || '').length > 1).slice(0,12);
      DOM.expLiveCsTop.innerHTML = items.length
        ? items.slice(0,12).map((it, idx)=>miniRow(`Top${idx+1} · 总能量 ${formatMaybe(it.total_energy)} · 把握感 ${formatMaybe(it.event_grasp)}`, `${truncateText(it.visible_text || it.display_text || it.event_text || '-', 220)}\nref ${it.event_ref_id || '-'} | st ${it.structure_id || '-'} | 组分 ${it.component_count ?? '-'}`)).join('')
        : (fallbackTop.length
            ? fallbackTop.map((it, idx)=>miniRow(`字符串对象 Top${idx+1} · CP ${formatMaybe(it.cp_abs)} · ER ${formatMaybe(it.er)}`, `${truncateText(it.display || '-', 220)}\nid ${it.ref_object_id || '-'} | 类型 ${it.ref_object_type || '-'}`)).join('')
            : emptyState('当前没有认知拼接叙事 Top，也没有可回退展示的字符串对象 Top。'));
    }
    const totals = dash?.state_snapshot?.summary?.bound_attribute_energy_totals || {};
    const cfsRows = Object.values(totals).filter((r)=> String(r?.attribute_name || '').startsWith('cfs_')).sort((l,r)=> asNumber(r?.total_energy,0)-asNumber(l?.total_energy,0));
    if (DOM.expLiveCfsTotals) DOM.expLiveCfsTotals.innerHTML = cfsRows.length ? cfsRows.slice(0,18).map((r)=>miniRow(`${r.attribute_name || '-'} · 总 ${formatMaybe(r.total_energy)}`, `ER ${formatMaybe(r.total_er)} | EV ${formatMaybe(r.total_ev)} | 覆盖对象 ${formatCount(r.item_count || 0)} | 属性条目 ${formatCount(r.attribute_count || 0)}`)).join('') : emptyState('当前没有 cfs_* 的 bound attributes 聚合数据。');

    const lastTick = rep?.trace_id || dash?.trace_id || `t${Date.now()}`;
    const autoTune = rep?.auto_tuner_short_term || rep?.auto_tuner || {};
    const appliedUpdates = asArray(autoTune?.applied_updates);
    STATE.liveAutoTuneLog = pushBounded(STATE.liveAutoTuneLog, {
      trace_id: lastTick,
      title: `tick ${lastTick} | 短期微调 ${appliedUpdates.length ? '生效' : '未生效'}`,
      desc: appliedUpdates.length ? appliedUpdates.slice(0,6).map((u)=> `${u.param || '-'} | Δ=${formatMaybe(u.delta,6)} | ${u.reason || '-'}`).join('\n') : `原因：${autoTune?.reason || '当前窗口没有需要处理的明显偏离。'}`,
    }, 80);
    if (DOM.expLiveAutoTuneLog) DOM.expLiveAutoTuneLog.innerHTML = STATE.liveAutoTuneLog.slice().reverse().slice(0,10).map((it)=>miniRow(it.title, truncateText(it.desc, 140))).join('') || emptyState('还没有短期微调条目。');

    const action = rep?.action || {};
    const executed = asArray(action?.executed_actions);
    const triggers = asArray(action?.triggered_actions || action?.triggered || []);
    STATE.liveActionLog = pushBounded(STATE.liveActionLog, {
      trace_id: lastTick,
      title: `tick ${lastTick} | 触发 ${formatCount(triggers.length)} | 执行 ${formatCount(executed.filter((x)=>x?.success).length)}/${formatCount(executed.length)}`,
      desc: executed.length ? executed.slice(0,6).map((row)=> {
        const kind = String(row?.action_kind || row?.kind || 'unknown');
        const params = row?.params && typeof row.params === 'object' ? Object.entries(row.params).slice(0,4).map(([k,v])=> `${k}=${String(v)}`).join(', ') : '';
        return `${actionKindLabel(kind)}${row?.action_id ? `(${row.action_id})` : ''} | ${row?.success ? 'OK' : 'SKIP'}${params ? ` | 参数 ${params}` : ''}${row?.reason ? ` | ${row.reason}` : ''}`;
      }).join('\n') : '本 tick 没有执行动作。',
    }, 80);
    if (DOM.expLiveActionLog) DOM.expLiveActionLog.innerHTML = STATE.liveActionLog.slice().reverse().slice(0,10).map((it)=>miniRow(it.title, truncateText(it.desc, 140))).join('') || emptyState('还没有行动触发/执行条目。');
  } catch(error) {}
}
window.refreshLiveMonitor = refreshLiveMonitor;

async function refreshLlmConfig(silent=false){
  try {
    const res = await apiGet('/api/experiment/llm_review/config');
    const cfg = res.data?.config || {};
    if (DOM.expLlmConfigMeta) DOM.expLlmConfigMeta.textContent = `来源：配置文件 | Key：${cfg.api_key_masked || '-'}`;
    if (DOM.expLlmEnabledChk) DOM.expLlmEnabledChk.checked = Boolean(cfg.enabled);
    if (DOM.expLlmAutoChk) DOM.expLlmAutoChk.checked = Boolean(cfg.auto_review_on_completion);
    if (DOM.expLlmBaseUrl) DOM.expLlmBaseUrl.value = String(cfg.base_url || '');
    if (DOM.expLlmModel) DOM.expLlmModel.value = String(cfg.model || '');
    if (DOM.expLlmMaxPromptChars) DOM.expLlmMaxPromptChars.value = String(cfg.max_prompt_chars || 900000);
    if (!silent) setFeedback(DOM.expLlmSaveFeedback, '已刷新 LLM Review 配置。', 'ok');
  } catch(error){ if(!silent) setFeedback(DOM.expLlmSaveFeedback, `刷新 LLM 配置失败：${error.message}`, 'err'); }
}
async function saveLlmConfig(){
  try {
    const config = {
      enabled: Boolean(DOM.expLlmEnabledChk?.checked),
      auto_review_on_completion: Boolean(DOM.expLlmAutoChk?.checked),
      base_url: String(DOM.expLlmBaseUrl?.value || '').trim(),
      model: String(DOM.expLlmModel?.value || '').trim(),
      api_key: String(DOM.expLlmApiKey?.value || '').trim(),
      max_prompt_chars: Math.max(1000, asNumber(DOM.expLlmMaxPromptChars?.value, 900000)),
    };
    await apiPost('/api/experiment/llm_review/config/save', { config });
    await refreshLlmConfig(true);
    setFeedback(DOM.expLlmSaveFeedback, '已保存 LLM Review 配置。', 'ok');
  } catch(error){ setFeedback(DOM.expLlmSaveFeedback, `保存 LLM 配置失败：${error.message}`, 'err'); }
}
async function refreshLlmStatus(runId, silent=false){
  const rid = String(runId || STATE.selectedRunId || '').trim();
  if(!rid) return;
  try {
    const [st, rp] = await Promise.all([
      apiGet(`/api/experiment/run/llm_review_status?run_id=${encodeURIComponent(rid)}`),
      apiGet(`/api/experiment/run/llm_review_report?run_id=${encodeURIComponent(rid)}`),
    ]);
    const status = st.data || {};
    const report = String(rp.data?.text || rp.data?.report_markdown || rp.data?.report_text || '');
    const stage = String(status.stage || '-');
    const errorCode = String(status.error || status.error_code || '-');
    const message = String(status.message || '-');
    const reportPath = String(status.report_path || '-');
    const errorPath = String(status.error_path || '-');
    const receivedChars = asNumber(status.received_chars, 0);
    if (DOM.expLlmStatusMeta) {
      DOM.expLlmStatusMeta.textContent = `run_id=${rid} | status=${status.status || '-'} | stage=${stage} | job_id=${status.job_id || '-'} | chars=${receivedChars} | error=${errorCode}`;
    }
    if (DOM.expLlmReport) {
      DOM.expLlmReport.textContent = report || [
        '当前还没有 LLM Review 报告。',
        `状态：${status.status || '-'}`,
        `阶段：${stage}`,
        `错误码：${errorCode}`,
        `消息：${message}`,
        `报告路径：${reportPath}`,
        `错误路径：${errorPath}`,
      ].join('\n');
    }
    if (!silent) setFeedback(DOM.expLlmStatusFeedback, '已刷新 LLM Review 状态。', 'ok');
  } catch(error){ if(!silent) setFeedback(DOM.expLlmStatusFeedback, `刷新 LLM Review 状态失败：${error.message}`, 'err'); }
}
function stopLlmPolling(){ if(STATE.llmPollTimer){ clearInterval(STATE.llmPollTimer); STATE.llmPollTimer = null; } }
function startLlmPolling(runId){
  const rid = String(runId || '').trim();
  if(!rid) return;
  stopLlmPolling();
  refreshLlmStatus(rid, true).catch(()=>{});
  STATE.llmPollTimer = setInterval(async ()=> {
    try {
      await refreshLlmStatus(rid, true);
      const meta = String(DOM.expLlmStatusMeta?.textContent || '');
      if (/status=(completed|failed|cancelled|done|error)/i.test(meta)) stopLlmPolling();
    } catch {}
  }, 2000);
}
async function startLlmReview(){
  const rid = String(STATE.selectedRunId || '').trim();
  if(!rid) return setFeedback(DOM.expLlmStatusFeedback, '请先选择一个运行记录。', 'err');
  try {
    await apiPost('/api/experiment/run/llm_review/start', { run_id: rid, force: false });
    setFeedback(DOM.expLlmStatusFeedback, '已启动 LLM Review 任务。', 'ok');
    await refreshLlmStatus(rid, true);
    startLlmPolling(rid);
  } catch(error){ setFeedback(DOM.expLlmStatusFeedback, `启动 LLM Review 失败：${error.message}`, 'err'); }
}
async function startLlmReviewForce(){
  const rid = String(STATE.selectedRunId || '').trim();
  if(!rid) return setFeedback(DOM.expLlmStatusFeedback, '请先选择一个运行记录。', 'err');
  try {
    await apiPost('/api/experiment/run/llm_review/start', { run_id: rid, force: true });
    setFeedback(DOM.expLlmStatusFeedback, '已强制启动 LLM Review 任务。', 'ok');
    await refreshLlmStatus(rid, true);
    startLlmPolling(rid);
  } catch(error){ setFeedback(DOM.expLlmStatusFeedback, `强制启动 LLM Review 失败：${error.message}`, 'err'); }
}
function copyLlmReport(){ const text = String(DOM.expLlmReport?.textContent || ''); if(!text) return; navigator.clipboard?.writeText(text); }
function downloadLlmReport(){ const text = String(DOM.expLlmReport?.textContent || ''); if(!text) return; const blob = new Blob([text], {type:'text/markdown;charset=utf-8'}); const url = URL.createObjectURL(blob); const a = document.createElement('a'); a.href = url; a.download = `llm_review_${STATE.selectedRunId || 'latest'}.md`; a.click(); URL.revokeObjectURL(url); }

document.addEventListener('DOMContentLoaded', async () => {
  bindDom();
  if (DOM.expBackBtn) DOM.expBackBtn.addEventListener('click', ()=> { try { window.history.back(); } catch { window.location.href = '/'; } });
  if (DOM.expRefreshProtocolBtn) DOM.expRefreshProtocolBtn.addEventListener('click', ()=> refreshProtocol());
  if (DOM.expRefreshDatasetsBtn) DOM.expRefreshDatasetsBtn.addEventListener('click', ()=> refreshDatasets());
  if (DOM.expPreviewBtn) DOM.expPreviewBtn.addEventListener('click', ()=> previewDataset());
  if (DOM.expExpandBtn) DOM.expExpandBtn.addEventListener('click', ()=> expandDataset());
  if (DOM.expImportBtn) DOM.expImportBtn.addEventListener('click', ()=> importDataset());
  if (DOM.expRunStartBtn) DOM.expRunStartBtn.addEventListener('click', ()=> window.startRun());
  if (DOM.expRunStopBtn) DOM.expRunStopBtn.addEventListener('click', ()=> window.stopRun());
  if (DOM.expLivePauseBtn) DOM.expLivePauseBtn.addEventListener('click', ()=> {
    STATE.livePaused = !STATE.livePaused;
    DOM.expLivePauseBtn.textContent = STATE.livePaused ? '继续刷新' : '暂停刷新';
    if (!STATE.livePaused) refreshLiveMonitor();
  });
  if (DOM.expLiveClearBtn) DOM.expLiveClearBtn.addEventListener('click', ()=> {
    STATE.liveActionLog = [];
    STATE.liveAutoTuneLog = [];
    if (DOM.expLiveAutoTuneLog) DOM.expLiveAutoTuneLog.innerHTML = emptyState('还没有短期微调条目。');
    if (DOM.expLiveActionLog) DOM.expLiveActionLog.innerHTML = emptyState('还没有行动触发/执行条目。');
  });
  if (DOM.expRefreshRunsBtn) DOM.expRefreshRunsBtn.addEventListener('click', ()=> refreshRuns());
  if (DOM.expRefreshRunsInlineBtn) DOM.expRefreshRunsInlineBtn.addEventListener('click', ()=> refreshRuns());
  if (DOM.expRefreshRunSummaryBtn) DOM.expRefreshRunSummaryBtn.addEventListener('click', ()=> { if (STATE.selectedRunId) selectRun(STATE.selectedRunId, { reloadMetrics: true }); else refreshRuns(); });
  if (DOM.expDeleteRunBtn) DOM.expDeleteRunBtn.addEventListener('click', ()=> deleteSelectedRun());
  if (DOM.expClearRunsBtn) DOM.expClearRunsBtn.addEventListener('click', ()=> clearRuns());
  if (DOM.expClearRuntimeBtn) DOM.expClearRuntimeBtn.addEventListener('click', ()=> clearRuntime());
  if (DOM.expClearHdbBtn) DOM.expClearHdbBtn.addEventListener('click', ()=> clearHdb());
  if (DOM.expClearAllBtn) DOM.expClearAllBtn.addEventListener('click', ()=> clearAll());
  if (DOM.expDatasetSelect) DOM.expDatasetSelect.addEventListener('change', ()=> { STATE.selectedDatasetKey = String(DOM.expDatasetSelect.value || ''); renderDatasets(); });
  if (DOM.expDownsampleEvery) DOM.expDownsampleEvery.addEventListener('change', ()=> { if (STATE.selectedRunId) selectRun(STATE.selectedRunId, { reloadMetrics: true }); });
  if (DOM.expLlmRefreshBtn) DOM.expLlmRefreshBtn.addEventListener('click', ()=> refreshLlmConfig());
  if (DOM.expLlmStatusRefreshBtn) DOM.expLlmStatusRefreshBtn.addEventListener('click', ()=> refreshLlmStatus(STATE.selectedRunId));
  if (DOM.expLlmSaveBtn) DOM.expLlmSaveBtn.addEventListener('click', ()=> saveLlmConfig());
  if (DOM.expLlmStartBtn) DOM.expLlmStartBtn.addEventListener('click', ()=> startLlmReview());
  if (DOM.expLlmStartForceBtn) DOM.expLlmStartForceBtn.addEventListener('click', ()=> startLlmReviewForce());
  if (DOM.expLlmCopyReportBtn) DOM.expLlmCopyReportBtn.addEventListener('click', ()=> copyLlmReport());
  if (DOM.expLlmDownloadReportBtn) DOM.expLlmDownloadReportBtn.addEventListener('click', ()=> downloadLlmReport());
  await refreshProtocol(false).catch(()=>{});
  await refreshDatasets(false).catch(()=>{});
  await refreshRuns(false).catch(()=>{});
  await refreshLlmConfig(true).catch(()=>{});
  if (STATE.selectedRunId) await refreshLlmStatus(STATE.selectedRunId, true).catch(()=>{});
  await refreshLiveMonitor().catch(()=>{});
  LIVE_TIMER = setInterval(()=> { if (!STATE.livePaused) refreshLiveMonitor(); }, 3000);
  if (DOM.expChartModalCloseBtn) DOM.expChartModalCloseBtn.addEventListener('click', closeChartModal);
  if (DOM.expChartModalScrim) DOM.expChartModalScrim.addEventListener('click', closeChartModal);
  if (DOM.expChartModalFullscreenBtn) DOM.expChartModalFullscreenBtn.addEventListener('click', ()=> DOM.expChartModal?.classList.toggle('modal-fullscreen'));
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && DOM.expChartModal && !DOM.expChartModal.hidden) closeChartModal();
  });
});
