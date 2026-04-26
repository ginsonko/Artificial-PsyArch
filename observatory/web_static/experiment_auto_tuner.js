(function () {
  if (typeof STATE === "undefined" || typeof apiGet !== "function" || typeof apiPost !== "function") {
    return;
  }

  const AT = {
    dom: {},
    ruleDisabled: new Set(),
    ruleProtected: new Set(),
    llmPollTimer: null,
  };

  STATE.autoTunerConfig = null;
  STATE.autoTunerCatalog = null;
  STATE.autoTunerState = null;
  STATE.autoTunerRules = null;
  STATE.autoTunerAudit = null;
  STATE.autoTunerRollbackPoints = null;
  STATE.autoTunerLlmConfig = null;
  STATE.autoTunerLlmJobs = null;

  function q(id) {
    return document.getElementById(id);
  }

  function feedback(id, message, kind = "ok") {
    setFeedback(AT.dom[id], message, kind);
  }

  function boolLabel(value) {
    return value ? "已开启" : "已关闭";
  }

  function formatMaybe(value, digits = 4) {
    return value === null || value === undefined || value === "" ? "-" : formatNumber(value, digits);
  }

  function ruleChip(label, cls = "") {
    return `<span class="chip ${cls}">${esc(label)}</span>`;
  }

  function renamePageChrome() {
    document.title = "长期运行数据观测台";
    const brand = document.querySelector(".sidebar .brand h1");
    if (brand) brand.textContent = "长期运行数据观测台";
    const brandText = document.querySelector(".sidebar .brand p");
    if (brandText) {
      brandText.textContent = "用于观察长期运行指标、管理自适应调参、审计调参历史，并与“单次 tick 数据观测台”区分开。";
    }
    const heroEyebrow = document.querySelector(".hero .eyebrow");
    if (heroEyebrow) heroEyebrow.textContent = "AP Prototype Long-Run Observatory";
    const heroTitle = document.querySelector(".hero h2");
    if (heroTitle) heroTitle.textContent = "长期运行数据观测台";
    const heroDesc = document.querySelector(".hero p");
    if (heroDesc) {
      heroDesc.textContent =
        "这里面向长跑、长期指标、调参闭环与复盘证据。它和主页面的“单次 tick 数据观测台”不同，重点不是看某一轮细节，而是看系统在较长时间内是否稳定、是否贴近理论预期、以及为什么会偏离。";
    }
    const nav = document.querySelector(".sidebar-nav");
    if (nav && !nav.querySelector('a[href="#exp_auto_tuner"]')) {
      const anchor = document.createElement("a");
      anchor.href = "#exp_auto_tuner";
      anchor.textContent = "自适应调参器";
      const ref = nav.querySelector('a[href="#exp_llm_review"]');
      if (ref) {
        nav.insertBefore(anchor, ref);
      } else {
        nav.appendChild(anchor);
      }
    }
  }

  function collectMetricTargets() {
    return Array.from(document.querySelectorAll("[data-at-metric-row]")).map((row) => {
      const key = String(row.getAttribute("data-key") || "");
      return {
        key,
        expected_min: asNumber(row.querySelector('[data-field="expected_min"]')?.value, 0),
        expected_max: asNumber(row.querySelector('[data-field="expected_max"]')?.value, 0),
        ideal: asNumber(row.querySelector('[data-field="ideal"]')?.value, 0),
        min_std: asNumber(row.querySelector('[data-field="min_std"]')?.value, 0),
        weight: asNumber(row.querySelector('[data-field="weight"]')?.value, 1),
      };
    });
  }

  function renderOverview() {
    const cfg = STATE.autoTunerConfig?.config || {};
    const state = STATE.autoTunerState?.summary || {};
    const rulesSummary = STATE.autoTunerRules?.catalog?.summary || {};
    const llmCfg = STATE.autoTunerLlmConfig?.config || {};
    if (!AT.dom.expAutoTunerOverviewCards) return;
    AT.dom.expAutoTunerOverviewCards.innerHTML = [
      metricCard("调参器状态", boolLabel(Boolean(cfg.enabled)), `短期：${boolLabel(Boolean(cfg.enable_short_term))} | 长期：${boolLabel(Boolean(cfg.enable_long_term))}`),
      metricCard("持久参数数", formatCount(state.persisted_param_count), `运行时参数：${formatCount(state.runtime_param_count)}`),
      metricCard("活跃试验数", formatCount(state.active_trial_count), `已归档试验：${formatCount(state.trial_history_count)}`),
      metricCard("规则总量", formatCount((rulesSummary.builtin_count || 0) + (rulesSummary.generated_count || 0) + (rulesSummary.custom_count || 0)), `禁用：${formatCount(rulesSummary.disabled_count)} | 白名单：${formatCount(rulesSummary.protected_count)}`),
      metricCard("规则健康记录", formatCount(state.rule_health_count), "用于判断某条规则是长期有效、长期失效，还是经常触发回滚。"),
      metricCard("LLM 分析", boolLabel(Boolean(llmCfg.enabled)), `模型：${llmCfg.model || "-"} | 来源：${STATE.autoTunerLlmConfig?.source || "-"}`),
      metricCard("LLM 候选规则", formatCount(state.llm_candidate_rule_count), `已固化：${formatCount(state.llm_solidified_rule_count)} | 已拒绝：${formatCount(state.llm_rejected_rule_count)}`),
    ].join("");
  }

  function renderMetricTargets() {
    const items = STATE.autoTunerConfig?.metric_targets || [];
    if (!AT.dom.expAtMetricTargets) return;
    if (!items.length) {
      AT.dom.expAtMetricTargets.innerHTML = emptyState("当前没有可编辑的长期指标基线。");
      return;
    }
    AT.dom.expAtMetricTargets.innerHTML = items.map((item) => {
      return `
        <details class="details-panel" data-at-metric-row data-key="${esc(item.key)}">
          <summary>
            <div class="mini-row">
              <div class="title">${esc(item.title || item.key)}</div>
              <div class="desc">${esc(item.description || "")}</div>
              <div class="chips">
                ${ruleChip(item.group || "未分组")}
                ${ruleChip(`正常范围 ${formatMaybe(item.expected_min)} ~ ${formatMaybe(item.expected_max)}`, "accent")}
                ${ruleChip(`理想值 ${formatMaybe(item.ideal)}`, "warn")}
              </div>
            </div>
          </summary>
          <div class="details-body">
            <div class="settings-grid">
              <article class="setting-item">
                <label>正常范围下限</label>
                <input data-field="expected_min" type="number" step="0.01" value="${esc(item.expected_min)}" />
              </article>
              <article class="setting-item">
                <label>正常范围上限</label>
                <input data-field="expected_max" type="number" step="0.01" value="${esc(item.expected_max)}" />
              </article>
              <article class="setting-item">
                <label>理想值</label>
                <input data-field="ideal" type="number" step="0.01" value="${esc(item.ideal)}" />
              </article>
              <article class="setting-item">
                <label>最小自然波动</label>
                <input data-field="min_std" type="number" step="0.01" value="${esc(item.min_std || 0)}" />
              </article>
              <article class="setting-item">
                <label>规则权重</label>
                <input data-field="weight" type="number" step="0.01" value="${esc(item.weight ?? 1)}" />
                <small>数值越高，表示调参器越重视这个长期指标的偏离。</small>
              </article>
            </div>
          </div>
        </details>
      `;
    }).join("");
  }

  function renderParamCatalog() {
    const data = STATE.autoTunerCatalog;
    if (!AT.dom.expAtParamCatalog) return;
    const all = Array.isArray(data?.params) ? data.params : [];
    const keyword = String(AT.dom.expAtParamSearch?.value || "").trim().toLowerCase();
    const rows = all.filter((item) => {
      if (!keyword) return true;
      const hay = [item.param_id, item.module, (item.impacts || []).join(" "), (item.tags || []).join(" ")].join(" ").toLowerCase();
      return hay.includes(keyword);
    });
    if (!rows.length) {
      AT.dom.expAtParamCatalog.innerHTML = emptyState("没有匹配的参数。");
      return;
    }
    AT.dom.expAtParamCatalog.innerHTML = rows.slice(0, 220).map((item) => {
      const bound = data?.param_bounds?.[item.param_id] || null;
      return `
        <details class="details-panel">
          <summary>
            <div class="mini-row">
              <div class="title">${esc(item.param_id)}</div>
              <div class="desc">模块：${esc(item.module)} | 类型：${esc(item.value_type)} | 当前值：${esc(String(item.value))}</div>
              <div class="chips">
                ${ruleChip(item.auto_tune_allowed ? "允许自动调参" : "仅观测", item.auto_tune_allowed ? "accent" : "danger")}
                ${(item.tags || []).slice(0, 5).map((tag) => ruleChip(tag)).join("")}
              </div>
            </div>
          </summary>
          <div class="details-body">
            <div class="stack">
              <div class="mini-row">
                <div class="title">影响的长期指标</div>
                <div class="desc">${esc((item.impacts || []).join("、") || "未识别")}</div>
              </div>
              <div class="mini-row">
                <div class="title">推荐边界</div>
                <div class="desc">${
                  bound
                    ? `范围 ${formatMaybe(bound.min_value)} ~ ${formatMaybe(bound.max_value)}，单步不超过 ${formatMaybe(bound.max_step_abs)}，量化粒度 ${formatMaybe(bound.quantum)}`
                    : "当前没有额外边界说明。"
                }</div>
              </div>
              <div class="mini-row">
                <div class="title">说明</div>
                <div class="desc">${esc(item.note || "这是自动索引得到的参数，可用于建立“参数-指标-规则”的可审计对应关系。")}</div>
              </div>
            </div>
          </div>
        </details>
      `;
    }).join("");
  }

  function renderRules() {
    const rules = STATE.autoTunerRules;
    if (!AT.dom.expAtRuleCatalog) return;
    const builtin = rules?.catalog?.builtin_rules || [];
    const custom = rules?.catalog?.custom_rules || [];
    const generated = rules?.catalog?.generated_rules || [];
    const summary = rules?.catalog?.summary || {};
    if (AT.dom.expAtRuleSummary) {
      AT.dom.expAtRuleSummary.textContent =
        `${formatTime(Date.now())} | 内建规则 ${formatCount(summary.builtin_count)} 条，生成规则 ${formatCount(summary.generated_count)} 条，自定义规则 ${formatCount(summary.custom_count)} 条，禁用 ${formatCount(summary.disabled_count)} 条，白名单 ${formatCount(summary.protected_count)} 条。`;
    }
    const renderRuleBlock = (title, items, limit = items.length) => {
      if (!items.length) return "";
      return `
        <details class="details-panel">
          <summary><h4>${esc(title)}</h4></summary>
          <div class="details-body stack">
            ${items.slice(0, limit).map((rule) => {
              const ruleId = String(rule.rule_id || rule.id || "");
              const disabled = AT.ruleDisabled.has(ruleId);
              const protectedFlag = AT.ruleProtected.has(ruleId);
              return `
                <article class="mini-row rule-row">
                  <div class="title">${esc(rule.title || ruleId)}</div>
                  <div class="desc">
                    指标：${esc(rule.metric_key || "-")} | 模式：${esc(rule.issue_mode || "-")} | 参数：${esc(rule.param_id || "-")}
                  </div>
                  <div class="chips">
                    ${ruleChip(rule.source || "rule")}
                    ${ruleChip(disabled ? "已禁用" : "启用中", disabled ? "danger" : "accent")}
                    ${ruleChip(protectedFlag ? "LLM 白名单" : "允许 LLM 分析", protectedFlag ? "warn" : "")}
                    ${rule.module ? ruleChip(rule.module) : ""}
                  </div>
                  <div class="actions compact-actions exp-auto-rule-actions">
                    <button type="button" class="ghost" data-at-rule-toggle="disable" data-rule-id="${esc(ruleId)}">${disabled ? "恢复规则" : "禁用规则"}</button>
                    <button type="button" class="ghost" data-at-rule-toggle="protect" data-rule-id="${esc(ruleId)}">${protectedFlag ? "移出白名单" : "加入白名单"}</button>
                  </div>
                </article>
              `;
            }).join("")}
          </div>
        </details>
      `;
    };
    AT.dom.expAtRuleCatalog.innerHTML = [
      renderRuleBlock("内建规则", builtin),
      renderRuleBlock("自定义规则", custom),
      renderRuleBlock("生成规则（按参数-指标影响自动展开，已截断展示前 180 条）", generated, 180),
    ].join("");

    AT.dom.expAtRuleCatalog.querySelectorAll("[data-at-rule-toggle]").forEach((btn) => {
      btn.addEventListener("click", () => {
        const ruleId = String(btn.getAttribute("data-rule-id") || "");
        const mode = String(btn.getAttribute("data-at-rule-toggle") || "");
        if (!ruleId) return;
        if (mode === "disable") {
          if (AT.ruleDisabled.has(ruleId)) {
            AT.ruleDisabled.delete(ruleId);
          } else {
            AT.ruleDisabled.add(ruleId);
          }
        } else if (mode === "protect") {
          if (AT.ruleProtected.has(ruleId)) {
            AT.ruleProtected.delete(ruleId);
          } else {
            AT.ruleProtected.add(ruleId);
          }
        }
        renderRules();
      });
    });

    if (AT.dom.expAtCustomRulesEditor) {
      AT.dom.expAtCustomRulesEditor.value = JSON.stringify(rules?.rules?.custom_rules || [], null, 2);
    }
  }

  function renderRecentLlmSuggestions() {
    const items = STATE.autoTunerState?.recent_llm_suggestions || [];
    if (!AT.dom.expAtLlmSuggestionList) return;
    if (!items.length) {
      AT.dom.expAtLlmSuggestionList.innerHTML = emptyState("当前还没有自动闭环建议。只有在规则长期失效、回滚偏多或问题持续累积时，才会触发这部分分析。");
      return;
    }
    AT.dom.expAtLlmSuggestionList.innerHTML = items.slice(0, 12).map((item) => {
      const parsed = item.parsed_json || {};
      const counts = item.counts || {};
      const apply = item.auto_apply_result || {};
      const changed = Boolean(apply.changed);
      const applyText = apply.success
        ? (changed
          ? `已自动应用 ${formatCount((apply.applied_rule_changes || []).length)} 条规则动作，加入 ${formatCount((apply.added_experiments || []).length)} 个候选试验。`
          : "已完成分析，但本轮没有满足自动应用条件的变更。")
        : "当前还没有自动应用结果。";
      return `
        <details class="details-panel">
          <summary>
            <div class="mini-row">
              <div class="title">${esc(formatTime(item.created_at_ms))} | run ${esc(item.run_id || "global")}</div>
              <div class="desc">${esc(parsed.summary || "这次建议没有填写额外摘要。")}</div>
              <div class="chips">
                ${ruleChip(`发现 ${formatCount(counts.metric_findings)} 项指标问题`, "accent")}
                ${ruleChip(`规则动作 ${formatCount(counts.rule_changes)}`)}
                ${ruleChip(`候选试验 ${formatCount(counts.experiments)}`, "warn")}
                ${ruleChip(changed ? "本轮已自动应用" : "本轮未自动落地", changed ? "accent" : "")}
              </div>
            </div>
          </summary>
          <div class="details-body stack">
            ${miniRow("自动应用结果", applyText)}
            ${miniRow("重点指标", (item.focus_metrics || []).join("、") || "这次没有显式限定重点指标。")}
            ${miniRow("主要发现", (parsed.metric_findings || []).map((row) => `${row.metric_key} | ${row.status} | ${row.reason || "未写原因"}`).join("\n") || "没有结构化 metric_findings。")}
            ${miniRow("补充说明", (parsed.notes || []).join("\n") || "没有补充说明。")}
            ${miniRow("报告摘录", item.report_excerpt || "没有可展示的报告摘录。")}
          </div>
        </details>
      `;
    }).join("");
  }

  function renderStateAndAudit() {
    const stateWrap = STATE.autoTunerState || {};
    const state = stateWrap.state || {};
    const audit = STATE.autoTunerAudit?.items || [];
    const ruleHealth = state.rule_health || {};
    const healthRows = Object.values(ruleHealth).sort((a, b) => asNumber(b.hit_count, 0) - asNumber(a.hit_count, 0)).slice(0, 16);
    if (AT.dom.expAtStateSummary) {
      AT.dom.expAtStateSummary.innerHTML = [
        miniRow("持久参数", `当前持久参数 ${formatCount(Object.keys(state.persisted_params || {}).length)} 个；活跃试验 ${formatCount((state.active_trials || []).length)} 个；试验历史 ${formatCount((state.trial_history || []).length)} 条。`),
        miniRow(
          "规则健康度 Top",
          healthRows.length
            ? healthRows.map((row) => `${row.rule_id} | 命中 ${formatCount(row.hit_count)} | 成功 ${formatCount(row.success_count)} | 失败 ${formatCount(row.failure_count)} | 回滚 ${formatCount(row.rollback_count)}`).join("\n")
            : "当前还没有规则健康记录。"
        ),
      ].join("");
    }
    if (AT.dom.expAtAuditLog) {
      if (!audit.length) {
        AT.dom.expAtAuditLog.innerHTML = emptyState("当前还没有调参审计日志。");
      } else {
        AT.dom.expAtAuditLog.innerHTML = audit.slice(0, 40).map((item) => {
          return miniRow(
            `${formatTime(item.ts_ms)} | ${item.kind || "event"}`,
            JSON.stringify(item, null, 2)
          );
        }).join("");
      }
    }
    renderRecentLlmSuggestions();
  }

  function renderRollbackPoints() {
    const points = STATE.autoTunerRollbackPoints?.points || [];
    if (!AT.dom.expAtRollbackList) return;
    if (!points.length) {
      AT.dom.expAtRollbackList.innerHTML = emptyState("当前还没有回滚点。");
      return;
    }
    AT.dom.expAtRollbackList.innerHTML = points.map((point) => {
      return `
        <article class="mini-row">
          <div class="title">${esc(point.point_id || "-")}</div>
          <div class="desc">时间：${esc(formatTime(point.created_at_ms))}\n原因：${esc(point.reason || "-")}\n参数数：${formatCount(Object.keys(point.persisted_params || {}).length)}</div>
          <div class="actions compact-actions">
            <button type="button" class="ghost danger" data-at-rollback="${esc(point.point_id)}">恢复到此回滚点</button>
          </div>
        </article>
      `;
    }).join("");
    AT.dom.expAtRollbackList.querySelectorAll("[data-at-rollback]").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const pointId = String(btn.getAttribute("data-at-rollback") || "");
        if (!pointId) return;
        if (!window.confirm(`确定要回滚到 ${pointId} 吗？这会恢复当前持久参数并尝试立即热更新运行态。`)) {
          return;
        }
        feedback("expAtConfigFeedback", `正在回滚到 ${pointId}…`, "busy");
        try {
          await apiPost("/api/experiment/auto_tuner/rollback", { point_id: pointId });
          feedback("expAtConfigFeedback", `已回滚到 ${pointId}。`, "ok");
          await refreshAutoTunerAll({ silent: true });
        } catch (error) {
          feedback("expAtConfigFeedback", `回滚失败：${error.message}`, "err");
        }
      });
    });
  }

  function renderLlmConfig() {
    const cfg = STATE.autoTunerLlmConfig?.config || {};
    if (AT.dom.expAtLlmMeta) {
      AT.dom.expAtLlmMeta.textContent = `来源：${STATE.autoTunerLlmConfig?.source || "-"} | Key：${cfg.api_key_masked || "-"}`;
    }
    if (AT.dom.expAtLlmEnabledChk) AT.dom.expAtLlmEnabledChk.checked = Boolean(cfg.enabled);
    if (AT.dom.expAtLlmAutoChk) AT.dom.expAtLlmAutoChk.checked = Boolean(cfg.auto_analyze_on_completion);
    if (AT.dom.expAtLlmBaseUrl) AT.dom.expAtLlmBaseUrl.value = String(cfg.base_url || "");
    if (AT.dom.expAtLlmModel) AT.dom.expAtLlmModel.value = String(cfg.model || "");
    if (AT.dom.expAtLlmMaxPromptChars) AT.dom.expAtLlmMaxPromptChars.value = String(cfg.max_prompt_chars || 900000);
    if (AT.dom.expAtLlmApiKey) AT.dom.expAtLlmApiKey.value = "";
  }

  function renderLlmJobs() {
    const jobs = STATE.autoTunerLlmJobs?.jobs || [];
    if (!AT.dom.expAtLlmJobs) return;
    if (!jobs.length) {
      AT.dom.expAtLlmJobs.innerHTML = emptyState("当前还没有调参器 LLM 分析任务。");
      if (AT.dom.expAtLlmReport) AT.dom.expAtLlmReport.textContent = "等待分析结果…";
      return;
    }
    AT.dom.expAtLlmJobs.innerHTML = jobs.map((job) => {
      return miniRow(
        `${job.job_id} | ${job.status}`,
        `运行记录：${job.run_id || "未指定"}\n开始：${formatTime(job.started_at_ms)}\n结束：${formatTime(job.finished_at_ms)}\n错误：${job.error || "无"}`
      );
    }).join("");
    const latest = jobs[0];
    const text = latest?.result?.report_text || latest?.result?.raw || "";
    if (AT.dom.expAtLlmReport) {
      AT.dom.expAtLlmReport.textContent = text || "最近一次任务还没有返回可展示的结果。";
    }
    const hasRunning = jobs.some((job) => ["queued", "running"].includes(String(job.status || "")));
    if (hasRunning && !AT.llmPollTimer) {
      AT.llmPollTimer = setInterval(() => refreshAutoTunerLlmJobs({ silent: true }), 2500);
    }
    if (!hasRunning && AT.llmPollTimer) {
      clearInterval(AT.llmPollTimer);
      AT.llmPollTimer = null;
    }
  }

  async function refreshAutoTunerConfig({ silent = false } = {}) {
    const res = await apiGet("/api/experiment/auto_tuner/config");
    STATE.autoTunerConfig = res.data || null;
    const cfg = STATE.autoTunerConfig?.config || {};
    if (AT.dom.expAtEnabledChk) AT.dom.expAtEnabledChk.checked = Boolean(cfg.enabled);
    if (AT.dom.expAtShortChk) AT.dom.expAtShortChk.checked = Boolean(cfg.enable_short_term);
    if (AT.dom.expAtLongChk) AT.dom.expAtLongChk.checked = Boolean(cfg.enable_long_term);
    if (AT.dom.expAtLlmAssistChk) AT.dom.expAtLlmAssistChk.checked = Boolean(cfg.llm_assist_enabled);
    if (AT.dom.expAtShortWindow) AT.dom.expAtShortWindow.value = String(cfg.short_window_ticks ?? 10);
    if (AT.dom.expAtLongWindow) AT.dom.expAtLongWindow.value = String(cfg.long_window_ticks ?? 40);
    if (AT.dom.expAtCooldown) AT.dom.expAtCooldown.value = String(cfg.decision_cooldown_ticks ?? 2);
    if (AT.dom.expAtMaxUpdates) AT.dom.expAtMaxUpdates.value = String(cfg.max_param_updates_per_tick ?? 4);
    renderMetricTargets();
    if (!silent) {
      feedback("expAtConfigFeedback", "已刷新调参器配置。", "ok");
    }
  }

  async function refreshAutoTunerCatalog({ silent = false } = {}) {
    const res = await apiGet("/api/experiment/auto_tuner/catalog");
    STATE.autoTunerCatalog = res.data || null;
    renderParamCatalog();
    if (typeof window.renderCharts === "function") {
      try {
        window.renderCharts();
      } catch {}
    }
    if (!silent && AT.dom.expAtConfigMeta) {
      const summary = STATE.autoTunerCatalog?.summary || {};
      AT.dom.expAtConfigMeta.textContent = `参数目录 ${formatCount(summary.param_count)} 项，可自动调参 ${formatCount(summary.auto_tune_allowed_count)} 项。`;
    }
  }

  async function refreshAutoTunerRules({ silent = false } = {}) {
    const res = await apiGet("/api/experiment/auto_tuner/rules");
    STATE.autoTunerRules = res.data || null;
    AT.ruleDisabled = new Set(STATE.autoTunerRules?.rules?.disabled_rule_ids || []);
    AT.ruleProtected = new Set(STATE.autoTunerRules?.rules?.protected_rule_ids || []);
    renderRules();
    if (!silent) feedback("expAtConfigFeedback", "已刷新规则目录。", "ok");
  }

  async function refreshAutoTunerState({ silent = false } = {}) {
    const [stateRes, auditRes, rollbackRes] = await Promise.all([
      apiGet("/api/experiment/auto_tuner/state"),
      apiGet("/api/experiment/auto_tuner/audit?limit=80"),
      apiGet("/api/experiment/auto_tuner/rollback_points?limit=40"),
    ]);
    STATE.autoTunerState = stateRes.data || null;
    STATE.autoTunerAudit = auditRes.data || null;
    STATE.autoTunerRollbackPoints = rollbackRes.data || null;
    renderOverview();
    renderStateAndAudit();
    renderRollbackPoints();
    if (!silent) feedback("expAtConfigFeedback", "已刷新调参器状态。", "ok");
  }

  async function refreshAutoTunerLlmConfig({ silent = false } = {}) {
    const res = await apiGet("/api/experiment/auto_tuner/llm/config");
    STATE.autoTunerLlmConfig = res.data || null;
    renderLlmConfig();
    if (!silent) feedback("expAtLlmFeedback", "已刷新 LLM 配置。", "ok");
  }

  async function refreshAutoTunerLlmJobs({ silent = false } = {}) {
    const res = await apiGet("/api/experiment/auto_tuner/llm/jobs");
    STATE.autoTunerLlmJobs = res.data || { jobs: [] };
    renderLlmJobs();
    if (!silent) feedback("expAtLlmAnalyzeFeedback", "已刷新分析任务。", "ok");
  }

  async function refreshAutoTunerAll({ silent = false } = {}) {
    await Promise.all([
      refreshAutoTunerConfig({ silent: true }),
      refreshAutoTunerCatalog({ silent: true }),
      refreshAutoTunerRules({ silent: true }),
      refreshAutoTunerState({ silent: true }),
      refreshAutoTunerLlmConfig({ silent: true }),
      refreshAutoTunerLlmJobs({ silent: true }),
    ]);
    renderOverview();
    if (!silent) feedback("expAtConfigFeedback", "已刷新全部自适应调参器数据。", "ok");
  }

  async function saveAutoTunerConfig() {
    feedback("expAtConfigFeedback", "正在保存调参器配置…", "busy");
    try {
      const payload = {
        enabled: Boolean(AT.dom.expAtEnabledChk?.checked),
        enable_short_term: Boolean(AT.dom.expAtShortChk?.checked),
        enable_long_term: Boolean(AT.dom.expAtLongChk?.checked),
        llm_assist_enabled: Boolean(AT.dom.expAtLlmAssistChk?.checked),
        short_window_ticks: asNumber(AT.dom.expAtShortWindow?.value, 10),
        long_window_ticks: asNumber(AT.dom.expAtLongWindow?.value, 40),
        decision_cooldown_ticks: asNumber(AT.dom.expAtCooldown?.value, 2),
        max_param_updates_per_tick: asNumber(AT.dom.expAtMaxUpdates?.value, 4),
        metric_targets: collectMetricTargets(),
      };
      const res = await apiPost("/api/experiment/auto_tuner/config/save", { config: payload });
      STATE.autoTunerConfig = res.data || null;
      renderMetricTargets();
      renderOverview();
      feedback("expAtConfigFeedback", "已保存调参器配置。", "ok");
    } catch (error) {
      feedback("expAtConfigFeedback", `保存失败：${error.message}`, "err");
    }
  }

  async function saveRules() {
    feedback("expAtConfigFeedback", "正在保存规则配置…", "busy");
    try {
      let customRules = [];
      if (AT.dom.expAtCustomRulesEditor?.value.trim()) {
        customRules = JSON.parse(AT.dom.expAtCustomRulesEditor.value);
      }
      const res = await apiPost("/api/experiment/auto_tuner/rules/save", {
        rules: {
          disabled_rule_ids: Array.from(AT.ruleDisabled),
          protected_rule_ids: Array.from(AT.ruleProtected),
          custom_rules: Array.isArray(customRules) ? customRules : [],
        },
      });
      STATE.autoTunerRules = res.data || null;
      AT.ruleDisabled = new Set(STATE.autoTunerRules?.rules?.disabled_rule_ids || []);
      AT.ruleProtected = new Set(STATE.autoTunerRules?.rules?.protected_rule_ids || []);
      renderRules();
      renderOverview();
      feedback("expAtConfigFeedback", "已保存规则配置。", "ok");
    } catch (error) {
      feedback("expAtConfigFeedback", `规则保存失败：${error.message}`, "err");
    }
  }

  async function saveLlmConfig() {
    feedback("expAtLlmFeedback", "正在保存 LLM 配置…", "busy");
    try {
      const payload = {
        enabled: Boolean(AT.dom.expAtLlmEnabledChk?.checked),
        auto_analyze_on_completion: Boolean(AT.dom.expAtLlmAutoChk?.checked),
        base_url: String(AT.dom.expAtLlmBaseUrl?.value || "").trim(),
        api_key: String(AT.dom.expAtLlmApiKey?.value || "").trim(),
        model: String(AT.dom.expAtLlmModel?.value || "").trim(),
        max_prompt_chars: asNumber(AT.dom.expAtLlmMaxPromptChars?.value, 900000),
      };
      const res = await apiPost("/api/experiment/auto_tuner/llm/config/save", { config: payload });
      STATE.autoTunerLlmConfig = res.data || null;
      renderLlmConfig();
      renderOverview();
      feedback("expAtLlmFeedback", "已保存 LLM 配置。", "ok");
    } catch (error) {
      feedback("expAtLlmFeedback", `保存失败：${error.message}`, "err");
    }
  }

  async function startLlmAnalyze() {
    feedback("expAtLlmAnalyzeFeedback", "正在提交调参器分析任务…", "busy");
    try {
      const runId = String(STATE.selectedRunId || "").trim();
      const prompt = String(AT.dom.expAtLlmPrompt?.value || "").trim();
      const res = await apiPost("/api/experiment/auto_tuner/llm/analyze", {
        run_id: runId,
        user_prompt: prompt,
        focus_metrics: [],
      });
      feedback("expAtLlmAnalyzeFeedback", `已提交分析任务：${res.data?.job_id || "-"}`, "ok");
      await refreshAutoTunerLlmJobs({ silent: true });
    } catch (error) {
      feedback("expAtLlmAnalyzeFeedback", `提交失败：${error.message}`, "err");
    }
  }

  function renderOverview() {
    const cfg = STATE.autoTunerConfig?.config || {};
    const state = STATE.autoTunerState?.summary || {};
    const rulesSummary = STATE.autoTunerRules?.catalog?.summary || {};
    const llmCfg = STATE.autoTunerLlmConfig?.config || {};
    if (!AT.dom.expAutoTunerOverviewCards) return;
    AT.dom.expAutoTunerOverviewCards.innerHTML = [
      metricCard("调参器状态", boolLabel(Boolean(cfg.enabled)), `短期：${boolLabel(Boolean(cfg.enable_short_term))} | 长期：${boolLabel(Boolean(cfg.enable_long_term))}`),
      metricCard("持久参数数", formatCount(state.persisted_param_count), `运行时参数：${formatCount(state.runtime_param_count)}`),
      metricCard("活跃试验数", formatCount(state.active_trial_count), `已归档试验：${formatCount(state.trial_history_count)}`),
      metricCard("规则总量", formatCount((rulesSummary.builtin_count || 0) + (rulesSummary.generated_count || 0) + (rulesSummary.custom_count || 0)), `禁用：${formatCount(rulesSummary.disabled_count)} | 白名单：${formatCount(rulesSummary.protected_count)}`),
      metricCard("规则健康记录", formatCount(state.rule_health_count), "用于判断某条规则是长期有效、长期失效，还是经常触发回滚。"),
      metricCard("LLM 分析", boolLabel(Boolean(llmCfg.enabled)), `模型：${llmCfg.model || "-"} | 来源：${STATE.autoTunerLlmConfig?.source || "-"}`),
      metricCard("LLM 候选规则", formatCount(state.llm_candidate_rule_count), `已固化：${formatCount(state.llm_solidified_rule_count)} | 已拒绝：${formatCount(state.llm_rejected_rule_count)}`),
      metricCard("观察区", formatCount(state.observation_active_count), `待复审：${formatCount(state.observation_reviewable_count)} | 已归档：${formatCount(state.observation_history_count)}`),
      metricCard("自动验收", boolLabel(Boolean(cfg.llm_auto_validation_enabled)), `最近复审动作：${formatCount(state.last_observation_review_action_count)} | 历史：${formatCount(state.observation_review_history_count)}`),
    ].join("");
  }

  function renderObservationZone() {
    const stateWrap = STATE.autoTunerState || {};
    const state = stateWrap.state || {};
    const summary = stateWrap.summary || {};
    const active = Array.isArray(state.rule_observations) ? state.rule_observations : [];
    const history = Array.isArray(state.observation_history) ? state.observation_history : [];
    const lastReview = state.last_observation_review || {};

    if (AT.dom.expAtObservationSummary) {
      AT.dom.expAtObservationSummary.innerHTML = [
        miniRow("观察区概况", `当前观察区 ${formatCount(summary.observation_active_count)} 条；其中已满足最少观察轮数、可进入自动验收的有 ${formatCount(summary.observation_reviewable_count)} 条。`),
        miniRow("为什么需要观察区", "观察区的目标不是让 LLM 直接永久改规则，而是先让规则带着证据运行几轮，再判断它究竟是有效、无效，还是需要小步修订后继续观察。"),
      ].join("");
    }

    if (AT.dom.expAtObservationZone) {
      if (!active.length) {
        AT.dom.expAtObservationZone.innerHTML = emptyState("当前还没有进入观察区的规则。只有 LLM 自动建议真正落地后，才会在这里开始积累“生效前 / 生效后”的证据。");
      } else {
        AT.dom.expAtObservationZone.innerHTML = active.slice().reverse().map((item) => {
          const observedRuns = Array.isArray(item.observed_runs) ? item.observed_runs : [];
          const baseline = item.baseline_metric_summary || {};
          const latest = observedRuns.length ? observedRuns[observedRuns.length - 1] : null;
          const effect = latest?.effect || {};
          return `
            <details class="details-panel">
              <summary>
                <div class="mini-row">
                  <div class="title">${esc(item.title || item.rule_id || item.observation_id || "观察项")}</div>
                  <div class="desc">${esc(item.rule_id || "-")} | 来源：${esc(item.source_kind || "-")} | 动作：${esc(item.action || "-")}</div>
                  <div class="chips">
                    ${ruleChip(`观察轮数 ${formatCount(observedRuns.length)}`, "accent")}
                    ${ruleChip(`主指标 ${item.metric_key || "-"}`)}
                    ${ruleChip(`最近结论 ${effect.result || "待观察"}`, effect.result === "better" ? "accent" : effect.result === "worse" ? "danger" : "warn")}
                  </div>
                </div>
              </summary>
              <div class="details-body stack">
                ${miniRow("触发原因", item.reason || "本次没有额外填写原因。")}
                ${miniRow("基线摘要", baseline.metric_key ? `${baseline.metric_key} | 均值 ${formatMaybe(baseline.mean)} | 波动 ${formatMaybe(baseline.std)} | 最新 ${formatMaybe(baseline.latest)}` : "当前还没有可用的基线摘要。")}
                ${miniRow("最近一轮观察", latest ? `${latest.run_id || "-"} | 均值 ${formatMaybe(latest.metric_summary?.mean)} | 波动 ${formatMaybe(latest.metric_summary?.std)} | 最新 ${formatMaybe(latest.metric_summary?.latest)} | 改善比例 ${formatMaybe(effect.ratio)}` : "这条规则刚进入观察区，还没有后续运行证据。")}
                ${miniRow("自动验收状态", `已复审 ${formatCount(item.review_count || 0)} 次 | 最近动作：${item.last_review_result?.action || "尚未复审"} | 最近理由：${item.last_review_result?.reason || "暂无"}`)}
              </div>
            </details>
          `;
        }).join("");
      }
    }

    if (AT.dom.expAtObservationHistory) {
      if (!history.length) {
        AT.dom.expAtObservationHistory.innerHTML = emptyState("观察区历史还没有内容。等自动验收真正做出“固化、回退、移除”等结论后，这里会留下完整痕迹。");
      } else {
        AT.dom.expAtObservationHistory.innerHTML = history.slice().reverse().slice(0, 80).map((item) => {
          const observedRuns = Array.isArray(item.observed_runs) ? item.observed_runs : [];
          return miniRow(
            `${formatTime(item.resolved_at_ms || item.last_review_at_ms || item.created_at_ms)} | ${item.title || item.rule_id || item.observation_id}`,
            `状态：${item.status || "-"}\n规则：${item.rule_id || "-"}\n观察轮数：${formatCount(observedRuns.length)}\n最近动作：${item.last_review_result?.action || "-"}\n理由：${item.last_review_result?.reason || item.reason || "暂无"}`
          );
        }).join("");
      }
    }

    if (AT.dom.expAtObservationReview) {
      const decisions = Array.isArray(lastReview.decisions) ? lastReview.decisions : [];
      if (!lastReview.review_id) {
        AT.dom.expAtObservationReview.innerHTML = emptyState("当前还没有自动验收结果。启用自动验收后，观察区里满足最少观察轮数的规则会在 run 结束时自动复审。");
      } else {
        AT.dom.expAtObservationReview.innerHTML = [
          miniRow(`${formatTime(lastReview.reviewed_at_ms)} | ${lastReview.review_id}`, `运行：${lastReview.run_id || "-"}\n摘要：${lastReview.summary || "暂无摘要"}`),
          miniRow("本轮决策", decisions.length ? decisions.map((item) => `${item.rule_id || item.observation_id || "-"} | ${item.action || "-"}${item.status ? ` | ${item.status}` : ""}`).join("\n") : "这次自动验收没有形成可落地的决策。"),
          miniRow("补充说明", (lastReview.notes || []).join("\n") || "没有额外补充说明。"),
          miniRow("报告摘录", lastReview.report_excerpt || "当前没有可展示的报告摘录。"),
        ].join("");
      }
    }
  }

  function renderStateAndAudit() {
    const stateWrap = STATE.autoTunerState || {};
    const state = stateWrap.state || {};
    const audit = STATE.autoTunerAudit?.items || [];
    const ruleHealth = state.rule_health || {};
    const healthRows = Object.values(ruleHealth).sort((a, b) => asNumber(b.hit_count, 0) - asNumber(a.hit_count, 0)).slice(0, 16);
    if (AT.dom.expAtStateSummary) {
      AT.dom.expAtStateSummary.innerHTML = [
        miniRow("持久参数", `当前持久参数 ${formatCount(Object.keys(state.persisted_params || {}).length)} 个；活跃试验 ${formatCount((state.active_trials || []).length)} 个；试验历史 ${formatCount((state.trial_history || []).length)} 条。`),
        miniRow("规则健康度 Top", healthRows.length ? healthRows.map((row) => `${row.rule_id} | 命中 ${formatCount(row.hit_count)} | 成功 ${formatCount(row.success_count)} | 失败 ${formatCount(row.failure_count)} | 回滚 ${formatCount(row.rollback_count)}`).join("\n") : "当前还没有规则健康记录。"),
      ].join("");
    }
    if (AT.dom.expAtAuditLog) {
      if (!audit.length) {
        AT.dom.expAtAuditLog.innerHTML = emptyState("当前还没有调参审计日志。");
      } else {
        AT.dom.expAtAuditLog.innerHTML = audit.slice(0, 40).map((item) => miniRow(`${formatTime(item.ts_ms)} | ${item.kind || "event"}`, JSON.stringify(item, null, 2))).join("");
      }
    }
    renderRecentLlmSuggestions();
    renderObservationZone();
  }

  async function refreshAutoTunerConfig({ silent = false } = {}) {
    const res = await apiGet("/api/experiment/auto_tuner/config");
    STATE.autoTunerConfig = res.data || null;
    const cfg = STATE.autoTunerConfig?.config || {};
    if (AT.dom.expAtEnabledChk) AT.dom.expAtEnabledChk.checked = Boolean(cfg.enabled);
    if (AT.dom.expAtShortChk) AT.dom.expAtShortChk.checked = Boolean(cfg.enable_short_term);
    if (AT.dom.expAtLongChk) AT.dom.expAtLongChk.checked = Boolean(cfg.enable_long_term);
    if (AT.dom.expAtLlmAssistChk) AT.dom.expAtLlmAssistChk.checked = Boolean(cfg.llm_assist_enabled);
    if (AT.dom.expAtAutoValidationChk) AT.dom.expAtAutoValidationChk.checked = Boolean(cfg.llm_auto_validation_enabled);
    if (AT.dom.expAtShortWindow) AT.dom.expAtShortWindow.value = String(cfg.short_window_ticks ?? 10);
    if (AT.dom.expAtLongWindow) AT.dom.expAtLongWindow.value = String(cfg.long_window_ticks ?? 40);
    if (AT.dom.expAtCooldown) AT.dom.expAtCooldown.value = String(cfg.decision_cooldown_ticks ?? 2);
    if (AT.dom.expAtMaxUpdates) AT.dom.expAtMaxUpdates.value = String(cfg.max_param_updates_per_tick ?? 4);
    if (AT.dom.expAtAutoValidationMinRuns) AT.dom.expAtAutoValidationMinRuns.value = String(cfg.llm_auto_validation_min_runs ?? 2);
    if (AT.dom.expAtAutoValidationMaxItems) AT.dom.expAtAutoValidationMaxItems.value = String(cfg.llm_auto_validation_max_observations_per_review ?? 4);
    if (AT.dom.expAtAutoValidationEveryRunChk) AT.dom.expAtAutoValidationEveryRunChk.checked = Boolean(cfg.llm_auto_validation_review_every_run);
    renderMetricTargets();
    if (!silent) feedback("expAtConfigFeedback", "已刷新调参器配置。", "ok");
  }

  async function saveAutoTunerConfig() {
    feedback("expAtConfigFeedback", "正在保存调参器配置…", "busy");
    try {
      const payload = {
        enabled: Boolean(AT.dom.expAtEnabledChk?.checked),
        enable_short_term: Boolean(AT.dom.expAtShortChk?.checked),
        enable_long_term: Boolean(AT.dom.expAtLongChk?.checked),
        llm_assist_enabled: Boolean(AT.dom.expAtLlmAssistChk?.checked),
        llm_auto_validation_enabled: Boolean(AT.dom.expAtAutoValidationChk?.checked),
        short_window_ticks: asNumber(AT.dom.expAtShortWindow?.value, 10),
        long_window_ticks: asNumber(AT.dom.expAtLongWindow?.value, 40),
        decision_cooldown_ticks: asNumber(AT.dom.expAtCooldown?.value, 2),
        max_param_updates_per_tick: asNumber(AT.dom.expAtMaxUpdates?.value, 4),
        llm_auto_validation_min_runs: asNumber(AT.dom.expAtAutoValidationMinRuns?.value, 2),
        llm_auto_validation_max_observations_per_review: asNumber(AT.dom.expAtAutoValidationMaxItems?.value, 4),
        llm_auto_validation_review_every_run: Boolean(AT.dom.expAtAutoValidationEveryRunChk?.checked),
        metric_targets: collectMetricTargets(),
      };
      const res = await apiPost("/api/experiment/auto_tuner/config/save", { config: payload });
      STATE.autoTunerConfig = res.data || null;
      renderMetricTargets();
      renderOverview();
      feedback("expAtConfigFeedback", "已保存调参器配置。", "ok");
    } catch (error) {
      feedback("expAtConfigFeedback", `保存失败：${error.message}`, "err");
    }
  }

  const refreshAutoTunerConfigBase = refreshAutoTunerConfig;
  refreshAutoTunerConfig = async function refreshAutoTunerConfigWithRunSync(options = {}) {
    await refreshAutoTunerConfigBase(options);
    const cfg = STATE.autoTunerConfig?.config || null;
    if (typeof window.syncRunAutoTunerDefaultsFromConfig === "function") {
      window.syncRunAutoTunerDefaultsFromConfig(cfg);
    }
  };

  const saveAutoTunerConfigBase = saveAutoTunerConfig;
  saveAutoTunerConfig = async function saveAutoTunerConfigWithRunSync() {
    await saveAutoTunerConfigBase();
    const cfg = STATE.autoTunerConfig?.config || null;
    if (typeof window.syncRunAutoTunerDefaultsFromConfig === "function") {
      window.syncRunAutoTunerDefaultsFromConfig(cfg);
    }
  };

  document.addEventListener("DOMContentLoaded", async () => {
    [
      "expAutoTunerOverviewCards",
      "expAtConfigMeta",
      "expAtEnabledChk",
      "expAtShortChk",
      "expAtLongChk",
      "expAtLlmAssistChk",
      "expAtShortWindow",
      "expAtLongWindow",
      "expAtCooldown",
      "expAtMaxUpdates",
      "expAtConfigSaveBtn",
      "expAtRefreshBtn",
      "expAtConfigFeedback",
      "expAtMetricTargets",
      "expAtParamSearch",
      "expAtParamCatalog",
      "expAtRulesSaveBtn",
      "expAtRuleSummary",
      "expAtRuleCatalog",
      "expAtCustomRulesEditor",
      "expAtStateSummary",
      "expAtAuditLog",
      "expAtRollbackList",
      "expAtLlmMeta",
      "expAtLlmEnabledChk",
      "expAtLlmAutoChk",
      "expAtLlmBaseUrl",
      "expAtLlmApiKey",
      "expAtLlmModel",
      "expAtLlmMaxPromptChars",
      "expAtLlmSaveBtn",
      "expAtLlmFeedback",
      "expAtLlmPrompt",
      "expAtLlmAnalyzeBtn",
      "expAtLlmJobsRefreshBtn",
      "expAtLlmAnalyzeFeedback",
      "expAtLlmJobs",
      "expAtLlmReport",
      "expAtLlmSuggestionList",
    ].forEach((id) => {
      AT.dom[id] = q(id);
    });

    [
      "expAtAutoValidationChk",
      "expAtAutoValidationMinRuns",
      "expAtAutoValidationMaxItems",
      "expAtAutoValidationEveryRunChk",
      "expAtObservationSummary",
      "expAtObservationZone",
      "expAtObservationHistory",
      "expAtObservationReview",
    ].forEach((id) => {
      AT.dom[id] = q(id);
    });

    renamePageChrome();

    if (AT.dom.expAtConfigSaveBtn) AT.dom.expAtConfigSaveBtn.addEventListener("click", saveAutoTunerConfig);
    if (AT.dom.expAtRefreshBtn) AT.dom.expAtRefreshBtn.addEventListener("click", () => refreshAutoTunerAll());
    if (AT.dom.expAtRulesSaveBtn) AT.dom.expAtRulesSaveBtn.addEventListener("click", saveRules);
    if (AT.dom.expAtLlmSaveBtn) AT.dom.expAtLlmSaveBtn.addEventListener("click", saveLlmConfig);
    if (AT.dom.expAtLlmAnalyzeBtn) AT.dom.expAtLlmAnalyzeBtn.addEventListener("click", startLlmAnalyze);
    if (AT.dom.expAtLlmJobsRefreshBtn) AT.dom.expAtLlmJobsRefreshBtn.addEventListener("click", () => refreshAutoTunerLlmJobs());
    if (AT.dom.expAtParamSearch) AT.dom.expAtParamSearch.addEventListener("input", () => renderParamCatalog());

    try {
      await refreshAutoTunerAll({ silent: true });
      feedback("expAtConfigFeedback", "自适应调参器模块已加载。", "ok");
    } catch (error) {
      feedback("expAtConfigFeedback", `加载失败：${error.message}`, "err");
    }
  });
})();
