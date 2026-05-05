# 1. 总评与架构定位

本次 run `exp_ap_behavioral_curriculum_small_v2_20260506_004306_7604` 可以评价为：**一个已经具备新版 AP 主链特征的可运行原型，而不是单纯规则流或普通 agent loop**。它在 300 个 source tick + 3 个 synthetic tick 的短程行为课程中，体现出较清晰的持续状态维护、感应生长、刺激级查存一体、行动合约闭环、时间感受和 NT/注意力/行动阈值调制观测链。最强证据来自：`induction_projection_mode_growth` 303/303 非零、`induction_growth_target_count` 299/303 非零、`residual_tail_memory_projection_handled/applied` 303/303 非零、`runtime_residual_package_*` 与 promotion 全程 0、expectation contracts 3 注册 3 成功 0 失败，以及 NT/attention/action threshold 字段全覆盖。

整体健康度：**B / 可继续优化的健康原型**。  
整体置信度：**中等偏高 0.72**。  
置信度没有更高，主要因为本提示未提供 `curriculum_metrics_summary.json` 中的 `top5_snapshots`、`top5_quality_summary`、`identity_maturation`、`identity_resolution_summary`、`segments`、`performance_hdb_diagnostic_summary` 等新版审阅首选字段；因此 Top5 叙事质量、identity 分段成熟曲线、严格 CFS/NT/action 因果链和性能慢尾归因只能依据 metrics digest 与 compact JSONL 摘要做间接判断。

**正面能力（Observed）**：  
- 新版 growth 主链确实在跑：`induction_projection_mode_growth` 303/303 非零，`induction_growth_target_count` 299/303 非零，`induction_growth_identity_hit_count` 137/303 非零，`induction_growth_identity_created_count` 208/303 非零，`induction_growth_identity_shared_cache_hit_count` 298/303 非零，`induction_growth_identity_shared_cache_stale_count` 0。  
- 刺激尾巴按 memory_id 新口径处理，而不是旧 residual package fallback：`residual_tail_memory_projection_handled` 与 `applied` 303/303 非零；`runtime_residual_package_applied`、`runtime_residual_promotion_attempted_count/promoted_count`、`timing_runtime_residual_promotion_ms` 全程 0。  
- 行动课程的合约闭环成功：manifest `expectation_contracts.registered_count=3`、`success_count=3`、`failure_count=0`；事件摘要显示隐式天气、显式天气执行成功，弱天气不执行也成功。  
- 注意力、NT、行动阈值调制都有观测字段：`nt_DA/ADR/OXY/SER/END/COR/NOV/FOC` 303/303 非零；`attention_energy_budget`、`attention_net_delta_energy`、`action_threshold_*_scale_mean` 均存在且 303/303 覆盖。

**主要短板（Observed/Inferred）**：  
- 注意力预算与状态池可能偏热。AutoTuner LLM 分析报告指出 `attention_net_delta_energy` 多次基线在 11.5–12.5，高于目标上沿 11.0；manifest long-term persisted params 中 `attention.attention_filter_gain_floor=0.58`、`observatory.attention_memory_energy_ratio=0.58`，而 config 原始文件为 0.52/0.5，说明运行期曾被调高。  
- identity 创建仍很多。digest 显示 `induction_growth_identity_created_count` 208/303 非零，同时 `hit_count` 137/303 非零、shared cache 298/303 非零。短程冷启动下这不一定异常，但缺少 `identity_resolution_summary`，无法判断 created 是否主要由新语料、exact lookup skipped、deduped 或缓存策略解释。  
- cache priority neutralization 的实际能量消费为 0：`cache_priority_consumed_er=0`、`cache_priority_consumed_ev=0` 全程 0。结合 `timing_cache_neutralization_ms` 303/303 非零与多种 fast path 非零，这可能是“本 run 没有形成可消费缺口”或统计口径没有纳入能量消费，而不能直接判定中和失败。

**最关键不确定性（Unknown）**：  
- 缺少 Top5 快照与 root 统计，无法直接评价“Top5 是否从原子噪声转向结构对象”、是否存在 root 级重复、字符片段化或运行态分辨率变体。  
- 缺少 expectation contract windows 的 `causal_chain` 数组，无法严格证明 CFS/NT/drive/threshold/attempted/scheduled/executed 的逐 tick 因果链，只能证明窗口合约结果正确。  
- 缺少 performance diagnostic summary 的 slowest ticks 与 correlation 表，性能慢尾只能从首尾 compact 行和 timing 字段粗略定位。

---

# 2. 与其它架构的对比评估

| 对比对象 | AP 相似点 | AP 差异点 | 这次 run 体现出的优势或特点 | 代价/局限 | 证据或缺证 |
|---|---|---|---|---|---|
| 纯规则系统 | 有 IESM 先天规则、expectation contract、行动阈值等规则化成分 | 主链不是“if-then 输出”，而是状态池能量、HDB 查存一体、感应生长、注意力预算共同演化 | 天气合约不是单纯全局硬规则：隐式/显式天气在窗口内执行，弱天气不执行；合约结果 3/3 成功 | 规则仍较强地参与冷启动行动，需防止训练集契约过拟合 | manifest `expectation_contracts success_count=3/failure_count=0`；事件摘要 source tick 120/132 执行、144 不执行；action config/IESM rules enabled |
| 纯向量/RAG 检索记忆 | 都有记忆召回和相似匹配 | AP 使用 SA/ST、最大共同切割、HDB 局部数据库和 ER/EV 能量传播，不是 embedding top-k 文档拼接 | `stimulus_match_v2_score_mean` 首尾约 0.90，`stimulus_object_projection_dominates_unhandled_residual=1`，说明对象投影压过未处理残差 | 没有 Top5 内容快照，不能证明语义召回质量优于 RAG；短程中文课程无法证明大规模知识检索能力 | 首行 tick0 `stimulus_match_v2_score_mean=0.909924`、尾行 tick302 `0.900503`；`stimulus_unhandled_residual_total=0` |
| 普通工具调用 agent loop | 都可触发工具/行动并接收反馈 | AP 将 action node、drive、threshold、reward/punish、NT、CFS 纳入运行态，而非 LLM 计划后直接调用工具 | 天气 action 在合适窗口执行，在弱提及时保持不执行，符合“阈值化行动”而非每次想到就调用 | 缺少 attempted/scheduled/drive margin 的窗口 causal_chain，无法严格证明每个内部步骤 | 合约事件：`weather_implicit_success` settled at source 122；`weather_explicit_success` settled at 134；`weather_weak_no_execute` settled at 146 with metric current 0 |
| 传统强化学习/行为策略 | 有 reward/punish、drive gain/penalty、行动阈值调制 | 学习信号进入 SA/ST/HDB 共现和能量系统，不是端到端 policy gradient 或 Q 值表 | 教师奖励/惩罚字段、action threshold modulation 和 learning delta 类字段可观测；`action_learning_punish_drive_penalty_total` 在 tick0 compact 行非零 0.146439 | 本 run 不是 RL 对照实验，不能证明策略泛化或长期收益优化 | `action_threshold_rwd_pun_scale_mean` 303/303；`action_threshold_nt_scale_mean` 303/303；dataset teacher rwd/pun labels；缺少分段学习曲线 |
| 预测加工/主动推断类架构 | 都强调预测—现实差异、认知压、注意力调制 | AP 工程化为 ER/EV、CP、CFS、HDB induction growth、state pool 能量，而非统一自由能优化数学框架 | NT/CFS/attention budget/action threshold 均有运行指标；`attention_net_delta_energy` 与 CP/NT 可用于审计预测压力 | 缺少逐对象 CP live_total、CFS causal_chain、Top5 root，不能严格证明预测误差最小化过程 | field audit：NT 全通道 303/303，attention budget 303/303，action threshold 303/303；CFS live_total 摘要不足 |

---

# 3. 创新点、特点与可应用场景

## 3.1 理论设计层面的创新/特点

1. **ER/EV 双能量与认知压**  
   - 理论潜力：区分现实证据 ER 与预测/想象 EV，并通过 CP 驱动注意、CFS、学习。  
   - 本次证据：NT、attention、action threshold 字段覆盖充分；但 ER/EV Top5 和 CP live_total 未在摘要中展开，不能细评对象级语义。

2. **感应生长 A+B**  
   - 理论潜力：从 source A 的局部 HDB 残差 B 直接投影为完整结构 A+B，替代旧 residual+CS 半成品路径。  
   - 本次已观察：`induction_projection_mode_growth` 303/303 非零；`induction_growth_target_count` 299/303 非零；`induction_growth_target_apply_ref_fast_merge_enabled` 303/303 非零。

3. **完整身份汇聚**  
   - 理论潜力：完整内容解析 identity，不把 owner DB/growth_source 当身份。  
   - 本次部分支持：`induction_growth_identity_shared_cache_hit_count` 298/303 非零、`shared_cache_stale_count=0`、`lookup_disabled_count=0`，说明 identity cache 路径活跃且无 stale/disabled 症状。  
   - 缺证：无 `identity_resolution_summary`，无法判断 exact hit、deduped、create_exact_lookup_skipped 的结构性原因。

4. **运行态分辨率下降**  
   - 理论潜力：低能组件只影响 StatePool 运行态解释，不创建退化 HDB id。  
   - 本次缺证：提示中未提供 `pool_runtime_resolution_degraded_item_count`、`pool_runtime_resolution_active_component_count`、`maintenance_runtime_resolution_*` 的具体摘要。不能判定是否触发。

5. **SA/ST 结构层级与字符 SA 字符串模式**  
   - 理论潜力：字符 SA 承能量，字符串关系保顺序敏感，支持最大共同切割。  
   - 配置支持：`enable_goal_b_char_sa_string_mode=true`；dataset 为纯中文文本。  
   - 本次效果缺证：未提供 Top5 内容与结构/原子 SA 比例摘要。

6. **内源刺激汇聚**  
   - 理论潜力：CAM/注意力图景重新采样为内源刺激，参与下一轮查存一体。  
   - 本次间接支持：空 tick 250 个，尾部 tick302 仍有 `stimulus_object_projection_total=17.465704`、`stimulus_memory_tail_absorbed_total=3.255046`、`time_sensor_bucket_energy_sum=2.445198`，说明空段并非完全无状态。

7. **CFS/NT、自适应注意/行动/奖惩链路**  
   - 理论潜力：感受与情绪不是标签，而影响注意力、行动阈值、学习。  
   - 本次支持：`nt_*` 全通道 303/303 非零；`action_threshold_nt_scale_mean`、`action_threshold_rwd_pun_scale_mean`、`action_threshold_fatigue_scale_mean` 全覆盖。  
   - 缺证：没有 contract window `causal_chain`，不能做严格因果证明。

8. **缓存中和/认知压性能路径**  
   - 理论潜力：新刺激先中和状态池高 CP 对象，降低不必要查存成本。  
   - 本次部分支持：`timing_cache_neutralization_ms` 303/303 非零，fast path 如 `cache_priority_cut_single_group_fast_path_hit_count` 219/303、`ordered_subsequence_fast_path_hit_count` 174/303。  
   - 弱点：`cache_priority_consumed_er/ev` 全程 0，需判断是无可中和缺口还是采集口径未覆盖。

## 3.2 本次 run 已观察到的实现效果

- **growth 主链落地明确**：`induction_projection_mode_growth=1` 全程；CS 关闭且 `timing_cognitive_stitching_ms=0`，符合新版默认口径。  
- **尾巴 memory_id 接管明确**：`residual_tail_memory_projection_handled/applied` 全程非零，旧 residual package/promotion 全程 0。  
- **行动合约闭环明确**：3 个 expectation contract 全部成功，且 source tick 与 synthetic tick 映射已给出。  
- **性能护栏部分生效**：stimulus cut 多种 fast path、cache zero-copy、target apply fast ref merge 均有命中；insert log suppressed 非零代表日志抑制，不是 target 跳过。

## 3.3 可应用场景

| 场景 | 适配原因 | 当前不足 |
|---|---|---|
| 可解释长期记忆原型 | HDB、SA/ST、growth identity、residual tail memory projection 均有审计字段 | 缺少大规模语料与跨轮 accumulated summary，不能证明百万级扩展 |
| 带自我状态观测的 agent | NT、CFS、attention budget、action threshold 指标覆盖 | 缺少 CFS live_total 到 action 的逐 tick causal_chain |
| 教师奖惩塑形实验 | dataset 含 teacher rwd/pun；action threshold rwd/pun scale 全覆盖；合约成功 | 需要更多反事实样本检验非硬编码泛化 |
| 认知过程可视化 | metrics key 多、growth/刺激/尾巴/行动链可观测 | 本提示没有 Top5 snapshots，前端叙事质量无法审阅 |
| 局部情绪/注意调制实验 | NT 全通道非零，attention budget 和 net delta 有字段 | AutoTuner 可能把注意力抬热，需要对照固定参数 |
| 低层符号-能量混合研究 | char SA string mode 开启，刺激级共同切割 fast path 活跃 | 缺少结构内容样例与 root overlap 统计 |

---

# 4. 拟人度评估

## 4.1 表现出较好拟人效果的现象

1. **保守行动阈值与语境选择**  
   - Observed：天气合约中隐式天气 `source_dataset_tick_index=120` 注册，`settled_source_tick_cursor=122` 成功执行；显式天气 `132→134` 成功执行；弱天气 `144→146` 要求不执行，实际 `action_executed_weather_stub_source_visible current=0 target=0` 成功。  
   - Inferred：这比“只要出现天气词就调用工具”更接近拟人化的语境阈值判断。  
   - 边界：没有窗口内 drive/threshold/margin causal_chain，不能证明内部所有步骤都是学习后产生。

2. **思考有代价/注意力预算化**  
   - Observed：`attention_energy_budget_enabled=1`、`attention_energy_budget` 303/303 非零；`attention_energy_filter_applied` 303/303 非零；`attention_net_delta_energy` 302/303 非零。  
   - Inferred：注意力不是纯展示 Top-N，而参与能量预算调制。  
   - 风险：AutoTuner 报告指出净增能量偏高，可能由拟人化“活跃注意”滑向过热。

3. **内源残响与空 tick 状态延续**  
   - Observed：dataset 有 250 个 empty ticks；尾部 tick302 仍有 `stimulus_object_projection_total=17.465704`、`residual_tail_memory_projection_total_energy` 非零覆盖 303/303、`time_sensor_bucket_energy_sum=2.445198`。  
   - Inferred：系统不是每个空 tick 清空上下文，而存在残响、时间感受和运行态记忆投影。

4. **情绪/递质调制可观测**  
   - Observed：`nt_DA/ADR/OXY/SER/END/COR/NOV/FOC` 均 303/303 非零；`action_threshold_nt_scale_mean` 303/303。  
   - Inferred：本次运行具备“内部状态影响阈值”的工程形态。  
   - 边界：仍缺逐事件解释，例如某次压力上升是否导致天气 action 延迟。

5. **教师奖惩并非完全外部标签**  
   - Observed：dataset teacher rwd/pun 通过 labels 提供；metrics 有 `action_threshold_rwd_pun_scale_mean` 303/303、`action_learning_punish_drive_penalty_total` 在 tick0 compact 行为 0.146439。  
   - Inferred：奖惩至少进入行动阈值/学习统计链路。  
   - 边界：缺少 reward_signal/punish_signal 的对象绑定 Top 或 HDB 结构共现证据。

## 4.2 仍显机械、僵硬或缺证的部分

1. **Top5 叙事质量缺证**  
   - Unknown：本提示没有 `top5_snapshots`、`top5_root_summary` 或 Top 项文本；不能判断是否存在单字符 EV Top、结构化 Top 或重复自指。  
   - Recommendation：下次必须提供 source tick 0/60/120/180/240/300 左右 Top5 与 root summary。

2. **CS 关闭不是问题，但叙事拼接能力本 run 不能评价**  
   - Observed：`enable_cognitive_stitching=false`，CS config `enabled=false`，`timing_cognitive_stitching_ms=0`。  
   - Inferred：符合新版 growth 默认口径。  
   - Unknown：不能用本次 run 评价 CS 的拟人化叙事拼接效果。

3. **注意力可能过热**  
   - Observed：AutoTuner LLM 报告称 `attention_net_delta_energy` 多次 11.5–12.5，高于目标 11.0；long-term persisted params 中 `attention.attention_filter_gain_floor=0.58`。  
   - Inferred：拟人化的“持续想”可能过强，造成状态池高位或性能成本。  
   - 边界：需要原始分段 trend 验证。

4. **identity 成熟只能间接判断**  
   - Observed：`identity_created_count` 208/303 非零，`identity_hit_count` 137/303 非零，shared cache 298/303 非零。  
   - Unknown：无 exact hit/deduped/create_exact_lookup_skipped 汇总，不能断言身份汇聚成熟或失败。

---

# 5. 本次最可靠结论与最大风险

1. **Observed：新版 growth 主链已启用并全程运行。**  
   证据：`induction_projection_mode_growth` 303/303 非零；CS 关闭，`timing_cognitive_stitching_ms=0`。

2. **Observed：旧 residual package/promotion fallback 没有介入主链。**  
   证据：`runtime_residual_package_applied=0`、`runtime_residual_promotion_attempted_count=0`、`timing_runtime_residual_promotion_ms=0` 全程；`residual_tail_memory_projection_handled/applied` 303/303 非零。

3. **Observed：行动合约结果正确。**  
   证据：manifest expectation contracts 3/3 success；事件摘要 tick 120/132 执行，tick 144 不执行。

4. **Observed：identity cache 路径活跃且没有 stale/disabled 症状。**  
   证据：`induction_growth_identity_shared_cache_hit_count` 298/303 非零；`shared_cache_stale_count=0`；`lookup_disabled_count=0`。

5. **Inferred：短程 run 中已经出现局部记忆/预测/行动闭环雏形。**  
   证据组合：growth target、residual tail memory projection、time sensor、action contracts、NT/action threshold 全覆盖。

6. **Observed/Inferred：注意力和感应 fanout 可能偏热。**  
   证据：AutoTuner 报告中 `attention_net_delta_energy` 11.5–12.5；`attention_filter_gain_floor` 被长期调到 0.58；`induction_growth_target_count` 299/303 非零且 identity created 很多。

7. **Unknown：Top5 可读性与结构化程度未能评价。**  
   缺少 `top5_snapshots/top5_root_summary`。

8. **Unknown：CFS/NT 到 action 的严格因果链未能证明。**  
   有 NT/action threshold 字段，但缺 `expectation_contract_windows.causal_chain`。

9. **Recommendation：下一轮最该补的是 curriculum summary，而不是先改主逻辑。**

---

# 6. 证据清单与字段覆盖审计

## 6.1 运行与数据集

- run_id：`exp_ap_behavioral_curriculum_small_v2_20260506_004306_7604`  
- status：`completed`  
- dataset：`ap_behavioral_curriculum_small_v2`  
- dataset sha256：`6348920c3615202000c98142f4bc85ce100916fa32972ff6a8138adcef756776`  
- manifest tick：`total_ticks=300`，`effective_text_ticks=50`，`empty_ticks=250`，`labeled_ticks=50`  
- metrics rows：303，tick range 0–302；source rows 300，synthetic rows 3  
- manifest：`tick_done=300`，`source_tick_done=300`，`synthetic_tick_done=3`，`executed_tick_done_total=303`

## 6.2 关键配置

- 新版主链背景：  
  - `observatory_config.enable_cognitive_stitching=false`  
  - `cognitive_stitching_config.enabled=false`  
  - `observatory_config.enable_structure_level_retrieval_storage=false`  
  - `observatory_config.enable_goal_b_char_sa_string_mode=true`  
- Energy Balance Controller：`energy_balance_config.enabled=false`，因此不要把 legacy `energy_balance_*` 0 或折叠当主故障。  
- Action：`action_config.enabled=true`，`threshold_scale_by_rwd_pun_enabled=true`。  
- TimeSensor：`enabled=true`，`source_mode=runtime_memory_projection`，但 config 中 `time_basis=wallclock`，manifest `options.time_sensor_time_basis=null`。dataset 声称 `time_basis=tick`、`tick_dt_ms=3000`，这里存在潜在口径差异，应后续核对。  
- AutoTuner：manifest `auto_tune_enabled=true`，long_term `applied=true`，`applied_count=3`，可能影响固定参数解释。

## 6.3 metrics 覆盖

- NT：`nt_DA/ADR/OXY/SER/END/COR/NOV/FOC` 全部存在且 303/303 非零。  
- 注意力预算：`attention_energy_budget*`、`attention_net_delta_energy` 存在，`attention_net_delta_energy` 302/303 非零。  
- 行动阈值：`action_threshold_scale_mean`、`action_threshold_nt_scale_mean`、`action_threshold_rwd_pun_scale_mean`、`action_threshold_fatigue_scale_mean` 全覆盖。  
- growth：`induction_growth_*` 主字段存在，含 identity/cache/target apply/log suppression。  
- residual tail：新口径字段存在；legacy runtime residual package/promotion 全程 0。  
- stimulus performance：cut fast path、normalize、cache、timing 字段存在。  
- cache neutralization：性能字段存在，但 consumed ER/EV 全程 0。  
- 缺少或摘要不足：`top5_snapshots`、`top5_root_summary`、`identity_resolution_summary`、`identity_maturation`、`expectation_contract_windows.causal_chain`、`performance_hdb_diagnostic_summary` 未提供；不能声称系统没有这些能力，只能说本审阅材料不足。

---

# 7. 理论对齐矩阵

| 机制 | 理论预期 | 证据字段与 tick 区间 | 观测结果 | 判断 | 置信度 |
|---|---|---|---|---|---|
| ER/EV 权重语义 | ER 表现实证，EV 表预测；EV 不应误写 ER | `residual_tail_memory_projection_er/ev` 292/264 非零；`induction_growth_total_delta_er/ev` 摘要未展开 | ER/EV 字段存在，但对象级分配不足 | 部分符合/缺证 | 中 |
| 虚能量乘法磨损 | EV 对权重磨损不应硬扣 0 | 缺少权重更新 delta、ev wear 统计 | 无法判断 | 缺证 | 低 |
| 感应生长 A+B | growth 模式投影完整 A+B | `induction_projection_mode_growth` 303/303；`induction_growth_target_count` 299/303 | 主链持续运行 | 符合 | 高 |
| 完整身份解析与汇聚 | 完整内容 identity，cache 减少重复解析 | `identity_hit_count` 137/303；`created_count` 208/303；`shared_cache_hit_count` 298/303；`stale=0`；`lookup_disabled=0` | cache 活跃，无 stale；created 仍多 | 部分符合 | 中 |
| 运行态分辨率下降 | 只刷新 runtime_resolution，不建退化 HDB id | 未提供 `pool_runtime_resolution_*` | 无法判断是否触发 | 缺证 | 低 |
| 刺激尾巴按 memory_id 合并 | residual tail 由 episodic memory_id 接管 | `residual_tail_memory_projection_handled/applied` 303/303；`stimulus_memory_tail_absorbed_total` tick0=10.956591，tick302=3.255046 | 新尾巴路径持续生效 | 符合 | 高 |
| 旧残余包 fallback 关闭 | 默认不产生 rt_residual package/promotion | `runtime_residual_package_*` 与 `runtime_residual_promotion_*` 全 0 | 符合新版默认 | 符合 | 高 |
| 内源刺激合流 | 空 tick 仍可有残响、时间感受、记忆投影 | empty ticks=250；tick302 仍有 object projection 与 time sensor energy | 间接支持内源/残响 | 部分符合 | 中 |
| CFS/NT | CFS/NT 调制注意和行动 | NT 全通道 303/303；action threshold scale 全覆盖；CFS live 摘要不足 | NT/阈值链路存在，CFS 因果不足 | 部分符合 | 中 |
| 注意力预算 | CAM 预算有限、滤波应用 | `attention_energy_budget` 303/303；`attention_energy_filter_applied` 303/303；`attention_net_delta_energy` 302/303 | 预算机制存在；可能过热 | 部分符合 | 中高 |
| 行动/教师奖惩 | action node 可学习，奖惩影响阈值/drive | contracts 3/3；`action_threshold_rwd_pun_scale_mean` 303/303；dataset teacher labels | 合约成功，调制存在 | 部分符合 | 中高 |
| HDB 增长 | 短程应有结构/identity 增长和 cache 成熟 | growth created/hit/cache 字段 | 有创建与命中，但无 hdb_growth 分段 | 部分符合 | 中 |

---

# 8. 数据异常与解释

## 8.1 CS 全 0 / `timing_cognitive_stitching_ms=0`

- Observed：config `enable_cognitive_stitching=false`，CS `enabled=false`；compact 首尾 `timing_cognitive_stitching_ms=0.0`。  
- Inferred：这是新版 growth 默认口径，不是故障。  
- Unknown：本 run 不能评价 CS 对照路径质量。

## 8.2 legacy residual package/promotion 全 0

- Observed：`runtime_residual_package_applied`、`runtime_residual_promotion_attempted_count/promoted_count`、`timing_runtime_residual_promotion_ms` 303/303 均 0。  
- Inferred：符合“尾巴按 memory_id 接管”的新口径。  
- Recommendation：不要因为这些字段为 0 开启旧 fallback，除非做 A/B 对照。

## 8.3 `cache_priority_consumed_er/ev=0`

- Observed：两者 303/303 均 0；但 `timing_cache_neutralization_ms` 303/303 非零，fast path 字段活跃。  
- Inferred：可能是本 run 中没有满足正向中和消费条件，也可能是摘要只统计实际消费而不统计候选评分。  
- Unknown：需要逐 tick 查看 `pool_cp_top5`、中和候选数、缺口能量、`neutralization_min_effect_threshold` 命中情况。  
- Recommendation：补充 `cache_priority_candidate_count`、`neutralization_skipped_reason_count`、`cp_reduction_after_neutralization`。

## 8.4 attention budget base 非零覆盖只有 107/303

- Observed：field audit `attention_energy_budget_base` count 303，nonzero 107；但 `attention_energy_budget` 303/303 非零。  
- Inferred：可能是 AutoTuner/NT 调制后 runtime budget 与 base 展示口径不同，或 base 只在 source/特定阶段记录。  
- Unknown：需要分 source/synthetic、text/empty tick 查看。  
- Recommendation：报告中同时显示 base、modulated、final budget 和 tick_source。

## 8.5 identity created 多

- Observed：`induction_growth_identity_created_count` 208/303 非零，hit 137/303 非零。  
- Inferred：短程冷启动 + 50 条真实文本 + 内源生长可能导致大量新建，未必异常。  
- Unknown：缺 `identity_resolution_summary.create_exact_lookup_skipped/deduped/exact_hit/local/shared cache`。  
- Recommendation：不要直接判 identity 失败，先补 identity summary。

## 8.6 性能慢尾

- Observed：compact tick0 `timing_total_logic_ms=339`、`timing_stimulus_level_ms=99`；tick302 `timing_total_logic_ms=721`、`timing_stimulus_level_ms=371`、`timing_cache_neutralization_ms=29`。  
- Inferred：尾部逻辑耗时上升可能与 HDB/状态池增长、stimulus candidates 增多有关；tick302 `stimulus_shadow_raw_residual_candidate_count=145` 高于 tick0 的 49。  
- Unknown：没有 slowest ticks/correlation summary，不能做因果断言。  
- Recommendation：下一轮提供 `performance_hdb_diagnostic_summary.slowest_ticks_by_total_logic_ms` 与 correlation。

---

# 9. 行动与教师奖惩学习链路

## 9.1 合约结果

- Observed：manifest `registered_count=3`、`success_count=3`、`failure_count=0`、`synthetic_tick_count=3`。  
- Observed：事件摘要 6 个事件，包括 3 registered + 3 settled。

### 窗口 1：隐式天气成功

- Registered：`weather_implicit_success_00020`，`source_tick_cursor=121`，`source_dataset_tick_index=120`，deadline 123。  
- Settled：`settled_source_tick_cursor=122`，matched detail：`action_executed_weather_stub_source_visible current=1 target=1`。  
- 判断：Observed 合约内执行正确。  
- 边界：未提供 drive/threshold/margin causal_chain，因此只能说窗口结果符合，不说完整因果已证明。

### 窗口 2：显式天气成功

- Registered：`weather_explicit_success_00022`，`source_tick_cursor=133`，`source_dataset_tick_index=132`，deadline 135。  
- Settled：`settled_source_tick_cursor=134`，matched `action_executed_weather_stub_source_visible current=1 target=1`。  
- 判断：Observed 显式触发执行正确。

### 窗口 3：弱天气不执行成功

- Registered：`weather_weak_no_execute_00024`，`source_tick_cursor=145`，`source_dataset_tick_index=144`，deadline 146。  
- Settled：`settled_source_tick_cursor=146`，matched `metric_eq action_executed_weather_stub_source_visible current=0 target=0`。  
- 判断：Observed 系统没有把弱天气提及误执行为工具调用，这是正面信号。

## 9.2 action node / threshold / reward-punish

- Observed：action module enabled；compact 尾部 `action_node_count=4`，`action_local_targeted_node_count_attention_focus=4`，`action_threshold_rwd_pun_enabled_node_count=4`。  
- Observed：尾部 `action_threshold_nt_scale_mean=0.978508`、`action_threshold_rwd_pun_scale_mean=1.0`、`action_threshold_fatigue_scale_mean=1.104625`、`action_threshold_scale_mean=1.080885`。  
- Observed：tick0 compact 行 `action_learning_punish_drive_penalty_total=0.146439`。  
- Inferred：行动节点阈值受到 NT、疲劳、奖惩配置影响；但具体到天气窗口内的 action 是否由学习塑形导致，需要 contract windows causal_chain。

## 9.3 对天气工具 action 的审阅判断

- Observed：weather_stub 不是高频乱执行。尾部 compact 行 `action_executed_weather_stub=0`，但合约窗口中该执行时执行、不该执行时不执行。  
- Inferred：这更符合“行动阈值化、语境化”的设计，而不是执行频次低的缺陷。  
- Unknown：缺少 `action_attempted/scheduled/weather_stub drive_margin` 的窗口明细。

---

# 10. ER/EV、注意力预算与叙事化观察

## 10.1 刺激级对象投影

- Observed tick0：  
  - `stimulus_object_projection_total=39.143409`  
  - `stimulus_unhandled_residual_total=0.0`  
  - `stimulus_object_projection_to_unhandled_residual_ratio=39.143409`  
  - `stimulus_object_projection_dominates_unhandled_residual=1`  
  - `stimulus_memory_tail_absorbed_total=10.956591`  
- Observed tick302：  
  - `stimulus_object_projection_total=17.465704`  
  - `stimulus_unhandled_residual_total=0.0`  
  - `stimulus_object_projection_to_unhandled_residual_ratio=17.465704`  
  - `stimulus_object_projection_dominates_unhandled_residual=1`  
  - `stimulus_memory_tail_absorbed_total=3.255046`  
- 判断：Observed 新版验收口径下，对象投影压过未处理残差；raw residual tail 被 memory tail 吸收，不应误判为污染。

## 10.2 感应生长与 target apply

- Observed：  
  - `induction_growth_target_apply_ref_fast_merge_enabled` 303/303 非零。  
  - `induction_growth_target_apply_fast_ref_hit_merge_count` 284/303 非零。  
  - `induction_growth_target_apply_insert_log_enabled` 0/303 非零。  
  - `induction_growth_target_apply_insert_log_suppressed_count` 298/303 非零。  
- 判断：fast ref merge 是 StatePool 性能路径；insert log suppression 是跳过 brief/detail 文件日志，不代表 targets 被跳过。  
- Recommendation：性能分析应看 `timing_induction_target_apply_ms` 与 fast merge hit，而不是把 log suppressed 视为缺失。

## 10.3 注意力预算

- Observed：`attention_cam_item_cap=16`，`attention_energy_budget` 全程非零，tail tick302 final budget 11.059201。  
- Inferred：注意力预算在运行中被调制，且可能高于 base 8.0；AutoTuner 报告也提示注意力净增偏高。  
- Unknown：缺少 per-segment `attention_net_delta_energy` trend。

## 10.4 Top5 叙事化观察

- Unknown：本提示未包含 `top5_snapshots`、`top5_quality_summary`、`top5_root_summary`。  
- 因此不能评价至少 3 个 source tick 快照的 ER/EV/CP top1、root overlap、原子 SA/结构比例，也不能判断 Top 重复是 root 级重复、运行态分辨率变体还是语义自循环。  
- Recommendation：下一轮必须导出 curriculum summary；至少覆盖 source tick 0、60、120、180、240、300 附近。

## 10.5 identity maturation

- Observed：identity created/hit/cache 字段存在，shared cache stale 0。  
- Unknown：无 `identity_maturation` 分段曲线和 `identity_resolution_summary`，无法评价 hit/(hit+created)、created/target、shared/local cache/target 随课程推进是否成熟。

---

# 11. 参数优先建议

以下建议优先为低风险配置调参，不改变主逻辑。

| 参数/位置 | 建议方向 | 预期改善 | 副作用 | 验证指标 |
|---|---|---|---|---|
| `attention.attention_filter_gain_floor` / attention config 或 AutoTuner persisted | 若当前运行有效值仍约 0.58，建议回落到 0.54–0.56 | 降低低质量对象常态放大，缓解 attention overheat | CAM 可能变短，弱线索召回下降 | `attention_net_delta_energy` 均值降至目标内；`cam_item_count` 不低于 min；Top5 可读性不下降 |
| `observatory.attention_memory_energy_ratio` / observatory persisted | 从 0.58 回到 0.50–0.54 | 减少注意抽取对状态池扰动 | 内源残响可能变弱 | `attention_net_delta_energy`、`internal_*`、contract success |
| `state_pool.energy_injection_fatigue_same_side_knee_ev` | 若 AutoTuner 已调至 5.8，可小步 5.8→6.0/6.1；不建议大跳 | 放松 EV 注入过度节流，改善 growth target 落地 | EV Top 可能更热 | `pool_energy_injection_throttle_ratio_total`、`pool_ev/er`、Top5 EV 原子比例 |
| `state_pool.energy_injection_repeat_fatigue_step` | 0.19→0.18 小幅放松 | 减少重复赋能被压扁 | 自循环风险略增 | `induction_growth_target_apply_fast_ref_hit_merge_count`、Top root duplicate |
| `hdb.stimulus_early_stop_min_progress_ratio` | 当前 config 0.10；若尾部慢尾明显，可试 0.08–0.10 对照，不建议直接更高 | 平衡尾巴处理与性能 | 过高会漏弱结构，过低会增耗时 | `stimulus_round_count`、`stimulus_object_projection_dominates_unhandled_residual`、`timing_stimulus_level_ms` |
| `growth_projection_overprediction_gate_enabled` | 默认不启用；仅做 A/B 对照开启 | 检验发散是否由 overprediction 导致 | 可能压制拟人化联想 | `induction_growth_target_count`、Top duplicate/root overlap、contract success |
| `time_sensor.time_basis` | 与 dataset `time_basis=tick` 对齐，建议下一轮显式设 `tick` | 避免 wallclock 与 dataset tick_dt 口径不一致 | 与历史 wallclock run 不可直接比 | `time_sensor_bucket_energy_sum`、delayed task register/execute、recall action |

---

# 12. 工程与观测建议

1. **新增/导出 curriculum summary 作为默认审阅产物**  
   - 字段：`top5_snapshots`、`top5_root_summary`、`identity_maturation`、`identity_resolution_summary`、`expectation_contract_windows.causal_chain`、`performance_hdb_diagnostic_summary`。  
   - 风险：仅增加报告体积，无主逻辑风险。  
   - 回归：确认 300 tick run 报告大小可控。

2. **cache neutralization 增加 skip reason**  
   - 现状：`cache_priority_consumed_er/ev=0`，但 timing 和 fast path 活跃。  
   - 建议字段：`cache_priority_candidate_count`、`cache_priority_effective_candidate_count`、`cache_priority_skipped_low_cp_count`、`cache_priority_skipped_min_effect_count`、`cp_abs_reduced_total`。  
   - 目的：区分真实无中和、阈值过高、显示口径缺口。

3. **identity created 细分原因**  
   - 字段：`create_exact_lookup_skipped_count`、`deduped_count`、`exact_lookup_attempt_count`、`exact_hit_count`、`created_due_new_content_count`。  
   - 目的：避免用 created_count 单项误判 identity 不成熟。

4. **target apply 性能慢尾独立展示**  
   - 已有字段：`timing_induction_target_apply_ms`、`induction_growth_target_apply_fast_ref_hit_merge_count`、`insert_log_suppressed_count`。  
   - 建议在 performance summary 中显示 p50/p95/max 与 slowest tick。  
   - 注意：insert-log suppression 只代表日志跳过，不代表 targets 跳过。

5. **TimeSensor 口径显示**  
   - 显示 manifest dataset `time_basis`、runtime config `time_basis`、实际使用 basis。  
   - 本次 config wallclock 与 dataset tick 口径有潜在差异，应可视化避免误读。

6. **性能慢尾加入相关但非因果提示**  
   - 字段：slowest ticks by `timing_total_logic_ms`，相关指标如 `stimulus_shadow_raw_residual_candidate_count`、`stimulus_best_match_candidate_count`、`induction_growth_target_count`。  
   - 报告中标注相关性仅作定位线索。

---

# 13. 下一轮对照实验

1. **固定参数 vs AutoTuner 开启对照**  
   - 开关：`auto_tune_enabled=false` 对照当前 true。  
   - 数据集：同 `ap_behavioral_curriculum_small_v2`。  
   - 预期：若 attention overheat 主要由 AutoTuner 引起，`attention_net_delta_energy` 应下降或更稳定。  
   - 判定：contract success 不下降；`timing_total_logic_ms`、`cam_item_count`、Top5 质量。

2. **attention gain floor 回落对照**  
   - 改动：`attention_filter_gain_floor` 0.58→0.56。  
   - 预期：注意力净增能量降低，Top 重复减少。  
   - 风险：弱天气/隐式天气 action 可能漏触发。  
   - 判定：3 个 weather contract 仍 3/3；Top5 root diversity 提升。

3. **time_basis tick 对照**  
   - 改动：TimeSensor `time_basis=tick`，`tick_interval_sec=3.0` 或明确 dataset tick basis。  
   - 预期：time bucket 与 5 empty ticks 的节奏更稳定。  
   - 判定：`time_sensor_bucket_energy_sum`、delayed task registered/executed、recall action。

4. **growth overprediction gate A/B**  
   - 改动：`growth_projection_overprediction_gate_enabled=true` 仅对照。  
   - 预期：`induction_growth_target_count`、identity created、Top duplicate 降低。  
   - 风险：联想/拟人残响变弱。  
   - 判定：object projection dominance、contract success、Top5 可读性。

5. **CS residual 回滚对照，不作为主线**  
   - 改动：`enable_cognitive_stitching=true` + residual mode 仅 A/B。  
   - 预期：若叙事 Top 改善但性能上升，可定位 CS 的增益/成本。  
   - 判定：`cs_action_count`、`timing_cognitive_stitching_ms`、Top5 narrative overlap。

---

# 14. 不应过度断言的部分

1. **不能断言已具备百万语料级逻辑能力**  
   - 本 run 只有 300 source ticks、50 text ticks、短程课程。

2. **不能断言 Top5 已结构化或未结构化**  
   - 缺 `top5_snapshots/top5_root_summary`。

3. **不能断言 identity 成熟或失败**  
   - 有 created/hit/cache 字段，但缺 exact/deduped/skipped 分解和分段趋势。

4. **不能断言 CFS/NT 严格导致 action**  
   - 有 NT/action threshold 字段和合约结果，但缺窗口 causal_chain。

5. **不能断言 cache neutralization 无效**  
   - consumed ER/EV 为 0，但 timing 和 fast path 活跃；需 skip reason 与 CP reduction。

6. **不能把 CS 关闭当故障**  
   - manifest/config 明确新版 growth 默认，CS disabled 是预期背景。

7. **不能把 runtime residual promotion 全 0 当记忆失败**  
   - 新口径 residual tail memory projection 全程 handled/applied，旧 promotion 应接近 0。

8. **不能把 insert log suppression 当 target skipped**  
   - 该字段只说明 brief/detail 文件日志被抑制，非执行跳过。

---

# 15. 最终审阅判断

本次 AP 原型最像一个**以 HDB 查存一体、ER/EV 能量动力学、感应生长和可审计行动阈值为核心的持续认知实验系统**；它最不像纯规则 bot，也不像只靠向量检索拼上下文的 RAG agent。最有价值的实验信号是：**新版 growth 主链全程启用、尾巴 memory_id 接管稳定、旧 residual fallback 关闭、天气行动合约 3/3 正确，并且 NT/attention/action threshold 观测链完整存在**。下一轮最值得优先修的不是主逻辑，而是**补齐 curriculum summary、Top5/root/identity/causal_chain/performance slow-tail 观测，并温和回落注意力过热参数**。在这些证据补齐前，应避免把 identity created 多、cache consumed 为 0、CS 关闭或 legacy residual 为 0 直接解释为机制失败。
