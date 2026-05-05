import type { ApiResponse, DashboardData, DatasetRef, MetricRow } from '../types/api';

export const REQUEST_TIMEOUT_STORAGE_KEY = 'ap_next_request_timeout_ms';
const DEFAULT_TIMEOUT_MS = 60000;
const MIN_REQUEST_TIMEOUT_MS = 5000;
const MAX_REQUEST_TIMEOUT_MS = 600000;

function canUseBrowserStorage(): boolean {
  return typeof window !== 'undefined' && typeof window.localStorage !== 'undefined';
}

export function clampRequestTimeoutMs(value: unknown): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return DEFAULT_TIMEOUT_MS;
  return Math.min(MAX_REQUEST_TIMEOUT_MS, Math.max(MIN_REQUEST_TIMEOUT_MS, Math.round(parsed)));
}

export function getRequestTimeoutMs(): number {
  if (!canUseBrowserStorage()) return DEFAULT_TIMEOUT_MS;
  try {
    const raw = window.localStorage.getItem(REQUEST_TIMEOUT_STORAGE_KEY);
    if (!raw) return DEFAULT_TIMEOUT_MS;
    return clampRequestTimeoutMs(raw);
  } catch {
    return DEFAULT_TIMEOUT_MS;
  }
}

export function setRequestTimeoutMs(value: unknown): number {
  const next = clampRequestTimeoutMs(value);
  if (canUseBrowserStorage()) {
    try {
      window.localStorage.setItem(REQUEST_TIMEOUT_STORAGE_KEY, String(next));
      window.dispatchEvent(new CustomEvent('ap-next-timeout-changed', { detail: next }));
    } catch {
      // Ignore storage failures and still return the normalized value.
    }
  }
  return next;
}

function resolveTimeoutMs(timeoutMs?: number): number {
  const base = getRequestTimeoutMs();
  if (!Number.isFinite(Number(timeoutMs))) return base;
  return Math.max(base, clampRequestTimeoutMs(timeoutMs));
}

export class ApiError extends Error {
  status?: number;

  constructor(message: string, status?: number) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
  }
}

async function request<T>(url: string, options: RequestInit = {}, timeoutMs?: number): Promise<T> {
  const effectiveTimeoutMs = resolveTimeoutMs(timeoutMs);
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), effectiveTimeoutMs);
  try {
    const res = await fetch(url, {
      ...options,
      signal: controller.signal,
      headers: {
        ...(options.body ? { 'Content-Type': 'application/json' } : {}),
        ...(options.headers || {}),
      },
    });
    const payload = (await res.json().catch(() => ({}))) as ApiResponse<T>;
    if (!res.ok || payload.success === false) {
      throw new ApiError(payload.message || payload.error || `HTTP ${res.status}`, res.status);
    }
    return (payload.data ?? payload) as T;
  } catch (error) {
    if (error instanceof DOMException && error.name === 'AbortError') {
      throw new ApiError(`请求等待超过 ${Math.round(effectiveTimeoutMs / 1000)} 秒，已自动取消。请稍后重试，或在顶部把请求超时调高。`);
    }
    if (error instanceof Error && /aborted|abort/i.test(error.message || '')) {
      throw new ApiError('请求已取消或超时。若页面仍在加载大量数据，可以稍后重试，或提高顶部请求超时设置。');
    }
    throw error;
  } finally {
    window.clearTimeout(timer);
  }
}

export function apiGet<T>(url: string, timeoutMs?: number): Promise<T> {
  return request<T>(url, { method: 'GET' }, timeoutMs);
}

export function apiPost<T>(url: string, body: unknown = {}, timeoutMs?: number): Promise<T> {
  return request<T>(url, { method: 'POST', body: JSON.stringify(body ?? {}) }, timeoutMs);
}

async function apiPostFallback<T>(urls: string[], body: unknown = {}, timeoutMs?: number): Promise<T> {
  let lastError: unknown = null;
  for (const url of urls) {
    try {
      return await apiPost<T>(url, body, timeoutMs);
    } catch (error) {
      lastError = error;
      if (!(error instanceof ApiError) || error.status !== 404) throw error;
    }
  }
  throw lastError instanceof Error ? lastError : new ApiError(String(lastError || 'request failed'));
}

export const api = {
  health: () => apiGet<{ status: string }>('/api/health'),
  dashboard: (full = false) => apiGet<DashboardData>(`/api/dashboard${full ? '?full=1' : ''}`, 20000),
  state: (topK?: number) => apiGet<any>(`/api/state${topK ? `?top_k=${encodeURIComponent(String(topK))}` : ''}`, 20000),
  hdb: () => apiGet<any>('/api/hdb', 20000),
  actionRuntime: () => apiGet<any>('/api/action_runtime', 12000),
  config: () => apiGet<any>('/api/config', 12000),
  saveConfig: (module: string, values: Record<string, unknown>) => apiPost<any>('/api/config/save', { module, values }, 30000),
  runCycle: (text: string) => apiPost<any>('/api/cycle', { text }, 120000),
  runTicks: (count: number) => apiPost<any>('/api/tick', { count }, 120000),
  reload: () => apiPost<any>('/api/reload', {}, 60000),
  shutdown: () => apiPost<any>('/api/shutdown', {}, 10000),
  restart: () => apiPost<any>('/api/restart', {}, 10000),
  clearAll: () => apiPostFallback<any>(['/api/clear_all', '/api/experiment/clear_all'], {}, 120000),
  clearRuntime: () => apiPostFallback<any>(['/api/clear_runtime', '/api/experiment/runtime/clear'], {}, 120000),
  clearHdb: () => apiPostFallback<any>(['/api/clear_hdb', '/api/experiment/hdb/clear'], {}, 120000),
  checkHdb: (target: string | null) => apiPost<any>('/api/check', { target }, 30000),
  repairHdb: (target: string) => apiPost<any>('/api/repair', { target }, 120000),
  repairAllHdb: () => apiPost<any>('/api/repair_all', {}, 30000),
  stopRepair: (repairJobId: string) => apiPost<any>('/api/stop_repair', { repair_job_id: repairJobId }, 30000),
  maintenanceJobs: () => apiGet<any>('/api/maintenance_jobs', 10000),
  backgroundJobs: () => apiGet<any>('/api/background_jobs?limit=120', 10000),
  idleConsolidate: (
    background = true,
    options: { rebuild_pointer_index?: boolean; apply_soft_limits?: boolean; batch_limit?: number | null; max_cs_events?: number | null; reason?: string } = {},
  ) => apiPost<any>('/api/idle_consolidate', { background, ...options }, 120000),
  idleConsolidateStatus: (jobId: string) =>
    apiGet<any>(`/api/idle_consolidate_status?job_id=${encodeURIComponent(jobId)}`, 30000),
  actionStop: (payload: { mode: string; value?: string | null; hold_ticks?: number; reason?: string }) =>
    apiPost<any>('/api/action_stop', payload, 30000),
  queryStructure: (id: string) => apiGet<any>(`/api/structure?structure_id=${encodeURIComponent(id)}`, 20000),
  queryGroup: (id: string) => apiGet<any>(`/api/group?group_id=${encodeURIComponent(id)}`, 20000),
  episodic: (limit = 20) => apiGet<any>(`/api/episodic?limit=${limit}`, 20000),
  innateRules: () => apiGet<any>('/api/innate_rules', 20000),
  validateInnateRules: (payload: any) => apiPost<any>('/api/innate_rules/validate', payload, 30000),
  saveInnateRules: (payload: any) => apiPost<any>('/api/innate_rules/save', payload, 30000),
  reloadInnateRules: () => apiPost<any>('/api/innate_rules/reload', {}, 30000),
  simulateInnateRules: () => apiPost<any>('/api/innate_rules/simulate', {}, 30000),
  datasetProtocol: () => apiGet<any>('/api/experiment/dataset_protocol', 20000),
  datasets: () => apiGet<any>('/api/experiment/datasets', 30000),
  previewDataset: (datasetRef: DatasetRef, limit = 24) =>
    apiPost<any>('/api/experiment/datasets/preview', { dataset_ref: datasetRef, limit }, 30000),
  expandDataset: (datasetRef: DatasetRef, limit = 120) =>
    apiPost<any>('/api/experiment/datasets/expand', { dataset_ref: datasetRef, limit }, 30000),
  importDataset: (filename: string, format: string, content: string) =>
    apiPost<any>('/api/experiment/datasets/import', { filename, format, content }, 30000),
  experimentRuns: (limit = 48) => apiGet<any>(`/api/experiment/runs?limit=${limit}`, 30000),
  experimentLivePreview: () => apiGet<any>('/api/experiment/live_preview', 10000),
  runManifest: (runId: string) => apiGet<any>(`/api/experiment/run/manifest?run_id=${encodeURIComponent(runId)}`, 30000),
  runMetrics: (runId: string, downsampleEvery = 1) =>
    apiGet<{ rows?: MetricRow[]; items?: MetricRow[] }>(
      `/api/experiment/run/metrics?run_id=${encodeURIComponent(runId)}&downsample_every=${downsampleEvery}`,
      60000,
    ),
  startExperiment: (datasetRef: DatasetRef, options: any) =>
    apiPost<any>('/api/experiment/run/start', { dataset_ref: datasetRef, options }, 30000),
  stopExperiment: (jobId: string) => apiPost<any>('/api/experiment/run/stop', { job_id: jobId }, 30000),
  experimentJob: (jobId: string) => apiGet<any>(`/api/experiment/jobs?job_id=${encodeURIComponent(jobId)}`, 15000),
  experimentJobs: () => apiGet<any>('/api/experiment/jobs', 15000),
  deleteRun: (runId: string) => apiPost<any>('/api/experiment/run/delete', { run_id: runId }, 30000),
  clearRuns: () => apiPost<any>('/api/experiment/runs/clear', {}, 30000),
  llmReviewJobs: () => apiGet<any>('/api/experiment/llm_review/jobs', 20000),
  saveLlmReviewConfig: (config: Record<string, unknown>) => apiPost<any>('/api/experiment/llm_review/config/save', { config }, 30000),
  autoTunerConfig: () => apiGet<any>('/api/experiment/auto_tuner/config', 20000),
  autoTunerCatalog: () => apiGet<any>('/api/experiment/auto_tuner/catalog', 30000),
  autoTunerState: () => apiGet<any>('/api/experiment/auto_tuner/state', 30000),
  autoTunerAudit: () => apiGet<any>('/api/experiment/auto_tuner/audit', 30000),
  autoTunerRules: () => apiGet<any>('/api/experiment/auto_tuner/rules', 30000),
  autoTunerRollbackPoints: () => apiGet<any>('/api/experiment/auto_tuner/rollback_points', 30000),
  autoTunerRollback: (pointId: string) => apiPost<any>('/api/experiment/auto_tuner/rollback', { point_id: pointId }, 30000),
  saveAutoTunerConfig: (config: Record<string, unknown>) => apiPost<any>('/api/experiment/auto_tuner/config/save', { config }, 30000),
  saveAutoTunerRules: (rules: Record<string, unknown>) => apiPost<any>('/api/experiment/auto_tuner/rules/save', { rules }, 30000),
  autoTunerLlmConfig: () => apiGet<any>('/api/experiment/auto_tuner/llm/config', 20000),
  saveAutoTunerLlmConfig: (config: Record<string, unknown>) =>
    apiPost<any>('/api/experiment/auto_tuner/llm/config/save', { config }, 30000),
  autoTunerLlmJobs: () => apiGet<any>('/api/experiment/auto_tuner/llm/jobs', 20000),
  startAutoTunerLlmAnalyze: (runId: string, userPrompt = '', focusMetrics: string[] = []) =>
    apiPost<any>('/api/experiment/auto_tuner/llm/analyze', { run_id: runId, user_prompt: userPrompt, focus_metrics: focusMetrics }, 30000),
  llmReviewConfig: () => apiGet<any>('/api/experiment/llm_review/config', 20000),
  llmReviewStatus: (runId: string) =>
    apiGet<any>(`/api/experiment/run/llm_review_status?run_id=${encodeURIComponent(runId)}`, 20000),
  llmReviewReport: (runId: string) =>
    apiGet<any>(`/api/experiment/run/llm_review_report?run_id=${encodeURIComponent(runId)}`, 20000),
  startLlmReview: (runId: string, force = false) =>
    apiPost<any>('/api/experiment/run/llm_review/start', { run_id: runId, force }, 30000),
};
