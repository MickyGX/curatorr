import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const ARR_BASE_IDS = new Set(['radarr', 'sonarr', 'lidarr', 'readarr', 'bazarr', 'prowlarr']);
const DOWNLOADER_BASE_IDS = new Set(['transmission', 'qbittorrent', 'nzbget', 'sabnzbd']);
const DOWNLOAD_RULE_ORDER = ['slowSpeed', 'slowTime', 'stalled', 'seedCriteria'];
const ARR_RULE_ORDER = ['failedImport', 'badFiles', 'unmonitored', 'orphaned'];
const RULE_ORDER = [...DOWNLOAD_RULE_ORDER, ...ARR_RULE_ORDER];
const ACTIONS = new Set(['warn', 'pause', 'remove']);
const AGGRESSION_LEVEL_ORDER = ['patient', 'low', 'medium', 'high'];
const AGGRESSION_LEVELS = new Set(AGGRESSION_LEVEL_ORDER);
const DEFAULT_AGGRESSION_LEVEL = 'medium';
const ARR_MATCH_IMPORT_FAILURE_HINTS = Object.freeze([
  'import failed',
  'unable to import',
  'failed to import',
  'not imported',
  'no files found are eligible for import',
  'one or more episodes expected in this release were not imported',
  'downloaded - waiting to import',
  'import blocked',
  'download client unavailable',
]);
const ARR_MATCH_BAD_FILE_HINTS = Object.freeze([
  'sample',
  'corrupt',
  'damaged',
  'invalid',
  'unsupported',
  'password',
  'repair failed',
  'parity',
  'crc',
  'verification failed',
  'not a valid',
]);
const AGGRESSION_PRESETS = Object.freeze({
  patient: {
    pollIntervalSeconds: 600,
    safety: {
      allowPrivateTrackerRemovals: false,
      deleteLocalData: false,
    },
    rules: {
      slowSpeed: {
        enabled: true,
        minBps: 100 * 1024,
        graceMinutes: 30,
        maxStrikes: 1,
        cooldownMinutes: 60,
        action: 'warn',
      },
      slowTime: {
        enabled: false,
        minEtaMinutes: 180,
        graceMinutes: 30,
        maxStrikes: 2,
        cooldownMinutes: 60,
        action: 'warn',
      },
      stalled: {
        enabled: true,
        noProgressMinutes: 10,
        graceMinutes: 10,
        maxStrikes: 1,
        cooldownMinutes: 45,
        metadlTimeBasedStrikes: true,
        action: 'warn',
      },
      seedCriteria: {
        enabled: false,
        requireBoth: false,
        graceMinutes: 0,
        maxStrikes: 1,
        cooldownMinutes: 60,
        action: 'remove',
      },
      failedImport: {
        enabled: true,
        graceMinutes: 60,
        maxStrikes: 1,
        cooldownMinutes: 120,
        action: 'remove',
      },
      badFiles: {
        enabled: true,
        graceMinutes: 30,
        maxStrikes: 1,
        cooldownMinutes: 120,
        action: 'remove',
      },
      unmonitored: {
        enabled: true,
        graceMinutes: 10,
        maxStrikes: 1,
        cooldownMinutes: 120,
        action: 'remove',
      },
      orphaned: {
        enabled: false,
        graceMinutes: 90,
        maxStrikes: 1,
        cooldownMinutes: 180,
        action: 'warn',
        onlyCompleted: true,
      },
    },
  },
  low: {
    pollIntervalSeconds: 300,
    safety: {
      allowPrivateTrackerRemovals: false,
      deleteLocalData: false,
    },
    rules: {
      slowSpeed: {
        enabled: false,
        minBps: 64 * 1024,
        graceMinutes: 30,
        maxStrikes: 5,
        cooldownMinutes: 90,
        action: 'warn',
      },
      slowTime: {
        enabled: false,
        minEtaMinutes: 180,
        graceMinutes: 30,
        maxStrikes: 5,
        cooldownMinutes: 90,
        action: 'warn',
      },
      stalled: {
        enabled: true,
        noProgressMinutes: 90,
        graceMinutes: 45,
        maxStrikes: 5,
        cooldownMinutes: 90,
        metadlTimeBasedStrikes: true,
        action: 'warn',
      },
      seedCriteria: {
        enabled: false,
        requireBoth: false,
        graceMinutes: 0,
        maxStrikes: 1,
        cooldownMinutes: 60,
        action: 'remove',
      },
      failedImport: {
        enabled: false,
        graceMinutes: 90,
        maxStrikes: 1,
        cooldownMinutes: 120,
        action: 'warn',
      },
      badFiles: {
        enabled: false,
        graceMinutes: 60,
        maxStrikes: 1,
        cooldownMinutes: 120,
        action: 'warn',
      },
      unmonitored: {
        enabled: false,
        graceMinutes: 30,
        maxStrikes: 1,
        cooldownMinutes: 120,
        action: 'warn',
      },
      orphaned: {
        enabled: false,
        graceMinutes: 180,
        maxStrikes: 1,
        cooldownMinutes: 180,
        action: 'warn',
        onlyCompleted: true,
      },
    },
  },
  medium: {
    pollIntervalSeconds: 180,
    safety: {
      allowPrivateTrackerRemovals: false,
      deleteLocalData: false,
    },
    rules: {
      slowSpeed: {
        enabled: true,
        minBps: 128 * 1024,
        graceMinutes: 15,
        maxStrikes: 3,
        cooldownMinutes: 30,
        action: 'warn',
      },
      slowTime: {
        enabled: false,
        minEtaMinutes: 120,
        graceMinutes: 20,
        maxStrikes: 3,
        cooldownMinutes: 30,
        action: 'warn',
      },
      stalled: {
        enabled: true,
        noProgressMinutes: 30,
        graceMinutes: 20,
        maxStrikes: 3,
        cooldownMinutes: 30,
        metadlTimeBasedStrikes: true,
        action: 'warn',
      },
      seedCriteria: {
        enabled: false,
        requireBoth: false,
        graceMinutes: 0,
        maxStrikes: 1,
        cooldownMinutes: 60,
        action: 'remove',
      },
      failedImport: {
        enabled: true,
        graceMinutes: 60,
        maxStrikes: 1,
        cooldownMinutes: 120,
        action: 'warn',
      },
      badFiles: {
        enabled: true,
        graceMinutes: 45,
        maxStrikes: 1,
        cooldownMinutes: 120,
        action: 'warn',
      },
      unmonitored: {
        enabled: true,
        graceMinutes: 20,
        maxStrikes: 1,
        cooldownMinutes: 120,
        action: 'warn',
      },
      orphaned: {
        enabled: false,
        graceMinutes: 120,
        maxStrikes: 1,
        cooldownMinutes: 180,
        action: 'warn',
        onlyCompleted: true,
      },
    },
  },
  high: {
    pollIntervalSeconds: 90,
    safety: {
      allowPrivateTrackerRemovals: false,
      deleteLocalData: false,
    },
    rules: {
      slowSpeed: {
        enabled: true,
        minBps: 512 * 1024,
        graceMinutes: 10,
        maxStrikes: 2,
        cooldownMinutes: 20,
        action: 'pause',
      },
      slowTime: {
        enabled: true,
        minEtaMinutes: 45,
        graceMinutes: 10,
        maxStrikes: 2,
        cooldownMinutes: 20,
        action: 'pause',
      },
      stalled: {
        enabled: true,
        noProgressMinutes: 20,
        graceMinutes: 10,
        maxStrikes: 2,
        cooldownMinutes: 20,
        metadlTimeBasedStrikes: true,
        action: 'pause',
      },
      seedCriteria: {
        enabled: false,
        requireBoth: true,
        graceMinutes: 0,
        maxStrikes: 1,
        cooldownMinutes: 30,
        action: 'remove',
      },
      failedImport: {
        enabled: true,
        graceMinutes: 30,
        maxStrikes: 1,
        cooldownMinutes: 90,
        action: 'remove',
      },
      badFiles: {
        enabled: true,
        graceMinutes: 20,
        maxStrikes: 1,
        cooldownMinutes: 90,
        action: 'remove',
      },
      unmonitored: {
        enabled: true,
        graceMinutes: 10,
        maxStrikes: 1,
        cooldownMinutes: 90,
        action: 'remove',
      },
      orphaned: {
        enabled: true,
        graceMinutes: 60,
        maxStrikes: 1,
        cooldownMinutes: 120,
        action: 'remove',
        onlyCompleted: true,
      },
    },
  },
});
const AGGRESSION_LEVEL_META = Object.freeze({
  patient: {
    label: 'Patient',
    description: 'A patient cleanup profile: 10-minute sweeps, 10-minute stalled checks, 30-minute slow checks, and 60-minute failed-import cleanup, while keeping Sweeparr dry-run-safe by default.',
  },
  low: {
    label: 'Low',
    description: 'More conservative than the guide\'s Patient preset. Slow/ETA checks stay relaxed, stalled waits 90 minutes, and ARR cleanup rules stay mostly informational.',
  },
  medium: {
    label: 'Medium',
    description: 'Balanced defaults for mixed public/private queues. Enables ARR issue detection with warn-first actions so you can tune before going live.',
  },
  high: {
    label: 'High',
    description: 'Fastest cleanup profile. Tight queue thresholds plus ARR-backed removals intended for operators who already validated dry-run output.',
  },
});

const DEFAULT_MODULE_CONFIG = Object.freeze({
  enabled: true,
  dryRun: true,
  runOnStartup: true,
  pollIntervalSeconds: 180,
  maxEvents: 300,
  stateRetentionHours: 72,
  aggressionLevel: DEFAULT_AGGRESSION_LEVEL,
  selectedDownloaderIds: [],
  selectedArrIds: [],
  trackerPrivacyOverrides: {},
  safety: {
    allowPrivateTrackerRemovals: false,
    deleteLocalData: false,
  },
  rules: {
    slowSpeed: {
      enabled: true,
      minBps: 128 * 1024,
      graceMinutes: 15,
      maxStrikes: 3,
      cooldownMinutes: 30,
      action: 'warn',
    },
    slowTime: {
      enabled: false,
      minEtaMinutes: 120,
      graceMinutes: 20,
      maxStrikes: 3,
      cooldownMinutes: 30,
      action: 'warn',
    },
    stalled: {
      enabled: true,
      noProgressMinutes: 30,
      graceMinutes: 20,
      maxStrikes: 3,
      cooldownMinutes: 30,
      metadlTimeBasedStrikes: true,
      action: 'warn',
    },
    seedCriteria: {
      enabled: false,
      requireBoth: false,
      graceMinutes: 0,
      maxStrikes: 1,
      cooldownMinutes: 60,
      action: 'remove',
    },
    failedImport: {
      enabled: true,
      graceMinutes: 60,
      maxStrikes: 1,
      cooldownMinutes: 120,
      action: 'warn',
    },
    badFiles: {
      enabled: true,
      graceMinutes: 45,
      maxStrikes: 1,
      cooldownMinutes: 120,
      action: 'warn',
    },
    unmonitored: {
      enabled: true,
      graceMinutes: 20,
      maxStrikes: 1,
      cooldownMinutes: 120,
      action: 'warn',
    },
    orphaned: {
      enabled: false,
      graceMinutes: 120,
      maxStrikes: 1,
      cooldownMinutes: 180,
      action: 'warn',
      onlyCompleted: true,
    },
  },
});

const runtime = {
  ctx: null,
  moduleContext: null,
  timer: null,
  intervalMs: 0,
  nextRunAtMs: 0,
  running: false,
  lastRunAt: '',
  lastError: '',
  lastSummary: null,
};

function normalizeId(value) {
  return String(value || '').trim().toLowerCase();
}

function fallbackBaseId(appId) {
  return normalizeId(appId).split('-')[0] || '';
}

function toPositiveInt(value, fallback, min = 1, max = Number.MAX_SAFE_INTEGER) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(max, Math.max(min, Math.round(parsed)));
}

function toNonNegativeNumber(value, fallback = 0) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(0, parsed);
}

function toBoolean(value, fallback = false) {
  if (value === undefined) return fallback;
  return Boolean(value);
}

function normalizeAggressionLevel(value, fallback = DEFAULT_AGGRESSION_LEVEL) {
  const raw = String(value || '').trim().toLowerCase();
  const normalized = raw === 'patientdragon' || raw === 'patient-dragon'
    ? 'patient'
    : raw;
  if (AGGRESSION_LEVELS.has(normalized)) return normalized;
  return fallback;
}

function cloneJson(value, fallback = {}) {
  try {
    return JSON.parse(JSON.stringify(value));
  } catch (_err) {
    return fallback;
  }
}

function buildAggressionPresetPatch(level) {
  const normalized = normalizeAggressionLevel(level, DEFAULT_AGGRESSION_LEVEL);
  const preset = AGGRESSION_PRESETS[normalized] || AGGRESSION_PRESETS[DEFAULT_AGGRESSION_LEVEL];
  return {
    aggressionLevel: normalized,
    ...cloneJson(preset, {}),
  };
}

function getAggressionPresetsResponse() {
  const levels = AGGRESSION_LEVEL_ORDER.map((id) => ({
    id,
    label: AGGRESSION_LEVEL_META[id]?.label || id,
    description: AGGRESSION_LEVEL_META[id]?.description || '',
  }));

  const presets = {};
  AGGRESSION_LEVEL_ORDER.forEach((id) => {
    presets[id] = cloneJson(AGGRESSION_PRESETS[id], {});
  });

  return {
    defaultLevel: DEFAULT_AGGRESSION_LEVEL,
    levels,
    presets,
  };
}

function sanitizeRuleConfig(ruleName, rawRule = {}) {
  const base = DEFAULT_MODULE_CONFIG.rules[ruleName] || {};
  const next = {
    enabled: toBoolean(rawRule.enabled, base.enabled),
    maxStrikes: toPositiveInt(rawRule.maxStrikes, base.maxStrikes, 1, 10),
    graceMinutes: toPositiveInt(rawRule.graceMinutes, base.graceMinutes, 0, 24 * 60),
    cooldownMinutes: toPositiveInt(rawRule.cooldownMinutes, base.cooldownMinutes, 0, 24 * 60),
    action: ACTIONS.has(String(rawRule.action || '').trim().toLowerCase())
      ? String(rawRule.action || '').trim().toLowerCase()
      : base.action,
  };

  if (ruleName === 'slowSpeed') {
    next.minBps = toNonNegativeNumber(rawRule.minBps, base.minBps);
  } else if (ruleName === 'slowTime') {
    next.minEtaMinutes = toPositiveInt(rawRule.minEtaMinutes, base.minEtaMinutes, 1, 24 * 60);
  } else if (ruleName === 'stalled') {
    next.noProgressMinutes = toPositiveInt(rawRule.noProgressMinutes, base.noProgressMinutes, 1, 24 * 60);
    next.metadlTimeBasedStrikes = toBoolean(rawRule.metadlTimeBasedStrikes, toBoolean(base.metadlTimeBasedStrikes, true));
  } else if (ruleName === 'seedCriteria') {
    next.requireBoth = toBoolean(rawRule.requireBoth, toBoolean(base.requireBoth, false));
  } else if (ruleName === 'orphaned') {
    next.onlyCompleted = toBoolean(rawRule.onlyCompleted, toBoolean(base.onlyCompleted, true));
  }

  return next;
}

function sanitizeSafetyConfig(rawSafety = {}) {
  const base = DEFAULT_MODULE_CONFIG.safety || {};
  return {
    allowPrivateTrackerRemovals: toBoolean(rawSafety.allowPrivateTrackerRemovals, toBoolean(base.allowPrivateTrackerRemovals, false)),
    deleteLocalData: toBoolean(rawSafety.deleteLocalData, toBoolean(base.deleteLocalData, false)),
  };
}

function sanitizeTrackerPrivacyOverrides(rawOverrides = {}) {
  const input = rawOverrides && typeof rawOverrides === 'object' && !Array.isArray(rawOverrides)
    ? rawOverrides
    : {};
  const next = {};
  Object.entries(input).forEach(([rawTracker, rawPrivacy]) => {
    const trackerHost = normalizeTrackerHost(rawTracker);
    if (!trackerHost) return;
    const privacy = String(rawPrivacy || '').trim().toLowerCase();
    if (privacy !== 'public' && privacy !== 'private') return;
    next[trackerHost] = privacy;
  });
  return next;
}

function sanitizeSelectedDownloaderIds(rawIds = []) {
  if (!Array.isArray(rawIds)) return [];
  const seen = new Set();
  const next = [];
  rawIds.forEach((value) => {
    const id = normalizeId(value);
    if (!id || seen.has(id)) return;
    seen.add(id);
    next.push(id);
  });
  return next;
}

function sanitizeModuleConfig(raw = {}) {
  const input = raw && typeof raw === 'object' && !Array.isArray(raw) ? raw : {};
  const next = {
    enabled: toBoolean(input.enabled, DEFAULT_MODULE_CONFIG.enabled),
    dryRun: toBoolean(input.dryRun, DEFAULT_MODULE_CONFIG.dryRun),
    runOnStartup: toBoolean(input.runOnStartup, DEFAULT_MODULE_CONFIG.runOnStartup),
    pollIntervalSeconds: toPositiveInt(input.pollIntervalSeconds, DEFAULT_MODULE_CONFIG.pollIntervalSeconds, 30, 3600),
    maxEvents: toPositiveInt(input.maxEvents, DEFAULT_MODULE_CONFIG.maxEvents, 50, 5000),
    stateRetentionHours: toPositiveInt(input.stateRetentionHours, DEFAULT_MODULE_CONFIG.stateRetentionHours, 1, 24 * 30),
    aggressionLevel: normalizeAggressionLevel(input.aggressionLevel, DEFAULT_MODULE_CONFIG.aggressionLevel),
    selectedDownloaderIds: sanitizeSelectedDownloaderIds(input.selectedDownloaderIds || []),
    selectedArrIds: sanitizeSelectedDownloaderIds(input.selectedArrIds || []),
    trackerPrivacyOverrides: sanitizeTrackerPrivacyOverrides(input.trackerPrivacyOverrides || {}),
    safety: sanitizeSafetyConfig(input.safety || {}),
    rules: {},
  };

  const rawRules = input.rules && typeof input.rules === 'object' ? input.rules : {};
  for (const ruleName of RULE_ORDER) {
    next.rules[ruleName] = sanitizeRuleConfig(ruleName, rawRules[ruleName] || {});
  }
  return next;
}

function deepMerge(target, patch) {
  if (!patch || typeof patch !== 'object' || Array.isArray(patch)) return target;
  const next = { ...(target && typeof target === 'object' ? target : {}) };
  Object.entries(patch).forEach(([key, value]) => {
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      next[key] = deepMerge(next[key], value);
      return;
    }
    next[key] = value;
  });
  return next;
}

function getModuleDir(moduleContext = {}) {
  const configured = String(moduleContext.moduleDir || '').trim();
  if (configured) return configured;
  return path.resolve(__dirname, '..');
}

function getLegacyDataDir(moduleContext = {}) {
  return path.join(getModuleDir(moduleContext), 'data');
}

function getPersistentDataDir(moduleContext = {}) {
  const runtimeDataRoot = String(runtime?.ctx?.DATA_DIR || '').trim();
  if (!runtimeDataRoot) return '';
  const moduleId = normalizeId(moduleContext.id) || 'sweeparr';
  return path.join(runtimeDataRoot, 'local-modules', moduleId);
}

function getDataDir(moduleContext = {}) {
  return getPersistentDataDir(moduleContext) || getLegacyDataDir(moduleContext);
}

function getConfigPath(moduleContext = {}) {
  return path.join(getDataDir(moduleContext), 'sweeparr-config.json');
}

function getStatePath(moduleContext = {}) {
  return path.join(getDataDir(moduleContext), 'sweeparr-state.json');
}

function getEventsPath(moduleContext = {}) {
  return path.join(getDataDir(moduleContext), 'sweeparr-events.json');
}

function getUiPath(moduleContext = {}) {
  return path.join(getModuleDir(moduleContext), 'server', 'ui.html');
}

function ensureDir(dirPath) {
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
}

function ensureDataFilesLocation(moduleContext = {}) {
  const persistentDir = getPersistentDataDir(moduleContext);
  if (!persistentDir) {
    ensureDir(getLegacyDataDir(moduleContext));
    return;
  }

  ensureDir(persistentDir);
  const legacyDir = getLegacyDataDir(moduleContext);
  const fileNames = ['sweeparr-config.json', 'sweeparr-state.json', 'sweeparr-events.json'];

  fileNames.forEach((name) => {
    const preferredPath = path.join(persistentDir, name);
    if (fs.existsSync(preferredPath)) return;
    const legacyPath = path.join(legacyDir, name);
    if (fs.existsSync(legacyPath)) {
      fs.copyFileSync(legacyPath, preferredPath);
    }
  });
}

function readJsonFile(filePath, fallback) {
  try {
    const raw = fs.readFileSync(filePath, 'utf8');
    return JSON.parse(raw);
  } catch (_err) {
    return fallback;
  }
}

function writeJsonAtomic(filePath, payload) {
  const dirPath = path.dirname(filePath);
  ensureDir(dirPath);
  const tmpPath = `${filePath}.tmp`;
  fs.writeFileSync(tmpPath, JSON.stringify(payload, null, 2));
  fs.renameSync(tmpPath, filePath);
}

function loadModuleConfig(moduleContext = {}) {
  const configPath = getConfigPath(moduleContext);
  const existing = readJsonFile(configPath, null);
  const config = sanitizeModuleConfig(existing || DEFAULT_MODULE_CONFIG);
  if (!existing) {
    writeJsonAtomic(configPath, config);
  }
  return config;
}

function saveModuleConfig(moduleContext = {}, config) {
  const next = sanitizeModuleConfig(config);
  writeJsonAtomic(getConfigPath(moduleContext), next);
  return next;
}

function loadModuleState(moduleContext = {}) {
  const statePath = getStatePath(moduleContext);
  const state = readJsonFile(statePath, null);
  if (!state || typeof state !== 'object' || Array.isArray(state)) {
    return { items: {}, lastRunAt: '', lastSummary: null };
  }
  if (!state.items || typeof state.items !== 'object' || Array.isArray(state.items)) {
    state.items = {};
  }
  return state;
}

function saveModuleState(moduleContext = {}, state) {
  writeJsonAtomic(getStatePath(moduleContext), state);
}

function appendModuleEvents(moduleContext = {}, events = [], maxEvents = DEFAULT_MODULE_CONFIG.maxEvents) {
  if (!Array.isArray(events) || !events.length) return;
  const eventsPath = getEventsPath(moduleContext);
  const current = readJsonFile(eventsPath, []);
  const next = Array.isArray(current) ? current.concat(events) : [...events];
  const bounded = next.slice(-Math.max(50, maxEvents));
  writeJsonAtomic(eventsPath, bounded);
}

function listModuleEvents(moduleContext = {}, options = 100) {
  const isNumericInput = typeof options === 'number' || typeof options === 'string';
  const limit = isNumericInput
    ? toPositiveInt(options, 100, 1, 500)
    : toPositiveInt(options?.limit, 100, 1, 500);
  const runId = isNumericInput ? '' : String(options?.runId || '').trim();
  const typeFilter = isNumericInput
    ? []
    : (Array.isArray(options?.types) ? options.types : []);
  const typeSet = new Set(typeFilter.map((value) => String(value || '').trim()).filter(Boolean));
  const eventsPath = getEventsPath(moduleContext);
  const current = readJsonFile(eventsPath, []);
  if (!Array.isArray(current)) return [];
  const filtered = current.filter((event) => {
    if (!event || typeof event !== 'object') return false;
    if (runId && String(event.runId || '').trim() !== runId) return false;
    if (typeSet.size && !typeSet.has(String(event.type || '').trim())) return false;
    return true;
  });
  return filtered.slice(-Math.max(1, Math.min(500, Number(limit) || 100)));
}

function listModuleRemovals(moduleContext = {}, options = {}) {
  const limit = toPositiveInt(options?.limit, 100, 1, 500);
  const runId = String(options?.runId || '').trim();
  const events = listModuleEvents(moduleContext, {
    limit: Math.max(limit * 5, limit),
    runId,
    types: ['action'],
  });

  const removals = events
    .filter((event) => String(event.action || '').trim().toLowerCase() === 'remove')
    .map((event) => {
      const dryRun = Boolean(event.dryRun);
      const ok = Boolean(event.ok);
      const performed = Boolean(event.performed);
      let outcome = 'flagged';
      if (dryRun) outcome = 'dry-run';
      else if (performed && ok) outcome = 'removed';
      else if (!ok) outcome = 'failed';
      return {
        ts: String(event.ts || '').trim(),
        runId: String(event.runId || '').trim(),
        clientId: String(event.clientId || '').trim(),
        clientBaseId: String(event.clientBaseId || '').trim(),
        itemKey: String(event.itemKey || '').trim(),
        itemName: String(event.itemName || '').trim(),
        rule: String(event.rule || '').trim(),
        outcome,
        dryRun,
        ok,
        performed,
        reason: String(event.reason || '').trim(),
        error: String(event.error || '').trim(),
      };
    });

  return removals.slice(-limit);
}

function nowIso() {
  return new Date().toISOString();
}

function normalizeBaseUrl(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  try {
    const parsed = new URL(raw);
    parsed.hash = '';
    return parsed.toString().replace(/\/+$/, '');
  } catch (_err) {
    return '';
  }
}

function pickClientApiBase(appItem) {
  const explicitApi = normalizeBaseUrl(appItem?.apiUrl);
  if (explicitApi) return explicitApi;

  // Prefer localUrl for server-to-server calls inside the Docker/NAS network.
  const local = normalizeBaseUrl(appItem?.localUrl);
  if (local) return local;

  const remote = normalizeBaseUrl(appItem?.remoteUrl);
  if (remote) return remote;

  return normalizeBaseUrl(appItem?.url);
}

function buildClientRecord(appItem, baseId) {
  const apiBase = pickClientApiBase(appItem);
  return {
    id: String(appItem.id || '').trim(),
    baseId,
    name: String(appItem.name || appItem.id || '').trim(),
    url: apiBase,
    apiUrl: apiBase,
    localUrl: normalizeBaseUrl(appItem?.localUrl),
    remoteUrl: normalizeBaseUrl(appItem?.remoteUrl),
    username: String(appItem.username || '').trim(),
    password: String(appItem.password || ''),
    apiKey: String(appItem.apiKey || ''),
    hasPassword: Boolean(String(appItem.password || '').trim()),
    hasApiKey: Boolean(String(appItem.apiKey || '').trim()),
  };
}

function sanitizeClientRecord(client) {
  return {
    id: client.id,
    baseId: client.baseId,
    name: client.name,
    url: client.url,
    apiUrl: client.apiUrl,
    username: client.username,
    hasPassword: client.hasPassword,
    hasApiKey: client.hasApiKey,
  };
}

function resolveLauncharrClients(ctx) {
  const resolveBaseId = typeof ctx.getAppBaseId === 'function' ? ctx.getAppBaseId : fallbackBaseId;
  const config = typeof ctx.loadConfig === 'function' ? ctx.loadConfig() : {};
  const apps = Array.isArray(config?.apps) ? config.apps : [];
  const arrClients = [];
  const downloaderClients = [];

  apps.forEach((appItem) => {
    if (!appItem || appItem.removed) return;
    const baseId = normalizeId(resolveBaseId(appItem.id));
    if (!baseId) return;
    if (ARR_BASE_IDS.has(baseId)) {
      arrClients.push(buildClientRecord(appItem, baseId));
      return;
    }
    if (DOWNLOADER_BASE_IDS.has(baseId)) {
      downloaderClients.push(buildClientRecord(appItem, baseId));
    }
  });

  return { arrClients, downloaderClients };
}

function ensureSweeparrAppEnabled(ctx) {
  if (!ctx || typeof ctx.loadConfig !== 'function' || typeof ctx.saveConfig !== 'function') return;
  const config = ctx.loadConfig();
  if (!config || !Array.isArray(config.apps)) return;

  const appIndex = config.apps.findIndex((item) => normalizeId(item?.id) === 'sweeparr');
  if (appIndex < 0) return;

  const current = config.apps[appIndex] && typeof config.apps[appIndex] === 'object'
    ? config.apps[appIndex]
    : {};
  const next = { ...current };
  let changed = false;

  if (next.removed) {
    next.removed = false;
    changed = true;
  }
  if (!String(next.name || '').trim()) {
    next.name = 'Sweeparr';
    changed = true;
  }
  if (!String(next.category || '').trim()) {
    next.category = 'Arr Suite';
    changed = true;
  }

  const general = typeof ctx.resolveGeneralSettings === 'function'
    ? ctx.resolveGeneralSettings(config)
    : {};
  const withSweeparrPath = (value) => {
    const base = normalizeBaseUrl(value);
    if (!base) return '';
    return base.endsWith('/sweeparr') ? base : `${base}/sweeparr`;
  };
  const defaultLocal = withSweeparrPath(general?.localUrl || '');
  const defaultRemote = withSweeparrPath(general?.remoteUrl || '');

  if (!String(next.localUrl || '').trim() && defaultLocal) {
    next.localUrl = defaultLocal;
    changed = true;
  }
  if (!String(next.remoteUrl || '').trim() && defaultRemote) {
    next.remoteUrl = defaultRemote;
    changed = true;
  }
  if (!String(next.url || '').trim()) {
    const fallbackUrl = defaultLocal || defaultRemote;
    if (fallbackUrl) {
      next.url = fallbackUrl;
      changed = true;
    }
  }

  if (!changed) return;
  const nextConfig = {
    ...config,
    apps: config.apps.map((item, idx) => (idx === appIndex ? next : item)),
  };
  ctx.saveConfig(nextConfig);
}

function sanitizeClientsForResponse(clients) {
  return {
    arrClients: (Array.isArray(clients?.arrClients) ? clients.arrClients : []).map(sanitizeClientRecord),
    downloaderClients: (Array.isArray(clients?.downloaderClients) ? clients.downloaderClients : []).map(sanitizeClientRecord),
  };
}

function filterDownloaderClientsBySelection(allClients = [], selectedIds = []) {
  const candidates = Array.isArray(allClients) ? allClients : [];
  const selected = sanitizeSelectedDownloaderIds(selectedIds);
  if (!selected.length) {
    return {
      selectedClients: [...candidates],
      selectedIds: [],
      selectedMode: 'all',
    };
  }
  const selectedSet = new Set(selected);
  const selectedClients = candidates.filter((client) => selectedSet.has(normalizeId(client?.id)));
  return {
    selectedClients,
    selectedIds: selected,
    selectedMode: 'selected',
  };
}

function filterArrClientsBySelection(allClients = [], selectedIds = []) {
  const candidates = Array.isArray(allClients) ? allClients : [];
  const selected = sanitizeSelectedDownloaderIds(selectedIds);
  if (!selected.length) {
    return {
      selectedClients: [...candidates],
      selectedIds: [],
      selectedMode: 'all',
    };
  }
  const selectedSet = new Set(selected);
  const selectedClients = candidates.filter((client) => selectedSet.has(normalizeId(client?.id)));
  return {
    selectedClients,
    selectedIds: selected,
    selectedMode: 'selected',
  };
}

function safeMessage(ctx, err, fallback = 'Unknown error') {
  if (typeof ctx.safeMessage === 'function') return ctx.safeMessage(err) || fallback;
  const message = String(err?.message || err || '').trim();
  return message || fallback;
}

function buildAuthHeader(ctx, appItem) {
  if (typeof ctx.buildBasicAuthHeader === 'function') {
    return ctx.buildBasicAuthHeader(appItem?.username || '', appItem?.password || '');
  }
  const user = String(appItem?.username || '').trim();
  const pass = String(appItem?.password || '').trim();
  if (!user && !pass) return '';
  return `Basic ${Buffer.from(`${user}:${pass}`).toString('base64')}`;
}

function buildApiUrl(ctx, baseUrl, endpoint) {
  if (typeof ctx.buildAppApiUrl === 'function') {
    return ctx.buildAppApiUrl(baseUrl, endpoint);
  }
  const root = String(baseUrl || '').trim().replace(/\/+$/, '');
  const cleanEndpoint = String(endpoint || '').trim().replace(/^\/+/, '');
  return new URL(`${root}/${cleanEndpoint}`);
}

function normalizeName(value) {
  return String(value || '').trim();
}

function normalizeTitleMatchKey(value) {
  return normalizeName(value)
    .toLowerCase()
    .replace(/\[[^\]]*\]/g, ' ')
    .replace(/\([^)]*\)/g, ' ')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim()
    .replace(/\s+/g, ' ');
}

function hashFallback(input) {
  return crypto.createHash('sha1').update(String(input || '')).digest('hex').slice(0, 16);
}

function parseEtaTextToSeconds(value) {
  const raw = String(value || '').trim();
  if (!raw) return 0;
  if (/^\d+$/.test(raw)) return Number(raw);
  const parts = raw.split(':').map((item) => Number(item));
  if (!parts.every((item) => Number.isFinite(item))) return 0;
  if (parts.length === 3) {
    return (parts[0] * 3600) + (parts[1] * 60) + parts[2];
  }
  if (parts.length === 2) {
    return (parts[0] * 60) + parts[1];
  }
  return 0;
}

function normalizeRatioValue(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return null;
  return Math.max(0, numeric);
}

function normalizeSecondsValue(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return null;
  return Math.max(0, Math.round(numeric));
}

function normalizeRatioLimitValue(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return null;
  if (numeric < 0) return Math.round(numeric);
  return Number(numeric.toFixed(3));
}

function normalizeIntLimitValue(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return null;
  return Math.round(numeric);
}

function normalizeTrackerValue(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  if (/^\[.*\]$/.test(raw)) return raw;
  try {
    const parsed = new URL(raw);
    return parsed.hostname || raw;
  } catch (_err) {
    return raw;
  }
}

function normalizeTrackerHost(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) return '';
  if (/^\[.*\]$/.test(raw)) return '';
  try {
    return String(new URL(raw).hostname || '').trim().toLowerCase();
  } catch (_err) {
    return raw;
  }
}

const KNOWN_PUBLIC_TRACKER_HOST_HINTS = new Set([
  'opentrackr.org',
  'openbittorrent.com',
  'publictracker.xyz',
  'tracker.publicbt.com',
  'exodus.desync.com',
  'tracker.torrent.eu.org',
  'bttracker.debian.org',
  'open.stealth.si',
  'gbitt.info',
]);

function inferPrivacyFromTracker(value) {
  const host = normalizeTrackerHost(value);
  if (!host) return '';
  for (const hint of KNOWN_PUBLIC_TRACKER_HOST_HINTS) {
    if (host === hint || host.endsWith(`.${hint}`)) return 'public';
  }
  return 'private';
}

function normalizePrivacyValue(value) {
  if (value === 1 || value === '1') return 'private';
  if (value === 0 || value === '0') return 'public';
  if (value === true) return 'private';
  if (value === false) return 'public';
  return '';
}

function transmissionStatusLabel(statusCode) {
  const code = Number(statusCode);
  if (code === 0) return 'stopped';
  if (code === 1 || code === 2) return 'checking';
  if (code === 3) return 'queued';
  if (code === 4) return 'downloading';
  if (code === 5) return 'queued-seeding';
  if (code === 6) return 'seeding';
  return 'unknown';
}

function normalizeTransmissionItems(client, items) {
  return (Array.isArray(items) ? items : []).map((item) => {
    const rawId = String(item?.id ?? '').trim();
    const downloadId = String(item?.hashString || item?.hash || rawId || '').trim().toLowerCase();
    const name = normalizeName(item?.name || rawId || 'Unknown');
    const key = `${client.id}:transmission:${rawId || hashFallback(name)}`;
    const status = transmissionStatusLabel(item?.status);
    const progress = Math.max(0, Math.min(1, Number(item?.percentDone) || 0));
    const speedBps = Math.max(0, Number(item?.rateDownload) || 0);
    const etaSeconds = Math.max(0, Number(item?.eta) || 0);
    const ratio = normalizeRatioValue(item?.uploadRatio);
    const seedTimeSeconds = normalizeSecondsValue(item?.secondsSeeding);
    const seedRatioMode = Number(item?.seedRatioMode);
    const seedIdleMode = Number(item?.seedIdleMode);
    const rawSeedRatioLimit = normalizeRatioLimitValue(item?.seedRatioLimit);
    const rawSeedIdleLimit = normalizeIntLimitValue(item?.seedIdleLimit);
    const ratioLimit = seedRatioMode === 2
      ? -1
      : (seedRatioMode === 0
        ? -2
        : rawSeedRatioLimit);
    const seedTimeLimit = seedIdleMode === 2
      ? -1
      : (seedIdleMode === 0
        ? -2
        : rawSeedIdleLimit);
    const trackers = Array.isArray(item?.trackers) ? item.trackers : [];
    const firstTracker = trackers.find((tracker) => String(tracker?.announce || tracker?.sitename || '').trim()) || null;
    const tracker = normalizeTrackerValue(firstTracker?.announce || firstTracker?.sitename || '');
    const privacy = normalizePrivacyValue(item?.isPrivate) || inferPrivacyFromTracker(tracker);
    const addedDate = Number(item?.addedDate);
    const addedAt = Number.isFinite(addedDate) && addedDate > 0
      ? new Date(addedDate * 1000).toISOString()
      : '';
    return {
      key,
      clientId: client.id,
      clientBaseId: client.baseId,
      clientName: client.name,
      rawId,
      downloadId,
      name,
      status,
      progress,
      speedBps,
      etaSeconds,
      ratio,
      seedTimeSeconds,
      ratioLimit,
      seedTimeLimit,
      tracker,
      privacy,
      addedAt,
      isDownloading: status === 'downloading' || status === 'queued',
    };
  });
}

function normalizeQbitItems(client, items) {
  return (Array.isArray(items) ? items : []).map((item) => {
    const rawId = String(item?.hash || '').trim().toLowerCase();
    const downloadId = rawId;
    const name = normalizeName(item?.name || rawId || 'Unknown');
    const key = `${client.id}:qbittorrent:${rawId || hashFallback(name)}`;
    const state = normalizeId(item?.state);
    const progress = Math.max(0, Math.min(1, Number(item?.progress) || 0));
    const speedBps = Math.max(0, Number(item?.dlspeed) || 0);
    const etaSeconds = Math.max(0, Number(item?.eta) || 0);
    const ratio = normalizeRatioValue(item?.ratio);
    const seedTimeSeconds = normalizeSecondsValue(item?.seeding_time);
    const ratioLimit = normalizeRatioLimitValue(item?.ratio_limit);
    const seedTimeLimit = normalizeIntLimitValue(item?.seeding_time_limit);
    const tracker = normalizeTrackerValue(item?.tracker);
    const privacy = normalizePrivacyValue(item?.is_private ?? item?.private) || inferPrivacyFromTracker(tracker);
    const addedOn = Number(item?.added_on);
    const addedAt = Number.isFinite(addedOn) && addedOn > 0
      ? new Date(addedOn * 1000).toISOString()
      : '';
    const isDownloading = ['downloading', 'forceddl', 'metadl', 'stalleddl', 'queueddl', 'checkinguploading'].includes(state)
      || state.includes('dl');
    return {
      key,
      clientId: client.id,
      clientBaseId: client.baseId,
      clientName: client.name,
      rawId,
      downloadId,
      name,
      status: state || 'unknown',
      progress,
      speedBps,
      etaSeconds,
      ratio,
      seedTimeSeconds,
      ratioLimit,
      seedTimeLimit,
      tracker,
      privacy,
      addedAt,
      isDownloading,
    };
  });
}

function normalizeNzbgetItems(client, items) {
  return (Array.isArray(items) ? items : []).map((item) => {
    const rawId = String(item?.NZBID ?? '').trim();
    const downloadId = String(item?.NZBID ?? rawId ?? '').trim().toLowerCase();
    const name = normalizeName(item?.NZBName || rawId || 'Unknown');
    const key = `${client.id}:nzbget:${rawId || hashFallback(name)}`;
    const fileSizeMb = Number(item?.FileSizeMB) || 0;
    const downloadedMb = Number(item?.DownloadedSizeMB) || 0;
    const speedKb = Math.max(0, Number(item?.DownloadRate) || 0);
    const speedBps = speedKb * 1024;
    const progress = fileSizeMb > 0 ? Math.max(0, Math.min(1, downloadedMb / fileSizeMb)) : 0;
    const remainingMb = Math.max(0, fileSizeMb - downloadedMb);
    const etaSeconds = speedBps > 0 ? Math.round((remainingMb * 1024 * 1024) / speedBps) : 0;
    const status = normalizeId(item?.Status || 'unknown');
    const isDownloading = ['downloading', 'queued', 'paused'].includes(status) || status.includes('down');
    return {
      key,
      clientId: client.id,
      clientBaseId: client.baseId,
      clientName: client.name,
      rawId,
      downloadId,
      name,
      status,
      progress,
      speedBps,
      etaSeconds,
      ratio: null,
      seedTimeSeconds: null,
      ratioLimit: null,
      seedTimeLimit: null,
      tracker: '',
      privacy: '',
      addedAt: '',
      isDownloading,
    };
  });
}

function normalizeSabItems(client, items) {
  return (Array.isArray(items) ? items : []).map((item) => {
    const rawId = String(item?.nzo_id || '').trim();
    const downloadId = rawId.toLowerCase();
    const name = normalizeName(item?.filename || rawId || 'Unknown');
    const key = `${client.id}:sabnzbd:${rawId || hashFallback(name)}`;
    const mb = Number(item?.mb) || 0;
    const mbLeft = Number(item?.mbleft) || 0;
    const progress = mb > 0 ? Math.max(0, Math.min(1, (mb - mbLeft) / mb)) : 0;
    const etaSeconds = parseEtaTextToSeconds(item?.timeleft || item?.eta || '');
    const status = normalizeId(item?.status || 'unknown');
    const isDownloading = status.includes('down') || status.includes('queue');
    return {
      key,
      clientId: client.id,
      clientBaseId: client.baseId,
      clientName: client.name,
      rawId,
      downloadId,
      name,
      status,
      progress,
      speedBps: null,
      etaSeconds,
      ratio: null,
      seedTimeSeconds: null,
      ratioLimit: null,
      seedTimeLimit: null,
      tracker: '',
      privacy: '',
      addedAt: '',
      isDownloading,
    };
  });
}

async function fetchTransmissionDetailedQueue(ctx, client) {
  const authHeader = buildAuthHeader(ctx, client);
  const result = await transmissionRpc(String(client.apiUrl || client.url || '').trim(), authHeader, {
    method: 'torrent-get',
    arguments: {
      fields: [
        'id',
        'hashString',
        'name',
        'status',
        'percentDone',
        'eta',
        'rateDownload',
        'rateUpload',
        'sizeWhenDone',
        'totalSize',
        'leftUntilDone',
        'addedDate',
        'isFinished',
        'isStalled',
        'error',
        'errorString',
        'uploadRatio',
        'secondsSeeding',
        'seedRatioMode',
        'seedRatioLimit',
        'seedIdleMode',
        'seedIdleLimit',
        'trackers',
      ],
    },
  });
  return Array.isArray(result?.arguments?.torrents) ? result.arguments.torrents : [];
}

async function fetchJsonWithTimeout(url, options = {}, timeoutMs = 12000) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal,
    });
    const text = await response.text();
    let payload = {};
    try {
      payload = text ? JSON.parse(text) : {};
    } catch (_err) {
      payload = {};
    }
    return { ok: response.ok, status: response.status, payload, text };
  } finally {
    clearTimeout(timeout);
  }
}

function flattenArrStatusMessages(rawMessages = []) {
  const messages = Array.isArray(rawMessages) ? rawMessages : [];
  return messages
    .flatMap((entry) => {
      if (!entry || typeof entry !== 'object') return [];
      const title = String(entry.title || '').trim();
      const inner = Array.isArray(entry.messages) ? entry.messages : [];
      const parts = [
        title,
        ...inner.map((message) => String(message || '').trim()),
      ].filter(Boolean);
      return parts;
    })
    .filter(Boolean);
}

function matchesAnyHint(text, hints = []) {
  const haystack = String(text || '').trim().toLowerCase();
  if (!haystack) return false;
  return hints.some((hint) => haystack.includes(String(hint || '').trim().toLowerCase()));
}

function normalizeArrQueueItem(client, entry = {}) {
  const title = normalizeName(
    entry.title
    || entry.releaseTitle
    || entry.movie?.title
    || entry.series?.title
    || entry.episode?.title
    || entry.episodeFile?.relativePath
    || entry.id
  ) || 'Unknown';
  const rawMessages = flattenArrStatusMessages(entry.statusMessages || []);
  const errorMessage = String(entry.errorMessage || '').trim();
  const allMessages = [...rawMessages, errorMessage].filter(Boolean);
  const messageText = allMessages.join(' | ');
  const trackedDownloadStatus = normalizeId(entry.trackedDownloadStatus);
  const trackedDownloadState = normalizeId(entry.trackedDownloadState);
  const queueStatus = normalizeId(entry.status);
  const monitoredFlags = [
    entry.movie?.monitored,
    entry.series?.monitored,
    ...(Array.isArray(entry.episodes) ? entry.episodes.map((episode) => episode?.monitored) : []),
  ].filter((value) => typeof value === 'boolean');
  const monitored = monitoredFlags.length ? monitoredFlags.every(Boolean) : true;
  const failedImport = (
    trackedDownloadStatus === 'warning'
    || trackedDownloadStatus === 'error'
    || trackedDownloadState === 'importpending'
    || trackedDownloadState === 'importblocked'
  ) && matchesAnyHint(messageText, ARR_MATCH_IMPORT_FAILURE_HINTS);
  const badFiles = matchesAnyHint(messageText, ARR_MATCH_BAD_FILE_HINTS);
  return {
    appId: String(client.id || '').trim(),
    appBaseId: String(client.baseId || '').trim(),
    appName: String(client.name || client.id || '').trim(),
    id: String(entry.id ?? '').trim(),
    title,
    titleKey: normalizeTitleMatchKey(title),
    downloadId: String(entry.downloadId || entry.downloadClientId || '').trim().toLowerCase(),
    protocol: normalizeId(entry.protocol),
    status: queueStatus || trackedDownloadState || trackedDownloadStatus || 'unknown',
    trackedDownloadStatus,
    trackedDownloadState,
    monitored,
    sizeleft: Number.isFinite(Number(entry.sizeleft)) ? Math.max(0, Number(entry.sizeleft)) : null,
    timeleft: Number.isFinite(Number(entry.timeleft)) ? Math.max(0, Number(entry.timeleft)) : null,
    failedImport,
    badFiles,
    messageText,
    messages: allMessages,
    raw: entry,
  };
}

async function fetchArrQueueItems(ctx, client) {
  const apiKey = String(client.apiKey || '').trim();
  const baseUrl = String(client.apiUrl || client.url || '').trim();
  if (!apiKey || !baseUrl) {
    return { items: [], error: `Missing API config for ${client.id}.` };
  }

  const headers = {
    Accept: 'application/json',
    'X-Api-Key': apiKey,
  };
  const pageSize = 250;
  const items = [];
  let page = 1;
  let totalRecords = pageSize;

  while (items.length < totalRecords && page <= 10) {
    const url = buildApiUrl(ctx, baseUrl, 'api/v3/queue');
    url.searchParams.set('page', String(page));
    url.searchParams.set('pageSize', String(pageSize));
    const result = await fetchJsonWithTimeout(url.toString(), { headers });
    if (!result.ok) {
      return {
        items: [],
        error: `${client.name || client.id} queue request failed (${result.status}).`,
      };
    }
    const payload = result.payload && typeof result.payload === 'object' ? result.payload : {};
    const pageItems = Array.isArray(payload.records) ? payload.records : [];
    totalRecords = Number(payload.totalRecords);
    totalRecords = Number.isFinite(totalRecords) && totalRecords >= 0 ? totalRecords : pageItems.length;
    items.push(...pageItems.map((entry) => normalizeArrQueueItem(client, entry)));
    if (!pageItems.length) break;
    page += 1;
  }

  return { items };
}

async function fetchArrWantedCount(ctx, client, pathSuffix) {
  const apiKey = String(client.apiKey || '').trim();
  const baseUrl = String(client.apiUrl || client.url || '').trim();
  if (!apiKey || !baseUrl) return null;
  const url = buildApiUrl(ctx, baseUrl, `api/v3/${pathSuffix}`);
  url.searchParams.set('page', '1');
  url.searchParams.set('pageSize', '1');
  const result = await fetchJsonWithTimeout(url.toString(), {
    headers: {
      Accept: 'application/json',
      'X-Api-Key': apiKey,
    },
  });
  if (!result.ok) return null;
  const payload = result.payload && typeof result.payload === 'object' ? result.payload : {};
  const total = Number(payload.totalRecords ?? payload.total);
  if (Number.isFinite(total) && total >= 0) return Math.round(total);
  const records = Array.isArray(payload.records) ? payload.records : [];
  return records.length;
}

async function buildArrQueueSnapshot(ctx, config = {}) {
  const clients = resolveLauncharrClients(ctx);
  const arrSelection = filterArrClientsBySelection(
    clients.arrClients.filter((client) => client.baseId === 'radarr' || client.baseId === 'sonarr'),
    config.selectedArrIds || []
  );
  const byDownloadId = new Map();
  const byTitleKey = new Map();
  const summary = [];
  const errors = [];

  for (const client of arrSelection.selectedClients) {
    let queueResult = { items: [], error: '' };
    try {
      queueResult = await fetchArrQueueItems(ctx, client);
    } catch (err) {
      queueResult = { items: [], error: safeMessage(ctx, err, `Failed to fetch ARR queue for ${client.id}.`) };
    }

    const queueItems = Array.isArray(queueResult.items) ? queueResult.items : [];
    if (queueResult.error) {
      errors.push({
        appId: client.id,
        appBaseId: client.baseId,
        error: String(queueResult.error || '').trim(),
      });
    }

    const missingWanted = await fetchArrWantedCount(ctx, client, 'wanted/missing').catch(() => null);
    const cutoffWanted = await fetchArrWantedCount(ctx, client, 'wanted/cutoff').catch(() => null);

    queueItems.forEach((entry) => {
      if (entry.downloadId) {
        const existing = byDownloadId.get(entry.downloadId) || [];
        existing.push(entry);
        byDownloadId.set(entry.downloadId, existing);
      }
      if (entry.titleKey) {
        const existing = byTitleKey.get(entry.titleKey) || [];
        existing.push(entry);
        byTitleKey.set(entry.titleKey, existing);
      }
    });

    summary.push({
      appId: client.id,
      appBaseId: client.baseId,
      appName: client.name,
      queueItems: queueItems.length,
      failedImport: queueItems.filter((entry) => entry.failedImport).length,
      badFiles: queueItems.filter((entry) => entry.badFiles).length,
      unmonitored: queueItems.filter((entry) => entry.monitored === false).length,
      missingWanted,
      cutoffWanted,
    });
  }

  return {
    summary,
    errors,
    byDownloadId,
    byTitleKey,
    selection: {
      mode: arrSelection.selectedMode,
      selectedArrIds: arrSelection.selectedIds,
    },
    counts: {
      arrApps: arrSelection.selectedClients.length,
      totalArrApps: clients.arrClients.filter((client) => client.baseId === 'radarr' || client.baseId === 'sonarr').length,
    },
  };
}

function getMatchedArrEntriesForItem(item, arrSnapshot) {
  if (!arrSnapshot || typeof arrSnapshot !== 'object') return [];
  const downloadId = String(item?.downloadId || item?.rawId || '').trim().toLowerCase();
  const titleKey = normalizeTitleMatchKey(item?.name || '');
  if (downloadId && arrSnapshot.byDownloadId instanceof Map && arrSnapshot.byDownloadId.has(downloadId)) {
    return arrSnapshot.byDownloadId.get(downloadId) || [];
  }
  if (titleKey && arrSnapshot.byTitleKey instanceof Map && arrSnapshot.byTitleKey.has(titleKey)) {
    return arrSnapshot.byTitleKey.get(titleKey) || [];
  }
  return [];
}

function summarizeArrMatch(entries = []) {
  const matches = Array.isArray(entries) ? entries : [];
  if (!matches.length) {
    return {
      matched: false,
      appNames: [],
      appName: '',
      statuses: [],
      monitored: true,
      failedImport: false,
      badFiles: false,
      messageText: '',
    };
  }
  return {
    matched: true,
    appNames: Array.from(new Set(matches.map((entry) => String(entry.appName || '').trim()).filter(Boolean))),
    appName: String(matches[0].appName || '').trim(),
    statuses: Array.from(new Set(matches.map((entry) => String(entry.status || '').trim()).filter(Boolean))),
    monitored: matches.every((entry) => entry.monitored !== false),
    failedImport: matches.some((entry) => entry.failedImport),
    badFiles: matches.some((entry) => entry.badFiles),
    messageText: matches
      .map((entry) => String(entry.messageText || '').trim())
      .filter(Boolean)
      .join(' | '),
  };
}

async function fetchDownloaderItems(ctx, client) {
  const apiBase = String(client.apiUrl || client.url || '').trim();
  if (!apiBase) {
    return { items: [], error: `Missing API URL for ${client.id}.` };
  }

  if (client.baseId === 'transmission') {
    try {
      const items = await fetchTransmissionDetailedQueue(ctx, client);
      return { items: normalizeTransmissionItems(client, items), error: '' };
    } catch (err) {
      return { items: [], error: safeMessage(ctx, err, 'Failed to reach Transmission.') };
    }
  }

  if (client.baseId === 'qbittorrent') {
    if (typeof ctx.fetchQbittorrentQueue !== 'function') {
      return { items: [], error: 'qBittorrent integration is unavailable in runtime context.' };
    }
    const result = await ctx.fetchQbittorrentQueue(apiBase, client.username || '', client.password || '');
    return {
      items: normalizeQbitItems(client, result?.items || []),
      error: String(result?.error || '').trim(),
    };
  }

  if (client.baseId === 'nzbget') {
    if (typeof ctx.fetchNzbgetQueue !== 'function') {
      return { items: [], error: 'NZBGet integration is unavailable in runtime context.' };
    }
    const authHeader = buildAuthHeader(ctx, client);
    const result = await ctx.fetchNzbgetQueue(apiBase, authHeader);
    return {
      items: normalizeNzbgetItems(client, result?.items || []),
      error: String(result?.error || '').trim(),
    };
  }

  if (client.baseId === 'sabnzbd') {
    if (typeof ctx.fetchSabnzbdQueue !== 'function') {
      return { items: [], error: 'SABnzbd integration is unavailable in runtime context.' };
    }
    const authHeader = buildAuthHeader(ctx, client);
    const result = await ctx.fetchSabnzbdQueue(apiBase, String(client.apiKey || '').trim(), authHeader);
    return {
      items: normalizeSabItems(client, result?.items || []),
      error: String(result?.error || '').trim(),
    };
  }

  return { items: [], error: `Unsupported downloader type "${client.baseId}".` };
}

function toStrikeCount(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return 0;
  return Math.max(0, Math.round(parsed));
}

async function listDownloaderItemsWithStrikes(ctx, moduleContext = {}) {
  const config = loadModuleConfig(moduleContext);
  const privacyOverrides = config?.trackerPrivacyOverrides && typeof config.trackerPrivacyOverrides === 'object'
    ? config.trackerPrivacyOverrides
    : {};
  const clients = resolveLauncharrClients(ctx);
  const arrSnapshot = await buildArrQueueSnapshot(ctx, config);
  const downloaderSelection = filterDownloaderClientsBySelection(
    clients.downloaderClients,
    config.selectedDownloaderIds || []
  );
  const state = loadModuleState(moduleContext);
  const stateItems = state && typeof state.items === 'object' && state.items ? state.items : {};
  const rows = [];
  const errors = [];

  for (const client of downloaderSelection.selectedClients) {
    let result = { items: [], error: '' };
    try {
      result = await fetchDownloaderItems(ctx, client);
    } catch (err) {
      result = { items: [], error: safeMessage(ctx, err, `Failed to fetch queue for ${client.id}.`) };
    }

    const error = String(result?.error || '').trim();
    if (error) {
      errors.push({
        clientId: client.id,
        clientBaseId: client.baseId,
        error,
      });
    }

    const items = Array.isArray(result?.items) ? result.items : [];
    items.forEach((item) => {
      const stateItem = stateItems[item.key] && typeof stateItems[item.key] === 'object'
        ? stateItems[item.key]
        : {};
      const strikes = stateItem.strikes && typeof stateItem.strikes === 'object'
        ? stateItem.strikes
        : {};
      const tracker = String(item.tracker || '').trim();
      const trackerHost = normalizeTrackerHost(tracker);
      const privacyOverride = trackerHost ? String(privacyOverrides[trackerHost] || '').trim().toLowerCase() : '';
      const basePrivacy = String(item.privacy || '').trim().toLowerCase();
      const privacy = privacyOverride || basePrivacy;
      const arrMatches = getMatchedArrEntriesForItem(item, arrSnapshot);
      const arr = summarizeArrMatch(arrMatches);

      rows.push({
        key: item.key,
        name: item.name,
        clientId: item.clientId,
        clientBaseId: item.clientBaseId,
        clientName: item.clientName,
        status: item.status,
        isDownloading: Boolean(item.isDownloading),
        downloadId: String(item.downloadId || item.rawId || '').trim().toLowerCase(),
        progress: Math.max(0, Math.min(1, Number(item.progress) || 0)),
        speedBps: Number.isFinite(Number(item.speedBps)) ? Math.max(0, Number(item.speedBps)) : null,
        etaSeconds: Number.isFinite(Number(item.etaSeconds)) ? Math.max(0, Number(item.etaSeconds)) : null,
        ratio: Number.isFinite(Number(item.ratio)) ? Math.max(0, Number(item.ratio)) : null,
        seedTimeSeconds: Number.isFinite(Number(item.seedTimeSeconds)) ? Math.max(0, Math.round(Number(item.seedTimeSeconds))) : null,
        ratioLimit: Number.isFinite(Number(item.ratioLimit)) ? Number(item.ratioLimit) : null,
        seedTimeLimit: Number.isFinite(Number(item.seedTimeLimit)) ? Number(item.seedTimeLimit) : null,
        tracker,
        trackerHost,
        privacy,
        privacyOverride,
        arr,
        strikes: {
          slowSpeed: toStrikeCount(strikes.slowSpeed),
          slowTime: toStrikeCount(strikes.slowTime),
          stalled: toStrikeCount(strikes.stalled),
          seedCriteria: toStrikeCount(strikes.seedCriteria),
          failedImport: toStrikeCount(strikes.failedImport),
          badFiles: toStrikeCount(strikes.badFiles),
          unmonitored: toStrikeCount(strikes.unmonitored),
          orphaned: toStrikeCount(strikes.orphaned),
        },
      });
    });
  }

  rows.sort((a, b) => {
    if (a.isDownloading !== b.isDownloading) return a.isDownloading ? -1 : 1;
    return String(a.name || '').localeCompare(String(b.name || ''), undefined, { sensitivity: 'base' });
  });

  return {
    items: rows,
    errors,
    counts: {
      downloaders: downloaderSelection.selectedClients.length,
      totalDownloaders: clients.downloaderClients.length,
      items: rows.length,
      downloading: rows.filter((row) => row.isDownloading).length,
      erroredClients: errors.length,
      arrMatchedItems: rows.filter((row) => row.arr?.matched).length,
      failedImportCandidates: rows.filter((row) => row.arr?.failedImport).length,
      badFileCandidates: rows.filter((row) => row.arr?.badFiles).length,
      unmonitoredCandidates: rows.filter((row) => row.arr?.matched && row.arr?.monitored === false).length,
      orphanedCandidates: rows.filter((row) => !row.arr?.matched).length,
    },
    selection: {
      mode: downloaderSelection.selectedMode,
      selectedDownloaderIds: downloaderSelection.selectedIds,
      arrMode: arrSnapshot.selection?.mode || 'all',
      selectedArrIds: Array.isArray(arrSnapshot.selection?.selectedArrIds) ? arrSnapshot.selection.selectedArrIds : [],
    },
    arrSummary: Array.isArray(arrSnapshot.summary) ? arrSnapshot.summary : [],
    arrErrors: Array.isArray(arrSnapshot.errors) ? arrSnapshot.errors : [],
  };
}

function ensureStateItem(stateItems, item, now) {
  const existing = stateItems[item.key] && typeof stateItems[item.key] === 'object'
    ? stateItems[item.key]
    : {
      firstSeenAt: now,
      lastProgressAt: now,
      lastProgress: item.progress,
      strikes: {},
      lastActionAt: '',
      lastActionRule: '',
      lastAction: '',
    };

  if (!existing.firstSeenAt) existing.firstSeenAt = now;
  if (!existing.lastProgressAt) existing.lastProgressAt = now;
  if (!existing.strikes || typeof existing.strikes !== 'object') existing.strikes = {};

  existing.lastSeenAt = now;
  existing.name = item.name;
  existing.clientId = item.clientId;
  existing.clientBaseId = item.clientBaseId;
  existing.downloadId = String(item.downloadId || item.rawId || '').trim().toLowerCase();

  if (item.progress > Number(existing.lastProgress || 0) + 0.0001) {
    existing.lastProgress = item.progress;
    existing.lastProgressAt = now;
  }

  stateItems[item.key] = existing;
  return existing;
}

function minutesSince(timestamp, nowMs) {
  const parsed = Date.parse(String(timestamp || '').trim());
  if (!Number.isFinite(parsed)) return Number.POSITIVE_INFINITY;
  return (nowMs - parsed) / 60000;
}

function isMetaDlStatus(item) {
  return normalizeId(item?.status) === 'metadl';
}

function computeElapsedStalledStrikes(stateItem, ruleCfg, nowMs) {
  const noProgressMinutes = minutesSince(stateItem.lastProgressAt, nowMs);
  const windowMinutes = Math.max(1, Number(ruleCfg?.noProgressMinutes) || 1);
  if (!Number.isFinite(noProgressMinutes) || noProgressMinutes < windowMinutes) return 0;
  return Math.max(1, Math.floor(noProgressMinutes / windowMinutes));
}

function resolveSeedTimeLimitSeconds(item) {
  const raw = Number(item?.seedTimeLimit);
  if (!Number.isFinite(raw)) return null;
  if (raw < 0) return raw;
  if (normalizeId(item?.clientBaseId) === 'transmission') {
    return Math.max(0, Math.round(raw * 60));
  }
  return Math.max(0, Math.round(raw));
}

function evaluateSeedCriteria(item, ruleCfg = {}) {
  const ratio = Number(item?.ratio);
  const ratioLimit = Number(item?.ratioLimit);
  const seedTimeSeconds = Number(item?.seedTimeSeconds);
  const seedTimeLimitSeconds = resolveSeedTimeLimitSeconds(item);
  const ratioComparable = Number.isFinite(ratio) && Number.isFinite(ratioLimit) && ratioLimit >= 0;
  const seedComparable = Number.isFinite(seedTimeSeconds) && Number.isFinite(seedTimeLimitSeconds) && seedTimeLimitSeconds >= 0;
  const ratioMet = ratioComparable ? ratio >= ratioLimit : false;
  const seedTimeMet = seedComparable ? seedTimeSeconds >= seedTimeLimitSeconds : false;
  const requireBoth = Boolean(ruleCfg.requireBoth);
  const met = requireBoth
    ? (ratioComparable && seedComparable && ratioMet && seedTimeMet)
    : ((ratioComparable && ratioMet) || (seedComparable && seedTimeMet));

  return {
    met,
    ratioMet,
    seedTimeMet,
    ratio,
    ratioLimit,
    seedTimeSeconds,
    seedTimeLimitSeconds,
  };
}

function shouldProtectPrivateRemoval(item, action, config = {}) {
  const destructive = action === 'remove';
  if (!destructive) return false;
  const allowPrivate = Boolean(config?.safety?.allowPrivateTrackerRemovals);
  return String(item?.privacy || '').trim().toLowerCase() === 'private' && !allowPrivate;
}

function evaluateReasons(item, stateItem, config, nowMs) {
  const reasons = [];
  const ageMinutes = minutesSince(stateItem.firstSeenAt, nowMs);
  const noProgressMinutes = minutesSince(stateItem.lastProgressAt, nowMs);
  const arr = item?.arr && typeof item.arr === 'object' ? item.arr : {};

  const seedCriteria = config.rules.seedCriteria;
  if (seedCriteria?.enabled && item.progress >= 1 && ageMinutes >= seedCriteria.graceMinutes) {
    const seedEval = evaluateSeedCriteria(item, seedCriteria);
    if (seedEval.met) {
      reasons.push('seedCriteria');
    }
  }

  const failedImport = config.rules.failedImport;
  if (failedImport?.enabled && arr.failedImport && ageMinutes >= failedImport.graceMinutes) {
    reasons.push('failedImport');
  }

  const badFiles = config.rules.badFiles;
  if (badFiles?.enabled && arr.badFiles && ageMinutes >= badFiles.graceMinutes) {
    reasons.push('badFiles');
  }

  const unmonitored = config.rules.unmonitored;
  if (unmonitored?.enabled && arr.matched && arr.monitored === false && ageMinutes >= unmonitored.graceMinutes) {
    reasons.push('unmonitored');
  }

  const orphaned = config.rules.orphaned;
  if (orphaned?.enabled && !arr.matched && ageMinutes >= orphaned.graceMinutes) {
    const onlyCompleted = Boolean(orphaned.onlyCompleted);
    if (!onlyCompleted || item.progress >= 1 || !item.isDownloading) {
      reasons.push('orphaned');
    }
  }

  if (!item.isDownloading || item.progress >= 1) return reasons;

  const slowSpeed = config.rules.slowSpeed;
  if (slowSpeed.enabled
    && ageMinutes >= slowSpeed.graceMinutes
    && Number.isFinite(item.speedBps)
    && item.speedBps <= slowSpeed.minBps) {
    reasons.push('slowSpeed');
  }

  const slowTime = config.rules.slowTime;
  if (slowTime.enabled
    && ageMinutes >= slowTime.graceMinutes
    && item.etaSeconds > 0
    && item.etaSeconds >= (slowTime.minEtaMinutes * 60)) {
    reasons.push('slowTime');
  }

  const stalled = config.rules.stalled;
  if (stalled.enabled
    && ageMinutes >= stalled.graceMinutes
    && noProgressMinutes >= stalled.noProgressMinutes) {
    reasons.push('stalled');
  }

  return reasons;
}

function formatMetric(value, suffix = '') {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return 'n/a';
  return `${Math.round(numeric)}${suffix}`;
}

function buildRuleReason(ruleName, item, stateItem, ruleCfg, nowMs) {
  const ageMinutes = minutesSince(stateItem.firstSeenAt, nowMs);
  const noProgressMinutes = minutesSince(stateItem.lastProgressAt, nowMs);
  const speedBps = Number(item.speedBps);
  const etaMinutes = Number(item.etaSeconds) / 60;

  if (ruleName === 'slowSpeed') {
    return `slowSpeed: speed ${formatMetric(speedBps, ' B/s')} <= ${formatMetric(ruleCfg.minBps, ' B/s')} after ${formatMetric(ageMinutes, ' min')} (grace ${formatMetric(ruleCfg.graceMinutes, ' min')})`;
  }
  if (ruleName === 'slowTime') {
    return `slowTime: eta ${formatMetric(etaMinutes, ' min')} >= ${formatMetric(ruleCfg.minEtaMinutes, ' min')} after ${formatMetric(ageMinutes, ' min')} (grace ${formatMetric(ruleCfg.graceMinutes, ' min')})`;
  }
  if (ruleName === 'stalled') {
    if (toBoolean(ruleCfg?.metadlTimeBasedStrikes, true) && isMetaDlStatus(item)) {
      const elapsedStrikes = computeElapsedStalledStrikes(stateItem, ruleCfg, nowMs);
      return `stalled: metadl no progress for ${formatMetric(noProgressMinutes, ' min')} >= ${formatMetric(ruleCfg.noProgressMinutes, ' min')} (grace ${formatMetric(ruleCfg.graceMinutes, ' min')}, elapsed strikes ${elapsedStrikes})`;
    }
    return `stalled: no progress for ${formatMetric(noProgressMinutes, ' min')} >= ${formatMetric(ruleCfg.noProgressMinutes, ' min')} (grace ${formatMetric(ruleCfg.graceMinutes, ' min')})`;
  }
  if (ruleName === 'seedCriteria') {
    const seedEval = evaluateSeedCriteria(item, ruleCfg);
    const ratioPart = Number.isFinite(seedEval.ratioLimit) && seedEval.ratioLimit >= 0
      ? `ratio ${formatMetric(seedEval.ratio)} >= ${formatMetric(seedEval.ratioLimit)}`
      : 'ratio n/a';
    const seedTimeLimitMinutes = Number.isFinite(seedEval.seedTimeLimitSeconds) && seedEval.seedTimeLimitSeconds >= 0
      ? (seedEval.seedTimeLimitSeconds / 60)
      : null;
    const seedTimePart = Number.isFinite(seedTimeLimitMinutes)
      ? `seedTime ${formatMetric((seedEval.seedTimeSeconds / 60), ' min')} >= ${formatMetric(seedTimeLimitMinutes, ' min')}`
      : 'seedTime n/a';
    const joiner = ruleCfg?.requireBoth ? ' AND ' : ' OR ';
    return `seedCriteria: ${ratioPart}${joiner}${seedTimePart}`;
  }
  if (ruleName === 'failedImport') {
    const messageText = String(item?.arr?.messageText || '').trim();
    return `failedImport: ARR reports an import failure or blocked import${messageText ? ` (${messageText.slice(0, 220)})` : ''}`;
  }
  if (ruleName === 'badFiles') {
    const messageText = String(item?.arr?.messageText || '').trim();
    return `badFiles: ARR flagged this release as a bad or invalid import candidate${messageText ? ` (${messageText.slice(0, 220)})` : ''}`;
  }
  if (ruleName === 'unmonitored') {
    const appName = String(item?.arr?.appName || '').trim() || 'ARR';
    return `unmonitored: ${appName} no longer marks this item as monitored.`;
  }
  if (ruleName === 'orphaned') {
    return `orphaned: no matching Radarr/Sonarr queue item was found for this downloader item.`;
  }
  return `rule matched: ${ruleName}`;
}

async function transmissionRpc(baseUrl, authHeader, payload) {
  const rpcUrl = new URL('transmission/rpc', String(baseUrl || '').replace(/\/+$/, '/') || '');
  const headers = {
    Accept: 'application/json',
    'Content-Type': 'application/json',
  };
  if (authHeader) headers.Authorization = authHeader;

  let sessionId = '';
  for (let attempt = 0; attempt < 3; attempt += 1) {
    if (sessionId) headers['X-Transmission-Session-Id'] = sessionId;
    const response = await fetch(rpcUrl.toString(), {
      method: 'POST',
      headers,
      body: JSON.stringify(payload),
    });
    if (response.status === 409) {
      sessionId = String(response.headers.get('x-transmission-session-id') || '').trim();
      if (!sessionId) break;
      continue;
    }
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Transmission RPC failed (${response.status}): ${text.slice(0, 180)}`);
    }
    const text = await response.text();
    return text ? JSON.parse(text) : {};
  }
  throw new Error('Transmission RPC session negotiation failed.');
}

async function qbLogin(baseUrl, username, password) {
  const loginUrl = new URL('api/v2/auth/login', String(baseUrl || '').replace(/\/+$/, '/') || '');
  const loginPayload = new URLSearchParams({
    username: String(username || '').trim(),
    password: String(password || '').trim(),
  });
  const response = await fetch(loginUrl.toString(), {
    method: 'POST',
    headers: {
      Accept: 'text/plain',
      'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    },
    body: loginPayload.toString(),
  });
  const text = await response.text();
  if (!response.ok || !/^ok\.?$/i.test(String(text || '').trim())) {
    throw new Error(`qBittorrent authentication failed (${response.status}).`);
  }
  const setCookie = String(response.headers.get('set-cookie') || '').trim();
  const firstCookie = setCookie.split(';')[0].trim();
  if (!firstCookie) throw new Error('qBittorrent authentication cookie missing.');
  return firstCookie;
}

async function applyAction(ctx, client, item, ruleName, action, dryRun, config = {}) {
  const deleteLocalData = Boolean(config?.safety?.deleteLocalData);
  if (action === 'warn') {
    return { ok: true, performed: false, dryRun, action };
  }
  if (dryRun) {
    return { ok: true, performed: false, dryRun: true, action };
  }

  if (client.baseId === 'transmission') {
    const baseUrl = String(client.apiUrl || client.url || '').trim();
    const torrentId = Number(item.rawId);
    if (!Number.isFinite(torrentId)) {
      return { ok: false, performed: false, dryRun: false, action, error: 'Missing Transmission torrent id.' };
    }
    const authHeader = buildAuthHeader(ctx, client);
    if (action === 'pause') {
      await transmissionRpc(baseUrl, authHeader, {
        method: 'torrent-stop',
        arguments: { ids: [torrentId] },
      });
      return { ok: true, performed: true, dryRun: false, action };
    }
    if (action === 'remove') {
      await transmissionRpc(baseUrl, authHeader, {
        method: 'torrent-remove',
        arguments: { ids: [torrentId], 'delete-local-data': deleteLocalData },
      });
      return { ok: true, performed: true, dryRun: false, action };
    }
  }

  if (client.baseId === 'qbittorrent') {
    const baseUrl = String(client.apiUrl || client.url || '').trim();
    const hash = String(item.rawId || '').trim();
    if (!hash) {
      return { ok: false, performed: false, dryRun: false, action, error: 'Missing qBittorrent hash.' };
    }
    const cookieHeader = await qbLogin(baseUrl, client.username || '', client.password || '');
    if (action === 'pause') {
      const pauseUrl = buildApiUrl(ctx, baseUrl, 'api/v2/torrents/pause');
      const payload = new URLSearchParams({ hashes: hash });
      const response = await fetch(pauseUrl.toString(), {
        method: 'POST',
        headers: {
          Cookie: cookieHeader,
          'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        },
        body: payload.toString(),
      });
      if (!response.ok) throw new Error(`qBittorrent pause failed (${response.status}).`);
      return { ok: true, performed: true, dryRun: false, action };
    }
    if (action === 'remove') {
      const deleteUrl = buildApiUrl(ctx, baseUrl, 'api/v2/torrents/delete');
      const payload = new URLSearchParams({ hashes: hash, deleteFiles: deleteLocalData ? 'true' : 'false' });
      const response = await fetch(deleteUrl.toString(), {
        method: 'POST',
        headers: {
          Cookie: cookieHeader,
          'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        },
        body: payload.toString(),
      });
      if (!response.ok) throw new Error(`qBittorrent remove failed (${response.status}).`);
      return { ok: true, performed: true, dryRun: false, action };
    }
  }

  return {
    ok: false,
    performed: false,
    dryRun: false,
    action,
    error: `Action "${action}" not implemented for ${client.baseId}.`,
    rule: ruleName,
  };
}

function buildEventRecord(type, payload = {}) {
  return {
    ts: nowIso(),
    type,
    ...payload,
  };
}

function trimState(state, nowMs, retentionHours) {
  const cutoffMs = nowMs - (retentionHours * 60 * 60 * 1000);
  const nextItems = {};
  let removed = 0;
  Object.entries(state.items || {}).forEach(([key, value]) => {
    const lastSeenMs = Date.parse(String(value?.lastSeenAt || '').trim());
    if (Number.isFinite(lastSeenMs) && lastSeenMs < cutoffMs) {
      removed += 1;
      return;
    }
    nextItems[key] = value;
  });
  state.items = nextItems;
  return removed;
}

async function runSweep(trigger = 'manual', options = {}) {
  if (runtime.running) {
    return { ok: false, skipped: true, reason: 'Sweep already running.' };
  }
  runtime.running = true;

  const startedAt = nowIso();
  const startedMs = Date.now();
  const runId = `run-${startedMs.toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
  const ctx = runtime.ctx;
  const moduleContext = runtime.moduleContext || {};
  const events = [];
  const pushRunEvent = (type, payload = {}) => {
    events.push(buildEventRecord(type, { runId, ...payload }));
  };
  const hasDryRunOverride = Object.prototype.hasOwnProperty.call(options || {}, 'dryRunOverride');
  const dryRunOverride = hasDryRunOverride ? Boolean(options.dryRunOverride) : null;

  try {
    const config = loadModuleConfig(moduleContext);
    const effectiveDryRun = dryRunOverride === null ? config.dryRun : dryRunOverride;
    if (!config.enabled) {
      const summary = {
        ok: true,
        runId,
        trigger,
        startedAt,
        completedAt: nowIso(),
        skipped: true,
        reason: 'Module disabled',
        dryRun: effectiveDryRun,
        configDryRun: config.dryRun,
      };
      runtime.lastRunAt = summary.completedAt;
      runtime.lastSummary = summary;
      return summary;
    }

    const state = loadModuleState(moduleContext);
    const clients = resolveLauncharrClients(ctx);
    const arrSnapshot = await buildArrQueueSnapshot(ctx, config);
    const downloaderSelection = filterDownloaderClientsBySelection(
      clients.downloaderClients,
      config.selectedDownloaderIds || []
    );

    let totalQueueItems = 0;
    let activeDownloads = 0;
    let arrMatchedItems = 0;
    let strikesRaised = 0;
    let actionsPlanned = 0;
    let actionsPerformed = 0;
    let removalsFlagged = 0;
    let blockedPrivateActions = 0;
    const errors = [];
    const seenKeys = new Set();

    for (const client of downloaderSelection.selectedClients) {
      const result = await fetchDownloaderItems(ctx, client);
      if (result.error) {
        errors.push({ clientId: client.id, error: result.error });
        pushRunEvent('client_error', {
          clientId: client.id,
          clientBaseId: client.baseId,
          message: result.error,
        });
      }

      const items = Array.isArray(result.items) ? result.items : [];
      totalQueueItems += items.length;

      for (const item of items) {
        seenKeys.add(item.key);
        if (item.isDownloading) activeDownloads += 1;
        const arrMatches = getMatchedArrEntriesForItem(item, arrSnapshot);
        const arr = summarizeArrMatch(arrMatches);
        const itemWithArr = { ...item, arr };
        if (arr.matched) arrMatchedItems += 1;
        const stateItem = ensureStateItem(state.items, itemWithArr, startedAt);
        const reasons = evaluateReasons(itemWithArr, stateItem, config, startedMs);
        const reasonSet = new Set(reasons);

        for (const ruleName of RULE_ORDER) {
          const ruleCfg = config.rules[ruleName];
          if (!ruleCfg || !ruleCfg.enabled) {
            stateItem.strikes[ruleName] = 0;
            continue;
          }

          if (!reasonSet.has(ruleName)) {
            stateItem.strikes[ruleName] = 0;
            continue;
          }

          let nextStrike = Number(stateItem.strikes[ruleName] || 0) + 1;
          let strikeMode = 'run';
          if (ruleName === 'stalled'
            && toBoolean(ruleCfg.metadlTimeBasedStrikes, true)
            && isMetaDlStatus(item)) {
            nextStrike = Math.max(nextStrike, computeElapsedStalledStrikes(stateItem, ruleCfg, startedMs));
            strikeMode = 'elapsed';
          }
          stateItem.strikes[ruleName] = nextStrike;
          strikesRaised += 1;

          pushRunEvent('strike', {
            rule: ruleName,
            strikeMode,
            strike: nextStrike,
            maxStrikes: ruleCfg.maxStrikes,
            clientId: itemWithArr.clientId,
            clientBaseId: itemWithArr.clientBaseId,
            itemKey: itemWithArr.key,
            itemName: itemWithArr.name,
            status: itemWithArr.status,
            speedBps: Number.isFinite(Number(itemWithArr.speedBps)) ? Math.round(Number(itemWithArr.speedBps)) : null,
            etaSeconds: itemWithArr.etaSeconds,
            progress: itemWithArr.progress,
            arrMatched: arr.matched,
            arrApp: arr.appName,
          });

          if (nextStrike < ruleCfg.maxStrikes) continue;

          const lastActionAgo = minutesSince(stateItem.lastActionAt, startedMs);
          if (stateItem.lastActionRule === ruleName && lastActionAgo < ruleCfg.cooldownMinutes) {
            continue;
          }

          actionsPlanned += 1;
          const actionReason = buildRuleReason(ruleName, itemWithArr, stateItem, ruleCfg, startedMs);
          if (shouldProtectPrivateRemoval(itemWithArr, ruleCfg.action, config)) {
            blockedPrivateActions += 1;
            pushRunEvent('action_blocked', {
              rule: ruleName,
              action: ruleCfg.action,
              reason: `${actionReason} | blocked: private tracker protection is enabled`,
              clientId: itemWithArr.clientId,
              clientBaseId: itemWithArr.clientBaseId,
              itemKey: itemWithArr.key,
              itemName: itemWithArr.name,
              privacy: itemWithArr.privacy,
            });
            stateItem.lastActionAt = nowIso();
            stateItem.lastActionRule = ruleName;
            stateItem.lastAction = 'blocked-private';
            stateItem.strikes[ruleName] = 0;
            continue;
          }
          if (ruleCfg.action === 'remove') removalsFlagged += 1;
          let actionResult;
          try {
            actionResult = await applyAction(ctx, client, itemWithArr, ruleName, ruleCfg.action, effectiveDryRun, config);
          } catch (err) {
            actionResult = {
              ok: false,
              performed: false,
              dryRun: false,
              action: ruleCfg.action,
              error: safeMessage(ctx, err, `Failed ${ruleCfg.action} action.`),
            };
          }

          if (actionResult.performed) actionsPerformed += 1;

          pushRunEvent('action', {
            rule: ruleName,
            action: ruleCfg.action,
            dryRun: Boolean(actionResult.dryRun),
            ok: Boolean(actionResult.ok),
            performed: Boolean(actionResult.performed),
            error: String(actionResult.error || '').trim(),
            reason: actionReason,
            clientId: itemWithArr.clientId,
            clientBaseId: itemWithArr.clientBaseId,
            itemKey: itemWithArr.key,
            itemName: itemWithArr.name,
            privacy: itemWithArr.privacy,
            arrMatched: arr.matched,
            arrApp: arr.appName,
            strikesAtAction: nextStrike,
          });

          if (ruleCfg.action === 'remove') {
            pushRunEvent('removal_flagged', {
              rule: ruleName,
              reason: actionReason,
              dryRun: Boolean(actionResult.dryRun),
              ok: Boolean(actionResult.ok),
              performed: Boolean(actionResult.performed),
              error: String(actionResult.error || '').trim(),
              clientId: itemWithArr.clientId,
              clientBaseId: itemWithArr.clientBaseId,
              itemKey: itemWithArr.key,
              itemName: itemWithArr.name,
              strikesAtAction: nextStrike,
            });

            if (typeof ctx.pushLog === 'function') {
              ctx.pushLog({
                level: actionResult.ok ? 'warn' : 'error',
                app: 'sweeparr',
                action: 'removal_flagged',
                message: `Removal flagged for "${itemWithArr.name || itemWithArr.key}": ${actionReason}`,
                meta: {
                  clientId: itemWithArr.clientId,
                  clientBaseId: itemWithArr.clientBaseId,
                  itemKey: itemWithArr.key,
                  dryRun: Boolean(actionResult.dryRun),
                  performed: Boolean(actionResult.performed),
                  error: String(actionResult.error || '').trim(),
                },
              });
            }
          }

          stateItem.lastActionAt = nowIso();
          stateItem.lastActionRule = ruleName;
          stateItem.lastAction = ruleCfg.action;
          stateItem.strikes[ruleName] = 0;
        }
      }
    }

    Object.keys(state.items || {}).forEach((key) => {
      if (!seenKeys.has(key)) {
        state.items[key].lastSeenAt = state.items[key].lastSeenAt || startedAt;
      }
    });

    const pruned = trimState(state, startedMs, config.stateRetentionHours);
    const summary = {
      ok: errors.length === 0,
      runId,
      trigger,
      startedAt,
      completedAt: nowIso(),
      clients: {
        arr: arrSnapshot.counts?.arrApps || 0,
        totalArr: arrSnapshot.counts?.totalArrApps || clients.arrClients.length,
        downloaders: downloaderSelection.selectedClients.length,
        totalDownloaders: clients.downloaderClients.length,
        selectedDownloaderIds: downloaderSelection.selectedIds,
        downloaderSelectionMode: downloaderSelection.selectedMode,
        selectedArrIds: Array.isArray(arrSnapshot.selection?.selectedArrIds) ? arrSnapshot.selection.selectedArrIds : [],
        arrSelectionMode: arrSnapshot.selection?.mode || 'all',
      },
      totals: {
        queueItems: totalQueueItems,
        activeDownloads,
        arrMatchedItems,
        strikesRaised,
        actionsPlanned,
        actionsPerformed,
        removalsFlagged,
        blockedPrivateActions,
        prunedStateItems: pruned,
      },
      dryRun: effectiveDryRun,
      configDryRun: config.dryRun,
      arrSummary: Array.isArray(arrSnapshot.summary) ? arrSnapshot.summary : [],
      arrErrors: Array.isArray(arrSnapshot.errors) ? arrSnapshot.errors : [],
      errors,
    };

    state.lastRunAt = summary.completedAt;
    state.lastSummary = summary;
    saveModuleState(moduleContext, state);
    pushRunEvent('run_summary', summary);
    appendModuleEvents(moduleContext, events, config.maxEvents);

    runtime.lastRunAt = summary.completedAt;
    runtime.lastSummary = summary;
    runtime.lastError = '';
    return summary;
  } catch (err) {
    const message = safeMessage(ctx || {}, err, 'Sweeparr run failed.');
    runtime.lastError = message;
    const failedSummary = {
      ok: false,
      runId,
      trigger,
      startedAt,
      completedAt: nowIso(),
      error: message,
    };
    runtime.lastRunAt = failedSummary.completedAt;
    runtime.lastSummary = failedSummary;
    appendModuleEvents(moduleContext, [buildEventRecord('run_error', { runId, ...failedSummary })], loadModuleConfig(moduleContext).maxEvents);
    return failedSummary;
  } finally {
    runtime.running = false;
  }
}

function scheduleSweepLoop() {
  if (runtime.timer) {
    clearInterval(runtime.timer);
    runtime.timer = null;
  }
  runtime.intervalMs = 0;
  runtime.nextRunAtMs = 0;
  const config = loadModuleConfig(runtime.moduleContext || {});
  if (!config.enabled) return;
  const intervalMs = config.pollIntervalSeconds * 1000;
  runtime.intervalMs = intervalMs;
  runtime.nextRunAtMs = Date.now() + intervalMs;
  runtime.timer = setInterval(() => {
    runtime.nextRunAtMs = Date.now() + intervalMs;
    void runSweep('interval');
  }, intervalMs);
  if (typeof runtime.timer.unref === 'function') {
    runtime.timer.unref();
  }
}

function withRuntime(ctx, moduleContext) {
  runtime.ctx = ctx;
  runtime.moduleContext = moduleContext;
}

export function register(app, ctx, moduleContext = {}) {
  withRuntime(ctx, moduleContext);
  ensureDataFilesLocation(moduleContext);
  ensureSweeparrAppEnabled(ctx);
  const requireUser = typeof ctx.requireUser === 'function'
    ? ctx.requireUser
    : (_req, _res, next) => next();
  const requireAdmin = typeof ctx.requireAdmin === 'function'
    ? ctx.requireAdmin
    : requireUser;

  app.get('/sweeparr', requireAdmin, (req, res) => {
    const embed = String(req.query?.embed || '').trim().toLowerCase();
    if (embed !== '1' && embed !== 'true' && embed !== 'yes') {
      res.redirect('/apps/sweeparr');
      return;
    }
    const uiPath = getUiPath(moduleContext);
    if (!fs.existsSync(uiPath)) {
      res.status(404).type('text/plain').send('Sweeparr UI file not found.');
      return;
    }
    res.sendFile(uiPath);
  });

  app.get('/api/sweeparr/health', requireUser, (_req, res) => {
    res.json({
      ok: true,
      module: moduleContext.id || 'sweeparr',
      timestamp: nowIso(),
    });
  });

  app.get('/api/sweeparr/clients', requireAdmin, (_req, res) => {
    const clients = sanitizeClientsForResponse(resolveLauncharrClients(ctx));
    const supportedArrClients = clients.arrClients.filter((client) => ['radarr', 'sonarr'].includes(String(client.baseId || '').trim().toLowerCase()));
    res.json({
      module: moduleContext.id || 'sweeparr',
      arrClients: supportedArrClients,
      downloaderClients: clients.downloaderClients,
      counts: {
        arrClients: supportedArrClients.length,
        downloaderClients: clients.downloaderClients.length,
      },
    });
  });

  app.get('/api/sweeparr/config', requireAdmin, (_req, res) => {
    const config = loadModuleConfig(moduleContext);
    res.json({ module: moduleContext.id || 'sweeparr', config });
  });

  app.get('/api/sweeparr/aggression-levels', requireAdmin, (_req, res) => {
    const config = loadModuleConfig(moduleContext);
    res.json({
      module: moduleContext.id || 'sweeparr',
      currentLevel: normalizeAggressionLevel(config.aggressionLevel, DEFAULT_AGGRESSION_LEVEL),
      ...getAggressionPresetsResponse(),
    });
  });

  app.post('/api/sweeparr/config', requireAdmin, (req, res) => {
    const patch = req.body && typeof req.body === 'object' && !Array.isArray(req.body) ? req.body : {};
    const current = loadModuleConfig(moduleContext);
    const hasAggressionLevel = Object.prototype.hasOwnProperty.call(patch, 'aggressionLevel');
    const rawAggressionLevel = String(patch.aggressionLevel || '').trim().toLowerCase();
    if (hasAggressionLevel && !AGGRESSION_LEVELS.has(rawAggressionLevel)) {
      res.status(400).json({
        ok: false,
        module: moduleContext.id || 'sweeparr',
        error: 'Invalid aggressionLevel. Use one of: low, medium, high.',
      });
      return;
    }

    const applyAggressionDefaults = hasAggressionLevel
      ? toBoolean(patch.applyAggressionDefaults, true)
      : false;

    let baseConfig = current;
    if (hasAggressionLevel && applyAggressionDefaults) {
      baseConfig = deepMerge(baseConfig, buildAggressionPresetPatch(rawAggressionLevel));
    }

    const sanitizedPatch = { ...patch };
    delete sanitizedPatch.applyAggressionDefaults;
    const merged = deepMerge(baseConfig, sanitizedPatch);
    if (hasAggressionLevel) merged.aggressionLevel = rawAggressionLevel;
    const saved = saveModuleConfig(moduleContext, merged);
    scheduleSweepLoop();
    res.json({ ok: true, module: moduleContext.id || 'sweeparr', config: saved });
  });

  app.get('/api/sweeparr/events', requireAdmin, (req, res) => {
    const limit = toPositiveInt(req.query.limit, 100, 1, 500);
    const runId = String(req.query.runId || '').trim();
    const types = String(req.query.types || '')
      .split(',')
      .map((part) => String(part || '').trim())
      .filter(Boolean);
    res.json({
      module: moduleContext.id || 'sweeparr',
      runId,
      events: listModuleEvents(moduleContext, { limit, runId, types }),
    });
  });

  app.get('/api/sweeparr/removals', requireAdmin, (req, res) => {
    const limit = toPositiveInt(req.query.limit, 100, 1, 500);
    const runId = String(req.query.runId || '').trim();
    const removals = listModuleRemovals(moduleContext, { limit, runId });
    res.json({
      module: moduleContext.id || 'sweeparr',
      runId,
      removals,
      count: removals.length,
    });
  });

  app.post('/api/sweeparr/privacy-override', requireAdmin, (req, res) => {
    const body = req.body && typeof req.body === 'object' && !Array.isArray(req.body) ? req.body : {};
    const trackerHost = normalizeTrackerHost(body.tracker);
    if (!trackerHost) {
      res.status(400).json({
        ok: false,
        module: moduleContext.id || 'sweeparr',
        error: 'tracker is required (hostname or tracker URL).',
      });
      return;
    }

    const privacyInput = String(body.privacy || '').trim().toLowerCase();
    if (!['public', 'private', 'auto', ''].includes(privacyInput)) {
      res.status(400).json({
        ok: false,
        module: moduleContext.id || 'sweeparr',
        error: 'privacy must be public, private, or auto.',
      });
      return;
    }

    const current = loadModuleConfig(moduleContext);
    const overrides = sanitizeTrackerPrivacyOverrides(current.trackerPrivacyOverrides || {});
    if (privacyInput === 'public' || privacyInput === 'private') {
      overrides[trackerHost] = privacyInput;
    } else {
      delete overrides[trackerHost];
    }

    const saved = saveModuleConfig(moduleContext, {
      ...current,
      trackerPrivacyOverrides: overrides,
    });

    res.json({
      ok: true,
      module: moduleContext.id || 'sweeparr',
      tracker: trackerHost,
      privacy: String(saved.trackerPrivacyOverrides?.[trackerHost] || 'auto'),
      trackerPrivacyOverrides: saved.trackerPrivacyOverrides || {},
    });
  });

  app.get('/api/sweeparr/items', requireAdmin, async (_req, res) => {
    try {
      const payload = await listDownloaderItemsWithStrikes(ctx, moduleContext);
      res.json({
        ok: true,
        module: moduleContext.id || 'sweeparr',
        ...payload,
      });
    } catch (err) {
      res.status(500).json({
        ok: false,
        module: moduleContext.id || 'sweeparr',
        error: safeMessage(ctx, err, 'Failed to load downloader items.'),
        items: [],
        errors: [],
        counts: {
          downloaders: 0,
          items: 0,
          downloading: 0,
          erroredClients: 0,
        },
      });
    }
  });

  app.get('/api/sweeparr/status', requireAdmin, (_req, res) => {
    const config = loadModuleConfig(moduleContext);
    const clients = sanitizeClientsForResponse(resolveLauncharrClients(ctx));
    const supportedArrClients = clients.arrClients.filter((client) => ['radarr', 'sonarr'].includes(String(client.baseId || '').trim().toLowerCase()));
    const nowMs = Date.now();
    const nextRunAt = runtime.nextRunAtMs > 0 ? new Date(runtime.nextRunAtMs).toISOString() : '';
    const nextRunInSeconds = runtime.nextRunAtMs > 0
      ? Math.max(0, Math.ceil((runtime.nextRunAtMs - nowMs) / 1000))
      : null;
    res.json({
      module: moduleContext.id || 'sweeparr',
      running: runtime.running,
      lastRunAt: runtime.lastRunAt,
      lastError: runtime.lastError,
      lastSummary: runtime.lastSummary,
      scheduler: {
        active: Boolean(runtime.timer) && Boolean(config.enabled),
        intervalSeconds: Math.max(0, Math.round((runtime.intervalMs || 0) / 1000)),
        nextRunAt,
        nextRunInSeconds,
      },
      config,
      counts: {
        arrClients: supportedArrClients.length,
        downloaderClients: clients.downloaderClients.length,
      },
    });
  });

  app.post('/api/sweeparr/run-once', requireAdmin, async (req, res) => {
    const body = req.body && typeof req.body === 'object' && !Array.isArray(req.body) ? req.body : {};
    const mode = String(body.mode || '').trim().toLowerCase();
    const hasBodyDryRun = Object.prototype.hasOwnProperty.call(body, 'dryRun');
    const bodyDryRun = hasBodyDryRun ? Boolean(body.dryRun) : null;
    const dryRunOverride = mode === 'dry'
      ? true
      : (mode === 'live'
        ? false
        : bodyDryRun);
    const trigger = dryRunOverride === null
      ? 'manual'
      : (dryRunOverride ? 'manual.dry' : 'manual.live');
    const summary = await runSweep(trigger, dryRunOverride === null ? {} : { dryRunOverride });
    res.json(summary);
  });

  if (typeof ctx.pushLog === 'function') {
    ctx.pushLog({
      level: 'info',
      app: 'sweeparr',
      action: 'module.register',
      message: `Registered local module "${moduleContext.id || 'sweeparr'}".`,
    });
  }
}

export async function start(ctx, moduleContext = {}) {
  withRuntime(ctx, moduleContext);
  ensureDataFilesLocation(moduleContext);
  ensureDir(getDataDir(moduleContext));
  const config = loadModuleConfig(moduleContext);
  const statePath = getStatePath(moduleContext);
  const eventsPath = getEventsPath(moduleContext);
  if (!fs.existsSync(statePath)) saveModuleState(moduleContext, { items: {}, lastRunAt: '', lastSummary: null });
  if (!fs.existsSync(eventsPath)) writeJsonAtomic(eventsPath, []);

  scheduleSweepLoop();
  if (config.enabled && config.runOnStartup) {
    await runSweep('startup');
  }
}
