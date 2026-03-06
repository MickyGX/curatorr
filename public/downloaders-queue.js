(() => {
  const configs = resolveConfigs();
  if (!configs.length) return;

  configs.forEach((config) => {
    try {
      initQueue(config);
    } catch (err) {
      console.error('Downloader queue failed for', config?.appId || 'unknown', err);
    }
  });

  function resolveConfigs() {
    const list = [];
    const multi = Array.isArray(window.DOWNLOADER_QUEUE_CONFIGS)
      ? window.DOWNLOADER_QUEUE_CONFIGS
      : [];
    multi.forEach((entry) => {
      if (entry && entry.appId) list.push(entry);
    });

    const single = window.DOWNLOADER_QUEUE_CONFIG;
    if (single && single.appId) list.push(single);

    const dedupe = new Map();
    list.forEach((entry) => {
      const key = String(entry.appId || '').trim().toLowerCase();
      if (!key) return;
      const prefix = String(entry.prefix || entry.appId || '').trim() || key;
      dedupe.set(`${key}:${prefix}`, {
        appId: key,
        appName: String(entry.appName || key).trim() || key,
        prefix,
        sources: Array.isArray(entry.sources) ? entry.sources : [],
      });
    });

    return Array.from(dedupe.values());
  }

  function initQueue(config) {
    const appId = String(config.appId || '').trim().toLowerCase();
    if (!appId) return;

    const prefix = String(config.prefix || appId).trim() || appId;
    const appName = String(config.appName || appId).trim() || appId;
    const table = document.querySelector('#' + prefix + '-activity-queue .queue-table');
    const body = document.getElementById(prefix + 'QueueBody');
    if (!body) return;
    const typeFilter = document.getElementById(prefix + 'QueueTypeFilter');
    const statusFilter = document.getElementById(prefix + 'QueueStatusFilter');
    const logo = document.getElementById(prefix + 'Logo');
    const sortHeaders = Array.from(document.querySelectorAll('#' + prefix + '-activity-queue .queue-row.header > div'));
    const sources = Array.isArray(config.sources) ? config.sources : [];
    const isCombined = sources.length > 0;

    const state = {
      items: [],
      sortDir: 'asc',
      sortIndex: 0,
    };

    syncQueueTableLayout(table);

    typeFilter?.addEventListener('change', () => {
      if (isCombined && logo) {
        const selected = typeFilter.options[typeFilter.selectedIndex];
        const icon = selected?.getAttribute('data-icon') || logo.getAttribute('data-default-icon') || logo.src;
        if (icon) logo.src = icon;
      }
      applyFilters();
    });
    statusFilter?.addEventListener('change', applyFilters);
    if (sortHeaders.length) {
      sortHeaders.forEach((header, index) => {
        header.classList.add('queue-sortable');
        header.addEventListener('click', () => {
          if (state.sortIndex === index) {
            state.sortDir = state.sortDir === 'asc' ? 'desc' : 'asc';
          } else {
            state.sortIndex = index;
            state.sortDir = 'asc';
          }
          applyFilters();
        });
      });
    }
    loadQueue();

    async function loadQueue() {
      body.innerHTML = '<div class="queue-empty">Loading...</div>';

      try {
        const items = [];
        const targets = isCombined
          ? sources.map((source) => ({
            appId: String(source?.appId || '').trim().toLowerCase(),
            appName: String(source?.appName || source?.appId || '').trim(),
          })).filter((source) => source.appId)
          : [{ appId, appName }];

        for (let index = 0; index < targets.length; index += 1) {
          const target = targets[index];
          try {
            const response = await fetch(`/api/downloaders/${encodeURIComponent(target.appId)}/queue`, {
              headers: { Accept: 'application/json' },
            });
            if (!response.ok) throw new Error(`Request failed (${response.status})`);
            const payload = await response.json();
            const list = Array.isArray(payload?.items) ? payload.items : [];
            list.forEach((entry) => {
            const mapped = mapQueueItem(target.appId, entry, {
              combined: isCombined,
              sourceName: target.appName || target.appId,
              sourceId: target.appId,
            });
            if (mapped) items.push(mapped);
          });
        } catch (err) {
          console.warn('Downloader queue failed for', target.appId, err);
        }
      }

        state.items = items;
        if (isCombined && logo && typeFilter && typeFilter.value === 'all') {
          const defaultIcon = logo.getAttribute('data-default-icon');
          if (defaultIcon) logo.src = defaultIcon;
        }
        applyFilters();
      } catch (err) {
        body.innerHTML = '<div class="queue-empty">Unable to load ' + escapeHtml(appName) + ' queue.</div>';
      }
    }

    function applyFilters() {
      const typeValue = String(typeFilter?.value || 'all');
      const statusValue = String(statusFilter?.value || 'all');
      const filtered = state.items.filter((item) => {
        const typeOk = typeValue === 'all' || (isCombined ? item.sourceId === typeValue : item.kind === typeValue);
        const statusOk = statusValue === 'all' || (Array.isArray(item.statusKeys) ? item.statusKeys.includes(statusValue) : item.statusKey === statusValue);
        return typeOk && statusOk;
      });
      filtered.sort((a, b) => {
        const left = queueSortValue(a, state.sortIndex);
        const right = queueSortValue(b, state.sortIndex);
        if (left < right) return state.sortDir === 'asc' ? -1 : 1;
        if (left > right) return state.sortDir === 'asc' ? 1 : -1;
        return 0;
      });
      renderQueueRows(body, filtered, table);
    }

    function queueSortValue(item, index) {
      if (!item) return '';
      switch (index) {
        case 0:
          return String(item.title || '').toLowerCase();
        case 1:
          return String(item.episode || '').toLowerCase();
        case 2:
          return String(item.episodeTitle || '').toLowerCase();
        case 3:
          return String(item.quality || '').toLowerCase();
        case 4:
          return String(item.protocol || '').toLowerCase();
        case 5:
          return String(item.timeLeft || '').toLowerCase();
        case 6:
          return Number(item.progress || 0);
        default:
          return String(item.title || '').toLowerCase();
      }
    }
  }

  function mapQueueItem(appId, item, options = {}) {
    const baseId = resolveDownloaderBaseId(appId);
    if (baseId === 'transmission') return mapTransmissionItem(item, options);
    if (baseId === 'nzbget') return mapNzbgetItem(item, options);
    if (baseId === 'qbittorrent') return mapQbittorrentItem(item, options);
    if (baseId === 'sabnzbd') return mapSabnzbdItem(item, options);
    return null;
  }

  function resolveDownloaderBaseId(value) {
    const appId = String(value || '').trim().toLowerCase();
    if (!appId) return '';
    if (appId === 'transmission' || appId.startsWith('transmission-')) return 'transmission';
    if (appId === 'nzbget' || appId.startsWith('nzbget-')) return 'nzbget';
    if (appId === 'qbittorrent' || appId.startsWith('qbittorrent-')) return 'qbittorrent';
    if (appId === 'sabnzbd' || appId.startsWith('sabnzbd-')) return 'sabnzbd';
    return appId;
  }

  function mapTransmissionItem(item, options = {}) {
    const status = transmissionStatus(item);
    const statusKey = transmissionStatusKey(item);
    const statusKeys = transmissionStatusKeys(item, statusKey);
    const sizeBytes = toNumber(item?.sizeWhenDone || item?.totalSize || 0);
    const rateDownload = toNumber(item?.rateDownload || 0);
    const rateUpload = toNumber(item?.rateUpload || 0);
    const eta = toNumber(item?.eta);
    const percentDone = Number(item?.percentDone);
    const progress = Number.isFinite(percentDone)
      ? Math.max(0, Math.min(100, percentDone * 100))
      : progressFromSizes(sizeBytes, toNumber(item?.leftUntilDone || 0));

    const rateLabel = rateDownload > 0
      ? `DL ${formatBytes(rateDownload)}/s`
      : (rateUpload > 0 ? `UL ${formatBytes(rateUpload)}/s` : '-');

    const baseDetail = status;
    const baseSubDetail = rateLabel;
    const detail = options.combined
      ? String(options.sourceName || 'Transmission')
      : baseDetail;
    const subDetail = options.combined
      ? [baseDetail, baseSubDetail].filter(Boolean).join(' · ')
      : baseSubDetail;

    return {
      kind: 'torrent',
      title: String(item?.name || 'Unknown'),
      episode: detail,
      episodeTitle: subDetail,
      quality: sizeBytes ? formatBytes(sizeBytes) : '-',
      protocol: 'torrent',
      timeLeft: eta > 0 ? formatDuration(eta) : '-',
      progress,
      statusKey,
      statusKeys,
      sourceId: options.sourceId || '',
    };
  }

  function mapNzbgetItem(item, options = {}) {
    const title = String(item?.NZBName || item?.Filename || 'Unknown');
    const category = String(item?.Category || '').trim();
    const status = String(item?.Status || '').trim();
    const statusKey = nzbgetStatusKey(status);
    const statusKeys = statusKey ? [statusKey] : [];
    const sizeBytes = toNumber(item?.FileSizeMB || 0) * 1024 * 1024;
    const remainingBytes = toNumber(item?.RemainingSizeMB || 0) * 1024 * 1024;
    const downloadedBytes = toNumber(item?.DownloadedSizeMB || 0) * 1024 * 1024;
    const rate = toNumber(item?.DownloadRate || 0);
    const progress = sizeBytes > 0
      ? Math.max(0, Math.min(100, ((sizeBytes - remainingBytes) / sizeBytes) * 100))
      : progressFromSizes(sizeBytes, Math.max(0, sizeBytes - downloadedBytes));
    const timeLeft = rate > 0 && remainingBytes > 0 ? formatDuration(remainingBytes / rate) : '-';

    const baseDetail = category || 'Queue';
    const baseSubDetail = status || '-';
    const detail = options.combined
      ? String(options.sourceName || 'NZBGet')
      : baseDetail;
    const subDetail = options.combined
      ? [baseDetail, baseSubDetail].filter(Boolean).join(' · ')
      : baseSubDetail;

    return {
      kind: 'usenet',
      title,
      episode: detail,
      episodeTitle: subDetail,
      quality: sizeBytes ? formatBytes(sizeBytes) : '-',
      protocol: 'usenet',
      timeLeft,
      progress,
      statusKey,
      statusKeys,
      sourceId: options.sourceId || '',
    };
  }

  function mapQbittorrentItem(item, options = {}) {
    const status = qbittorrentStatus(item);
    const statusKey = qbittorrentStatusKey(item);
    const statusKeys = qbittorrentStatusKeys(statusKey);
    const sizeBytes = toNumber(item?.total_size || item?.size || item?.amount_left || 0);
    const amountLeft = toNumber(item?.amount_left || 0);
    const progressRaw = Number(item?.progress);
    const progress = Number.isFinite(progressRaw)
      ? Math.max(0, Math.min(100, progressRaw * 100))
      : progressFromSizes(sizeBytes, amountLeft);
    const eta = toNumber(item?.eta || 0);
    const dlSpeed = toNumber(item?.dlspeed || 0);
    const upSpeed = toNumber(item?.upspeed || 0);
    const rateLabel = dlSpeed > 0
      ? `DL ${formatBytes(dlSpeed)}/s`
      : (upSpeed > 0 ? `UL ${formatBytes(upSpeed)}/s` : '-');

    const baseDetail = status;
    const baseSubDetail = rateLabel;
    const detail = options.combined
      ? String(options.sourceName || 'qBittorrent')
      : baseDetail;
    const subDetail = options.combined
      ? [baseDetail, baseSubDetail].filter(Boolean).join(' · ')
      : baseSubDetail;
    const timeLeft = eta > 0 && eta < 8640000 ? formatDuration(eta) : '-';

    return {
      kind: 'torrent',
      title: String(item?.name || 'Unknown'),
      episode: detail,
      episodeTitle: subDetail,
      quality: sizeBytes ? formatBytes(sizeBytes) : '-',
      protocol: 'torrent',
      timeLeft,
      progress,
      statusKey,
      statusKeys,
      sourceId: options.sourceId || '',
    };
  }

  function mapSabnzbdItem(item, options = {}) {
    const title = String(item?.filename || item?.nzb_name || item?.name || 'Unknown');
    const category = String(item?.cat || item?.category || '').trim();
    const status = String(item?.status || item?.state || '').trim();
    const statusKey = sabnzbdStatusKey(status);
    const statusKeys = sabnzbdStatusKeys(statusKey);
    const sizeMB = toNumber(item?.mb || item?.size || 0);
    const leftMB = toNumber(item?.mbleft || item?.mb_left || 0);
    const sizeBytes = sizeMB > 0 ? sizeMB * 1024 * 1024 : 0;
    const remainingBytes = leftMB > 0 ? leftMB * 1024 * 1024 : 0;
    const percentage = toNumber(item?.percentage || item?.percent || 0);
    const progress = percentage > 0
      ? Math.max(0, Math.min(100, percentage))
      : progressFromSizes(sizeBytes, remainingBytes);
    const timeLeft = String(item?.timeleft || item?.time_left || '').trim() || '-';

    const baseDetail = category || 'Queue';
    const baseSubDetail = status || '-';
    const detail = options.combined
      ? String(options.sourceName || 'SABnzbd')
      : baseDetail;
    const subDetail = options.combined
      ? [baseDetail, baseSubDetail].filter(Boolean).join(' · ')
      : baseSubDetail;

    return {
      kind: 'usenet',
      title,
      episode: detail,
      episodeTitle: subDetail,
      quality: sizeBytes ? formatBytes(sizeBytes) : '-',
      protocol: 'usenet',
      timeLeft,
      progress,
      statusKey,
      statusKeys,
      sourceId: options.sourceId || '',
    };
  }

  function transmissionStatus(item) {
    const status = Number(item?.status);
    const map = {
      0: 'Stopped',
      1: 'Check pending',
      2: 'Checking',
      3: 'Download pending',
      4: 'Downloading',
      5: 'Seed pending',
      6: 'Seeding',
    };
    if (Number.isFinite(status) && map[status]) return map[status];
    if (item?.isFinished) return 'Seeding';
    if (item?.isStalled) return 'Stalled';
    return 'Queued';
  }

  function transmissionStatusKey(item) {
    const status = Number(item?.status);
    if (item?.isStalled) return 'stalled';
    if (item?.isFinished) return 'seeding';
    if (Number.isFinite(status)) {
      if (status === 0) return 'stopped';
      if (status === 1 || status === 2) return 'checking';
      if (status === 3) return 'queued';
      if (status === 4) return 'downloading';
      if (status === 5 || status === 6) return 'seeding';
    }
    return 'queued';
  }

  function transmissionStatusKeys(item, primary) {
    const keys = new Set();
    if (primary) keys.add(primary);
    if (primary === 'downloading' || primary === 'seeding') keys.add('active');
    if (primary === 'queued' || primary === 'checking') keys.add('downloading');
    if (primary === 'stopped') keys.add('paused');
    if (item?.isFinished) {
      keys.add('finished');
      keys.add('completed');
    }
    const errorValue = Number(item?.error || 0);
    if (errorValue > 0 || String(item?.errorString || '').trim()) keys.add('error');
    return Array.from(keys);
  }

  function qbittorrentStatus(item) {
    const value = String(item?.state || '').trim().toLowerCase();
    if (!value) return 'Queued';
    if (value.includes('error') || value.includes('missing')) return 'Error';
    if (value.includes('pause')) return 'Paused';
    if (value.includes('meta') || value.includes('downloading') || value.includes('forceddl')) return 'Downloading';
    if (value.includes('upload') || value.includes('seed') || value.includes('forcedup')) return 'Seeding';
    if (value.includes('queue')) return 'Queued';
    if (value.includes('check') || value.includes('moving') || value.includes('stalled')) return 'Checking';
    if (value.includes('complete')) return 'Completed';
    return 'Queued';
  }

  function qbittorrentStatusKey(item) {
    const value = String(item?.state || '').trim().toLowerCase();
    if (!value) return 'queued';
    if (value.includes('error') || value.includes('missing')) return 'error';
    if (value.includes('pause')) return 'paused';
    if (value.includes('meta') || value.includes('downloading') || value.includes('forceddl')) return 'downloading';
    if (value.includes('upload') || value.includes('seed') || value.includes('forcedup')) return 'seeding';
    if (value.includes('queue')) return 'queued';
    if (value.includes('check') || value.includes('moving') || value.includes('stalled')) return 'checking';
    if (value.includes('complete')) return 'completed';
    return 'queued';
  }

  function qbittorrentStatusKeys(primary) {
    const keys = new Set();
    if (primary) keys.add(primary);
    if (primary === 'downloading' || primary === 'seeding') keys.add('active');
    if (primary === 'checking') keys.add('downloading');
    return Array.from(keys);
  }

  function nzbgetStatusKey(rawStatus) {
    const value = String(rawStatus || '').trim().toLowerCase();
    if (!value) return 'queued';
    if (value.includes('download')) return 'downloading';
    if (value.includes('pause')) return 'paused';
    if (value.includes('queued')) return 'queued';
    if (value.includes('check') || value.includes('repair') || value.includes('verify')) return 'checking';
    if (value.includes('complete') || value.includes('success')) return 'completed';
    if (value.includes('fail') || value.includes('error')) return 'error';
    return 'queued';
  }

  function sabnzbdStatusKey(rawStatus) {
    const value = String(rawStatus || '').trim().toLowerCase();
    if (!value) return 'queued';
    if (value.includes('download')) return 'downloading';
    if (value.includes('pause')) return 'paused';
    if (value.includes('queue')) return 'queued';
    if (value.includes('check') || value.includes('verify') || value.includes('repair')) return 'checking';
    if (value.includes('complete') || value.includes('finished') || value.includes('success')) return 'completed';
    if (value.includes('fail') || value.includes('error')) return 'error';
    return 'queued';
  }

  function sabnzbdStatusKeys(primary) {
    const keys = new Set();
    if (primary) keys.add(primary);
    if (primary === 'downloading') keys.add('active');
    return Array.from(keys);
  }

  function queueColumnVisibility(table) {
    if (!table) {
      return {
        detail: true,
        subdetail: true,
        size: true,
        protocol: true,
        timeleft: true,
        progress: true,
      };
    }
    return {
      detail: !table.classList.contains('queue-hide-detail'),
      subdetail: !table.classList.contains('queue-hide-subdetail'),
      size: !table.classList.contains('queue-hide-size'),
      protocol: !table.classList.contains('queue-hide-protocol'),
      timeleft: !table.classList.contains('queue-hide-timeleft'),
      progress: !table.classList.contains('queue-hide-progress'),
    };
  }

  function buildQueueGridTemplate(visibility) {
    const columns = ['minmax(220px, 1fr)'];
    if (visibility.detail) columns.push('140px');
    if (visibility.subdetail) columns.push('160px');
    if (visibility.size) columns.push('130px');
    if (visibility.protocol) columns.push('116px');
    if (visibility.timeleft) columns.push('110px');
    if (visibility.progress) columns.push('170px');
    return columns.join(' ');
  }

  function setQueueColumnDisplay(table, selector, show) {
    if (!table) return;
    table.querySelectorAll(selector).forEach((cell) => {
      cell.style.display = show ? '' : 'none';
    });
  }

  function syncQueueTableLayout(table) {
    if (!table) return;
    const visibility = queueColumnVisibility(table);
    table.style.setProperty('--queue-grid-template', buildQueueGridTemplate(visibility));
    setQueueColumnDisplay(table, '.queue-col-detail', visibility.detail);
    setQueueColumnDisplay(table, '.queue-col-subdetail', visibility.subdetail);
    setQueueColumnDisplay(table, '.queue-col-size', visibility.size);
    setQueueColumnDisplay(table, '.queue-col-protocol', visibility.protocol);
    setQueueColumnDisplay(table, '.queue-col-time', visibility.timeleft);
    setQueueColumnDisplay(table, '.queue-col-progress', visibility.progress);
  }

  function renderQueueRows(body, items, table) {
    if (!items.length) {
      body.innerHTML = '<div class="queue-empty">No items in queue.</div>';
      syncQueueTableLayout(table);
      return;
    }

    body.innerHTML = items.map((item, index) => {
      const protocol = escapeHtml(item.protocol || '-');
      const quality = escapeHtml(item.quality || '-');
      const episode = escapeHtml(item.detail || item.episode || '-');
      const episodeTitle = escapeHtml(item.subDetail || item.episodeTitle || '-');
      const timeLeft = escapeHtml(item.timeLeft || '-');
      const progress = Math.max(0, Math.min(100, Math.round(Number(item.progress) || 0)));
      const protocolClass = item.protocol === 'usenet' ? ' usenet' : '';
      return (
        '<div class="queue-row" data-index="' + index + '">' +
          '<div class="queue-col-title">' + escapeHtml(item.title || 'Unknown') + '</div>' +
          '<div class="queue-col-detail queue-episode">' + episode + '</div>' +
          '<div class="queue-col-subdetail queue-ep-title">' + episodeTitle + '</div>' +
          '<div class="queue-col-size"><span class="queue-quality">' + quality + '</span></div>' +
          '<div class="queue-col-protocol queue-protocol' + protocolClass + '">' + protocol + '</div>' +
          '<div class="queue-col-time queue-time">' + timeLeft + '</div>' +
          '<div class="queue-col-progress queue-progress"><span style="width:' + progress + '%"></span></div>' +
        '</div>'
      );
    }).join('');
    syncQueueTableLayout(table);
  }

  function formatBytes(value) {
    const bytes = toNumber(value);
    if (!bytes) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
    let index = 0;
    let size = bytes;
    while (size >= 1024 && index < units.length - 1) {
      size /= 1024;
      index += 1;
    }
    const precision = size >= 10 || index === 0 ? 0 : 1;
    return size.toFixed(precision) + ' ' + units[index];
  }

  function formatDuration(value) {
    const seconds = Math.max(0, Math.round(toNumber(value)));
    if (!seconds) return '-';
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const secs = seconds % 60;
    if (hours > 0) return hours + 'h ' + minutes + 'm';
    if (minutes > 0) return minutes + 'm ' + secs + 's';
    return secs + 's';
  }

  function progressFromSizes(size, left) {
    const total = toNumber(size);
    const remaining = toNumber(left);
    if (total <= 0) return 0;
    return Math.max(0, Math.min(100, 100 - (remaining / total * 100)));
  }

  function toNumber(value) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  function escapeHtml(value) {
    return String(value || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }
})();
