(() => {
  const body = document.body;
  const root = document.documentElement;
  if (root && root.dataset && root.dataset.launcharrEmbed === '1') return;
  const isSupportedPage = Boolean(
    body
      && root
      && (
        body.classList.contains('dash-body')
        || body.classList.contains('login-body')
        || body.classList.contains('landing-body')
      )
  );
  if (!isSupportedPage) return;

  const prefersReducedMotion = window.matchMedia
    ? window.matchMedia('(prefers-reduced-motion: reduce)')
    : null;
  const mobileMotionQuery = window.matchMedia
    ? window.matchMedia('(max-width: 980px)')
    : null;

  let canvas = document.getElementById('dashStarfieldCanvas');
  if (!canvas) {
    canvas = document.createElement('canvas');
    canvas.id = 'dashStarfieldCanvas';
    canvas.className = 'dash-starfield-canvas';
    canvas.setAttribute('aria-hidden', 'true');
    body.prepend(canvas);
  }

  const ctx = canvas.getContext('2d', { alpha: true });
  if (!ctx) return;

  body.classList.add('starfield-3d');

  const clamp = (value, min, max) => Math.min(max, Math.max(min, value));
  const parseCssNumber = (name, fallback) => {
    const raw = getComputedStyle(root).getPropertyValue(name).trim();
    const parsed = Number.parseFloat(raw.replace(/[^0-9.+-]/g, ''));
    return Number.isFinite(parsed) ? parsed : fallback;
  };

  let width = 0;
  let height = 0;
  let centerX = 0;
  let centerY = 0;
  let depth = 0;
  let fov = 0;
  let dpr = 1;
  let notes = [];
  let noteCount = 0;
  let rafId = 0;
  let lastTs = 0;
  let lastSettingsTs = 0;

  const state = {
    motionEnabled: true,
    size: 1.2,
    speedSec: 45,
    speedFactor: 1,
    color: { r: 28, g: 198, b: 194 },
  };

  const NOTE_GLYPHS = ['\u2669', '\u266a', '\u266b', '\u266c'];

  function densityToCount(density) {
    // Text rendering is heavier than dots, so keep the field slightly sparser.
    const count = Math.round(24000 / clamp(density, 20, 220));
    return clamp(count, 120, 980);
  }

  function speedToFactor(speedSeconds) {
    // Smaller slider value = faster travel.
    const t = (clamp(speedSeconds, 8, 60) - 8) / (60 - 8);
    return 3.1 - (t * 2.65);
  }

  function pickColor() {
    const raw = getComputedStyle(root).getPropertyValue('--brand-rgb').trim();
    const parts = raw.split(',').map((item) => Number.parseInt(item.trim(), 10));
    if (parts.length >= 3 && parts.every((n) => Number.isFinite(n))) {
      return {
        r: clamp(parts[0], 0, 255),
        g: clamp(parts[1], 0, 255),
        b: clamp(parts[2], 0, 255),
      };
    }
    return { r: 28, g: 198, b: 194 };
  }

  function pickNoteGlyph() {
    return NOTE_GLYPHS[Math.floor(Math.random() * NOTE_GLYPHS.length)];
  }

  function resetNote(note) {
    const spreadX = width * 1.15;
    const spreadY = height * 1.15;
    note.x = (Math.random() * 2 - 1) * spreadX;
    note.y = (Math.random() * 2 - 1) * spreadY;
    note.z = 1 + Math.random() * depth;
    note.speedMul = 0.5 + Math.random() * 1.6;
    note.alphaMul = 0.55 + Math.random() * 0.45;
    note.rotation = (Math.random() * 0.8) - 0.4;
    note.spin = (Math.random() * 0.02) - 0.01;
    note.glyph = pickNoteGlyph();
  }

  function buildNotes(count) {
    notes = Array.from({ length: count }, () => {
      const note = {
        x: 0, y: 0, z: 0,
        speedMul: 1,
        alphaMul: 1,
        rotation: 0,
        spin: 0,
        glyph: NOTE_GLYPHS[0],
      };
      resetNote(note);
      return note;
    });
  }

  function resizeCanvas() {
    dpr = clamp(window.devicePixelRatio || 1, 1, 2);
    width = Math.max(1, Math.floor(window.innerWidth));
    height = Math.max(1, Math.floor(window.innerHeight));
    centerX = width / 2;
    centerY = height / 2;
    depth = Math.max(700, Math.hypot(width, height) * 0.95);
    fov = Math.max(260, Math.min(width, height) * 0.58);

    canvas.width = Math.floor(width * dpr);
    canvas.height = Math.floor(height * dpr);
    canvas.style.width = `${width}px`;
    canvas.style.height = `${height}px`;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  }

  function readSettings(forceRebuild = false) {
    const density = clamp(parseCssNumber('--star-density', 165), 20, 220);
    const speedSec = clamp(parseCssNumber('--star-speed', 45), 8, 60);
    const size = clamp(parseCssNumber('--star-size', 1.2), 0.8, 3);
    const motionForcedOff = root.dataset.maximized === '1'
      || Boolean(mobileMotionQuery && mobileMotionQuery.matches);
    const motionAllowed = root.dataset.bgMotion !== '0'
      && !motionForcedOff
      && !(prefersReducedMotion && prefersReducedMotion.matches);
    const count = densityToCount(density);

    if (forceRebuild || count !== noteCount) {
      noteCount = count;
      buildNotes(noteCount);
    }

    state.motionEnabled = motionAllowed;
    state.speedSec = speedSec;
    state.speedFactor = speedToFactor(speedSec);
    state.size = size;
    state.color = pickColor();
  }

  function drawFrame(ts) {
    rafId = window.requestAnimationFrame(drawFrame);
    const dt = clamp(((ts - lastTs) || 16) / 1000, 0.001, 0.05);
    lastTs = ts;

    if ((ts - lastSettingsTs) > 220) {
      readSettings(false);
      lastSettingsTs = ts;
    }

    ctx.clearRect(0, 0, width, height);

    const { r, g, b } = state.color;
    const motionStep = state.motionEnabled ? (state.speedFactor * dt * 60) : 0;

    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';

    for (let index = 0; index < notes.length; index += 1) {
      const note = notes[index];
      note.rotation += note.spin * (state.motionEnabled ? 1 : 0.2);
      note.z -= motionStep * note.speedMul;
      if (note.z <= 1) {
        resetNote(note);
        continue;
      }

      const invZ = 1 / note.z;
      const px = centerX + (note.x * fov * invZ);
      const py = centerY + (note.y * fov * invZ);

      if (px < -80 || px > (width + 80) || py < -80 || py > (height + 80)) {
        resetNote(note);
        continue;
      }

      const depthNorm = 1 - (note.z / depth);
      const fontSize = Math.max(5, state.size * (5 + depthNorm * 24));
      const alpha = clamp((0.12 + depthNorm * 0.9) * note.alphaMul, 0.06, 1);

      ctx.save();
      ctx.translate(px, py);
      ctx.rotate(note.rotation);
      ctx.font = `700 ${fontSize}px "Segoe UI Symbol", "Apple Symbols", "Noto Sans Symbols", "Arial Unicode MS", sans-serif`;
      ctx.shadowColor = `rgba(${r},${g},${b},${alpha * 0.45})`;
      ctx.shadowBlur = Math.max(6, fontSize * 0.85);
      ctx.fillStyle = `rgba(${r},${g},${b},${alpha * 0.38})`;
      ctx.fillText(note.glyph, 0, 0);
      ctx.shadowBlur = Math.max(2, fontSize * 0.2);
      ctx.fillStyle = `rgba(255,255,255,${alpha})`;
      ctx.fillText(note.glyph, 0, 0);
      ctx.restore();
    }
  }

  function handleResize() {
    resizeCanvas();
    readSettings(true);
  }

  const mutationObserver = new MutationObserver(() => {
    readSettings(false);
  });

  mutationObserver.observe(root, {
    attributes: true,
    attributeFilter: ['data-bg-motion', 'data-maximized', 'style', 'data-brand-theme'],
  });

  if (prefersReducedMotion) {
    prefersReducedMotion.addEventListener('change', () => readSettings(false));
  }
  if (mobileMotionQuery) {
    if (typeof mobileMotionQuery.addEventListener === 'function') {
      mobileMotionQuery.addEventListener('change', () => readSettings(false));
    } else if (typeof mobileMotionQuery.addListener === 'function') {
      mobileMotionQuery.addListener(() => readSettings(false));
    }
  }

  window.addEventListener('resize', handleResize, { passive: true });
  window.addEventListener('orientationchange', handleResize, { passive: true });
  window.addEventListener('pagehide', () => {
    if (rafId) window.cancelAnimationFrame(rafId);
    mutationObserver.disconnect();
  }, { once: true });

  handleResize();
  drawFrame(performance.now());
})();
