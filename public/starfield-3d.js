(() => {
  const body = document.body;
  const root = document.documentElement;
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
  let stars = [];
  let starCount = 0;
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

  function densityToCount(density) {
    // Lower density value = many more stars.
    const count = Math.round(52000 / clamp(density, 20, 220));
    return clamp(count, 220, 2600);
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

  function resetStar(star) {
    const spreadX = width * 1.15;
    const spreadY = height * 1.15;
    star.x = (Math.random() * 2 - 1) * spreadX;
    star.y = (Math.random() * 2 - 1) * spreadY;
    star.z = 1 + Math.random() * depth;
    star.speedMul = 0.5 + Math.random() * 1.6;
    star.alphaMul = 0.55 + Math.random() * 0.45;
  }

  function buildStars(count) {
    stars = Array.from({ length: count }, () => {
      const star = { x: 0, y: 0, z: 0, speedMul: 1, alphaMul: 1 };
      resetStar(star);
      return star;
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

    if (forceRebuild || count !== starCount) {
      starCount = count;
      buildStars(starCount);
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

    for (let index = 0; index < stars.length; index += 1) {
      const star = stars[index];
      const previousZ = star.z;
      star.z -= motionStep * star.speedMul;
      if (star.z <= 1) {
        resetStar(star);
        continue;
      }

      const invZ = 1 / star.z;
      const px = centerX + (star.x * fov * invZ);
      const py = centerY + (star.y * fov * invZ);

      if (px < -80 || px > (width + 80) || py < -80 || py > (height + 80)) {
        resetStar(star);
        continue;
      }

      const depthNorm = 1 - (star.z / depth);
      const radius = Math.max(0.22, state.size * (0.2 + depthNorm * 2.2));
      const alpha = clamp((0.16 + depthNorm * 0.92) * star.alphaMul, 0.08, 1);

      // Soft glow.
      ctx.beginPath();
      ctx.fillStyle = `rgba(${r},${g},${b},${alpha * 0.24})`;
      ctx.arc(px, py, radius * 3.1, 0, Math.PI * 2);
      ctx.fill();

      // Core star.
      ctx.beginPath();
      ctx.fillStyle = `rgba(255,255,255,${alpha})`;
      ctx.arc(px, py, radius, 0, Math.PI * 2);
      ctx.fill();

      // Motion streak.
      if (state.motionEnabled) {
        const prevInvZ = 1 / previousZ;
        const lx = centerX + (star.x * fov * prevInvZ);
        const ly = centerY + (star.y * fov * prevInvZ);
        const lineDx = px - lx;
        const lineDy = py - ly;
        if ((lineDx * lineDx + lineDy * lineDy) > 0.35) {
          ctx.beginPath();
          ctx.strokeStyle = `rgba(255,255,255,${alpha * 0.35})`;
          ctx.lineWidth = Math.max(0.32, radius * 0.55);
          ctx.moveTo(lx, ly);
          ctx.lineTo(px, py);
          ctx.stroke();
        }
      }
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
