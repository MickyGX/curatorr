// Keyboard behaviour shared by the .spm-overlay dialogs.
//
// Pages open and close these dialogs by toggling style.display, each with its
// own open/close function. Rather than wiring every one of them, this watches
// the overlays and adds the standard dialog behaviour:
//   - Escape closes the topmost open dialog through its close (✕) button, so
//     the page's own close logic (including any unsaved-changes check) runs.
//   - Tab and Shift+Tab stay inside the topmost open dialog.
//   - Opening moves focus into the dialog; closing returns it to whatever had
//     focus before.
(function () {
  'use strict';

  var OVERLAY_SELECTOR = '.spm-overlay';
  var FOCUSABLE = [
    'a[href]',
    'button:not([disabled])',
    'input:not([disabled]):not([type="hidden"])',
    'select:not([disabled])',
    'textarea:not([disabled])',
    '[tabindex]:not([tabindex="-1"])',
  ].join(',');

  var returnFocus = new WeakMap();
  var observed = new WeakSet();

  function isOpen(overlay) {
    if (!overlay || !overlay.isConnected) return false;
    return window.getComputedStyle(overlay).display !== 'none';
  }

  function openOverlays() {
    return Array.prototype.filter.call(document.querySelectorAll(OVERLAY_SELECTOR), isOpen);
  }

  function topmostOverlay() {
    var open = openOverlays();
    if (!open.length) return null;
    // Highest z-index wins; for ties, the later element in the DOM is on top.
    return open.reduce(function (top, overlay) {
      var topZ = parseInt(window.getComputedStyle(top).zIndex, 10) || 0;
      var z = parseInt(window.getComputedStyle(overlay).zIndex, 10) || 0;
      return z >= topZ ? overlay : top;
    });
  }

  function dialogOf(overlay) {
    return overlay.querySelector('[role="dialog"]') || overlay;
  }

  function focusableIn(container) {
    return Array.prototype.filter.call(container.querySelectorAll(FOCUSABLE), function (el) {
      return el.offsetWidth > 0 || el.offsetHeight > 0 || el.getClientRects().length > 0;
    });
  }

  function focusInto(overlay) {
    var dialog = dialogOf(overlay);
    if (dialog.contains(document.activeElement)) return;
    // Prefer the first form field, so typing can start straight away; fall
    // back to the first control, then the dialog itself.
    var items = focusableIn(dialog);
    var field = items.find(function (el) {
      return /^(INPUT|SELECT|TEXTAREA)$/.test(el.tagName) && !el.classList.contains('spm-close');
    });
    var target = field || items.find(function (el) { return !el.classList.contains('spm-close'); }) || items[0];
    if (!target) {
      if (!dialog.hasAttribute('tabindex')) dialog.setAttribute('tabindex', '-1');
      target = dialog;
    }
    try { target.focus({ preventScroll: true }); } catch (e) { target.focus(); }
  }

  function handleVisibilityChange(overlay, wasOpen) {
    var nowOpen = isOpen(overlay);
    if (nowOpen === wasOpen) return;
    if (nowOpen) {
      var active = document.activeElement;
      if (active && active !== document.body && !overlay.contains(active)) returnFocus.set(overlay, active);
      // Let the page finish rendering the dialog contents first.
      window.requestAnimationFrame(function () {
        if (isOpen(overlay)) focusInto(overlay);
      });
    } else {
      var previous = returnFocus.get(overlay);
      returnFocus.delete(overlay);
      if (previous && previous.isConnected && !openOverlays().length) {
        try { previous.focus({ preventScroll: true }); } catch (e) { previous.focus(); }
      }
    }
  }

  function observe(overlay) {
    if (observed.has(overlay)) return;
    observed.add(overlay);
    var wasOpen = isOpen(overlay);
    new MutationObserver(function () {
      var before = wasOpen;
      wasOpen = isOpen(overlay);
      handleVisibilityChange(overlay, before);
    }).observe(overlay, { attributes: true, attributeFilter: ['style', 'class', 'hidden'] });
  }

  function observeAll() {
    Array.prototype.forEach.call(document.querySelectorAll(OVERLAY_SELECTOR), observe);
  }

  document.addEventListener('keydown', function (event) {
    if (event.defaultPrevented) return;
    var overlay = topmostOverlay();
    if (!overlay) return;

    if (event.key === 'Escape') {
      var closeBtn = overlay.querySelector('.spm-close');
      if (!closeBtn) return;
      event.preventDefault();
      closeBtn.click();
      return;
    }

    if (event.key !== 'Tab') return;
    var dialog = dialogOf(overlay);
    var items = focusableIn(dialog);
    if (!items.length) {
      event.preventDefault();
      return;
    }
    var first = items[0];
    var last = items[items.length - 1];
    var active = document.activeElement;
    if (!dialog.contains(active)) {
      event.preventDefault();
      (event.shiftKey ? last : first).focus();
    } else if (event.shiftKey && active === first) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && active === last) {
      event.preventDefault();
      first.focus();
    }
  });

  // Watch straight away: this script is loaded after the dialog markup, and a
  // page's inline script can open a dialog before DOMContentLoaded (for
  // example the playlist editor opened from ?editGlobal=). The later calls
  // pick up any overlays added afterwards; observe() ignores repeats.
  observeAll();
  document.addEventListener('DOMContentLoaded', observeAll);
  window.addEventListener('load', observeAll);
})();
