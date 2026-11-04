// Toasts and confirm dialogs that match the Curatorr theme, replacing the
// browser's alert() and confirm().
//
//   CuratorrUI.toast('Saved.', { type: 'success' })
//   CuratorrUI.toast('Could not delete: …', { type: 'error' })
//   CuratorrUI.confirm('Delete "Mix"?', { confirmLabel: 'Delete', danger: true })
//     .then(function (ok) { if (ok) … })
//   CuratorrUI.flash('Playlist deleted.')   // shown as a toast after the next page load
//
// Forms can ask for confirmation declaratively:
//   <form data-confirm="Delete this user?" data-confirm-label="Delete" data-confirm-danger>
(function () {
  'use strict';

  var FLASH_KEY = 'curatorr-flash-toast';
  var region = null;

  function ensureRegion() {
    if (region && region.isConnected) return region;
    region = document.createElement('div');
    region.className = 'cur-toast-region';
    region.setAttribute('role', 'status');
    region.setAttribute('aria-live', 'polite');
    document.body.appendChild(region);
    return region;
  }

  function toast(message, options) {
    options = options || {};
    var type = options.type || 'info';
    var duration = options.duration != null ? options.duration : (type === 'error' ? 8000 : 4000);
    var el = document.createElement('div');
    el.className = 'cur-toast cur-toast--' + type;
    if (type === 'error') el.setAttribute('role', 'alert');

    var text = document.createElement('span');
    text.className = 'cur-toast-text';
    text.textContent = String(message || '');
    el.appendChild(text);

    var timer = null;
    function dismiss() {
      if (timer) clearTimeout(timer);
      el.classList.add('is-leaving');
      setTimeout(function () { if (el.parentNode) el.parentNode.removeChild(el); }, 200);
    }

    if (options.action && options.action.label && typeof options.action.onClick === 'function') {
      var actionBtn = document.createElement('button');
      actionBtn.type = 'button';
      actionBtn.className = 'cur-toast-action';
      actionBtn.textContent = options.action.label;
      actionBtn.addEventListener('click', function () {
        dismiss();
        options.action.onClick();
      });
      el.appendChild(actionBtn);
    }

    var closeBtn = document.createElement('button');
    closeBtn.type = 'button';
    closeBtn.className = 'cur-toast-close';
    closeBtn.setAttribute('aria-label', 'Dismiss');
    closeBtn.textContent = '✕';
    closeBtn.addEventListener('click', dismiss);
    el.appendChild(closeBtn);

    ensureRegion().appendChild(el);
    if (duration > 0) {
      timer = setTimeout(dismiss, duration);
      // Keep it on screen while the pointer or focus is on it.
      el.addEventListener('mouseenter', function () { if (timer) { clearTimeout(timer); timer = null; } });
      el.addEventListener('mouseleave', function () { if (!timer) timer = setTimeout(dismiss, 2500); });
    }
    return { dismiss: dismiss };
  }

  function flash(message, options) {
    try {
      sessionStorage.setItem(FLASH_KEY, JSON.stringify({ message: String(message || ''), type: (options && options.type) || 'success' }));
    } catch (e) { /* storage unavailable: skip the message */ }
  }

  function showPendingFlash() {
    var raw = null;
    try {
      raw = sessionStorage.getItem(FLASH_KEY);
      sessionStorage.removeItem(FLASH_KEY);
    } catch (e) { return; }
    if (!raw) return;
    try {
      var data = JSON.parse(raw);
      if (data && data.message) toast(data.message, { type: data.type || 'success' });
    } catch (e) { /* ignore malformed entry */ }
  }

  var confirmOpen = false;

  function confirmDialog(message, options) {
    options = options || {};
    return new Promise(function (resolve) {
      if (confirmOpen) { resolve(false); return; }
      confirmOpen = true;
      var previousFocus = document.activeElement;

      var backdrop = document.createElement('div');
      backdrop.className = 'cur-confirm-backdrop';

      var dialog = document.createElement('div');
      dialog.className = 'cur-confirm';
      dialog.setAttribute('role', 'alertdialog');
      dialog.setAttribute('aria-modal', 'true');

      var titleId = 'cur-confirm-title-' + Date.now();
      var bodyId = titleId + '-body';
      if (options.title) {
        var title = document.createElement('h2');
        title.className = 'cur-confirm-title';
        title.id = titleId;
        title.textContent = options.title;
        dialog.appendChild(title);
        dialog.setAttribute('aria-labelledby', titleId);
      }
      var body = document.createElement('div');
      body.className = 'cur-confirm-body';
      body.id = bodyId;
      String(message || '').split(/\n{2,}/).forEach(function (para) {
        var p = document.createElement('p');
        p.textContent = para.trim();
        body.appendChild(p);
      });
      dialog.appendChild(body);
      dialog.setAttribute('aria-describedby', bodyId);
      if (!options.title) dialog.setAttribute('aria-label', 'Confirm');

      var actions = document.createElement('div');
      actions.className = 'cur-confirm-actions';
      var cancelBtn = document.createElement('button');
      cancelBtn.type = 'button';
      cancelBtn.className = 'btn btn-ghost';
      cancelBtn.textContent = options.cancelLabel || 'Cancel';
      var okBtn = document.createElement('button');
      okBtn.type = 'button';
      okBtn.className = options.danger ? 'btn btn-danger' : 'btn btn-primary';
      okBtn.textContent = options.confirmLabel || 'OK';
      actions.appendChild(cancelBtn);
      actions.appendChild(okBtn);
      dialog.appendChild(actions);

      backdrop.appendChild(dialog);
      document.body.appendChild(backdrop);

      function finish(result) {
        document.removeEventListener('keydown', onKeydown, true);
        if (backdrop.parentNode) backdrop.parentNode.removeChild(backdrop);
        confirmOpen = false;
        if (previousFocus && previousFocus.isConnected && typeof previousFocus.focus === 'function') {
          try { previousFocus.focus({ preventScroll: true }); } catch (e) { previousFocus.focus(); }
        }
        resolve(result);
      }

      // Capture phase, so dialogs underneath (and their Escape handling) never
      // see keys while this is open.
      function onKeydown(event) {
        if (event.key === 'Escape') {
          event.preventDefault();
          event.stopPropagation();
          finish(false);
        } else if (event.key === 'Tab') {
          event.preventDefault();
          event.stopPropagation();
          (document.activeElement === okBtn ? cancelBtn : okBtn).focus();
        }
      }
      document.addEventListener('keydown', onKeydown, true);
      cancelBtn.addEventListener('click', function () { finish(false); });
      okBtn.addEventListener('click', function () { finish(true); });
      backdrop.addEventListener('click', function (event) { if (event.target === backdrop) finish(false); });

      // Destructive actions start on Cancel so Enter does not delete by accident.
      (options.danger ? cancelBtn : okBtn).focus();
    });
  }

  // <form data-confirm="…"> support for plain HTML forms.
  document.addEventListener('submit', function (event) {
    var form = event.target;
    if (!form || !form.hasAttribute || !form.hasAttribute('data-confirm')) return;
    if (form.__curConfirmed) { form.__curConfirmed = false; return; }
    event.preventDefault();
    var submitter = event.submitter || null;
    confirmDialog(form.getAttribute('data-confirm'), {
      confirmLabel: form.getAttribute('data-confirm-label') || 'OK',
      danger: form.hasAttribute('data-confirm-danger'),
    }).then(function (ok) {
      if (!ok) return;
      form.__curConfirmed = true;
      if (typeof form.requestSubmit === 'function') form.requestSubmit(submitter || undefined);
      else form.submit();
    });
  }, true);

  window.CuratorrUI = { toast: toast, confirm: confirmDialog, flash: flash };

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', showPendingFlash);
  else showPendingFlash();
})();
