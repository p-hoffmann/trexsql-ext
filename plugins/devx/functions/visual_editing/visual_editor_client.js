/**
 * DevX Visual Editor Client
 * Injected into the user's app iframe via proxy.
 * Handles live style modifications, text editing, and computed style queries.
 */
(function () {
  "use strict";

  // Track elements with applied inline styles for reset
  const modifiedElements = new Map(); // devxId → { original styles }

  function findElementByDevxId(id) {
    return document.querySelector('[data-devx-id="' + CSS.escape(id) + '"]');
  }

  function applyStyles(devxId, styles) {
    const el = findElementByDevxId(devxId);
    if (!el) {
      window.parent.postMessage(
        { type: "devx-editor-error", error: "Element not found: " + devxId },
        "*"
      );
      return;
    }

    // Save original inline style on first modification
    if (!modifiedElements.has(devxId)) {
      modifiedElements.set(devxId, el.getAttribute("style") || "");
    }

    // Apply each style property
    for (var prop in styles) {
      if (Object.prototype.hasOwnProperty.call(styles, prop)) {
        el.style[prop] = styles[prop];
      }
    }

    window.parent.postMessage(
      { type: "devx-styles-applied", devxId: devxId },
      "*"
    );
  }

  function getStyles(devxId) {
    const el = findElementByDevxId(devxId);
    if (!el) {
      window.parent.postMessage(
        { type: "devx-editor-error", error: "Element not found: " + devxId },
        "*"
      );
      return;
    }

    const computed = window.getComputedStyle(el);
    window.parent.postMessage(
      {
        type: "devx-component-styles",
        devxId: devxId,
        styles: {
          marginTop: computed.marginTop,
          marginRight: computed.marginRight,
          marginBottom: computed.marginBottom,
          marginLeft: computed.marginLeft,
          paddingTop: computed.paddingTop,
          paddingRight: computed.paddingRight,
          paddingBottom: computed.paddingBottom,
          paddingLeft: computed.paddingLeft,
          borderWidth: computed.borderWidth,
          borderRadius: computed.borderRadius,
          borderColor: computed.borderColor,
          backgroundColor: computed.backgroundColor,
          fontSize: computed.fontSize,
          fontWeight: computed.fontWeight,
          color: computed.color,
        },
      },
      "*"
    );
  }

  function enableTextEditing(devxId) {
    const el = findElementByDevxId(devxId);
    if (!el) return;

    el.setAttribute("contenteditable", "true");
    el.focus();

    // On blur, finalize and remove contenteditable
    function onBlur() {
      el.removeAttribute("contenteditable");
      el.removeEventListener("blur", onBlur);
      window.parent.postMessage(
        {
          type: "devx-text-finalized",
          devxId: devxId,
          textContent: el.textContent || "",
        },
        "*"
      );
    }

    el.addEventListener("blur", onBlur);
  }

  function resetStyles(devxId) {
    if (devxId) {
      // Reset specific element
      const el = findElementByDevxId(devxId);
      if (el && modifiedElements.has(devxId)) {
        const original = modifiedElements.get(devxId);
        if (original) {
          el.setAttribute("style", original);
        } else {
          el.removeAttribute("style");
        }
        modifiedElements.delete(devxId);
      }
    } else {
      // Reset all modified elements
      modifiedElements.forEach(function (original, id) {
        const el = findElementByDevxId(id);
        if (el) {
          if (original) {
            el.setAttribute("style", original);
          } else {
            el.removeAttribute("style");
          }
        }
      });
      modifiedElements.clear();
    }

    window.parent.postMessage(
      { type: "devx-styles-reset", devxId: devxId || null },
      "*"
    );
  }

  // Listen for messages from parent (DevX) — only accept from parent window
  window.addEventListener("message", function (e) {
    if (!e.data || !e.data.type) return;
    // Only accept messages from our parent frame
    if (e.source !== window.parent) return;

    switch (e.data.type) {
      case "modify-devx-component-styles":
        applyStyles(e.data.devxId, e.data.styles);
        break;
      case "get-devx-component-styles":
        getStyles(e.data.devxId);
        break;
      case "enable-devx-text-editing":
        enableTextEditing(e.data.devxId);
        break;
      case "reset-devx-component-styles":
        resetStyles(e.data.devxId);
        break;
    }
  });
})();
