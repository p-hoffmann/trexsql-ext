/**
 * DevX Component Selector Client
 * Injected into the user's app iframe via proxy.
 * Handles hover/click selection of components with data-devx-id attributes.
 * Supports two modes: single-select (toolbar) and multi-select (AI).
 */
(function () {
  "use strict";

  // === Single-select mode (existing toolbar flow) ===
  // State machine: inactive → inspecting → selected
  let state = "inactive";
  let overlayEl = null;
  let labelEl = null;
  let currentTarget = null;
  let selectedTarget = null;
  let rafId = null;

  // === Multi-select mode (AI select flow) ===
  let multiState = "inactive"; // inactive | inspecting
  let multiHoverOverlay = null;
  let multiHoverLabel = null;
  let multiCurrentTarget = null;
  let multiSelections = new Map(); // devxId → { el, overlay, label }
  let multiRafId = null;

  // Walk up DOM tree to find nearest element with data-devx-id
  function findDevxAncestor(el) {
    while (el && el !== document.documentElement) {
      if (el.getAttribute && el.getAttribute("data-devx-id")) {
        return el;
      }
      el = el.parentElement;
    }
    return null;
  }

  // === Single-select overlay helpers ===

  function createOverlay() {
    if (overlayEl) return;

    overlayEl = document.createElement("div");
    overlayEl.id = "__devx-selector-overlay";
    overlayEl.style.cssText =
      "position:fixed;pointer-events:none;border:2px solid #8b5cf6;background:rgba(139,92,246,0.08);z-index:2147483646;transition:all 0.1s ease;display:none;";

    labelEl = document.createElement("div");
    labelEl.id = "__devx-selector-label";
    labelEl.style.cssText =
      "position:fixed;pointer-events:none;z-index:2147483647;background:#8b5cf6;color:#fff;font-size:11px;font-family:system-ui,sans-serif;padding:2px 6px;border-radius:3px;white-space:nowrap;display:none;";

    document.body.appendChild(overlayEl);
    document.body.appendChild(labelEl);
  }

  function removeOverlay() {
    if (overlayEl) {
      overlayEl.remove();
      overlayEl = null;
    }
    if (labelEl) {
      labelEl.remove();
      labelEl = null;
    }
  }

  function positionOverlay(el) {
    if (!overlayEl || !labelEl || !el) return;
    const rect = el.getBoundingClientRect();

    overlayEl.style.top = rect.top + "px";
    overlayEl.style.left = rect.left + "px";
    overlayEl.style.width = rect.width + "px";
    overlayEl.style.height = rect.height + "px";
    overlayEl.style.display = "block";

    const name = el.getAttribute("data-devx-name") || el.tagName.toLowerCase();
    labelEl.textContent = name;
    labelEl.style.left = rect.left + "px";
    labelEl.style.top = Math.max(0, rect.top - 22) + "px";
    labelEl.style.display = "block";
  }

  function hideOverlay() {
    if (overlayEl) overlayEl.style.display = "none";
    if (labelEl) labelEl.style.display = "none";
  }

  // Reposition on scroll/resize via rAF
  function startRepositionLoop() {
    function update() {
      const target = state === "selected" ? selectedTarget : currentTarget;
      if (target) {
        positionOverlay(target);
      }
      rafId = requestAnimationFrame(update);
    }
    rafId = requestAnimationFrame(update);
  }

  function stopRepositionLoop() {
    if (rafId) {
      cancelAnimationFrame(rafId);
      rafId = null;
    }
  }

  function onMouseMove(e) {
    if (state !== "inspecting") return;
    const devxEl = findDevxAncestor(e.target);
    if (devxEl !== currentTarget) {
      currentTarget = devxEl;
      if (devxEl) {
        positionOverlay(devxEl);
      } else {
        hideOverlay();
      }
    }
  }

  function onClick(e) {
    if (state !== "inspecting") return;
    e.preventDefault();
    e.stopPropagation();
    e.stopImmediatePropagation();

    const devxEl = findDevxAncestor(e.target);
    if (!devxEl) return;

    state = "selected";
    selectedTarget = devxEl;
    currentTarget = null;

    const rect = devxEl.getBoundingClientRect();
    const devxId = devxEl.getAttribute("data-devx-id") || "";
    const devxName = devxEl.getAttribute("data-devx-name") || devxEl.tagName.toLowerCase();

    // Change overlay to selected style (solid border)
    if (overlayEl) {
      overlayEl.style.borderColor = "#7c3aed";
      overlayEl.style.borderWidth = "2px";
      overlayEl.style.background = "rgba(124,58,237,0.05)";
    }

    window.parent.postMessage(
      {
        type: "devx-component-selected",
        devxId: devxId,
        devxName: devxName,
        tagName: devxEl.tagName.toLowerCase(),
        boundingRect: {
          top: rect.top,
          left: rect.left,
          width: rect.width,
          height: rect.height,
        },
      },
      "*"
    );
  }

  function onKeyDown(e) {
    if (e.key === "Escape") {
      if (state === "selected") {
        state = "inspecting";
        selectedTarget = null;
        hideOverlay();
        if (overlayEl) {
          overlayEl.style.borderColor = "#8b5cf6";
          overlayEl.style.borderWidth = "2px";
          overlayEl.style.background = "rgba(139,92,246,0.08)";
        }
        window.parent.postMessage({ type: "devx-component-deselected" }, "*");
      } else if (state === "inspecting") {
        deactivate();
        window.parent.postMessage({ type: "devx-selector-closed" }, "*");
      }
    }
  }

  function activate() {
    if (state !== "inactive") return;
    state = "inspecting";
    createOverlay();
    document.addEventListener("mousemove", onMouseMove, true);
    document.addEventListener("click", onClick, true);
    document.addEventListener("keydown", onKeyDown, true);
    document.body.style.cursor = "crosshair";
    startRepositionLoop();
  }

  function deactivate() {
    state = "inactive";
    currentTarget = null;
    selectedTarget = null;
    document.removeEventListener("mousemove", onMouseMove, true);
    document.removeEventListener("click", onClick, true);
    document.removeEventListener("keydown", onKeyDown, true);
    document.body.style.cursor = "";
    stopRepositionLoop();
    removeOverlay();
  }

  // === Multi-select mode helpers ===

  function createMultiHoverOverlay() {
    if (multiHoverOverlay) return;

    multiHoverOverlay = document.createElement("div");
    multiHoverOverlay.id = "__devx-multi-hover-overlay";
    multiHoverOverlay.style.cssText =
      "position:fixed;pointer-events:none;border:2px dashed #6366f1;background:rgba(99,102,241,0.06);z-index:2147483644;transition:all 0.1s ease;display:none;";

    multiHoverLabel = document.createElement("div");
    multiHoverLabel.id = "__devx-multi-hover-label";
    multiHoverLabel.style.cssText =
      "position:fixed;pointer-events:none;z-index:2147483645;background:#6366f1;color:#fff;font-size:11px;font-family:system-ui,sans-serif;padding:2px 6px;border-radius:3px;white-space:nowrap;display:none;";

    document.body.appendChild(multiHoverOverlay);
    document.body.appendChild(multiHoverLabel);
  }

  function removeMultiHoverOverlay() {
    if (multiHoverOverlay) { multiHoverOverlay.remove(); multiHoverOverlay = null; }
    if (multiHoverLabel) { multiHoverLabel.remove(); multiHoverLabel = null; }
  }

  function positionMultiHoverOverlay(el) {
    if (!multiHoverOverlay || !multiHoverLabel || !el) return;
    const rect = el.getBoundingClientRect();
    multiHoverOverlay.style.top = rect.top + "px";
    multiHoverOverlay.style.left = rect.left + "px";
    multiHoverOverlay.style.width = rect.width + "px";
    multiHoverOverlay.style.height = rect.height + "px";
    multiHoverOverlay.style.display = "block";

    const name = el.getAttribute("data-devx-name") || el.tagName.toLowerCase();
    const devxId = el.getAttribute("data-devx-id") || "";
    const isSelected = multiSelections.has(devxId);
    multiHoverLabel.textContent = isSelected ? name + " ✓" : name;
    multiHoverLabel.style.left = rect.left + "px";
    multiHoverLabel.style.top = Math.max(0, rect.top - 22) + "px";
    multiHoverLabel.style.display = "block";
  }

  function hideMultiHoverOverlay() {
    if (multiHoverOverlay) multiHoverOverlay.style.display = "none";
    if (multiHoverLabel) multiHoverLabel.style.display = "none";
  }

  function createSelectionOverlay(el, devxId) {
    const overlay = document.createElement("div");
    overlay.className = "__devx-multi-selection-overlay";
    overlay.style.cssText =
      "position:fixed;pointer-events:none;border:2px solid #4f46e5;background:rgba(79,70,229,0.08);z-index:2147483643;";

    const label = document.createElement("div");
    label.className = "__devx-multi-selection-label";
    label.style.cssText =
      "position:fixed;pointer-events:none;z-index:2147483644;background:#4f46e5;color:#fff;font-size:10px;font-family:system-ui,sans-serif;padding:1px 5px;border-radius:3px;white-space:nowrap;";

    const name = el.getAttribute("data-devx-name") || el.tagName.toLowerCase();
    label.textContent = name;

    document.body.appendChild(overlay);
    document.body.appendChild(label);

    return { el, overlay, label, devxId };
  }

  function positionSelectionOverlay(entry) {
    const rect = entry.el.getBoundingClientRect();
    entry.overlay.style.top = rect.top + "px";
    entry.overlay.style.left = rect.left + "px";
    entry.overlay.style.width = rect.width + "px";
    entry.overlay.style.height = rect.height + "px";

    entry.label.style.left = rect.left + "px";
    entry.label.style.top = Math.max(0, rect.top - 20) + "px";
  }

  function removeSelectionOverlay(entry) {
    entry.overlay.remove();
    entry.label.remove();
  }

  function clearAllSelections() {
    for (const entry of multiSelections.values()) {
      removeSelectionOverlay(entry);
    }
    multiSelections.clear();
  }

  function sendMultiSelectionUpdate() {
    const components = [];
    for (const [devxId, entry] of multiSelections) {
      const name = entry.el.getAttribute("data-devx-name") || entry.el.tagName.toLowerCase();
      components.push({
        devxId: devxId,
        devxName: name,
        tagName: entry.el.tagName.toLowerCase(),
      });
    }
    window.parent.postMessage(
      { type: "devx-components-selected", components: components },
      "*"
    );
  }

  function startMultiRepositionLoop() {
    function update() {
      if (multiCurrentTarget) {
        positionMultiHoverOverlay(multiCurrentTarget);
      }
      for (const entry of multiSelections.values()) {
        positionSelectionOverlay(entry);
      }
      multiRafId = requestAnimationFrame(update);
    }
    multiRafId = requestAnimationFrame(update);
  }

  function stopMultiRepositionLoop() {
    if (multiRafId) {
      cancelAnimationFrame(multiRafId);
      multiRafId = null;
    }
  }

  function onMultiMouseMove(e) {
    if (multiState !== "inspecting") return;
    const devxEl = findDevxAncestor(e.target);
    if (devxEl !== multiCurrentTarget) {
      multiCurrentTarget = devxEl;
      if (devxEl) {
        positionMultiHoverOverlay(devxEl);
      } else {
        hideMultiHoverOverlay();
      }
    }
  }

  function onMultiClick(e) {
    if (multiState !== "inspecting") return;
    e.preventDefault();
    e.stopPropagation();
    e.stopImmediatePropagation();

    const devxEl = findDevxAncestor(e.target);
    if (!devxEl) return;

    const devxId = devxEl.getAttribute("data-devx-id") || "";
    if (!devxId) return;

    // Toggle selection
    if (multiSelections.has(devxId)) {
      removeSelectionOverlay(multiSelections.get(devxId));
      multiSelections.delete(devxId);
    } else {
      const entry = createSelectionOverlay(devxEl, devxId);
      positionSelectionOverlay(entry);
      multiSelections.set(devxId, entry);
    }

    sendMultiSelectionUpdate();
  }

  function onMultiKeyDown(e) {
    if (e.key === "Escape") {
      multiDeactivate();
      window.parent.postMessage({ type: "devx-multi-selector-closed" }, "*");
    }
  }

  function multiActivate() {
    if (multiState !== "inactive") return;
    multiState = "inspecting";
    createMultiHoverOverlay();
    document.addEventListener("mousemove", onMultiMouseMove, true);
    document.addEventListener("click", onMultiClick, true);
    document.addEventListener("keydown", onMultiKeyDown, true);
    document.body.style.cursor = "crosshair";
    startMultiRepositionLoop();
  }

  function multiDeactivate() {
    multiState = "inactive";
    multiCurrentTarget = null;
    document.removeEventListener("mousemove", onMultiMouseMove, true);
    document.removeEventListener("click", onMultiClick, true);
    document.removeEventListener("keydown", onMultiKeyDown, true);
    document.body.style.cursor = "";
    stopMultiRepositionLoop();
    removeMultiHoverOverlay();
    clearAllSelections();
  }

  function multiDeselectById(devxId) {
    if (multiSelections.has(devxId)) {
      removeSelectionOverlay(multiSelections.get(devxId));
      multiSelections.delete(devxId);
    }
  }

  // Listen for messages from parent (DevX) — only accept from parent window
  window.addEventListener("message", function (e) {
    if (!e.data || !e.data.type) return;
    // Only accept messages from our parent frame
    if (e.source !== window.parent) return;

    switch (e.data.type) {
      case "activate-devx-component-selector":
        activate();
        break;
      case "deactivate-devx-component-selector":
        deactivate();
        break;
      case "activate-devx-multi-selector":
        multiActivate();
        break;
      case "deactivate-devx-multi-selector":
        multiDeactivate();
        break;
      case "devx-deselect-component":
        multiDeselectById(e.data.devxId);
        sendMultiSelectionUpdate();
        break;
    }
  });
})();
