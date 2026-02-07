(function whaleEasterEgg() {
  const mapEl = document.getElementById("ocean-map");
  if (!mapEl) {
    return;
  }

  if (window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches) {
    return;
  }

  const MAX_WHALES = 1;
  const FALLBACK_QUOTES = [
    "Call me Ishmael.",
    "It is not down on any map; true places never are.",
    "Better sleep with a sober cannibal than a drunken Christian.",
    "There is a wisdom that is woe; but there is a woe that is madness.",
    "To produce a mighty book, you must choose a mighty theme.",
  ];

  const layerEl = document.createElement("div");
  layerEl.className = "whale-easter-layer";
  layerEl.setAttribute("aria-hidden", "true");
  mapEl.appendChild(layerEl);

  const popupEl = document.createElement("div");
  popupEl.className = "whale-easter-popup";
  popupEl.setAttribute("aria-hidden", "true");
  popupEl.setAttribute("role", "dialog");
  popupEl.setAttribute("aria-modal", "true");
  layerEl.appendChild(popupEl);

  const popupCardEl = document.createElement("div");
  popupCardEl.className = "whale-easter-popup__card";
  const popupHeaderEl = document.createElement("div");
  popupHeaderEl.className = "whale-easter-popup__head";
  const popupTitleEl = document.createElement("p");
  popupTitleEl.className = "whale-easter-popup__title";
  popupTitleEl.textContent = "Moby-Dick";
  const popupCloseEl = document.createElement("button");
  popupCloseEl.className = "whale-easter-popup__close";
  popupCloseEl.type = "button";
  popupCloseEl.textContent = "Close";
  const popupQuoteEl = document.createElement("blockquote");
  popupQuoteEl.className = "whale-easter-popup__quote";
  popupHeaderEl.appendChild(popupTitleEl);
  popupHeaderEl.appendChild(popupCloseEl);
  popupCardEl.appendChild(popupHeaderEl);
  popupCardEl.appendChild(popupQuoteEl);
  popupEl.appendChild(popupCardEl);

  let quotes = FALLBACK_QUOTES.slice();
  let spawnTimer = null;
  const whaleTimers = new Map();

  function randomBetween(min, max) {
    return min + (Math.random() * (max - min));
  }

  function randomInt(min, max) {
    return Math.floor(randomBetween(min, max + 1));
  }

  function oneQuote() {
    if (!quotes.length) {
      return FALLBACK_QUOTES[randomInt(0, FALLBACK_QUOTES.length - 1)];
    }
    return quotes[randomInt(0, quotes.length - 1)];
  }

  function scheduleWhaleDrift(whaleEl, immediate = false) {
    if (!whaleEl.isConnected) {
      return;
    }
    const nextX = randomBetween(8, 92);
    const nextY = randomBetween(12, 86);
    const durationMs = randomInt(7000, 16500);
    whaleEl.style.setProperty("--whale-float-ms", `${durationMs}ms`);
    whaleEl.dataset.x = String(nextX);
    whaleEl.dataset.y = String(nextY);

    if (immediate) {
      whaleEl.style.left = `${nextX}%`;
      whaleEl.style.top = `${nextY}%`;
    } else {
      requestAnimationFrame(() => {
        whaleEl.style.left = `${nextX}%`;
        whaleEl.style.top = `${nextY}%`;
      });
    }

    const nextTimer = window.setTimeout(() => {
      scheduleWhaleDrift(whaleEl, false);
    }, durationMs + randomInt(120, 460));

    const timers = whaleTimers.get(whaleEl) || {};
    if (timers.driftTimer) {
      window.clearTimeout(timers.driftTimer);
    }
    whaleTimers.set(whaleEl, { ...timers, driftTimer: nextTimer });
  }

  function hidePopup() {
    popupEl.classList.remove("visible");
    popupEl.setAttribute("aria-hidden", "true");
  }

  function showQuotePopup() {
    const quote = String(oneQuote() || "").trim() || FALLBACK_QUOTES[0];
    popupQuoteEl.textContent = quote;
    popupEl.setAttribute("aria-hidden", "false");
    popupEl.classList.add("visible");
    popupCloseEl.focus({ preventScroll: true });
  }

  function renderHarpoonStrikeAt(xPct, yPct) {
    const rect = layerEl.getBoundingClientRect();
    if (!rect.width || !rect.height) {
      return;
    }
    const targetX = (xPct / 100) * rect.width;
    const targetY = (yPct / 100) * rect.height;

    const edge = randomInt(0, 3);
    let startX = 0;
    let startY = 0;
    if (edge === 0) {
      startX = -30;
      startY = randomBetween(0, rect.height);
    } else if (edge === 1) {
      startX = rect.width + 30;
      startY = randomBetween(0, rect.height);
    } else if (edge === 2) {
      startX = randomBetween(0, rect.width);
      startY = -22;
    } else {
      startX = randomBetween(0, rect.width);
      startY = rect.height + 22;
    }

    const dx = targetX - startX;
    const dy = targetY - startY;
    const length = Math.hypot(dx, dy);
    if (length < 1) {
      return;
    }
    const angleDeg = (Math.atan2(dy, dx) * 180) / Math.PI;

    const spearEl = document.createElement("div");
    spearEl.className = "whale-harpoon-strike";
    spearEl.style.left = `${startX}px`;
    spearEl.style.top = `${startY}px`;
    spearEl.style.width = `${length}px`;
    spearEl.style.setProperty("--harpoon-angle", `${angleDeg}deg`);
    layerEl.appendChild(spearEl);

    const impactEl = document.createElement("div");
    impactEl.className = "whale-harpoon-impact";
    impactEl.style.left = `${targetX}px`;
    impactEl.style.top = `${targetY}px`;
    layerEl.appendChild(impactEl);

    window.setTimeout(() => {
      spearEl.remove();
      impactEl.remove();
    }, 580);
  }

  function spawnHarpoonStrike(whaleEl) {
    const xPct = Number(whaleEl.dataset.x || 50);
    const yPct = Number(whaleEl.dataset.y || 50);
    renderHarpoonStrikeAt(xPct, yPct);
  }

  function clearWhale(whaleEl) {
    const timers = whaleTimers.get(whaleEl);
    if (timers?.driftTimer) {
      window.clearTimeout(timers.driftTimer);
    }
    if (timers?.despawnTimer) {
      window.clearTimeout(timers.despawnTimer);
    }
    whaleTimers.delete(whaleEl);
    whaleEl.remove();
  }

  function spawnWhale() {
    if (document.hidden) {
      return;
    }
    if (layerEl.querySelectorAll(".whale-easter").length >= MAX_WHALES) {
      return;
    }

    const whaleEl = document.createElement("button");
    whaleEl.type = "button";
    whaleEl.className = "whale-easter";
    whaleEl.style.setProperty("--whale-size", `${randomInt(42, 62)}px`);
    whaleEl.setAttribute("aria-label", "White whale easter egg quote");

    const glyphEl = document.createElement("span");
    glyphEl.className = "whale-easter__glyph";
    glyphEl.textContent = "🐋";

    const beaconEl = document.createElement("span");
    beaconEl.className = "whale-easter__beacon";
    beaconEl.textContent = "✦";

    whaleEl.appendChild(glyphEl);
    whaleEl.appendChild(beaconEl);
    layerEl.appendChild(whaleEl);

    whaleEl.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();
      spawnHarpoonStrike(whaleEl);
      whaleEl.classList.remove("is-clicked");
      void whaleEl.offsetWidth;
      whaleEl.classList.add("is-clicked");
      showQuotePopup();
      whaleEl.classList.add("is-dying");
      window.setTimeout(() => {
        clearWhale(whaleEl);
      }, 260);
    });

    scheduleWhaleDrift(whaleEl, true);

    const timers = whaleTimers.get(whaleEl) || {};
    const despawnTimer = window.setTimeout(() => {
      if (popupEl.classList.contains("visible")) {
        hidePopup();
      }
      clearWhale(whaleEl);
    }, randomInt(75000, 145000));
    whaleTimers.set(whaleEl, { ...timers, despawnTimer });
  }

  function scheduleSpawn(initial = false) {
    if (spawnTimer) {
      window.clearTimeout(spawnTimer);
    }
    const waitMs = initial ? randomInt(3500, 8500) : randomInt(18000, 38000);
    spawnTimer = window.setTimeout(() => {
      spawnWhale();
      scheduleSpawn(false);
    }, waitMs);
  }

  async function loadQuotes() {
    try {
      const resp = await fetch("/moby_quotes.json", { cache: "no-store" });
      if (!resp.ok) {
        return;
      }
      const parsed = await resp.json();
      if (!Array.isArray(parsed)) {
        return;
      }
      const next = parsed
        .map((entry) => String(entry || "").trim())
        .filter(Boolean);
      if (next.length) {
        quotes = next;
      }
    } catch (_error) {
      // Keep fallback quotes.
    }
  }

  document.addEventListener("visibilitychange", () => {
    if (document.hidden) {
      if (spawnTimer) {
        window.clearTimeout(spawnTimer);
        spawnTimer = null;
      }
      return;
    }
    if (!spawnTimer) {
      scheduleSpawn(false);
    }
  });

  popupEl.addEventListener("click", (event) => {
    if (event.target === popupEl) {
      hidePopup();
    }
  });

  popupCloseEl.addEventListener("click", () => {
    hidePopup();
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && popupEl.classList.contains("visible")) {
      hidePopup();
    }
  });

  loadQuotes();
  spawnWhale();
  scheduleSpawn(true);
})();
