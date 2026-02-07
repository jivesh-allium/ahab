const state = {
  whales: [],
  alerts: [],
  events: [],
  filters: {},
  filtersMeta: {},
  seaState: {},
  generatedAt: 0,
};

const ui = {
  paused: false,
  controlsMounted: false,
  viewsMounted: false,
  selectedEventId: "",
  selectedRouteKey: "",
  expandedClusterId: "",
  feedMounted: false,
  mapMounted: false,
  initialUrlApplied: false,
  mapViewport: {
    scale: 1,
    x: 0,
    y: 0,
    dragging: false,
    pointerId: null,
    startClientX: 0,
    startClientY: 0,
    startX: 0,
    startY: 0,
    lastDragAtMs: 0,
  },
  savedViews: [],
  replayTimers: [],
  routeFilters: {
    exact: true,
    anchored: true,
    pseudo: false,
    unknown: false,
  },
};

const MAP_MIN_SCALE = 1;
const MAP_MAX_SCALE = 4.5;
const MAP_DRAG_CLICK_GUARD_MS = 220;
const MAP_ZOOM_STEP = 0.0018;

const mapEl = document.getElementById("marker-layer");
const arcLayerEl = document.getElementById("arc-layer");
const eventLayerEl = document.getElementById("event-layer");
const oceanMapEl = document.getElementById("ocean-map");
const mapViewportEl = document.getElementById("map-viewport");
const mapViewStateEl = document.getElementById("map-view-state");
const alertFeedEl = document.getElementById("alert-feed");
const mapPanelEl = document.querySelector(".map-panel");
const feedPanelEl = document.querySelector(".feed-panel");
const feedStatusEl = document.getElementById("feed-status");
const whaleCardsEl = document.getElementById("whale-cards");
const pollNowBtn = document.getElementById("poll-now");
const refreshGeoBtn = document.getElementById("refresh-geo");
const refreshBalancesBtn = document.getElementById("refresh-balances");
const mobyQuoteEl = document.getElementById("moby-quote");
const seaStateEl = document.getElementById("sea-state");
const seaScoreEl = document.getElementById("sea-score");
const typeFiltersEl = document.getElementById("type-filters");
const routeFiltersEl = document.getElementById("route-filters");
const chainFiltersEl = document.getElementById("chain-filters");
const minUsdEl = document.getElementById("min-usd");
const minUsdValueEl = document.getElementById("min-usd-value");
const windowSecondsEl = document.getElementById("window-seconds");
const windowSecondsValueEl = document.getElementById("window-seconds-value");
const pauseLiveEl = document.getElementById("pause-live");
const replayOffsetEl = document.getElementById("replay-offset");
const replayOffsetValueEl = document.getElementById("replay-offset-value");
const routeSummaryEl = document.getElementById("route-summary");
const focusEventEl = document.getElementById("focus-event");
const savedViewSelectEl = document.getElementById("saved-view-select");
const saveViewBtn = document.getElementById("save-view");
const deleteViewBtn = document.getElementById("delete-view");
const copyLinkBtn = document.getElementById("copy-link");

const ROUTE_FILTER_OPTIONS = [
  { value: "exact", label: "Geo", hint: "Mapped by Allium geo data." },
  { value: "anchored", label: "Anchored", hint: "Inferred near a mapped counterparty wallet." },
  { value: "pseudo", label: "Pseudo", hint: "Approximate fallback coordinate from address hash." },
  { value: "unknown", label: "Unknown", hint: "No location confidence available yet." },
];

const CHAIN_EXPLORERS = {
  ethereum: {
    name: "Etherscan",
    txPrefix: "https://etherscan.io/tx/",
    addressPrefix: "https://etherscan.io/address/",
  },
  arbitrum: {
    name: "Arbiscan",
    txPrefix: "https://arbiscan.io/tx/",
    addressPrefix: "https://arbiscan.io/address/",
  },
  optimism: {
    name: "Optimistic Etherscan",
    txPrefix: "https://optimistic.etherscan.io/tx/",
    addressPrefix: "https://optimistic.etherscan.io/address/",
  },
  base: {
    name: "Basescan",
    txPrefix: "https://basescan.org/tx/",
    addressPrefix: "https://basescan.org/address/",
  },
  polygon: {
    name: "Polygonscan",
    txPrefix: "https://polygonscan.com/tx/",
    addressPrefix: "https://polygonscan.com/address/",
  },
  avalanche: {
    name: "Snowtrace",
    txPrefix: "https://snowtrace.io/tx/",
    addressPrefix: "https://snowtrace.io/address/",
  },
  solana: {
    name: "Solscan",
    txPrefix: "https://solscan.io/tx/",
    addressPrefix: "https://solscan.io/account/",
  },
};

const FALLBACK_QUOTES = [
  "Call me Ishmael.",
  "There is a wisdom that is woe; but there is a woe that is madness.",
  "It is not down on any map; true places never are.",
  "A whale ship was my Yale College and my Harvard.",
  "Better sleep with a sober cannibal than a drunken Christian.",
  "I know not all that may be coming, but be it what it will, I'll go to it laughing.",
  "From hell's heart I stab at thee; for hate's sake I spit my last breath at thee.",
  "To produce a mighty book, you must choose a mighty theme.",
];

const SAVED_VIEWS_KEY = "pequod.savedViews.v1";

let mobyQuotes = [];
let lastQuoteIndex = -1;
let feedPanelSyncRaf = 0;

function syncFeedPanelHeight() {
  if (!mapPanelEl || !feedPanelEl) {
    return;
  }
  if (window.matchMedia("(max-width: 980px)").matches) {
    feedPanelEl.style.removeProperty("height");
    feedPanelEl.style.removeProperty("max-height");
    return;
  }
  const mapHeight = Math.ceil(mapPanelEl.getBoundingClientRect().height);
  if (!Number.isFinite(mapHeight) || mapHeight <= 0) {
    return;
  }
  const targetHeight = `${mapHeight}px`;
  if (feedPanelEl.style.height !== targetHeight) {
    feedPanelEl.style.height = targetHeight;
    feedPanelEl.style.maxHeight = targetHeight;
  }
}

function scheduleFeedPanelSync() {
  if (feedPanelSyncRaf) {
    return;
  }
  feedPanelSyncRaf = window.requestAnimationFrame(() => {
    feedPanelSyncRaf = 0;
    syncFeedPanelHeight();
  });
}

function shortAddr(address) {
  if (!address) return "unknown";
  if (address.length <= 14) return address;
  return `${address.slice(0, 8)}...${address.slice(-6)}`;
}

function normalizeChain(chain) {
  return String(chain || "").trim().toLowerCase();
}

function chainExplorer(chain) {
  const normalized = normalizeChain(chain);
  if (!normalized) return null;
  if (Object.prototype.hasOwnProperty.call(CHAIN_EXPLORERS, normalized)) {
    return CHAIN_EXPLORERS[normalized];
  }
  if (normalized.startsWith("sol")) {
    return CHAIN_EXPLORERS.solana;
  }
  if (normalized.startsWith("eth") || normalized.includes("evm")) {
    return CHAIN_EXPLORERS.ethereum;
  }
  return null;
}

function txExplorerUrl(chain, txId) {
  const explorer = chainExplorer(chain);
  const value = String(txId || "").trim();
  if (!explorer || !value) return "";
  return `${explorer.txPrefix}${encodeURIComponent(value)}`;
}

function addressExplorerUrl(chain, address) {
  const explorer = chainExplorer(chain);
  const value = String(address || "").trim();
  if (!explorer || !value) return "";
  return `${explorer.addressPrefix}${encodeURIComponent(value)}`;
}

function explorerName(chain) {
  const explorer = chainExplorer(chain);
  return explorer ? explorer.name : "Explorer";
}

function whaleLabelText(label, fallbackAddress = "") {
  const raw = String(label || "").trim();
  const cleaned = raw.replace(/^wake[_\-\s]+/i, "").trim();
  if (cleaned) return cleaned;
  return shortAddr(fallbackAddress);
}

function whaleLabelForDisplay(whale) {
  return whaleLabelText(whale?.label, whale?.address || "");
}

function formatUsd(value) {
  if (typeof value !== "number") return "n/a";
  return value.toLocaleString(undefined, { style: "currency", currency: "USD", maximumFractionDigits: 2 });
}

function formatFlowUsd(value) {
  if (!hasNumber(value) || value <= 0) {
    return "Unpriced";
  }
  return formatUsd(value);
}

function hasNumber(value) {
  return typeof value === "number" && Number.isFinite(value);
}

function formatLastUsd(value) {
  if (!hasNumber(value) || value <= 0) return "No priced sightings yet";
  return formatUsd(value);
}

function formatPortfolioUsd(value) {
  if (!hasNumber(value) || value <= 0) return "unpriced";
  return formatUsd(value);
}

function formatDuration(seconds) {
  const total = Math.max(0, Number(seconds) || 0);
  if (total < 60) {
    return `${total}s`;
  }
  if (total < 3600) {
    const minutes = Math.round(total / 60);
    return `${minutes}m`;
  }
  if (total < 86400) {
    const hours = Math.round(total / 3600);
    return `${hours}h`;
  }
  const days = Math.round(total / 86400);
  return `${days}d`;
}

function formatAgoFromTs(eventTs, nowTs = state.generatedAt) {
  if (!hasNumber(eventTs)) return "unknown";
  const ref = hasNumber(nowTs) ? nowTs : Math.floor(Date.now() / 1000);
  const delta = Math.max(0, Math.floor(ref - eventTs));
  return `${formatDuration(delta)} ago`;
}

function csvFromList(values) {
  return (values || [])
    .map((item) => String(item || "").trim().toLowerCase())
    .filter(Boolean)
    .join(",");
}

function listFromCsv(value) {
  return String(value || "")
    .split(",")
    .map((item) => item.trim().toLowerCase())
    .filter(Boolean);
}

function parseInitialUrlSpec() {
  const params = new URLSearchParams(window.location.search || "");
  const routes = new Set(listFromCsv(params.get("routes")));
  return {
    eventId: String(params.get("event") || ""),
    txId: String(params.get("tx") || ""),
    viewName: String(params.get("view") || ""),
    filters: {
      min_usd: params.get("min_usd") !== null ? Number(params.get("min_usd")) : null,
      window_seconds: params.get("window_seconds") !== null ? Number(params.get("window_seconds")) : null,
      replay_offset_seconds:
        params.get("replay_offset_seconds") !== null ? Number(params.get("replay_offset_seconds")) : null,
      types: listFromCsv(params.get("types")),
      chains: listFromCsv(params.get("chains")),
    },
    routeFilters:
      routes.size > 0
        ? {
            exact: routes.has("exact"),
            anchored: routes.has("anchored"),
            pseudo: routes.has("pseudo"),
            unknown: routes.has("unknown"),
          }
        : null,
  };
}

function currentRouteFilters() {
  return {
    exact: routeClassEnabled("exact"),
    anchored: routeClassEnabled("anchored"),
    pseudo: routeClassEnabled("pseudo"),
    unknown: routeClassEnabled("unknown"),
  };
}

function currentViewPayload(name = "") {
  return {
    name: String(name || "").trim(),
    saved_at: Math.floor(Date.now() / 1000),
    filters: {
      types: selectedFromChips(typeFiltersEl),
      chains: selectedFromChips(chainFiltersEl),
      min_usd: Number(minUsdEl.value || 0),
      window_seconds: Number(windowSecondsEl.value || 3600),
      replay_offset_seconds: Number(replayOffsetEl.value || 0),
    },
    route_filters: currentRouteFilters(),
  };
}

function buildShareUrl(eventId = "") {
  const params = new URLSearchParams();
  const filters = currentViewPayload("").filters;
  if (filters.types.length) params.set("types", csvFromList(filters.types));
  if (filters.chains.length) params.set("chains", csvFromList(filters.chains));
  if (filters.min_usd > 0) params.set("min_usd", String(filters.min_usd));
  if (filters.window_seconds && Number(filters.window_seconds) !== 3600) {
    params.set("window_seconds", String(filters.window_seconds));
  }
  if (filters.replay_offset_seconds > 0) {
    params.set("replay_offset_seconds", String(filters.replay_offset_seconds));
  }
  const enabledRoutes = Object.entries(currentRouteFilters())
    .filter((entry) => Boolean(entry[1]))
    .map((entry) => entry[0]);
  if (enabledRoutes.length !== ROUTE_FILTER_OPTIONS.length) {
    params.set("routes", enabledRoutes.join(","));
  }
  if (eventId) params.set("event", eventId);
  const query = params.toString();
  return `${window.location.origin}${window.location.pathname}${query ? `?${query}` : ""}`;
}

function syncLocationFromState(eventId = "") {
  const url = buildShareUrl(eventId);
  window.history.replaceState({}, "", url);
}

async function copyShareLink(eventId = "") {
  const link = buildShareUrl(eventId);
  try {
    if (navigator?.clipboard?.writeText) {
      await navigator.clipboard.writeText(link);
      return true;
    }
  } catch (_error) {
    return false;
  }
  return false;
}

function loadSavedViewsFromStorage() {
  try {
    const raw = window.localStorage.getItem(SAVED_VIEWS_KEY);
    if (!raw) {
      ui.savedViews = [];
      return;
    }
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      ui.savedViews = [];
      return;
    }
    ui.savedViews = parsed
      .filter((item) => item && typeof item === "object")
      .map((item) => ({
        name: String(item.name || "").trim(),
        saved_at: Number(item.saved_at || 0),
        filters: item.filters && typeof item.filters === "object" ? item.filters : {},
        route_filters: item.route_filters && typeof item.route_filters === "object" ? item.route_filters : {},
      }))
      .filter((item) => item.name);
  } catch (_error) {
    ui.savedViews = [];
  }
}

function persistSavedViews() {
  window.localStorage.setItem(SAVED_VIEWS_KEY, JSON.stringify(ui.savedViews.slice(0, 24)));
}

function renderSavedViewsSelect() {
  if (!savedViewSelectEl) {
    return;
  }
  const current = savedViewSelectEl.value;
  savedViewSelectEl.innerHTML = "";
  const placeholder = document.createElement("option");
  placeholder.value = "";
  placeholder.textContent = "Select saved view";
  savedViewSelectEl.appendChild(placeholder);
  const sorted = [...ui.savedViews].sort((a, b) => Number(b.saved_at || 0) - Number(a.saved_at || 0));
  for (const view of sorted) {
    const option = document.createElement("option");
    option.value = view.name;
    option.textContent = view.name;
    savedViewSelectEl.appendChild(option);
  }
  if (current && sorted.some((item) => item.name === current)) {
    savedViewSelectEl.value = current;
  } else {
    savedViewSelectEl.value = "";
  }
}

function formatTs(ts) {
  if (!ts) return "n/a";
  return new Date(ts * 1000).toLocaleString();
}

function routeGeoMeta(event) {
  const source = String(event?.source?.geo_source || "unknown").toLowerCase();
  const target = String(event?.target?.geo_source || "unknown").toLowerCase();
  if (source === "geo" && target === "geo") {
    return { cls: "exact", label: "Geo" };
  }
  if (source === "anchored" || target === "anchored") {
    return { cls: "anchored", label: "Anchored" };
  }
  if (source === "pseudo" || target === "pseudo") {
    return { cls: "pseudo", label: "Pseudo" };
  }
  return { cls: "unknown", label: "Unknown" };
}

function alertScoreOf(item) {
  const value = Number(item?.score);
  if (!Number.isFinite(value)) return 0;
  return Math.max(0, Math.min(100, value));
}

function topReasonLabel(item) {
  const reasons = Array.isArray(item?.score_reasons) ? item.score_reasons : [];
  const first = reasons.find((entry) => entry && typeof entry === "object");
  if (!first) return "";
  return String(first.label || first.key || "").trim();
}

function entityDisplay(row, fallbackAddress = "") {
  if (row && typeof row === "object") {
    const display = String(row.display_name || row.label || "").trim();
    if (display) return whaleLabelText(display, fallbackAddress);
  }
  return shortAddr(fallbackAddress);
}

function routeClassEnabled(routeClass) {
  return Boolean(ui.routeFilters[String(routeClass || "unknown")]);
}

function routeVisibleForEvent(event) {
  return routeClassEnabled(routeGeoMeta(event).cls);
}

function renderRouteFilterChips() {
  if (!routeFiltersEl) {
    return;
  }
  routeFiltersEl.innerHTML = "";
  for (const option of ROUTE_FILTER_OPTIONS) {
    const chip = document.createElement("button");
    const active = routeClassEnabled(option.value);
    chip.className = `chip ${active ? "active" : ""}`;
    chip.dataset.value = option.value;
    chip.textContent = option.label;
    chip.title = option.hint || option.label;
    routeFiltersEl.appendChild(chip);
  }
}

function renderRouteSummary(events) {
  if (!routeSummaryEl) {
    return;
  }
  const counts = { exact: 0, anchored: 0, pseudo: 0, unknown: 0 };
  for (const event of events || []) {
    const cls = routeGeoMeta(event).cls;
    counts[cls] = (counts[cls] || 0) + 1;
  }
  const hidden = ROUTE_FILTER_OPTIONS
    .filter((option) => !routeClassEnabled(option.value))
    .map((option) => option.label)
    .join(", ");
  routeSummaryEl.textContent = hidden
    ? `Routes: Geo ${counts.exact}, Anchored ${counts.anchored}, Pseudo ${counts.pseudo}, Unknown ${counts.unknown}. Hidden: ${hidden}.`
    : `Routes: Geo ${counts.exact}, Anchored ${counts.anchored}, Pseudo ${counts.pseudo}, Unknown ${counts.unknown}.`;
}

function lonToX(lon) {
  if (typeof lon !== "number") return 50;
  return ((lon + 180) / 360) * 100;
}

function latToY(lat) {
  if (typeof lat !== "number") return 50;
  return ((90 - lat) / 180) * 100;
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function mapPanBounds(scale = ui.mapViewport.scale) {
  if (!oceanMapEl) {
    return { minX: 0, maxX: 0, minY: 0, maxY: 0 };
  }
  const rect = oceanMapEl.getBoundingClientRect();
  const width = Number(rect.width || 0);
  const height = Number(rect.height || 0);
  if (width <= 0 || height <= 0 || scale <= 1.0001) {
    return { minX: 0, maxX: 0, minY: 0, maxY: 0 };
  }
  return {
    minX: width - width * scale,
    maxX: 0,
    minY: height - height * scale,
    maxY: 0,
  };
}

function clampMapPan(x, y, scale = ui.mapViewport.scale) {
  const bounds = mapPanBounds(scale);
  return {
    x: clamp(x, bounds.minX, bounds.maxX),
    y: clamp(y, bounds.minY, bounds.maxY),
  };
}

function applyMapViewportTransform() {
  if (!mapViewportEl || !oceanMapEl) {
    return;
  }
  const normalizedScale = clamp(ui.mapViewport.scale, MAP_MIN_SCALE, MAP_MAX_SCALE);
  const normalizedPan = clampMapPan(ui.mapViewport.x, ui.mapViewport.y, normalizedScale);
  ui.mapViewport.scale = normalizedScale;
  ui.mapViewport.x = normalizedPan.x;
  ui.mapViewport.y = normalizedPan.y;
  mapViewportEl.style.transform = `translate(${ui.mapViewport.x}px, ${ui.mapViewport.y}px) scale(${ui.mapViewport.scale})`;
  oceanMapEl.classList.toggle("zoomed", ui.mapViewport.scale > 1.0001);
  if (mapViewStateEl) {
    mapViewStateEl.textContent = `${ui.mapViewport.scale.toFixed(ui.mapViewport.scale >= 2 ? 2 : 1)}x`;
  }
}

function resetMapViewport() {
  ui.mapViewport.scale = 1;
  ui.mapViewport.x = 0;
  ui.mapViewport.y = 0;
  applyMapViewportTransform();
}

function zoomMapAtClientPoint(nextScale, clientX, clientY) {
  if (!oceanMapEl) {
    return;
  }
  const rect = oceanMapEl.getBoundingClientRect();
  const width = Number(rect.width || 0);
  const height = Number(rect.height || 0);
  if (width <= 0 || height <= 0) {
    return;
  }
  const scale = clamp(nextScale, MAP_MIN_SCALE, MAP_MAX_SCALE);
  const px = clamp(clientX - rect.left, 0, width);
  const py = clamp(clientY - rect.top, 0, height);
  const previousScale = ui.mapViewport.scale;
  const worldX = (px - ui.mapViewport.x) / previousScale;
  const worldY = (py - ui.mapViewport.y) / previousScale;
  const nextX = px - worldX * scale;
  const nextY = py - worldY * scale;
  const clamped = clampMapPan(nextX, nextY, scale);
  ui.mapViewport.scale = scale;
  ui.mapViewport.x = clamped.x;
  ui.mapViewport.y = clamped.y;
  applyMapViewportTransform();
}

function eventIdOf(event) {
  if (!event) return "";
  return String(event.event_id || event.dedupe_key || event.tx_id || "");
}

function markerSize(usd) {
  if (!usd || usd <= 0) return 26;
  const scaled = 20 + Math.log10(usd + 1) * 7;
  return Math.max(24, Math.min(64, scaled));
}

function quotesPool() {
  return mobyQuotes.length ? mobyQuotes : FALLBACK_QUOTES;
}

function randomQuote() {
  const quotes = quotesPool();
  if (!quotes.length) return "Call me Ishmael.";
  let idx = Math.floor(Math.random() * quotes.length);
  if (quotes.length > 1 && idx === lastQuoteIndex) {
    idx = (idx + 1) % quotes.length;
  }
  lastQuoteIndex = idx;
  return quotes[idx];
}

async function loadMobyQuotes() {
  try {
    const resp = await fetch("/moby_quotes.json", { cache: "no-store" });
    if (!resp.ok) {
      mobyQuotes = [];
      return;
    }
    const payload = await resp.json();
    let values = [];
    if (Array.isArray(payload)) {
      values = payload;
    } else if (payload && Array.isArray(payload.quotes)) {
      values = payload.quotes;
    }
    mobyQuotes = values.filter((q) => typeof q === "string").map((q) => q.trim()).filter(Boolean);
  } catch (_error) {
    mobyQuotes = [];
  }
}

function renderStats(snapshot) {
  const metrics = snapshot.metrics || {};
  document.getElementById("stat-watch").textContent = snapshot.watch_count ?? 0;
  document.getElementById("stat-geo").textContent = snapshot.geo_count ?? 0;
  const alert24h = (snapshot.whales || []).reduce((sum, whale) => sum + (whale.alerts_24h || 0), 0);
  document.getElementById("stat-alerts").textContent = alert24h;
  document.getElementById("stat-refresh").textContent = snapshot.geo_last_refresh_at
    ? new Date(snapshot.geo_last_refresh_at * 1000).toLocaleTimeString()
    : "-";
  document.getElementById("stat-balance-refresh").textContent = snapshot.balance_last_refresh_at
    ? new Date(snapshot.balance_last_refresh_at * 1000).toLocaleTimeString()
    : "-";
  document.getElementById("stat-events-ingested").textContent = Number(
    snapshot.events_ingested ?? metrics.events_ingested ?? 0
  ).toLocaleString();
  document.getElementById("stat-events-usable").textContent = Number(
    snapshot.events_usable ?? metrics.events_usable ?? 0
  ).toLocaleString();
  document.getElementById("stat-events-per-min").textContent = Number(
    snapshot.events_per_min ?? metrics.events_per_min ?? 0
  ).toLocaleString();
  document.getElementById("stat-active-whales-5m").textContent = Number(
    snapshot.active_whales_5m ?? metrics.active_whales_5m ?? 0
  ).toLocaleString();
  const missRate = Number(snapshot.price_miss_rate ?? metrics.price_miss_rate ?? 0);
  document.getElementById("stat-price-miss-rate").textContent = `${(missRate * 100).toFixed(1)}%`;
}

function renderSeaState(seaState) {
  const tier = seaState?.tier || "calm";
  const score = seaState?.score_15m || 0;
  seaStateEl.className = `sea-state ${tier}`;
  seaStateEl.textContent = tier;
  seaScoreEl.textContent = `score ${Number(score).toFixed(1)}`;
}

function whaleStrengthUsd(whale) {
  const lastUsd = hasNumber(whale?.last_alert_usd) ? Number(whale.last_alert_usd) : 0;
  const portfolioUsd = hasNumber(whale?.holdings_total_usd) ? Number(whale.holdings_total_usd) : 0;
  return Math.max(lastUsd, portfolioUsd);
}

function whaleMapPoint(whale) {
  return {
    x: clamp(lonToX(whale?.lon), 1.5, 98.5),
    y: clamp(latToY(whale?.lat), 1.5, 98.5),
  };
}

function clusterIdFromWhales(whales) {
  return whales
    .map((whale) => String(whale?.address || ""))
    .filter(Boolean)
    .sort()
    .join("|");
}

function clusterWhales(whales, thresholdPct = 3.9) {
  const clusters = [];
  for (const whale of whales) {
    const point = whaleMapPoint(whale);
    let matched = null;
    for (const cluster of clusters) {
      const dist = Math.hypot(cluster.x - point.x, cluster.y - point.y);
      if (dist <= thresholdPct) {
        matched = cluster;
        break;
      }
    }
    if (!matched) {
      clusters.push({ x: point.x, y: point.y, whales: [whale] });
      continue;
    }
    matched.whales.push(whale);
    const count = matched.whales.length;
    matched.x = ((matched.x * (count - 1)) + point.x) / count;
    matched.y = ((matched.y * (count - 1)) + point.y) / count;
  }
  return clusters.map((cluster) => ({
    ...cluster,
    id: clusterIdFromWhales(cluster.whales),
  }));
}

function latestEventForWhale(address) {
  const normalized = String(address || "").toLowerCase();
  if (!normalized) return null;
  return (state.events || []).find((event) => {
    const source = String(event?.source?.address || "").toLowerCase();
    const target = String(event?.target?.address || "").toLowerCase();
    const watch = String(event?.watch_address || "").toLowerCase();
    return source === normalized || target === normalized || watch === normalized;
  });
}

function buildMarkerTitle(whale, quote, clusterHint = "") {
  const lastUsd = hasNumber(whale.last_alert_usd) ? whale.last_alert_usd : null;
  const portfolioUsd = hasNumber(whale.holdings_total_usd) ? whale.holdings_total_usd : null;
  const displayLabel = whaleLabelForDisplay(whale);
  const lines = [
    displayLabel,
    shortAddr(whale.address),
    `${whale.primary_country || "Unknown country"} ${whale.primary_region ? `(${whale.primary_region})` : ""}`.trim(),
    `Portfolio: ${formatPortfolioUsd(portfolioUsd)}`,
    `Latest flow: ${formatLastUsd(lastUsd)}`,
    `24h alerts: ${whale.alerts_24h || 0} • Tokens tracked: ${whale.holdings_token_count || 0}`,
    `"${quote}"`,
  ];
  if (clusterHint) {
    lines.splice(3, 0, clusterHint);
  }
  return lines.join("\n");
}

function renderWhaleMarker(whale, x, y, extraClass = "") {
  const marker = document.createElement("button");
  const strength = whaleStrengthUsd(whale);
  const lastUsd = hasNumber(whale?.last_alert_usd) ? Number(whale.last_alert_usd) : 0;
  const portfolioUsd = hasNumber(whale?.holdings_total_usd) ? Number(whale.holdings_total_usd) : 0;
  const displayLabel = whaleLabelForDisplay(whale);
  const size = markerSize(strength);
  const quote = randomQuote();
  marker.className = `whale-marker ${extraClass} ${(lastUsd >= 1000000 || portfolioUsd >= 100000000) ? "hot" : ""}`.trim();
  marker.style.left = `${x}%`;
  marker.style.top = `${y}%`;
  marker.style.width = `${size}px`;
  marker.style.height = `${size}px`;
  marker.setAttribute(
    "data-label",
    `${displayLabel} • ${whale.primary_country || "unknown waters"} • Portfolio ${formatPortfolioUsd(whale.holdings_total_usd)}`
  );
  marker.title = buildMarkerTitle(whale, quote);

  const sprite = document.createElement("span");
  sprite.className = "sprite";
  sprite.style.width = `${size}px`;
  sprite.style.height = `${size}px`;
  sprite.textContent = "🐋";
  marker.appendChild(sprite);

  marker.addEventListener("click", () => {
    const event = latestEventForWhale(whale.address);
    if (event) {
      replayEvent(event);
    }
  });

  mapEl.appendChild(marker);
}

function renderClusterMarker(cluster) {
  const marker = document.createElement("button");
  const size = clamp(34 + Math.log2(cluster.whales.length + 1) * 9, 36, 72);
  marker.className = "whale-marker cluster";
  marker.style.left = `${cluster.x}%`;
  marker.style.top = `${cluster.y}%`;
  marker.style.width = `${size}px`;
  marker.style.height = `${size}px`;
  marker.setAttribute("data-label", `${cluster.whales.length} whales overlap here • click to fan out`);
  marker.title = `Whale cluster (${cluster.whales.length})\n${cluster.whales
    .slice(0, 6)
    .map((item) => whaleLabelForDisplay(item))
    .join("\n")}\nClick to fan out`;

  const sprite = document.createElement("span");
  sprite.className = "sprite";
  sprite.style.width = `${size}px`;
  sprite.style.height = `${size}px`;
  sprite.textContent = "🌊";
  marker.appendChild(sprite);

  const badge = document.createElement("span");
  badge.className = "cluster-count";
  badge.textContent = String(cluster.whales.length);
  marker.appendChild(badge);

  marker.addEventListener("click", () => {
    ui.expandedClusterId = ui.expandedClusterId === cluster.id ? "" : cluster.id;
    renderMap(state.whales);
  });

  mapEl.appendChild(marker);
}

function renderExpandedCluster(cluster) {
  const center = document.createElement("button");
  center.className = "whale-marker cluster-core";
  center.style.left = `${cluster.x}%`;
  center.style.top = `${cluster.y}%`;
  center.style.width = "36px";
  center.style.height = "36px";
  center.setAttribute("data-label", "Collapse cluster");
  center.title = `Cluster expanded (${cluster.whales.length}) • click to collapse`;
  const core = document.createElement("span");
  core.className = "sprite";
  core.style.width = "36px";
  core.style.height = "36px";
  core.textContent = "×";
  center.appendChild(core);
  center.addEventListener("click", () => {
    ui.expandedClusterId = "";
    renderMap(state.whales);
  });
  mapEl.appendChild(center);

  const radius = clamp(2.2 + (cluster.whales.length * 0.2), 2.6, 5.8);
  const whales = [...cluster.whales].sort((a, b) => whaleStrengthUsd(b) - whaleStrengthUsd(a));
  whales.forEach((whale, index) => {
    const angle = ((Math.PI * 2) / whales.length) * index - (Math.PI / 2);
    const x = clamp(cluster.x + Math.cos(angle) * radius, 1.5, 98.5);
    const y = clamp(cluster.y + Math.sin(angle) * radius, 1.5, 98.5);
    renderWhaleMarker(whale, x, y, "cluster-child");
  });
}

function renderMap(whales) {
  mapEl.innerHTML = "";
  const ordered = [...(whales || [])].sort((a, b) => whaleStrengthUsd(b) - whaleStrengthUsd(a));
  const clusters = clusterWhales(ordered);
  if (ui.expandedClusterId && !clusters.some((cluster) => cluster.id === ui.expandedClusterId)) {
    ui.expandedClusterId = "";
  }
  for (const cluster of clusters) {
    if (cluster.whales.length <= 1) {
      const whale = cluster.whales[0];
      renderWhaleMarker(whale, cluster.x, cluster.y);
      continue;
    }
    if (ui.expandedClusterId === cluster.id) {
      renderExpandedCluster(cluster);
      continue;
    }
    renderClusterMarker(cluster);
  }
  if (oceanMapEl) {
    oceanMapEl.classList.toggle("cluster-expanded", Boolean(ui.expandedClusterId));
  }
  applyMapViewportTransform();
}

function sanitizePoint(point) {
  const rawLon = Number(point?.lon);
  const rawLat = Number(point?.lat);
  const lon = Number.isFinite(rawLon) ? clamp(rawLon, -179.5, 179.5) : 0;
  const lat = Number.isFinite(rawLat) ? clamp(rawLat, -84, 84) : 0;
  return { lon, lat };
}

function buildArcPath(source, target, routeIndex = 0, routeTotal = 1) {
  const sourcePoint = sanitizePoint(source);
  const targetPoint = sanitizePoint(target);
  const sx = lonToX(sourcePoint.lon);
  const sy = latToY(sourcePoint.lat);
  const tx = lonToX(targetPoint.lon);
  const ty = latToY(targetPoint.lat);
  const dx = tx - sx;
  const dy = ty - sy;
  const dist = Math.hypot(dx, dy);
  const centerOffset = routeIndex - (routeTotal - 1) / 2;
  if (dist < 1.2) {
    const loop = 1.8 + Math.abs(centerOffset) * 0.75;
    const d = `M ${sx} ${sy} m -${loop},0 a ${loop},${loop} 0 1,0 ${loop * 2},0 a ${loop},${loop} 0 1,0 -${loop * 2},0`;
    return { d, sx, sy, tx, ty, dist };
  }

  const mx = (sx + tx) / 2;
  const my = (sy + ty) / 2;
  const nx = -dy / dist;
  const ny = dx / dist;
  const spread = clamp(centerOffset * 2.2, -8, 8);
  const arcRise = Math.max(4, Math.min(18, dist * 0.3));
  const cx = mx + nx * spread;
  const cy = my + ny * spread - arcRise;
  return { d: `M ${sx} ${sy} Q ${cx} ${cy} ${tx} ${ty}`, sx, sy, tx, ty, dist };
}

function routeKey(event) {
  const source = event?.source?.address || "unknown_source";
  const target = event?.target?.address || "unknown_target";
  const type = event?.event_type || "unknown_event";
  return `${source}|${target}|${type}`;
}

function pickRenderableEvents(events, maxTotal = 64, perRouteMax = 1) {
  const picked = [];
  const counts = new Map();
  for (const event of events || []) {
    if (!event?.source || !event?.target) {
      continue;
    }
    const key = routeKey(event);
    const count = counts.get(key) || 0;
    if (count >= perRouteMax) {
      continue;
    }
    counts.set(key, count + 1);
    picked.push(event);
    if (picked.length >= maxTotal) {
      break;
    }
  }
  return picked;
}

function visibleEventsForMap(events) {
  const all = Array.isArray(events) ? events : [];
  const selectedEventId = ui.selectedEventId;
  const routeFiltered = all.filter((event) => routeVisibleForEvent(event) || eventIdOf(event) === selectedEventId);
  if (!ui.selectedEventId) {
    return pickRenderableEvents(routeFiltered, 84, 2);
  }
  const selected = routeFiltered.find((event) => eventIdOf(event) === ui.selectedEventId);
  if (!selected) {
    return pickRenderableEvents(routeFiltered, 84, 2);
  }
  const selectedRoute = routeKey(selected);
  const sameRoute = routeFiltered.filter((event) => routeKey(event) === selectedRoute).slice(0, 16);
  const context = pickRenderableEvents(
    routeFiltered.filter((event) => routeKey(event) !== selectedRoute),
    48,
    1
  );
  const out = [...sameRoute, ...context];
  return out.length ? out : [selected];
}

function routeLayout(events) {
  const totals = new Map();
  for (const event of events) {
    const key = routeKey(event);
    totals.set(key, (totals.get(key) || 0) + 1);
  }
  const seen = new Map();
  const layout = new Map();
  for (const event of events) {
    const key = routeKey(event);
    const index = seen.get(key) || 0;
    seen.set(key, index + 1);
    layout.set(eventIdOf(event), { routeIndex: index, routeTotal: totals.get(key) || 1 });
  }
  return layout;
}

function safeRemove(node) {
  if (node && node.parentNode) {
    node.parentNode.removeChild(node);
  }
}

function drawEventVisual(event, options = {}) {
  const source = event?.source;
  const target = event?.target;
  if (!source || !target) {
    return;
  }
  const routeIndex = Number(options.routeIndex || 0);
  const routeTotal = Number(options.routeTotal || 1);
  const { d, tx, ty, dist } = buildArcPath(source, target, routeIndex, routeTotal);
  const isFocus = Boolean(options.focus);
  const isRouteFocus = Boolean(options.routeFocus);
  const isMuted = Boolean(options.muted);
  const isReplay = Boolean(options.replay);
  const isTransient = Boolean(options.transient);

  const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
  path.setAttribute("d", d);
  path.classList.add("event-arc", event.severity || "calm");
  path.classList.add((event.event_type || "transfer_large").replace(/[^a-z0-9_]/gi, ""));
  if (isFocus) path.classList.add("focus");
  if (isRouteFocus) path.classList.add("route-focus");
  if (isMuted) path.classList.add("muted");
  if (isReplay) path.classList.add("replay");
  if (typeof event.score === "number") {
    const width = clamp(0.35 + Math.log10(event.score + 10) * 0.2, 0.35, 1.55);
    path.style.strokeWidth = `${width.toFixed(2)}`;
  }
  arcLayerEl.appendChild(path);

  const pulse = document.createElement("div");
  pulse.className = `event-pulse ${event.severity || "calm"}`;
  if (isFocus) pulse.classList.add("focus");
  if (isRouteFocus) pulse.classList.add("route-focus");
  if (isMuted) pulse.classList.add("muted");
  if (isReplay) pulse.classList.add("replay");
  pulse.style.left = `${tx}%`;
  pulse.style.top = `${ty}%`;
  pulse.title = `${event.event_type || "event"} • ${formatFlowUsd(event.usd_value)} • score ${alertScoreOf(event).toFixed(1)} • ${shortAddr(event.tx_id)}`;
  eventLayerEl.appendChild(pulse);

  const dot = document.createElement("div");
  dot.className = "event-dot";
  if (isMuted) dot.classList.add("muted");
  if (isReplay) dot.classList.add("replay");
  dot.style.left = `${tx}%`;
  dot.style.top = `${ty}%`;
  eventLayerEl.appendChild(dot);

  if (isReplay && dist > 1.2) {
    const sourceDot = document.createElement("div");
    sourceDot.className = "event-dot replay";
    sourceDot.style.left = `${lonToX(source.lon)}%`;
    sourceDot.style.top = `${latToY(source.lat)}%`;
    eventLayerEl.appendChild(sourceDot);
    if (isTransient) {
      window.setTimeout(() => safeRemove(sourceDot), 1500);
    }
  }

  if (isTransient) {
    window.setTimeout(() => {
      safeRemove(path);
      safeRemove(pulse);
      safeRemove(dot);
    }, isReplay ? 2100 : 3200);
  }
}

function renderEvents(events) {
  arcLayerEl.innerHTML = "";
  eventLayerEl.innerHTML = "";
  const visible = visibleEventsForMap(events);
  renderRouteSummary(visible);
  const selected = findEventForReplay({ eventId: ui.selectedEventId, txId: "" });
  const selectedRoute = selected ? routeKey(selected) : "";
  ui.selectedRouteKey = selectedRoute;
  if (oceanMapEl) {
    oceanMapEl.classList.toggle("focus-mode", Boolean(selected));
  }
  const layout = routeLayout(visible);
  for (const event of visible) {
    const eventId = eventIdOf(event);
    const route = layout.get(eventId) || { routeIndex: 0, routeTotal: 1 };
    const routeId = routeKey(event);
    const routeFocus = Boolean(selectedRoute) && routeId === selectedRoute;
    drawEventVisual(event, {
      focus: eventId === ui.selectedEventId,
      routeFocus,
      muted: Boolean(selectedRoute) && !routeFocus,
      routeIndex: route.routeIndex,
      routeTotal: route.routeTotal,
    });
  }
  scheduleFeedPanelSync();
}

function findEventForReplay({ eventId, txId }) {
  if (eventId) {
    const exact = (state.events || []).find((event) => eventIdOf(event) === eventId);
    if (exact) return exact;
  }
  if (txId) {
    const first = (state.events || []).find((event) => String(event.tx_id || "") === txId);
    if (first) return first;
  }
  return null;
}

function clearFocusEvent() {
  ui.selectedEventId = "";
  ui.selectedRouteKey = "";
  renderEvents(state.events);
  renderFeed(state.alerts);
  renderFocusEvent(null);
  syncLocationFromState("");
}

function renderFocusEvent(event) {
  if (!focusEventEl) {
    return;
  }
  if (!event) {
    focusEventEl.className = "focus-event empty";
    focusEventEl.innerHTML = `
      <div>
        <p class="focus-title">Route Focus</p>
        <p class="focus-main">Click any Captain's Log entry to lock and replay that wake.</p>
      </div>
    `;
    return;
  }
  const geo = routeGeoMeta(event);
  const score = alertScoreOf(event);
  const reason = topReasonLabel(event);
  const fromName = entityDisplay(event?.entities?.from, event.from_address);
  const toName = entityDisplay(event?.entities?.to, event.to_address);
  const explorer = explorerName(event.chain);
  const txUrl = txExplorerUrl(event.chain, event.tx_id);
  const fromUrl = addressExplorerUrl(event.chain, event.from_address);
  const toUrl = addressExplorerUrl(event.chain, event.to_address);
  const explorerLinks = [
    txUrl ? `<a class="deep-link" href="${txUrl}" target="_blank" rel="noopener noreferrer">Tx on ${explorer}</a>` : "",
    fromUrl ? `<a class="deep-link" href="${fromUrl}" target="_blank" rel="noopener noreferrer">From wallet</a>` : "",
    toUrl ? `<a class="deep-link" href="${toUrl}" target="_blank" rel="noopener noreferrer">To wallet</a>` : "",
  ]
    .filter(Boolean)
    .join(" • ");
  const deepLink = typeof event?.deep_link === "string" ? event.deep_link : "";
  focusEventEl.className = "focus-event active";
  focusEventEl.innerHTML = `
    <div>
      <p class="focus-title">Route Focus • ${geo.label}</p>
      <p class="focus-main">${event.event_type || "event"} • ${formatFlowUsd(event.usd_value)} • score ${score.toFixed(1)} • ${String(event.chain || "unknown").toLowerCase()}</p>
      <p class="focus-meta">${fromName} → ${toName} • ${formatAgoFromTs(event.timestamp)} • ${formatTs(event.timestamp)}</p>
      <p class="focus-meta">${reason ? `Driver: ${reason}` : "Driver: flow magnitude"}</p>
      ${explorerLinks ? `<p class="focus-meta">${explorerLinks}</p>` : ""}
      ${deepLink ? `<a class="deep-link" href="${deepLink}" target="_blank" rel="noopener noreferrer">Open Deep Link</a>` : ""}
    </div>
    <button id="focus-clear" class="action soft tiny">Clear Focus</button>
  `;
  const clearBtn = document.getElementById("focus-clear");
  if (clearBtn) {
    clearBtn.addEventListener("click", clearFocusEvent);
  }
}

function replayEvent(event) {
  if (!event) return;
  const eventId = eventIdOf(event);
  ui.selectedEventId = eventId;
  ui.selectedRouteKey = routeKey(event);
  renderFocusEvent(event);
  renderEvents(state.events);
  while (ui.replayTimers.length) {
    const timerId = ui.replayTimers.pop();
    if (typeof timerId === "number") {
      window.clearTimeout(timerId);
    }
  }
  const selectedRoute = routeKey(event);
  const routeEvents = (state.events || []).filter((row) => routeKey(row) === selectedRoute).slice(0, 8).reverse();
  const replayQueue = routeEvents.length ? routeEvents : [event, event, event];
  const replayLayout = routeLayout(replayQueue);
  replayQueue.forEach((item, index) => {
    const timerId = window.setTimeout(() => {
      const route = replayLayout.get(eventIdOf(item)) || { routeIndex: 0, routeTotal: 1 };
      drawEventVisual(item, {
        replay: true,
        focus: true,
        transient: true,
        routeIndex: route.routeIndex,
        routeTotal: route.routeTotal,
      });
    }, index * 170);
    ui.replayTimers.push(timerId);
  });
  const clearTimer = window.setTimeout(() => {
    ui.replayTimers = [];
  }, replayQueue.length * 170 + 2400);
  ui.replayTimers.push(clearTimer);
  if (routeEvents.length <= 1) {
    for (let burst = 0; burst < 2; burst += 1) {
      const timerId = window.setTimeout(() => {
        drawEventVisual(event, {
          replay: true,
          focus: true,
          transient: true,
          routeIndex: burst,
          routeTotal: 2,
        });
      }, burst * 120);
      ui.replayTimers.push(timerId);
    }
  }
  renderFeed(state.alerts);
  syncLocationFromState(eventId);
}

function renderFeed(alerts) {
  alertFeedEl.innerHTML = "";
  const windowSeconds = Number(state.filters?.window_seconds || 3600);
  const nowTs = Number(state.generatedAt || Math.floor(Date.now() / 1000));
  if (!alerts.length) {
    alertFeedEl.classList.add("sparse");
    const empty = document.createElement("li");
    empty.className = "alert-item";
    empty.innerHTML = `<p class="meta">No recent alerts in ${formatDuration(windowSeconds)}. Use Scout Now or lower Min USD.</p>`;
    alertFeedEl.appendChild(empty);
    if (feedStatusEl) {
      feedStatusEl.textContent = `Quiet waters. No alerts in your current ${formatDuration(windowSeconds)} window.`;
    }
    return;
  }
  alertFeedEl.classList.toggle("sparse", alerts.length < 4);
  for (const alert of alerts.slice(0, 60)) {
    const li = document.createElement("li");
    const eventId = String(alert.dedupe_key || "");
    const txId = String(alert.tx_id || "");
    const event = findEventForReplay({ eventId, txId });
    const geo = routeGeoMeta(event);
    const score = alertScoreOf(alert);
    const reason = topReasonLabel(alert);
    const entities = alert?.entities || {};
    const fromName = entityDisplay(entities.from, alert.from_address);
    const toName = entityDisplay(entities.to, alert.to_address);
    const txUrl = txExplorerUrl(alert.chain, alert.tx_id);
    const fromUrl = addressExplorerUrl(alert.chain, alert.from_address);
    const toUrl = addressExplorerUrl(alert.chain, alert.to_address);
    const deepLink = typeof alert?.deep_link === "string" ? alert.deep_link : "";
    const txDisplay = txUrl
      ? `<a class="deep-link" href="${txUrl}" target="_blank" rel="noopener noreferrer">${shortAddr(alert.tx_id)}</a>`
      : shortAddr(alert.tx_id);
    const fromDisplay = fromUrl
      ? `<a class="deep-link" href="${fromUrl}" target="_blank" rel="noopener noreferrer">${fromName}</a>`
      : fromName;
    const toDisplay = toUrl
      ? `<a class="deep-link" href="${toUrl}" target="_blank" rel="noopener noreferrer">${toName}</a>`
      : toName;
    const selectedClass = eventId && eventId === ui.selectedEventId ? " selected" : "";
    li.className = `alert-item replayable${selectedClass}`;
    li.dataset.eventId = eventId;
    li.dataset.txId = txId;
    li.innerHTML = `
      <p class="usd">${formatFlowUsd(alert.usd_value)}</p>
      <p class="score">Score ${score.toFixed(1)} / 100</p>
      <p class="meta"><span class="geo-badge geo-${geo.cls}">${geo.label}</span> ${alert.event_type || alert.tx_type || "event"} • ${alert.chain} • ${formatTs(alert.timestamp)}</p>
      <p class="meta">Tx ${txDisplay} • from ${fromDisplay} to ${toDisplay}</p>
      ${reason ? `<p class="reason">Driver: ${reason}</p>` : ""}
      <p class="meta replay-hint">Click to replay wake</p>
      ${deepLink ? `<a class="deep-link" href="${deepLink}" target="_blank" rel="noopener noreferrer">Open deep link</a>` : ""}
    `;
    alertFeedEl.appendChild(li);
  }
  const latestTs = Number(alerts[0]?.timestamp || 0);
  const latestAge = latestTs > 0 ? Math.max(0, nowTs - latestTs) : null;
  if (feedStatusEl) {
    if (latestAge === null) {
      feedStatusEl.textContent = "Live feed is connected.";
    } else if (latestAge > 180) {
      feedStatusEl.textContent = `Latest wake was ${formatDuration(latestAge)} ago. Try Scout Now or widen window.`;
    } else {
      feedStatusEl.textContent = `Live now. Latest wake ${formatDuration(latestAge)} ago.`;
    }
  }
}

function mountFeedReplay() {
  if (ui.feedMounted) {
    return;
  }
  ui.feedMounted = true;
  alertFeedEl.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof Element)) {
      return;
    }
    if (target.closest("a")) {
      return;
    }
    const item = target.closest(".alert-item");
    if (!item) return;
    const replay = findEventForReplay({
      eventId: item.dataset.eventId || "",
      txId: item.dataset.txId || "",
    });
    if (replay) {
      replayEvent(replay);
    }
  });
}

function mountMapInteractions() {
  if (ui.mapMounted || !oceanMapEl) {
    return;
  }
  ui.mapMounted = true;
  applyMapViewportTransform();

  const finishDrag = (event) => {
    if (!ui.mapViewport.dragging) {
      return;
    }
    if (
      event &&
      typeof event.pointerId === "number" &&
      typeof ui.mapViewport.pointerId === "number" &&
      event.pointerId !== ui.mapViewport.pointerId
    ) {
      return;
    }
    if (
      event &&
      typeof event.pointerId === "number" &&
      typeof oceanMapEl.hasPointerCapture === "function" &&
      oceanMapEl.hasPointerCapture(event.pointerId)
    ) {
      try {
        oceanMapEl.releasePointerCapture(event.pointerId);
      } catch (_error) {
        // Ignore pointer release races.
      }
    }
    ui.mapViewport.dragging = false;
    ui.mapViewport.pointerId = null;
    oceanMapEl.classList.remove("dragging");
  };

  oceanMapEl.addEventListener("pointerdown", (event) => {
    if (event.button !== 0) {
      return;
    }
    const target = event.target;
    if (target instanceof Element && target.closest(".whale-marker")) {
      return;
    }
    if (target instanceof Element && target.closest(".map-view-state")) {
      return;
    }
    if (ui.mapViewport.scale <= 1.0001) {
      return;
    }
    ui.mapViewport.dragging = true;
    ui.mapViewport.pointerId = event.pointerId;
    ui.mapViewport.startClientX = event.clientX;
    ui.mapViewport.startClientY = event.clientY;
    ui.mapViewport.startX = ui.mapViewport.x;
    ui.mapViewport.startY = ui.mapViewport.y;
    oceanMapEl.classList.add("dragging");
    if (typeof oceanMapEl.setPointerCapture === "function") {
      try {
        oceanMapEl.setPointerCapture(event.pointerId);
      } catch (_error) {
        // Ignore pointer capture errors from stale pointers.
      }
    }
    event.preventDefault();
  });

  oceanMapEl.addEventListener("pointermove", (event) => {
    if (!ui.mapViewport.dragging || event.pointerId !== ui.mapViewport.pointerId) {
      return;
    }
    const dx = event.clientX - ui.mapViewport.startClientX;
    const dy = event.clientY - ui.mapViewport.startClientY;
    const next = clampMapPan(ui.mapViewport.startX + dx, ui.mapViewport.startY + dy, ui.mapViewport.scale);
    ui.mapViewport.x = next.x;
    ui.mapViewport.y = next.y;
    if (Math.abs(dx) > 3 || Math.abs(dy) > 3) {
      ui.mapViewport.lastDragAtMs = Date.now();
    }
    applyMapViewportTransform();
    event.preventDefault();
  });

  oceanMapEl.addEventListener("pointerup", finishDrag);
  oceanMapEl.addEventListener("pointercancel", finishDrag);
  oceanMapEl.addEventListener("lostpointercapture", finishDrag);

  oceanMapEl.addEventListener(
    "wheel",
    (event) => {
      event.preventDefault();
      const factor = Math.exp(-event.deltaY * MAP_ZOOM_STEP);
      zoomMapAtClientPoint(ui.mapViewport.scale * factor, event.clientX, event.clientY);
    },
    { passive: false }
  );

  oceanMapEl.addEventListener("dblclick", (event) => {
    const target = event.target;
    if (target instanceof Element && target.closest(".whale-marker")) {
      return;
    }
    if (target instanceof Element && target.closest(".map-view-state")) {
      return;
    }
    event.preventDefault();
    resetMapViewport();
  });

  oceanMapEl.addEventListener("click", (event) => {
    const target = event.target;
    if (target instanceof Element && target.closest(".whale-marker")) {
      return;
    }
    if (target instanceof Element && target.closest(".map-view-state")) {
      return;
    }
    if (Date.now() - ui.mapViewport.lastDragAtMs < MAP_DRAG_CLICK_GUARD_MS) {
      return;
    }
    if (!ui.selectedEventId) {
      return;
    }
    clearFocusEvent();
  });
}

function mountViewControls() {
  if (ui.viewsMounted) {
    return;
  }
  ui.viewsMounted = true;
  loadSavedViewsFromStorage();
  renderSavedViewsSelect();

  if (savedViewSelectEl) {
    savedViewSelectEl.addEventListener("change", async () => {
      const name = String(savedViewSelectEl.value || "");
      if (!name) return;
      const preset = ui.savedViews.find((item) => item.name === name);
      if (!preset) return;
      await applyViewPreset(preset);
      syncLocationFromState(ui.selectedEventId || "");
    });
  }

  if (saveViewBtn) {
    saveViewBtn.addEventListener("click", async () => {
      const suggested = savedViewSelectEl?.value || "";
      const name = (window.prompt("Save current view as:", suggested || "My View") || "").trim();
      if (!name) return;
      const next = currentViewPayload(name);
      const existingIdx = ui.savedViews.findIndex((item) => item.name.toLowerCase() === name.toLowerCase());
      if (existingIdx >= 0) {
        ui.savedViews[existingIdx] = next;
      } else {
        ui.savedViews.push(next);
      }
      persistSavedViews();
      renderSavedViewsSelect();
      if (savedViewSelectEl) {
        savedViewSelectEl.value = name;
      }
      syncLocationFromState(ui.selectedEventId || "");
      await tick();
    });
  }

  if (deleteViewBtn) {
    deleteViewBtn.addEventListener("click", () => {
      const name = String(savedViewSelectEl?.value || "").trim();
      if (!name) return;
      const before = ui.savedViews.length;
      ui.savedViews = ui.savedViews.filter((item) => item.name !== name);
      if (ui.savedViews.length === before) return;
      persistSavedViews();
      renderSavedViewsSelect();
      syncLocationFromState(ui.selectedEventId || "");
    });
  }

  if (copyLinkBtn) {
    copyLinkBtn.addEventListener("click", async () => {
      const ok = await copyShareLink(ui.selectedEventId || "");
      if (!ok) {
        alert("Copy link failed in this browser.");
      }
    });
  }
}

function renderWhaleCards(whales) {
  whaleCardsEl.innerHTML = "";
  for (const whale of whales.slice(0, 40)) {
    const label = whaleLabelForDisplay(whale);
    const explorer = explorerName(whale.chain);
    const addressUrl = addressExplorerUrl(whale.chain, whale.address);
    const lastUsd = hasNumber(whale.last_alert_usd) ? whale.last_alert_usd : null;
    const portfolioUsd = hasNumber(whale.holdings_total_usd) ? whale.holdings_total_usd : null;
    const topHoldings = Array.isArray(whale.top_holdings) ? whale.top_holdings : [];
    const holdingsText = topHoldings
      .slice(0, 3)
      .map((item) => {
        const symbol = item?.symbol || shortAddr(item?.token_address || "");
        const usdValue = hasNumber(item?.usd_value) ? formatUsd(item.usd_value) : "n/a";
        return `${symbol} ${usdValue}`;
      })
      .filter(Boolean)
      .join(" • ");
    const div = document.createElement("article");
    div.className = "whale-card";
    div.innerHTML = `
      <h4>${label}</h4>
      <p>${
        addressUrl
          ? `<a class="deep-link" href="${addressUrl}" target="_blank" rel="noopener noreferrer">${shortAddr(whale.address)}</a> • ${explorer}`
          : shortAddr(whale.address)
      }</p>
      <p>${whale.primary_country || "Unknown waters"} ${whale.primary_region ? `• ${whale.primary_region}` : ""}</p>
      <p>Geo confidence: ${whale.confidence || "n/a"}</p>
      <p>Portfolio: ${formatPortfolioUsd(portfolioUsd)} ${whale.holdings_token_count ? `• ${whale.holdings_token_count} tokens` : ""}</p>
      <p>Top holdings: ${holdingsText || "n/a"}</p>
      <p>Last sighting: ${formatLastUsd(lastUsd)} ${whale.last_alert_at ? `• ${formatTs(whale.last_alert_at)}` : ""}</p>
      <p>24h alerts: ${whale.alerts_24h || 0} • Total: ${whale.alert_count_total || 0}</p>
    `;
    whaleCardsEl.appendChild(div);
  }
}

function selectedFromChips(container) {
  const values = [];
  for (const chip of container.querySelectorAll(".chip.active")) {
    const value = chip.dataset.value;
    if (value) values.push(value);
  }
  return values;
}

async function postFilters(payload) {
  const resp = await fetch("/api/state/filters", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!resp.ok) {
    throw new Error(`Filter update failed: ${resp.status}`);
  }
  return await resp.json();
}

function renderChipRow(container, items, selectedValues) {
  const selected = new Set((selectedValues || []).map((value) => String(value).toLowerCase()));
  container.innerHTML = "";
  for (const item of items || []) {
    const value = String(item).toLowerCase();
    const chip = document.createElement("button");
    chip.className = `chip ${selected.has(value) ? "active" : ""}`;
    chip.dataset.value = value;
    chip.textContent = value;
    container.appendChild(chip);
  }
}

function setChipSelection(container, values) {
  const selected = new Set((values || []).map((value) => String(value).toLowerCase()));
  for (const chip of container.querySelectorAll(".chip")) {
    const value = String(chip.dataset.value || "").toLowerCase();
    chip.classList.toggle("active", selected.has(value));
  }
}

function setRouteFilterSelection(routeFilters) {
  const fallback = {
    exact: true,
    anchored: true,
    pseudo: false,
    unknown: false,
  };
  const next = routeFilters && typeof routeFilters === "object" ? routeFilters : fallback;
  ui.routeFilters = {
    exact: Boolean(next.exact),
    anchored: Boolean(next.anchored),
    pseudo: Boolean(next.pseudo),
    unknown: Boolean(next.unknown),
  };
  if (!ui.routeFilters.exact && !ui.routeFilters.anchored && !ui.routeFilters.pseudo && !ui.routeFilters.unknown) {
    ui.routeFilters = fallback;
  }
  renderRouteFilterChips();
}

async function applyViewPreset(preset) {
  if (!preset || typeof preset !== "object") {
    return;
  }
  const filters = preset.filters && typeof preset.filters === "object" ? preset.filters : {};
  if (Array.isArray(filters.types)) {
    setChipSelection(typeFiltersEl, filters.types);
  }
  if (Array.isArray(filters.chains)) {
    setChipSelection(chainFiltersEl, filters.chains);
  }
  if (hasNumber(filters.min_usd)) {
    minUsdEl.value = String(Math.max(0, Number(filters.min_usd)));
    minUsdValueEl.textContent = formatUsd(Number(minUsdEl.value || 0));
  }
  if (hasNumber(filters.window_seconds)) {
    windowSecondsEl.value = String(Math.max(60, Number(filters.window_seconds)));
    windowSecondsValueEl.textContent = formatDuration(Number(windowSecondsEl.value || 3600));
  }
  if (hasNumber(filters.replay_offset_seconds)) {
    replayOffsetEl.value = String(Math.max(0, Number(filters.replay_offset_seconds)));
    replayOffsetValueEl.textContent = Number(replayOffsetEl.value) > 0 ? `${replayOffsetEl.value}s ago` : "Live";
    ui.paused = Number(replayOffsetEl.value) > 0;
    pauseLiveEl.textContent = ui.paused ? "Resume Live" : "Pause";
  }
  setRouteFilterSelection(preset.route_filters || preset.routeFilters || null);
  await applyCurrentFilters();
}

async function applyCurrentFilters() {
  const payload = {
    types: selectedFromChips(typeFiltersEl),
    chains: selectedFromChips(chainFiltersEl),
    min_usd: Number(minUsdEl.value || 0),
    window_seconds: Number(windowSecondsEl.value || 3600),
    replay_offset_seconds: Number(replayOffsetEl.value || 0),
  };
  if (!ui.paused) {
    payload.replay_offset_seconds = 0;
  }
  await postFilters(payload);
  await tick();
  syncLocationFromState(ui.selectedEventId || "");
}

function mountControls(snapshot) {
  const filters = snapshot.filters || {};
  const meta = snapshot.filters_meta || {};
  renderChipRow(typeFiltersEl, meta.available_event_types || [], filters.types || []);
  renderRouteFilterChips();
  renderChipRow(chainFiltersEl, meta.available_chains || [], filters.chains || []);

  minUsdEl.value = String(filters.min_usd || 0);
  minUsdValueEl.textContent = formatUsd(Number(filters.min_usd || 0));

  windowSecondsEl.value = String(filters.window_seconds || 3600);
  windowSecondsValueEl.textContent = formatDuration(Number(windowSecondsEl.value || 3600));

  replayOffsetEl.max = String(meta.max_replay_offset_seconds || 300);
  replayOffsetEl.value = String(filters.replay_offset_seconds || 0);
  replayOffsetValueEl.textContent = Number(replayOffsetEl.value) > 0 ? `${replayOffsetEl.value}s ago` : "Live";

  if (ui.controlsMounted) {
    return;
  }
  ui.controlsMounted = true;

  typeFiltersEl.addEventListener("click", async (event) => {
    const button = event.target.closest(".chip");
    if (!button) return;
    button.classList.toggle("active");
    await applyCurrentFilters();
  });

  if (routeFiltersEl) {
    routeFiltersEl.addEventListener("click", (event) => {
      const button = event.target.closest(".chip");
      if (!button) return;
      const value = String(button.dataset.value || "");
      if (!value) return;
      ui.routeFilters[value] = !routeClassEnabled(value);
      button.classList.toggle("active", routeClassEnabled(value));
      renderEvents(state.events);
      renderFeed(state.alerts);
      syncLocationFromState(ui.selectedEventId || "");
    });
  }

  chainFiltersEl.addEventListener("click", async (event) => {
    const button = event.target.closest(".chip");
    if (!button) return;
    button.classList.toggle("active");
    await applyCurrentFilters();
  });

  windowSecondsEl.addEventListener("input", () => {
    windowSecondsValueEl.textContent = formatDuration(Number(windowSecondsEl.value || 3600));
  });

  windowSecondsEl.addEventListener("change", async () => {
    await applyCurrentFilters();
  });

  minUsdEl.addEventListener("input", () => {
    minUsdValueEl.textContent = formatUsd(Number(minUsdEl.value || 0));
  });

  minUsdEl.addEventListener("change", async () => {
    await applyCurrentFilters();
  });

  pauseLiveEl.addEventListener("click", async () => {
    ui.paused = !ui.paused;
    pauseLiveEl.textContent = ui.paused ? "Resume Live" : "Pause";
    if (!ui.paused) {
      replayOffsetEl.value = "0";
      replayOffsetValueEl.textContent = "Live";
    }
    await applyCurrentFilters();
  });

  replayOffsetEl.addEventListener("input", () => {
    const value = Number(replayOffsetEl.value || 0);
    replayOffsetValueEl.textContent = value > 0 ? `${value}s ago` : "Live";
  });

  replayOffsetEl.addEventListener("change", async () => {
    ui.paused = true;
    pauseLiveEl.textContent = "Resume Live";
    await applyCurrentFilters();
  });
}

async function fetchState() {
  const resp = await fetch("/api/state", { cache: "no-store" });
  if (!resp.ok) {
    throw new Error(`State request failed with ${resp.status}`);
  }
  return await resp.json();
}

async function applyInitialUrlState() {
  if (ui.initialUrlApplied) {
    return;
  }
  ui.initialUrlApplied = true;
  const parsed = parseInitialUrlSpec();

  if (parsed.viewName) {
    const preset = ui.savedViews.find((item) => item.name === parsed.viewName);
    if (preset) {
      if (savedViewSelectEl) {
        savedViewSelectEl.value = preset.name;
      }
      await applyViewPreset(preset);
    }
  }

  const filterSpec = parsed.filters || {};
  const hasFilterSpec =
    (Array.isArray(filterSpec.types) && filterSpec.types.length > 0) ||
    (Array.isArray(filterSpec.chains) && filterSpec.chains.length > 0) ||
    hasNumber(filterSpec.min_usd) ||
    hasNumber(filterSpec.window_seconds) ||
    hasNumber(filterSpec.replay_offset_seconds);

  if (hasFilterSpec) {
    if (Array.isArray(filterSpec.types)) {
      setChipSelection(typeFiltersEl, filterSpec.types);
    }
    if (Array.isArray(filterSpec.chains)) {
      setChipSelection(chainFiltersEl, filterSpec.chains);
    }
    if (hasNumber(filterSpec.min_usd)) {
      minUsdEl.value = String(Math.max(0, Number(filterSpec.min_usd)));
      minUsdValueEl.textContent = formatUsd(Number(minUsdEl.value || 0));
    }
    if (hasNumber(filterSpec.window_seconds)) {
      windowSecondsEl.value = String(Math.max(60, Number(filterSpec.window_seconds)));
      windowSecondsValueEl.textContent = formatDuration(Number(windowSecondsEl.value || 3600));
    }
    if (hasNumber(filterSpec.replay_offset_seconds)) {
      replayOffsetEl.value = String(Math.max(0, Number(filterSpec.replay_offset_seconds)));
      replayOffsetValueEl.textContent = Number(replayOffsetEl.value) > 0 ? `${replayOffsetEl.value}s ago` : "Live";
      ui.paused = Number(replayOffsetEl.value) > 0;
      pauseLiveEl.textContent = ui.paused ? "Resume Live" : "Pause";
    }
    await applyCurrentFilters();
  }

  if (parsed.routeFilters) {
    setRouteFilterSelection(parsed.routeFilters);
    renderEvents(state.events);
    renderFeed(state.alerts);
  }

  const replay = findEventForReplay({ eventId: parsed.eventId, txId: parsed.txId });
  if (replay) {
    replayEvent(replay);
  } else if (parsed.eventId) {
    ui.selectedEventId = parsed.eventId;
    renderEvents(state.events);
    renderFeed(state.alerts);
  }

  syncLocationFromState(ui.selectedEventId || "");
}

async function refreshGeo() {
  refreshGeoBtn.disabled = true;
  try {
    const resp = await fetch("/api/refresh-geo", { method: "POST" });
    if (!resp.ok) throw new Error(`Geo refresh failed: ${resp.status}`);
    await tick();
  } catch (error) {
    console.error(error);
    alert("Geo refresh failed. Check server logs.");
  } finally {
    refreshGeoBtn.disabled = false;
  }
}

async function pollNow() {
  if (!pollNowBtn) return;
  pollNowBtn.disabled = true;
  try {
    const resp = await fetch("/api/poll-now", { method: "POST" });
    if (!resp.ok) throw new Error(`Poll failed: ${resp.status}`);
    await tick();
  } catch (error) {
    console.error(error);
    alert("Polling failed. Check server logs.");
  } finally {
    pollNowBtn.disabled = false;
  }
}

async function refreshBalances() {
  if (!refreshBalancesBtn) return;
  refreshBalancesBtn.disabled = true;
  try {
    const resp = await fetch("/api/refresh-balances", { method: "POST" });
    if (!resp.ok) throw new Error(`Balance refresh failed: ${resp.status}`);
    await tick();
  } catch (error) {
    console.error(error);
    alert("Holdings refresh failed. Check server logs.");
  } finally {
    refreshBalancesBtn.disabled = false;
  }
}

async function tick() {
  try {
    const snapshot = await fetchState();
    state.whales = snapshot.whales || [];
    state.alerts = snapshot.alerts || [];
    state.events = snapshot.events || [];
    state.filters = snapshot.filters || {};
    state.filtersMeta = snapshot.filters_meta || {};
    state.seaState = snapshot.sea_state || {};
    state.generatedAt = snapshot.generated_at || 0;

    const selected = findEventForReplay({ eventId: ui.selectedEventId, txId: "" });
    if (!selected) {
      ui.selectedEventId = "";
      ui.selectedRouteKey = "";
      renderFocusEvent(null);
    } else {
      renderFocusEvent(selected);
    }

    if (mobyQuoteEl) {
      mobyQuoteEl.textContent = `"${randomQuote()}"`;
    }
    mountControls(snapshot);
    renderStats(snapshot);
    renderSeaState(state.seaState);
    renderMap(state.whales);
    renderEvents(state.events);
    renderFeed(state.alerts);
    renderWhaleCards(state.whales);
    scheduleFeedPanelSync();
  } catch (error) {
    console.error(error);
  }
}

if (pollNowBtn) {
  pollNowBtn.addEventListener("click", pollNow);
}
refreshGeoBtn.addEventListener("click", refreshGeo);
if (refreshBalancesBtn) {
  refreshBalancesBtn.addEventListener("click", refreshBalances);
}

async function boot() {
  await loadMobyQuotes();
  mountFeedReplay();
  mountMapInteractions();
  mountViewControls();
  window.addEventListener("resize", () => {
    scheduleFeedPanelSync();
    applyMapViewportTransform();
  });
  scheduleFeedPanelSync();
  applyMapViewportTransform();
  await tick();
  await applyInitialUrlState();
  scheduleFeedPanelSync();
  applyMapViewportTransform();
  setInterval(async () => {
    if (ui.paused) {
      return;
    }
    await tick();
  }, 12000);
}

boot();
