const PALETTE = [
  '#e94560', '#48c9b0', '#f4a261', '#a29bfe', '#ff6b6b',
  '#6ec6ff', '#ffd93d', '#6bcb77', '#ee6c4d', '#98c1d9',
  '#e0aaff', '#b8f2e6', '#ffa69e', '#aed9e0', '#cdb4db',
];

const BOOK_COLORS = {};
const BOOK_NAMES = {};
const BOOK_AUTHORS = {};

let map;
let locationIndex = {};   // locationKey -> feature.properties
let locationCoords = {};   // locationKey -> [lon, lat]
let allLocationKeys = [];
let allMarkers = [];
let clusterGroup;
let heatLayer = null;
let heatVisible = false;
let activeJourneyLine = null;
let journeyMarkerGroup = null;
let activeJourney = null;
let journeyAnimation = null;
let currentRelevanceThreshold = 0.30;

// Passage chunk cache
const chunkCache = {};
let passageRequestId = 0;
let viewportDebounceTimer = null;

// ── Init ─────────────────────────────────────────────

async function init() {
  map = L.map('map', { zoomControl: false, preferCanvas: true }).setView([40, 0], 3);
  L.control.zoom({ position: 'topright' }).addTo(map);

  L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; <a href="https://carto.com/">CARTO</a>',
    maxZoom: 19,
  }).addTo(map);

  clusterGroup = L.markerClusterGroup({
    maxClusterRadius: 40,
    spiderfyOnMaxZoom: true,
    showCoverageOnHover: false,
  });

  await loadData();
  buildSidebar();
  updateStats();
  setupSearch();
  setupViewportTracking();
  setupRelevanceSlider();

  document.getElementById('close-panel').addEventListener('click', () => {
    document.getElementById('passage-panel').classList.add('hidden');
  });
  document.getElementById('random-btn').addEventListener('click', randomPassage);
  document.getElementById('heatmap-toggle').addEventListener('click', toggleHeatmap);
}

// ── Data Loading ─────────────────────────────────────

async function loadData() {
  let resp = await fetch('../data/locations.geojson');
  if (!resp.ok) {
    resp = await fetch('../data/mock_locations.geojson');
  }
  const geojson = await resp.json();

  // Register books from top-level metadata
  if (geojson.books) {
    let colorIdx = 0;
    for (const [id, meta] of Object.entries(geojson.books)) {
      BOOK_NAMES[id] = meta.title;
      BOOK_AUTHORS[id] = meta.author;
      BOOK_COLORS[id] = PALETTE[colorIdx % PALETTE.length];
      colorIdx++;
    }
  }

  // Build location index
  geojson.features.forEach(f => {
    const key = f.properties.k;
    locationIndex[key] = f.properties;
    locationCoords[key] = f.geometry.coordinates;
    allLocationKeys.push(key);

    // Fallback: discover books from features if no top-level metadata
    if (!geojson.books) {
      f.properties.b.forEach(bk => {
        if (!BOOK_NAMES[bk.i]) {
          BOOK_NAMES[bk.i] = bk.i;
          BOOK_COLORS[bk.i] = PALETTE[Object.keys(BOOK_NAMES).length % PALETTE.length];
        }
      });
    }
  });

  // Build markers
  buildMarkers();

  // Heatmap data — weighted by passage count * relevance
  const heatData = allLocationKeys.map(key => {
    const props = locationIndex[key];
    const coords = locationCoords[key];
    return [coords[1], coords[0], (props.r || 0.5) * Math.min(props.c, 10)];
  });
  heatLayer = L.heatLayer(heatData, {
    radius: 30, blur: 20, maxZoom: 10,
    gradient: { 0.2: '#0f3460', 0.4: '#e94560', 0.6: '#f4a261', 1: '#ffffff' },
  });

  map.addLayer(clusterGroup);
}

// ── Fetch passages on demand ─────────────────────────

async function fetchPassages(key) {
  const chunkId = locationIndex[key].ch;
  if (chunkCache[chunkId]) {
    return chunkCache[chunkId][key] || [];
  }
  const resp = await fetch(`../data/passages/chunk_${String(chunkId).padStart(2, '0')}.json`);
  if (!resp.ok) return [];
  const data = await resp.json();
  chunkCache[chunkId] = data;
  return data[key] || [];
}

// ── Markers ──────────────────────────────────────────

function buildMarkers() {
  clusterGroup.clearLayers();
  allMarkers = [];

  allLocationKeys.forEach(key => {
    const props = locationIndex[key];
    const coords = locationCoords[key];
    const bookIds = props.b.map(bk => bk.i);
    const isMulti = bookIds.length > 1;

    if (props.r < currentRelevanceThreshold) return;

    const color = isMulti ? '#ffffff' : BOOK_COLORS[bookIds[0]];
    const baseRadius = isMulti ? 10 : 7;
    const radius = baseRadius + Math.round(props.r * 4);

    const marker = L.circleMarker([coords[1], coords[0]], {
      radius, fillColor: color,
      color: isMulti ? '#e94560' : 'white',
      weight: isMulti ? 3 : 2,
      opacity: 1, fillOpacity: isMulti ? 0.95 : 0.85,
    });

    const displayName = props.n.replace(/\s*\(.*\)/, '');
    const relevanceLabel = props.r >= 0.5 ? 'setting' : 'mention';
    const tooltipText = isMulti
      ? `${displayName} — ${props.c} passages, ${bookIds.length} books (${relevanceLabel})`
      : `${displayName} (${relevanceLabel})`;

    marker.bindTooltip(tooltipText, { direction: 'top', offset: [0, -6] });
    marker.on('click', () => showLocationPassages(key));

    marker.locationKey = key;
    marker.bookIds = bookIds;
    marker.maxRelevance = props.r;
    allMarkers.push({ marker, locationKey: key, maxRelevance: props.r, bookIds });

    clusterGroup.addLayer(marker);
  });
}

// ── Relevance Slider ─────────────────────────────────

function setupRelevanceSlider() {
  const slider = document.getElementById('relevance-slider');
  const label = document.getElementById('relevance-label');

  slider.addEventListener('input', () => {
    const val = parseInt(slider.value);
    currentRelevanceThreshold = 0.85 * Math.pow(val / 100, 1.5);

    if (val === 0) label.textContent = 'All locations';
    else if (val < 30) label.textContent = 'Filtering noise';
    else if (val < 70) label.textContent = 'Recurring places';
    else label.textContent = 'Key settings only';

    buildMarkers();
    updateStats();
    updateViewportList();
  });
}

// ── Show Location Passages (async, stacked) ──────────

async function showLocationPassages(key) {
  const props = locationIndex[key];
  if (!props) return;

  const panel = document.getElementById('passage-panel');
  const content = document.getElementById('passage-content');

  const displayName = props.n.replace(/\s*\(.*\)/, '');
  const bookCount = props.b.length;
  const relevanceLabel = props.r >= 0.5 ? 'Setting' : 'Mention';

  // Show header immediately
  content.innerHTML = `
    <div class="passage-header">
      <div class="passage-location-name">${displayName}</div>
      <div class="passage-location-count">
        ${props.c} passage${props.c > 1 ? 's' : ''} from ${bookCount} book${bookCount > 1 ? 's' : ''}
        <span class="relevance-badge relevance-${relevanceLabel.toLowerCase()}">${relevanceLabel}</span>
      </div>
    </div>
    <div style="color:#888;padding:12px 0;">Loading passages...</div>
  `;
  panel.classList.remove('hidden');

  // Track request to avoid stale renders
  const reqId = ++passageRequestId;
  const passages = await fetchPassages(key);
  if (reqId !== passageRequestId) return;

  let html = `
    <div class="passage-header">
      <div class="passage-location-name">${displayName}</div>
      <div class="passage-location-count">
        ${props.c} passage${props.c > 1 ? 's' : ''} from ${bookCount} book${bookCount > 1 ? 's' : ''}
        <span class="relevance-badge relevance-${relevanceLabel.toLowerCase()}">${relevanceLabel}</span>
      </div>
    </div>
  `;

  // Sort by relevance descending
  const sorted = [...passages].sort((a, b) => (b.r || 0) - (a.r || 0));

  sorted.forEach(p => {
    const color = BOOK_COLORS[p.b] || '#888';
    const bookTitle = BOOK_NAMES[p.b] || p.b;
    const author = BOOK_AUTHORS[p.b] || '';
    const rel = p.r != null ? p.r : 0.5;
    const type = p.t || (rel >= 0.5 ? 'setting' : 'mention');
    html += `
      <div class="passage-entry">
        <div class="passage-book-line">
          <span class="passage-book-dot" style="background:${color}"></span>
          <span class="passage-book">${bookTitle}</span>
          <span class="passage-type passage-type-${type}">${type}</span>
        </div>
        <div class="passage-author">by ${author}</div>
        <div class="passage-meta">${p.ch || ''} ${p.l ? '&middot; Line ' + p.l : ''}</div>
        <div class="passage-text" style="border-left-color:${color}">${p.p}</div>
      </div>
    `;
  });

  content.innerHTML = html;
}

// ── Search ───────────────────────────────────────────

function setupSearch() {
  const input = document.getElementById('search-input');
  const results = document.getElementById('search-results');
  let searchTimer = null;

  input.addEventListener('input', () => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => runSearch(input, results), 100);
  });

  document.addEventListener('click', (e) => {
    if (!e.target.closest('#search-container')) {
      results.classList.add('hidden');
    }
  });
}

function runSearch(input, results) {
    const query = input.value.toLowerCase().trim();
    if (query.length < 2) {
      results.classList.add('hidden');
      return;
    }

    const matches = [];

    allLocationKeys.forEach(key => {
      const props = locationIndex[key];
      const nameMatch = props.n.toLowerCase().includes(query);
      const bookMatch = props.b.some(bk => {
        const title = (BOOK_NAMES[bk.i] || '').toLowerCase();
        const author = (BOOK_AUTHORS[bk.i] || '').toLowerCase();
        return title.includes(query) || author.includes(query);
      });

      if (nameMatch || bookMatch) {
        matches.push({ key, props });
      }
    });

    matches.sort((a, b) => b.props.r - a.props.r);

    if (matches.length === 0) {
      results.innerHTML = '<div class="search-result"><span class="sr-location">No results</span></div>';
      results.classList.remove('hidden');
      return;
    }

    results.innerHTML = matches.slice(0, 10).map(({ key, props }) => {
      const displayName = props.n.replace(/\s*\(.*\)/, '');
      const bookNames = props.b.map(bk => BOOK_NAMES[bk.i] || bk.i).join(', ');
      const type = props.r >= 0.5 ? 'setting' : 'mention';
      return `
        <div class="search-result" data-key="${key}">
          <span class="sr-count">${props.c}</span>
          <div class="sr-location">${displayName} <span class="sr-type sr-type-${type}">${type}</span></div>
          <div class="sr-book">${bookNames}</div>
        </div>
      `;
    }).join('');

    results.querySelectorAll('.search-result').forEach(el => {
      el.addEventListener('click', () => {
        const k = el.dataset.key;
        const coords = locationCoords[k];
        map.flyTo([coords[1], coords[0]], 8, { duration: 1 });
        showLocationPassages(k);
        results.classList.add('hidden');
        input.value = '';
      });
    });

    results.classList.remove('hidden');
}

// ── Random Passage ───────────────────────────────────

function randomPassage() {
  const visible = allMarkers.filter(m => m.maxRelevance >= currentRelevanceThreshold);
  if (visible.length === 0) return;
  const pick = visible[Math.floor(Math.random() * visible.length)];
  const coords = locationCoords[pick.locationKey];
  map.flyTo([coords[1], coords[0]], 7, { duration: 1.5 });
  showLocationPassages(pick.locationKey);
}

// ── Heatmap Toggle ───────────────────────────────────

function toggleHeatmap() {
  const btn = document.getElementById('heatmap-toggle');
  if (heatVisible) {
    map.removeLayer(heatLayer);
    map.addLayer(clusterGroup);
    btn.textContent = 'Show Heatmap';
    btn.classList.remove('active');
  } else {
    map.removeLayer(clusterGroup);
    map.addLayer(heatLayer);
    btn.textContent = 'Show Markers';
    btn.classList.add('active');
  }
  heatVisible = !heatVisible;
}

// ── Viewport Tracking ────────────────────────────────

function setupViewportTracking() {
  map.on('moveend', () => {
    clearTimeout(viewportDebounceTimer);
    viewportDebounceTimer = setTimeout(updateViewportList, 150);
  });
  updateViewportList();
}

function updateViewportList() {
  const bounds = map.getBounds();
  const container = document.getElementById('viewport-locations');

  const visible = [];
  allLocationKeys.forEach(key => {
    const props = locationIndex[key];
    if (props.r < currentRelevanceThreshold) return;

    const coords = locationCoords[key];
    const latlng = L.latLng(coords[1], coords[0]);
    if (bounds.contains(latlng)) {
      visible.push({ key, props });
    }
  });

  if (visible.length === 0) {
    container.innerHTML = '<div style="font-size:0.8rem;color:#555;padding:8px 0;">Zoom in or pan to see locations</div>';
    return;
  }

  visible.sort((a, b) => {
    if (Math.abs(b.props.r - a.props.r) > 0.1) return b.props.r - a.props.r;
    const aBooks = a.props.b.length;
    const bBooks = b.props.b.length;
    if (bBooks !== aBooks) return bBooks - aBooks;
    return a.props.n.localeCompare(b.props.n);
  });

  const shown = visible.slice(0, 50);

  container.innerHTML = shown.map(({ key, props }) => {
    const displayName = props.n.replace(/\s*\(.*\)/, '');
    const bookIds = props.b.map(bk => bk.i);
    const isMulti = bookIds.length > 1;
    const type = props.r >= 0.5 ? 'setting' : 'mention';

    let dots;
    if (isMulti) {
      dots = `<div class="viewport-multi">${bookIds.slice(0, 5).map(id =>
        `<span class="viewport-dot" style="background:${BOOK_COLORS[id]}"></span>`
      ).join('')}</div>`;
    } else {
      dots = `<span class="viewport-dot" style="background:${BOOK_COLORS[bookIds[0]]}"></span>`;
    }

    const countBadge = props.c > 1
      ? `<span class="viewport-count">${props.c}</span>`
      : '';

    return `
      <div class="viewport-item" data-key="${key}">
        ${dots}
        <span class="viewport-name">${displayName}</span>
        <span class="viewport-type viewport-type-${type}">${type[0].toUpperCase()}</span>
        ${countBadge}
      </div>
    `;
  }).join('') + (visible.length > 50 ? `<div style="font-size:0.75rem;color:#555;padding:8px;">+${visible.length - 50} more</div>` : '');

  container.querySelectorAll('.viewport-item').forEach(el => {
    el.addEventListener('click', () => {
      const k = el.dataset.key;
      const coords = locationCoords[k];
      map.flyTo([coords[1], coords[0]], 10, { duration: 0.8 });
      showLocationPassages(k);
    });
  });
}

// ── Sidebar / Journeys ───────────────────────────────

function buildSidebar() {
  const journeySelect = document.getElementById('journey-select');

  Object.keys(BOOK_NAMES)
    .sort((a, b) => BOOK_NAMES[a].localeCompare(BOOK_NAMES[b]))
    .forEach(bookId => {
      const option = document.createElement('option');
      option.value = bookId;
      option.textContent = BOOK_NAMES[bookId];
      journeySelect.appendChild(option);
    });

  journeySelect.addEventListener('change', (e) => selectJourney(e.target.value));
  document.getElementById('play-journey').addEventListener('click', playJourney);
}

function getJourneyStops(bookId) {
  const stops = [];
  allLocationKeys.forEach(key => {
    const props = locationIndex[key];
    const bookEntry = props.b.find(bk => bk.i === bookId);
    if (bookEntry && bookEntry.r >= 0.5) {
      stops.push({ key, props, order: bookEntry.o, chapter: bookEntry.ch });
    }
  });
  stops.sort((a, b) => a.order - b.order);
  return stops;
}

function selectJourney(bookId) {
  const info = document.getElementById('journey-info');
  const stops = document.getElementById('journey-stops');

  if (activeJourneyLine) {
    map.removeLayer(activeJourneyLine);
    activeJourneyLine = null;
  }
  if (journeyMarkerGroup) {
    map.removeLayer(journeyMarkerGroup);
    journeyMarkerGroup = null;
  }

  if (!bookId) {
    info.classList.add('hidden');
    activeJourney = null;
    if (!heatVisible) map.addLayer(clusterGroup);
    map.setView([40, 0], 3);
    return;
  }

  activeJourney = bookId;
  info.classList.remove('hidden');
  map.removeLayer(clusterGroup);

  const journeyStops = getJourneyStops(bookId);

  journeyMarkerGroup = L.featureGroup();
  journeyStops.forEach(stop => {
    const coords = locationCoords[stop.key];
    const latlng = [coords[1], coords[0]];
    const color = BOOK_COLORS[bookId];
    const marker = L.circleMarker(latlng, {
      radius: 10, fillColor: color, color: 'white',
      weight: 3, opacity: 1, fillOpacity: 0.9,
    });
    marker.bindTooltip(stop.props.n, { direction: 'top', offset: [0, -8], permanent: true, className: 'journey-tooltip' });
    marker.on('click', () => showLocationPassages(stop.key));
    journeyMarkerGroup.addLayer(marker);
  });
  journeyMarkerGroup.addTo(map);

  const coordsList = journeyStops.map(s => {
    const c = locationCoords[s.key];
    return [c[1], c[0]];
  });
  activeJourneyLine = L.polyline(coordsList, {
    color: BOOK_COLORS[bookId], weight: 3, opacity: 0.7,
    dashArray: '12 6', className: 'journey-path',
  }).addTo(map);

  if (coordsList.length > 0) {
    map.fitBounds(L.latLngBounds(coordsList).pad(0.3));
  }

  stops.innerHTML = '';
  journeyStops.forEach((stop, i) => {
    const el = document.createElement('div');
    el.className = 'journey-stop';
    el.innerHTML = `
      <span class="stop-num">${i + 1}</span>
      <div class="stop-details">
        <div class="stop-location">${stop.props.n}</div>
        <div class="stop-chapter">${stop.chapter || ''}</div>
      </div>
    `;
    el.addEventListener('click', () => {
      const c = locationCoords[stop.key];
      map.flyTo([c[1], c[0]], 8, { duration: 1 });
      showLocationPassages(stop.key);
      document.querySelectorAll('.journey-stop').forEach(s => s.classList.remove('active'));
      el.classList.add('active');
    });
    stops.appendChild(el);
  });
}

// ── Journey Animation ────────────────────────────────

function playJourney() {
  if (!activeJourney) return;

  const journeyStops = getJourneyStops(activeJourney);
  let step = 0;
  const stopEls = document.querySelectorAll('.journey-stop');

  if (journeyAnimation) clearInterval(journeyAnimation);

  function animate() {
    if (step >= journeyStops.length) {
      clearInterval(journeyAnimation);
      journeyAnimation = null;
      return;
    }

    const stop = journeyStops[step];
    const c = locationCoords[stop.key];
    map.flyTo([c[1], c[0]], 7, { duration: 1.5 });
    showLocationPassages(stop.key);

    stopEls.forEach(s => s.classList.remove('active'));
    if (stopEls[step]) stopEls[step].classList.add('active');

    step++;
  }

  animate();
  journeyAnimation = setInterval(animate, 3000);
}

// ── Stats ────────────────────────────────────────────

function updateStats() {
  const bookSet = new Set();
  let totalPassages = 0;
  let visibleLocations = 0;
  let multiBook = 0;
  let settings = 0;

  allLocationKeys.forEach(key => {
    const props = locationIndex[key];
    if (props.r < currentRelevanceThreshold) return;

    visibleLocations++;
    totalPassages += props.c;
    props.b.forEach(bk => bookSet.add(bk.i));

    if (props.b.length > 1) multiBook++;
    if (props.r >= 0.5) settings++;
  });

  document.getElementById('stats-content').innerHTML = `
    <div>${bookSet.size} books &middot; ${totalPassages.toLocaleString()} passages</div>
    <div>${visibleLocations} locations (${settings} settings)</div>
    <div>${multiBook} shared across books</div>
  `;
}

// ── Start ────────────────────────────────────────────

init();
