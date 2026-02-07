// Maryland Growth & Family Viability Atlas - Map Interface
// Uses Mapbox GL JS to visualize synthesis groupings

// Configuration
const MAPBOX_TOKEN = 'pk.eyJ1IjoiZWxrYXJpMjMiLCJhIjoiY2tubm04b3BkMTYwcTJzcG5tZDZ2YTV5MSJ9.S0oAvquhkkMoDGrRJ_oP-Q';
const MAPBOX_STYLE_URL = 'mapbox://styles/elkari23/cmlapbzzr005001s57o3mdhxk';
const API_BASE_URL = `${window.location.protocol}//${window.location.hostname}:8000/api/v1`;
// GeoJSON source URLs — tries in order until one succeeds.
// Set GEOJSON_BLOB_URL in a <script> tag before map.js to use Azure Blob Storage.
const GEOJSON_PATHS = [
    ...(typeof GEOJSON_BLOB_URL !== 'undefined' && GEOJSON_BLOB_URL ? [GEOJSON_BLOB_URL] : []),
    `${API_BASE_URL}/layers/counties/latest`,
    './md_counties_latest.geojson',
    '/md_counties_latest.geojson'
];

// Currently selected county FIPS (for layer detail lookups)
let currentFipsCode = null;
let currentCountySummary = null;

// Compare mode state
let compareCountyA = null;
let compareCountyB = null;

// Color schemes for different layers
const SYNTHESIS_COLORS = {
    'emerging_tailwinds': '#2d5016',
    'conditional_growth': '#7cb342',
    'stable_constrained': '#fdd835',
    'at_risk_headwinds': '#f4511e',
    'high_uncertainty': '#757575'
};

const DIRECTIONAL_COLORS = {
    'improving': '#2d5016',
    'stable': '#fdd835',
    'at_risk': '#f4511e'
};

const CONFIDENCE_COLORS = {
    'strong': '#1976d2',
    'conditional': '#ff9800',
    'fragile': '#e53935'
};

// Human-readable labels
const SYNTHESIS_LABELS = {
    'emerging_tailwinds': 'Emerging Tailwinds',
    'conditional_growth': 'Conditional Growth',
    'stable_constrained': 'Stable but Constrained',
    'at_risk_headwinds': 'At Risk / Headwinds',
    'high_uncertainty': 'High Uncertainty'
};

const DIRECTIONAL_LABELS = {
    'improving': 'Improving Trajectory',
    'stable': 'Stable Trajectory',
    'at_risk': 'At-Risk Trajectory'
};

const CONFIDENCE_LABELS = {
    'strong': 'Strong Evidence',
    'conditional': 'Conditional Evidence',
    'fragile': 'Fragile Evidence'
};

const LAYER_VIEW_LABELS = {
    synthesis: 'Overall Growth Outlook',
    directional: 'Direction of Change',
    confidence: 'Evidence Strength'
};

const LAYER_DECISION_GUIDANCE = {
    employment_gravity: {
        signal: 'Can households reach strong jobs and wage growth opportunities?',
        use: 'Prioritize workforce and employer strategies where this score is low.'
    },
    mobility_optionality: {
        signal: 'How many practical commute options exist across modes?',
        use: 'Low scores indicate transit/reliability constraints that can block growth.'
    },
    school_trajectory: {
        signal: 'Are school systems supporting long-run family stability?',
        use: 'Low scores suggest education quality or enrollment pressure risk.'
    },
    housing_elasticity: {
        signal: 'Can housing supply respond to demand without major affordability strain?',
        use: 'Low scores flag affordability pressure and permitting bottlenecks.'
    },
    demographic_momentum: {
        signal: 'Are working-age and family population dynamics reinforcing growth?',
        use: 'Low scores suggest out-migration or weaker household formation.'
    },
    risk_drag: {
        signal: 'How much environmental/infrastructure risk can erode gains?',
        use: 'Higher risk drag means stronger resilience mitigation is needed.'
    }
};

// Initialize Mapbox
mapboxgl.accessToken = MAPBOX_TOKEN;

const map = new mapboxgl.Map({
    container: 'map',
    style: MAPBOX_STYLE_URL,
    center: [-77.0, 39.0], // Center on Maryland
    zoom: 7,
    minZoom: 6,
    maxZoom: 16
});

const mapControlsHost = document.getElementById('map-controls');
function mountMapControlContainer() {
    if (!mapControlsHost) {
        return;
    }
    const mapContainer = map.getContainer();
    const controlContainer = mapContainer ? mapContainer.querySelector('.mapboxgl-control-container') : null;
    if (controlContainer && !mapControlsHost.contains(controlContainer)) {
        mapControlsHost.appendChild(controlContainer);
    }
}

const PITCH_START_ZOOM = 9.5;
const PITCH_END_ZOOM = 13.2;
const MAX_PITCH = 45;
const PULSE_RADIUS_MILES = 5;
const URBAN_PULSE_ENABLED = false;
const PREFERS_REDUCED_MOTION = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
const DEVICE_MEMORY_GB = Number(navigator.deviceMemory || 0);
const CPU_THREADS = Number(navigator.hardwareConcurrency || 0);
const LOW_PERFORMANCE_MODE = PREFERS_REDUCED_MOTION || (DEVICE_MEMORY_GB > 0 && DEVICE_MEMORY_GB <= 4) || (CPU_THREADS > 0 && CPU_THREADS <= 4);
const HOVER_FRAME_INTERVAL_MS = LOW_PERFORMANCE_MODE ? 48 : 16;
const PANEL_EASE_DURATION_MS = LOW_PERFORMANCE_MODE ? 220 : 350;
const MAP_FLY_DURATION_MS = LOW_PERFORMANCE_MODE ? 800 : 1200;
const MAP_EASE_DURATION_MS = LOW_PERFORMANCE_MODE ? 420 : 700;

const URBAN_PULSE_THEMES = {
    live: {
        label: 'Live',
        color: '#4caf50',
        classes: ['park', 'residential', 'garden', 'cemetery', 'stadium']
    },
    work: {
        label: 'Work',
        color: '#2196f3',
        classes: ['office', 'industrial', 'commercial', 'warehouse', 'shop']
    },
    learn: {
        label: 'Learn',
        color: '#fbc02d',
        classes: ['school', 'college', 'university', 'library']
    },
    transit: {
        label: 'Transit',
        color: '#e53935',
        classes: ['bus', 'bus_stop', 'rail', 'railway', 'station', 'ferry']
    }
};

const URBAN_PULSE_LAYER_IDS = Object.keys(URBAN_PULSE_THEMES).map((key) => `urban-${key}-poi`);
let activePulse = null;

function updatePitch() {
    const zoom = map.getZoom();
    const t = Math.max(0, Math.min(1, (zoom - PITCH_START_ZOOM) / (PITCH_END_ZOOM - PITCH_START_ZOOM)));
    const pitch = t * MAX_PITCH;
    if (Math.abs(map.getPitch() - pitch) > 0.5) {
        map.setPitch(pitch);
    }
}

function setPanelOpenState(isOpen) {
    // Split-layout mode keeps the detail panel in a fixed sidebar,
    // so map padding animations are unnecessary.
    void isOpen;
}

// Add map controls (bottom-right position)
map.addControl(new mapboxgl.NavigationControl({
    showCompass: false  // Compass not needed for 2D view
}), 'bottom-right');

map.addControl(new mapboxgl.GeolocateControl({
    positionOptions: {
        enableHighAccuracy: true
    },
    trackUserLocation: false,
    showUserHeading: false
}), 'bottom-right');

const fullscreenContainer = document.getElementById('app') || map.getContainer();
map.addControl(new mapboxgl.FullscreenControl({ container: fullscreenContainer }), 'bottom-right');
mountMapControlContainer();

// Popup for hover tooltips
const popup = new mapboxgl.Popup({
    closeButton: false,
    closeOnClick: false,
    offset: 12,
    maxWidth: '280px'
});

// Debounce helper for hover performance
let hoverDebounceTimer = null;
let lastHoverFrameTimestamp = 0;
function debounceHover(callback, minInterval = HOVER_FRAME_INTERVAL_MS) {
    return function(...args) {
        if (hoverDebounceTimer) cancelAnimationFrame(hoverDebounceTimer);
        hoverDebounceTimer = requestAnimationFrame((timestamp) => {
            if (timestamp - lastHoverFrameTimestamp < minInterval) {
                return;
            }
            lastHoverFrameTimestamp = timestamp;
            callback.apply(this, args);
        });
    };
}

// Current layer selection
let currentLayer = 'synthesis';
let activeLegendFilter = null;
let activeBivariateFilter = null; // { dir: 'improving', conf: 'strong' } or null
const DEFAULT_FILL_OPACITY = 0.68;
const DIMMED_FILL_OPACITY = 0.18;
const SOFT_DIMMED_FILL_OPACITY_EXPR = ['interpolate', ['linear'], ['zoom'], 6, 0.16, 8, 0.22, 11, 0.30];
const HARD_DIMMED_FILL_OPACITY_EXPR = ['interpolate', ['linear'], ['zoom'], 6, 0.10, 8, 0.16, 11, 0.24];

function getCurrentLayerPropertyName() {
    if (currentLayer === 'directional') {
        return 'directional_class';
    }
    if (currentLayer === 'confidence') {
        return 'confidence_class';
    }
    return 'synthesis_grouping';
}

function getCurrentLayerGroupLabels() {
    if (currentLayer === 'directional') {
        return DIRECTIONAL_LABELS;
    }
    if (currentLayer === 'confidence') {
        return CONFIDENCE_LABELS;
    }
    return SYNTHESIS_LABELS;
}

function findCountyNameByFips(fipsCode) {
    if (!fipsCode || !map || !map.getSource) {
        return null;
    }
    const source = map.getSource('counties');
    const features = source && source._data && source._data.features ? source._data.features : [];
    const match = features.find((feature) => feature.properties && feature.properties.fips_code === fipsCode);
    return match && match.properties ? match.properties.county_name : null;
}

function setChipState(chipId, text, muted = false) {
    const chip = document.getElementById(chipId);
    if (!chip) {
        return;
    }
    chip.textContent = text;
    chip.classList.toggle('muted', muted);
}

function updateMapStateChips() {
    // Legacy map-state chips are no longer rendered.
}

function setCompareGuidance(isActive) {
    const guidance = document.getElementById('compare-guidance');
    if (!guidance) {
        return;
    }
    if (isActive) {
        const countyAName = compareCountyA?.county_name || currentCountySummary?.county_name;
        guidance.textContent = countyAName
            ? `Compare mode active. ${countyAName} is County A. Click a second county on the map.`
            : 'Compare mode active. Click a second county on the map.';
        guidance.classList.add('visible');
        return;
    }
    guidance.classList.remove('visible');
}


function setCompareButtonState(enabled) {
    const button = document.querySelector('.compare-toggle');
    if (!button) {
        return;
    }
    button.disabled = !enabled;
    if (!enabled) {
        button.setAttribute('aria-pressed', 'false');
        button.textContent = 'Compare';
    }
}

function normalizeAnalysisItems(items) {
    if (!Array.isArray(items)) {
        return [];
    }
    return items
        .map((item) => String(item || '').trim())
        .filter((item) => item.length > 0);
}

function renderAnalysisList(items, emptyText, icon) {
    if (!items.length) {
        return `<ul class="analysis-float-list"><li>${emptyText}</li></ul>`;
    }
    return `<ul class="analysis-float-list">${items.map((item) => `<li>${icon ? `${icon} ` : ''}${item}</li>`).join('')}</ul>`;
}

function hideFloatingAnalysisPanel() {
    const panel = document.getElementById('analysis-float');
    if (!panel) {
        return;
    }
    panel.classList.add('hidden');
}

function setupAnalysisPanel() {
    const panel = document.getElementById('analysis-float');
    const host = document.getElementById('map-pane');
    const handle = document.getElementById('analysis-float-handle');
    const closeButton = document.getElementById('analysis-float-close');
    if (!panel || !host || !handle || !closeButton || panel.dataset.bound === 'true') {
        return;
    }

    panel.dataset.bound = 'true';
    let dragState = null;

    const clampAndApply = (left, top) => {
        const hostRect = host.getBoundingClientRect();
        const panelRect = panel.getBoundingClientRect();
        const padding = 8;
        const maxLeft = Math.max(padding, hostRect.width - panelRect.width - padding);
        const maxTop = Math.max(padding, hostRect.height - panelRect.height - padding);
        const clampedLeft = Math.min(Math.max(left, padding), maxLeft);
        const clampedTop = Math.min(Math.max(top, padding), maxTop);
        panel.style.left = `${clampedLeft}px`;
        panel.style.top = `${clampedTop}px`;
        panel.style.right = 'auto';
        panel.style.bottom = 'auto';
        panel.dataset.positioned = 'true';
    };

    closeButton.addEventListener('click', () => {
        hideFloatingAnalysisPanel();
    });
    closeButton.addEventListener('pointerdown', (event) => {
        event.stopPropagation();
    });

    handle.addEventListener('pointerdown', (event) => {
        if (event.target && event.target.closest && event.target.closest('#analysis-float-close')) {
            return;
        }
        if (window.matchMedia('(max-width: 980px)').matches || event.button !== 0) {
            return;
        }
        const panelRect = panel.getBoundingClientRect();
        dragState = {
            offsetX: event.clientX - panelRect.left,
            offsetY: event.clientY - panelRect.top,
            pointerId: event.pointerId
        };
        panel.classList.add('dragging');
        handle.setPointerCapture(event.pointerId);
        event.preventDefault();
    });

    handle.addEventListener('pointermove', (event) => {
        if (!dragState || dragState.pointerId !== event.pointerId) {
            return;
        }
        const hostRect = host.getBoundingClientRect();
        const left = event.clientX - hostRect.left - dragState.offsetX;
        const top = event.clientY - hostRect.top - dragState.offsetY;
        clampAndApply(left, top);
    });

    const endDrag = (event) => {
        if (!dragState || dragState.pointerId !== event.pointerId) {
            return;
        }
        dragState = null;
        panel.classList.remove('dragging');
        handle.releasePointerCapture(event.pointerId);
    };

    handle.addEventListener('pointerup', endDrag);
    handle.addEventListener('pointercancel', endDrag);

    window.addEventListener('resize', () => {
        if (panel.classList.contains('hidden')) {
            return;
        }
        if (window.matchMedia('(max-width: 980px)').matches) {
            panel.style.left = '';
            panel.style.top = '';
            panel.style.right = '';
            panel.style.bottom = '';
            return;
        }
        const left = Number.parseFloat(panel.style.left);
        const top = Number.parseFloat(panel.style.top);
        clampAndApply(Number.isFinite(left) ? left : 16, Number.isFinite(top) ? top : 16);
    });
}

function showFloatingAnalysisPanel(data) {
    const panel = document.getElementById('analysis-float');
    const titleEl = document.getElementById('analysis-float-title');
    const contentEl = document.getElementById('analysis-float-content');
    if (!panel || !titleEl || !contentEl) {
        return;
    }

    setupAnalysisPanel();

    const strengths = normalizeAnalysisItems(data?.primary_strengths);
    const weaknesses = normalizeAnalysisItems(data?.primary_weaknesses);
    const trends = normalizeAnalysisItems(data?.key_trends);

    titleEl.textContent = `${data?.county_name || 'County'} Analysis`;
    contentEl.innerHTML = `
        <h4>Primary Strengths</h4>
        ${renderAnalysisList(strengths, 'No strengths reported for this county.', '✓')}
        <h4>Primary Weaknesses</h4>
        ${renderAnalysisList(weaknesses, 'No weaknesses reported for this county.', '⚠')}
        <h4>Key Trends</h4>
        ${renderAnalysisList(trends, 'No trend notes available for this county.', '')}
        <div class="analysis-float-meta">Data Year: ${data?.data_year ?? 'N/A'}</div>
    `;

    if (!window.matchMedia('(max-width: 980px)').matches && panel.dataset.positioned !== 'true') {
        panel.style.left = '16px';
        panel.style.top = '16px';
        panel.style.right = 'auto';
        panel.style.bottom = 'auto';
        panel.dataset.positioned = 'true';
    }

    panel.classList.remove('hidden');
}

function setupLayoutResizer() {
    const shell = document.querySelector('.atlas-shell');
    const resizer = document.getElementById('layout-resizer');
    if (!shell || !resizer || resizer.dataset.bound === 'true') {
        return;
    }

    const STORAGE_KEY = 'atlas_sidebar_width_px';
    const MIN_MAP_WIDTH_PX = 420;
    const STEP_PX = 24;
    let dragPointerId = null;
    let mapResizeRaf = null;

    const queueMapResize = () => {
        if (mapResizeRaf !== null) {
            return;
        }
        mapResizeRaf = requestAnimationFrame(() => {
            mapResizeRaf = null;
            if (map && typeof map.resize === 'function') {
                map.resize();
            }
        });
    };

    const getNumericVar = (varName, fallback) => {
        const raw = getComputedStyle(shell).getPropertyValue(varName);
        const parsed = Number.parseFloat(raw);
        return Number.isFinite(parsed) ? parsed : fallback;
    };

    const getBounds = () => {
        const shellRect = shell.getBoundingClientRect();
        const minSidebar = getNumericVar('--sidebar-min-width', 320);
        const maxSidebarVar = getNumericVar('--sidebar-max-width', 680);
        const resizerWidth = getNumericVar('--resizer-width', 12);
        const maxByAvailableSpace = shellRect.width - MIN_MAP_WIDTH_PX - resizerWidth;
        const maxSidebar = Math.max(minSidebar, Math.min(maxSidebarVar, maxByAvailableSpace));
        return { minSidebar, maxSidebar };
    };

    const applySidebarWidth = (widthPx, persist = false) => {
        const { minSidebar, maxSidebar } = getBounds();
        const clamped = Math.min(Math.max(widthPx, minSidebar), maxSidebar);
        shell.style.setProperty('--sidebar-width', `${Math.round(clamped)}px`);
        if (persist) {
            try {
                localStorage.setItem(STORAGE_KEY, String(Math.round(clamped)));
            } catch (error) {
                console.warn('Unable to persist sidebar width:', error);
            }
        }
        queueMapResize();
    };

    const updateFromPointer = (clientX, persist = false) => {
        const shellRect = shell.getBoundingClientRect();
        const nextWidth = shellRect.right - clientX;
        applySidebarWidth(nextWidth, persist);
    };

    const syncToBounds = () => {
        if (window.matchMedia('(max-width: 980px)').matches) {
            return;
        }
        const current = Number.parseFloat(getComputedStyle(shell).getPropertyValue('--sidebar-width'));
        if (Number.isFinite(current)) {
            applySidebarWidth(current, false);
        }
    };

    resizer.dataset.bound = 'true';

    try {
        const savedWidth = Number.parseFloat(localStorage.getItem(STORAGE_KEY));
        if (Number.isFinite(savedWidth)) {
            applySidebarWidth(savedWidth, false);
        }
    } catch (error) {
        console.warn('Unable to load persisted sidebar width:', error);
    }

    resizer.addEventListener('pointerdown', (event) => {
        if (window.matchMedia('(max-width: 980px)').matches || event.button !== 0) {
            return;
        }
        dragPointerId = event.pointerId;
        resizer.classList.add('dragging');
        document.body.classList.add('layout-resizing');
        resizer.setPointerCapture(event.pointerId);
        event.preventDefault();
    });

    resizer.addEventListener('pointermove', (event) => {
        if (dragPointerId === null || event.pointerId !== dragPointerId) {
            return;
        }
        updateFromPointer(event.clientX, false);
    });

    const finishDrag = (event) => {
        if (dragPointerId === null || event.pointerId !== dragPointerId) {
            return;
        }
        if (event.type === 'pointerup') {
            updateFromPointer(event.clientX, true);
        }
        dragPointerId = null;
        resizer.classList.remove('dragging');
        document.body.classList.remove('layout-resizing');
        resizer.releasePointerCapture(event.pointerId);
    };

    resizer.addEventListener('pointerup', finishDrag);
    resizer.addEventListener('pointercancel', finishDrag);

    resizer.addEventListener('keydown', (event) => {
        if (window.matchMedia('(max-width: 980px)').matches) {
            return;
        }
        const current = Number.parseFloat(getComputedStyle(shell).getPropertyValue('--sidebar-width')) || 420;
        if (event.key === 'ArrowLeft') {
            applySidebarWidth(current + STEP_PX, true);
            event.preventDefault();
            return;
        }
        if (event.key === 'ArrowRight') {
            applySidebarWidth(current - STEP_PX, true);
            event.preventDefault();
            return;
        }
        if (event.key === 'Home') {
            applySidebarWidth(getBounds().minSidebar, true);
            event.preventDefault();
            return;
        }
        if (event.key === 'End') {
            applySidebarWidth(getBounds().maxSidebar, true);
            event.preventDefault();
        }
    });

    window.addEventListener('resize', syncToBounds);
}

function buildFillOpacityExpression() {
    const activeProperty = getCurrentLayerPropertyName();
    const hasSelection = Boolean(currentFipsCode);
    const hasFilter = Boolean(activeLegendFilter);
    const hasBivariateFilter = Boolean(activeBivariateFilter);

    // Bivariate filter: match on both directional_class AND confidence_class
    if (hasBivariateFilter) {
        const matchExpr = ['all',
            ['==', ['get', 'directional_class'], activeBivariateFilter.dir],
            ['==', ['get', 'confidence_class'], activeBivariateFilter.conf]
        ];
        if (hasSelection) {
            return ['case',
                ['==', ['get', 'fips_code'], currentFipsCode],
                DEFAULT_FILL_OPACITY,
                matchExpr,
                DEFAULT_FILL_OPACITY,
                DIMMED_FILL_OPACITY
            ];
        }
        return ['case', matchExpr, DEFAULT_FILL_OPACITY, DIMMED_FILL_OPACITY];
    }

    if (hasSelection && hasFilter) {
        return ['case',
            ['==', ['get', 'fips_code'], currentFipsCode],
            DEFAULT_FILL_OPACITY,
            ['==', ['get', activeProperty], activeLegendFilter],
            0.46,
            HARD_DIMMED_FILL_OPACITY_EXPR
        ];
    }

    if (hasSelection) {
        return ['case',
            ['==', ['get', 'fips_code'], currentFipsCode],
            DEFAULT_FILL_OPACITY,
            SOFT_DIMMED_FILL_OPACITY_EXPR
        ];
    }

    if (hasFilter) {
        return ['case',
            ['==', ['get', activeProperty], activeLegendFilter],
            DEFAULT_FILL_OPACITY,
            DIMMED_FILL_OPACITY
        ];
    }

    return DEFAULT_FILL_OPACITY;
}

function buildBorderOpacityExpression() {
    if (currentFipsCode) {
        return ['case',
            ['==', ['get', 'fips_code'], currentFipsCode],
            0.95,
            0.2
        ];
    }
    if (activeLegendFilter) {
        return 0.25;
    }
    return ['interpolate', ['linear'], ['zoom'], 6, 0.24, 8, 0.35, 11, 0.5];
}

function buildBorderWidthExpression() {
    const baseWidth = ['interpolate', ['linear'], ['zoom'], 6, 0.5, 10, 1.1];
    if (!currentFipsCode) {
        return baseWidth;
    }
    return ['case',
        ['==', ['get', 'fips_code'], currentFipsCode],
        2.4,
        baseWidth
    ];
}

function applyMapVisualFocus() {
    if (!map || !map.getLayer) {
        return;
    }
    if (map.getLayer('counties-fill')) {
        map.setPaintProperty('counties-fill', 'fill-opacity', buildFillOpacityExpression());
    }
    if (map.getLayer('counties-border')) {
        map.setPaintProperty('counties-border', 'line-opacity', buildBorderOpacityExpression());
        map.setPaintProperty('counties-border', 'line-width', buildBorderWidthExpression());
    }
}

async function fetchGeojsonWithFallback(paths) {
    const errors = [];

    for (const path of paths) {
        try {
            const response = await fetch(path);
            if (!response.ok) {
                errors.push(`${path} (${response.status})`);
                continue;
            }
            const data = await response.json();
            if (!data || data.type !== 'FeatureCollection') {
                errors.push(`${path} (invalid GeoJSON payload)`);
                continue;
            }
            return data;
        } catch (error) {
            errors.push(`${path} (${error.message})`);
        }
    }

    throw new Error(`No reachable GeoJSON source. Tried: ${errors.join('; ')}`);
}

// Update loading status helper
function updateLoadingStatus(text) {
    const statusEl = document.getElementById('loading-status');
    if (statusEl) statusEl.textContent = text;
}

// Map load event
map.on('load', async () => {
    mountMapControlContainer();
    try {
        updateLoadingStatus('Fetching county data...');

        add3DBuildings();

        map.on('zoom', () => {
            updatePitch();
        });

        // Fetch GeoJSON data (API first, local fallback)
        const geojsonData = await fetchGeojsonWithFallback(GEOJSON_PATHS);

        updateLoadingStatus('Rendering map layers...');

        // Add source
        map.addSource('counties', {
            type: 'geojson',
            data: geojsonData
        });

        // Add fill layer (synthesis grouping - default)
        map.addLayer({
            id: 'counties-fill',
            type: 'fill',
            source: 'counties',
            paint: {
                'fill-color': getSynthesisFillExpression(),
                'fill-opacity': DEFAULT_FILL_OPACITY
            }
        });

        // Add border layer
        map.addLayer({
            id: 'counties-border',
            type: 'line',
            source: 'counties',
            paint: {
                'line-color': '#333',
                'line-width': ['interpolate', ['linear'], ['zoom'], 6, 0.5, 10, 1.1],
                'line-opacity': ['interpolate', ['linear'], ['zoom'], 6, 0.24, 8, 0.35, 11, 0.5]
            }
        });

        // Add hover highlight layer
        map.addLayer({
            id: 'counties-hover',
            type: 'line',
            source: 'counties',
            paint: {
                'line-color': '#000',
                'line-width': 3
            },
            filter: ['==', 'fips_code', '']
        });

        // Add selected county highlight layer
        map.addLayer({
            id: 'counties-selected',
            type: 'line',
            source: 'counties',
            paint: {
                'line-color': '#1a73e8',
                'line-width': 4,
                'line-opacity': 0.9
            },
            filter: ['==', 'fips_code', '']
        });

        // Add county name labels (centroids computed via Turf.js)
        if (typeof turf !== 'undefined') {
            const labelFeatures = geojsonData.features.map(f => {
                const centroid = turf.centroid(f);
                centroid.properties = { county_name: f.properties.county_name };
                return centroid;
            });
            map.addSource('county-labels', {
                type: 'geojson',
                data: { type: 'FeatureCollection', features: labelFeatures }
            });
            map.addLayer({
                id: 'county-labels',
                type: 'symbol',
                source: 'county-labels',
                minzoom: 8,
                layout: {
                    'text-field': ['get', 'county_name'],
                    'text-size': 12,
                    'text-font': ['DIN Pro Medium', 'Arial Unicode MS Regular'],
                    'text-allow-overlap': false,
                    'text-ignore-placement': false
                },
                paint: {
                    'text-color': '#333',
                    'text-halo-color': 'rgba(255,255,255,0.9)',
                    'text-halo-width': 1.5
                }
            });
        }

        // Set up interactivity
        setupInteractivity();
        updateLegend(currentLayer);
        applyMapVisualFocus();
        updateMapStateChips();

        // Add Urban Pulse layers above county fills (disabled by default)
        if (URBAN_PULSE_ENABLED) {
            addUrbanPulseLayers();
        }

        // Setup county search autocomplete
        setupCountySearch();

        // Hide loading screen
        document.getElementById('loading').style.display = 'none';

        // Restore state from URL hash (after map is fully loaded)
        restoreFromHash();

    } catch (error) {
        console.error('Error loading map data:', error);
        document.getElementById('loading').innerHTML = `
            <div class="spinner"></div>
            <p>Error loading map data</p>
            <p class="text-sm text-muted mt-3">
                ${error.message}
            </p>
        `;
    }
});

function addUrbanPulseLayers() {
    if (!map.getSource('pulse-circle')) {
        map.addSource('pulse-circle', {
            type: 'geojson',
            data: { type: 'FeatureCollection', features: [] }
        });
    }
    if (!map.getSource('pulse-mask')) {
        map.addSource('pulse-mask', {
            type: 'geojson',
            data: { type: 'FeatureCollection', features: [] }
        });
    }

    if (!map.getLayer('pulse-mask')) {
        map.addLayer({
            id: 'pulse-mask',
            type: 'fill',
            source: 'pulse-mask',
            paint: {
                'fill-color': 'rgba(0,0,0,0.25)',
                'fill-opacity': 0
            }
        });
    }

    if (!map.getLayer('pulse-fill')) {
        map.addLayer({
            id: 'pulse-fill',
            type: 'fill',
            source: 'pulse-circle',
            paint: {
                'fill-color': '#ffffff',
                'fill-opacity': 0.08
            }
        });
    }

    if (!map.getLayer('pulse-outline')) {
        map.addLayer({
            id: 'pulse-outline',
            type: 'line',
            source: 'pulse-circle',
            paint: {
                'line-color': '#ffffff',
                'line-width': 3,
                'line-opacity': 0.7
            }
        });
    }

    Object.entries(URBAN_PULSE_THEMES).forEach(([key, theme]) => {
        const layerId = `urban-${key}-poi`;
        if (map.getLayer(layerId)) {
            return;
        }
        map.addLayer({
            id: layerId,
            type: 'circle',
            source: 'composite',
            'source-layer': 'poi_label',
            minzoom: 10,
            filter: ['in', ['get', 'class'], ['literal', theme.classes]],
            paint: {
                'circle-color': theme.color,
                'circle-radius': [
                    'interpolate',
                    ['linear'],
                    ['zoom'],
                    10, 2,
                    14, 5
                ],
                'circle-opacity': [
                    'interpolate',
                    ['linear'],
                    ['zoom'],
                    10, 0,
                    11, 0.35,
                    14, 0.6
                ],
                'circle-stroke-color': '#ffffff',
                'circle-stroke-width': 1
            }
        });

        map.on('mouseenter', layerId, () => {
            map.getCanvas().style.cursor = 'pointer';
        });
        map.on('mouseleave', layerId, () => {
            map.getCanvas().style.cursor = '';
        });
    });
}

function getPulseFeatureAt(point) {
    if (!URBAN_PULSE_ENABLED) {
        return null;
    }
    const existingLayers = map.getStyle().layers || [];
    const hasPulseLayers = URBAN_PULSE_LAYER_IDS.some((id) => existingLayers.find((layer) => layer.id === id));
    if (!hasPulseLayers) {
        return null;
    }
    const features = map.queryRenderedFeatures(point, { layers: URBAN_PULSE_LAYER_IDS });
    if (!features.length) {
        return null;
    }
    const hit = features[0];
    const layerId = hit.layer.id;
    const themeKey = layerId.replace('urban-', '').replace('-poi', '');
    return { feature: hit, themeKey };
}

function buildMaskPolygon(circleFeature) {
    const world = [
        [-180, -90],
        [180, -90],
        [180, 90],
        [-180, 90],
        [-180, -90]
    ];
    const hole = circleFeature.geometry.coordinates[0].slice().reverse();
    return {
        type: 'Feature',
        geometry: {
            type: 'Polygon',
            coordinates: [world, hole]
        },
        properties: {}
    };
}

function setPulseLayerColor(color) {
    if (map.getLayer('pulse-fill')) {
        map.setPaintProperty('pulse-fill', 'fill-color', color);
    }
    if (map.getLayer('pulse-outline')) {
        map.setPaintProperty('pulse-outline', 'line-color', color);
    }
}

function animatePulse(center, color) {
    if (!window.turf) {
        return;
    }
    const start = performance.now();
    const duration = 700;

    setPulseLayerColor(color);

    function frame(now) {
        const t = Math.min(1, (now - start) / duration);
        const eased = t * t * (3 - 2 * t);
        const radius = eased * PULSE_RADIUS_MILES;
        const circle = turf.circle(center, Math.max(radius, 0.01), { steps: 96, units: 'miles' });
        const mask = buildMaskPolygon(circle);

        map.getSource('pulse-circle').setData(circle);
        map.getSource('pulse-mask').setData(mask);
        map.setPaintProperty('pulse-mask', 'fill-opacity', 0.25);
        map.setPaintProperty('pulse-fill', 'fill-opacity', 0.12);
        map.setPaintProperty('pulse-outline', 'line-opacity', 0.9);

        if (t < 1) {
            requestAnimationFrame(frame);
        }
    }

    requestAnimationFrame(frame);
}

function clearPulse() {
    activePulse = null;
    if (map.getSource('pulse-circle')) {
        map.getSource('pulse-circle').setData({ type: 'FeatureCollection', features: [] });
    }
    if (map.getSource('pulse-mask')) {
        map.getSource('pulse-mask').setData({ type: 'FeatureCollection', features: [] });
    }
    if (map.getLayer('pulse-mask')) {
        map.setPaintProperty('pulse-mask', 'fill-opacity', 0);
    }
}

function getFeatureCenter(feature) {
    if (!window.turf) {
        return feature.geometry.coordinates;
    }
    if (feature.geometry.type === 'Point') {
        return feature.geometry.coordinates;
    }
    if (feature.geometry.type === 'MultiPoint') {
        return feature.geometry.coordinates[0];
    }
    return turf.centerOfMass(feature).geometry.coordinates;
}

function countNearbyFeatures(themeKey, circle) {
    const layerId = `urban-${themeKey}-poi`;
    const candidates = map.queryRenderedFeatures({ layers: [layerId] });
    if (!window.turf || !candidates.length) {
        return 0;
    }
    let count = 0;
    candidates.forEach((feature) => {
        const point = getFeatureCenter(feature);
        if (turf.booleanPointInPolygon(turf.point(point), circle)) {
            count += 1;
        }
    });
    return count;
}

function openPulsePanel(themeKey, featureName, nearbyCount) {
    const panel = document.getElementById('side-panel');
    const titleEl = document.getElementById('panel-title');
    const subtitleEl = document.getElementById('panel-subtitle');
    const contentEl = document.getElementById('panel-content');
    if (!panel || !titleEl || !subtitleEl || !contentEl) {
        return;
    }

    panel.classList.remove('pulse-live', 'pulse-work', 'pulse-learn', 'pulse-transit');
    panel.classList.add(`pulse-${themeKey}`);

    const theme = URBAN_PULSE_THEMES[themeKey];
    titleEl.textContent = featureName || `${theme.label} Location`;
    subtitleEl.textContent = `Urban Pulse • ${theme.label}`;

    contentEl.innerHTML = `
        <div class="panel-section">
            <div class="pulse-badge">
                <span class="pulse-dot" style="background: ${theme.color};"></span>
                ${theme.label.toUpperCase()} SIGNAL
            </div>
            <p class="text-md text-muted">
                Showing nearby ${theme.label.toLowerCase()}-related places within a 5-mile pulse radius.
            </p>
            <div class="pulse-metric">
                <div class="pulse-count">${nearbyCount}</div>
                <div class="pulse-label">Nearby ${theme.label} locations</div>
            </div>
        </div>
    `;

    document.getElementById('side-panel').setAttribute('data-compare-ready', 'false');
    document.getElementById('side-panel').setAttribute('data-compare-mode', 'off');
    setCompareButtonState(false);
    hideFloatingAnalysisPanel();
    setPanelOpenState(true);
    document.getElementById('side-panel').classList.add('open');
}

function handlePulseSelection(feature, themeKey) {
    if (!window.turf || !feature || !themeKey) {
        return;
    }

    clearPulse();

    const center = getFeatureCenter(feature);
    const circle = turf.circle(center, PULSE_RADIUS_MILES, { steps: 96, units: 'miles' });
    const nearbyCount = countNearbyFeatures(themeKey, circle);
    const theme = URBAN_PULSE_THEMES[themeKey];

    activePulse = { themeKey, center };
    animatePulse(center, theme.color);

    const name =
        feature.properties?.name ||
        feature.properties?.name_en ||
        feature.properties?.name_local ||
        `${theme.label} location`;

    openPulsePanel(themeKey, name, nearbyCount);

    map.easeTo({
        center,
        zoom: Math.max(map.getZoom(), 15),
        pitch: MAX_PITCH,
        duration: MAP_EASE_DURATION_MS
    });
}


function add3DBuildings() {
    if (LOW_PERFORMANCE_MODE) {
        return;
    }
    const styleLayers = map.getStyle().layers || [];
    const labelLayerId = styleLayers.find(
        (layer) => layer.type === 'symbol' && layer.layout && layer.layout['text-field']
    )?.id;

    if (!map.getSource('composite') || map.getLayer('3d-buildings')) {
        return;
    }

    map.addLayer({
        id: '3d-buildings',
        source: 'composite',
        'source-layer': 'building',
        type: 'fill-extrusion',
        minzoom: 12.2,
        paint: {
            'fill-extrusion-color': '#cbd3ce',
            'fill-extrusion-opacity': 0.8,
            'fill-extrusion-height': [
                'interpolate',
                ['linear'],
                ['zoom'],
                12.2, 0,
                13.4, ['get', 'height']
            ],
            'fill-extrusion-base': [
                'interpolate',
                ['linear'],
                ['zoom'],
                12.2, 0,
                13.4, ['get', 'min_height']
            ]
        }
    }, labelLayerId);
}

// Get fill expression for synthesis grouping
function getSynthesisFillExpression() {
    return [
        'match',
        ['get', 'synthesis_grouping'],
        'emerging_tailwinds', SYNTHESIS_COLORS.emerging_tailwinds,
        'conditional_growth', SYNTHESIS_COLORS.conditional_growth,
        'stable_constrained', SYNTHESIS_COLORS.stable_constrained,
        'at_risk_headwinds', SYNTHESIS_COLORS.at_risk_headwinds,
        'high_uncertainty', SYNTHESIS_COLORS.high_uncertainty,
        '#cccccc' // fallback
    ];
}

// Get fill expression for directional status
function getDirectionalFillExpression() {
    return [
        'match',
        ['get', 'directional_class'],
        'improving', DIRECTIONAL_COLORS.improving,
        'stable', DIRECTIONAL_COLORS.stable,
        'at_risk', DIRECTIONAL_COLORS.at_risk,
        '#cccccc'
    ];
}

// Get fill expression for confidence level
function getConfidenceFillExpression() {
    return [
        'match',
        ['get', 'confidence_class'],
        'strong', CONFIDENCE_COLORS.strong,
        'conditional', CONFIDENCE_COLORS.conditional,
        'fragile', CONFIDENCE_COLORS.fragile,
        '#cccccc'
    ];
}

// Setup map interactivity
function setupInteractivity() {
    // Change cursor on hover
    map.on('mouseenter', 'counties-fill', () => {
        map.getCanvas().style.cursor = 'pointer';
    });

    map.on('mouseleave', 'counties-fill', () => {
        map.getCanvas().style.cursor = '';
        popup.remove();
        map.setFilter('counties-hover', ['==', 'fips_code', '']);
        setHoveredResult(null);
    });

    // Show tooltip on hover (debounced for performance)
    const handleHover = debounceHover((e) => {
        if (!e || !e.features || e.features.length === 0) {
            popup.remove();
            return;
        }
        if (e.features.length > 0) {
            const feature = e.features[0];
            const props = feature.properties;

            // Update hover highlight
            map.setFilter('counties-hover', ['==', 'fips_code', props.fips_code]);
            setHoveredResult(props.fips_code);

            // Get current layer label
            const label = currentLayer === 'synthesis'
                ? SYNTHESIS_LABELS[props.synthesis_grouping]
                : currentLayer === 'directional'
                ? DIRECTIONAL_LABELS[props.directional_class]
                : CONFIDENCE_LABELS[props.confidence_class];

            // Get grouping class for color styling based on active layer
            const groupingClass = currentLayer === 'synthesis'
                ? (props.synthesis_grouping || 'high_uncertainty')
                : currentLayer === 'directional'
                ? (props.directional_class || 'stable')
                : (props.confidence_class || 'conditional');

            // Format composite score
            const score = props.composite_score !== null && props.composite_score !== undefined
                ? parseFloat(props.composite_score).toFixed(2)
                : 'N/A';

            // Layer-specific detail line
            let layerDetail = '';
            if (currentLayer === 'directional') {
                layerDetail = `<div class="tooltip-score">
                    <span class="tooltip-score-label">Trajectory:</span>
                    <span class="tooltip-score-value">${DIRECTIONAL_LABELS[props.directional_class] || 'Unknown'}</span>
                </div>`;
            } else if (currentLayer === 'confidence') {
                layerDetail = `<div class="tooltip-score">
                    <span class="tooltip-score-label">Confidence:</span>
                    <span class="tooltip-score-value">${CONFIDENCE_LABELS[props.confidence_class] || 'Unknown'}</span>
                </div>`;
            }

            // Enhanced tooltip with score + sparkline
            const sparkline = renderTooltipSparkline(props);
            popup
                .setLngLat(e.lngLat)
                .setHTML(`
                    <div class="tooltip-title">${props.county_name}</div>
                    <div class="tooltip-score">
                        <span class="tooltip-score-label">Composite score (0-1):</span>
                        <span class="tooltip-score-value">${score}</span>
                    </div>
                    ${layerDetail}
                    <div class="tooltip-grouping ${groupingClass}">${label}</div>
                    ${sparkline}
                `)
                .addTo(map);
        }
    });

    map.on('mousemove', 'counties-fill', handleHover);

    // Click to show detail panel (or compare second county)
    map.on('click', 'counties-fill', async (e) => {
        const pulseHit = getPulseFeatureAt(e.point);
        if (pulseHit) {
            handlePulseSelection(pulseHit.feature, pulseHit.themeKey);
            return;
        }
        if (e.features.length > 0) {
            const feature = e.features[0];
            const fipsCode = feature.properties.fips_code;

            // Check if compare mode is active
            const panel = document.getElementById('side-panel');
            if (panel && panel.getAttribute('data-compare-mode') === 'active' && compareCountyA) {
                // Load second county for comparison
                await loadCompareCounty(fipsCode);
            } else {
                focusCountyByFips(fipsCode, false);
                await loadCountyDetail(fipsCode);
            }
            setHoveredResult(fipsCode);
        }
    });

}

// Switch map layer with cross-fade animation
let _layerTransitionInProgress = false;

function switchLayer(layer) {
    if (layer === currentLayer || _layerTransitionInProgress) return;

    let fillExpression;
    switch(layer) {
        case 'synthesis':
            fillExpression = getSynthesisFillExpression();
            break;
        case 'directional':
            fillExpression = getDirectionalFillExpression();
            break;
        case 'confidence':
            fillExpression = getConfidenceFillExpression();
            break;
    }

    // Clear any active filters when switching layers
    activeLegendFilter = null;
    if (activeBivariateFilter) {
        activeBivariateFilter = null;
        updateBivariateCellStyles();
    }
    currentLayer = layer;
    updateLegend(layer);
    updateMapStateChips();
    renderCountyResults();
    updateUrlHash();

    // Sync extrusion layer color if active
    if (extrusionActive && map.getLayer('counties-extrusion')) {
        map.setPaintProperty('counties-extrusion', 'fill-extrusion-color', fillExpression);
    }

    // Animate: fade out → swap color → fade in
    const FADE_MS = 320;
    const fillLayer = 'counties-fill';
    if (!map.getLayer(fillLayer) || PREFERS_REDUCED_MOTION) {
        map.setPaintProperty(fillLayer, 'fill-color', fillExpression);
        applyMapVisualFocus();
        return;
    }

    _layerTransitionInProgress = true;
    const startOpacity = DEFAULT_FILL_OPACITY;
    const fadeStart = performance.now();

    // Phase 1: fade out
    function fadeOut(now) {
        const t = Math.min(1, (now - fadeStart) / FADE_MS);
        const eased = t * t;
        const opacity = startOpacity * (1 - eased);
        map.setPaintProperty(fillLayer, 'fill-opacity', Math.max(0.02, opacity));
        if (t < 1) {
            requestAnimationFrame(fadeOut);
        } else {
            // Swap color at minimum opacity
            map.setPaintProperty(fillLayer, 'fill-color', fillExpression);
            requestAnimationFrame(fadeIn);
        }
    }

    // Phase 2: fade in
    const fadeInStart = { value: 0 };
    function fadeIn(now) {
        if (!fadeInStart.ts) fadeInStart.ts = now;
        const t = Math.min(1, (now - fadeInStart.ts) / FADE_MS);
        const eased = t * (2 - t); // ease-out-quad
        const opacity = 0.02 + (startOpacity - 0.02) * eased;
        map.setPaintProperty(fillLayer, 'fill-opacity', opacity);
        if (t < 1) {
            requestAnimationFrame(fadeIn);
        } else {
            _layerTransitionInProgress = false;
            applyMapVisualFocus();
        }
    }

    requestAnimationFrame(fadeOut);
}

// Legend category descriptions per layer
const SYNTHESIS_DESCRIPTIONS = {
    'emerging_tailwinds': 'High upside with reinforcing strengths',
    'conditional_growth': 'Upside exists, but delivery risk matters',
    'stable_constrained': 'Balanced conditions with limited upside',
    'at_risk_headwinds': 'Headwinds currently outweigh strengths',
    'high_uncertainty': 'Thin coverage; interpret cautiously'
};

const DIRECTIONAL_DESCRIPTIONS = {
    'improving': 'Signals are strengthening',
    'stable': 'Signals are mixed and mostly steady',
    'at_risk': 'Signals are deteriorating'
};

const CONFIDENCE_DESCRIPTIONS = {
    'strong': 'Robust coverage across layers',
    'conditional': 'Partial coverage; use with caution',
    'fragile': 'Sparse coverage; validate locally'
};

// Update legend when layer changes
function updateLegend(layer) {
    const legendContent = document.getElementById('legend-content');
    const legendTitle = document.querySelector('.legend-header h3');
    if (!legendContent || !legendTitle) return;

    let colors, labels, descriptions;

    switch(layer) {
        case 'synthesis':
            legendTitle.textContent = LAYER_VIEW_LABELS.synthesis;
            colors = SYNTHESIS_COLORS;
            labels = SYNTHESIS_LABELS;
            descriptions = SYNTHESIS_DESCRIPTIONS;
            break;
        case 'directional':
            legendTitle.textContent = LAYER_VIEW_LABELS.directional;
            colors = DIRECTIONAL_COLORS;
            labels = DIRECTIONAL_LABELS;
            descriptions = DIRECTIONAL_DESCRIPTIONS;
            break;
        case 'confidence':
            legendTitle.textContent = LAYER_VIEW_LABELS.confidence;
            colors = CONFIDENCE_COLORS;
            labels = CONFIDENCE_LABELS;
            descriptions = CONFIDENCE_DESCRIPTIONS;
            break;
    }

    let html = '';
    for (const [key, color] of Object.entries(colors)) {
        html += `
            <button class="legend-item" data-group="${key}" type="button" aria-pressed="false"
                    aria-label="Filter map to show only ${labels[key]} counties">
                <span class="legend-color" style="background: ${color};"></span>
                <span class="legend-label">${labels[key]}</span>
            </button>
            <div class="legend-description">${descriptions[key]}</div>
        `;
    }
    html += `
        <div class="legend-filter-actions">
            <span class="text-xs text-faint">Legend filter</span>
            <button id="legend-reset" class="legend-reset hidden" type="button" onclick="clearLegendFilter()">Show All</button>
        </div>
    `;

    legendContent.innerHTML = html;
    setupLegendFiltering();
}

function clampScore(value) {
    if (value === null || value === undefined || Number.isNaN(value)) {
        return null;
    }
    return Math.max(0, Math.min(1, value));
}

function renderScoreBar(value) {
    const clamped = clampScore(value);
    if (clamped === null) {
        return '';
    }
    const pct = (clamped * 100).toFixed(0);
    return `
        <div class="score-bar" aria-hidden="true">
            <div class="score-bar-fill" style="width: ${pct}%"></div>
        </div>
    `;
}

// Render a visual context bar for composite score (0-1)
function renderScoreContext(score) {
    if (score === null || score === undefined) return '';
    const pct = Math.max(0, Math.min(100, score * 100));
    return `
        <div class="score-context-bar">
            <div class="score-context-zone-risk" title="At Risk (0-0.3)"></div>
            <div class="score-context-zone-constrained" title="Constrained (0.3-0.5)"></div>
            <div class="score-context-zone-conditional" title="Conditional (0.5-0.7)"></div>
            <div class="score-context-zone-tailwinds" title="Tailwinds (0.7-1.0)"></div>
            <div class="score-context-marker" style="left: ${pct}%"></div>
        </div>
        <div class="score-context-labels">
            <span>At Risk</span>
            <span>Constrained</span>
            <span>Conditional</span>
            <span>Tailwinds</span>
        </div>
    `;
}

function classifyScoreBand(score) {
    if (score === null || score === undefined || Number.isNaN(score)) {
        return 'No score available';
    }
    if (score >= 0.67) return 'Strong';
    if (score >= 0.34) return 'Moderate';
    return 'Weak';
}

function renderDecisionLens(data) {
    const directional = DIRECTIONAL_LABELS[data.directional_class] || 'Unknown';
    const confidence = CONFIDENCE_LABELS[data.confidence_class] || 'Unknown';
    const scoreBand = classifyScoreBand(data.composite_score);

    return `
        <div class="panel-section decision-lens">
            <h4>How To Use This</h4>
            <div class="list-item"><strong>Directional Trajectory:</strong> ${directional} describes current pressure direction.</div>
            <div class="list-item"><strong>Evidence Confidence:</strong> ${confidence} indicates how much to trust the signal coverage.</div>
            <div class="list-item"><strong>Composite Score:</strong> ${scoreBand} (${data.composite_score !== null && data.composite_score !== undefined ? data.composite_score.toFixed(3) : 'N/A'}) is relative county standing (0-1).</div>
            <div class="list-item">Use weak + strong confidence as a near-term intervention priority.</div>
        </div>
    `;
}

function renderLayerGuidance(layerKey, scoreValue) {
    const guidance = LAYER_DECISION_GUIDANCE[layerKey];
    if (!guidance) {
        return '';
    }
    const scoreBand = classifyScoreBand(scoreValue);
    return `
        <div class="layer-guidance">
            <h4>What This Means For Decisions</h4>
            <p><strong>Signal:</strong> ${guidance.signal}</p>
            <p><strong>Interpretation:</strong> ${scoreBand} layer strength for this county.</p>
            <p><strong>Action:</strong> ${guidance.use}</p>
        </div>
    `;
}

// Load county detail from API
async function loadCountyDetail(fipsCode) {
    try {
        clearPulse();
        const panel = document.getElementById('side-panel');
        if (panel) {
            panel.classList.remove('pulse-live', 'pulse-work', 'pulse-learn', 'pulse-transit');
        }
        currentFipsCode = fipsCode;
        applyMapVisualFocus();
        updateMapStateChips();
        renderCountyResults();

        // Show loading state immediately
        document.getElementById('panel-title').textContent = 'Loading...';
        document.getElementById('panel-subtitle').textContent = '';
        document.getElementById('panel-content').innerHTML = `
            <div class="panel-section panel-loading">
                <div class="loading-icon"></div>
                <p class="text-md text-muted">Fetching county data...</p>
            </div>
        `;
        setCompareButtonState(false);
        document.getElementById('side-panel').classList.add('open');
        setPanelOpenState(true);

        // Highlight selected county on map
        if (map.getLayer('counties-selected')) {
            map.setFilter('counties-selected', ['==', 'fips_code', fipsCode]);
        }

        const response = await fetch(`${API_BASE_URL}/areas/${fipsCode}`);
        if (!response.ok) {
            throw new Error(`Area detail API returned ${response.status}`);
        }
        const data = await response.json();
        currentCountySummary = data;
        const primaryStrengths = normalizeAnalysisItems(data.primary_strengths);
        const primaryWeaknesses = normalizeAnalysisItems(data.primary_weaknesses);
        const keyTrends = normalizeAnalysisItems(data.key_trends);

        // Populate side panel
        document.getElementById('panel-title').textContent = data.county_name;
        document.getElementById('panel-subtitle').textContent =
            `Data Year: ${data.data_year}`;

        const layerScoresContent = Object.entries(data.layer_scores).map(([key, value]) => {
            const hasValue = value !== null && value !== undefined;
            const displayValue = hasValue ? parseFloat(value).toFixed(3) : '—';
            const nullHint = hasValue ? '' : '<div class="text-xs text-faint mt-1">Click for detail</div>';
            return `
                <div class="score-item clickable" data-layer-key="${key}" onclick="loadLayerDetail('${key}')" title="Click to see factor breakdown">
                    <div class="score-label">${formatLayerName(key)}</div>
                    <div class="score-value ${!hasValue ? 'null' : ''}">
                        ${displayValue}
                    </div>
                    ${renderScoreBar(value)}
                    ${nullHint}
                </div>
            `;
        }).join('');

        const badgeTextColor = data.synthesis_grouping === 'stable_constrained' ? '#333' : 'white';

        const content = `
            <div class="panel-tabs">
                <button class="panel-tab active" onclick="switchPanelTab('summary', this)">Summary</button>
                <button class="panel-tab" onclick="switchPanelTab('scores', this)">Scores</button>
            </div>

            <div id="tab-summary" class="panel-tab-content active">
                <div class="panel-section">
                    <h4>County Growth Synthesis</h4>
                    <div class="synthesis-badge" style="background: ${SYNTHESIS_COLORS[data.synthesis_grouping]}; color: ${badgeTextColor};">
                        ${SYNTHESIS_LABELS[data.synthesis_grouping]}
                    </div>
                    <p class="text-md text-muted mt-3">
                        ${getSynthesisDescription(data.synthesis_grouping)}
                    </p>
                </div>

                <div class="panel-section county-narrative">
                    <h4>County Snapshot</h4>
                    <p class="narrative-text">${generateCountyNarrative(data)}</p>
                </div>

                <div class="panel-section">
                    <h4>Classification Details</h4>
                    <div class="score-grid">
                        <div class="score-item">
                            <div class="score-label">Directional Trajectory</div>
                            <div class="score-value">${DIRECTIONAL_LABELS[data.directional_class]}</div>
                        </div>
                        <div class="score-item">
                            <div class="score-label">Evidence Confidence</div>
                            <div class="score-value">${CONFIDENCE_LABELS[data.confidence_class]}</div>
                        </div>
                        <div class="score-item">
                            <div class="score-label">Composite Score</div>
                            <div class="score-value">${data.composite_score !== null && data.composite_score !== undefined ? data.composite_score.toFixed(3) : 'N/A'}</div>
                            ${renderScoreBar(data.composite_score)}
                        </div>
                        <div class="score-item">
                            <div class="score-label">Data Year</div>
                            <div class="score-value">${data.data_year}</div>
                        </div>
                    </div>
                    ${renderScoreContext(data.composite_score)}
                </div>

                <div class="panel-section panel-meta-row">
                    <div class="flex justify-between items-center">
                        <div class="text-sm text-faint">
                            Data Year: ${data.data_year} | Last Updated: ${new Date(data.last_updated).toLocaleDateString()}
                        </div>
                        <button onclick="exportCountyCSV()" class="btn text-xs">Download CSV</button>
                    </div>
                </div>
            </div>

            <div id="tab-scores" class="panel-tab-content">
                <div class="panel-section">
                    <h4>Layer Scores</h4>
                    <div class="text-xs text-faint mb-2">Click a layer for factor detail</div>
                    <div class="score-grid">
                        ${layerScoresContent}
                    </div>
                </div>

                <div class="panel-section layer-detail-inline" id="layer-detail-inline">
                    <h4 id="layer-detail-title">Layer Detail</h4>
                    <div id="layer-detail-body" class="layer-empty-state">
                        Click a layer score above to see factor breakdown and guidance.
                    </div>
                </div>
            </div>
        `;

        document.getElementById('panel-content').innerHTML = content;
        document.getElementById('side-panel').setAttribute('data-compare-ready', 'true');
        setCompareButtonState(true);
        document.getElementById('side-panel').scrollTop = 0;
        setCompareGuidance(false);
        showFloatingAnalysisPanel({
            county_name: data.county_name,
            primary_strengths: primaryStrengths,
            primary_weaknesses: primaryWeaknesses,
            key_trends: keyTrends,
            data_year: data.data_year
        });
        updateMapStateChips();
        renderCountyResults();
        updateUrlHash();

        // Focus management for accessibility
        const panelTitle = document.getElementById('panel-title');
        if (panelTitle) {
            panelTitle.setAttribute('tabindex', '-1');
            panelTitle.focus();
        }

    } catch (error) {
        console.error('Error loading county detail:', error);
        document.getElementById('panel-title').textContent = 'Connection Error';
        document.getElementById('panel-subtitle').textContent = '';
        document.getElementById('panel-content').innerHTML = `
            <div class="panel-section panel-error">
                <div class="panel-error-icon">&#x26A0;</div>
                <h4 class="text-lg text-primary mb-2">Backend Unavailable</h4>
                <p class="text-md text-muted">
                    County detail data requires the API server.<br>
                    Start it with <code class="code-inline">make serve</code>
                </p>
            </div>
        `;
        document.getElementById('side-panel').classList.add('open');
        setPanelOpenState(true);
        setCompareButtonState(false);
        setCompareGuidance(false);
        hideFloatingAnalysisPanel();
        updateMapStateChips();
        renderCountyResults();
    }
}

// Load layer detail inline in the right-side county panel
async function loadLayerDetail(layerKey) {
    if (!currentFipsCode) {
        console.error('No county selected');
        return;
    }

    const detailTitleEl = document.getElementById('layer-detail-title');
    const detailBodyEl = document.getElementById('layer-detail-body');
    const detailSectionEl = document.getElementById('layer-detail-inline');
    if (!detailTitleEl || !detailBodyEl || !detailSectionEl) {
        console.error('Layer detail inline container not found');
        return;
    }

    detailTitleEl.textContent = `${formatLayerName(layerKey)} Detail`;
    detailBodyEl.innerHTML = '<div class="layer-empty-note">Loading layer detail...</div>';
    document.querySelectorAll('.score-item.clickable').forEach((el) => {
        el.classList.toggle('active', el.getAttribute('data-layer-key') === layerKey);
    });

    try {
        const response = await fetch(`${API_BASE_URL}/areas/${currentFipsCode}/layers/${layerKey}`);

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = await response.json();
        const scoreFromSummary = currentCountySummary?.layer_scores?.[layerKey];
        const effectiveScore = data.score !== null && data.score !== undefined
            ? data.score
            : (typeof scoreFromSummary === 'number' ? scoreFromSummary : null);
        detailTitleEl.textContent = `${data.display_name} Detail`;

        // Get trend icon
        const trendIcon = getTrendIcon(data.momentum_direction);
        const trendText = getTrendText(data.momentum_direction, data.momentum_slope);

        // Build factor list
        const availableFactors = data.factors.filter((f) => f.value !== null && f.value !== undefined);
        const missingFactorCount = Math.max(0, data.factors.length - availableFactors.length);

        const factorHtml = availableFactors.map(f => {
            const factorTrend = f.trend ? getTrendIcon(f.trend) : '';
            const weightText = f.weight ? `Weight: ${(f.weight * 100).toFixed(0)}%` : '';

            return `
                <div class="factor-item">
                    <div class="factor-info">
                        <div class="factor-name">${f.name}</div>
                        <div class="factor-desc">${f.description}</div>
                    </div>
                    <div class="factor-value">
                        <div class="factor-value-main">${f.formatted_value || (f.value !== null ? f.value.toFixed(3) : 'N/A')}</div>
                        <div class="factor-weight">${weightText}</div>
                    </div>
                    ${factorTrend ? `<div class="factor-trend ${f.trend}">${factorTrend}</div>` : ''}
                </div>
            `;
        }).join('');

        // Build inline detail content
        const bodyContent = `
            <div class="layer-score-main">
                <div>
                    <div class="text-sm text-muted mb-1">Overall Score</div>
                    <div class="layer-score-value">${effectiveScore !== null ? effectiveScore.toFixed(3) : 'N/A'}</div>
                </div>
                <div class="layer-score-trend">
                    <div class="layer-trend-icon ${data.momentum_direction || ''}">${trendIcon}</div>
                    <div class="layer-trend-text">${trendText}</div>
                </div>
            </div>

            <div class="layer-formula">
                <strong>Formula:</strong> ${data.formula}
            </div>

            ${renderLayerGuidance(layerKey, effectiveScore)}

            <div class="layer-factors">
                <h4>Contributing Factors</h4>
                ${factorHtml || '<div class="layer-empty-state">Detailed factor metrics are not populated for this county/year. Use the layer score and trend for decisions.</div>'}
                ${missingFactorCount > 0 ? `<div class="layer-empty-note">${missingFactorCount} factor(s) hidden because values are unavailable.</div>` : ''}
            </div>

            <div class="layer-metadata">
                <span>Version: ${data.version}</span>
                <span>Data Year: ${data.data_year}</span>
                <span>Coverage: ${data.coverage_years || 'N/A'} years</span>
            </div>
        `;

        detailBodyEl.innerHTML = bodyContent;
        detailSectionEl.scrollIntoView({ behavior: 'smooth', block: 'start' });

    } catch (error) {
        console.error('Error loading layer detail:', error);
        detailTitleEl.textContent = `${formatLayerName(layerKey)} Detail`;
        detailBodyEl.innerHTML = `
            <div class="layer-empty-state">
                <p>Unable to load detailed factor breakdown.</p>
                <p class="text-sm mt-3">The data may not be available for this layer in this year.</p>
            </div>
        `;
        detailSectionEl.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
}

// Get trend icon based on direction
function getTrendIcon(direction) {
    switch(direction) {
        case 'up': return '↑';
        case 'down': return '↓';
        case 'stable': return '→';
        default: return '';
    }
}

// Get trend text based on direction and slope
function getTrendText(direction, slope) {
    if (!direction) return 'No trend data';

    const pct = slope ? `${slope > 0 ? '+' : ''}${slope.toFixed(1)}%` : '';

    switch(direction) {
        case 'up': return `Improving ${pct}`;
        case 'down': return `Declining ${pct}`;
        case 'stable': return 'Stable';
        default: return 'No trend data';
    }
}

// Close side panel
function closePanel() {
    document.getElementById('side-panel').classList.remove('open');
    setPanelOpenState(false);
    const panel = document.getElementById('side-panel');
    if (panel) {
        panel.setAttribute('data-compare-ready', 'false');
        panel.setAttribute('data-compare-mode', 'off');
        panel.classList.remove('pulse-live', 'pulse-work', 'pulse-learn', 'pulse-transit');
    }
    setCompareButtonState(false);
    compareCountyA = null;
    compareCountyB = null;
    clearPulse();
    // Hide clear selection button
    // Clear map selection highlight
    if (map.getLayer('counties-hover')) {
        map.setFilter('counties-hover', ['==', 'fips_code', '']);
    }
    if (map.getLayer('counties-selected')) {
        map.setFilter('counties-selected', ['==', 'fips_code', '']);
    }
    currentFipsCode = null;
    currentCountySummary = null;
    document.getElementById('panel-title').textContent = 'County Selected';
    document.getElementById('panel-subtitle').textContent = 'No county selected yet';
    document.getElementById('panel-content').innerHTML = `
        <div class="panel-empty-state">
            County analysis, layer scores, and strengths/weaknesses will appear here after selection.
        </div>
    `;
    hideFloatingAnalysisPanel();
    setHoveredResult(null);
    setCompareGuidance(false);
    applyMapVisualFocus();
    updateMapStateChips();
    renderCountyResults();
    updateUrlHash();

    // Return focus to map for accessibility
    if (map && map.getCanvas()) {
        map.getCanvas().focus();
    }
}

// Clear selection (called from header button)
function clearSelection() {
    closePanel();
}

// Toggle legend visibility
function toggleLegend() {
    const legend = document.getElementById('legend');
    const toggleIcon = document.getElementById('legend-toggle-icon');
    const legendHeader = legend.querySelector('.legend-header');

    legend.classList.toggle('expanded');

    const isExpanded = legend.classList.contains('expanded');
    toggleIcon.textContent = isExpanded ? '−' : '+';
    legendHeader.setAttribute('aria-expanded', isExpanded);
}

function setupLegendFiltering() {
    const legendItems = document.querySelectorAll('.legend-item[data-group]');
    legendItems.forEach((item) => {
        item.addEventListener('click', () => {
            const group = item.getAttribute('data-group');
            toggleLegendFilter(group);
        });
    });
}

function toggleLegendFilter(group) {
    // Clear bivariate filter when using regular legend filter
    if (activeBivariateFilter) {
        activeBivariateFilter = null;
        updateBivariateCellStyles();
    }
    if (activeLegendFilter === group) {
        clearLegendFilter();
        return;
    }
    activeLegendFilter = group;
    applyLegendFilter();
}

function applyLegendFilter() {
    const legendItems = document.querySelectorAll('.legend-item[data-group]');
    const resetButton = document.getElementById('legend-reset');
    applyMapVisualFocus();

    legendItems.forEach((item) => {
        const group = item.getAttribute('data-group');
        const isActive = activeLegendFilter === group;
        item.classList.toggle('active', isActive);
        item.classList.toggle('dimmed', activeLegendFilter !== null && !isActive);
        item.setAttribute('aria-pressed', isActive ? 'true' : 'false');
    });

    if (resetButton) {
        resetButton.classList.toggle('hidden', activeLegendFilter === null);
    }

    updateMapStateChips();
    renderCountyResults();
}

function clearLegendFilter() {
    activeLegendFilter = null;
    applyLegendFilter();
}

// ── Bivariate Legend Click Filter ──
function toggleBivariateFilter(dir, conf) {
    // If same filter is already active, clear it
    if (activeBivariateFilter && activeBivariateFilter.dir === dir && activeBivariateFilter.conf === conf) {
        clearBivariateFilter();
        return;
    }

    // Clear regular legend filter if active
    if (activeLegendFilter) {
        activeLegendFilter = null;
        applyLegendFilter();
    }

    activeBivariateFilter = { dir, conf };
    applyMapVisualFocus();
    updateBivariateCellStyles();
}

function clearBivariateFilter() {
    activeBivariateFilter = null;
    applyMapVisualFocus();
    updateBivariateCellStyles();
}

function updateBivariateCellStyles() {
    document.querySelectorAll('.bivariate-cell[data-dir]').forEach(cell => {
        const isMatch = activeBivariateFilter &&
            cell.dataset.dir === activeBivariateFilter.dir &&
            cell.dataset.conf === activeBivariateFilter.conf;
        cell.classList.toggle('active', isMatch);
    });
}

function setupBivariateFilter() {
    document.querySelectorAll('.bivariate-cell[data-dir]').forEach(cell => {
        cell.addEventListener('click', () => {
            toggleBivariateFilter(cell.dataset.dir, cell.dataset.conf);
        });
        cell.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' || e.key === ' ') {
                e.preventDefault();
                toggleBivariateFilter(cell.dataset.dir, cell.dataset.conf);
            }
        });
    });
}

function toggleLayerScores() {
    const section = document.getElementById('layer-scores');
    const content = document.getElementById('layer-scores-content');
    const toggle = section ? section.querySelector('.section-toggle') : null;

    if (!section || !content || !toggle) {
        return;
    }

    const isExpanded = section.classList.toggle('expanded');
    toggle.setAttribute('aria-expanded', isExpanded ? 'true' : 'false');
}

function toggleCompareMode() {
    const panel = document.getElementById('side-panel');
    const button = document.querySelector('.compare-toggle');
    if (!panel || !button) {
        return;
    }
    if (panel.getAttribute('data-compare-ready') !== 'true') {
        return;
    }
    const isActive = panel.getAttribute('data-compare-mode') === 'active';
    if (!isActive) {
        // Entering compare mode — store current county as A
        compareCountyA = currentCountySummary ? { ...currentCountySummary } : null;
        compareCountyB = null;
        panel.setAttribute('data-compare-mode', 'active');
        button.setAttribute('aria-pressed', 'true');
        button.textContent = 'Select 2nd County';
        setCompareGuidance(true);
    } else {
        // Exiting compare mode
        compareCountyA = null;
        compareCountyB = null;
        panel.setAttribute('data-compare-mode', 'off');
        button.setAttribute('aria-pressed', 'false');
        button.textContent = 'Compare';
        setCompareGuidance(false);
        // Reload original county detail if we have a FIPS code
        if (currentFipsCode) {
            loadCountyDetail(currentFipsCode);
        }
    }
}

// Load second county for comparison
async function loadCompareCounty(fipsCode) {
    if (fipsCode === compareCountyA?.fips_code) {
        return; // Can't compare county to itself
    }

    try {
        const response = await fetch(`${API_BASE_URL}/areas/${fipsCode}`);
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }
        compareCountyB = await response.json();
        renderComparisonPanel(compareCountyA, compareCountyB);
    } catch (error) {
        console.error('Error loading compare county:', error);
    }
}

// Render side-by-side comparison panel
function renderComparisonPanel(dataA, dataB) {
    if (!dataA || !dataB) return;

    const panel = document.getElementById('side-panel');
    hideFloatingAnalysisPanel();
    const strengthsA = normalizeAnalysisItems(dataA.primary_strengths);
    const strengthsB = normalizeAnalysisItems(dataB.primary_strengths);
    const weaknessesA = normalizeAnalysisItems(dataA.primary_weaknesses);
    const weaknessesB = normalizeAnalysisItems(dataB.primary_weaknesses);
    document.getElementById('panel-title').textContent = 'County Comparison';
    document.getElementById('panel-subtitle').textContent = `${dataA.county_name} vs ${dataB.county_name}`;

    const layerKeys = Object.keys(dataA.layer_scores);
    const scoresHtml = layerKeys.map(key => {
        const valA = dataA.layer_scores[key];
        const valB = dataB.layer_scores[key];
        const fmtA = valA !== null ? parseFloat(valA).toFixed(3) : 'N/A';
        const fmtB = valB !== null ? parseFloat(valB).toFixed(3) : 'N/A';
        const diff = (valA !== null && valB !== null) ? (valB - valA) : null;
        const diffStr = diff !== null ? (diff > 0 ? `+${diff.toFixed(3)}` : diff.toFixed(3)) : '';
        const diffColor = diff !== null ? (diff > 0 ? '#2e7d32' : diff < 0 ? '#c62828' : '#666') : '#666';
        return `
            <tr>
                <td class="fw-medium">${formatLayerName(key)}</td>
                <td class="text-right">${fmtA}</td>
                <td class="text-right">${fmtB}</td>
                <td class="text-right text-sm" style="color: ${diffColor};">${diffStr}</td>
            </tr>
        `;
    }).join('');

    const content = `
        <div class="panel-section">
            <div class="flex gap-3 mb-4">
                <div class="compare-card">
                    <div class="compare-label">County A</div>
                    <div class="compare-name">${dataA.county_name}</div>
                    <div class="synthesis-badge" style="background: ${SYNTHESIS_COLORS[dataA.synthesis_grouping]}; color: ${dataA.synthesis_grouping === 'stable_constrained' ? '#333' : 'white'}; margin-top: 8px; font-size: 11px;">
                        ${SYNTHESIS_LABELS[dataA.synthesis_grouping]}
                    </div>
                    <div class="compare-score">${dataA.composite_score?.toFixed(3) || 'N/A'}</div>
                    <div class="compare-score-label">Composite Score</div>
                </div>
                <div class="compare-card">
                    <div class="compare-label">County B</div>
                    <div class="compare-name">${dataB.county_name}</div>
                    <div class="synthesis-badge" style="background: ${SYNTHESIS_COLORS[dataB.synthesis_grouping]}; color: ${dataB.synthesis_grouping === 'stable_constrained' ? '#333' : 'white'}; margin-top: 8px; font-size: 11px;">
                        ${SYNTHESIS_LABELS[dataB.synthesis_grouping]}
                    </div>
                    <div class="compare-score">${dataB.composite_score?.toFixed(3) || 'N/A'}</div>
                    <div class="compare-score-label">Composite Score</div>
                </div>
            </div>
        </div>

        <div class="panel-section">
            <h4>Layer Score Comparison</h4>
            <table class="compare-table">
                <thead>
                    <tr>
                        <th>Layer</th>
                        <th class="text-right">${dataA.county_name.split(' ')[0]}</th>
                        <th class="text-right">${dataB.county_name.split(' ')[0]}</th>
                        <th class="text-right">Diff</th>
                    </tr>
                </thead>
                <tbody>
                    ${scoresHtml}
                </tbody>
            </table>
        </div>

        <div class="panel-section">
            <div class="flex gap-3">
                <div class="flex-1">
                    <h4>${dataA.county_name.split(' ')[0]} Strengths</h4>
                    ${strengthsA.length ? strengthsA.map(s => `<div class="list-item text-base">&#10003; ${s}</div>`).join('') : '<div class="list-item text-base">No strengths reported.</div>'}
                </div>
                <div class="flex-1">
                    <h4>${dataB.county_name.split(' ')[0]} Strengths</h4>
                    ${strengthsB.length ? strengthsB.map(s => `<div class="list-item text-base">&#10003; ${s}</div>`).join('') : '<div class="list-item text-base">No strengths reported.</div>'}
                </div>
            </div>
        </div>

        <div class="panel-section">
            <div class="flex gap-3">
                <div class="flex-1">
                    <h4>${dataA.county_name.split(' ')[0]} Weaknesses</h4>
                    ${weaknessesA.length ? weaknessesA.map(w => `<div class="list-item text-base">&#9888; ${w}</div>`).join('') : '<div class="list-item text-base">No weaknesses reported.</div>'}
                </div>
                <div class="flex-1">
                    <h4>${dataB.county_name.split(' ')[0]} Weaknesses</h4>
                    ${weaknessesB.length ? weaknessesB.map(w => `<div class="list-item text-base">&#9888; ${w}</div>`).join('') : '<div class="list-item text-base">No weaknesses reported.</div>'}
                </div>
            </div>
        </div>
    `;

    document.getElementById('panel-content').innerHTML = content;
    setCompareGuidance(false);

    // Reset compare UI
    const button = document.querySelector('.compare-toggle');
    if (button) {
        button.textContent = 'Exit Compare';
    }
}

// Switch panel tabs
function switchPanelTab(tabId, clickedTab) {
    // Update tab buttons
    document.querySelectorAll('.panel-tab').forEach(t => t.classList.remove('active'));
    if (clickedTab) clickedTab.classList.add('active');

    // Update tab content
    document.querySelectorAll('.panel-tab-content').forEach(c => c.classList.remove('active'));
    const target = document.getElementById(`tab-${tabId}`);
    if (target) target.classList.add('active');
}

// Keyboard support for legend toggle
document.addEventListener('DOMContentLoaded', () => {
    setupExtrusionToggle();
    setupBivariateFilter();
    updateMapStateChips();
    setupAnalysisPanel();
    setupLayoutResizer();
    const legendHeader = document.querySelector('.legend-header');
    if (legendHeader) {
        legendHeader.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' || e.key === ' ') {
                e.preventDefault();
                toggleLegend();
            }
        });
    }

    // Auto-collapse legend on mobile
    if (window.matchMedia('(max-width: 768px)').matches) {
        const legend = document.getElementById('legend');
        if (legend && legend.classList.contains('expanded')) {
            toggleLegend();
        }
    }

});

// Get synthesis grouping description
function getSynthesisDescription(grouping) {
    const descriptions = {
        'emerging_tailwinds': 'Multiple reinforcing tailwinds are present across available real-data layers. Persistence is likely if current conditions hold.',
        'conditional_growth': 'Upside exists, but delivery risk and local context drive outcomes. Signals are mixed across layers.',
        'stable_constrained': 'Systems are steady with balanced pressures, but upside is limited under current conditions.',
        'at_risk_headwinds': 'Structural headwinds dominate, creating challenges for growth capacity and resilience.',
        'high_uncertainty': 'Coverage is thin or inconsistent across layers; interpret cautiously and prioritize local validation.'
    };
    return descriptions[grouping] || 'No description available.';
}

// Generate a 2-3 sentence narrative about a county from its data
function generateCountyNarrative(data) {
    const name = data.county_name || 'This county';
    const groupLabel = SYNTHESIS_LABELS[data.synthesis_grouping] || 'Unclassified';
    const strengths = normalizeAnalysisItems(data.primary_strengths);
    const weaknesses = normalizeAnalysisItems(data.primary_weaknesses);
    const trends = normalizeAnalysisItems(data.key_trends);
    const score = data.composite_score;
    const scoreText = score !== null && score !== undefined ? score.toFixed(2) : null;

    // Opening sentence: classification + score context
    const GROUPING_VERBS = {
        'emerging_tailwinds': 'shows reinforcing structural tailwinds',
        'conditional_growth': 'has growth potential tempered by delivery risk',
        'stable_constrained': 'maintains steady conditions with limited upside',
        'at_risk_headwinds': 'faces structural headwinds that constrain growth',
        'high_uncertainty': 'has sparse data coverage, making assessment uncertain'
    };
    const verb = GROUPING_VERBS[data.synthesis_grouping] || `is classified as ${groupLabel}`;
    let narrative = `${name} ${verb}`;
    if (scoreText) {
        narrative += ` (composite ${scoreText})`;
    }
    narrative += '.';

    // Middle sentence: strengths and weaknesses
    if (strengths.length > 0 && weaknesses.length > 0) {
        const strengthList = strengths.slice(0, 2).join(' and ');
        const weaknessList = weaknesses.slice(0, 2).join(' and ');
        narrative += ` Key strengths include ${strengthList}, while ${weaknessList} present challenges.`;
    } else if (strengths.length > 0) {
        narrative += ` Notable strengths: ${strengths.slice(0, 2).join(' and ')}.`;
    } else if (weaknesses.length > 0) {
        narrative += ` Key challenges include ${weaknesses.slice(0, 2).join(' and ')}.`;
    }

    // Closing sentence: trend
    if (trends.length > 0) {
        narrative += ` ${trends[0]}.`.replace('..', '.');
    }

    return narrative;
}

// Layer score keys and their short labels for sparklines
const SPARKLINE_LAYERS = [
    { key: 'employment_gravity_score', label: 'Emp' },
    { key: 'mobility_optionality_score', label: 'Mob' },
    { key: 'school_trajectory_score', label: 'Sch' },
    { key: 'housing_elasticity_score', label: 'Hsg' },
    { key: 'demographic_momentum_score', label: 'Dem' },
    { key: 'risk_drag_score', label: 'Risk' }
];

// Generate an inline SVG sparkline bar chart from county properties
function renderTooltipSparkline(props) {
    const W = 180, H = 48, barGap = 4, labelH = 10;
    const chartH = H - labelH;
    const barCount = SPARKLINE_LAYERS.length;
    const barW = (W - barGap * (barCount - 1)) / barCount;

    let bars = '';
    let labels = '';

    SPARKLINE_LAYERS.forEach((layer, i) => {
        const raw = props[layer.key];
        const val = (raw !== null && raw !== undefined && raw !== 'null') ? parseFloat(raw) : null;
        const x = i * (barW + barGap);

        if (val !== null && !isNaN(val)) {
            const clamped = Math.max(0, Math.min(1, val));
            const barH = Math.max(2, clamped * chartH);
            const y = chartH - barH;
            const color = clamped >= 0.6 ? '#2d5016' : clamped >= 0.35 ? '#fdd835' : '#f4511e';
            bars += `<rect x="${x}" y="${y}" width="${barW}" height="${barH}" rx="2" fill="${color}" opacity="0.85"/>`;
        } else {
            bars += `<rect x="${x}" y="${chartH - 2}" width="${barW}" height="2" rx="1" fill="#cbd5e1" opacity="0.5"/>`;
        }

        const labelX = x + barW / 2;
        labels += `<text x="${labelX}" y="${H}" text-anchor="middle" fill="#94a3b8" font-size="7" font-family="Inter, sans-serif">${layer.label}</text>`;
    });

    return `<svg width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" xmlns="http://www.w3.org/2000/svg" style="display:block;margin-top:6px;">${bars}${labels}</svg>`;
}

// Format layer name for display
function formatLayerName(key) {
    const names = {
        'employment_gravity': 'Employment',
        'mobility_optionality': 'Mobility',
        'school_trajectory': 'Schools',
        'housing_elasticity': 'Housing',
        'demographic_momentum': 'Demographics',
        'risk_drag': 'Risk Drag'
    };
    return names[key] || key;
}

// ESC key to close panels/modals
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
        closePanel();
        // Also close search results
        const results = document.getElementById('county-search-results');
        if (results) results.classList.remove('visible');
    }
});

// ============================================================
// County Search / Autocomplete
// ============================================================
let countyFeatures = []; // Populated after map data loads
let countySearchQuery = '';
let hoveredResultFips = null;

function getSortValue(featureRecord) {
    const p = featureRecord.feature?.properties || {};
    if (currentLayer === 'directional') {
        return p.directional_class === 'improving' ? 3 : p.directional_class === 'stable' ? 2 : 1;
    }
    if (currentLayer === 'confidence') {
        return p.confidence_class === 'strong' ? 3 : p.confidence_class === 'conditional' ? 2 : 1;
    }
    return Number(p.composite_score ?? -Infinity);
}

function getListMetric(featureRecord) {
    const p = featureRecord.feature?.properties || {};
    if (currentLayer === 'directional') {
        return DIRECTIONAL_LABELS[p.directional_class] || 'Unknown';
    }
    if (currentLayer === 'confidence') {
        return CONFIDENCE_LABELS[p.confidence_class] || 'Unknown';
    }
    if (p.composite_score === null || p.composite_score === undefined) {
        return 'N/A';
    }
    return Number(p.composite_score).toFixed(3);
}

function filterCountyRecords(records) {
    const propertyName = getCurrentLayerPropertyName();
    const query = countySearchQuery.trim().toLowerCase();

    return records.filter((record) => {
        const p = record.feature?.properties || {};
        const matchesFilter = !activeLegendFilter || p[propertyName] === activeLegendFilter;
        const matchesSearch = !query || String(record.name || '').toLowerCase().includes(query);
        return matchesFilter && matchesSearch;
    });
}

function setHoveredResult(fipsCode) {
    hoveredResultFips = fipsCode || null;
    document.querySelectorAll('.county-result-item[data-fips]').forEach((item) => {
        item.classList.toggle('hovered', hoveredResultFips !== null && item.getAttribute('data-fips') === hoveredResultFips);
    });
}

function focusCountyByFips(fipsCode, loadDetail = true) {
    const county = countyFeatures.find((c) => c.fips === fipsCode);
    if (!county) {
        return;
    }

    if (typeof turf !== 'undefined' && county.feature) {
        const centroid = turf.centroid(county.feature);
        map.flyTo({
            center: centroid.geometry.coordinates,
            zoom: 9,
            duration: MAP_FLY_DURATION_MS
        });
    }

    if (loadDetail) {
        loadCountyDetail(fipsCode);
    }
}

function renderCountyResults() {
    const listEl = document.getElementById('county-results-list');
    const countEl = document.getElementById('county-results-count');
    if (!listEl || !countEl) {
        return;
    }

    const visible = filterCountyRecords(countyFeatures)
        .sort((a, b) => {
            const diff = getSortValue(b) - getSortValue(a);
            if (Number.isFinite(diff) && diff !== 0) return diff;
            return a.name.localeCompare(b.name);
        });

    countEl.textContent = `${visible.length}`;

    if (visible.length === 0) {
        listEl.innerHTML = '<div class="county-results-empty">No counties match the current filters.</div>';
        return;
    }

    listEl.innerHTML = visible.map((county) => {
        const isActive = currentFipsCode === county.fips;
        const isHovered = hoveredResultFips === county.fips;
        return `
            <button class="county-result-item ${isActive ? 'active' : ''} ${isHovered ? 'hovered' : ''}" type="button" data-fips="${county.fips}" aria-pressed="${isActive ? 'true' : 'false'}">
                <span class="county-result-name">${county.name}</span>
                <span class="county-result-metric">${getListMetric(county)}</span>
            </button>
        `;
    }).join('');

    listEl.querySelectorAll('.county-result-item[data-fips]').forEach((item) => {
        const fips = item.getAttribute('data-fips');
        item.addEventListener('mouseenter', () => {
            if (map.getLayer('counties-hover')) {
                map.setFilter('counties-hover', ['==', 'fips_code', fips]);
            }
            setHoveredResult(fips);
        });
        item.addEventListener('mouseleave', () => {
            if (map.getLayer('counties-hover')) {
                map.setFilter('counties-hover', ['==', 'fips_code', '']);
            }
            setHoveredResult(null);
        });
        item.addEventListener('click', () => {
            focusCountyByFips(fips, true);
        });
    });
}

function setupCountySearch() {
    const input = document.getElementById('county-search');
    const results = document.getElementById('county-search-results');
    if (!input || !results) return;

    // Extract county data from GeoJSON source
    const source = map.getSource('counties');
    if (source && source._data && source._data.features) {
        countyFeatures = source._data.features.map(f => ({
            name: f.properties.county_name,
            fips: f.properties.fips_code,
            feature: f
        }));
    }

    renderCountyResults();

    let highlightedIndex = -1;

    input.addEventListener('input', () => {
        const query = input.value.trim().toLowerCase();
        countySearchQuery = query;
        renderCountyResults();
        highlightedIndex = -1;

        if (!query) {
            results.classList.remove('visible');
            results.innerHTML = '';
            return;
        }

        const matches = countyFeatures.filter(c =>
            c.name.toLowerCase().includes(query)
        ).slice(0, 10);

        if (matches.length === 0) {
            results.innerHTML = '<div class="county-search-result text-faint" style="cursor: default;">No matches found</div>';
            results.classList.add('visible');
            return;
        }

        results.innerHTML = matches.map((c, i) =>
            `<div class="county-search-result" data-fips="${c.fips}" data-index="${i}">${c.name}</div>`
        ).join('');
        results.classList.add('visible');

        // Click handler for results
        results.querySelectorAll('.county-search-result[data-fips]').forEach(el => {
            el.addEventListener('click', () => {
                selectSearchResult(el.getAttribute('data-fips'));
            });
        });
    });

    // Keyboard navigation
    input.addEventListener('keydown', (e) => {
        const items = results.querySelectorAll('.county-search-result[data-fips]');
        if (!items.length) return;

        if (e.key === 'ArrowDown') {
            e.preventDefault();
            highlightedIndex = Math.min(highlightedIndex + 1, items.length - 1);
            updateHighlight(items, highlightedIndex);
        } else if (e.key === 'ArrowUp') {
            e.preventDefault();
            highlightedIndex = Math.max(highlightedIndex - 1, 0);
            updateHighlight(items, highlightedIndex);
        } else if (e.key === 'Enter' && highlightedIndex >= 0) {
            e.preventDefault();
            const fips = items[highlightedIndex].getAttribute('data-fips');
            selectSearchResult(fips);
        }
    });

    // Close on outside click
    document.addEventListener('click', (e) => {
        if (!input.contains(e.target) && !results.contains(e.target)) {
            results.classList.remove('visible');
        }
    });
}

function updateHighlight(items, index) {
    items.forEach((el, i) => {
        el.classList.toggle('highlighted', i === index);
    });
    if (items[index]) {
        items[index].scrollIntoView({ block: 'nearest' });
    }
}

function selectSearchResult(fipsCode) {
    const input = document.getElementById('county-search');
    const results = document.getElementById('county-search-results');

    // Find the county feature
    const county = countyFeatures.find(c => c.fips === fipsCode);
    if (!county) return;

    // Update input and close dropdown
    input.value = county.name;
    countySearchQuery = county.name.toLowerCase();
    results.classList.remove('visible');
    renderCountyResults();
    focusCountyByFips(fipsCode, true);
    updateUrlHash();
}

// ============================================================
// URL Hash Routing
// ============================================================
function updateUrlHash() {
    const parts = [];
    if (currentFipsCode) parts.push(`county=${currentFipsCode}`);
    const hash = parts.length > 0 ? parts.join('&') : '';
    if (window.location.hash.slice(1) !== hash) {
        history.replaceState(null, '', hash ? `#${hash}` : window.location.pathname);
    }
}

function restoreFromHash() {
    const hash = window.location.hash.slice(1);
    if (!hash) return;

    const params = {};
    hash.split('&').forEach(part => {
        const [key, value] = part.split('=');
        if (key && value) params[key] = value;
    });

    if (params.county) {
        loadCountyDetail(params.county);
    }
}

// ============================================================
// Data Export
// ============================================================
function exportCountyCSV() {
    if (!currentCountySummary) return;
    const d = currentCountySummary;
    const rows = [
        ['Field', 'Value'],
        ['County', d.county_name],
        ['FIPS', d.fips_code],
        ['Data Year', d.data_year],
        ['Synthesis Grouping', d.synthesis_grouping],
        ['Directional Class', d.directional_class],
        ['Confidence Class', d.confidence_class],
        ['Composite Score', d.composite_score],
    ];
    // Add layer scores
    if (d.layer_scores) {
        Object.entries(d.layer_scores).forEach(([key, val]) => {
            rows.push([formatLayerName(key), val !== null ? val : 'N/A']);
        });
    }
    rows.push(['Primary Strengths', d.primary_strengths.join('; ')]);
    rows.push(['Primary Weaknesses', d.primary_weaknesses.join('; ')]);
    rows.push(['Key Trends', d.key_trends.join('; ')]);

    const csv = rows.map(r => r.map(c => `"${String(c).replace(/"/g, '""')}"`).join(',')).join('\n');
    downloadBlob(csv, `${d.county_name.replace(/\s+/g, '_')}_data.csv`, 'text/csv');
}

function exportAllCountiesCSV() {
    const source = map.getSource('counties');
    if (!source || !source._data || !source._data.features) return;

    const features = source._data.features;
    const header = ['County', 'FIPS', 'Synthesis', 'Directional', 'Confidence', 'Composite Score',
        'Employment', 'Mobility', 'Schools', 'Housing', 'Demographics', 'Risk Drag'];

    const rows = features.map(f => {
        const p = f.properties;
        return [
            p.county_name, p.fips_code,
            SYNTHESIS_LABELS[p.synthesis_grouping] || p.synthesis_grouping,
            DIRECTIONAL_LABELS[p.directional_class] || p.directional_class,
            CONFIDENCE_LABELS[p.confidence_class] || p.confidence_class,
            p.composite_score,
            p.employment_gravity_score, p.mobility_optionality_score,
            p.school_trajectory_score, p.housing_elasticity_score,
            p.demographic_momentum_score, p.risk_drag_score
        ].map(c => `"${String(c ?? 'N/A').replace(/"/g, '""')}"`).join(',');
    });

    const csv = header.join(',') + '\n' + rows.join('\n');
    downloadBlob(csv, 'maryland_counties_all.csv', 'text/csv');
}

function downloadBlob(content, filename, type) {
    const blob = new Blob([content], { type });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
}

// ============================================================
// 3D Score Extrusion
// ============================================================
let extrusionActive = false;
const EXTRUSION_MAX_HEIGHT = 40000; // meters for max score (1.0)

function addExtrusionLayer() {
    if (map.getLayer('counties-extrusion')) return;

    // Insert below county labels but above fills
    const beforeLayer = map.getLayer('county-labels') ? 'county-labels' : undefined;

    map.addLayer({
        id: 'counties-extrusion',
        type: 'fill-extrusion',
        source: 'counties',
        paint: {
            'fill-extrusion-color': getSynthesisFillExpression(),
            'fill-extrusion-height': 0,
            'fill-extrusion-base': 0,
            'fill-extrusion-opacity': 0
        }
    }, beforeLayer);
}

function toggleExtrusion() {
    extrusionActive = !extrusionActive;

    const btn = document.getElementById('extrude-toggle');
    if (btn) {
        btn.setAttribute('aria-pressed', extrusionActive ? 'true' : 'false');
    }

    if (!map.getLayer('counties-extrusion')) {
        addExtrusionLayer();
    }

    if (extrusionActive) {
        // Sync color with current layer
        const fillExpr = currentLayer === 'directional'
            ? getDirectionalFillExpression()
            : currentLayer === 'confidence'
                ? getConfidenceFillExpression()
                : getSynthesisFillExpression();
        map.setPaintProperty('counties-extrusion', 'fill-extrusion-color', fillExpr);

        // Animate height up
        map.setPaintProperty('counties-extrusion', 'fill-extrusion-height', [
            'interpolate', ['linear'],
            ['coalesce', ['to-number', ['get', 'composite_score']], 0],
            0, 0,
            1, EXTRUSION_MAX_HEIGHT
        ]);
        map.setPaintProperty('counties-extrusion', 'fill-extrusion-opacity', 0.78);

        // Hide flat fill to avoid z-fighting
        map.setPaintProperty('counties-fill', 'fill-opacity', 0);

        // Tilt the camera for 3D effect
        map.easeTo({
            pitch: 50,
            bearing: -15,
            zoom: Math.max(map.getZoom(), 7.2),
            duration: 800
        });
    } else {
        // Animate back to flat
        map.setPaintProperty('counties-extrusion', 'fill-extrusion-height', 0);
        map.setPaintProperty('counties-extrusion', 'fill-extrusion-opacity', 0);

        // Explicitly restore the 2D fill layer visibility
        map.setPaintProperty('counties-fill', 'fill-opacity', DEFAULT_FILL_OPACITY);
        applyMapVisualFocus(); // apply selection/filter dimming if any

        map.easeTo({
            pitch: 0,
            bearing: 0,
            duration: 600
        });
    }
}

function setupExtrusionToggle() {
    const btn = document.getElementById('extrude-toggle');
    if (!btn || btn.dataset.bound === 'true') return;
    btn.dataset.bound = 'true';
    btn.addEventListener('click', toggleExtrusion);
}

// ============================================================
// Statewide Ranking Table View
// ============================================================
let tableViewActive = false;

function toggleTableView() {
    tableViewActive = !tableViewActive;
    const tableEl = document.getElementById('table-view');
    const mapEl = document.getElementById('map');
    const toggleBtn = document.getElementById('view-toggle');

    if (tableViewActive) {
        document.body.classList.add('table-mode');
        renderTableView();
        tableEl.classList.add('visible');
        mapEl.style.display = 'none';
        hideFloatingAnalysisPanel();
        if (toggleBtn) toggleBtn.textContent = 'Map View';
    } else {
        document.body.classList.remove('table-mode');
        tableEl.classList.remove('visible');
        mapEl.style.display = 'block';
        if (currentCountySummary) {
            showFloatingAnalysisPanel(currentCountySummary);
        }
        if (toggleBtn) toggleBtn.textContent = 'Table View';
        setTimeout(() => map.resize(), 50);
    }
}

function renderTableView() {
    const tableEl = document.getElementById('table-view');
    if (!tableEl) return;

    const source = map.getSource('counties');
    if (!source || !source._data || !source._data.features) return;

    const features = [...source._data.features].sort((a, b) =>
        (b.properties.composite_score || 0) - (a.properties.composite_score || 0)
    );

    let sortCol = 'composite_score';
    let sortDir = 'desc';
    let filterText = '';

    const buildTable = (data) => {
        const filtered = filterText
            ? data.filter(f => f.properties.county_name.toLowerCase().includes(filterText))
            : data;

        return `
            <table class="ranking-table">
                <thead>
                    <tr>
                        <th data-col="rank">#</th>
                        <th data-col="county_name" class="${sortCol === 'county_name' ? (sortDir === 'asc' ? 'sorted-asc' : 'sorted-desc') : ''}">County</th>
                        <th data-col="synthesis_grouping" class="${sortCol === 'synthesis_grouping' ? (sortDir === 'asc' ? 'sorted-asc' : 'sorted-desc') : ''}">Synthesis</th>
                        <th data-col="composite_score" class="${sortCol === 'composite_score' ? (sortDir === 'asc' ? 'sorted-asc' : 'sorted-desc') : ''}">Score</th>
                        <th data-col="directional_class" class="${sortCol === 'directional_class' ? (sortDir === 'asc' ? 'sorted-asc' : 'sorted-desc') : ''}">Trajectory</th>
                        <th data-col="confidence_class" class="${sortCol === 'confidence_class' ? (sortDir === 'asc' ? 'sorted-asc' : 'sorted-desc') : ''}">Confidence</th>
                    </tr>
                </thead>
                <tbody>
                    ${filtered.map((f, i) => {
                        const p = f.properties;
                        const badgeBg = SYNTHESIS_COLORS[p.synthesis_grouping] || '#ccc';
                        const badgeColor = p.synthesis_grouping === 'stable_constrained' ? '#333' : 'white';
                        const score = p.composite_score !== null ? parseFloat(p.composite_score) : null;
                        const scorePct = score !== null ? (score * 100).toFixed(0) : 0;
                        return `
                            <tr onclick="tableRowClick('${p.fips_code}')">
                                <td><span class="rank-number ${i < 3 ? 'top-3' : ''}">${i + 1}</span></td>
                                <td class="fw-bold">${p.county_name}</td>
                                <td><span class="table-synthesis-badge" style="background: ${badgeBg}; color: ${badgeColor};">${SYNTHESIS_LABELS[p.synthesis_grouping] || p.synthesis_grouping}</span></td>
                                <td>
                                    <div class="table-score-cell">
                                        <span class="fw-bold" style="min-width: 42px;">${score !== null ? score.toFixed(3) : 'N/A'}</span>
                                        <div class="table-score-bar"><div class="table-score-bar-fill" style="width: ${scorePct}%"></div></div>
                                    </div>
                                </td>
                                <td>${DIRECTIONAL_LABELS[p.directional_class] || p.directional_class}</td>
                                <td>${CONFIDENCE_LABELS[p.confidence_class] || p.confidence_class}</td>
                            </tr>
                        `;
                    }).join('')}
                </tbody>
            </table>
        `;
    };

    const rebuildTable = () => {
        const sorted = [...features].sort((a, b) => {
            if (sortCol === 'rank') return 0;
            let va = a.properties[sortCol], vb = b.properties[sortCol];
            if (typeof va === 'string') {
                return sortDir === 'asc' ? va.localeCompare(vb) : vb.localeCompare(va);
            }
            va = va ?? -Infinity;
            vb = vb ?? -Infinity;
            return sortDir === 'asc' ? va - vb : vb - va;
        });

        const container = document.getElementById('ranking-table-container');
        if (container) container.innerHTML = buildTable(sorted);
        bindSortHandlers();
    };

    const bindSortHandlers = () => {
        tableEl.querySelectorAll('th[data-col]').forEach(th => {
            th.addEventListener('click', () => {
                const col = th.getAttribute('data-col');
                if (col === 'rank') return;
                if (sortCol === col) {
                    sortDir = sortDir === 'asc' ? 'desc' : 'asc';
                } else {
                    sortCol = col;
                    sortDir = col === 'composite_score' ? 'desc' : 'asc';
                }
                rebuildTable();
            });
        });
    };

    tableEl.innerHTML = `
        <div class="table-header-bar">
            <h2>Maryland County Rankings</h2>
            <input type="text" class="table-search" id="table-search" placeholder="Filter counties..." autocomplete="off">
            <div class="table-header-actions">
                <button onclick="exportAllCountiesCSV()" class="btn">Download CSV</button>
                <button onclick="toggleTableView()" class="btn btn-primary">Map View</button>
            </div>
        </div>
        <div class="table-scroll-area">
            <div id="ranking-table-container">
                ${buildTable(features)}
            </div>
        </div>
    `;

    bindSortHandlers();

    const searchInput = document.getElementById('table-search');
    if (searchInput) {
        searchInput.addEventListener('input', () => {
            filterText = searchInput.value.trim().toLowerCase();
            rebuildTable();
        });
    }
}

function tableRowClick(fipsCode) {
    // Switch to map view and navigate to county
    if (tableViewActive) {
        toggleTableView();
    }

    const county = countyFeatures.find(c => c.fips === fipsCode);
    if (county && typeof turf !== 'undefined' && county.feature) {
        map.flyTo({
            center: turf.centroid(county.feature).geometry.coordinates,
            zoom: 9,
            duration: MAP_FLY_DURATION_MS
        });
    }

    loadCountyDetail(fipsCode);
}

// ============================================================
// Scrollytelling Intro Tour
// ============================================================
const STORY_STORAGE_KEY = 'atlas_story_seen';
const STORY_CHAPTERS = [
    {
        title: 'Welcome to the Maryland Atlas',
        body: 'This interactive map reveals structural growth trajectories across all 24 Maryland counties. Each county is scored on employment access, mobility, school quality, housing elasticity, demographics, and risk factors.',
        center: [-77.0, 39.0],
        zoom: 7.2,
        pitch: 0,
        bearing: 0
    },
    {
        title: 'Montgomery County: Emerging Tailwinds',
        body: 'Maryland\'s most populous county shows strong employment gravity and school trajectory scores. Its proximity to D.C. creates robust commuting options, though housing elasticity remains a challenge.',
        center: [-77.2, 39.14],
        zoom: 9.2,
        pitch: 30,
        bearing: -10
    },
    {
        title: 'Baltimore County: Conditional Growth',
        body: 'With solid school systems and employment access, Baltimore County has real upside — but delivery risk matters. Housing supply constraints and mixed mobility signals require careful attention.',
        center: [-76.61, 39.44],
        zoom: 9.0,
        pitch: 25,
        bearing: 5
    },
    {
        title: 'Allegany County: Headwinds in the West',
        body: 'Rural western Maryland faces structural headwinds: limited job access, weaker school trajectories, and out-migration pressure. However, low housing costs create a potential entry point for targeted investment.',
        center: [-78.7, 39.62],
        zoom: 9.3,
        pitch: 35,
        bearing: -20
    },
    {
        title: 'Start Exploring',
        body: 'Click any county to see its full analysis, layer scores, and strengths/weaknesses. Use the sidebar search to jump to a county, or toggle 3D Scores to see composite scores rise off the map.',
        center: [-77.0, 39.0],
        zoom: 7.2,
        pitch: 0,
        bearing: 0
    }
];

function shouldShowStory() {
    try {
        return !localStorage.getItem(STORY_STORAGE_KEY);
    } catch {
        return false;
    }
}

function markStorySeen() {
    try {
        localStorage.setItem(STORY_STORAGE_KEY, '1');
    } catch { /* ignore */ }
}

function launchStoryTour() {
    const overlay = document.getElementById('story-overlay');
    if (!overlay) return;

    let step = 0;
    overlay.style.display = '';

    function renderStep() {
        const chapter = STORY_CHAPTERS[step];
        const total = STORY_CHAPTERS.length;
        document.getElementById('story-title').textContent = chapter.title;
        document.getElementById('story-body').textContent = chapter.body;
        document.getElementById('story-step-indicator').textContent = `${step + 1} / ${total}`;
        document.getElementById('story-progress-fill').style.width = `${((step + 1) / total) * 100}%`;

        const nextBtn = document.getElementById('story-next');
        nextBtn.textContent = step === total - 1 ? 'Get Started' : 'Next';

        // Animate the card
        const card = document.getElementById('story-card');
        card.style.animation = 'none';
        card.offsetHeight; // force reflow
        card.style.animation = 'story-card-enter 0.4s ease-out';

        map.flyTo({
            center: chapter.center,
            zoom: chapter.zoom,
            pitch: chapter.pitch,
            bearing: chapter.bearing,
            duration: 1400,
            essential: true
        });
    }

    function close() {
        overlay.style.display = 'none';
        markStorySeen();
    }

    document.getElementById('story-next').addEventListener('click', () => {
        if (step < STORY_CHAPTERS.length - 1) {
            step++;
            renderStep();
        } else {
            close();
        }
    });

    document.getElementById('story-skip').addEventListener('click', () => {
        // Fly back to overview before closing
        map.flyTo({ center: [-77.0, 39.0], zoom: 7.2, pitch: 0, bearing: 0, duration: 600 });
        close();
    });

    renderStep();
}

// Hook into map load to start story after data is ready
map.once('idle', () => {
    if (shouldShowStory() && !window.location.hash) {
        // Small delay so the map is fully settled
        setTimeout(launchStoryTour, 600);
    }
});
