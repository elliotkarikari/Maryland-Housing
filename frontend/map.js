// Maryland Growth & Family Viability Atlas - single-signal bivariate map + story/compare/chat

const MAPBOX_TOKEN =
    window.MAPBOX_ACCESS_TOKEN ||
    '';
const MAPBOX_STYLE_URL = 'mapbox://styles/elkari23/cmlapbzzr005001s57o3mdhxk';
const PAGE_THEME_STORAGE_KEY = 'atlas.page.theme';

const API_BASE_OVERRIDE =
    typeof window.ATLAS_API_BASE_URL === 'string'
        ? window.ATLAS_API_BASE_URL.trim()
        : '';

const API_BASE_CANDIDATES = (() => {
    if (API_BASE_OVERRIDE) {
        return [trimTrailingSlash(API_BASE_OVERRIDE)];
    }

    const protocol = window.location.protocol;
    const host = window.location.hostname;
    const candidates = [
        `${protocol}//${host}:8000/api/v1`,
        `${protocol}//127.0.0.1:8000/api/v1`,
        `${protocol}//localhost:8000/api/v1`
    ];

    const deduped = [];
    candidates.forEach((candidate) => {
        const normalized = trimTrailingSlash(candidate);
        if (!deduped.includes(normalized)) {
            deduped.push(normalized);
        }
    });
    return deduped;
})();
const COUNTY_GEOJSON_FALLBACK_STATIC_PATHS = [
    './md_counties_latest.geojson',
    'md_counties_latest.geojson',
    '/md_counties_latest.geojson',
    '/frontend/md_counties_latest.geojson'
];

const SIDEBAR_WIDTH_KEY = 'atlas.sidebar.width';
const PITCH_START_ZOOM = 9.5;
const PITCH_END_ZOOM = 13.2;
const MAX_PITCH = 45;

const TRAJECTORY_ORDER = ['at_risk', 'stable', 'improving'];
const STRENGTH_ORDER = ['high', 'mid', 'low'];

const TRAJECTORY_LABELS = {
    at_risk: 'Risk Trajectory',
    stable: 'Stable Trajectory',
    improving: 'Growth Trajectory'
};

const TRAJECTORY_SHORT_LABELS = {
    at_risk: 'Risk',
    stable: 'Stable',
    improving: 'Growth'
};

const STRENGTH_LABELS = {
    high: 'High Strength',
    mid: 'Mid Strength',
    low: 'Low Strength'
};

const BIVARIATE_COLORS = {
    'at_risk|high': 'rgba(244, 81, 30, 0.68)',
    'stable|high': 'rgba(235, 187, 58, 0.68)',
    'improving|high': 'rgba(124, 179, 66, 0.68)',
    'at_risk|mid': 'rgba(246, 122, 72, 0.64)',
    'stable|mid': 'rgba(243, 204, 109, 0.64)',
    'improving|mid': 'rgba(153, 191, 97, 0.64)',
    'at_risk|low': 'rgba(248, 172, 140, 0.58)',
    'stable|low': 'rgba(248, 229, 167, 0.58)',
    'improving|low': 'rgba(184, 212, 145, 0.58)'
};

const BIVARIATE_MAP_COLORS = {
    'at_risk|high': '#f4511e',
    'stable|high': '#ebbb3a',
    'improving|high': '#7cb342',
    'at_risk|mid': '#f67a48',
    'stable|mid': '#f3cc6d',
    'improving|mid': '#99bf61',
    'at_risk|low': '#f8ac8c',
    'stable|low': '#f8e5a7',
    'improving|low': '#b8d491'
};

const FALLBACK_COLOR = 'rgba(117, 117, 117, 0.5)';
const FALLBACK_MAP_COLOR = '#757575';

const LAYER_ROWS = [
    ['employment_gravity', 'Employment Gravity'],
    ['mobility_optionality', 'Mobility Optionality'],
    ['school_trajectory', 'School Trajectory'],
    ['housing_elasticity', 'Housing Elasticity'],
    ['demographic_momentum', 'Demographic Momentum'],
    ['risk_drag', 'Risk Drag']
];

const QUICK_PROMPTS = [
    'Explain this county signal in plain language for families deciding where to live.',
    'What are the top pressure points this county should address over the next 2 years?',
    'How does this county compare with nearby counties on housing and schools?'
];

const appState = {
    map: null,
    popup: null,
    geojson: null,
    countyFeaturesByFips: new Map(),
    countyCentersByFips: new Map(),
    countyBoundsByFips: new Map(),
    areaCache: new Map(),
    selectedCounty: null,
    strengthThresholds: { q1: 0, q2: 0 },
    legendFilter: null,
    panelState: 'empty',
    compare: {
        active: false,
        countyA: null,
        countyB: null
    },
    chat: {
        open: false,
        busy: false,
        messages: [],
        history: [],
        previousResponseId: null,
        returnMode: 'empty'
    },
    pendingCountyRequest: null,
    moveOpacityTimer: null,
    apiBaseUrl: null
};

const dom = {
    panel: document.getElementById('atlas-panel'),
    panelTitle: document.getElementById('atlas-panel-title'),
    panelSubtitle: document.getElementById('atlas-panel-subtitle'),
    panelBody: document.getElementById('atlas-panel-body'),
    compareBtn: document.getElementById('compare-btn'),
    panelCloseBtn: document.getElementById('atlas-panel-close'),
    searchInput: document.getElementById('county-search'),
    searchList: document.getElementById('county-search-list'),
    legendCells: Array.from(document.querySelectorAll('.legend-cell')),
    legendHover: document.getElementById('legend-hover'),
    legendFilterNote: document.getElementById('legend-filter-note'),
    askPill: document.getElementById('ask-atlas-pill'),
    askShell: document.getElementById('ask-atlas-input-shell'),
    askInput: document.getElementById('ask-atlas-inline-input'),
    askSend: document.getElementById('ask-atlas-send'),
    askClose: document.getElementById('ask-atlas-close'),
    mapThemeToggle: document.getElementById('map-theme-toggle'),
    analysisFloat: document.getElementById('analysis-float'),
    analysisFloatHandle: document.getElementById('analysis-float-handle'),
    analysisFloatClose: document.getElementById('analysis-float-close'),
    analysisFloatTitle: document.getElementById('analysis-float-title'),
    analysisFloatContent: document.getElementById('analysis-float-content'),
    mapControlsHost: document.getElementById('map-controls'),
    resizeHandle: document.getElementById('sidebar-resize-handle')
};

if (!MAPBOX_TOKEN) {
    renderFatal('Mapbox access token is missing. Set MAPBOX_ACCESS_TOKEN in your environment.');
} else {
    mapboxgl.accessToken = MAPBOX_TOKEN;
    initialize();
}

function initialize() {
    setupLegendSwatches();
    setupLegendInteractions();
    setupPanelControls();
    setupAskAtlasPill();
    setupPageThemeToggle();
    setupAnalysisFloat();
    setupSidebarResize();
    setupGlobalEvents();
    setCompareButtonState(false, false);
    initializeMap();
}

function renderFatal(message) {
    if (dom.panelTitle) {
        dom.panelTitle.textContent = 'Map Error';
    }
    if (dom.panelSubtitle) {
        dom.panelSubtitle.textContent = '';
    }
    if (dom.panelBody) {
        dom.panelBody.innerHTML = `<div class="error-inline">${escapeHtml(message)}</div>`;
    }
}

function updatePitch() {
    if (!appState.map) {
        return;
    }
    const zoom = appState.map.getZoom();
    const t = Math.max(0, Math.min(1, (zoom - PITCH_START_ZOOM) / (PITCH_END_ZOOM - PITCH_START_ZOOM)));
    const pitch = t * MAX_PITCH;
    if (Math.abs(appState.map.getPitch() - pitch) > 0.5) {
        appState.map.setPitch(pitch);
    }
}

function applyPageTheme(theme, options = {}) {
    const normalized = theme === 'dark' ? 'dark' : 'light';
    document.body.classList.toggle('theme-dark', normalized === 'dark');

    if (dom.mapThemeToggle) {
        dom.mapThemeToggle.textContent = normalized === 'dark' ? 'Light Mode' : 'Dark Mode';
        dom.mapThemeToggle.setAttribute('aria-pressed', normalized === 'dark' ? 'true' : 'false');
    }

    if (options.persist !== false) {
        try {
            window.localStorage.setItem(PAGE_THEME_STORAGE_KEY, normalized);
        } catch (_error) {
            // Ignore localStorage write errors.
        }
    }
}

function setupPageThemeToggle() {
    if (!dom.mapThemeToggle) {
        return;
    }

    let preferred = 'light';
    try {
        const stored = window.localStorage.getItem(PAGE_THEME_STORAGE_KEY);
        if (stored === 'dark' || stored === 'light') {
            preferred = stored;
        }
    } catch (_error) {
        // Ignore localStorage read errors.
    }

    applyPageTheme(preferred, { persist: false });
    dom.mapThemeToggle.addEventListener('click', () => {
        const next = document.body.classList.contains('theme-dark') ? 'light' : 'dark';
        applyPageTheme(next);
    });
}

function setupAnalysisFloat() {
    if (!dom.analysisFloat || !dom.analysisFloatHandle || !dom.analysisFloatClose) {
        return;
    }
    if (dom.analysisFloat.dataset.bound === 'true') {
        return;
    }
    dom.analysisFloat.dataset.bound = 'true';

    let dragState = null;

    const clampAndApply = (left, top) => {
        const host = document.querySelector('.map-stage');
        if (!host) {
            return;
        }
        const hostRect = host.getBoundingClientRect();
        const panelRect = dom.analysisFloat.getBoundingClientRect();
        const padding = 8;
        const maxLeft = Math.max(padding, hostRect.width - panelRect.width - padding);
        const maxTop = Math.max(padding, hostRect.height - panelRect.height - padding);
        const clampedLeft = Math.min(Math.max(left, padding), maxLeft);
        const clampedTop = Math.min(Math.max(top, padding), maxTop);

        dom.analysisFloat.style.left = `${clampedLeft}px`;
        dom.analysisFloat.style.top = `${clampedTop}px`;
        dom.analysisFloat.style.right = 'auto';
        dom.analysisFloat.style.bottom = 'auto';
        dom.analysisFloat.dataset.positioned = 'true';
    };

    dom.analysisFloatClose.addEventListener('click', () => {
        hideFloatingAnalysisPanel();
    });

    dom.analysisFloatClose.addEventListener('pointerdown', (event) => {
        event.stopPropagation();
    });

    dom.analysisFloatHandle.addEventListener('pointerdown', (event) => {
        if (window.matchMedia('(max-width: 1040px)').matches || event.button !== 0) {
            return;
        }
        const panelRect = dom.analysisFloat.getBoundingClientRect();
        dragState = {
            pointerId: event.pointerId,
            offsetX: event.clientX - panelRect.left,
            offsetY: event.clientY - panelRect.top
        };
        dom.analysisFloat.classList.add('dragging');
        dom.analysisFloatHandle.setPointerCapture(event.pointerId);
        event.preventDefault();
    });

    dom.analysisFloatHandle.addEventListener('pointermove', (event) => {
        if (!dragState || dragState.pointerId !== event.pointerId) {
            return;
        }
        const host = document.querySelector('.map-stage');
        if (!host) {
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
        dom.analysisFloat.classList.remove('dragging');
        dom.analysisFloatHandle.releasePointerCapture(event.pointerId);
    };

    dom.analysisFloatHandle.addEventListener('pointerup', endDrag);
    dom.analysisFloatHandle.addEventListener('pointercancel', endDrag);

    window.addEventListener('resize', () => {
        if (!dom.analysisFloat || dom.analysisFloat.classList.contains('hidden')) {
            return;
        }
        if (window.matchMedia('(max-width: 1040px)').matches) {
            dom.analysisFloat.style.left = '';
            dom.analysisFloat.style.top = '';
            dom.analysisFloat.style.right = '';
            dom.analysisFloat.style.bottom = '';
            return;
        }
        const left = Number.parseFloat(dom.analysisFloat.style.left);
        const top = Number.parseFloat(dom.analysisFloat.style.top);
        clampAndApply(Number.isFinite(left) ? left : 12, Number.isFinite(top) ? top : 12);
    });
}

function hideFloatingAnalysisPanel() {
    if (dom.analysisFloat) {
        dom.analysisFloat.classList.add('hidden');
    }
}

function renderAnalysisList(items, emptyText, iconPrefix = '') {
    if (!items.length) {
        return `<ul class=\"analysis-float-list\"><li>${escapeHtml(emptyText)}</li></ul>`;
    }

    return `<ul class=\"analysis-float-list\">${items
        .map((item) => `<li>${iconPrefix ? `${iconPrefix} ` : ''}${escapeHtml(item)}</li>`)
        .join('')}</ul>`;
}

function showFloatingAnalysisPanel(county) {
    if (!dom.analysisFloat || !dom.analysisFloatContent || !dom.analysisFloatTitle) {
        return;
    }

    const strengths = Array.isArray(county.primary_strengths) ? county.primary_strengths : [];
    const weaknesses = Array.isArray(county.primary_weaknesses) ? county.primary_weaknesses : [];
    const trends = Array.isArray(county.key_trends) ? county.key_trends : [];

    dom.analysisFloatTitle.textContent = `${county.county_name || 'County'} Analysis`;
    dom.analysisFloatContent.innerHTML = `
        <h4>Primary Strengths</h4>
        ${renderAnalysisList(strengths, 'No strengths reported for this county.', '✓')}
        <h4>Primary Weaknesses</h4>
        ${renderAnalysisList(weaknesses, 'No weaknesses reported for this county.', '⚠')}
        <h4>Key Trends</h4>
        ${renderAnalysisList(trends, 'No trend notes available for this county.')}
        <div class=\"analysis-float-meta\">Data Year: ${county.data_year || 'N/A'}</div>
    `;

    if (!window.matchMedia('(max-width: 1040px)').matches && dom.analysisFloat.dataset.positioned !== 'true') {
        dom.analysisFloat.style.left = '12px';
        dom.analysisFloat.style.top = '12px';
        dom.analysisFloat.style.right = 'auto';
        dom.analysisFloat.style.bottom = 'auto';
        dom.analysisFloat.dataset.positioned = 'true';
    }

    dom.analysisFloat.classList.remove('hidden');
}

function initializeMap() {
    appState.map = new mapboxgl.Map({
        container: 'map',
        style: MAPBOX_STYLE_URL,
        center: [-76.9, 39.02],
        zoom: 7,
        minZoom: 6,
        maxZoom: 16,
        attributionControl: false
    });

    appState.map.addControl(
        new mapboxgl.NavigationControl({ showCompass: false, visualizePitch: false }),
        'bottom-right'
    );
    appState.map.addControl(
        new mapboxgl.GeolocateControl({
            positionOptions: { enableHighAccuracy: true },
            trackUserLocation: false,
            showUserHeading: false
        }),
        'bottom-right'
    );
    appState.map.addControl(
        new mapboxgl.FullscreenControl({ container: document.getElementById('app') }),
        'bottom-right'
    );
    appState.map.addControl(new mapboxgl.AttributionControl({ compact: true }), 'bottom-left');

    appState.popup = new mapboxgl.Popup({
        closeButton: false,
        closeOnClick: false,
        offset: 10,
        maxWidth: '250px'
    });

    appState.map.on('load', async () => {
        moveControlContainer();
        wireMapMotionTransparency();
        updatePitch();
        await loadCountyLayer();
    });
    appState.map.on('zoom', () => {
        updatePitch();
    });

    appState.map.on('error', (event) => {
        const maybeMessage = event && event.error && event.error.message;
        if (maybeMessage) {
            renderTransientPanelError(`Map rendering error: ${maybeMessage}`);
        }
    });
}

function moveControlContainer() {
    const container = appState.map.getContainer().querySelector('.mapboxgl-control-container');
    if (container && dom.mapControlsHost && !dom.mapControlsHost.contains(container)) {
        dom.mapControlsHost.appendChild(container);
    }
}

function wireMapMotionTransparency() {
    const setMoving = (moving) => {
        if (moving) {
            if (appState.moveOpacityTimer) {
                clearTimeout(appState.moveOpacityTimer);
            }
            document.body.classList.add('map-moving');
            return;
        }
        appState.moveOpacityTimer = window.setTimeout(() => {
            document.body.classList.remove('map-moving');
        }, 130);
    };

    appState.map.on('movestart', () => setMoving(true));
    appState.map.on('moveend', () => setMoving(false));
}

async function loadCountyLayer() {
    try {
        const geojson = await loadCountyGeoJson();
        appState.geojson = decorateGeoJson(geojson);
        populateCountySearch();

        appState.map.addSource('counties', {
            type: 'geojson',
            data: appState.geojson
        });

        appState.map.addLayer({
            id: 'counties-fill',
            type: 'fill',
            source: 'counties',
            paint: {
                'fill-color': buildBivariateFillExpression(),
                'fill-opacity': 0.66
            }
        });

        appState.map.addLayer({
            id: 'counties-border',
            type: 'line',
            source: 'counties',
            paint: {
                'line-color': 'rgba(26, 53, 79, 0.56)',
                'line-width': 1,
                'line-opacity': 0.58
            }
        });

        appState.map.addLayer({
            id: 'counties-hover',
            type: 'line',
            source: 'counties',
            filter: ['==', 'fips_code', ''],
            paint: {
                'line-color': '#2f6fb4',
                'line-width': 2.4,
                'line-opacity': 1
            }
        });

        appState.map.addLayer({
            id: 'counties-selected',
            type: 'line',
            source: 'counties',
            filter: ['==', 'fips_code', ''],
            paint: {
                'line-color': '#1565c0',
                'line-width': 3,
                'line-opacity': 0.95
            }
        });

        wireCountyInteractions();
        applyLegendFilter();
    } catch (error) {
        renderTransientPanelError(getReadableFetchError(error, '/layers/counties/latest'));
    }
}

async function loadCountyGeoJson() {
    let apiError = null;

    try {
        const { response, url: resolvedUrl } = await fetchApi('/layers/counties/latest');
        if (!response.ok) {
            throw new Error(`Could not load ${resolvedUrl} (HTTP ${response.status})`);
        }

        const geojson = await response.json();
        if (isValidCountyGeoJson(geojson)) {
            return geojson;
        }
        throw new Error('County GeoJSON response is invalid.');
    } catch (error) {
        apiError = error;
    }

    const fallbackPaths = getCountyGeoJsonFallbackPaths();
    for (const fallbackPath of fallbackPaths) {
        try {
            const response = await fetch(fallbackPath, { cache: 'no-store' });
            if (!response.ok) {
                continue;
            }
            const geojson = await response.json();
            if (isValidCountyGeoJson(geojson)) {
                return geojson;
            }
        } catch (_error) {
            // Try next fallback path.
        }
    }

    const fallbackError = `County GeoJSON is unavailable from local fallback paths (${fallbackPaths.join(', ')}).`;
    if (apiError) {
        throw new Error(`${getReadableFetchError(apiError, '/layers/counties/latest')} ${fallbackError}`);
    }
    throw new Error(fallbackError);
}

function isValidCountyGeoJson(geojson) {
    return Boolean(
        geojson &&
        Array.isArray(geojson.features) &&
        geojson.features.length > 0
    );
}

function getCountyGeoJsonFallbackPaths() {
    const protocol = window.location.protocol || 'http:';
    const unique = new Set(COUNTY_GEOJSON_FALLBACK_STATIC_PATHS);

    try {
        unique.add(new URL('md_counties_latest.geojson', window.location.href).href);
    } catch (_error) {
        // Ignore malformed location URL.
    }

    const scriptTag = Array.from(document.getElementsByTagName('script'))
        .find((script) => script.src && /\/map\.js(\?|$)/.test(script.src));
    if (scriptTag && scriptTag.src) {
        try {
            unique.add(new URL('md_counties_latest.geojson', scriptTag.src).href);
        } catch (_error) {
            // Ignore malformed script URL.
        }
    }

    unique.add(`${protocol}//localhost:3000/md_counties_latest.geojson`);
    unique.add(`${protocol}//127.0.0.1:3000/md_counties_latest.geojson`);

    return Array.from(unique);
}

function decorateGeoJson(geojson) {
    const scores = geojson.features
        .map((feature) => safeNumber(feature.properties && feature.properties.composite_score))
        .filter((value) => value !== null)
        .sort((a, b) => a - b);

    const q1Index = Math.floor(scores.length / 3);
    const q2Index = Math.floor((scores.length * 2) / 3);
    appState.strengthThresholds = {
        q1: scores[q1Index] || 0,
        q2: scores[q2Index] || 0
    };

    geojson.features.forEach((feature) => {
        const props = feature.properties || {};
        const fips = props.fips_code || props.geoid;
        if (!fips) {
            return;
        }

        const trajectoryRaw = (props.directional_class || props.directional_status || 'stable').toLowerCase();
        const trajectory = TRAJECTORY_ORDER.includes(trajectoryRaw) ? trajectoryRaw : 'stable';
        const score = safeNumber(props.composite_score);
        const strength = deriveStrength(score);
        const key = `${trajectory}|${strength}`;

        props.directional_class = trajectory;
        props.signal_strength = strength;
        props.bivariate_key = key;
        props.bivariate_label = buildSignalLabel(trajectory, strength);
        props.bivariate_color = getBivariateColor(key);

        appState.countyFeaturesByFips.set(fips, feature);
        appState.countyCentersByFips.set(fips, getFeatureCenter(feature));
        appState.countyBoundsByFips.set(fips, getFeatureBounds(feature));
    });

    return geojson;
}

function deriveStrength(score) {
    if (score === null) {
        return 'low';
    }
    if (score < appState.strengthThresholds.q1) {
        return 'low';
    }
    if (score < appState.strengthThresholds.q2) {
        return 'mid';
    }
    return 'high';
}

function buildBivariateFillExpression() {
    const expression = ['case'];

    STRENGTH_ORDER.forEach((strength) => {
        TRAJECTORY_ORDER.forEach((trajectory) => {
            const key = `${trajectory}|${strength}`;
            expression.push(getBivariateCondition(trajectory, strength), getBivariateColor(key, 'map'));
        });
    });

    expression.push(FALLBACK_MAP_COLOR);
    return expression;
}

function getBivariateColor(key, target = 'ui') {
    if (target === 'map') {
        return BIVARIATE_MAP_COLORS[key] || FALLBACK_MAP_COLOR;
    }
    return BIVARIATE_COLORS[key] || FALLBACK_COLOR;
}

function getTrajectoryExpression() {
    return ['coalesce', ['get', 'directional_class'], ['get', 'directional_status'], 'stable'];
}

function getScoreExpression() {
    return ['to-number', ['get', 'composite_score'], -999];
}

function getStrengthCondition(strength) {
    const q1 = appState.strengthThresholds.q1;
    const q2 = appState.strengthThresholds.q2;
    const scoreExpr = getScoreExpression();

    if (strength === 'high') {
        return ['>=', scoreExpr, q2];
    }
    if (strength === 'mid') {
        return ['all', ['>=', scoreExpr, q1], ['<', scoreExpr, q2]];
    }
    return ['<', scoreExpr, q1];
}

function getBivariateCondition(trajectory, strength) {
    return [
        'all',
        ['==', getTrajectoryExpression(), trajectory],
        getStrengthCondition(strength)
    ];
}

function getBivariateConditionFromKey(key) {
    const [trajectory, strength] = String(key || '').split('|');
    if (!TRAJECTORY_ORDER.includes(trajectory) || !STRENGTH_ORDER.includes(strength)) {
        return ['boolean', false];
    }
    return getBivariateCondition(trajectory, strength);
}

function buildSignalLabel(trajectory, strength) {
    return `${STRENGTH_LABELS[strength] || 'Mid Strength'} + ${TRAJECTORY_LABELS[trajectory] || 'Stable Trajectory'}`;
}

function setupLegendSwatches() {
    dom.legendCells.forEach((cell) => {
        const trajectory = cell.dataset.trajectory;
        const strength = cell.dataset.strength;
        const key = `${trajectory}|${strength}`;
        cell.style.background = getBivariateColor(key);
    });
}

function setupLegendInteractions() {
    dom.legendCells.forEach((cell) => {
        const trajectory = cell.dataset.trajectory;
        const strength = cell.dataset.strength;

        cell.addEventListener('mouseenter', () => {
            dom.legendHover.textContent = buildSignalLabel(trajectory, strength);
        });

        cell.addEventListener('mouseleave', () => {
            dom.legendHover.textContent = 'Hover a color to preview county signal.';
        });

        cell.addEventListener('click', () => {
            const key = `${trajectory}|${strength}`;
            if (appState.legendFilter && appState.legendFilter.key === key) {
                appState.legendFilter = null;
            } else {
                appState.legendFilter = { trajectory, strength, key };
            }
            applyLegendFilter();
        });
    });
}

function applyLegendFilter() {
    const activeKey = appState.legendFilter ? appState.legendFilter.key : null;

    dom.legendCells.forEach((cell) => {
        const key = `${cell.dataset.trajectory}|${cell.dataset.strength}`;
        const isActive = Boolean(activeKey && activeKey === key);
        cell.classList.toggle('active', isActive);
        cell.classList.toggle('dimmed', Boolean(activeKey && activeKey !== key));
    });

    if (activeKey) {
        const count = countCountiesForLegendKey(activeKey);
        dom.legendFilterNote.textContent = `Filter active: ${buildSignalLabel(appState.legendFilter.trajectory, appState.legendFilter.strength)} (${count} ${count === 1 ? 'county' : 'counties'})`;
    } else {
        dom.legendFilterNote.textContent = 'No filter active.';
    }

    if (!appState.map || !appState.map.getLayer('counties-fill')) {
        return;
    }

    const activeCondition = activeKey ? getBivariateConditionFromKey(activeKey) : null;
    const fillOpacity = activeKey
        ? ['case', activeCondition, 0.72, 0.14]
        : 0.66;

    const borderOpacity = activeKey
        ? ['case', activeCondition, 0.8, 0.22]
        : 0.58;

    appState.map.setPaintProperty('counties-fill', 'fill-opacity', fillOpacity);
    appState.map.setPaintProperty('counties-border', 'line-opacity', borderOpacity);
}

function countCountiesForLegendKey(key) {
    if (!appState.geojson || !Array.isArray(appState.geojson.features)) {
        return 0;
    }

    return appState.geojson.features.reduce((count, feature) => {
        return count + (featureMatchesLegendKey(feature.properties, key) ? 1 : 0);
    }, 0);
}

function featureMatchesLegendKey(properties, key) {
    if (!properties) {
        return false;
    }

    const [trajectory, strength] = String(key || '').split('|');
    if (!TRAJECTORY_ORDER.includes(trajectory) || !STRENGTH_ORDER.includes(strength)) {
        return false;
    }

    const propTrajectory = normalizeTrajectory(properties.directional_class || properties.directional_status);
    const propStrength = deriveStrength(safeNumber(properties.composite_score));
    return propTrajectory === trajectory && propStrength === strength;
}

function wireCountyInteractions() {
    appState.map.on('mouseenter', 'counties-fill', () => {
        appState.map.getCanvas().style.cursor = 'pointer';
    });

    appState.map.on('mouseleave', 'counties-fill', () => {
        appState.map.getCanvas().style.cursor = '';
        appState.map.setFilter('counties-hover', ['==', 'fips_code', '']);
        appState.popup.remove();
    });

    appState.map.on('mousemove', 'counties-fill', (event) => {
        const feature = event.features && event.features[0];
        if (!feature || !feature.properties) {
            appState.popup.remove();
            return;
        }

        const props = feature.properties;
        const fips = props.fips_code;
        appState.map.setFilter('counties-hover', ['==', 'fips_code', fips || '']);

        const score = safeNumber(props.composite_score);
        const scoreText = score === null ? 'N/A' : score.toFixed(3);
        const trajectory = normalizeTrajectory(props.directional_class || props.directional_status);
        const strength = props.signal_strength || deriveStrength(score);
        const key = props.bivariate_key || `${trajectory}|${strength}`;
        const label = props.bivariate_label || buildSignalLabel(trajectory, strength);
        const color = props.bivariate_color || getBivariateColor(key);

        appState.popup
            .setLngLat(event.lngLat)
            .setHTML(
                `<div style="font-family:'Work Sans',sans-serif; color:#1c3a56; min-width:170px;">
                    <div style="font-weight:700; font-size:13px; margin-bottom:4px;">${escapeHtml(props.county_name || 'County')}</div>
                    <div style="font-size:12px; color:#48637c; margin-bottom:6px;">Overall Signal Score: <strong>${scoreText}</strong></div>
                    <div style="display:inline-block; border:1px solid rgba(19,38,56,0.16); border-radius:999px; padding:4px 8px; font-size:11px; font-weight:600; background:${color}; color:#18324b;">
                        ${escapeHtml(label || 'County Signal')}
                    </div>
                </div>`
            )
            .addTo(appState.map);
    });

    appState.map.on('click', 'counties-fill', async (event) => {
        const feature = event.features && event.features[0];
        if (!feature || !feature.properties) {
            return;
        }
        await handleCountySelection(feature.properties.fips_code, feature.properties);
    });

    appState.map.on('click', (event) => {
        const features = appState.map.queryRenderedFeatures(event.point, { layers: ['counties-fill'] });
        if (features.length === 0) {
            appState.map.setFilter('counties-hover', ['==', 'fips_code', '']);
            appState.popup.remove();
        }
    });
}

async function handleCountySelection(fipsCode, featureProps) {
    if (!fipsCode) {
        return;
    }

    const optimistic = toCountyStateModel(featureProps);
    if (optimistic.county_name) {
        dom.searchInput.value = optimistic.county_name;
    }
    highlightSelectedCounty(fipsCode);
    focusCountyByFips(fipsCode);

    if (appState.compare.active) {
        if (!appState.compare.countyA || appState.compare.countyA.fips_code === fipsCode) {
            appState.compare.countyA = optimistic;
            appState.compare.countyB = null;
            appState.selectedCounty = optimistic;
            renderComparePanel();
        } else {
            appState.compare.countyB = optimistic;
            renderComparePanel();
        }
    } else {
        appState.selectedCounty = optimistic;
        renderStoryPanel(optimistic, { loading: true });
    }

    const detailResult = await fetchCountyDetail(fipsCode);
    if (!detailResult.ok) {
        if (appState.compare.active) {
            renderComparePanel({ error: detailResult.error });
        } else if (appState.selectedCounty && appState.selectedCounty.fips_code === fipsCode) {
            renderStoryPanel(appState.selectedCounty, { error: detailResult.error });
        }
        return;
    }

    const detail = detailResult.data;
    const merged = mergeCountyData(optimistic, detail);
    appState.areaCache.set(fipsCode, merged);

    if (appState.compare.active) {
        if (appState.compare.countyA && appState.compare.countyA.fips_code === fipsCode) {
            appState.compare.countyA = merged;
            appState.selectedCounty = merged;
        }
        if (appState.compare.countyB && appState.compare.countyB.fips_code === fipsCode) {
            appState.compare.countyB = merged;
        }
        renderComparePanel();
    } else if (appState.selectedCounty && appState.selectedCounty.fips_code === fipsCode) {
        appState.selectedCounty = merged;
        renderStoryPanel(merged);
    }
}

function getSidebarPaddingRight() {
    const raw = getComputedStyle(document.documentElement).getPropertyValue('--sidebar-width');
    const width = Number.parseFloat(raw);
    if (!Number.isFinite(width)) {
        return 420;
    }
    return width;
}

function focusCountyByFips(fipsCode) {
    if (!appState.map) {
        return;
    }

    const bounds = appState.countyBoundsByFips.get(fipsCode);
    if (!bounds) {
        const center = appState.countyCentersByFips.get(fipsCode);
        if (center) {
            appState.map.easeTo({ center, zoom: Math.max(appState.map.getZoom(), 8.4), duration: 520 });
        }
        return;
    }

    const isMobile = window.matchMedia('(max-width: 1040px)').matches;
    const padding = isMobile
        ? { top: 38, right: 38, bottom: 220, left: 38 }
        : { top: 38, right: getSidebarPaddingRight() + 44, bottom: 38, left: 38 };

    appState.map.fitBounds(
        [
            [bounds.minX, bounds.minY],
            [bounds.maxX, bounds.maxY]
        ],
        {
            padding,
            duration: 820,
            maxZoom: 10.8,
            essential: true
        }
    );
}

function toCountyStateModel(raw) {
    const fips = raw.fips_code || raw.geoid;
    const trajectory = normalizeTrajectory(raw.directional_class || raw.directional_status);
    const score = safeNumber(raw.composite_score);
    const strength = raw.signal_strength || deriveStrength(score);
    const key = raw.bivariate_key || `${trajectory}|${strength}`;
    const layerScores = raw.layer_scores || {};

    return {
        fips_code: fips,
        county_name: raw.county_name || countyNameFromFips(fips),
        data_year: raw.data_year || null,
        directional_class: trajectory,
        composite_score: score,
        signal_strength: strength,
        bivariate_key: key,
        bivariate_label: raw.bivariate_label || buildSignalLabel(trajectory, strength),
        bivariate_color: raw.bivariate_color || getBivariateColor(key),
        layer_scores: {
            employment_gravity: safeNumber(raw.employment_gravity_score ?? layerScores.employment_gravity),
            mobility_optionality: safeNumber(raw.mobility_optionality_score ?? layerScores.mobility_optionality),
            school_trajectory: safeNumber(raw.school_trajectory_score ?? layerScores.school_trajectory),
            housing_elasticity: safeNumber(raw.housing_elasticity_score ?? layerScores.housing_elasticity),
            demographic_momentum: safeNumber(raw.demographic_momentum_score ?? layerScores.demographic_momentum),
            risk_drag: safeNumber(raw.risk_drag_score ?? layerScores.risk_drag)
        },
        primary_strengths: Array.isArray(raw.primary_strengths) ? raw.primary_strengths : [],
        primary_weaknesses: Array.isArray(raw.primary_weaknesses) ? raw.primary_weaknesses : [],
        key_trends: Array.isArray(raw.key_trends) ? raw.key_trends : [],
        last_updated: raw.last_updated || null
    };
}

function mergeCountyData(base, detail) {
    const merged = {
        ...base,
        ...detail
    };

    const score = safeNumber(merged.composite_score);
    const trajectory = normalizeTrajectory(merged.directional_class);
    const strength = deriveStrength(score);
    const key = `${trajectory}|${strength}`;

    merged.directional_class = trajectory;
    merged.composite_score = score;
    merged.signal_strength = strength;
    merged.bivariate_key = key;
    merged.bivariate_label = buildSignalLabel(trajectory, strength);
    merged.bivariate_color = getBivariateColor(key);
    merged.primary_strengths = Array.isArray(merged.primary_strengths) ? merged.primary_strengths : [];
    merged.primary_weaknesses = Array.isArray(merged.primary_weaknesses) ? merged.primary_weaknesses : [];
    merged.key_trends = Array.isArray(merged.key_trends) ? merged.key_trends : [];

    return merged;
}

function normalizeTrajectory(value) {
    const normalized = (value || 'stable').toLowerCase();
    return TRAJECTORY_ORDER.includes(normalized) ? normalized : 'stable';
}

async function fetchCountyDetail(fipsCode) {
    if (appState.areaCache.has(fipsCode)) {
        return { ok: true, data: appState.areaCache.get(fipsCode) };
    }

    if (appState.pendingCountyRequest) {
        appState.pendingCountyRequest.abort();
    }

    const controller = new AbortController();
    appState.pendingCountyRequest = controller;

    try {
        const { response } = await fetchApi(`/areas/${fipsCode}`, {
            signal: controller.signal
        });

        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }

        const payload = await response.json();
        return { ok: true, data: toCountyStateModel(payload) };
    } catch (error) {
        if (error.name === 'AbortError') {
            return { ok: false, error: null };
        }

        const fallbackFeature = appState.countyFeaturesByFips.get(fipsCode);
        if (fallbackFeature && fallbackFeature.properties) {
            return { ok: true, data: toCountyStateModel(fallbackFeature.properties) };
        }

        return {
            ok: false,
            error: getReadableFetchError(error, `/areas/${fipsCode}`)
        };
    } finally {
        if (appState.pendingCountyRequest === controller) {
            appState.pendingCountyRequest = null;
        }
    }
}

function renderStoryPanel(county, options = {}) {
    appState.panelState = 'story';
    appState.compare.active = false;
    appState.compare.countyA = null;
    appState.compare.countyB = null;
    setCompareButtonState(false, true);

    const score = county.composite_score;
    const scoreText = score === null ? 'N/A' : score.toFixed(3);
    const trendText = describeTrajectory(county.directional_class, score);
    const strengths = county.primary_strengths.length
        ? county.primary_strengths.slice(0, 2)
        : ['No named reinforcing strengths available yet.'];

    dom.panel.dataset.state = 'story';
    dom.panel.classList.remove('chat-mode');
    dom.panelTitle.textContent = county.county_name || 'County Story';
    dom.panelSubtitle.textContent = `Data Year: ${county.data_year || 'N/A'}`;

    const errorHtml = options.error
        ? `<div class="error-inline">${escapeHtml(options.error)}</div>`
        : '';

    const loadingLabel = options.loading
        ? '<span style="font-size:12px;color:#607286;">Refreshing county story...</span>'
        : '';

    dom.panelBody.innerHTML = `
        <div style="display:flex; align-items:center; justify-content:space-between; gap:10px; flex-wrap:wrap;">
            <span class="signal-chip" style="background:${county.bivariate_color}; color:#15334f;">${escapeHtml(county.bivariate_label)}</span>
            <span class="score-pill">${scoreText}</span>
        </div>

        ${loadingLabel}
        ${errorHtml}

        <section class="story-section">
            <h4>Trajectory Snapshot</h4>
            <p>${escapeHtml(trendText)}</p>
            <div class="story-list" style="margin-top:8px;">
                ${strengths.map((item) => `<div class="story-list-item">• ${escapeHtml(item)}</div>`).join('')}
            </div>
        </section>

        <section class="story-section">
            <h4>Analysis Detail</h4>
            <p>Full strengths, weaknesses, and trend notes are shown in the floating analysis panel on the map.</p>
        </section>

        <section class="story-section">
            <h4>Ask Atlas</h4>
            <div class="quick-prompts">
                ${QUICK_PROMPTS.map((prompt, idx) => `<button class="quick-prompt" type="button" data-quick-prompt="${idx}">${escapeHtml(shortPromptLabel(prompt))}</button>`).join('')}
            </div>
        </section>
    `;

    bindQuickPromptButtons();
    showFloatingAnalysisPanel(county);
}

function renderComparePanel(options = {}) {
    appState.panelState = 'compare';
    dom.panel.dataset.state = 'compare';
    dom.panel.classList.remove('chat-mode');
    hideFloatingAnalysisPanel();

    setCompareButtonState(true, Boolean(appState.compare.countyA));

    const countyA = appState.compare.countyA;
    const countyB = appState.compare.countyB;

    if (!countyA) {
        dom.panelTitle.textContent = 'County Comparison';
        dom.panelSubtitle.textContent = 'Select County A on the map to start compare mode.';
        dom.panelBody.innerHTML = '<div class="empty-state">Click a county to set County A, then click a second county to compare.</div>';
        return;
    }

    if (!countyB) {
        dom.panelTitle.textContent = `${countyA.county_name} vs ...`;
        dom.panelSubtitle.textContent = `Data Year: ${countyA.data_year || 'N/A'}`;
        dom.panelBody.innerHTML = `
            <div class="compare-shell">
                <div class="compare-header-row">
                    <span class="signal-chip" style="background:${countyA.bivariate_color}; color:#15334f;">${escapeHtml(countyA.bivariate_label)}</span>
                    <button class="exit-compare-btn" type="button" id="exit-compare-btn">Exit Compare</button>
                </div>
                <div class="empty-state">County A selected: <strong>${escapeHtml(countyA.county_name)}</strong>. Click another county on the map to set County B.</div>
                ${options.error ? `<div class="error-inline">${escapeHtml(options.error)}</div>` : ''}
            </div>
        `;
        bindExitCompareButton();
        return;
    }

    const dataYear = countyA.data_year || countyB.data_year || 'N/A';
    dom.panelTitle.textContent = `${countyA.county_name} vs ${countyB.county_name}`;
    dom.panelSubtitle.textContent = `Data Year: ${dataYear}`;

    const tableRows = LAYER_ROWS.map(([key, label]) => {
        const a = safeNumber(countyA.layer_scores && countyA.layer_scores[key]);
        const b = safeNumber(countyB.layer_scores && countyB.layer_scores[key]);
        const diff = a !== null && b !== null ? a - b : null;

        return `
            <tr>
                <td>${escapeHtml(label)}</td>
                <td>${a === null ? 'N/A' : a.toFixed(3)}</td>
                <td>${b === null ? 'N/A' : b.toFixed(3)}</td>
                <td class="${diffClass(diff)}">${formatDiff(diff)}</td>
            </tr>
        `;
    }).join('');

    dom.panelBody.innerHTML = `
        <div class="compare-shell">
            <div class="compare-header-row">
                <div class="signal-chip" style="background:rgba(240,247,253,0.9); color:#224766;">County Comparison</div>
                <button class="exit-compare-btn" type="button" id="exit-compare-btn">Exit Compare</button>
            </div>

            ${options.error ? `<div class="error-inline">${escapeHtml(options.error)}</div>` : ''}

            <div class="compare-cards">
                <article class="compare-card">
                    <h5>County A</h5>
                    <div class="county-name">${escapeHtml(countyA.county_name)}</div>
                    <span class="signal-chip" style="background:${countyA.bivariate_color}; color:#16334f;">${escapeHtml(countyA.bivariate_label)}</span>
                    <div class="compare-score">${countyA.composite_score === null ? 'N/A' : countyA.composite_score.toFixed(3)}</div>
                </article>
                <article class="compare-card">
                    <h5>County B</h5>
                    <div class="county-name">${escapeHtml(countyB.county_name)}</div>
                    <span class="signal-chip" style="background:${countyB.bivariate_color}; color:#16334f;">${escapeHtml(countyB.bivariate_label)}</span>
                    <div class="compare-score">${countyB.composite_score === null ? 'N/A' : countyB.composite_score.toFixed(3)}</div>
                </article>
            </div>

            <div class="compare-table-wrap">
                <table class="compare-table">
                    <thead>
                        <tr>
                            <th>Layer</th>
                            <th>A</th>
                            <th>B</th>
                            <th>Diff</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${tableRows}
                    </tbody>
                </table>
            </div>
        </div>
    `;

    bindExitCompareButton();
}

function describeTrajectory(trajectory, score) {
    const trajectoryText = {
        improving: 'This county is trending toward stronger growth fundamentals.',
        stable: 'This county is showing balanced but moderate movement.',
        at_risk: 'This county is experiencing headwinds that can constrain family outcomes.'
    }[trajectory] || 'This county has mixed movement across signals.';

    if (score === null) {
        return `${trajectoryText} Overall signal score is currently unavailable.`;
    }

    if (score >= 0.5) {
        return `${trajectoryText} Overall Signal Score is ${score.toFixed(3)}, which indicates comparatively strong county conditions.`;
    }

    if (score >= 0.3) {
        return `${trajectoryText} Overall Signal Score is ${score.toFixed(3)}, indicating a moderate county footing with mixed momentum.`;
    }

    return `${trajectoryText} Overall Signal Score is ${score.toFixed(3)}, suggesting elevated pressure on family viability and housing resilience.`;
}

function bindQuickPromptButtons() {
    const buttons = Array.from(dom.panelBody.querySelectorAll('[data-quick-prompt]'));
    buttons.forEach((button) => {
        button.addEventListener('click', async () => {
            const promptIndex = Number.parseInt(button.dataset.quickPrompt, 10);
            const prompt = QUICK_PROMPTS[promptIndex];
            if (!prompt) {
                return;
            }
            await submitAtlasQuestion(prompt);
        });
    });
}

function bindExitCompareButton() {
    const button = document.getElementById('exit-compare-btn');
    if (!button) {
        return;
    }
    button.addEventListener('click', () => {
        disableCompareMode();
    });
}

function setupPanelControls() {
    dom.compareBtn.addEventListener('click', async () => {
        if (appState.chat.open) {
            exitChatMode();
        }

        if (!appState.compare.active) {
            if (!appState.selectedCounty) {
                renderTransientPanelError('Select a county first, then enable compare mode.');
                return;
            }
            appState.compare.active = true;
            appState.compare.countyA = appState.selectedCounty;
            appState.compare.countyB = null;
            renderComparePanel();
            return;
        }

        disableCompareMode();
    });

    dom.panelCloseBtn.addEventListener('click', () => {
        if (appState.chat.open) {
            exitChatMode();
            return;
        }

        if (appState.compare.active) {
            disableCompareMode();
            return;
        }

        clearSelection();
    });

    dom.searchInput.addEventListener('keydown', async (event) => {
        if (event.key === 'Enter') {
            event.preventDefault();
            await runCountySearch(dom.searchInput.value);
        }
    });

    dom.searchInput.addEventListener('change', async () => {
        await runCountySearch(dom.searchInput.value);
    });
}

function setCompareButtonState(active, ready) {
    dom.compareBtn.classList.toggle('active', active);
    dom.compareBtn.setAttribute('aria-pressed', active ? 'true' : 'false');
    dom.compareBtn.disabled = !ready;
    dom.compareBtn.textContent = active ? 'Comparing' : 'Compare';
}

function populateCountySearch() {
    const counties = Array.from(appState.countyFeaturesByFips.values())
        .map((feature) => feature.properties && feature.properties.county_name)
        .filter(Boolean)
        .sort((a, b) => a.localeCompare(b));

    dom.searchList.innerHTML = counties
        .map((name) => `<option value="${escapeHtml(name)}"></option>`)
        .join('');
}

async function runCountySearch(query) {
    const normalized = (query || '').trim().toLowerCase();
    if (!normalized) {
        return;
    }

    const match = Array.from(appState.countyFeaturesByFips.values())
        .find((feature) => {
            const name = feature.properties && feature.properties.county_name;
            return name && name.toLowerCase().includes(normalized);
        });

    if (!match || !match.properties) {
        renderTransientPanelError(`No county matched "${query}".`);
        return;
    }

    const fips = match.properties.fips_code;
    await handleCountySelection(fips, match.properties);
}

function clearSelection() {
    appState.selectedCounty = null;
    appState.compare.active = false;
    appState.compare.countyA = null;
    appState.compare.countyB = null;

    if (appState.map && appState.map.getLayer('counties-selected')) {
        appState.map.setFilter('counties-selected', ['==', 'fips_code', '']);
    }

    setCompareButtonState(false, false);
    dom.panel.dataset.state = 'empty';
    dom.panel.classList.remove('chat-mode');
    dom.panelTitle.textContent = 'County Selected';
    dom.panelSubtitle.textContent = 'No county selected yet';
    dom.panelBody.innerHTML = `
        <div class="empty-state">
            Click a county on the map to open story mode. Use Compare to select two counties and view layer-by-layer score differences.
        </div>
    `;
    hideFloatingAnalysisPanel();
}

function highlightSelectedCounty(fipsCode) {
    if (appState.map && appState.map.getLayer('counties-selected')) {
        appState.map.setFilter('counties-selected', ['==', 'fips_code', fipsCode || '']);
    }
}

function disableCompareMode() {
    appState.compare.active = false;
    appState.compare.countyA = null;
    appState.compare.countyB = null;

    if (appState.selectedCounty) {
        renderStoryPanel(appState.selectedCounty);
    } else {
        clearSelection();
    }
}

function setupAskAtlasPill() {
    dom.askPill.addEventListener('click', () => {
        openAskInput();
    });

    dom.askClose.addEventListener('click', () => {
        closeAskInput();
    });

    dom.askSend.addEventListener('click', async () => {
        await submitInlineAskQuestion();
    });

    dom.askInput.addEventListener('keydown', async (event) => {
        if (event.key === 'Enter') {
            event.preventDefault();
            await submitInlineAskQuestion();
        } else if (event.key === 'Escape') {
            closeAskInput();
        }
    });
}

function openAskInput(prefill = '') {
    dom.askPill.style.display = 'none';
    dom.askShell.classList.add('expanded');
    dom.askShell.setAttribute('aria-hidden', 'false');
    if (prefill) {
        dom.askInput.value = prefill;
    }
    window.setTimeout(() => dom.askInput.focus(), 30);
}

function closeAskInput() {
    dom.askShell.classList.remove('expanded');
    dom.askShell.classList.remove('sending');
    dom.askShell.setAttribute('aria-hidden', 'true');
    dom.askPill.style.display = 'inline-flex';
}

async function submitInlineAskQuestion() {
    const question = dom.askInput.value.trim();
    if (!question || appState.chat.busy) {
        return;
    }

    dom.askShell.classList.add('sending');
    dom.askInput.value = '';
    closeAskInput();
    await submitAtlasQuestion(question);
}

async function submitAtlasQuestion(question) {
    enterChatMode();
    await sendChatMessage(question);
}

function enterChatMode() {
    if (!appState.chat.open) {
        appState.chat.returnMode = appState.panelState;
    }

    appState.chat.open = true;
    appState.panelState = 'chat';
    dom.panel.dataset.state = 'chat';
    dom.panel.classList.add('chat-mode');
    renderChatPanel();
}

function exitChatMode() {
    if (!appState.chat.open) {
        return;
    }

    appState.chat.open = false;
    dom.panel.classList.remove('chat-mode');

    if (appState.compare.active) {
        renderComparePanel();
        return;
    }

    if (appState.selectedCounty) {
        renderStoryPanel(appState.selectedCounty);
        return;
    }

    clearSelection();
}

function renderChatPanel() {
    dom.panelTitle.textContent = 'Ask Atlas';
    dom.panelSubtitle.textContent = appState.selectedCounty
        ? `Context: ${appState.selectedCounty.county_name}`
        : 'General Atlas mode';

    const contextText = appState.selectedCounty
        ? `${appState.selectedCounty.county_name} context enabled`
        : 'General Atlas mode · select a county for place-specific answers';

    const messagesHtml = appState.chat.messages.length
        ? appState.chat.messages.map((message) => {
            const roleClass = message.role === 'user' ? 'user' : message.role === 'thinking' ? 'thinking' : 'assistant';
            return `<div class="chat-msg ${roleClass}">${escapeHtml(message.content)}</div>`;
        }).join('')
        : '<div class="chat-msg assistant">Hi, I\'m Atlas. Ask me anything about Maryland county signals and what they mean for families and housing.</div>';

    dom.panelBody.innerHTML = `
        <div class="chat-context-chip">${escapeHtml(contextText)}</div>
        <div id="chat-messages" class="chat-messages">${messagesHtml}</div>
        <form id="chat-form" class="chat-panel-input" autocomplete="off">
            <input id="chat-input" type="text" placeholder="Ask Atlas a follow-up..." maxlength="500" ${appState.chat.busy ? 'disabled' : ''}>
            <button type="submit" ${appState.chat.busy ? 'disabled' : ''}>Send</button>
            <button id="chat-close" type="button" class="chat-close-btn">Close</button>
        </form>
    `;

    const chatMessages = document.getElementById('chat-messages');
    if (chatMessages) {
        chatMessages.scrollTop = chatMessages.scrollHeight;
    }

    const chatInput = document.getElementById('chat-input');
    const chatForm = document.getElementById('chat-form');
    const chatClose = document.getElementById('chat-close');

    if (chatForm) {
        chatForm.addEventListener('submit', async (event) => {
            event.preventDefault();
            if (!chatInput) {
                return;
            }
            const question = chatInput.value.trim();
            if (!question) {
                return;
            }
            chatInput.value = '';
            await sendChatMessage(question);
        });
    }

    if (chatClose) {
        chatClose.addEventListener('click', () => {
            exitChatMode();
        });
    }

    if (chatInput && !appState.chat.busy) {
        chatInput.focus();
    }
}

async function sendChatMessage(message) {
    if (appState.chat.busy) {
        return;
    }

    appState.chat.busy = true;
    appState.chat.messages.push({ role: 'user', content: message });
    appState.chat.history.push({ role: 'user', content: message });
    appState.chat.messages.push({ role: 'thinking', content: 'Atlas is thinking...' });
    renderChatPanel();

    try {
        const payload = {
            message,
            context: buildChatContext(),
            history: appState.chat.history.slice(-10),
            previous_response_id: appState.chat.previousResponseId
        };

        const { response } = await fetchApi('/chat', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });

        if (!response.ok) {
            let detail = `HTTP ${response.status}`;
            try {
                const errorPayload = await response.json();
                detail = errorPayload.detail || detail;
            } catch (_error) {
                // Keep default detail.
            }
            throw new Error(detail);
        }

        const data = await response.json();
        const answer = (data.response || '').trim() || 'No response text returned.';

        appState.chat.previousResponseId = data.response_id || null;
        appState.chat.history.push({ role: 'assistant', content: answer });
        replaceThinkingMessage({ role: 'assistant', content: answer });
    } catch (error) {
        replaceThinkingMessage({
            role: 'assistant',
            content: `Sorry, I couldn't respond right now. ${getReadableFetchError(error, '/chat')}`
        });
    } finally {
        appState.chat.busy = false;
        renderChatPanel();
    }
}

function replaceThinkingMessage(newMessage) {
    const idx = appState.chat.messages.findIndex((message) => message.role === 'thinking');
    if (idx !== -1) {
        appState.chat.messages.splice(idx, 1, newMessage);
    } else {
        appState.chat.messages.push(newMessage);
    }
}

function buildChatContext() {
    if (!appState.selectedCounty) {
        return {
            mode: 'general',
            app: 'Maryland Growth & Family Viability Atlas'
        };
    }

    const county = appState.selectedCounty;
    return {
        mode: 'county',
        fips_code: county.fips_code,
        county_name: county.county_name,
        data_year: county.data_year,
        directional_class: county.directional_class,
        composite_score: county.composite_score,
        signal_label: county.bivariate_label,
        layer_scores: county.layer_scores,
        primary_strengths: county.primary_strengths,
        primary_weaknesses: county.primary_weaknesses,
        key_trends: county.key_trends
    };
}

function setupGlobalEvents() {
    document.addEventListener('keydown', (event) => {
        if (event.key !== 'Escape') {
            return;
        }

        if (dom.askShell.classList.contains('expanded')) {
            closeAskInput();
            return;
        }

        if (appState.chat.open) {
            exitChatMode();
            return;
        }

        if (appState.compare.active) {
            disableCompareMode();
            return;
        }

        clearSelection();
    });

    document.addEventListener('pointerdown', (event) => {
        const target = event.target;

        if (dom.askShell.classList.contains('expanded')) {
            const insideAsk = dom.askShell.contains(target);
            if (!insideAsk) {
                closeAskInput();
            }
        }

        if (!appState.chat.open) {
            return;
        }

        const insideSidebar = document.getElementById('atlas-sidebar').contains(target);
        const insideAsk = dom.askShell.contains(target) || dom.askPill.contains(target);

        if (!insideSidebar && !insideAsk) {
            exitChatMode();
        }
    });
}

function setupSidebarResize() {
    const handle = dom.resizeHandle;
    if (!handle) {
        return;
    }

    const isDesktop = () => window.matchMedia('(min-width: 1041px)').matches;

    const savedWidth = Number.parseInt(window.localStorage.getItem(SIDEBAR_WIDTH_KEY) || '', 10);
    if (Number.isFinite(savedWidth)) {
        setSidebarWidth(savedWidth);
    }

    let resizing = false;

    const onPointerMove = (event) => {
        if (!resizing || !isDesktop()) {
            return;
        }

        const nextWidth = window.innerWidth - event.clientX - 12;
        setSidebarWidth(nextWidth);
        if (appState.map) {
            appState.map.resize();
        }
    };

    const stopResize = () => {
        if (!resizing) {
            return;
        }
        resizing = false;
        handle.classList.remove('active');
        window.removeEventListener('pointermove', onPointerMove);
        window.removeEventListener('pointerup', stopResize);
    };

    handle.addEventListener('pointerdown', (event) => {
        if (!isDesktop()) {
            return;
        }
        event.preventDefault();
        resizing = true;
        handle.classList.add('active');
        window.addEventListener('pointermove', onPointerMove);
        window.addEventListener('pointerup', stopResize);
    });

    window.addEventListener('resize', () => {
        if (appState.map) {
            appState.map.resize();
        }
    });
}

function setSidebarWidth(rawWidth) {
    const bounded = Math.max(340, Math.min(620, Math.round(rawWidth)));
    document.documentElement.style.setProperty('--sidebar-width', `${bounded}px`);
    window.localStorage.setItem(SIDEBAR_WIDTH_KEY, `${bounded}`);
}

function renderTransientPanelError(message) {
    const existingError = dom.panelBody.querySelector('.error-inline');
    if (existingError) {
        existingError.textContent = message;
        return;
    }

    const div = document.createElement('div');
    div.className = 'error-inline';
    div.textContent = message;
    dom.panelBody.prepend(div);
}

function safeNumber(value) {
    if (value === null || value === undefined || value === '') {
        return null;
    }
    const parsed = Number.parseFloat(value);
    return Number.isFinite(parsed) ? parsed : null;
}

function diffClass(diff) {
    if (diff === null) {
        return '';
    }
    if (diff > 0) {
        return 'diff-pos';
    }
    if (diff < 0) {
        return 'diff-neg';
    }
    return '';
}

function formatDiff(diff) {
    if (diff === null) {
        return 'N/A';
    }
    const sign = diff > 0 ? '+' : '';
    return `${sign}${diff.toFixed(3)}`;
}

function shortPromptLabel(prompt) {
    if (prompt.length <= 44) {
        return prompt;
    }
    return `${prompt.slice(0, 41)}...`;
}

function countyNameFromFips(fipsCode) {
    const feature = appState.countyFeaturesByFips.get(fipsCode);
    return feature && feature.properties && feature.properties.county_name
        ? feature.properties.county_name
        : 'Maryland County';
}

function trimTrailingSlash(value) {
    return String(value || '').replace(/\/+$/, '');
}

function apiBaseCandidatesInOrder() {
    const current = appState.apiBaseUrl;
    if (!current) {
        return API_BASE_CANDIDATES;
    }

    return [current, ...API_BASE_CANDIDATES.filter((candidate) => candidate !== current)];
}

function buildApiUrl(base, path) {
    return `${trimTrailingSlash(base)}${path.startsWith('/') ? path : `/${path}`}`;
}

async function fetchApi(path, init = {}) {
    const candidates = apiBaseCandidatesInOrder();
    let firstHttpFallback = null;
    const triedUrls = [];

    for (const base of candidates) {
        const url = buildApiUrl(base, path);
        triedUrls.push(url);

        try {
            const response = await fetch(url, init);
            if (response.ok) {
                appState.apiBaseUrl = base;
                return { response, url, base };
            }

            if (!firstHttpFallback) {
                firstHttpFallback = { response, url, base };
            }
        } catch (error) {
            if (error && error.name === 'AbortError') {
                throw error;
            }
        }
    }

    if (firstHttpFallback) {
        appState.apiBaseUrl = firstHttpFallback.base;
        return firstHttpFallback;
    }

    throw new Error(
        `Could not reach any API endpoint (${triedUrls.join(', ')}). ` +
        `Ensure the API server is running on port 8000.`
    );
}

function getReadableFetchError(error, url) {
    if (!error) {
        return '';
    }

    const message = String(error.message || '');
    if (message.startsWith('Could not reach any API endpoint')) {
        return message;
    }

    if (error.name === 'TypeError' || /failed to fetch/i.test(String(error.message))) {
        return `Could not reach ${url}. Ensure the API server is running and accessible.`;
    }

    return error.message || String(error);
}

function getFeatureBounds(feature) {
    const bounds = {
        minX: Number.POSITIVE_INFINITY,
        minY: Number.POSITIVE_INFINITY,
        maxX: Number.NEGATIVE_INFINITY,
        maxY: Number.NEGATIVE_INFINITY
    };

    const visitCoords = (coords) => {
        if (!Array.isArray(coords) || coords.length === 0) {
            return;
        }

        if (typeof coords[0] === 'number' && typeof coords[1] === 'number') {
            bounds.minX = Math.min(bounds.minX, coords[0]);
            bounds.maxX = Math.max(bounds.maxX, coords[0]);
            bounds.minY = Math.min(bounds.minY, coords[1]);
            bounds.maxY = Math.max(bounds.maxY, coords[1]);
            return;
        }

        coords.forEach((child) => visitCoords(child));
    };

    visitCoords(feature.geometry && feature.geometry.coordinates);

    if (!Number.isFinite(bounds.minX) || !Number.isFinite(bounds.minY)) {
        return null;
    }

    return bounds;
}

function getFeatureCenter(feature) {
    const bounds = getFeatureBounds(feature);
    if (!bounds) {
        return null;
    }

    return [
        (bounds.minX + bounds.maxX) / 2,
        (bounds.minY + bounds.maxY) / 2
    ];
}

function escapeHtml(value) {
    return String(value || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}
