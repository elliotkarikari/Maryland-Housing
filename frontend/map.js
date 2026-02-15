// Maryland Growth & Family Viability Atlas - single-signal bivariate map + story/compare/chat

const MAPBOX_TOKEN =
    window.MAPBOX_ACCESS_TOKEN ||
    (window.ATLAS_RUNTIME_CONFIG && window.ATLAS_RUNTIME_CONFIG.MAPBOX_ACCESS_TOKEN) ||
    '';
const MAPBOX_STYLE_URL = 'mapbox://styles/elkari23/cmlapbzzr005001s57o3mdhxk';

const API_BASE_OVERRIDE =
    typeof window.ATLAS_API_BASE_URL === 'string'
        ? window.ATLAS_API_BASE_URL.trim()
        : (
            window.ATLAS_RUNTIME_CONFIG &&
            typeof window.ATLAS_RUNTIME_CONFIG.ATLAS_API_BASE_URL === 'string'
        )
            ? window.ATLAS_RUNTIME_CONFIG.ATLAS_API_BASE_URL.trim()
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
const COUNTY_GEOJSON_MAX_ATTEMPTS = 6;
const COUNTY_GEOJSON_RETRY_BASE_MS = 1200;
const COUNTY_LAYER_AUTO_RECOVER_MS = 5000;

const SIDEBAR_WIDTH_KEY = 'atlas.sidebar.width';
const TOUR_STORAGE_KEY = 'atlas.guided_tour.dismissed.v1';

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

const SYNTHESIS_LABELS = {
    emerging_tailwinds: 'Emerging Tailwinds',
    conditional_growth: 'Conditional Growth',
    stable_constrained: 'Stable but Constrained',
    at_risk_headwinds: 'At Risk / Headwinds',
    high_uncertainty: 'High Uncertainty'
};

const SYNTHESIS_COLORS = {
    emerging_tailwinds: '#5b8c38',
    conditional_growth: '#78a94e',
    stable_constrained: '#edd252',
    at_risk_headwinds: '#e66336',
    high_uncertainty: '#8d97a2'
};

const CONFIDENCE_LABELS = {
    strong: 'Strong',
    conditional: 'Conditional',
    fragile: 'Fragile',
    unknown: 'Unknown'
};

const TRAJECTORY_SUMMARY_LABELS = {
    improving: 'Improving',
    stable: 'Stable',
    at_risk: 'At Risk',
    unknown: 'Unknown'
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

const COMPARE_SERIES_COLORS = ['#2f6fb4', '#5d9a3b', '#dd7f31', '#6e859b'];
const FLOW_DASH_SEQUENCE = [
    [0, 4, 3],
    [0.5, 4, 2.5],
    [1, 4, 2],
    [1.5, 4, 1.5],
    [2, 4, 1],
    [2.5, 4, 0.5],
    [3, 4, 0],
    [0, 0.5, 3, 3.5],
    [0, 1, 3, 3],
    [0, 1.5, 3, 2.5],
    [0, 2, 3, 2],
    [0, 2.5, 3, 1.5],
    [0, 3, 3, 1],
    [0, 3.5, 3, 0.5]
];

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

const LAYER_ICONS = {
    employment_gravity: '⊞',
    mobility_optionality: '↔',
    school_trajectory: '✎',
    housing_elasticity: '⌂',
    demographic_momentum: '◎',
    risk_drag: '△'
};

const LAYER_EXPLANATIONS = {
    employment_gravity: 'Employment Gravity: Access to jobs and local labor-market pull for residents.',
    mobility_optionality: 'Mobility Optionality: How many practical transportation choices families have.',
    school_trajectory: 'School Trajectory: Measures education trends supporting family futures.',
    housing_elasticity: 'Housing Elasticity: How responsive local housing supply is to demand and price pressure.',
    demographic_momentum: 'Demographic Momentum: Population and household trends that support long-term growth.',
    risk_drag: 'Risk Drag: Factors slowing growth like high costs or structural barriers.'
};

const MAP_ILLUSTRATION_DEFINITIONS = [
    {
        key: 'momentum',
        iconId: 'atlas-icon-momentum-up',
        tooltip: 'Positive Momentum: Signals point to improving county trajectory.',
        accent: '#5d9a3b',
        offsetKm: [-4.8, 3.8]
    },
    {
        key: 'viability',
        iconId: 'atlas-icon-family',
        tooltip: 'Family Viability: Composite conditions support stronger family outcomes.',
        accent: '#2f6fb4',
        offsetKm: [4.8, 3.8]
    },
    {
        key: 'housing',
        iconId: 'atlas-icon-house',
        tooltip: 'Housing Elasticity: Local housing conditions appear comparatively resilient.',
        accent: '#bb7f2a',
        offsetKm: [0, -5.2]
    }
];

const TOUR_STEPS = [
    {
        title: 'Explore The Map',
        body: 'Start by clicking any county on the map. The panel updates with a summary and score breakdown.',
        target: '#map',
        preferMapView: true
    },
    {
        title: 'Filter By Signal',
        body: 'Use the bivariate legend to filter counties by trajectory and signal strength.',
        target: '#legend-bivariate',
        preferMapView: true
    },
    {
        title: 'Read Layer Stories',
        body: 'Open the Layers tab and hover a layer icon to see plain-language explanations for each signal.',
        target: '#atlas-panel',
        preferMapView: true
    },
    {
        title: 'Ask Atlas',
        body: 'Use Ask Atlas for narrative insights and follow-up questions grounded in county context.',
        target: '#ask-atlas-pill',
        preferMapView: true
    },
    {
        title: 'Switch To Rankings',
        body: 'Use Table View for a best-to-worst county ranking when you want a quick statewide scan.',
        target: '#map-table-toggle'
    }
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
        counties: [],
        maxCount: 4,
        splitView: false,
        builderExpanded: false,
        dragFips: null,
        splitMaps: {
            left: null,
            right: null
        }
    },
    countySearchIndex: [],
    compareAutocomplete: {
        matches: [],
        activeIndex: -1
    },
    chat: {
        open: false,
        busy: false,
        messages: [],
        history: [],
        previousResponseId: null,
        returnMode: 'empty'
    },
    capabilities: window.AtlasCapabilities
        ? window.AtlasCapabilities.defaultCapabilities()
        : {
            loaded: false,
            chat_enabled: false,
            ai_enabled: false,
            api_version: null,
            year_policy: null
        },
    pendingCountyRequest: null,
    moveOpacityTimer: null,
    clickOpacityTimer: null,
    flowAnimationTimer: null,
    hoverPopupFips: null,
    apiBaseUrl: null,
    countyLayerLoaded: false,
    countyLayerRetryTimer: null,
    countyLayerRetryAttempt: 0,
    tableViewOpen: false,
    tour: {
        active: false,
        stepIndex: 0
    }
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
    mapTableToggle: document.getElementById('map-table-toggle'),
    countyTablePanel: document.getElementById('county-table-panel'),
    countyTableClose: document.getElementById('county-table-close'),
    countyTableBody: document.getElementById('county-table-body'),
    countyTableSubtitle: document.getElementById('county-table-subtitle'),
    analysisFloat: document.getElementById('analysis-float'),
    analysisFloatHandle: document.getElementById('analysis-float-handle'),
    analysisFloatClose: document.getElementById('analysis-float-close'),
    analysisFloatTitle: document.getElementById('analysis-float-title'),
    analysisFloatContent: document.getElementById('analysis-float-content'),
    layerModal: document.getElementById('layer-modal'),
    layerModalContent: document.getElementById('layer-modal-content'),
    layerModalClose: document.getElementById('layer-modal-close'),
    layerModalTitle: document.getElementById('layer-modal-title'),
    layerModalDesc: document.getElementById('layer-modal-desc'),
    layerModalBody: document.getElementById('layer-modal-body'),
    mapControlsHost: document.getElementById('map-controls'),
    resizeHandle: document.getElementById('sidebar-resize-handle'),
    mapStage: document.querySelector('.map-stage'),
    tourLaunchBtn: document.getElementById('tour-launch-btn'),
    tourOverlay: document.getElementById('tour-overlay'),
    tourCard: document.getElementById('tour-card'),
    tourHighlight: document.getElementById('tour-highlight'),
    tourStep: document.getElementById('tour-step'),
    tourTitle: document.getElementById('tour-title'),
    tourText: document.getElementById('tour-text'),
    tourBack: document.getElementById('tour-back'),
    tourNext: document.getElementById('tour-next'),
    tourSkip: document.getElementById('tour-skip'),
    compareBuilderCard: document.getElementById('compare-builder-card'),
    compareBuilderToggle: document.getElementById('compare-builder-toggle'),
    compareBuilder: document.getElementById('compare-builder'),
    compareBuilderMeta: document.getElementById('compare-builder-meta'),
    compareSearchInput: document.getElementById('compare-search-input'),
    compareAutocomplete: document.getElementById('compare-autocomplete'),
    compareChipList: document.getElementById('compare-chip-list'),
    splitViewToggle: document.getElementById('split-view-toggle')
};

if (!MAPBOX_TOKEN) {
    renderFatal('Mapbox access token is missing. Set MAPBOX_ACCESS_TOKEN in your environment.');
} else {
    mapboxgl.accessToken = MAPBOX_TOKEN;
    initialize().catch((error) => {
        renderFatal(`Failed to initialize map: ${getReadableFetchError(error, 'initialization')}`);
    });
}

async function initialize() {
    await loadCapabilities();
    setupLegendSwatches();
    setupLegendInteractions();
    setupPanelControls();
    setupCompareBuilder();
    setupCountyTableView();
    setupGuidedTour();
    setupAskAtlasPill();
    setupAnalysisFloat();
    setupLayerModal();
    setupSidebarResize();
    setupGlobalEvents();
    setCompareButtonState(false, false);
    applyCapabilitiesUI();
    initializeMap();
}

function isChatEnabled() {
    if (window.AtlasCapabilities) {
        return window.AtlasCapabilities.isChatEnabled(appState.capabilities);
    }
    return Boolean(appState.capabilities && appState.capabilities.chat_enabled);
}

async function loadCapabilities() {
    try {
        const { response } = await fetchApi('/metadata/capabilities');
        if (!response.ok) {
            return;
        }
        const payload = await response.json();
        appState.capabilities = window.AtlasCapabilities
            ? window.AtlasCapabilities.normalize(payload)
            : {
                loaded: true,
                chat_enabled: Boolean(payload.chat_enabled),
                ai_enabled: Boolean(payload.ai_enabled),
                api_version: payload.api_version || null,
                year_policy: payload.year_policy || null
            };
    } catch (_error) {
        // Keep conservative defaults when capabilities are unavailable.
    }
}

function applyCapabilitiesUI() {
    const chatEnabled = isChatEnabled();
    if (dom.askPill) {
        dom.askPill.style.display = chatEnabled ? 'inline-flex' : 'none';
    }
    if (dom.askShell) {
        dom.askShell.classList.remove('expanded');
        dom.askShell.classList.remove('sending');
        dom.askShell.setAttribute('aria-hidden', 'true');
        dom.askShell.style.display = chatEnabled ? 'inline-flex' : 'none';
    }
    if (!chatEnabled) {
        appState.chat.open = false;
        appState.chat.busy = false;
        if (dom.analysisFloat) {
            dom.analysisFloat.classList.remove('chat-mode');
        }
    }
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
    if (Math.abs(appState.map.getPitch()) > 0.1) {
        appState.map.setPitch(0);
    }
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
    appState.chat.open = false;
    if (!dom.analysisFloat) {
        return;
    }
    dom.analysisFloat.classList.remove('chat-mode');
    dom.analysisFloat.classList.add('hidden');
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

    if (appState.chat.open) {
        appState.chat.open = false;
    }
    dom.analysisFloat.classList.remove('chat-mode');

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

function setupLayerModal() {
    if (!dom.layerModal || !dom.layerModalClose || !dom.layerModalContent) {
        return;
    }

    dom.layerModal.addEventListener('pointerdown', (event) => {
        if (event.target === dom.layerModal) {
            closeLayerModal();
        }
    });

    dom.layerModalClose.addEventListener('click', () => {
        closeLayerModal();
    });
}

function closeLayerModal() {
    if (!dom.layerModal) {
        return;
    }
    dom.layerModal.classList.remove('open');
    dom.layerModal.setAttribute('aria-hidden', 'true');
}

function setupCountyTableView() {
    if (!dom.mapTableToggle || !dom.countyTablePanel || !dom.countyTableBody) {
        return;
    }

    dom.mapTableToggle.addEventListener('click', () => {
        if (appState.tableViewOpen) {
            closeCountyTableView();
            return;
        }
        openCountyTableView();
    });

    if (dom.countyTableClose) {
        dom.countyTableClose.addEventListener('click', () => {
            closeCountyTableView();
        });
    }

    dom.countyTableBody.addEventListener('click', async (event) => {
        const row = event.target.closest('tr[data-fips-code]');
        if (!row) {
            return;
        }

        const fipsCode = row.dataset.fipsCode;
        if (!fipsCode) {
            return;
        }

        const feature = appState.countyFeaturesByFips.get(fipsCode);
        if (!feature || !feature.properties) {
            return;
        }

        await handleCountySelection(fipsCode, feature.properties);
        closeCountyTableView();
    });

    dom.countyTableBody.addEventListener('keydown', async (event) => {
        if (event.key !== 'Enter' && event.key !== ' ') {
            return;
        }
        const row = event.target.closest('tr[data-fips-code]');
        if (!row) {
            return;
        }
        event.preventDefault();
        const fipsCode = row.dataset.fipsCode;
        if (!fipsCode) {
            return;
        }
        const feature = appState.countyFeaturesByFips.get(fipsCode);
        if (!feature || !feature.properties) {
            return;
        }
        await handleCountySelection(fipsCode, feature.properties);
        closeCountyTableView();
    });

    renderCountyTableView();
}

function openCountyTableView() {
    appState.tableViewOpen = true;
    if (dom.countyTablePanel) {
        dom.countyTablePanel.classList.add('open');
        dom.countyTablePanel.setAttribute('aria-hidden', 'false');
    }
    if (dom.mapStage) {
        dom.mapStage.classList.add('table-view-active');
    }
    if (dom.mapTableToggle) {
        dom.mapTableToggle.textContent = 'Map View';
        dom.mapTableToggle.setAttribute('aria-expanded', 'true');
    }
    renderCountyTableView();
    if (appState.tour.active) {
        window.requestAnimationFrame(() => renderTourStep());
    }
}

function closeCountyTableView() {
    appState.tableViewOpen = false;
    if (dom.countyTablePanel) {
        dom.countyTablePanel.classList.remove('open');
        dom.countyTablePanel.setAttribute('aria-hidden', 'true');
    }
    if (dom.mapStage) {
        dom.mapStage.classList.remove('table-view-active');
    }
    if (dom.mapTableToggle) {
        dom.mapTableToggle.textContent = 'Table View';
        dom.mapTableToggle.setAttribute('aria-expanded', 'false');
    }
    if (appState.tour.active) {
        window.requestAnimationFrame(() => renderTourStep());
    }
}

function setupGuidedTour() {
    if (
        !dom.tourOverlay ||
        !dom.tourCard ||
        !dom.tourHighlight ||
        !dom.tourStep ||
        !dom.tourTitle ||
        !dom.tourText ||
        !dom.tourBack ||
        !dom.tourNext ||
        !dom.tourSkip
    ) {
        return;
    }

    if (dom.tourLaunchBtn) {
        dom.tourLaunchBtn.addEventListener('click', () => {
            startGuidedTour({ force: true });
        });
    }

    dom.tourBack.addEventListener('click', () => {
        if (appState.tour.stepIndex > 0) {
            appState.tour.stepIndex -= 1;
            renderTourStep();
        }
    });

    dom.tourNext.addEventListener('click', () => {
        if (appState.tour.stepIndex >= TOUR_STEPS.length - 1) {
            stopGuidedTour({ dismiss: true });
            return;
        }
        appState.tour.stepIndex += 1;
        renderTourStep();
    });

    dom.tourSkip.addEventListener('click', () => {
        stopGuidedTour({ dismiss: true });
    });

    dom.tourOverlay.addEventListener('pointerdown', (event) => {
        if (event.target === dom.tourOverlay) {
            stopGuidedTour({ dismiss: true });
        }
    });

    dom.tourCard.addEventListener('pointerdown', (event) => {
        event.stopPropagation();
    });

    window.addEventListener('resize', () => {
        if (appState.tour.active) {
            renderTourStep();
        }
    });

    let dismissed = false;
    try {
        dismissed = window.localStorage.getItem(TOUR_STORAGE_KEY) === 'true';
    } catch (_error) {
        dismissed = false;
    }

    if (!dismissed) {
        window.setTimeout(() => {
            startGuidedTour({ force: false });
        }, 520);
    }
}

function startGuidedTour(options = {}) {
    if (!dom.tourOverlay || !dom.tourCard) {
        return;
    }

    if (appState.tour.active) {
        appState.tour.stepIndex = 0;
        renderTourStep();
        return;
    }

    appState.tour.active = true;
    appState.tour.stepIndex = 0;
    dom.tourOverlay.classList.add('open');
    dom.tourOverlay.setAttribute('aria-hidden', 'false');
    renderTourStep();

    if (options.force) {
        try {
            window.localStorage.setItem(TOUR_STORAGE_KEY, 'false');
        } catch (_error) {
            // Ignore localStorage write errors.
        }
    }
}

function stopGuidedTour(options = {}) {
    if (!dom.tourOverlay || !dom.tourHighlight) {
        return;
    }

    appState.tour.active = false;
    dom.tourOverlay.classList.remove('open');
    dom.tourOverlay.setAttribute('aria-hidden', 'true');
    dom.tourHighlight.classList.add('hidden');

    if (options.dismiss !== false) {
        try {
            window.localStorage.setItem(TOUR_STORAGE_KEY, 'true');
        } catch (_error) {
            // Ignore localStorage write errors.
        }
    }
}

function renderTourStep() {
    if (
        !appState.tour.active ||
        !dom.tourStep ||
        !dom.tourTitle ||
        !dom.tourText ||
        !dom.tourBack ||
        !dom.tourNext ||
        !dom.tourHighlight ||
        !dom.tourCard
    ) {
        return;
    }

    const step = TOUR_STEPS[appState.tour.stepIndex];
    if (!step) {
        stopGuidedTour({ dismiss: true });
        return;
    }

    if (step.preferMapView && appState.tableViewOpen) {
        closeCountyTableView();
    }

    dom.tourStep.textContent = `Step ${appState.tour.stepIndex + 1} of ${TOUR_STEPS.length}`;
    dom.tourTitle.textContent = step.title;
    dom.tourText.textContent = step.body;
    dom.tourBack.disabled = appState.tour.stepIndex === 0;
    dom.tourNext.textContent = appState.tour.stepIndex === TOUR_STEPS.length - 1 ? 'Done' : 'Next';

    const highlightedRect = updateTourHighlight(step.target);
    positionTourCard(highlightedRect);
}

function updateTourHighlight(targetSelector) {
    if (!dom.tourHighlight || !targetSelector) {
        return null;
    }

    const target = document.querySelector(targetSelector);
    if (!target) {
        dom.tourHighlight.classList.add('hidden');
        return null;
    }

    const rect = target.getBoundingClientRect();
    if (!rect || rect.width < 2 || rect.height < 2) {
        dom.tourHighlight.classList.add('hidden');
        return null;
    }

    const pad = 8;
    dom.tourHighlight.classList.remove('hidden');
    dom.tourHighlight.style.left = `${Math.max(6, rect.left - pad)}px`;
    dom.tourHighlight.style.top = `${Math.max(6, rect.top - pad)}px`;
    dom.tourHighlight.style.width = `${Math.max(20, rect.width + pad * 2)}px`;
    dom.tourHighlight.style.height = `${Math.max(20, rect.height + pad * 2)}px`;
    return rect;
}

function positionTourCard(targetRect) {
    if (!dom.tourCard) {
        return;
    }

    const margin = 14;
    const cardRect = dom.tourCard.getBoundingClientRect();
    const maxLeft = Math.max(margin, window.innerWidth - cardRect.width - margin);
    const maxTop = Math.max(margin, window.innerHeight - cardRect.height - margin);

    let left = maxLeft;
    let top = maxTop;

    if (targetRect) {
        const rightSpace = window.innerWidth - targetRect.right;
        const leftSpace = targetRect.left;
        const canFitRight = rightSpace >= cardRect.width + margin * 2;
        const canFitLeft = leftSpace >= cardRect.width + margin * 2;

        if (canFitRight) {
            left = Math.min(maxLeft, targetRect.right + margin);
            top = Math.min(maxTop, Math.max(margin, targetRect.top));
        } else if (canFitLeft) {
            left = Math.max(margin, targetRect.left - cardRect.width - margin);
            top = Math.min(maxTop, Math.max(margin, targetRect.top));
        } else {
            left = Math.min(maxLeft, Math.max(margin, targetRect.left));
            const belowTop = targetRect.bottom + margin;
            const aboveTop = targetRect.top - cardRect.height - margin;
            if (belowTop <= maxTop) {
                top = belowTop;
            } else if (aboveTop >= margin) {
                top = aboveTop;
            } else {
                top = maxTop;
            }
        }
    }

    dom.tourCard.style.left = `${Math.max(margin, Math.min(maxLeft, left))}px`;
    dom.tourCard.style.top = `${Math.max(margin, Math.min(maxTop, top))}px`;
}

function renderCountyTableView() {
    if (!dom.countyTableBody || !dom.countyTableSubtitle) {
        return;
    }

    const features = appState.geojson && Array.isArray(appState.geojson.features)
        ? appState.geojson.features
        : [];

    if (!features.length) {
        dom.countyTableSubtitle.textContent = 'No county data loaded';
        dom.countyTableBody.innerHTML = '<div class="county-table-empty">County data is loading.</div>';
        return;
    }

    const counties = features
        .map((feature) => toCountyStateModel(feature.properties || {}))
        .filter((county) => county.fips_code && county.county_name)
        .sort((a, b) => {
            const scoreA = safeNumber(a.composite_score);
            const scoreB = safeNumber(b.composite_score);
            if (scoreA === null && scoreB === null) {
                return a.county_name.localeCompare(b.county_name);
            }
            if (scoreA === null) {
                return 1;
            }
            if (scoreB === null) {
                return -1;
            }
            if (scoreA !== scoreB) {
                return scoreB - scoreA;
            }
            return a.county_name.localeCompare(b.county_name);
        });

    const filteredCounties = appState.legendFilter
        ? counties.filter((county) => county.bivariate_key === appState.legendFilter.key)
        : counties;

    dom.countyTableSubtitle.textContent = appState.legendFilter
        ? `${filteredCounties.length} of ${counties.length} counties ranked best to worst`
        : `${counties.length} counties ranked best to worst`;

    if (!filteredCounties.length) {
        dom.countyTableBody.innerHTML = '<div class="county-table-empty">No counties match the current legend filter.</div>';
        return;
    }

    const selectedFips = appState.selectedCounty ? appState.selectedCounty.fips_code : '';
    const rows = filteredCounties.map((county, index) => {
        const score = safeNumber(county.composite_score);
        const scoreText = score === null ? 'N/A' : score.toFixed(3);
        const trajectoryLabel = TRAJECTORY_SUMMARY_LABELS[county.directional_class] || 'Unknown';
        const selectedClass = county.fips_code === selectedFips ? ' selected' : '';
        const trajectoryClass = TRAJECTORY_ORDER.includes(county.directional_class)
            ? county.directional_class
            : 'stable';
        const scoreFill = score === null ? 0 : Math.max(0, Math.min(100, Math.round(score * 100)));
        const scoreColor = county.bivariate_color || getBivariateColor(county.bivariate_key || '', 'ui');
        return `
            <tr class="trajectory-${escapeHtml(trajectoryClass)}${selectedClass}" data-fips-code="${escapeHtml(county.fips_code)}" tabindex="0" role="button" aria-label="Select ${escapeHtml(county.county_name)} county">
                <td class="rank-cell">#${index + 1}</td>
                <td class="county-cell">${escapeHtml(county.county_name)}</td>
                <td class="trajectory-cell">
                    <span class="trajectory-pill trajectory-${escapeHtml(trajectoryClass)}">${escapeHtml(trajectoryLabel)}</span>
                </td>
                <td class="score-cell">
                    <span class="score-chip" style="--score-fill:${scoreFill}%; --score-color:${escapeHtml(scoreColor)};">${scoreText}</span>
                </td>
            </tr>
        `;
    }).join('');

    dom.countyTableBody.innerHTML = `
        <table class="county-table-grid" aria-label="Maryland county table">
            <thead>
                <tr>
                    <th>Rank</th>
                    <th>County</th>
                    <th>Trajectory</th>
                    <th>Score</th>
                </tr>
            </thead>
            <tbody>${rows}</tbody>
        </table>
    `;
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
    appState.map.dragRotate.disable();
    appState.map.touchZoomRotate.disableRotation();

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
    appState.map.on('remove', () => {
        stopDemographicFlowAnimation();
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

    const flashLegendTransparency = (duration = 180) => {
        if (appState.clickOpacityTimer) {
            clearTimeout(appState.clickOpacityTimer);
        }
        document.body.classList.add('map-clicking');
        appState.clickOpacityTimer = window.setTimeout(() => {
            document.body.classList.remove('map-clicking');
        }, duration);
    };

    appState.map.on('movestart', () => setMoving(true));
    appState.map.on('moveend', () => setMoving(false));
    appState.map.on('mousedown', () => flashLegendTransparency(220));
    appState.map.on('touchstart', () => flashLegendTransparency(240));
    appState.map.on('click', () => flashLegendTransparency(190));
}

async function loadCountyLayer() {
    try {
        const geojson = await loadCountyGeoJson();
        appState.geojson = decorateGeoJson(geojson);
        populateCountySearch();
        renderCountyTableView();

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
            id: 'counties-hover-fill',
            type: 'fill',
            source: 'counties',
            filter: ['==', 'fips_code', ''],
            paint: {
                'fill-color': '#ffffff',
                'fill-opacity': 0.14
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

        appState.countyLayerLoaded = true;
        appState.countyLayerRetryAttempt = 0;
        clearCountyLayerRetry();

        addDemographicFlowLayers();
        await addCountyIllustrationLayers();
        wireCountyInteractions();
        applyLegendFilter();
    } catch (error) {
        appState.countyLayerLoaded = false;
        const delaySeconds = Math.max(
            1,
            Math.round(
                Math.min(
                    COUNTY_LAYER_AUTO_RECOVER_MS * (appState.countyLayerRetryAttempt + 1),
                    30000
                ) / 1000
            )
        );
        renderTransientPanelError(
            `${getReadableFetchError(error, '/layers/counties/latest')} Retrying in ${delaySeconds}s.`
        );
        scheduleCountyLayerRetry();
    }
}

async function addCountyIllustrationLayers() {
    if (!appState.map || !appState.geojson || !Array.isArray(appState.geojson.features)) {
        return;
    }

    await ensureIllustrationIcons();

    const data = buildCountyIllustrationGeoJson(appState.geojson.features);
    const sourceId = 'county-illustrations';

    if (appState.map.getSource(sourceId)) {
        appState.map.getSource(sourceId).setData(data);
        return;
    }

    appState.map.addSource(sourceId, {
        type: 'geojson',
        data
    });

    appState.map.addLayer({
        id: 'county-illustration-glow',
        type: 'circle',
        source: sourceId,
        paint: {
            'circle-color': ['get', 'accent'],
            'circle-radius': 10,
            'circle-opacity': 0.2,
            'circle-blur': 0.35
        }
    });

    appState.map.addLayer({
        id: 'county-illustration-hover',
        type: 'circle',
        source: sourceId,
        filter: ['==', 'feature_id', ''],
        paint: {
            'circle-color': '#ffffff',
            'circle-radius': 12,
            'circle-opacity': 0.28,
            'circle-stroke-color': 'rgba(30, 71, 109, 0.65)',
            'circle-stroke-width': 1.4
        }
    });

    appState.map.addLayer({
        id: 'county-illustration-symbols',
        type: 'symbol',
        source: sourceId,
        layout: {
            'icon-image': ['get', 'icon_id'],
            'icon-size': 1,
            'icon-allow-overlap': true,
            'icon-ignore-placement': true
        },
        paint: {
            'icon-opacity': 0.94
        }
    });

    wireIllustrationInteractions();
}

async function ensureIllustrationIcons() {
    const iconSvgs = {
        'atlas-icon-momentum-up': `
            <svg xmlns="http://www.w3.org/2000/svg" width="64" height="64" viewBox="0 0 64 64">
                <g fill="none" stroke="none">
                    <circle cx="32" cy="32" r="30" fill="rgba(96,160,73,0.24)"/>
                    <path d="M32 13l14 14h-8v21h-12V27h-8z" fill="#5d9a3b"/>
                </g>
            </svg>
        `,
        'atlas-icon-family': `
            <svg xmlns="http://www.w3.org/2000/svg" width="64" height="64" viewBox="0 0 64 64">
                <g fill="none" stroke="none">
                    <circle cx="32" cy="32" r="30" fill="rgba(47,111,180,0.22)"/>
                    <circle cx="25" cy="24" r="6" fill="#2f6fb4"/>
                    <circle cx="38" cy="22" r="5" fill="#2f6fb4"/>
                    <path d="M16 45c0-6 4-11 9-11s9 5 9 11H16z" fill="#2f6fb4"/>
                    <path d="M31 45c0-5 3.2-9.2 7.2-9.2S45.4 40 45.4 45H31z" fill="#2f6fb4"/>
                </g>
            </svg>
        `,
        'atlas-icon-house': `
            <svg xmlns="http://www.w3.org/2000/svg" width="64" height="64" viewBox="0 0 64 64">
                <g fill="none" stroke="none">
                    <circle cx="32" cy="32" r="30" fill="rgba(187,127,42,0.22)"/>
                    <path d="M15 31L32 17l17 14v16a3 3 0 0 1-3 3H18a3 3 0 0 1-3-3z" fill="#bb7f2a"/>
                    <rect x="28" y="36" width="8" height="14" rx="1.2" fill="#f6f0e7"/>
                </g>
            </svg>
        `
    };

    const loads = Object.entries(iconSvgs).map(([id, svg]) => loadSvgIcon(id, svg));
    await Promise.all(loads);
}

function loadSvgIcon(iconId, svgMarkup) {
    return new Promise((resolve, reject) => {
        if (!appState.map) {
            resolve();
            return;
        }

        if (appState.map.hasImage(iconId)) {
            resolve();
            return;
        }

        const image = new Image(64, 64);
        image.onload = () => {
            if (!appState.map || appState.map.hasImage(iconId)) {
                resolve();
                return;
            }
            appState.map.addImage(iconId, image, { pixelRatio: 2 });
            resolve();
        };
        image.onerror = () => {
            reject(new Error(`Could not load map icon: ${iconId}`));
        };
        image.src = `data:image/svg+xml;charset=utf-8,${encodeURIComponent(svgMarkup)}`;
    });
}

function buildCountyIllustrationGeoJson(features) {
    const housingValues = features
        .map((feature) => safeNumber(
            feature.properties &&
            (feature.properties.housing_elasticity_score ||
            (feature.properties.layer_scores && feature.properties.layer_scores.housing_elasticity))
        ))
        .filter((value) => value !== null)
        .sort((a, b) => a - b);

    const housingThreshold = housingValues.length
        ? housingValues[Math.floor(housingValues.length * 0.6)]
        : 0.5;

    const iconFeatures = [];

    features.forEach((feature) => {
        const props = feature.properties || {};
        const fips = props.fips_code || props.geoid;
        if (!fips) {
            return;
        }

        const countyName = props.county_name || countyNameFromFips(fips);
        const center = appState.countyCentersByFips.get(fips) || getFeatureCenter(feature);
        if (!center) {
            return;
        }

        const trajectory = normalizeTrajectory(props.directional_class || props.directional_status);
        const score = safeNumber(props.composite_score);
        const housingScore = safeNumber(
            props.housing_elasticity_score ||
            (props.layer_scores && props.layer_scores.housing_elasticity)
        );
        const strength = props.signal_strength || deriveStrength(score);

        const shouldAdd = {
            momentum: trajectory === 'improving',
            viability: score !== null && score >= appState.strengthThresholds.q2,
            housing: housingScore !== null && housingScore >= housingThreshold
        };

        MAP_ILLUSTRATION_DEFINITIONS.forEach((definition) => {
            if (!shouldAdd[definition.key]) {
                return;
            }

            const [lng, lat] = offsetLngLatByKm(center, definition.offsetKm[0], definition.offsetKm[1]);
            iconFeatures.push({
                type: 'Feature',
                geometry: {
                    type: 'Point',
                    coordinates: [lng, lat]
                },
                properties: {
                    feature_id: `${fips}-${definition.key}`,
                    fips_code: fips,
                    county_name: countyName,
                    icon_key: definition.key,
                    icon_id: definition.iconId,
                    tooltip: definition.tooltip,
                    accent: definition.accent,
                    directional_class: trajectory,
                    composite_score: score,
                    signal_strength: strength
                }
            });
        });
    });

    return {
        type: 'FeatureCollection',
        features: iconFeatures
    };
}

function addDemographicFlowLayers() {
    if (!appState.map || !appState.geojson || !Array.isArray(appState.geojson.features)) {
        return;
    }

    const sourceId = 'demographic-flows';
    const data = buildDemographicFlowGeoJson(appState.geojson.features);

    if (appState.map.getSource(sourceId)) {
        appState.map.getSource(sourceId).setData(data);
        if (data.features.length) {
            startDemographicFlowAnimation();
        } else {
            stopDemographicFlowAnimation();
        }
        return;
    }

    appState.map.addSource(sourceId, {
        type: 'geojson',
        data
    });

    const beforeLayerId = appState.map.getLayer('counties-hover-fill') ? 'counties-hover-fill' : undefined;

    appState.map.addLayer({
        id: 'demographic-flow-line-glow',
        type: 'line',
        source: sourceId,
        layout: {
            'line-cap': 'round',
            'line-join': 'round'
        },
        paint: {
            'line-color': 'rgba(90, 173, 232, 0.38)',
            'line-width': [
                'interpolate',
                ['linear'],
                ['coalesce', ['get', 'flow_strength'], 0],
                0, 1.4,
                1, 4.2
            ],
            'line-opacity': 0.36,
            'line-blur': 0.55
        }
    }, beforeLayerId);

    appState.map.addLayer({
        id: 'demographic-flow-line',
        type: 'line',
        source: sourceId,
        layout: {
            'line-cap': 'round',
            'line-join': 'round'
        },
        paint: {
            'line-color': 'rgba(61, 153, 223, 0.95)',
            'line-width': [
                'interpolate',
                ['linear'],
                ['coalesce', ['get', 'flow_strength'], 0],
                0, 1.1,
                1, 3
            ],
            'line-opacity': 0.88,
            'line-dasharray': FLOW_DASH_SEQUENCE[0]
        }
    }, beforeLayerId);

    appState.map.addLayer({
        id: 'demographic-flow-arrows',
        type: 'symbol',
        source: sourceId,
        layout: {
            'symbol-placement': 'line',
            'symbol-spacing': 78,
            'text-field': '▶',
            'text-size': 11,
            'text-keep-upright': false,
            'text-allow-overlap': true,
            'text-ignore-placement': true
        },
        paint: {
            'text-color': '#4ba6e8',
            'text-opacity': 0.86,
            'text-halo-color': 'rgba(250, 253, 255, 0.84)',
            'text-halo-width': 0.65
        }
    }, beforeLayerId);

    if (data.features.length) {
        startDemographicFlowAnimation();
    }
}

function buildDemographicFlowGeoJson(features) {
    const counties = features.map((feature) => {
        const props = feature.properties || {};
        const fipsCode = props.fips_code || props.geoid;
        if (!fipsCode) {
            return null;
        }

        const center = appState.countyCentersByFips.get(fipsCode) || getFeatureCenter(feature);
        if (!center) {
            return null;
        }

        const score = safeNumber(
            props.demographic_momentum_score ??
            (props.layer_scores && props.layer_scores.demographic_momentum)
        );
        if (score === null) {
            return null;
        }

        return {
            fips_code: fipsCode,
            county_name: props.county_name || countyNameFromFips(fipsCode),
            center,
            score
        };
    }).filter(Boolean);

    if (counties.length < 2) {
        return { type: 'FeatureCollection', features: [] };
    }

    counties.sort((a, b) => b.score - a.score);
    const hubs = counties.slice(0, Math.max(4, Math.min(9, Math.ceil(counties.length * 0.35))));
    const lines = [];
    const seen = new Set();

    hubs.forEach((hub) => {
        const candidates = counties
            .filter((county) => county.fips_code !== hub.fips_code && county.score <= hub.score - 0.01)
            .map((county) => ({
                county,
                distanceKm: distanceBetweenPointsKm(county.center, hub.center)
            }))
            .filter((entry) => entry.distanceKm <= 225)
            .sort((a, b) => a.distanceKm - b.distanceKm)
            .slice(0, 2);

        candidates.forEach(({ county, distanceKm }) => {
            const key = `${county.fips_code}->${hub.fips_code}`;
            if (seen.has(key)) {
                return;
            }
            seen.add(key);

            const scoreGap = Math.max(0, hub.score - county.score);
            const normalizedGap = Math.max(0, Math.min(1, scoreGap / 0.45));
            const distanceBias = Math.max(0, 1 - (distanceKm / 225));
            const strength = Math.max(0.2, Math.min(1, normalizedGap * 0.72 + distanceBias * 0.28));

            lines.push({
                type: 'Feature',
                geometry: {
                    type: 'LineString',
                    coordinates: [county.center, hub.center]
                },
                properties: {
                    from_fips: county.fips_code,
                    to_fips: hub.fips_code,
                    from_name: county.county_name,
                    to_name: hub.county_name,
                    flow_strength: strength
                }
            });
        });
    });

    if (lines.length < 5 && counties.length >= 3) {
        for (let index = 0; index < Math.min(6, counties.length - 1); index += 1) {
            const fromCounty = counties[index + 1];
            const toCounty = counties[index];
            const key = `${fromCounty.fips_code}->${toCounty.fips_code}`;
            if (seen.has(key)) {
                continue;
            }
            seen.add(key);
            lines.push({
                type: 'Feature',
                geometry: {
                    type: 'LineString',
                    coordinates: [fromCounty.center, toCounty.center]
                },
                properties: {
                    from_fips: fromCounty.fips_code,
                    to_fips: toCounty.fips_code,
                    from_name: fromCounty.county_name,
                    to_name: toCounty.county_name,
                    flow_strength: 0.34
                }
            });
        }
    }

    return {
        type: 'FeatureCollection',
        features: lines
    };
}

function startDemographicFlowAnimation() {
    if (!appState.map || !appState.map.getLayer('demographic-flow-line')) {
        return;
    }

    stopDemographicFlowAnimation();

    let step = 0;
    appState.flowAnimationTimer = window.setInterval(() => {
        if (!appState.map || !appState.map.getLayer('demographic-flow-line')) {
            stopDemographicFlowAnimation();
            return;
        }

        appState.map.setPaintProperty(
            'demographic-flow-line',
            'line-dasharray',
            FLOW_DASH_SEQUENCE[step]
        );
        step = (step + 1) % FLOW_DASH_SEQUENCE.length;
    }, 170);
}

function stopDemographicFlowAnimation() {
    if (appState.flowAnimationTimer) {
        window.clearInterval(appState.flowAnimationTimer);
        appState.flowAnimationTimer = null;
    }
}

function distanceBetweenPointsKm(pointA, pointB) {
    if (
        !Array.isArray(pointA) ||
        !Array.isArray(pointB) ||
        pointA.length < 2 ||
        pointB.length < 2
    ) {
        return Number.POSITIVE_INFINITY;
    }

    const toRadians = (value) => (value * Math.PI) / 180;
    const earthRadiusKm = 6371;

    const dLat = toRadians(pointB[1] - pointA[1]);
    const dLon = toRadians(pointB[0] - pointA[0]);
    const lat1 = toRadians(pointA[1]);
    const lat2 = toRadians(pointB[1]);

    const a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
        Math.sin(dLon / 2) * Math.sin(dLon / 2) * Math.cos(lat1) * Math.cos(lat2);
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    return earthRadiusKm * c;
}

function offsetLngLatByKm(center, offsetXKm, offsetYKm) {
    const [lng, lat] = center;
    const latRad = (lat * Math.PI) / 180;
    const latDelta = offsetYKm / 110.574;
    const denom = Math.max(0.1, 111.320 * Math.cos(latRad));
    const lngDelta = offsetXKm / denom;
    return [lng + lngDelta, lat + latDelta];
}

async function loadCountyGeoJson() {
    let lastError = null;

    for (let attempt = 1; attempt <= COUNTY_GEOJSON_MAX_ATTEMPTS; attempt += 1) {
        try {
            const { response, url: resolvedUrl } = await fetchApi('/layers/counties/latest', {
                cache: 'no-store'
            });
            if (!response.ok) {
                throw new Error(`Could not load ${resolvedUrl} (HTTP ${response.status})`);
            }

            const geojson = await response.json();
            if (isValidCountyGeoJson(geojson)) {
                return geojson;
            }
            throw new Error('County GeoJSON response is invalid.');
        } catch (error) {
            lastError = error;
            if (attempt < COUNTY_GEOJSON_MAX_ATTEMPTS) {
                const delayMs = Math.min(
                    COUNTY_GEOJSON_RETRY_BASE_MS * (2 ** (attempt - 1)),
                    8000
                );
                await wait(delayMs);
            }
        }
    }

    throw new Error(
        `${getReadableFetchError(lastError, '/layers/counties/latest')} ` +
        `Retried ${COUNTY_GEOJSON_MAX_ATTEMPTS} times.`
    );
}

function isValidCountyGeoJson(geojson) {
    return Boolean(
        geojson &&
        Array.isArray(geojson.features) &&
        geojson.features.length > 0
    );
}

function clearCountyLayerRetry() {
    if (appState.countyLayerRetryTimer) {
        window.clearTimeout(appState.countyLayerRetryTimer);
        appState.countyLayerRetryTimer = null;
    }
}

function scheduleCountyLayerRetry() {
    if (appState.countyLayerLoaded || appState.countyLayerRetryTimer || !appState.map) {
        return;
    }

    const delayMs = Math.min(
        COUNTY_LAYER_AUTO_RECOVER_MS * (appState.countyLayerRetryAttempt + 1),
        30000
    );
    appState.countyLayerRetryAttempt += 1;

    appState.countyLayerRetryTimer = window.setTimeout(async () => {
        appState.countyLayerRetryTimer = null;
        if (!appState.map || appState.countyLayerLoaded) {
            return;
        }
        await loadCountyLayer();
    }, delayMs);
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
        const synthesisGrouping = normalizeSynthesisGrouping(
            props.synthesis_grouping || props.final_grouping,
            trajectory,
            score
        );
        const confidenceClass = normalizeConfidenceClass(
            props.confidence_class || props.confidence_level
        );

        props.directional_class = trajectory;
        props.synthesis_grouping = synthesisGrouping;
        props.confidence_class = confidenceClass;
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

    renderCountyTableView();

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

    if (appState.map.getLayer('county-illustration-symbols')) {
        const iconOpacity = activeKey
            ? ['case', activeCondition, 0.96, 0.2]
            : 0.94;
        appState.map.setPaintProperty('county-illustration-symbols', 'icon-opacity', iconOpacity);
    }

    if (appState.map.getLayer('county-illustration-glow')) {
        const glowOpacity = activeKey
            ? ['case', activeCondition, 0.24, 0.08]
            : 0.2;
        appState.map.setPaintProperty('county-illustration-glow', 'circle-opacity', glowOpacity);
    }

    if (appState.map.getLayer('demographic-flow-line')) {
        appState.map.setPaintProperty(
            'demographic-flow-line',
            'line-opacity',
            activeKey ? 0.46 : 0.88
        );
    }

    if (appState.map.getLayer('demographic-flow-line-glow')) {
        appState.map.setPaintProperty(
            'demographic-flow-line-glow',
            'line-opacity',
            activeKey ? 0.18 : 0.36
        );
    }

    if (appState.map.getLayer('demographic-flow-arrows')) {
        appState.map.setPaintProperty(
            'demographic-flow-arrows',
            'text-opacity',
            activeKey ? 0.4 : 0.86
        );
    }
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
        appState.map.setFilter('counties-hover-fill', ['==', 'fips_code', '']);
        appState.hoverPopupFips = null;
        appState.popup.remove();
    });

    appState.map.on('mousemove', 'counties-fill', (event) => {
        if (appState.map.getLayer('county-illustration-symbols')) {
            const iconHits = appState.map.queryRenderedFeatures(event.point, { layers: ['county-illustration-symbols'] });
            if (iconHits.length) {
                return;
            }
        }

        const feature = event.features && event.features[0];
        if (!feature || !feature.properties) {
            appState.popup.remove();
            return;
        }

        const props = feature.properties;
        const fips = props.fips_code;
        appState.map.setFilter('counties-hover', ['==', 'fips_code', fips || '']);
        appState.map.setFilter('counties-hover-fill', ['==', 'fips_code', fips || '']);

        const score = safeNumber(props.composite_score);
        const scoreText = score === null ? 'N/A' : score.toFixed(3);
        const trajectory = normalizeTrajectory(props.directional_class || props.directional_status);
        const strength = props.signal_strength || deriveStrength(score);
        const key = props.bivariate_key || `${trajectory}|${strength}`;
        const label = props.bivariate_label || buildSignalLabel(trajectory, strength);
        const color = props.bivariate_color || getBivariateColor(key);
        const html = `<div style="font-family:'Work Sans',sans-serif; color:#1c3a56; min-width:170px;">
                    <div style="font-weight:700; font-size:13px; margin-bottom:4px;">${escapeHtml(props.county_name || 'County')}</div>
                    <div style="font-size:12px; color:#48637c; margin-bottom:6px;">Overall Signal Score: <strong>${scoreText}</strong></div>
                    <div style="display:inline-block; border:1px solid rgba(19,38,56,0.16); border-radius:999px; padding:4px 8px; font-size:11px; font-weight:600; background:${color}; color:#18324b;">
                        ${escapeHtml(label || 'County Signal')}
                    </div>
                </div>`;
        const popupFips = fips || '';

        if (!appState.popup.isOpen()) {
            appState.popup
                .setLngLat(event.lngLat)
                .setHTML(html)
                .addTo(appState.map);
            appState.hoverPopupFips = popupFips;
            return;
        }

        appState.popup.setLngLat(event.lngLat);
        if (appState.hoverPopupFips !== popupFips) {
            appState.popup.setHTML(html);
            appState.hoverPopupFips = popupFips;
        }
    });

    appState.map.on('click', 'counties-fill', async (event) => {
        const feature = event.features && event.features[0];
        if (!feature || !feature.properties) {
            return;
        }
        await handleCountySelection(feature.properties.fips_code, feature.properties);
    });

    appState.map.on('click', 'county-illustration-symbols', async (event) => {
        const feature = event.features && event.features[0];
        if (!feature || !feature.properties) {
            return;
        }
        const fipsCode = feature.properties.fips_code;
        if (!fipsCode) {
            return;
        }
        const countyFeature = appState.countyFeaturesByFips.get(fipsCode);
        if (!countyFeature || !countyFeature.properties) {
            return;
        }
        await handleCountySelection(fipsCode, countyFeature.properties);
    });

    appState.map.on('click', (event) => {
        const features = appState.map.queryRenderedFeatures(event.point, { layers: ['counties-fill'] });
        if (features.length === 0) {
            appState.map.setFilter('counties-hover', ['==', 'fips_code', '']);
            appState.map.setFilter('counties-hover-fill', ['==', 'fips_code', '']);
            appState.hoverPopupFips = null;
            appState.popup.remove();
        }
    });
}

function wireIllustrationInteractions() {
    if (!appState.map) {
        return;
    }

    appState.map.on('mouseenter', 'county-illustration-symbols', () => {
        appState.map.getCanvas().style.cursor = 'pointer';
    });

    appState.map.on('mouseleave', 'county-illustration-symbols', () => {
        appState.map.getCanvas().style.cursor = '';
        if (appState.map.getLayer('county-illustration-hover')) {
            appState.map.setFilter('county-illustration-hover', ['==', 'feature_id', '']);
        }
        appState.hoverPopupFips = null;
        appState.popup.remove();
    });

    appState.map.on('mousemove', 'county-illustration-symbols', (event) => {
        const feature = event.features && event.features[0];
        if (!feature || !feature.properties) {
            return;
        }

        const props = feature.properties;
        const featureId = props.feature_id || '';
        const countyName = props.county_name || 'County';
        const tooltip = props.tooltip || 'County signal indicator';
        const html = `
            <div style="font-family:'Work Sans',sans-serif; color:#1c3a56; min-width:190px;">
                <div style="font-weight:700; font-size:13px; margin-bottom:4px;">${escapeHtml(countyName)}</div>
                <div style="font-size:12px; color:#48637c;">${escapeHtml(tooltip)}</div>
            </div>
        `;

        if (appState.map.getLayer('county-illustration-hover')) {
            appState.map.setFilter('county-illustration-hover', ['==', 'feature_id', featureId]);
        }

        if (!appState.popup.isOpen()) {
            appState.popup
                .setLngLat(event.lngLat)
                .setHTML(html)
                .addTo(appState.map);
            appState.hoverPopupFips = `icon:${featureId}`;
            return;
        }

        appState.popup.setLngLat(event.lngLat);
        const popupKey = `icon:${featureId}`;
        if (appState.hoverPopupFips !== popupKey) {
            appState.popup.setHTML(html);
            appState.hoverPopupFips = popupKey;
        }
    });
}

async function handleCountySelection(fipsCode, featureProps) {
    if (!fipsCode) {
        return;
    }

    if (appState.chat.open) {
        exitChatMode();
    }

    const optimistic = toCountyStateModel(featureProps);
    if (optimistic.county_name) {
        dom.searchInput.value = optimistic.county_name;
    }
    highlightSelectedCounty(fipsCode);
    focusCountyByFips(fipsCode);

    if (appState.compare.active) {
        appState.selectedCounty = optimistic;
        const upsertResult = upsertCompareCounty(optimistic);
        if (upsertResult.limitReached) {
            renderTransientPanelError('Compare Builder supports up to 4 counties. Remove one to add another.');
        }
        renderCompareBuilder();
        renderComparePanel();
    } else {
        appState.selectedCounty = optimistic;
        renderStoryPanel(optimistic, { loading: true });
    }
    renderCountyTableView();

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
        const index = appState.compare.counties.findIndex((county) => county.fips_code === fipsCode);
        if (index !== -1) {
            appState.compare.counties[index] = merged;
        }
        if (appState.selectedCounty && appState.selectedCounty.fips_code === fipsCode) {
            appState.selectedCounty = merged;
        }
        renderCompareBuilder();
        renderComparePanel();
    } else if (appState.selectedCounty && appState.selectedCounty.fips_code === fipsCode) {
        appState.selectedCounty = merged;
        renderStoryPanel(merged);
    }
    renderCountyTableView();
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
    const synthesisGrouping = normalizeSynthesisGrouping(
        raw.synthesis_grouping || raw.final_grouping,
        trajectory,
        score
    );
    const confidenceClass = normalizeConfidenceClass(raw.confidence_class || raw.confidence_level);

    return {
        fips_code: fips,
        county_name: raw.county_name || countyNameFromFips(fips),
        data_year: raw.data_year || null,
        directional_class: trajectory,
        confidence_class: confidenceClass,
        synthesis_grouping: synthesisGrouping,
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
    const confidenceClass = normalizeConfidenceClass(merged.confidence_class || merged.confidence_level);
    const synthesisGrouping = normalizeSynthesisGrouping(
        merged.synthesis_grouping || merged.final_grouping,
        trajectory,
        score
    );
    const strength = deriveStrength(score);
    const key = `${trajectory}|${strength}`;

    merged.directional_class = trajectory;
    merged.confidence_class = confidenceClass;
    merged.synthesis_grouping = synthesisGrouping;
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

function normalizeConfidenceClass(value) {
    const normalized = String(value || '').trim().toLowerCase();
    if (normalized === 'strong' || normalized === 'conditional' || normalized === 'fragile') {
        return normalized;
    }
    return 'unknown';
}

function normalizeSynthesisGrouping(value, trajectory = 'stable', score = null) {
    const normalized = String(value || '').trim().toLowerCase();
    if (normalized && SYNTHESIS_LABELS[normalized]) {
        return normalized;
    }

    if (score === null) {
        return 'high_uncertainty';
    }
    if (trajectory === 'improving') {
        return score >= 0.5 ? 'emerging_tailwinds' : 'conditional_growth';
    }
    if (trajectory === 'at_risk') {
        return 'at_risk_headwinds';
    }
    return 'stable_constrained';
}

function getSynthesisDescription(grouping) {
    const descriptions = {
        emerging_tailwinds: 'Multiple reinforcing tailwinds are present across available real-data layers. Persistence is likely if current conditions hold.',
        conditional_growth: 'Upside exists, but delivery risk and local context drive outcomes. Signals are mixed across layers.',
        stable_constrained: 'Systems are steady with balanced pressures, but upside is limited under current conditions.',
        at_risk_headwinds: 'Structural headwinds dominate, creating challenges for growth capacity and resilience.',
        high_uncertainty: 'Coverage is thin or inconsistent across layers; interpret cautiously and prioritize local validation.'
    };

    return descriptions[grouping] || 'No synthesis description available.';
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
    appState.compare.counties = [];
    appState.compare.splitView = false;
    destroySplitMaps();
    setCompareButtonState(false, true);
    renderCompareBuilder();

    const score = county.composite_score;
    const scoreText = score === null ? 'N/A' : score.toFixed(3);
    const trendText = describeTrajectory(county.directional_class, score);
    const strengths = county.primary_strengths.length
        ? county.primary_strengths.slice(0, 2)
        : ['No named reinforcing strengths available yet.'];
    const synthesisGrouping = normalizeSynthesisGrouping(
        county.synthesis_grouping,
        county.directional_class,
        score
    );
    const synthesisLabel = SYNTHESIS_LABELS[synthesisGrouping] || 'Unclassified';
    const synthesisColor = SYNTHESIS_COLORS[synthesisGrouping] || '#8d97a2';
    const synthesisTextColor = synthesisGrouping === 'stable_constrained' ? '#3f3f3f' : '#ffffff';
    const confidenceLabel = CONFIDENCE_LABELS[county.confidence_class] || 'Unknown';
    const trajectorySummary = TRAJECTORY_SUMMARY_LABELS[county.directional_class] || 'Unknown';
    const projection = buildProjectionModel(county);
    const rankedLayers = LAYER_ROWS
        .map(([key, label]) => ({ key, label, value: safeNumber(county.layer_scores && county.layer_scores[key]) }))
        .filter((item) => item.value !== null)
        .sort((a, b) => b.value - a.value);
    const strongestLayer = rankedLayers[0] || null;
    const weakestLayer = rankedLayers[rankedLayers.length - 1] || null;

    const layerScoresHtml = LAYER_ROWS.map(([key, label]) => {
        const value = safeNumber(county.layer_scores && county.layer_scores[key]);
        const displayValue = value === null ? 'No Data' : value.toFixed(3);
        return `
            <button class="story-score-item story-score-clickable" type="button" data-layer-key="${escapeHtml(key)}" title="Click for factor breakdown">
                <div class="story-score-label">${renderLayerLabelWithIcon(key, label)}</div>
                <div class="story-score-value ${value === null ? 'null' : ''}">${displayValue}</div>
                ${renderStoryScoreBar(value)}
            </button>
        `;
    }).join('');

    dom.panel.dataset.state = 'story';
    dom.panel.classList.add('detailed');
    dom.panel.classList.remove('chat-mode');
    dom.panelTitle.textContent = county.county_name || 'County Story';
    dom.panelSubtitle.textContent = `Data Year: ${county.data_year || 'N/A'}`;

    const errorHtml = options.error
        ? `<div class="error-inline">${escapeHtml(options.error)}</div>`
        : '';

    const loadingLabel = options.loading
        ? '<span style="font-size:12px;color:#607286;">Refreshing county story...</span>'
        : '';

    const strongestLayerHtml = strongestLayer
        ? `${renderLayerLabelWithIcon(strongestLayer.key, strongestLayer.label)} <strong>${strongestLayer.value.toFixed(3)}</strong>`
        : 'Not enough data to determine strongest layer.';
    const weakestLayerHtml = weakestLayer
        ? `${renderLayerLabelWithIcon(weakestLayer.key, weakestLayer.label)} <strong>${weakestLayer.value.toFixed(3)}</strong>`
        : 'Not enough data to determine pressure layer.';
    const askAtlasSectionHtml = isChatEnabled()
        ? `
            <section class="story-section">
                <h4>Ask Atlas</h4>
                <div class="quick-prompts">
                    ${QUICK_PROMPTS.map((prompt, idx) => `<button class="quick-prompt" type="button" data-quick-prompt="${idx}">${escapeHtml(shortPromptLabel(prompt))}</button>`).join('')}
                </div>
            </section>
        `
        : '';

    dom.panelBody.innerHTML = `
        <div style="display:flex; align-items:center; justify-content:space-between; gap:10px; flex-wrap:wrap;">
            <span class="signal-chip" style="background:${county.bivariate_color}; color:#15334f;">${escapeHtml(county.bivariate_label)}</span>
            <span class="score-pill">${scoreText}</span>
        </div>

        ${loadingLabel}
        ${errorHtml}

        <div class="story-tabs" role="tablist" aria-label="County panel tabs">
            <button class="story-tab active" type="button" data-story-tab="overview" aria-selected="true">Overview</button>
            <button class="story-tab" type="button" data-story-tab="layers" aria-selected="false">Layers</button>
            <button class="story-tab" type="button" data-story-tab="projections" aria-selected="false">Projections</button>
        </div>

        <div class="story-tab-content active" data-story-tab-content="overview">
            <section class="story-section">
                <h4>County Growth Synthesis</h4>
                <span class="synthesis-badge" style="background:${synthesisColor}; color:${synthesisTextColor};">${escapeHtml(synthesisLabel)}</span>
                <p style="margin-top:8px;">${escapeHtml(getSynthesisDescription(synthesisGrouping))}</p>
            </section>

            <section class="story-section">
                <h4>Classification Details</h4>
                <div class="story-score-grid">
                    <div class="story-score-item">
                        <div class="story-score-label">Directional Trajectory</div>
                        <div class="story-score-value">${escapeHtml(trajectorySummary)}</div>
                    </div>
                    <div class="story-score-item">
                        <div class="story-score-label">Evidence Confidence</div>
                        <div class="story-score-value">${escapeHtml(confidenceLabel)}</div>
                    </div>
                    <div class="story-score-item">
                        <div class="story-score-label">Composite Score</div>
                        <div class="story-score-value">${scoreText}</div>
                        ${renderStoryScoreBar(score)}
                    </div>
                </div>
            </section>

            <section class="story-section">
                <h4>Trajectory Snapshot</h4>
                <p>${escapeHtml(trendText)}</p>
                <div class="story-list" style="margin-top:8px;">
                    ${strengths.map((item) => `<div class="story-list-item">• ${escapeHtml(item)}</div>`).join('')}
                </div>
            </section>

            <section class="story-section">
                <h4>Top Signal Drivers</h4>
                <div class="story-list" style="margin-top:4px;">
                    <div class="story-list-item">Strongest Layer: ${strongestLayerHtml}</div>
                    <div class="story-list-item">Pressure Layer: ${weakestLayerHtml}</div>
                </div>
            </section>

            ${askAtlasSectionHtml}
        </div>

        <div class="story-tab-content" data-story-tab-content="layers">
            <section class="story-section">
                <h4>Layer Scores</h4>
                <p>Hover an icon for plain-language layer meaning, then click a row for full factor breakdown.</p>
                <div class="story-score-grid">
                    ${layerScoresHtml}
                </div>
            </section>
        </div>

        <div class="story-tab-content" data-story-tab-content="projections">
            <section class="story-section">
                <h4>Indicative Outlook</h4>
                <p>${escapeHtml(projection.summary)}</p>
                <div class="projection-grid">
                    <article class="projection-card">
                        <div class="projection-horizon">12-Month Signal</div>
                        <div class="projection-score">${projection.oneYearScore}</div>
                        <div class="projection-status ${projection.oneYearClass}">${escapeHtml(projection.oneYearLabel)}</div>
                    </article>
                    <article class="projection-card">
                        <div class="projection-horizon">36-Month Signal</div>
                        <div class="projection-score">${projection.threeYearScore}</div>
                        <div class="projection-status ${projection.threeYearClass}">${escapeHtml(projection.threeYearLabel)}</div>
                    </article>
                </div>
            </section>

            <section class="story-section">
                <h4>Projection Notes</h4>
                <div class="story-list">
                    ${projection.notes.map((note) => `<div class="story-list-item">• ${escapeHtml(note)}</div>`).join('')}
                </div>
            </section>
        </div>
    `;

    bindStoryTabButtons();
    bindLayerScoreButtons(county);
    bindQuickPromptButtons();
    showFloatingAnalysisPanel(county);
}

function bindStoryTabButtons() {
    const tabs = Array.from(dom.panelBody.querySelectorAll('[data-story-tab]'));
    const panels = Array.from(dom.panelBody.querySelectorAll('[data-story-tab-content]'));
    if (!tabs.length || !panels.length) {
        return;
    }

    const activateTab = (tabId) => {
        tabs.forEach((tab) => {
            const isActive = tab.dataset.storyTab === tabId;
            tab.classList.toggle('active', isActive);
            tab.setAttribute('aria-selected', isActive ? 'true' : 'false');
        });

        panels.forEach((panel) => {
            const isActive = panel.dataset.storyTabContent === tabId;
            panel.classList.toggle('active', isActive);
        });
    };

    tabs.forEach((tab) => {
        tab.addEventListener('click', () => {
            activateTab(tab.dataset.storyTab);
        });
    });
}

function bindLayerScoreButtons(county) {
    const layerButtons = Array.from(dom.panelBody.querySelectorAll('[data-layer-key]'));
    if (!layerButtons.length || !county || !county.fips_code) {
        return;
    }

    layerButtons.forEach((button) => {
        button.addEventListener('click', async () => {
            const layerKey = button.dataset.layerKey;
            if (!layerKey) {
                return;
            }
            await loadLayerDetail(county.fips_code, layerKey);
        });
    });
}

async function loadLayerDetail(fipsCode, layerKey) {
    if (!fipsCode || !layerKey) {
        return;
    }
    if (!dom.layerModal || !dom.layerModalBody || !dom.layerModalTitle || !dom.layerModalDesc) {
        return;
    }

    dom.layerModalTitle.textContent = 'Loading layer detail...';
    dom.layerModalDesc.textContent = '';
    dom.layerModalBody.innerHTML = '<div class="empty-state">Fetching factor-level breakdown...</div>';
    dom.layerModal.classList.add('open');
    dom.layerModal.setAttribute('aria-hidden', 'false');

    try {
        const { response } = await fetchApi(`/areas/${fipsCode}/layers/${layerKey}`);
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
        const trendIcon = getTrendIcon(data.momentum_direction);
        const trendText = getTrendText(data.momentum_direction, data.momentum_slope);
        const factors = Array.isArray(data.factors) ? data.factors : [];

        const factorHtml = factors.length
            ? factors.map((factor) => {
                const weightText = Number.isFinite(factor.weight) ? `Weight: ${(factor.weight * 100).toFixed(0)}%` : '';
                return `
                    <div class="factor-item">
                        <div class="factor-info">
                            <div class="factor-name">${escapeHtml(factor.name || 'Factor')}</div>
                            <div class="factor-desc">${escapeHtml(factor.description || '')}</div>
                        </div>
                        <div class="factor-value">
                            <div class="factor-value-main">${escapeHtml(formatFactorValue(factor))}</div>
                            <div class="factor-weight">${escapeHtml(weightText)}</div>
                        </div>
                    </div>
                `;
            }).join('')
            : '<div class="empty-state">No factor-level details available for this layer.</div>';

        const scoreText = Number.isFinite(data.score) ? Number(data.score).toFixed(3) : 'N/A';

        const layerName = data.display_name || formatLayerName(layerKey);
        dom.layerModalTitle.textContent = `${getLayerIcon(layerKey)} ${layerName}`;
        dom.layerModalDesc.textContent = data.description || '';
        dom.layerModalBody.innerHTML = `
            <div class="layer-score-main">
                <div>
                    <div style="font-size:11px; color:#5a6f84; margin-bottom:4px;">Overall Score</div>
                    <div class="layer-score-value">${scoreText}</div>
                </div>
                <div class="layer-score-trend">
                    <span class="layer-trend-icon ${escapeHtml(data.momentum_direction || '')}">${escapeHtml(trendIcon)}</span>
                    <span>${escapeHtml(trendText)}</span>
                </div>
            </div>

            <div class="layer-formula"><strong>Formula:</strong> ${escapeHtml(data.formula || 'N/A')}</div>

            <div class="layer-factors">
                <h4>Contributing Factors</h4>
                ${factorHtml}
            </div>

            <div class="layer-metadata">
                <span>Version: ${escapeHtml(data.version || 'N/A')}</span>
                <span>Data Year: ${escapeHtml(String(data.data_year || 'N/A'))}</span>
                <span>Coverage: ${escapeHtml(String(data.coverage_years || 'N/A'))} years</span>
            </div>
        `;
    } catch (error) {
        dom.layerModalTitle.textContent = `${getLayerIcon(layerKey)} ${formatLayerName(layerKey)}`;
        dom.layerModalDesc.textContent = 'Unable to load detailed layer breakdown.';
        dom.layerModalBody.innerHTML = `<div class="error-inline">${escapeHtml(getReadableFetchError(error, `/areas/${fipsCode}/layers/${layerKey}`))}</div>`;
    }
}

function formatFactorValue(factor) {
    if (factor && factor.formatted_value) {
        return factor.formatted_value;
    }
    const value = factor ? safeNumber(factor.value) : null;
    return value === null ? 'N/A' : value.toFixed(3);
}

function getTrendIcon(direction) {
    if (direction === 'up') {
        return '↑';
    }
    if (direction === 'down') {
        return '↓';
    }
    if (direction === 'stable') {
        return '→';
    }
    return '•';
}

function getTrendText(direction, slope) {
    const slopeNum = safeNumber(slope);
    if (direction === 'up') {
        return slopeNum === null ? 'Improving trend' : `Improving trend (${(slopeNum * 100).toFixed(1)}%)`;
    }
    if (direction === 'down') {
        return slopeNum === null ? 'Declining trend' : `Declining trend (${(slopeNum * 100).toFixed(1)}%)`;
    }
    if (direction === 'stable') {
        return 'Stable trend';
    }
    return 'Trend unavailable';
}

function renderComparePanel(options = {}) {
    appState.panelState = 'compare';
    dom.panel.dataset.state = 'compare';
    dom.panel.classList.add('detailed');
    dom.panel.classList.remove('chat-mode');
    hideFloatingAnalysisPanel();

    const counties = appState.compare.counties.slice(0, appState.compare.maxCount);
    setCompareButtonState(true, Boolean(counties.length || appState.selectedCounty));
    renderCompareBuilder();

    if (!counties.length) {
        dom.panelTitle.textContent = 'County Comparison';
        dom.panelSubtitle.textContent = 'Add counties from the Compare Builder to start.';
        dom.panelBody.innerHTML = '<div class="empty-state">Add at least two counties to compare. You can drag chips to reorder baseline vs comparators.</div>';
        return;
    }

    const anchorCounty = counties[0];
    if (counties.length === 1) {
        dom.panelTitle.textContent = `${anchorCounty.county_name} vs ...`;
        dom.panelSubtitle.textContent = `Data Year: ${anchorCounty.data_year || 'N/A'}`;
        dom.panelBody.innerHTML = `
            <div class="compare-shell">
                <div class="compare-header-row">
                    <span class="signal-chip" style="background:${anchorCounty.bivariate_color}; color:#15334f;">${escapeHtml(anchorCounty.bivariate_label)}</span>
                    <button class="exit-compare-btn" type="button" id="exit-compare-btn">Exit Compare</button>
                </div>
                <div class="empty-state">Baseline selected: <strong>${escapeHtml(anchorCounty.county_name)}</strong>. Add one or more comparator counties from Compare Builder.</div>
                ${options.error ? `<div class="error-inline">${escapeHtml(options.error)}</div>` : ''}
            </div>
        `;
        bindExitCompareButton();
        return;
    }

    const compareNames = counties.map((county) => county.county_name).join(' vs ');
    const dataYear = counties.map((county) => county.data_year).find(Boolean) || 'N/A';
    dom.panelTitle.textContent = compareNames;
    dom.panelSubtitle.textContent = `Data Year: ${dataYear}`;

    const countyCards = counties.map((county, index) => {
        const score = safeNumber(county.composite_score);
        const scoreText = score === null ? 'N/A' : score.toFixed(3);
        const anchorScore = safeNumber(anchorCounty.composite_score);
        const diff = index === 0 || score === null || anchorScore === null ? null : score - anchorScore;
        const diffLabel = index === 0 ? 'Baseline' : formatDiff(diff);
        const diffCls = index === 0 ? '' : diffClass(diff);
        const seriesColor = getCompareCountyColor(county, index);

        return `
            <article class="compare-card">
                <h5>County ${String.fromCharCode(65 + index)}</h5>
                <div class="county-name">${escapeHtml(county.county_name)}</div>
                <span class="signal-chip" style="background:${county.bivariate_color}; color:#16334f;">${escapeHtml(county.bivariate_label)}</span>
                <div class="compare-score">${scoreText}</div>
                <span class="compare-diff-pill ${diffCls}" style="box-shadow: inset 3px 0 0 ${seriesColor};">${escapeHtml(diffLabel)}</span>
            </article>
        `;
    }).join('');

    const tableHead = counties.map((county, index) => {
        const name = county.county_name || `County ${index + 1}`;
        return `<th title="${escapeHtml(name)}">${String.fromCharCode(65 + index)}</th>`;
    }).join('');

    const tableRows = LAYER_ROWS.map(([key, label]) => {
        const anchorValue = safeNumber(anchorCounty.layer_scores && anchorCounty.layer_scores[key]);
        const values = counties.map((county) => safeNumber(county.layer_scores && county.layer_scores[key]));
        const comparatorCells = values.map((value, index) => {
            if (index === 0) {
                return `<td>${value === null ? 'N/A' : value.toFixed(3)}</td>`;
            }
            const diff = value === null || anchorValue === null ? null : value - anchorValue;
            const diffText = diff === null ? '' : ` <span class="compare-cell-diff ${diffClass(diff)}">${formatDiff(diff)}</span>`;
            return `<td class="${diffClass(diff)}">${value === null ? 'N/A' : value.toFixed(3)}${diffText}</td>`;
        }).join('');

        return `
            <tr>
                <td>${renderLayerLabelWithIcon(key, label)}</td>
                ${comparatorCells}
            </tr>
        `;
    }).join('');

    const layerChartRows = LAYER_ROWS.map(([key, label]) => {
        const bars = counties.map((county, index) => {
            const value = safeNumber(county.layer_scores && county.layer_scores[key]);
            const width = value === null ? 0 : Math.round(Math.max(0, Math.min(1, value)) * 100);
            const color = getCompareCountyColor(county, index);
            const tag = String.fromCharCode(65 + index);
            const valueText = value === null ? 'N/A' : value.toFixed(2);
            return `
                <div class="compare-layer-bar-item" title="${escapeHtml(`${county.county_name}: ${valueText}`)}">
                    <span class="compare-layer-bar-tag">${tag}</span>
                    <div class="compare-layer-bar-rail">
                        <div class="compare-layer-bar" style="width:${width}%; background:${color};"></div>
                    </div>
                    <span class="compare-layer-bar-value">${valueText}</span>
                </div>
            `;
        }).join('');

        return `
            <div class="compare-layer-row">
                <div class="compare-layer-label">${escapeHtml(label)}</div>
                <div class="compare-layer-track">${bars}</div>
            </div>
        `;
    }).join('');

    const splitPair = chooseSplitComparisonPair(counties);
    const splitViewHtml = appState.compare.splitView && splitPair
        ? `
            <div class="compare-split">
                <h4>Split View</h4>
                <div class="compare-split-grid">
                    <article class="split-mini">
                        <div class="split-mini-head">
                            <div class="split-mini-title">${escapeHtml(splitPair.anchor.county_name)}</div>
                            <div class="split-mini-subtitle">Baseline county</div>
                        </div>
                        <div id="split-mini-map-left" class="split-mini-map"></div>
                    </article>
                    <article class="split-mini">
                        <div class="split-mini-head">
                            <div class="split-mini-title">${escapeHtml(splitPair.comparator.county_name)}</div>
                            <div class="split-mini-subtitle">Comparator ${escapeHtml(formatDiff(splitPair.diff))}</div>
                        </div>
                        <div id="split-mini-map-right" class="split-mini-map"></div>
                    </article>
                </div>
            </div>
        `
        : '';

    dom.panelBody.innerHTML = `
        <div class="compare-shell">
            <div class="compare-header-row">
                <div class="signal-chip" style="background:rgba(240,247,253,0.9); color:#224766;">County Comparison (${counties.length} counties)</div>
                <button class="exit-compare-btn" type="button" id="exit-compare-btn">Exit Compare</button>
            </div>

            ${options.error ? `<div class="error-inline">${escapeHtml(options.error)}</div>` : ''}

            <div class="compare-cards">
                ${countyCards}
            </div>

            <section class="compare-layer-chart">
                <h4>Layer Signal Bars</h4>
                <div class="compare-layer-bars">
                    ${layerChartRows}
                </div>
                <div class="compare-layer-chart-note">Bars show each county's normalized layer score (0 to 1). A is the baseline chip.</div>
            </section>

            ${splitViewHtml}

            <div class="compare-table-wrap">
                <table class="compare-table">
                    <thead>
                        <tr>
                            <th>Layer</th>
                            ${tableHead}
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
    if (appState.compare.splitView && splitPair) {
        syncSplitViewMaps(splitPair.anchor, splitPair.comparator, splitPair.diff);
    } else {
        destroySplitMaps();
    }
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

function buildProjectionModel(county) {
    const score = safeNumber(county.composite_score);
    const baseline = score === null ? 0.36 : score;
    const direction = normalizeTrajectory(county.directional_class);
    const confidence = normalizeConfidenceClass(county.confidence_class);
    const confidenceMultiplier = {
        strong: 1,
        conditional: 0.72,
        fragile: 0.46,
        unknown: 0.58
    }[confidence] || 0.58;

    const directionalVelocity = {
        improving: 0.045,
        stable: 0.008,
        at_risk: -0.038
    }[direction] || 0;

    const oneYear = clamp01(baseline + directionalVelocity * confidenceMultiplier);
    const threeYear = clamp01(baseline + directionalVelocity * 3 * confidenceMultiplier);
    const oneYearClass = classifyProjectionSignal(oneYear);
    const threeYearClass = classifyProjectionSignal(threeYear);

    const summary = direction === 'improving'
        ? 'Current direction suggests upside if strengths persist and pressure points remain controlled.'
        : direction === 'at_risk'
            ? 'Signals suggest continued headwinds unless risk factors are reduced through targeted interventions.'
            : 'Signals suggest relative stability with incremental movement unless major conditions shift.';

    const confidenceLabel = CONFIDENCE_LABELS[confidence] || 'Unknown';
    const notes = [
        `Confidence level: ${confidenceLabel}.`,
        'Projection is directional and based on current layer signal pattern, not a deterministic forecast.',
        'Use with local context, policy updates, and recent on-the-ground changes before making decisions.'
    ];

    return {
        summary,
        oneYearScore: oneYear.toFixed(3),
        threeYearScore: threeYear.toFixed(3),
        oneYearClass,
        threeYearClass,
        oneYearLabel: projectionLabelFromClass(oneYearClass),
        threeYearLabel: projectionLabelFromClass(threeYearClass),
        notes
    };
}

function classifyProjectionSignal(score) {
    if (score >= 0.5) {
        return 'improving';
    }
    if (score >= 0.32) {
        return 'stable';
    }
    return 'at_risk';
}

function projectionLabelFromClass(signalClass) {
    if (signalClass === 'improving') {
        return 'Growth-supportive signal range';
    }
    if (signalClass === 'stable') {
        return 'Balanced but mixed signal range';
    }
    return 'Constrained signal range';
}

function clamp01(value) {
    return Math.max(0, Math.min(1, value));
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

function renderStoryScoreBar(value) {
    if (value === null || !Number.isFinite(value)) {
        return '';
    }

    const bounded = Math.max(0, Math.min(1, value));
    const width = `${(bounded * 100).toFixed(0)}%`;
    return `<div class="story-score-bar" aria-hidden="true"><div class="story-score-bar-fill" style="width:${width};"></div></div>`;
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

function setCompareBuilderExpanded(expanded) {
    if (!dom.compareBuilderCard || !dom.compareBuilderToggle || !dom.compareBuilder) {
        return;
    }

    const isExpanded = Boolean(expanded);
    appState.compare.builderExpanded = isExpanded;

    dom.compareBuilderCard.classList.toggle('expanded', isExpanded);
    dom.compareBuilderCard.classList.toggle('collapsed', !isExpanded);
    dom.compareBuilderToggle.setAttribute('aria-expanded', isExpanded ? 'true' : 'false');
    dom.compareBuilder.hidden = !isExpanded;

    if (!isExpanded) {
        closeCompareAutocomplete();
    }
}

function setupCompareBuilder() {
    if (
        !dom.compareBuilderCard ||
        !dom.compareBuilderToggle ||
        !dom.compareBuilder ||
        !dom.compareBuilderMeta ||
        !dom.compareSearchInput ||
        !dom.compareAutocomplete ||
        !dom.compareChipList ||
        !dom.splitViewToggle
    ) {
        return;
    }

    setCompareBuilderExpanded(appState.compare.builderExpanded);

    dom.compareBuilderToggle.addEventListener('click', () => {
        setCompareBuilderExpanded(!appState.compare.builderExpanded);
    });

    dom.compareSearchInput.addEventListener('focus', () => {
        updateCompareAutocomplete(dom.compareSearchInput.value);
    });

    dom.compareSearchInput.addEventListener('input', () => {
        updateCompareAutocomplete(dom.compareSearchInput.value);
    });

    dom.compareSearchInput.addEventListener('keydown', async (event) => {
        if (event.key === 'Escape') {
            closeCompareAutocomplete();
            dom.compareSearchInput.blur();
            return;
        }

        if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
            event.preventDefault();
            if (!appState.compareAutocomplete.matches.length) {
                updateCompareAutocomplete(dom.compareSearchInput.value);
                return;
            }

            const direction = event.key === 'ArrowDown' ? 1 : -1;
            const length = appState.compareAutocomplete.matches.length;
            const current = appState.compareAutocomplete.activeIndex;
            appState.compareAutocomplete.activeIndex = (current + direction + length) % length;
            renderCompareAutocompleteList();
            return;
        }

        if (event.key !== 'Enter') {
            return;
        }

        event.preventDefault();
        const matches = appState.compareAutocomplete.matches;
        if (!matches.length) {
            await addCountyFromCompareQuery(dom.compareSearchInput.value);
            closeCompareAutocomplete();
            return;
        }

        const activeIndex = appState.compareAutocomplete.activeIndex;
        const target = matches[activeIndex] || matches[0];
        if (!target) {
            return;
        }

        await addCountyToCompareByFips(target.fips_code);
        dom.compareSearchInput.value = '';
        closeCompareAutocomplete();
    });

    dom.compareAutocomplete.addEventListener('mousedown', async (event) => {
        event.preventDefault();
        const button = event.target.closest('button[data-fips-code]');
        if (!button) {
            return;
        }

        const fipsCode = button.dataset.fipsCode;
        if (!fipsCode) {
            return;
        }

        await addCountyToCompareByFips(fipsCode);
        dom.compareSearchInput.value = '';
        closeCompareAutocomplete();
    });

    dom.compareChipList.addEventListener('click', async (event) => {
        const removeButton = event.target.closest('button[data-remove-fips]');
        if (removeButton) {
            const fipsCode = removeButton.dataset.removeFips;
            if (fipsCode) {
                removeCompareCounty(fipsCode);
            }
            return;
        }

        const chip = event.target.closest('[data-fips-code]');
        if (!chip) {
            return;
        }

        const fipsCode = chip.dataset.fipsCode;
        const feature = appState.countyFeaturesByFips.get(fipsCode || '');
        if (!fipsCode || !feature || !feature.properties) {
            return;
        }

        await handleCountySelection(fipsCode, feature.properties);
    });

    dom.compareChipList.addEventListener('dragstart', (event) => {
        const chip = event.target.closest('[data-fips-code]');
        if (!chip) {
            return;
        }
        const fipsCode = chip.dataset.fipsCode;
        if (!fipsCode) {
            return;
        }

        appState.compare.dragFips = fipsCode;
        chip.classList.add('dragging');
        if (event.dataTransfer) {
            event.dataTransfer.effectAllowed = 'move';
            event.dataTransfer.setData('text/plain', fipsCode);
        }
    });

    dom.compareChipList.addEventListener('dragover', (event) => {
        if (!appState.compare.dragFips) {
            return;
        }
        const targetChip = event.target.closest('[data-fips-code]');
        if (!targetChip) {
            return;
        }
        event.preventDefault();
        clearCompareDropTargets();
        if (targetChip.dataset.fipsCode !== appState.compare.dragFips) {
            targetChip.classList.add('drop-target');
        }
    });

    dom.compareChipList.addEventListener('drop', (event) => {
        if (!appState.compare.dragFips) {
            return;
        }
        event.preventDefault();
        const targetChip = event.target.closest('[data-fips-code]');
        if (!targetChip) {
            clearCompareDropTargets();
            return;
        }
        const targetFips = targetChip.dataset.fipsCode;
        if (targetFips) {
            reorderCompareCounties(appState.compare.dragFips, targetFips);
        }
        clearCompareDropTargets();
    });

    dom.compareChipList.addEventListener('dragend', () => {
        appState.compare.dragFips = null;
        clearCompareDropTargets();
        const dragging = dom.compareChipList.querySelector('.compare-chip.dragging');
        if (dragging) {
            dragging.classList.remove('dragging');
        }
    });

    dom.splitViewToggle.addEventListener('click', () => {
        if (appState.compare.counties.length < 2) {
            renderTransientPanelError('Add at least two counties before enabling split view.');
            return;
        }
        if (!appState.compare.active) {
            appState.compare.active = true;
        }
        appState.compare.splitView = !appState.compare.splitView;
        renderCompareBuilder();
        if (appState.compare.active) {
            renderComparePanel();
        }
    });

    renderCompareBuilder();
}

function clearCompareDropTargets() {
    if (!dom.compareChipList) {
        return;
    }
    dom.compareChipList.querySelectorAll('.compare-chip').forEach((chip) => {
        chip.classList.remove('drop-target');
        chip.classList.remove('dragging');
    });
}

function renderCompareBuilder() {
    if (!dom.compareBuilderMeta || !dom.compareChipList || !dom.splitViewToggle) {
        return;
    }

    const counties = appState.compare.counties.slice(0, appState.compare.maxCount);
    const anchorScore = counties.length ? safeNumber(counties[0].composite_score) : null;

    dom.compareBuilderMeta.textContent = `${counties.length} of ${appState.compare.maxCount}`;
    dom.compareChipList.classList.toggle('empty', counties.length === 0);

    dom.compareChipList.innerHTML = counties.map((county, index) => {
        const score = safeNumber(county.composite_score);
        const diff = index === 0 || score === null || anchorScore === null ? null : score - anchorScore;
        const diffLabel = index === 0 ? 'Base' : formatDiff(diff);
        const diffCls = index === 0 ? '' : diffClass(diff);
        const color = getCompareCountyColor(county, index);
        return `
            <div class="compare-chip" draggable="true" data-fips-code="${escapeHtml(county.fips_code)}" style="box-shadow: inset 3px 0 0 ${color};">
                <span class="compare-chip-rank">${index + 1}</span>
                <span class="compare-chip-name">${escapeHtml(county.county_name)}</span>
                <span class="compare-chip-diff ${diffCls}">${escapeHtml(diffLabel)}</span>
                <button class="compare-chip-remove" type="button" data-remove-fips="${escapeHtml(county.fips_code)}" aria-label="Remove ${escapeHtml(county.county_name)}">×</button>
            </div>
        `;
    }).join('');

    const splitReady = appState.compare.counties.length >= 2 && appState.compare.active;
    if (!splitReady && appState.compare.splitView) {
        appState.compare.splitView = false;
        destroySplitMaps();
    }

    dom.splitViewToggle.disabled = !splitReady;
    dom.splitViewToggle.classList.toggle('active', splitReady && appState.compare.splitView);
    dom.splitViewToggle.setAttribute('aria-pressed', splitReady && appState.compare.splitView ? 'true' : 'false');
}

function updateCompareAutocomplete(rawQuery) {
    if (!dom.compareAutocomplete || !dom.compareSearchInput) {
        return;
    }

    const query = String(rawQuery || '').trim().toLowerCase();
    const selectedFips = new Set(appState.compare.counties.map((county) => county.fips_code));

    const matches = appState.countySearchIndex
        .filter((county) => !selectedFips.has(county.fips_code))
        .filter((county) => {
            if (!query) {
                return true;
            }
            return county.county_name.toLowerCase().includes(query);
        })
        .sort((a, b) => {
            const aName = a.county_name.toLowerCase();
            const bName = b.county_name.toLowerCase();
            const aStarts = query ? aName.startsWith(query) : false;
            const bStarts = query ? bName.startsWith(query) : false;
            if (aStarts !== bStarts) {
                return aStarts ? -1 : 1;
            }
            return aName.localeCompare(bName);
        })
        .slice(0, 8);

    appState.compareAutocomplete.matches = matches;
    appState.compareAutocomplete.activeIndex = matches.length ? 0 : -1;
    renderCompareAutocompleteList();
}

function renderCompareAutocompleteList() {
    if (!dom.compareAutocomplete) {
        return;
    }

    const matches = appState.compareAutocomplete.matches;
    if (!matches.length) {
        dom.compareAutocomplete.classList.remove('open');
        dom.compareAutocomplete.innerHTML = '';
        return;
    }

    dom.compareAutocomplete.classList.add('open');
    dom.compareAutocomplete.innerHTML = matches.map((county, index) => `
        <button
            class="compare-suggestion ${index === appState.compareAutocomplete.activeIndex ? 'active' : ''}"
            type="button"
            role="option"
            data-fips-code="${escapeHtml(county.fips_code)}"
            aria-selected="${index === appState.compareAutocomplete.activeIndex ? 'true' : 'false'}"
        >
            ${escapeHtml(county.county_name)}
        </button>
    `).join('');
}

function closeCompareAutocomplete() {
    appState.compareAutocomplete.matches = [];
    appState.compareAutocomplete.activeIndex = -1;
    if (!dom.compareAutocomplete) {
        return;
    }
    dom.compareAutocomplete.classList.remove('open');
    dom.compareAutocomplete.innerHTML = '';
}

async function addCountyFromCompareQuery(query) {
    const normalized = String(query || '').trim().toLowerCase();
    if (!normalized) {
        return;
    }

    const exact = appState.countySearchIndex.find((county) => county.county_name.toLowerCase() === normalized);
    if (exact) {
        await addCountyToCompareByFips(exact.fips_code);
        dom.compareSearchInput.value = '';
        return;
    }

    const fallback = appState.countySearchIndex.find((county) => county.county_name.toLowerCase().includes(normalized));
    if (!fallback) {
        renderTransientPanelError(`No county matched "${query}".`);
        return;
    }

    await addCountyToCompareByFips(fallback.fips_code);
    dom.compareSearchInput.value = '';
}

async function addCountyToCompareByFips(fipsCode) {
    const feature = appState.countyFeaturesByFips.get(fipsCode || '');
    if (!feature || !feature.properties) {
        return;
    }

    if (!appState.compare.active) {
        appState.compare.active = true;
        appState.compare.counties = [];
        if (appState.selectedCounty && appState.selectedCounty.fips_code !== fipsCode) {
            upsertCompareCounty(appState.selectedCounty);
        }
    }
    setCompareBuilderExpanded(true);

    await handleCountySelection(fipsCode, feature.properties);
}

function upsertCompareCounty(county) {
    if (!county || !county.fips_code) {
        return { index: -1, added: false, limitReached: false };
    }

    const existingIndex = appState.compare.counties.findIndex((item) => item.fips_code === county.fips_code);
    if (existingIndex !== -1) {
        appState.compare.counties[existingIndex] = county;
        return { index: existingIndex, added: false, limitReached: false };
    }

    if (appState.compare.counties.length >= appState.compare.maxCount) {
        return { index: -1, added: false, limitReached: true };
    }

    appState.compare.counties.push(county);
    return {
        index: appState.compare.counties.length - 1,
        added: true,
        limitReached: false
    };
}

function removeCompareCounty(fipsCode) {
    const index = appState.compare.counties.findIndex((county) => county.fips_code === fipsCode);
    if (index === -1) {
        return;
    }

    appState.compare.counties.splice(index, 1);
    if (appState.selectedCounty && appState.selectedCounty.fips_code === fipsCode) {
        appState.selectedCounty = appState.compare.counties[0] || null;
    }

    renderCompareBuilder();
    renderCountyTableView();

    if (!appState.compare.counties.length) {
        appState.compare.active = false;
        appState.compare.splitView = false;
        destroySplitMaps();
        if (appState.selectedCounty) {
            renderStoryPanel(appState.selectedCounty);
        } else {
            clearSelection();
        }
        return;
    }

    if (appState.compare.active) {
        renderComparePanel();
    }
}

function reorderCompareCounties(dragFips, targetFips) {
    if (!dragFips || !targetFips || dragFips === targetFips) {
        return;
    }

    const counties = appState.compare.counties.slice();
    const fromIndex = counties.findIndex((county) => county.fips_code === dragFips);
    const toIndex = counties.findIndex((county) => county.fips_code === targetFips);
    if (fromIndex === -1 || toIndex === -1) {
        return;
    }

    const [moved] = counties.splice(fromIndex, 1);
    counties.splice(toIndex, 0, moved);
    appState.compare.counties = counties;

    renderCompareBuilder();
    if (appState.compare.active) {
        renderComparePanel();
    }
}

function chooseSplitComparisonPair(counties) {
    if (!Array.isArray(counties) || counties.length < 2) {
        return null;
    }

    const anchor = counties[0];
    const anchorScore = safeNumber(anchor.composite_score);
    let best = null;

    counties.slice(1).forEach((county) => {
        const score = safeNumber(county.composite_score);
        const diff = score === null || anchorScore === null ? null : score - anchorScore;
        const magnitude = diff === null ? -1 : Math.abs(diff);
        if (!best || magnitude > best.magnitude) {
            best = {
                comparator: county,
                diff,
                magnitude
            };
        }
    });

    if (!best) {
        return null;
    }

    return {
        anchor,
        comparator: best.comparator,
        diff: best.diff
    };
}

function syncSplitViewMaps(anchorCounty, comparatorCounty, diffValue) {
    destroySplitMaps();

    const leftContainer = document.getElementById('split-mini-map-left');
    const rightContainer = document.getElementById('split-mini-map-right');
    if (!leftContainer || !rightContainer || !appState.geojson) {
        return;
    }

    const leftMap = createSplitMiniMap(leftContainer, anchorCounty, 0);
    const rightMap = createSplitMiniMap(rightContainer, comparatorCounty, 1);
    appState.compare.splitMaps.left = leftMap;
    appState.compare.splitMaps.right = rightMap;

    if (diffValue !== null) {
        const readable = formatDiff(diffValue);
        leftContainer.setAttribute('title', `${anchorCounty.county_name} baseline`);
        rightContainer.setAttribute('title', `${comparatorCounty.county_name} diff ${readable} vs baseline`);
    }
}

function createSplitMiniMap(container, county, colorIndex) {
    const map = new mapboxgl.Map({
        container,
        style: MAPBOX_STYLE_URL,
        center: appState.countyCentersByFips.get(county.fips_code) || [-76.9, 39.02],
        zoom: 7.3,
        minZoom: 6,
        maxZoom: 11,
        interactive: false,
        attributionControl: false
    });

    map.on('load', () => {
        map.addSource('split-counties', {
            type: 'geojson',
            data: appState.geojson
        });

        map.addLayer({
            id: 'split-counties-fill',
            type: 'fill',
            source: 'split-counties',
            paint: {
                'fill-color': 'rgba(168, 188, 205, 0.32)',
                'fill-opacity': 0.38
            }
        });

        map.addLayer({
            id: 'split-counties-line',
            type: 'line',
            source: 'split-counties',
            paint: {
                'line-color': 'rgba(54, 84, 109, 0.52)',
                'line-width': 0.9,
                'line-opacity': 0.65
            }
        });

        map.addLayer({
            id: 'split-focus-fill',
            type: 'fill',
            source: 'split-counties',
            filter: ['==', 'fips_code', county.fips_code],
            paint: {
                'fill-color': getCompareCountyColor(county, colorIndex),
                'fill-opacity': 0.72
            }
        });

        map.addLayer({
            id: 'split-focus-line',
            type: 'line',
            source: 'split-counties',
            filter: ['==', 'fips_code', county.fips_code],
            paint: {
                'line-color': '#0f4f93',
                'line-width': 2.2,
                'line-opacity': 0.95
            }
        });

        const bounds = appState.countyBoundsByFips.get(county.fips_code);
        if (bounds) {
            map.fitBounds(
                [
                    [bounds.minX, bounds.minY],
                    [bounds.maxX, bounds.maxY]
                ],
                {
                    padding: 20,
                    duration: 0,
                    maxZoom: 9.6
                }
            );
        }

        window.setTimeout(() => {
            map.resize();
        }, 24);
    });

    return map;
}

function destroySplitMaps() {
    ['left', 'right'].forEach((slot) => {
        const map = appState.compare.splitMaps[slot];
        if (map && typeof map.remove === 'function') {
            map.remove();
        }
        appState.compare.splitMaps[slot] = null;
    });
}

function getCompareCountyColor(county, index = 0) {
    if (county && county.bivariate_key) {
        return getBivariateColor(county.bivariate_key, 'map');
    }
    return COMPARE_SERIES_COLORS[index % COMPARE_SERIES_COLORS.length];
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
            appState.compare.counties = [];
            upsertCompareCounty(appState.selectedCounty);
            setCompareBuilderExpanded(true);
            renderCompareBuilder();
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
        .map((feature) => {
            const props = feature.properties || {};
            return {
                fips_code: props.fips_code || props.geoid || '',
                county_name: props.county_name || ''
            };
        })
        .filter((county) => county.fips_code && county.county_name)
        .sort((a, b) => a.county_name.localeCompare(b.county_name));

    appState.countySearchIndex = counties;

    dom.searchList.innerHTML = counties
        .map((county) => `<option value="${escapeHtml(county.county_name)}"></option>`)
        .join('');

    renderCompareBuilder();
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
    appState.compare.counties = [];
    appState.compare.splitView = false;
    destroySplitMaps();
    setCompareBuilderExpanded(false);
    closeCompareAutocomplete();
    if (dom.compareSearchInput) {
        dom.compareSearchInput.value = '';
    }

    if (appState.map && appState.map.getLayer('counties-selected')) {
        appState.map.setFilter('counties-selected', ['==', 'fips_code', '']);
    }

    setCompareButtonState(false, false);
    renderCompareBuilder();
    dom.panel.dataset.state = 'empty';
    dom.panel.classList.remove('detailed');
    dom.panel.classList.remove('chat-mode');
    dom.panelTitle.textContent = 'County Selected';
    dom.panelSubtitle.textContent = 'No county selected yet';
    dom.panelBody.innerHTML = `
        <div class="empty-state">
            Click a county on the map to open story mode. Use Compare Builder to add up to four counties and view layer-by-layer score differences.
        </div>
    `;
    hideFloatingAnalysisPanel();
    renderCountyTableView();
}

function highlightSelectedCounty(fipsCode) {
    if (appState.map && appState.map.getLayer('counties-selected')) {
        appState.map.setFilter('counties-selected', ['==', 'fips_code', fipsCode || '']);
    }
}

function disableCompareMode() {
    appState.compare.active = false;
    appState.compare.counties = [];
    appState.compare.splitView = false;
    destroySplitMaps();
    setCompareBuilderExpanded(false);
    closeCompareAutocomplete();
    if (dom.compareSearchInput) {
        dom.compareSearchInput.value = '';
    }
    renderCompareBuilder();

    if (appState.selectedCounty) {
        renderStoryPanel(appState.selectedCounty);
    } else {
        clearSelection();
    }
}

function setupAskAtlasPill() {
    if (!dom.askPill || !dom.askClose || !dom.askSend || !dom.askInput) {
        return;
    }
    if (!isChatEnabled()) {
        applyCapabilitiesUI();
        return;
    }

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
    if (!isChatEnabled()) {
        return;
    }
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
    if (!isChatEnabled()) {
        return;
    }
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
    if (!isChatEnabled()) {
        return;
    }
    enterChatMode();
    await sendChatMessage(question);
}

function enterChatMode() {
    if (!isChatEnabled()) {
        return;
    }
    appState.chat.open = true;
    if (dom.analysisFloat) {
        dom.analysisFloat.classList.remove('hidden');
        dom.analysisFloat.classList.add('chat-mode');
        if (!window.matchMedia('(max-width: 1040px)').matches && dom.analysisFloat.dataset.positioned !== 'true') {
            dom.analysisFloat.style.left = '12px';
            dom.analysisFloat.style.top = '12px';
            dom.analysisFloat.style.right = 'auto';
            dom.analysisFloat.style.bottom = 'auto';
            dom.analysisFloat.dataset.positioned = 'true';
        }
    }
    renderChatPanel();
}

function exitChatMode() {
    if (!appState.chat.open) {
        return;
    }

    appState.chat.open = false;
    if (dom.analysisFloat) {
        dom.analysisFloat.classList.remove('chat-mode');
    }

    if (appState.selectedCounty) {
        showFloatingAnalysisPanel(appState.selectedCounty);
    } else {
        hideFloatingAnalysisPanel();
    }
}

function renderChatPanel() {
    if (!dom.analysisFloat || !dom.analysisFloatTitle || !dom.analysisFloatContent) {
        return;
    }

    dom.analysisFloatTitle.textContent = 'Ask Atlas';
    dom.analysisFloat.classList.add('chat-mode');

    const contextText = appState.selectedCounty
        ? `${appState.selectedCounty.county_name} context enabled`
        : 'General Atlas mode · select a county for place-specific answers';

    const messagesHtml = appState.chat.messages.length
        ? appState.chat.messages.map((message) => {
            const roleClass = message.role === 'user' ? 'user' : message.role === 'thinking' ? 'thinking' : 'assistant';
            return `<div class="chat-msg ${roleClass}">${escapeHtml(message.content)}</div>`;
        }).join('')
        : '<div class="chat-msg assistant">Hi, I\'m Atlas. Ask me anything about Maryland county signals and what they mean for families and housing.</div>';

    dom.analysisFloatContent.innerHTML = `
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
    if (!isChatEnabled()) {
        return;
    }
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
        confidence_class: county.confidence_class,
        synthesis_grouping: county.synthesis_grouping,
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
        if (appState.tour.active) {
            if (event.key === 'Escape') {
                stopGuidedTour({ dismiss: true });
            }
            return;
        }

        if (event.key !== 'Escape') {
            return;
        }

        if (dom.layerModal && dom.layerModal.classList.contains('open')) {
            closeLayerModal();
            return;
        }

        if (dom.askShell.classList.contains('expanded')) {
            closeAskInput();
            return;
        }

        if (appState.tableViewOpen) {
            closeCountyTableView();
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
        if (appState.tour.active) {
            return;
        }

        const target = event.target;

        if (
            dom.compareAutocomplete &&
            dom.compareAutocomplete.classList.contains('open') &&
            dom.compareBuilder &&
            !dom.compareBuilder.contains(target)
        ) {
            closeCompareAutocomplete();
        }

        if (dom.askShell.classList.contains('expanded')) {
            const insideAsk = dom.askShell.contains(target);
            if (!insideAsk) {
                closeAskInput();
            }
        }

        if (appState.tableViewOpen) {
            const insideTable =
                (dom.countyTablePanel && dom.countyTablePanel.contains(target)) ||
                (dom.mapTableToggle && dom.mapTableToggle.contains(target));
            if (!insideTable) {
                closeCountyTableView();
            }
        }

        if (!appState.chat.open) {
            return;
        }

        const insideAnalysis = dom.analysisFloat && dom.analysisFloat.contains(target);
        const insideAsk = dom.askShell.contains(target) || dom.askPill.contains(target);

        if (!insideAnalysis && !insideAsk) {
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

function getLayerIcon(layerKey) {
    return LAYER_ICONS[layerKey] || '•';
}

function getLayerExplanation(layerKey) {
    return LAYER_EXPLANATIONS[layerKey] || 'Layer score and factors for this county.';
}

function renderLayerLabelWithIcon(layerKey, label) {
    const explanation = getLayerExplanation(layerKey);
    return `
        <span class="layer-label-with-icon">
            <span class="layer-label-icon" title="${escapeHtml(explanation)}" aria-label="${escapeHtml(explanation)}">${escapeHtml(getLayerIcon(layerKey))}</span>
            <span class="layer-label-text">${escapeHtml(label)}</span>
        </span>
    `;
}

function formatLayerName(layerKey) {
    const match = LAYER_ROWS.find(([key]) => key === layerKey);
    return match ? match[1] : layerKey;
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

function wait(ms) {
    return new Promise((resolve) => {
        window.setTimeout(resolve, ms);
    });
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
