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
const COUNTY_GEOJSON_FALLBACK_STATIC_PATHS = [
    './md_counties_latest.geojson',
    'md_counties_latest.geojson',
    '/md_counties_latest.geojson',
    '/frontend/md_counties_latest.geojson'
];

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
    school_trajectory: 'School Trajectory: Direction and consistency of school system outcomes over time.',
    housing_elasticity: 'Housing Elasticity: How responsive local housing supply is to demand and price pressure.',
    demographic_momentum: 'Demographic Momentum: Population and household trends that support long-term growth.',
    risk_drag: 'Risk Drag: Factors slowing growth like high costs or structural barriers.'
};

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
        body: 'Open the Scores tab and hover a layer icon to see plain-language explanations for each signal.',
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
    clickOpacityTimer: null,
    hoverPopupFips: null,
    apiBaseUrl: null,
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
    tourSkip: document.getElementById('tour-skip')
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
    setupCountyTableView();
    setupGuidedTour();
    setupAskAtlasPill();
    setupAnalysisFloat();
    setupLayerModal();
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
    appState.compare.countyA = null;
    appState.compare.countyB = null;
    setCompareButtonState(false, true);

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

        <div class="story-tabs" role="tablist" aria-label="County panel tabs">
            <button class="story-tab active" type="button" data-story-tab="summary" aria-selected="true">Summary</button>
            <button class="story-tab" type="button" data-story-tab="scores" aria-selected="false">Scores</button>
        </div>

        <div class="story-tab-content active" data-story-tab-content="summary">
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
                <h4>Analysis Detail</h4>
                <p>Detailed strengths, weaknesses, and trend notes are shown in the floating analysis panel on the map.</p>
            </section>

            <section class="story-section">
                <h4>Ask Atlas</h4>
                <div class="quick-prompts">
                    ${QUICK_PROMPTS.map((prompt, idx) => `<button class="quick-prompt" type="button" data-quick-prompt="${idx}">${escapeHtml(shortPromptLabel(prompt))}</button>`).join('')}
                </div>
            </section>
        </div>

        <div class="story-tab-content" data-story-tab-content="scores">
            <section class="story-section">
                <h4>Layer Scores</h4>
                <div class="story-score-grid">
                    ${layerScoresHtml}
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
                <td>${renderLayerLabelWithIcon(key, label)}</td>
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
    renderCountyTableView();
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
