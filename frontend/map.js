// Maryland Growth & Family Viability Atlas - Map Interface
// Uses Mapbox GL JS to visualize synthesis groupings

// Configuration
const MAPBOX_TOKEN = 'pk.eyJ1IjoiZWxrYXJpMjMiLCJhIjoiY2tubm04b3BkMTYwcTJzcG5tZDZ2YTV5MSJ9.S0oAvquhkkMoDGrRJ_oP-Q';
const API_BASE_URL = `${window.location.protocol}//${window.location.hostname}:8000/api/v1`;
const GEOJSON_PATHS = [
    `${API_BASE_URL}/layers/counties/latest`,
    './md_counties_latest.geojson',
    '/md_counties_latest.geojson'
];

// Currently selected county FIPS (for layer detail lookups)
let currentFipsCode = null;
let currentCountySummary = null;

// Color schemes for different layers
const SYNTHESIS_COLORS = {
    'emerging_tailwinds': '#2d5016',
    'conditional_growth': '#7cb342',
    'stable_constrained': '#fdd835',
    'at_risk_headwinds': '#f4511e',
    'high_uncertainty': '#757575'
};

const DIRECTIONAL_COLORS = {
    'improving': '#4caf50',
    'stable': '#ffc107',
    'at_risk': '#f44336'
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
    style: 'mapbox://styles/mapbox/streets-v12',
    center: [-77.0, 39.0], // Center on Maryland
    zoom: 7,
    minZoom: 6,
    maxZoom: 16
});

const mapControlsHost = document.getElementById('map-controls');
const mapContainer = map.getContainer();
const controlContainer = mapContainer.querySelector('.mapboxgl-control-container');
if (mapControlsHost && controlContainer && !mapControlsHost.contains(controlContainer)) {
    mapControlsHost.appendChild(controlContainer);
}

const colorReveal = document.getElementById('color-reveal');
let lastPointer = null;
const REVEAL_START_ZOOM = 7.2;
const REVEAL_END_ZOOM = 11.4;
const PITCH_START_ZOOM = 9.5;
const PITCH_END_ZOOM = 13.2;
const MAX_PITCH = 45;
const PULSE_RADIUS_MILES = 5;
const URBAN_PULSE_ENABLED = false;

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

function updateColorReveal() {
    if (!colorReveal) {
        return;
    }
    const zoom = map.getZoom();
    const strength = Math.max(0, Math.min(1, (zoom - REVEAL_START_ZOOM) / (REVEAL_END_ZOOM - REVEAL_START_ZOOM)));
    const rect = map.getContainer().getBoundingClientRect();
    const radius = Math.hypot(rect.width, rect.height) * strength * 0.95;
    const point = lastPointer || { x: rect.width / 2, y: rect.height / 2 };
    const xPct = Math.max(0, Math.min(100, (point.x / rect.width) * 100));
    const yPct = Math.max(0, Math.min(100, (point.y / rect.height) * 100));

    colorReveal.style.setProperty('--reveal-x', `${xPct}%`);
    colorReveal.style.setProperty('--reveal-y', `${yPct}%`);
    colorReveal.style.setProperty('--reveal-radius', `${radius}px`);
    colorReveal.style.setProperty('--reveal-strength', strength.toFixed(3));
}

function updatePitch() {
    const zoom = map.getZoom();
    const t = Math.max(0, Math.min(1, (zoom - PITCH_START_ZOOM) / (PITCH_END_ZOOM - PITCH_START_ZOOM)));
    const pitch = t * MAX_PITCH;
    if (Math.abs(map.getPitch() - pitch) > 0.5) {
        map.setPitch(pitch);
    }
}

function setPanelOpenState(isOpen) {
    document.body.classList.toggle('panel-open', isOpen);
    const backdrop = document.getElementById('sidebar-backdrop');
    if (backdrop) {
        backdrop.classList.toggle('active', isOpen);
    }

    const panel = document.getElementById('side-panel');
    if (panel) {
        panel.style.willChange = 'transform';
        panel.addEventListener('transitionend', () => {
            panel.style.willChange = 'auto';
        }, { once: true });
    }

    const panelWidthValue = getComputedStyle(document.documentElement).getPropertyValue('--panel-width');
    const panelWidth = Number.parseFloat(panelWidthValue) || 400;
    const isMobile = window.matchMedia('(max-width: 768px)').matches;
    const paddingRight = isOpen && !isMobile ? panelWidth : 0;

    if (map && typeof map.easeTo === 'function') {
        map.easeTo({
            padding: { left: 0, right: paddingRight, top: 0, bottom: 0 },
            duration: 350,
            easing(t) {
                return t < 0.5 ? 2 * t * t : -1 + (4 - 2 * t) * t;
            }
        });
    } else if (map && typeof map.resize === 'function') {
        map.resize();
    }
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

// Popup for hover tooltips
const popup = new mapboxgl.Popup({
    closeButton: false,
    closeOnClick: false,
    offset: 12,
    maxWidth: '240px'
});

// Debounce helper for hover performance
let hoverDebounceTimer = null;
function debounceHover(callback, delay = 16) {
    return function(...args) {
        if (hoverDebounceTimer) cancelAnimationFrame(hoverDebounceTimer);
        hoverDebounceTimer = requestAnimationFrame(() => callback.apply(this, args));
    };
}

// Current layer selection
let currentLayer = 'synthesis';
let activeLegendFilter = null;
const DEFAULT_FILL_OPACITY = 0.7;
const DIMMED_FILL_OPACITY = 0.18;

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
    try {
        updateLoadingStatus('Fetching county data...');

        add3DBuildings();
        updateColorReveal();

        map.on('mousemove', (e) => {
            lastPointer = e.point;
        });

        map.on('touchmove', (e) => {
            if (e.points && e.points[0]) {
                lastPointer = e.points[0];
            }
        });

        map.on('zoom', () => {
            updateColorReveal();
            updatePitch();
        });
        map.on('move', updateColorReveal);
        map.on('resize', updateColorReveal);

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
                'line-width': 1,
                'line-opacity': 0.5
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

        // Set up interactivity
        setupInteractivity();
        setupLegendFiltering();

        // Add Urban Pulse layers above county fills (disabled by default)
        if (URBAN_PULSE_ENABLED) {
            addUrbanPulseLayers();
        }

        // Hide loading screen
        document.getElementById('loading').style.display = 'none';

    } catch (error) {
        console.error('Error loading map data:', error);
        document.getElementById('loading').innerHTML = `
            <div class="spinner"></div>
            <p>Error loading map data</p>
            <p style="font-size: 12px; color: #666; margin-top: 10px;">
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
            <p style="font-size: 14px; color: #666; line-height: 1.5;">
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
    setPanelOpenState(true);
    document.getElementById('side-panel').classList.add('open');
    document.getElementById('clear-selection').classList.add('visible');
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
        duration: 700
    });
}


function add3DBuildings() {
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

            // Get current layer label
            const label = currentLayer === 'synthesis'
                ? SYNTHESIS_LABELS[props.synthesis_grouping]
                : currentLayer === 'directional'
                ? DIRECTIONAL_LABELS[props.directional_class]
                : CONFIDENCE_LABELS[props.confidence_class];

            // Get grouping class for color styling
            const groupingClass = props.synthesis_grouping || 'high_uncertainty';

            // Format composite score
            const score = props.composite_score !== null && props.composite_score !== undefined
                ? parseFloat(props.composite_score).toFixed(2)
                : 'N/A';

            // Enhanced tooltip with score
            popup
                .setLngLat(e.lngLat)
                .setHTML(`
                    <div class="tooltip-title">${props.county_name}</div>
                    <div class="tooltip-score">
                        <span class="tooltip-score-label">Composite score (0-1):</span>
                        <span class="tooltip-score-value">${score}</span>
                    </div>
                    <div class="tooltip-grouping ${groupingClass}">${label}</div>
                `)
                .addTo(map);
        }
    });

    map.on('mousemove', 'counties-fill', handleHover);

    // Click to show detail panel
    map.on('click', 'counties-fill', async (e) => {
        const pulseHit = getPulseFeatureAt(e.point);
        if (pulseHit) {
            handlePulseSelection(pulseHit.feature, pulseHit.themeKey);
            return;
        }
        if (e.features.length > 0) {
            const feature = e.features[0];
            const fipsCode = feature.properties.fips_code;
            await loadCountyDetail(fipsCode);
        }
    });

    // Layer switching
    document.querySelectorAll('input[name="layer"]').forEach(radio => {
        radio.addEventListener('change', (e) => {
            currentLayer = e.target.value;
            switchLayer(currentLayer);
        });
    });
}

// Switch map layer
function switchLayer(layer) {
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

    map.setPaintProperty('counties-fill', 'fill-color', fillExpression);
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
        const response = await fetch(`${API_BASE_URL}/areas/${fipsCode}`);
        if (!response.ok) {
            throw new Error(`Area detail API returned ${response.status}`);
        }
        const data = await response.json();
        currentCountySummary = data;

        // Populate side panel
        document.getElementById('panel-title').textContent = data.county_name;
        document.getElementById('panel-subtitle').textContent =
            `FIPS: ${data.fips_code} | Data Year: ${data.data_year}`;

        const layerScoresContent = Object.entries(data.layer_scores).map(([key, value]) => {
            const displayValue = value !== null ? parseFloat(value).toFixed(3) : 'No Data';
            return `
                <div class="score-item clickable" data-layer-key="${key}" onclick="loadLayerDetail('${key}')" title="Click to see factor breakdown">
                    <div class="score-label">${formatLayerName(key)}</div>
                    <div class="score-value ${value === null ? 'null' : ''}">
                        ${displayValue}
                    </div>
                    ${renderScoreBar(value)}
                </div>
            `;
        }).join('');

        const content = `
            <div class="panel-section">
                <h4>County Growth Synthesis</h4>
                <div class="synthesis-badge" style="background: ${SYNTHESIS_COLORS[data.synthesis_grouping]}; color: white;">
                    ${SYNTHESIS_LABELS[data.synthesis_grouping]}
                </div>
                <p style="font-size: 14px; color: #666; margin-top: 12px; line-height: 1.5;">
                    ${getSynthesisDescription(data.synthesis_grouping)}
                </p>
                <p style="font-size: 12px; color: #8b8b8b; margin-top: 10px;">
                    Based on real-data layers available for this county and year.
                </p>
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
            </div>

            ${renderDecisionLens(data)}

            <div class="panel-section layer-scores" id="layer-scores">
                <button class="section-toggle" onclick="toggleLayerScores()" aria-expanded="false" aria-controls="layer-scores-content">
                    <span>Layer Scores</span>
                    <span class="section-toggle-icon">+</span>
                </button>
                <div class="layer-scores-content" id="layer-scores-content">
                    <div style="font-size: 11px; color: #999; margin: 8px 0 4px;">Click a layer for factor detail</div>
                    <div class="score-grid">
                        ${layerScoresContent}
                    </div>
                </div>
            </div>

            <div class="panel-section layer-detail-inline" id="layer-detail-inline">
                <h4 id="layer-detail-title">Layer Detail</h4>
                <div id="layer-detail-body" class="layer-empty-state">
                    Click a layer score to open factor detail and interpretation guidance.
                </div>
            </div>

            <div class="panel-section">
                <h4>Primary Strengths</h4>
                ${data.primary_strengths.map(s => `<div class="list-item">✓ ${s}</div>`).join('')}
            </div>

            <div class="panel-section">
                <h4>Primary Weaknesses</h4>
                ${data.primary_weaknesses.map(w => `<div class="list-item">⚠ ${w}</div>`).join('')}
            </div>

            <div class="panel-section">
                <h4>Key Trends</h4>
                ${data.key_trends.map(t => `<div class="list-item">${t}</div>`).join('')}
            </div>

            <div class="panel-section" style="border-top: 1px solid #e0e0e0; padding-top: 20px;">
                <div style="font-size: 12px; color: #999;">
                    Last Updated: ${new Date(data.last_updated).toLocaleDateString()}
                </div>
            </div>
        `;

        document.getElementById('panel-content').innerHTML = content;
        document.getElementById('side-panel').classList.add('open');
        document.getElementById('side-panel').setAttribute('data-compare-ready', 'true');
        setPanelOpenState(true);

        // Show clear selection button
        document.getElementById('clear-selection').classList.add('visible');

        // Highlight selected county on map
        if (map.getLayer('counties-selected')) {
            map.setFilter('counties-selected', ['==', 'fips_code', fipsCode]);
        }

    } catch (error) {
        console.error('Error loading county detail:', error);
        alert('County detail API is unavailable. Start backend with `make serve` to enable drill-down panels.');
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
                    <div style="font-size: 12px; color: #666; margin-bottom: 4px;">Overall Score</div>
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
                <p style="font-size: 12px; margin-top: 10px;">The data may not be available for this layer in this year.</p>
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

// Close layer modal
function closeLayerModal(event) {
    // If called from overlay click, only close if clicking the overlay itself
    if (event && event.target !== event.currentTarget) {
        return;
    }
    document.getElementById('layer-modal').classList.remove('open');
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
    const compareButton = document.querySelector('.compare-toggle');
    if (compareButton) {
        compareButton.setAttribute('aria-pressed', 'false');
    }
    clearPulse();
    // Hide clear selection button
    document.getElementById('clear-selection').classList.remove('visible');
    // Clear map selection highlight
    if (map.getLayer('counties-hover')) {
        map.setFilter('counties-hover', ['==', 'fips_code', '']);
    }
    currentFipsCode = null;
    currentCountySummary = null;
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
    const opacityExpr = activeLegendFilter
        ? ['case',
            ['==', ['get', 'synthesis_grouping'], activeLegendFilter],
            DEFAULT_FILL_OPACITY,
            DIMMED_FILL_OPACITY
        ]
        : DEFAULT_FILL_OPACITY;

    if (map.getLayer('counties-fill')) {
        map.setPaintProperty('counties-fill', 'fill-opacity', opacityExpr);
    }
    if (map.getLayer('counties-border')) {
        map.setPaintProperty('counties-border', 'line-opacity', activeLegendFilter ? 0.25 : 0.5);
    }

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
}

function clearLegendFilter() {
    activeLegendFilter = null;
    applyLegendFilter();
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
    panel.setAttribute('data-compare-mode', isActive ? 'off' : 'active');
    button.setAttribute('aria-pressed', isActive ? 'false' : 'true');
}

// Keyboard support for legend toggle
document.addEventListener('DOMContentLoaded', () => {
    const legendHeader = document.querySelector('.legend-header');
    if (legendHeader) {
        legendHeader.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' || e.key === ' ') {
                e.preventDefault();
                toggleLegend();
            }
        });
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
    }
});
