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
    style: 'mapbox://styles/mapbox/light-v11',
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
const DEFAULT_FILL_OPACITY = 0.75;
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
        setupLegendFiltering();

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

            // Enhanced tooltip with score
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
                await loadCountyDetail(fipsCode);
            }
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

    // Clear any active legend filter when switching layers
    activeLegendFilter = null;
    updateLegend(layer);
    updateUrlHash();
}

// Legend category descriptions per layer
const SYNTHESIS_DESCRIPTIONS = {
    'emerging_tailwinds': 'Reinforcing tailwinds across real-data layers',
    'conditional_growth': 'Upside exists; delivery risk matters',
    'stable_constrained': 'Steady pressures, limited upside',
    'at_risk_headwinds': 'Headwinds dominate outcomes',
    'high_uncertainty': 'Thin coverage; interpret cautiously'
};

const DIRECTIONAL_DESCRIPTIONS = {
    'improving': 'Multiple reinforcing structural tailwinds',
    'stable': 'Balanced signals, mixed pressures',
    'at_risk': 'Structural headwinds dominate'
};

const CONFIDENCE_DESCRIPTIONS = {
    'strong': 'High data coverage across layers',
    'conditional': 'Partial coverage, use with caution',
    'fragile': 'Sparse data, interpret cautiously'
};

// Update legend when layer changes
function updateLegend(layer) {
    const legendContent = document.getElementById('legend-content');
    const legendTitle = document.querySelector('.legend-header h3');
    if (!legendContent || !legendTitle) return;

    let colors, labels, descriptions;

    switch(layer) {
        case 'synthesis':
            legendTitle.textContent = 'County Growth Synthesis';
            colors = SYNTHESIS_COLORS;
            labels = SYNTHESIS_LABELS;
            descriptions = SYNTHESIS_DESCRIPTIONS;
            break;
        case 'directional':
            legendTitle.textContent = 'Directional Trajectory';
            colors = DIRECTIONAL_COLORS;
            labels = DIRECTIONAL_LABELS;
            descriptions = DIRECTIONAL_DESCRIPTIONS;
            break;
        case 'confidence':
            legendTitle.textContent = 'Evidence Confidence';
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
            <span style="font-size: 11px; color: #777;">Legend filter</span>
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
        <div class="score-context-bar" style="position: relative; height: 24px; border-radius: 4px; overflow: hidden; display: flex; margin-top: 8px;">
            <div style="flex: 30; background: #ffcdd2;" title="At Risk (0-0.3)"></div>
            <div style="flex: 20; background: #fff9c4;" title="Constrained (0.3-0.5)"></div>
            <div style="flex: 20; background: #c8e6c9;" title="Conditional (0.5-0.7)"></div>
            <div style="flex: 30; background: #2d5016; opacity: 0.3;" title="Tailwinds (0.7-1.0)"></div>
            <div style="position: absolute; left: ${pct}%; top: 0; bottom: 0; width: 3px; background: #333; transform: translateX(-50%); border-radius: 2px; box-shadow: 0 0 3px rgba(0,0,0,0.3);"></div>
        </div>
        <div style="display: flex; justify-content: space-between; font-size: 10px; color: #999; margin-top: 2px;">
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

        // Show loading state immediately
        document.getElementById('panel-title').textContent = 'Loading...';
        document.getElementById('panel-subtitle').textContent = '';
        document.getElementById('panel-content').innerHTML = `
            <div class="panel-section" style="text-align: center; padding: 40px 0;">
                <div class="loading-icon" style="width: 32px; height: 32px; margin: 0 auto 16px;"></div>
                <p style="font-size: 14px; color: #666;">Fetching county data...</p>
            </div>
        `;
        document.getElementById('side-panel').classList.add('open');
        setPanelOpenState(true);
        document.getElementById('clear-selection').classList.add('visible');

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

        // Populate side panel
        document.getElementById('panel-title').textContent = data.county_name;
        document.getElementById('panel-subtitle').textContent =
            `Data Year: ${data.data_year}`;

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

        const badgeTextColor = data.synthesis_grouping === 'stable_constrained' ? '#333' : 'white';

        const content = `
            <div class="panel-tabs">
                <button class="panel-tab active" onclick="switchPanelTab('summary', this)">Summary</button>
                <button class="panel-tab" onclick="switchPanelTab('scores', this)">Scores</button>
                <button class="panel-tab" onclick="switchPanelTab('analysis', this)">Analysis</button>
            </div>

            <div id="tab-summary" class="panel-tab-content active">
                <div class="panel-section">
                    <h4>County Growth Synthesis</h4>
                    <div class="synthesis-badge" style="background: ${SYNTHESIS_COLORS[data.synthesis_grouping]}; color: ${badgeTextColor};">
                        ${SYNTHESIS_LABELS[data.synthesis_grouping]}
                    </div>
                    <p style="font-size: 14px; color: #666; margin-top: 12px; line-height: 1.5;">
                        ${getSynthesisDescription(data.synthesis_grouping)}
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
                    ${renderScoreContext(data.composite_score)}
                </div>
            </div>

            <div id="tab-scores" class="panel-tab-content">
                <div class="panel-section">
                    <h4>Layer Scores</h4>
                    <div style="font-size: 11px; color: #999; margin: 0 0 8px;">Click a layer for factor detail</div>
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

            <div id="tab-analysis" class="panel-tab-content">
                <div class="panel-section">
                    <h4>Primary Strengths</h4>
                    ${data.primary_strengths.map(s => `<div class="list-item">&#10003; ${s}</div>`).join('')}
                </div>

                <div class="panel-section">
                    <h4>Primary Weaknesses</h4>
                    ${data.primary_weaknesses.map(w => `<div class="list-item">&#9888; ${w}</div>`).join('')}
                </div>

                <div class="panel-section">
                    <h4>Key Trends</h4>
                    ${data.key_trends.map(t => `<div class="list-item">${t}</div>`).join('')}
                </div>

                <details style="margin-top: 16px;">
                    <summary style="font-size: 12px; color: #666; cursor: pointer;">How To Use This</summary>
                    <div style="margin-top: 8px;">
                        ${renderDecisionLens(data)}
                    </div>
                </details>

                <div class="panel-section" style="border-top: 1px solid #e0e0e0; padding-top: 16px; margin-top: 20px;">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <div style="font-size: 12px; color: #999;">
                            Data Year: ${data.data_year} | Last Updated: ${new Date(data.last_updated).toLocaleDateString()}
                        </div>
                        <button onclick="exportCountyCSV()" style="padding: 4px 10px; border: 1px solid #ddd; border-radius: 4px; background: white; cursor: pointer; font-size: 11px;">Download CSV</button>
                    </div>
                </div>
            </div>
        `;

        document.getElementById('panel-content').innerHTML = content;
        document.getElementById('side-panel').setAttribute('data-compare-ready', 'true');
        document.getElementById('side-panel').scrollTop = 0;
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
            <div class="panel-section" style="text-align: center; padding: 30px 0;">
                <div style="font-size: 36px; margin-bottom: 16px;">&#x26A0;</div>
                <h4 style="font-size: 16px; color: #333; margin-bottom: 8px;">Backend Unavailable</h4>
                <p style="font-size: 14px; color: #666; line-height: 1.5;">
                    County detail data requires the API server.<br>
                    Start it with <code style="background: #f0f0f0; padding: 2px 6px; border-radius: 3px;">make serve</code>
                </p>
            </div>
        `;
        document.getElementById('side-panel').classList.add('open');
        setPanelOpenState(true);
        document.getElementById('clear-selection').classList.add('visible');
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
        compareButton.textContent = 'Compare';
    }
    compareCountyA = null;
    compareCountyB = null;
    clearPulse();
    // Hide clear selection button
    document.getElementById('clear-selection').classList.remove('visible');
    // Clear map selection highlight
    if (map.getLayer('counties-hover')) {
        map.setFilter('counties-hover', ['==', 'fips_code', '']);
    }
    if (map.getLayer('counties-selected')) {
        map.setFilter('counties-selected', ['==', 'fips_code', '']);
    }
    currentFipsCode = null;
    currentCountySummary = null;
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

    // Use the correct GeoJSON property for the active layer
    const propertyName = currentLayer === 'synthesis' ? 'synthesis_grouping'
        : currentLayer === 'directional' ? 'directional_class'
        : 'confidence_class';

    const opacityExpr = activeLegendFilter
        ? ['case',
            ['==', ['get', propertyName], activeLegendFilter],
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
    if (!isActive) {
        // Entering compare mode — store current county as A
        compareCountyA = currentCountySummary ? { ...currentCountySummary } : null;
        compareCountyB = null;
        panel.setAttribute('data-compare-mode', 'active');
        button.setAttribute('aria-pressed', 'true');
        button.textContent = 'Select 2nd County';
    } else {
        // Exiting compare mode
        compareCountyA = null;
        compareCountyB = null;
        panel.setAttribute('data-compare-mode', 'off');
        button.setAttribute('aria-pressed', 'false');
        button.textContent = 'Compare';
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
                <td style="font-weight: 500; font-size: 13px;">${formatLayerName(key)}</td>
                <td style="text-align: right; font-size: 13px;">${fmtA}</td>
                <td style="text-align: right; font-size: 13px;">${fmtB}</td>
                <td style="text-align: right; font-size: 12px; color: ${diffColor};">${diffStr}</td>
            </tr>
        `;
    }).join('');

    const content = `
        <div class="panel-section">
            <div style="display: flex; gap: 12px; margin-bottom: 16px;">
                <div style="flex: 1; text-align: center; padding: 12px; background: #f8f9fa; border-radius: 8px;">
                    <div style="font-size: 11px; color: #666; margin-bottom: 4px;">County A</div>
                    <div style="font-weight: 600; font-size: 14px;">${dataA.county_name}</div>
                    <div class="synthesis-badge" style="background: ${SYNTHESIS_COLORS[dataA.synthesis_grouping]}; color: ${dataA.synthesis_grouping === 'stable_constrained' ? '#333' : 'white'}; margin-top: 8px; font-size: 11px;">
                        ${SYNTHESIS_LABELS[dataA.synthesis_grouping]}
                    </div>
                    <div style="font-size: 20px; font-weight: 700; margin-top: 8px;">${dataA.composite_score?.toFixed(3) || 'N/A'}</div>
                    <div style="font-size: 11px; color: #666;">Composite Score</div>
                </div>
                <div style="flex: 1; text-align: center; padding: 12px; background: #f8f9fa; border-radius: 8px;">
                    <div style="font-size: 11px; color: #666; margin-bottom: 4px;">County B</div>
                    <div style="font-weight: 600; font-size: 14px;">${dataB.county_name}</div>
                    <div class="synthesis-badge" style="background: ${SYNTHESIS_COLORS[dataB.synthesis_grouping]}; color: ${dataB.synthesis_grouping === 'stable_constrained' ? '#333' : 'white'}; margin-top: 8px; font-size: 11px;">
                        ${SYNTHESIS_LABELS[dataB.synthesis_grouping]}
                    </div>
                    <div style="font-size: 20px; font-weight: 700; margin-top: 8px;">${dataB.composite_score?.toFixed(3) || 'N/A'}</div>
                    <div style="font-size: 11px; color: #666;">Composite Score</div>
                </div>
            </div>
        </div>

        <div class="panel-section">
            <h4>Layer Score Comparison</h4>
            <table style="width: 100%; border-collapse: collapse; font-size: 13px;">
                <thead>
                    <tr style="border-bottom: 2px solid #e0e0e0;">
                        <th style="text-align: left; padding: 6px 0; font-size: 11px; color: #666;">Layer</th>
                        <th style="text-align: right; padding: 6px 0; font-size: 11px; color: #666;">${dataA.county_name.split(' ')[0]}</th>
                        <th style="text-align: right; padding: 6px 0; font-size: 11px; color: #666;">${dataB.county_name.split(' ')[0]}</th>
                        <th style="text-align: right; padding: 6px 0; font-size: 11px; color: #666;">Diff</th>
                    </tr>
                </thead>
                <tbody>
                    ${scoresHtml}
                </tbody>
            </table>
        </div>

        <div class="panel-section">
            <div style="display: flex; gap: 12px;">
                <div style="flex: 1;">
                    <h4>${dataA.county_name.split(' ')[0]} Strengths</h4>
                    ${dataA.primary_strengths.map(s => `<div class="list-item" style="font-size: 13px;">&#10003; ${s}</div>`).join('')}
                </div>
                <div style="flex: 1;">
                    <h4>${dataB.county_name.split(' ')[0]} Strengths</h4>
                    ${dataB.primary_strengths.map(s => `<div class="list-item" style="font-size: 13px;">&#10003; ${s}</div>`).join('')}
                </div>
            </div>
        </div>

        <div class="panel-section">
            <div style="display: flex; gap: 12px;">
                <div style="flex: 1;">
                    <h4>${dataA.county_name.split(' ')[0]} Weaknesses</h4>
                    ${dataA.primary_weaknesses.map(w => `<div class="list-item" style="font-size: 13px;">&#9888; ${w}</div>`).join('')}
                </div>
                <div style="flex: 1;">
                    <h4>${dataB.county_name.split(' ')[0]} Weaknesses</h4>
                    ${dataB.primary_weaknesses.map(w => `<div class="list-item" style="font-size: 13px;">&#9888; ${w}</div>`).join('')}
                </div>
            </div>
        </div>
    `;

    document.getElementById('panel-content').innerHTML = content;

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
        })).sort((a, b) => a.name.localeCompare(b.name));
    }

    let highlightedIndex = -1;

    input.addEventListener('input', () => {
        const query = input.value.trim().toLowerCase();
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
            results.innerHTML = '<div class="county-search-result" style="color: #999; cursor: default;">No matches found</div>';
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
    results.classList.remove('visible');

    // Fly to county centroid
    if (typeof turf !== 'undefined' && county.feature) {
        const centroid = turf.centroid(county.feature);
        map.flyTo({
            center: centroid.geometry.coordinates,
            zoom: 9,
            duration: 1200
        });
    }

    // Load county detail
    loadCountyDetail(fipsCode);
    updateUrlHash();
}

// ============================================================
// URL Hash Routing
// ============================================================
function updateUrlHash() {
    const parts = [];
    if (currentLayer !== 'synthesis') parts.push(`layer=${currentLayer}`);
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

    if (params.layer && ['synthesis', 'directional', 'confidence'].includes(params.layer)) {
        currentLayer = params.layer;
        const radio = document.querySelector(`input[name="layer"][value="${params.layer}"]`);
        if (radio) radio.checked = true;
        switchLayer(currentLayer);
    }

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
        if (toggleBtn) toggleBtn.textContent = 'Map View';
    } else {
        document.body.classList.remove('table-mode');
        tableEl.classList.remove('visible');
        mapEl.style.display = 'block';
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
                                <td style="font-weight: 600;">${p.county_name}</td>
                                <td><span class="table-synthesis-badge" style="background: ${badgeBg}; color: ${badgeColor};">${SYNTHESIS_LABELS[p.synthesis_grouping] || p.synthesis_grouping}</span></td>
                                <td>
                                    <div class="table-score-cell">
                                        <span style="font-weight: 600; min-width: 42px;">${score !== null ? score.toFixed(3) : 'N/A'}</span>
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
            duration: 1200
        });
    }

    loadCountyDetail(fipsCode);
}
