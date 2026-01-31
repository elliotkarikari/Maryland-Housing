// Maryland Growth & Family Viability Atlas - Map Interface
// Uses Mapbox GL JS to visualize synthesis groupings

// Configuration
const MAPBOX_TOKEN = 'pk.eyJ1IjoiZWxrYXJpMjMiLCJhIjoiY2tubm04b3BkMTYwcTJzcG5tZDZ2YTV5MSJ9.S0oAvquhkkMoDGrRJ_oP-Q';
const API_BASE_URL = 'http://localhost:8000/api/v1';
const GEOJSON_PATH = `${API_BASE_URL}/layers/counties/latest`;  // API endpoint

// Currently selected county FIPS (for layer detail lookups)
let currentFipsCode = null;

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
    'improving': 'Improving',
    'stable': 'Stable',
    'at_risk': 'At Risk'
};

const CONFIDENCE_LABELS = {
    'strong': 'Strong',
    'conditional': 'Conditional',
    'fragile': 'Fragile'
};

// Initialize Mapbox
mapboxgl.accessToken = MAPBOX_TOKEN;

const map = new mapboxgl.Map({
    container: 'map',
    style: 'mapbox://styles/mapbox/light-v11',
    center: [-77.0, 39.0], // Center on Maryland
    zoom: 7,
    minZoom: 6,
    maxZoom: 12
});

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

map.addControl(new mapboxgl.FullscreenControl(), 'bottom-right');

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

// Update loading status helper
function updateLoadingStatus(text) {
    const statusEl = document.getElementById('loading-status');
    if (statusEl) statusEl.textContent = text;
}

// Map load event
map.on('load', async () => {
    try {
        updateLoadingStatus('Fetching county data...');

        // Fetch GeoJSON data
        const response = await fetch(GEOJSON_PATH);
        const geojsonData = await response.json();

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
                'fill-opacity': 0.7
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
            const score = props.composite_score
                ? parseFloat(props.composite_score).toFixed(2)
                : 'N/A';

            // Enhanced tooltip with score
            popup
                .setLngLat(e.lngLat)
                .setHTML(`
                    <div class="tooltip-title">${props.county_name}</div>
                    <div class="tooltip-score">
                        <span class="tooltip-score-label">Score:</span>
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

// Load county detail from API
async function loadCountyDetail(fipsCode) {
    try {
        currentFipsCode = fipsCode;
        const response = await fetch(`${API_BASE_URL}/areas/${fipsCode}`);
        const data = await response.json();

        // Populate side panel
        document.getElementById('panel-title').textContent = data.county_name;
        document.getElementById('panel-subtitle').textContent =
            `FIPS: ${data.fips_code} | Data Year: ${data.data_year}`;

        const content = `
            <div class="panel-section">
                <h4>Final Synthesis Grouping</h4>
                <div class="synthesis-badge" style="background: ${SYNTHESIS_COLORS[data.synthesis_grouping]}; color: white;">
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
                        <div class="score-label">Directional Status</div>
                        <div class="score-value">${DIRECTIONAL_LABELS[data.directional_class]}</div>
                    </div>
                    <div class="score-item">
                        <div class="score-label">Confidence Level</div>
                        <div class="score-value">${CONFIDENCE_LABELS[data.confidence_class]}</div>
                    </div>
                    <div class="score-item">
                        <div class="score-label">Composite Score</div>
                        <div class="score-value">${data.composite_score ? data.composite_score.toFixed(3) : 'N/A'}</div>
                    </div>
                    <div class="score-item">
                        <div class="score-label">Data Year</div>
                        <div class="score-value">${data.data_year}</div>
                    </div>
                </div>
            </div>

            <div class="panel-section">
                <h4>Layer Scores <span style="font-size: 11px; font-weight: normal; color: #999;">(click for details)</span></h4>
                <div class="score-grid">
                    ${Object.entries(data.layer_scores).map(([key, value]) => `
                        <div class="score-item clickable" onclick="loadLayerDetail('${key}')" title="Click to see factor breakdown">
                            <div class="score-label">${formatLayerName(key)}</div>
                            <div class="score-value ${value === null ? 'null' : ''}">
                                ${value !== null ? parseFloat(value).toFixed(3) : 'No Data'}
                            </div>
                        </div>
                    `).join('')}
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

        // Show clear selection button
        document.getElementById('clear-selection').classList.add('visible');

        // Highlight selected county on map
        if (map.getLayer('counties-selected')) {
            map.setFilter('counties-selected', ['==', 'fips_code', fipsCode]);
        }

    } catch (error) {
        console.error('Error loading county detail:', error);
        alert('Failed to load county details. Please try again.');
    }
}

// Load layer detail and show modal
async function loadLayerDetail(layerKey) {
    if (!currentFipsCode) {
        console.error('No county selected');
        return;
    }

    try {
        const response = await fetch(`${API_BASE_URL}/areas/${currentFipsCode}/layers/${layerKey}`);

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = await response.json();

        // Update modal header
        document.getElementById('layer-modal-title').textContent = data.display_name;
        document.getElementById('layer-modal-desc').textContent = data.description;

        // Get trend icon
        const trendIcon = getTrendIcon(data.momentum_direction);
        const trendText = getTrendText(data.momentum_direction, data.momentum_slope);

        // Build factor list
        const factorHtml = data.factors.map(f => {
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

        // Build modal body content
        const bodyContent = `
            <div class="layer-score-main">
                <div>
                    <div style="font-size: 12px; color: #666; margin-bottom: 4px;">Overall Score</div>
                    <div class="layer-score-value">${data.score !== null ? data.score.toFixed(3) : 'N/A'}</div>
                </div>
                <div class="layer-score-trend">
                    <div class="layer-trend-icon ${data.momentum_direction || ''}">${trendIcon}</div>
                    <div class="layer-trend-text">${trendText}</div>
                </div>
            </div>

            <div class="layer-formula">
                <strong>Formula:</strong> ${data.formula}
            </div>

            <div class="layer-factors">
                <h4>Contributing Factors</h4>
                ${factorHtml}
            </div>

            <div class="layer-metadata">
                <span>Version: ${data.version}</span>
                <span>Data Year: ${data.data_year}</span>
                <span>Coverage: ${data.coverage_years || 'N/A'} years</span>
            </div>
        `;

        document.getElementById('layer-modal-body').innerHTML = bodyContent;

        // Show modal
        document.getElementById('layer-modal').classList.add('open');

    } catch (error) {
        console.error('Error loading layer detail:', error);
        // Show a simpler error message instead of alert
        document.getElementById('layer-modal-title').textContent = 'Layer Details';
        document.getElementById('layer-modal-desc').textContent = '';
        document.getElementById('layer-modal-body').innerHTML = `
            <div style="padding: 20px; text-align: center; color: #666;">
                <p>Unable to load detailed factor breakdown.</p>
                <p style="font-size: 12px; margin-top: 10px;">The data may not be available for this layer.</p>
            </div>
        `;
        document.getElementById('layer-modal').classList.add('open');
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
    // Hide clear selection button
    document.getElementById('clear-selection').classList.remove('visible');
    // Clear map selection highlight
    if (map.getLayer('counties-hover')) {
        map.setFilter('counties-hover', ['==', 'fips_code', '']);
    }
    currentFipsCode = null;
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
        'emerging_tailwinds': 'Multiple reinforcing structural tailwinds are present with high confidence. Strong likelihood of persistence if current trends hold.',
        'conditional_growth': 'Growth signals exist, but execution and local context matter significantly. Outcomes depend on policy delivery and external factors.',
        'stable_constrained': 'Systems are holding steady with balanced pressures, but limited upside potential under current conditions.',
        'at_risk_headwinds': 'Structural headwinds dominate, creating challenges for growth capacity and resilience.',
        'high_uncertainty': 'Model confidence is low due to sparse data, contested classifications, or fragile policy persistence. Priority area for local knowledge and ground-truthing.'
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
        // Close layer modal first if open
        const layerModal = document.getElementById('layer-modal');
        if (layerModal.classList.contains('open')) {
            closeLayerModal();
        } else {
            closePanel();
        }
    }
});
