// Maryland Growth & Family Viability Atlas - Map Interface
// Uses Mapbox GL JS to visualize synthesis groupings

// Configuration
const MAPBOX_TOKEN = 'pk.eyJ1IjoiZWxrYXJpMjMiLCJhIjoiY2tubm04b3BkMTYwcTJzcG5tZDZ2YTV5MSJ9.S0oAvquhkkMoDGrRJ_oP-Q';
const API_BASE_URL = 'http://localhost:8000/api/v1';
const GEOJSON_PATH = 'md_counties_latest.geojson';  // Use static file for now

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

// Popup for hover tooltips
const popup = new mapboxgl.Popup({
    closeButton: false,
    closeOnClick: false,
    offset: 10
});

// Current layer selection
let currentLayer = 'synthesis';

// Map load event
map.on('load', async () => {
    try {
        // Fetch GeoJSON data
        const response = await fetch(GEOJSON_PATH);
        const geojsonData = await response.json();

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

    // Show tooltip on hover
    map.on('mousemove', 'counties-fill', (e) => {
        if (e.features.length > 0) {
            const feature = e.features[0];
            const props = feature.properties;

            // Update hover highlight
            map.setFilter('counties-hover', ['==', 'fips_code', props.fips_code]);

            // Show tooltip
            const label = currentLayer === 'synthesis'
                ? SYNTHESIS_LABELS[props.synthesis_grouping]
                : currentLayer === 'directional'
                ? DIRECTIONAL_LABELS[props.directional_class]
                : CONFIDENCE_LABELS[props.confidence_class];

            popup
                .setLngLat(e.lngLat)
                .setHTML(`
                    <div class="tooltip-title">${props.county_name}</div>
                    <div class="tooltip-grouping">${label}</div>
                `)
                .addTo(map);
        }
    });

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
                <h4>Layer Scores</h4>
                <div class="score-grid">
                    ${Object.entries(data.layer_scores).map(([key, value]) => `
                        <div class="score-item">
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

    } catch (error) {
        console.error('Error loading county detail:', error);
        alert('Failed to load county details. Please try again.');
    }
}

// Close side panel
function closePanel() {
    document.getElementById('side-panel').classList.remove('open');
}

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

// ESC key to close panel
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
        closePanel();
    }
});
