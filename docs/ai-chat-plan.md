# AI Chat Feature — Implementation Plan

> **Status**: Draft
> **Date**: 2026-02-07
> **Branch**: `V2.-Improving-build`

---

## 1. Vision

Add a conversational AI assistant to the Maryland Housing Atlas that lets users **ask questions about their selected area** and get answers grounded in our data. The assistant can:

- Explain what scores mean and why a county/tract scored the way it did
- Answer spatial questions like *"Show me schools within 5 miles"* and **highlight results on the map**
- Discuss policy implications — what a low housing elasticity score means for growth
- Compare areas conversationally — *"How does Howard compare to Montgomery for school access?"*
- Track and reference Capital Improvement Plans and policy documents we've already extracted

The chat is **context-aware** — it knows which county is selected, which layer is active, and what data is loaded. As users zoom in, the map transitions from county-level to **census tract-level** data, and the AI assistant seamlessly navigates both resolutions.

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│  Frontend (map.js / index.html)                         │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────────┐  │
│  │  Map      │  │ Sidebar  │  │  Chat Panel           │  │
│  │  + marker │  │ (detail) │  │  - messages            │  │
│  │  overlay  │  │          │  │  - input               │  │
│  │  layer    │  │          │  │  - map action buttons   │  │
│  └──────────┘  └──────────┘  └──────────────────────┘  │
│       ▲               ▲               │                  │
│       │               │               ▼                  │
│       └───────────────┴──── chat.js (new module) ───────│
│                                       │                  │
└───────────────────────────────────────┼──────────────────┘
                                        │ POST /api/v1/chat
                                        ▼
┌─────────────────────────────────────────────────────────┐
│  Backend (FastAPI)                                       │
│  ┌─────────────┐  ┌──────────────┐  ┌───────────────┐  │
│  │ chat_routes  │→│ ChatService   │→│ OpenAI (func   │  │
│  │ .py          │  │ (orchestrate)│  │  calling)      │  │
│  └─────────────┘  └──────┬───────┘  └───────────────┘  │
│                          │                               │
│                  ┌───────▼───────┐                       │
│                  │  Tool Functions│                       │
│                  │  - spatial     │                       │
│                  │  - scores      │                       │
│                  │  - policy      │                       │
│                  │  - compare     │                       │
│                  └───────────────┘                       │
└─────────────────────────────────────────────────────────┘
```

---

## 3. Frontend Design

### 3.1 Chat Panel Placement

**Recommended: Docked bottom-right floating panel** (like Intercom/ChatGPT widgets)

- Collapsed state: Small "Ask Atlas" pill button (bottom-right, above map controls)
- Expanded state: 380×500px floating panel with message list + input
- Does NOT replace or conflict with the sidebar — both visible simultaneously
- On mobile (<768px): Expands to full-width bottom sheet

```
┌────────────────────────────────┬──────────────┐
│                                │  Sidebar     │
│         MAP                   │  (county     │
│                                │   detail)    │
│                                │              │
│                                │              │
│              ┌─────────────┐  │              │
│              │  Chat Panel  │  │              │
│              │  (floating)  │  │              │
│              └─────────────┘  │              │
└────────────────────────────────┴──────────────┘
```

### 3.2 Chat Panel UI Components

```html
<!-- New elements in index.html -->
<button id="chat-toggle" class="chat-toggle" aria-label="Open AI assistant">
  <span class="chat-toggle-icon"><!-- sparkle/chat icon --></span>
  <span class="chat-toggle-label">Ask Atlas</span>
</button>

<div id="chat-panel" class="chat-panel" hidden>
  <div class="chat-header">
    <span class="chat-title">Atlas Assistant</span>
    <span class="chat-context-badge" id="chat-context-badge">
      <!-- shows "Montgomery County · Schools" when context is set -->
    </span>
    <button class="chat-close" aria-label="Close">&times;</button>
  </div>

  <div class="chat-messages" id="chat-messages">
    <!-- Welcome message + conversation -->
  </div>

  <div class="chat-input-area">
    <textarea id="chat-input" placeholder="Ask about this area..."
              rows="1" maxlength="1000"></textarea>
    <button id="chat-send" aria-label="Send">→</button>
  </div>
</div>
```

### 3.3 Chat Context System

The chat automatically inherits context from the map state:

```javascript
// chat.js — build context from current map state
function getChatContext() {
  return {
    selected_county: currentFipsCode,           // "24031"
    county_name: currentCountySummary?.county_name,  // "Montgomery"
    active_layer: currentLayer,                  // "synthesis"
    layer_scores: currentCountySummary?.layer_scores, // {employment: 0.72, ...}
    map_center: map.getCenter(),                 // {lng, lat}
    map_zoom: map.getZoom(),                     // 9.5
    active_filter: activeLegendFilter,           // "improving"
    compare_mode: !!compareCountyA               // true/false
  };
}
```

### 3.4 Map Action Rendering

When the AI responds with a spatial action (e.g., "here are schools within 5 miles"), the chat renders:

1. **Inline map preview** — small static view in the chat bubble
2. **"Show on map" button** — clicking adds a temporary overlay layer to the main map
3. **Marker cluster** — POIs displayed as markers with popup info
4. **Radius circle** — visual 5-mile radius circle on the map

```javascript
// chat.js — handle map actions from AI response
function renderMapAction(action) {
  if (action.type === 'highlight_points') {
    addTemporaryMarkers(action.points);  // [{lng, lat, name, type, ...}]
    addRadiusCircle(action.center, action.radius_miles);
  }
  if (action.type === 'highlight_counties') {
    highlightCounties(action.fips_codes);  // highlight multiple counties
  }
  if (action.type === 'compare') {
    loadCompareCounty(action.fips_code_b);  // trigger existing compare mode
  }
}
```

---

## 4. Backend Design

### 4.1 New Files

```
src/ai/
├── chat/
│   ├── __init__.py
│   ├── service.py          # ChatService — orchestrates conversation
│   ├── tools.py            # Tool function definitions for OpenAI function calling
│   ├── prompts.py          # System prompts and context builders
│   └── spatial.py          # Spatial query helpers (radius search, nearest)
src/api/
├── chat_routes.py          # POST /api/v1/chat endpoint
frontend/
├── chat.js                 # Chat UI logic (new module)
```

### 4.2 API Endpoint

```python
# src/api/chat_routes.py

@router.post("/chat")
async def chat(request: ChatRequest) -> ChatResponse:
    """
    Conversational AI endpoint with tool-use.

    Request:
      {
        "message": "What schools are within 5 miles of Bethesda?",
        "context": {
          "selected_county": "24031",
          "active_layer": "school_trajectory",
          "map_center": {"lng": -77.09, "lat": 38.98},
          "map_zoom": 11
        },
        "conversation_id": "uuid",    # optional, for multi-turn
        "history": [                    # last N messages for context
          {"role": "user", "content": "..."},
          {"role": "assistant", "content": "..."}
        ]
      }

    Response:
      {
        "message": "I found 12 schools within 5 miles of Bethesda...",
        "map_actions": [
          {
            "type": "highlight_points",
            "points": [{"lng": -77.1, "lat": 39.0, "name": "Walt Whitman HS", ...}],
            "center": {"lng": -77.09, "lat": 38.98},
            "radius_miles": 5
          }
        ],
        "sources": ["NCES CCD 2024", "MSDE Enrollment"],
        "conversation_id": "uuid",
        "tokens_used": 1250,
        "cost_estimate": 0.0045
      }
    """
```

### 4.3 Chat Service with Function Calling

The core innovation: **OpenAI function calling** lets the LLM decide when to query our data.

```python
# src/ai/chat/service.py

class ChatService:
    """
    Orchestrates AI chat with tool-use loop.

    Flow:
    1. User message + context → build system prompt
    2. Send to OpenAI with tool definitions
    3. If model calls a tool → execute it, return result to model
    4. Model generates final response (may include map_actions)
    5. Return response to frontend
    """

    TOOLS = [
        {
            "type": "function",
            "function": {
                "name": "search_nearby_pois",
                "description": "Search for points of interest within a radius",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "center_lat": {"type": "number"},
                        "center_lng": {"type": "number"},
                        "radius_miles": {"type": "number", "default": 5},
                        "poi_type": {
                            "type": "string",
                            "enum": ["school", "hospital", "transit_stop",
                                     "employer", "park", "library"]
                        }
                    },
                    "required": ["center_lat", "center_lng", "poi_type"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "get_county_scores",
                "description": "Get detailed scores and factors for a county",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "fips_code": {"type": "string"},
                        "layer_key": {"type": "string"}
                    },
                    "required": ["fips_code"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "compare_counties",
                "description": "Compare two counties across all layers",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "fips_code_a": {"type": "string"},
                        "fips_code_b": {"type": "string"}
                    },
                    "required": ["fips_code_a", "fips_code_b"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "get_policy_documents",
                "description": "Retrieve extracted CIP/policy data for a county",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "fips_code": {"type": "string"},
                        "document_type": {
                            "type": "string",
                            "enum": ["cip", "zoning", "comprehensive_plan"]
                        }
                    },
                    "required": ["fips_code"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "explain_classification",
                "description": "Explain why a county has its classification",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "fips_code": {"type": "string"}
                    },
                    "required": ["fips_code"]
                }
            }
        }
    ]
```

### 4.4 Spatial Query Engine

For "schools within 5 miles" type queries, we need a lightweight spatial search:

```python
# src/ai/chat/spatial.py

class SpatialQueryEngine:
    """
    Handles radius/proximity queries against POI data.

    Data sources:
    - NCES school locations (lat/lng from CCD)
    - Transit stops (GTFS)
    - Employer locations (from LEHD/LODES geocoded)

    For V1: Use Haversine distance on pre-loaded CSV/GeoJSON of POIs.
    For V2: PostGIS ST_DWithin queries for production performance.
    """

    def search_nearby(self, center, radius_miles, poi_type) -> list[dict]:
        """Return POIs within radius, sorted by distance."""
        ...

    def get_poi_details(self, poi_id, poi_type) -> dict:
        """Get detailed info for a specific POI."""
        ...
```

**POI data sources for V1:**

| POI Type | Source | Records | Format |
|----------|--------|---------|--------|
| Schools | NCES CCD | ~1,400 (MD) | CSV with lat/lng |
| Transit | GTFS (MTA Maryland) | ~9,000 stops | GTFS stops.txt |
| Hospitals | HIFLD Open Data | ~60 (MD) | GeoJSON |

### 4.5 System Prompt Design

```python
# src/ai/chat/prompts.py

SYSTEM_PROMPT = """You are Atlas, an AI assistant for the Maryland Housing Atlas.

You help users understand housing market conditions, growth potential, and policy
implications across Maryland counties and census tracts.

## Your Data
You have access to a 6-layer scoring framework:
1. Employment Gravity — job accessibility and economic diversity
2. Mobility Optionality — transit, walk, bike accessibility
3. School Trajectory — education quality and access
4. Housing Elasticity — affordability and supply responsiveness
5. Demographic Momentum — population growth and equity
6. Risk Drag — environmental and infrastructure risk (penalty)

Counties are classified as: Improving / Stable / At Risk
With confidence levels: Strong / Conditional / Fragile

## Current Context
{context_block}

## Rules
- Ground answers in our data. Cite specific scores and factors.
- When asked about nearby places, use the search_nearby_pois tool.
- When spatial results are returned, include a map_action in your response.
- Be honest about data limitations. Say "our data doesn't cover that" vs guessing.
- Keep responses concise (2-4 paragraphs max unless asked for detail).
- When discussing policy, explain mechanisms, don't make political judgments.
- Reference specific layer scores: e.g. "Montgomery's school trajectory score is 0.78"
"""
```

---

## 5. Implementation Phases

### Phase 1: Chat UI Shell + Basic Q&A (1-2 weeks)

**Goal**: Working chat panel with context-aware conversation, no spatial features yet.

**Frontend:**
- [ ] Add chat panel HTML/CSS to `index.html`
- [ ] Create `frontend/chat.js` — panel toggle, message rendering, send/receive
- [ ] Wire context from map state (selected county, active layer, scores)
- [ ] Streaming response display (SSE or chunked fetch)
- [ ] Welcome message with suggested questions based on current context

**Backend:**
- [ ] Create `src/api/chat_routes.py` with POST `/api/v1/chat`
- [ ] Create `src/ai/chat/service.py` — ChatService with OpenAI function calling
- [ ] Create `src/ai/chat/prompts.py` — system prompt with context injection
- [ ] Implement `get_county_scores` and `explain_classification` tools
- [ ] Implement `compare_counties` tool (reuses existing API logic)
- [ ] Add conversation history support (in-memory, last 10 messages)
- [ ] Add cost tracking (reuse existing `estimate_cost` from OpenAI provider)

**Config:**
- [ ] Add `CHAT_ENABLED`, `CHAT_MODEL`, `CHAT_MAX_TOKENS` to settings.py
- [ ] Add rate limiting (10 messages/min per session)

### Phase 2: Spatial Queries + Map Actions (1-2 weeks)

**Goal**: "Show me schools within 5 miles" → markers appear on map.

**Data Prep:**
- [ ] Download and process NCES CCD school locations for Maryland
- [ ] Download MTA GTFS feed (transit stops)
- [ ] Store as lightweight GeoJSON/CSV in `data/pois/`
- [ ] Create `src/ai/chat/spatial.py` — Haversine search engine

**Backend:**
- [ ] Implement `search_nearby_pois` tool function
- [ ] Add `map_actions` to chat response schema
- [ ] Support action types: `highlight_points`, `highlight_counties`, `radius_circle`

**Frontend:**
- [ ] Parse `map_actions` from chat response
- [ ] Add temporary marker layer to map (`chat-markers` source/layer)
- [ ] Render radius circle using Mapbox `turf.circle()`
- [ ] "Show on map" / "Clear markers" buttons in chat messages
- [ ] Marker popups with POI details (name, type, distance)
- [ ] Auto-zoom to fit markers + radius

### Phase 3: Policy Intelligence + Document Context (1 week)

**Goal**: Chat can reference extracted CIP data and discuss policy implications.

- [ ] Implement `get_policy_documents` tool — queries `ai_evidence_link` table
- [ ] Feed CIP extractions into chat context (school budgets, capital plans)
- [ ] Add `get_layer_trends` tool — factor momentum and year-over-year changes
- [ ] Prompt engineering for policy discussion (mechanisms, not politics)
- [ ] Citation format: "According to Montgomery County's FY25-30 CIP..."

### Phase 4: Tract-Level Data Layer + Zoom Transitions (2-3 weeks)

**Goal**: Map transitions from county → tract as users zoom in. Tracts show the same 6-layer scoring framework at finer granularity. AI chat navigates both levels.

> See **Section 13** below for full technical design.

**Data Pipeline:**
- [ ] Download Census Cartographic Boundary tracts for MD (`cb_2023_24_tract_500k`)
- [ ] Generate tract-level GeoJSON with scores from existing DB tables
- [ ] Extend `geojson_export.py` to support `level="tract"` export
- [ ] Extend scoring pipeline: run synthesis/classification at tract level
- [ ] Backfill layers 3-6 with tract-level computation (layers 1-2 already done)

**Frontend — Zoom Transition:**
- [ ] Add `tracts` GeoJSON source alongside existing `counties` source
- [ ] Add `tracts-fill`, `tracts-border`, `tracts-hover`, `tracts-selected` layers
- [ ] Set county layers `maxzoom: 9`, tract layers `minzoom: 8` (1-zoom crossfade)
- [ ] Opacity crossfade: counties fade out zoom 8→9, tracts fade in zoom 8→9
- [ ] Sync layer switching (synthesis/directional/confidence) across both levels
- [ ] Update sidebar: show tract detail when tract clicked at high zoom
- [ ] Update legend and county list for tract resolution

**Frontend — Interaction:**
- [ ] Click handler at zoom ≥9 queries tract features instead of county
- [ ] Tract detail panel: show tract FIPS, parent county, scores, factors
- [ ] Search: allow searching by tract number or neighborhood name
- [ ] URL hash: support `#tract=24031402100` deep linking

**Backend — API:**
- [ ] `GET /api/v1/layers/tracts/latest` — tract-level GeoJSON
- [ ] `GET /api/v1/areas/{tract_geoid}` — tract detail (11-digit FIPS)
- [ ] `GET /api/v1/areas/{tract_geoid}/layers/{layer_key}` — tract factor breakdown

**AI Chat Integration:**
- [ ] Extend chat context to include `resolution: "county" | "tract"` and `selected_tract`
- [ ] Add `get_tract_scores` tool function for AI
- [ ] Update system prompt: AI knows which zoom level the user is at
- [ ] AI can suggest "zoom in for tract-level detail" or "zoom out for county overview"
- [ ] Spatial queries (schools within 5mi) work from tract centroid when at tract level

### Phase 5: Polish + Production Hardening (1 week)

**Goal**: Performance, caching, error handling, mobile.

- [ ] Tract GeoJSON lazy-loading (only fetch when zoom crosses threshold)
- [ ] Cache tract GeoJSON in service worker or localStorage
- [ ] Vector tile conversion with tippecanoe for production (optional)
- [ ] Mobile bottom-sheet for tract detail
- [ ] Error boundaries for AI chat failures
- [ ] Analytics: track zoom level distribution, chat usage at county vs tract

---

## 6. Data Flow — Example Interaction

**User clicks Montgomery County, activates School Trajectory layer, opens chat:**

```
User: "What schools are within 5 miles of downtown Bethesda?"

→ Frontend sends:
  POST /api/v1/chat
  {
    "message": "What schools are within 5 miles of downtown Bethesda?",
    "context": {
      "selected_county": "24031",
      "active_layer": "school_trajectory",
      "map_center": {"lng": -77.0947, "lat": 38.9807}
    }
  }

→ Backend: ChatService builds system prompt with Montgomery scores
→ OpenAI response: function_call("search_nearby_pois", {
    center_lat: 38.9807, center_lng: -77.0947,
    radius_miles: 5, poi_type: "school"
  })
→ Backend executes spatial query, returns 15 schools
→ OpenAI generates natural language summary

→ Response:
  {
    "message": "I found 15 schools within 5 miles of downtown Bethesda.
      Montgomery County has a strong school trajectory score of 0.78,
      ranking 3rd among MD counties. Notable schools include...",
    "map_actions": [{
      "type": "highlight_points",
      "points": [{name: "Walt Whitman HS", lat: 38.97, lng: -77.10, ...}, ...],
      "center": {lng: -77.0947, lat: 38.9807},
      "radius_miles": 5
    }],
    "sources": ["NCES CCD 2024"]
  }

→ Frontend: Renders message + "Show on map" button
→ User clicks "Show on map" → markers + circle appear on map
```

---

## 7. Key Technical Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **LLM integration** | OpenAI function calling | Already have provider; function calling is purpose-built for tool-use |
| **Chat model** | `gpt-4o` (upgrade from `gpt-4-turbo`) | Faster, cheaper, better at function calling; easy swap in provider |
| **Spatial engine (V1)** | In-memory Haversine on CSV | Fast to implement, no new dependencies, sufficient for ~10K POIs |
| **Spatial engine (V2)** | PostGIS `ST_DWithin` | Production-grade for larger datasets, already have PostgreSQL |
| **Conversation state** | Client-side history (last 10 msgs) | No server-side session store needed; stateless API |
| **Streaming** | Server-Sent Events (SSE) | Better UX than waiting for full response; FastAPI supports it |
| **Chat panel** | Floating bottom-right | Non-destructive to existing layout; familiar pattern; works on mobile |
| **Map overlays** | Temporary Mapbox source/layer | Clean separation from core county layers; easy add/remove |
| **Cost control** | Per-session token budget | Prevent runaway costs; configurable limit |

---

## 8. Cost Estimates

| Model | Input ($/1M tokens) | Output ($/1M tokens) | Avg chat turn | Cost/turn |
|-------|---------------------|----------------------|---------------|-----------|
| gpt-4o | $2.50 | $10.00 | ~2K in, ~500 out | ~$0.01 |
| gpt-4-turbo | $10.00 | $30.00 | ~2K in, ~500 out | ~$0.035 |
| gpt-4o-mini | $0.15 | $0.60 | ~2K in, ~500 out | ~$0.0006 |

**Recommendation**: Use `gpt-4o-mini` for most chat interactions (very cheap), escalate to `gpt-4o` only for complex policy analysis or when mini produces poor results.

**Budget guardrails:**
- Per-session limit: 50 messages or $0.50 (whichever first)
- Per-day limit: $10.00 across all sessions
- Alert at 80% of daily limit

---

## 9. Files to Create/Modify

### New Files
| File | Purpose |
|------|---------|
| `frontend/chat.js` | Chat UI logic, context builder, map action renderer |
| `src/api/chat_routes.py` | POST /api/v1/chat endpoint |
| `src/ai/chat/__init__.py` | Package init |
| `src/ai/chat/service.py` | ChatService — orchestration + function calling loop |
| `src/ai/chat/tools.py` | Tool function definitions and executors |
| `src/ai/chat/prompts.py` | System prompts and context templates |
| `src/ai/chat/spatial.py` | Spatial query engine (Haversine search) |
| `data/pois/md_schools.csv` | Maryland school locations from NCES |
| `data/pois/md_transit_stops.csv` | Maryland transit stops from GTFS |
| `frontend/md_tracts_latest.geojson` | Tract-level boundaries + scores (~5-10 MB) |

### Modified Files
| File | Changes |
|------|---------|
| `frontend/index.html` | Add chat panel HTML + CSS (~100 lines); tract detail panel |
| `frontend/map.js` | Import chat.js, add tract layers + zoom transition, dual-resolution click handler, marker layer management |
| `src/api/main.py` | Register chat_routes router |
| `src/api/routes.py` | Extract shared logic for county scores (reusable by chat tools); add tract endpoints; unified `/areas/{geoid}` for county + tract |
| `config/settings.py` | Add CHAT_ENABLED, CHAT_MODEL, CHAT_MAX_TOKENS, CHAT_RATE_LIMIT |
| `src/ai/providers/openai_provider.py` | Add `chat_completion()` method alongside existing `extract_structured()` |
| `src/export/geojson_export.py` | Add `level="tract"` support, `export_tract_geojson()`, tract boundary fetching |

---

## 10. Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Hallucinated answers | Users trust wrong data | Ground in tool results; add "Based on our data..." framing; confidence indicators |
| Cost overruns | Unexpected API bills | Per-session + daily limits; default to gpt-4o-mini; token counting |
| Slow responses | Poor UX | SSE streaming; show typing indicator; cache common queries |
| POI data staleness | Wrong school/transit info | Version + date stamp all POI data; periodic refresh pipeline |
| Prompt injection | User manipulates system prompt | Input sanitization; role separation; don't expose raw system prompt |
| Mobile UX | Panel too small | Bottom-sheet pattern; collapsible; limit message length |
| Tract GeoJSON size | Slow initial load on mobile | Lazy-load at zoom ≥7; gzip (~2 MB); cache in localStorage; degrade to county-only on slow connections |
| Incomplete tract scores | Misleading precision at tract zoom | Show confidence badge ("county estimate" vs "tract data"); degrade confidence_class when using fallback |
| Zoom transition jarring | Visual pop between resolutions | Opacity crossfade over 1 zoom level (8→9); keep faint county borders at tract zoom for context |

---

## 11. Success Metrics

- **Engagement**: % of sessions that open chat; messages per session
- **Accuracy**: Manual review of 50 random conversations/week for correctness
- **Spatial accuracy**: Verify POI results match ground truth (sample)
- **Cost efficiency**: Average cost per session < $0.05
- **Response time**: p50 < 3s, p95 < 8s (including function calls)
- **User satisfaction**: Thumbs up/down on each response (optional)

---

## 12. Open Questions

1. **POI data freshness** — How often should we refresh school/transit data? Monthly? Quarterly?
2. **Authentication** — Should chat be open to all users or require auth? (Cost concern)
3. **Conversation persistence** — Do we want to save conversations for analytics, or keep them ephemeral?
4. **Multi-provider** — Should we add Claude as a chat provider option alongside OpenAI?
5. **Tract naming** — Should we show tract FIPS numbers (e.g., 2403140210) or derive neighborhood/place names for friendlier UX?
6. **Layers 3-6 at tract level** — What's the priority order for backfilling remaining layers with tract-level computation?

---

## 13. Tract-Level Zoom Transition — Technical Design

### 13.1 Current State

| Aspect | Today |
|--------|-------|
| **Map data** | 24 county polygons, single GeoJSON (~380KB) |
| **Map zoom** | Initial: 7, range: 6–16, no zoom-dependent switching |
| **Layers in map.js** | `counties-fill`, `counties-border`, `counties-hover`, `counties-selected`, `county-labels` |
| **DB tract data** | Layers 1 & 2 already compute and store full tract-level results (`layer1_economic_opportunity_tract`, `layer2_mobility_accessibility_tract`) |
| **DB county data** | All 6 layers + synthesis/classification at county level |
| **GeoJSON export** | County only — `geojson_export.py` raises ValueError for non-county |

**Key finding**: We already have tract-level scores for employment gravity and mobility optionality sitting in the database. The pipeline gap is layers 3-6 and the synthesis/classification step at tract level.

### 13.2 Maryland Tract Data Profile

| Metric | Value |
|--------|-------|
| Total census tracts | ~1,295 (2020 Census) |
| GeoJSON size (estimated) | 5–10 MB uncompressed, 1–3 MB gzipped |
| Boundary source | Census Cartographic Boundary `cb_2023_24_tract_500k` (pre-simplified) |
| Tract FIPS format | 11 digits: `{state:2}{county:3}{tract:6}` e.g., `24031402100` |
| Performance | Well within Mapbox GeoJSON source capability (~1,300 polygons) |

### 13.3 Zoom Transition Architecture

**Approach: Dual GeoJSON sources with opacity crossfade**

```
Zoom 6 ─────── 7 ─────── 8 ──── 8.5 ──── 9 ─────── 10 ─────── 16
       ┌──────────────────────────────────┐
       │   COUNTY LAYER (24 features)     │
       │   opacity: 0.85 ──────── 0.85 ──── fade ──── 0
       │   maxzoom: 9.5                   │
       └──────────────────────────────────┘
                                   ┌──────────────────────────────────┐
                                   │   TRACT LAYER (~1,295 features)  │
                                   │   opacity: 0 ──── fade ──── 0.85 │
                                   │   minzoom: 8                      │
                                   └──────────────────────────────────┘
```

**Zoom threshold**: **8.5** (crossfade range: 8–9)

At zoom 8, Maryland fills the viewport and individual counties are distinct. By zoom 9, you're looking at a single county and tracts become meaningful.

### 13.4 Frontend Implementation

#### Source & Layer Setup

```javascript
// Constants
const COUNTY_MAX_ZOOM = 9.5;
const TRACT_MIN_ZOOM = 8;
const CROSSFADE_LOW = 8;
const CROSSFADE_HIGH = 9;

// After GeoJSON loads, add tract source
async function loadTractData() {
    const urls = [
        GEOJSON_BLOB_URL?.replace('counties', 'tracts'),
        `${API_BASE}/layers/tracts/latest`,
        './md_tracts_latest.geojson'
    ];
    for (const url of urls) {
        try {
            const resp = await fetch(url);
            if (resp.ok) return await resp.json();
        } catch (e) { continue; }
    }
    return null;
}

// Add tract source + layers (mirrors county layer stack)
function addTractLayers(tractGeojson) {
    map.addSource('tracts', {
        type: 'geojson',
        data: tractGeojson,
        maxzoom: 12  // limit client-side tiling work
    });

    map.addLayer({
        id: 'tracts-fill',
        type: 'fill',
        source: 'tracts',
        minzoom: TRACT_MIN_ZOOM,
        paint: {
            'fill-color': getSynthesisFillExpression(),  // reuse same expressions
            'fill-opacity': ['interpolate', ['linear'], ['zoom'],
                CROSSFADE_LOW, 0,
                CROSSFADE_HIGH, 0.85
            ]
        }
    }, 'counties-border');  // insert below county borders

    map.addLayer({
        id: 'tracts-border',
        type: 'line',
        source: 'tracts',
        minzoom: TRACT_MIN_ZOOM,
        paint: {
            'line-color': '#666',
            'line-width': ['interpolate', ['linear'], ['zoom'], 8, 0.3, 12, 0.8],
            'line-opacity': ['interpolate', ['linear'], ['zoom'],
                CROSSFADE_LOW, 0,
                CROSSFADE_HIGH, 0.4
            ]
        }
    });

    map.addLayer({
        id: 'tracts-hover',
        type: 'line',
        source: 'tracts',
        minzoom: TRACT_MIN_ZOOM,
        paint: { 'line-color': '#000', 'line-width': 2 },
        filter: ['==', 'tract_geoid', '']
    });

    map.addLayer({
        id: 'tracts-selected',
        type: 'line',
        source: 'tracts',
        minzoom: TRACT_MIN_ZOOM,
        paint: { 'line-color': '#1a73e8', 'line-width': 3, 'line-opacity': 0.9 },
        filter: ['==', 'tract_geoid', '']
    });
}
```

#### County Layer Opacity Adjustment

Modify existing county layers to fade out at high zoom:

```javascript
// Update counties-fill paint to crossfade out
map.setPaintProperty('counties-fill', 'fill-opacity',
    ['interpolate', ['linear'], ['zoom'],
        CROSSFADE_LOW, 0.85,
        CROSSFADE_HIGH, 0
    ]
);

// County borders remain visible slightly longer (context)
map.setPaintProperty('counties-border', 'line-opacity',
    ['interpolate', ['linear'], ['zoom'],
        CROSSFADE_LOW, 0.4,
        CROSSFADE_HIGH + 1, 0.15,  // faint county borders at tract zoom
        13, 0
    ]
);
```

#### Resolution-Aware Click Handler

```javascript
function getActiveResolution() {
    return map.getZoom() >= CROSSFADE_HIGH ? 'tract' : 'county';
}

// Unified click handler
map.on('click', (e) => {
    const resolution = getActiveResolution();

    if (resolution === 'tract') {
        const features = map.queryRenderedFeatures(e.point, { layers: ['tracts-fill'] });
        if (features.length > 0) {
            const tract = features[0].properties;
            loadTractDetail(tract.tract_geoid);  // new function
        }
    } else {
        const features = map.queryRenderedFeatures(e.point, { layers: ['counties-fill'] });
        if (features.length > 0) {
            const county = features[0].properties;
            loadCountyDetail(county.fips_code);  // existing function
        }
    }
});
```

#### Layer Switching Sync

When user switches between synthesis/directional/confidence, BOTH county and tract layers must update:

```javascript
function switchLayer(layer) {
    currentLayer = layer;
    const expression = getLayerFillExpression(layer);

    // Animate county layer (existing logic)
    animateLayerTransition('counties-fill', expression);

    // Also update tract layer (same expression, same property names)
    if (map.getLayer('tracts-fill')) {
        animateLayerTransition('tracts-fill', expression);
    }
}
```

This works because **tract GeoJSON properties use the same field names** (`synthesis_grouping`, `directional_class`, `confidence_class`) as counties.

### 13.5 Backend — Tract GeoJSON Generation

Extend `src/export/geojson_export.py`:

```python
def export_geojson(level="county"):
    """Export GeoJSON at county or tract level."""
    if level == "county":
        return export_county_geojson()   # existing logic
    elif level == "tract":
        return export_tract_geojson()    # new
    else:
        raise ValueError(f"Unsupported level: {level}")

def export_tract_geojson():
    """
    Generate tract-level GeoJSON with scores and classifications.

    Data flow:
    1. Fetch tract boundaries (Census TIGER/Line via pygris or cached file)
    2. Join with layer1_economic_opportunity_tract (employment scores)
    3. Join with layer2_mobility_accessibility_tract (mobility scores)
    4. Join with tract-level synthesis (when available)
    5. Merge into FeatureCollection with same property schema as counties
    """
    ...
```

**Tract GeoJSON properties** (mirrors county schema):

```json
{
    "tract_geoid": "24031402100",
    "fips_code": "24031",
    "county_name": "Montgomery",
    "synthesis_grouping": "conditional_growth",
    "directional_class": "improving",
    "confidence_class": "conditional",
    "composite_score": 0.6432,
    "employment_gravity_score": 0.7123,
    "mobility_optionality_score": 0.6891,
    "school_trajectory_score": null,
    "housing_elasticity_score": null,
    "demographic_momentum_score": null,
    "risk_drag_score": null,
    "data_year": 2025,
    "primary_strengths": ["employment_gravity", "mobility_optionality"],
    "primary_weaknesses": [],
    "key_trends": ["Strong job accessibility from this tract"]
}
```

Layers without tract-level data show `null` scores — the frontend handles this gracefully (already shows "N/A" for null).

### 13.6 Backend — New API Endpoints

```python
# src/api/routes.py — new endpoints

@router.get("/layers/tracts/latest")
async def get_tract_geojson():
    """Serve tract-level GeoJSON (latest version)."""
    # Serve from file (exported by geojson_export.py)
    # Gzip-compressed, ~1-3 MB over wire
    ...

@router.get("/areas/{geoid}")
async def get_area_detail(geoid: str):
    """
    Unified endpoint — works for both county (5-digit) and tract (11-digit).

    Detects resolution from FIPS code length:
    - 5 digits → county detail (existing logic)
    - 11 digits → tract detail (new logic)
    """
    if len(geoid) == 5:
        return await get_county_detail(geoid)
    elif len(geoid) == 11:
        return await get_tract_detail(geoid)
    else:
        raise HTTPException(400, "Invalid GEOID length")
```

### 13.7 Tract-Level Scoring Pipeline

**What exists today:**

| Layer | Tract-level data? | Table |
|-------|-------------------|-------|
| 1. Employment Gravity | **Yes** | `layer1_economic_opportunity_tract` |
| 2. Mobility Optionality | **Yes** | `layer2_mobility_accessibility_tract` |
| 3. School Trajectory | No (county only) | — |
| 4. Housing Elasticity | No (county only) | — |
| 5. Demographic Momentum | No (county only) | — |
| 6. Risk Drag | No (county only) | — |
| Synthesis/Classification | No (county only) | — |

**Strategy for missing layers:**

For the initial tract-level release, we have three options for layers 3-6:

1. **Inherit from parent county** (fastest) — tract gets its county's score for missing layers. Simple but loses within-county variation.

2. **Proxy from available data** (moderate) — use ACS tract-level variables to estimate:
   - Layer 3 (Schools): NCES school proximity + ACS education attainment at tract level
   - Layer 4 (Housing): ACS median home value, rent burden, vacancy at tract level
   - Layer 5 (Demographics): ACS age structure, diversity, population change at tract level
   - Layer 6 (Risk): FEMA NFHL flood zones intersect with tracts; EJScreen has tract-level data

3. **Full tract-level pipeline** (best but slowest) — extend each layer's ingestion script to compute at tract resolution, same methodology as county but disaggregated.

**Recommended rollout:**
- V1: Layers 1-2 at tract level (already done), layers 3-6 inherited from county
- V2: Proxy layers 4-6 from ACS tract data (readily available)
- V3: Full tract-level pipeline for all 6 layers

**Tract-level synthesis classification:**

```python
def classify_tract(tract_scores: dict, parent_county_scores: dict) -> dict:
    """
    Classify a tract using same thresholds as county classification.

    For layers with tract-level scores, use those.
    For layers without, fall back to parent county scores.
    Confidence level degrades when using county fallback.
    """
    merged_scores = {}
    county_fallback_count = 0

    for layer_key in ALL_LAYERS:
        if tract_scores.get(layer_key) is not None:
            merged_scores[layer_key] = tract_scores[layer_key]
        else:
            merged_scores[layer_key] = parent_county_scores.get(layer_key)
            county_fallback_count += 1

    classification = apply_classification_rules(merged_scores)

    # Degrade confidence when relying on county-level data
    if county_fallback_count >= 3:
        classification['confidence_class'] = 'fragile'
    elif county_fallback_count >= 1:
        if classification['confidence_class'] == 'strong':
            classification['confidence_class'] = 'conditional'

    return classification
```

### 13.8 AI Chat Integration with Multi-Resolution

The chat context expands to understand resolution:

```javascript
// Updated chat.js context builder
function getChatContext() {
    const resolution = getActiveResolution();
    return {
        resolution: resolution,                           // "county" or "tract"
        selected_county: currentFipsCode,                 // always present
        selected_tract: resolution === 'tract' ? currentTractGeoid : null,
        county_name: currentCountySummary?.county_name,
        active_layer: currentLayer,
        layer_scores: resolution === 'tract'
            ? currentTractSummary?.layer_scores
            : currentCountySummary?.layer_scores,
        map_center: map.getCenter(),
        map_zoom: map.getZoom()
    };
}
```

**New AI tool function:**

```python
{
    "type": "function",
    "function": {
        "name": "get_tract_scores",
        "description": "Get scores for a specific census tract within a county. "
                       "Use when the user is zoomed in to tract level.",
        "parameters": {
            "type": "object",
            "properties": {
                "tract_geoid": {
                    "type": "string",
                    "description": "11-digit tract FIPS code, e.g. 24031402100"
                },
                "layer_key": {"type": "string"}
            },
            "required": ["tract_geoid"]
        }
    }
},
{
    "type": "function",
    "function": {
        "name": "compare_tracts",
        "description": "Compare two census tracts, optionally across specific layers",
        "parameters": {
            "type": "object",
            "properties": {
                "tract_a": {"type": "string"},
                "tract_b": {"type": "string"}
            },
            "required": ["tract_a", "tract_b"]
        }
    }
},
{
    "type": "function",
    "function": {
        "name": "navigate_map",
        "description": "Change the map view — zoom in to tract level, zoom out to county, "
                       "or fly to a specific area. Returns a map_action.",
        "parameters": {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["zoom_to_tract", "zoom_to_county", "fly_to"]
                },
                "geoid": {"type": "string"},
                "center": {
                    "type": "object",
                    "properties": {"lng": {"type": "number"}, "lat": {"type": "number"}}
                }
            },
            "required": ["action"]
        }
    }
}
```

**Updated system prompt addition:**

```
## Map Resolution
The user is currently viewing at {resolution} level (zoom {zoom}).
{if resolution == "tract":}
  Selected tract: {tract_geoid} in {county_name} County
  Tract scores: {tract_scores}
  Note: Some layers may show county-level fallback data (marked as estimated).
{else:}
  Selected county: {county_name} ({fips_code})
  You can suggest the user zoom in for tract-level detail if they ask about
  specific neighborhoods or small areas.
{endif}
```

**Example AI interactions at tract level:**

```
User (zoomed to tract 24031402100):
  "How does this tract compare to the one next to it for job access?"

AI calls: compare_tracts("24031402100", "24031402200")
AI responds: "Tract 4021 has an employment gravity score of 0.71, while
  neighboring tract 4022 scores 0.68. Both benefit from proximity to
  the Bethesda Metro station, but 4021 has slightly better access to
  the I-270 corridor job centers..."

---

User (zoomed to county level):
  "Show me which tracts in Montgomery have the weakest school access"

AI calls: navigate_map("zoom_to_county", geoid="24031")
AI responds: "Let me zoom into Montgomery County so we can see tract-level
  detail. [map zooms in] The tracts with the lowest school trajectory
  scores are in the Poolesville and upper Clarksburg areas..."
  map_actions: [{type: "fly_to", center: {lng: -77.4, lat: 39.2}, zoom: 10}]
```

### 13.9 Performance Considerations

| Concern | Mitigation |
|---------|------------|
| **10 MB tract GeoJSON download** | Lazy-load only when zoom approaches threshold (zoom ≥ 7). Gzip brings to ~2 MB. Cache in `localStorage` with version key. |
| **Client-side tiling of 1,295 polygons** | Set `maxzoom: 12` on source to limit tile generation. Well within geojson-vt performance. |
| **Dual layer paint recalculation** | Mapbox evaluates expressions per-feature per-frame only for visible zoom range. Non-visible layers (outside min/maxzoom) are skipped. |
| **Mobile memory** | Tract GeoJSON adds ~10 MB to heap. Test on low-end devices. Consider on-demand loading or vector tiles for mobile. |
| **Future: more states** | If expanding beyond MD, switch to **vector tiles** via tippecanoe. GeoJSON approach works for single-state scale. |

**Lazy loading pattern:**

```javascript
let tractDataLoaded = false;

map.on('zoom', () => {
    if (!tractDataLoaded && map.getZoom() >= TRACT_MIN_ZOOM - 1) {
        tractDataLoaded = true;
        loadTractData().then(data => {
            if (data) addTractLayers(data);
        });
    }
});
```

### 13.10 Data Acquisition — Tract Boundaries

**Recommended approach:**

```bash
# Option A: Download from Census (500k cartographic, pre-simplified)
curl -o cb_2023_24_tract_500k.zip \
  "https://www2.census.gov/geo/tiger/GENZ2023/shp/cb_2023_24_tract_500k.zip"
unzip cb_2023_24_tract_500k.zip
ogr2ogr -f GeoJSON md_tracts_raw.geojson cb_2023_24_tract_500k.shp

# Option B: Python with pygris (already a dependency)
python -c "
import pygris
tracts = pygris.tracts(state='MD', year=2023, cb=True)
tracts.to_file('md_tracts_raw.geojson', driver='GeoJSON')
"

# Simplify for web (reduce ~40% file size)
mapshaper md_tracts_raw.geojson \
  -simplify 50% keep-shapes \
  -o precision=0.00001 md_tracts_simplified.geojson
```

**Integration into export pipeline** (`geojson_export.py`):

```python
def fetch_maryland_tract_boundaries():
    """Fetch tract boundaries, same pattern as county boundaries."""
    if settings.DATA_BACKEND == "databricks":
        # Load from Delta table (geometry_geojson column)
        ...
    else:
        # pygris with caching
        import pygris
        tracts = pygris.tracts(state="MD", year=2023, cb=True)
        return tracts
```

### 13.11 Revised Phase Summary

```
Phase 1: Chat UI + Q&A ──────────── county-level context only
Phase 2: Spatial Queries ─────────── POI markers on map
Phase 3: Policy Intelligence ─────── CIP/document context
Phase 4: Tract-Level Zoom ───────── dual-resolution map + tract scores
   4a: Tract boundaries + layers 1-2 scores ← quickest win
   4b: Layers 3-6 proxy from ACS tract data
   4c: Full tract-level pipeline for all layers
Phase 5: Polish + Production ─────── caching, vector tiles, mobile
```

Phases 1-3 (chat) and Phase 4a (tract boundaries) can proceed **in parallel** since they touch different parts of the codebase.
