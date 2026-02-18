Urban Pulse (Future Concept)

Status
- Not active in current frontend.
- Hold for a future iteration that combines AI insights + score layer interactions.

Goal
- Provide a thematic, city-scale “pulse” interaction around four urban lenses:
  - Live (green)
  - Work (blue)
  - Learn (yellow)
  - Transit (red)

Proposed Interaction
- Click a themed POI or score-layer element.
- Generate a 5-mile radial buffer around the click point.
- Query nearby POIs in the same theme within the buffer.
- Animate a pulse ripple and dim the rest of the map.
- Fly the camera to a tilted, closer view.
- Show a theme-colored sidebar HUD with:
  - Feature name
  - Count of nearby places in radius
  - AI summary/insight (future)

Data + Tech Notes
- Turf.js for spatial buffer and point-in-polygon.
- Mapbox GL JS for themed POI layers and animation.
- Link the HUD to the existing score-layer click actions.
- AI integration: generate narrative explanations for why a theme is strong/weak in the radius.

Why Later
- Requires curated POI datasets and AI summary hooks to feel meaningful.
- Should be integrated with the existing scoring layer selection flow.
