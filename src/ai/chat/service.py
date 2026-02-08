"""
Ask Atlas — Chat service for the Maryland Housing Atlas.

Builds context-aware conversations grounded in the county/tract data
that the user is currently viewing on the map.
"""

from typing import Dict, Any, List, Optional

from src.ai.providers.openai_provider import OpenAIProvider, get_openai_provider
from src.ai.providers.base import AIProviderError
from src.utils.logging import get_logger

logger = get_logger(__name__)

SYSTEM_PROMPT = """\
You are **Atlas**, the AI assistant for the Maryland Growth & Family Viability Atlas.

## What the Atlas is
The Atlas scores every Maryland county across 6 data layers and classifies each into a growth synthesis grouping.
The 6 layers (each scored 0–1) are:
1. Employment Gravity — job accessibility and sector diversity
2. Mobility Optionality — commute, transit, and transportation access
3. School Trajectory — school enrollment trends, test performance, investment
4. Housing Elasticity — housing supply, affordability, vacancy dynamics
5. Demographic Momentum — population growth, age structure, diversity
6. Risk Drag — flood, environmental, and climate risk burden

**Synthesis groupings**: accelerating_growth, stable_growth, conditional_growth, stable_constrained, at_risk
**Directional trajectory**: improving, stable, declining
**Evidence confidence**: strong, conditional, fragile

## Your role
- Answer questions about the selected area using the data provided in the context.
- Explain what scores mean in plain language.
- Compare areas when asked.
- Discuss what scores imply for families, housing decisions, and community investment.
- Be concise (2-4 sentences typical). Use bullet points for comparisons.
- If the data doesn't contain enough information to answer, say so honestly.
- Never fabricate scores or statistics — only use what's in the context.
- Refer to yourself as "Atlas" if the user asks who you are.

## Formatting
- Use plain text, no markdown headers.
- Bullet points are fine for lists.
- Reference specific scores when relevant (e.g., "Employment Gravity is 0.72").
"""


def _build_context_block(context: Dict[str, Any]) -> str:
    """Build a context block from the map state to inject into the conversation."""
    parts = []

    county = context.get("county_name", "Unknown")
    fips = context.get("fips_code", "")
    parts.append(f"Selected county: {county} (FIPS {fips})")

    grouping = context.get("synthesis_grouping")
    if grouping:
        parts.append(f"Synthesis grouping: {grouping}")

    directional = context.get("directional_class")
    if directional:
        parts.append(f"Directional trajectory: {directional}")

    confidence = context.get("confidence_class")
    if confidence:
        parts.append(f"Evidence confidence: {confidence}")

    composite = context.get("composite_score")
    if composite is not None:
        parts.append(f"Composite score: {composite}")

    layer_scores = context.get("layer_scores")
    if layer_scores:
        lines = []
        for key, val in layer_scores.items():
            display = f"{val:.3f}" if val is not None else "N/A"
            lines.append(f"  - {key}: {display}")
        parts.append("Layer scores:\n" + "\n".join(lines))

    strengths = context.get("primary_strengths")
    if strengths:
        parts.append("Primary strengths: " + ", ".join(strengths))

    weaknesses = context.get("primary_weaknesses")
    if weaknesses:
        parts.append("Primary weaknesses: " + ", ".join(weaknesses))

    trends = context.get("key_trends")
    if trends:
        parts.append("Key trends: " + ", ".join(trends))

    active_layer = context.get("active_layer")
    if active_layer:
        parts.append(f"Active map layer: {active_layer}")

    return "\n".join(parts)


class ChatService:
    """Stateless chat service — each request carries its own history."""

    def __init__(self, provider: Optional[OpenAIProvider] = None):
        self.provider = provider or get_openai_provider()

    def reply(
        self,
        user_message: str,
        context: Dict[str, Any],
        history: Optional[List[Dict[str, str]]] = None,
        previous_response_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generate an AI reply grounded in the current map context.
        Uses the OpenAI Responses API with instructions + input.

        Args:
            user_message: what the user typed
            context: map state (county, scores, layer, etc.)
            history: prior messages [{"role": "user"|"assistant", "content": ...}]
            previous_response_id: ID from prior response for multi-turn

        Returns:
            dict with response, response_id, tokens_input, tokens_output,
            cost_estimate
        """
        context_block = _build_context_block(context)

        # System instructions = base prompt + live context
        instructions = SYSTEM_PROMPT + f"\n\n## Current map context\n{context_block}"

        # Build input messages: history + new user message
        input_messages = []
        if history:
            for msg in history[-10:]:
                input_messages.append({"role": msg["role"], "content": msg["content"]})
        input_messages.append({"role": "user", "content": user_message})

        result = self.provider.chat(
            instructions=instructions,
            input_messages=input_messages,
            previous_response_id=previous_response_id,
        )
        return result
