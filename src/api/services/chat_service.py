"""Chat service for Atlas conversational responses."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from src.ai.providers.base import AIProviderError
from src.ai.providers.openai_provider import get_openai_provider


class ChatService:
    """Builds grounded prompts and forwards chat requests to OpenAI."""

    def __init__(self) -> None:
        self.provider = get_openai_provider()

    def reply(
        self,
        message: str,
        context: Optional[Dict[str, Any]] = None,
        history: Optional[List[Dict[str, Any]]] = None,
        previous_response_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        prompt = (message or "").strip()
        if not prompt:
            raise ValueError("Message cannot be empty")

        trimmed_history = self._trim_history(history or [], max_turns=10)
        instructions = self._build_instructions(context or {})
        conversation = self._build_conversation(trimmed_history, prompt)

        try:
            return self.provider.chat(
                instructions=instructions,
                conversation_text=conversation,
                previous_response_id=previous_response_id,
                model="gpt-5.1-mini",
            )
        except AIProviderError:
            raise

    def _trim_history(self, history: List[Dict[str, Any]], max_turns: int) -> List[Dict[str, str]]:
        valid_turns: List[Dict[str, str]] = []

        for turn in history:
            role = str(turn.get("role", "")).strip().lower()
            content = str(turn.get("content", "")).strip()
            if role not in {"user", "assistant"}:
                continue
            if not content:
                continue
            valid_turns.append({"role": role, "content": content})

        return valid_turns[-max_turns:]

    def _build_instructions(self, context: Dict[str, Any]) -> str:
        base = (
            "You are Atlas, a concise policy and housing intelligence assistant for Maryland counties. "
            "Use plain language and tie claims directly to provided county context. "
            "If the question is outside the provided data, say so explicitly and offer a next-best interpretation. "
            "Do not fabricate metrics, sources, or years."
        )

        if not context:
            return base

        county_name = context.get("county_name")
        data_year = context.get("data_year")
        directional_class = context.get("directional_class")
        signal_label = context.get("signal_label")
        composite_score = context.get("composite_score")
        layer_scores = context.get("layer_scores") or {}
        strengths = context.get("primary_strengths") or []
        weaknesses = context.get("primary_weaknesses") or []
        trends = context.get("key_trends") or []

        lines = [base, "", "County context:"]

        if county_name:
            lines.append(f"- County: {county_name}")
        if data_year is not None:
            lines.append(f"- Data year: {data_year}")
        if directional_class:
            lines.append(f"- Trajectory: {directional_class}")
        if signal_label:
            lines.append(f"- County signal: {signal_label}")
        if composite_score is not None:
            lines.append(f"- Overall Signal Score: {composite_score}")

        if layer_scores:
            lines.append("- Layer scores:")
            for key, value in layer_scores.items():
                lines.append(f"  - {key}: {value}")

        if strengths:
            lines.append(f"- Primary strengths: {', '.join(str(s) for s in strengths)}")
        if weaknesses:
            lines.append(f"- Primary weaknesses: {', '.join(str(w) for w in weaknesses)}")
        if trends:
            lines.append(f"- Key trends: {', '.join(str(t) for t in trends)}")

        lines.append("")
        lines.append(
            "When useful, structure answers as: current signal, family/housing implications, and what to watch next."
        )

        return "\n".join(lines)

    def _build_conversation(self, history: List[Dict[str, str]], message: str) -> str:
        lines: List[str] = []
        for turn in history:
            prefix = "User" if turn["role"] == "user" else "Atlas"
            lines.append(f"{prefix}: {turn['content']}")

        lines.append(f"User: {message}")
        return "\n".join(lines)
