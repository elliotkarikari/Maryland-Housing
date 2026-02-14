"""
OpenAI Provider Implementation

Uses OpenAI API for structured document extraction with:
- JSON mode for reliable structured output
- Token counting and cost estimation
- Rate limit handling
- Automatic retry logic
"""

from typing import Type, Dict, Any, Optional, List
from pydantic import BaseModel, ValidationError as PydanticValidationError
import json
import time

from src.ai.providers.base import (
    AIProvider,
    AIProviderError,
    ValidationError,
    CostLimitExceededError,
    RateLimitError
)
from config.settings import get_settings
from src.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


class OpenAIProvider(AIProvider):
    """
    OpenAI API provider for document extraction.

    Uses OpenAI chat models with JSON mode for structured outputs.
    """

    # OpenAI pricing (as of 2026-01, subject to change)
    PRICING = {
        "gpt-5.1-mini": {
            "input": 0.00015 / 1000,  # keep aligned with lightweight mini-tier estimate
            "output": 0.00060 / 1000
        },
        "gpt-4o-mini": {
            "input": 0.00015 / 1000,  # $0.15 per 1M input tokens
            "output": 0.00060 / 1000  # $0.60 per 1M output tokens
        },
        "gpt-4-turbo-preview": {
            "input": 0.01 / 1000,   # $0.01 per 1K input tokens
            "output": 0.03 / 1000   # $0.03 per 1K output tokens
        },
        "gpt-4": {
            "input": 0.03 / 1000,
            "output": 0.06 / 1000
        },
        "gpt-3.5-turbo": {
            "input": 0.001 / 1000,
            "output": 0.002 / 1000
        }
    }

    def __init__(
        self,
        api_key: str = None,
        model: str = None,
        max_retries: int = 3
    ):
        """
        Initialize OpenAI provider.

        Args:
            api_key: OpenAI API key (default: from settings)
            model: Model name (default: from settings)
            max_retries: Maximum retry attempts for rate limits
        """
        self.api_key = api_key or settings.OPENAI_API_KEY
        self.model = model or settings.OPENAI_MODEL
        self.max_retries = max_retries

        if not self.api_key:
            raise AIProviderError("OpenAI API key not configured")

        # Lazy import to avoid dependency if AI not enabled
        try:
            from openai import OpenAI
            self.client = OpenAI(api_key=self.api_key)
        except ImportError:
            raise AIProviderError(
                "OpenAI package not installed. Run: pip install openai"
            )

        logger.info(f"Initialized OpenAI provider with model {self.model}")

    def extract_structured(
        self,
        document_text: str,
        task_name: str,
        schema: Type[BaseModel],
        prompt_version: str,
        system_prompt: str = None,
        max_tokens: int = 4000
    ) -> Dict[str, Any]:
        """
        Extract structured data using OpenAI API.

        Uses JSON mode with schema-guided prompting for reliable extraction.
        """
        logger.info(
            f"OpenAI extraction: task={task_name}, "
            f"prompt_version={prompt_version}, model={self.model}"
        )

        # Build prompt
        if system_prompt is None:
            system_prompt = self._build_default_system_prompt(task_name, schema)

        user_prompt = self._build_user_prompt(document_text, schema)

        # Count input tokens (approximate)
        input_text = system_prompt + user_prompt
        tokens_input = self.count_tokens(input_text)

        logger.debug(f"Estimated input tokens: {tokens_input}")

        # Call OpenAI API with retry logic
        for attempt in range(self.max_retries):
            try:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    response_format={"type": "json_object"},  # Force JSON output
                    max_tokens=max_tokens,
                    temperature=0.1  # Low temperature for factual extraction
                )

                break  # Success

            except Exception as e:
                error_str = str(e)

                # Rate limit handling
                if "rate_limit" in error_str.lower():
                    if attempt < self.max_retries - 1:
                        wait_time = 2 ** attempt  # Exponential backoff
                        logger.warning(
                            f"Rate limit hit, retrying in {wait_time}s "
                            f"(attempt {attempt + 1}/{self.max_retries})"
                        )
                        time.sleep(wait_time)
                        continue
                    else:
                        raise RateLimitError(f"Rate limit exceeded after {self.max_retries} attempts")

                # Other errors
                logger.error(f"OpenAI API error: {error_str}")
                raise AIProviderError(f"OpenAI API call failed: {error_str}")

        # Extract response
        raw_output = response.choices[0].message.content
        tokens_output = response.usage.completion_tokens
        tokens_input_actual = response.usage.prompt_tokens

        logger.debug(
            f"Response received: input_tokens={tokens_input_actual}, "
            f"output_tokens={tokens_output}"
        )

        # Parse JSON
        try:
            output_dict = json.loads(raw_output)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON output: {e}")
            return {
                "extracted_facts": None,
                "confidence": 0.0,
                "model": self.model,
                "tokens_input": tokens_input_actual,
                "tokens_output": tokens_output,
                "cost_estimate": self.estimate_cost(tokens_input_actual, tokens_output),
                "validation_status": "failed",
                "error_message": f"JSON parse error: {str(e)}",
                "raw_output": raw_output
            }

        # Validate against schema
        try:
            validated = schema(**output_dict)
            validation_status = "valid"
            error_message = None

            # Check confidence threshold
            if hasattr(validated, 'confidence') and validated.confidence < 0.6:
                validation_status = "manual_review"
                error_message = f"Low confidence: {validated.confidence}"

        except PydanticValidationError as e:
            logger.warning(f"Validation failed: {e}")
            validated = None
            validation_status = "failed"
            error_message = str(e)

        # Calculate cost
        cost = self.estimate_cost(tokens_input_actual, tokens_output)

        return {
            "extracted_facts": validated,
            "confidence": validated.confidence if validated and hasattr(validated, 'confidence') else None,
            "model": self.model,
            "tokens_input": tokens_input_actual,
            "tokens_output": tokens_output,
            "cost_estimate": cost,
            "validation_status": validation_status,
            "error_message": error_message,
            "raw_output": raw_output
        }

    def chat(
        self,
        instructions: str,
        conversation_text: str,
        previous_response_id: Optional[str] = None,
        model: str = "gpt-5.1-mini",
        max_output_tokens: int = 700
    ) -> Dict[str, Any]:
        """
        Generate a conversational response using the OpenAI Responses API.

        Args:
            instructions: System-style instructions and grounding context
            conversation_text: Conversation history + latest user prompt
            previous_response_id: Optional response id for multi-turn continuation
            model: Chat model name
            max_output_tokens: Maximum output tokens for response

        Returns:
            dict with text, response_id, model, token usage, and cost estimate

        Raises:
            AIProviderError: If chat generation fails
        """
        payload: Dict[str, Any] = {
            "model": model,
            "instructions": instructions,
            "input": conversation_text,
            "max_output_tokens": max_output_tokens
        }

        if previous_response_id:
            payload["previous_response_id"] = previous_response_id

        try:
            response = self.client.responses.create(**payload)
        except Exception as e:
            logger.error(f"OpenAI chat call failed: {e}")
            raise AIProviderError(f"OpenAI chat call failed: {e}") from e

        response_text = self._extract_response_text(response)
        usage = getattr(response, "usage", None)
        input_tokens = int(getattr(usage, "input_tokens", 0) or 0)
        output_tokens = int(getattr(usage, "output_tokens", 0) or 0)
        total_tokens = int(getattr(usage, "total_tokens", input_tokens + output_tokens) or (input_tokens + output_tokens))
        cost = self.estimate_cost(input_tokens, output_tokens, model_name=model)

        return {
            "response": response_text,
            "response_id": getattr(response, "id", None),
            "model": model,
            "tokens_input": input_tokens,
            "tokens_output": output_tokens,
            "tokens_total": total_tokens,
            "cost_estimate": cost
        }

    def _extract_response_text(self, response: Any) -> str:
        """
        Safely extract concatenated text from a Responses API result.
        """
        direct_text = getattr(response, "output_text", None)
        if direct_text:
            return str(direct_text).strip()

        chunks: List[str] = []
        for item in getattr(response, "output", []) or []:
            for content in getattr(item, "content", []) or []:
                text = getattr(content, "text", None)
                if text:
                    chunks.append(str(text))

        merged = "\n".join(chunks).strip()
        return merged or "I could not generate a response right now."

    def _build_default_system_prompt(
        self,
        task_name: str,
        schema: Type[BaseModel]
    ) -> str:
        """Build default system prompt for extraction task"""
        return f"""You are a precise data extraction assistant for government policy documents.

Your task: {task_name}

Extract information EXACTLY as it appears in the document. Do not infer, estimate, or hallucinate values.

Rules:
1. If a field is not present in the document, return null
2. Extract numbers without formatting (e.g., 5000000 not "5M")
3. Be conservative - if unsure, set confidence lower
4. Use only information explicitly stated in the document

Output must be valid JSON matching this schema:
{schema.model_json_schema()}

Return ONLY the JSON object, no other text."""

    def _build_user_prompt(
        self,
        document_text: str,
        schema: Type[BaseModel]
    ) -> str:
        """Build user prompt with document text"""
        # Truncate if too long (keep first + last portions)
        max_chars = 100000  # ~25K tokens

        if len(document_text) > max_chars:
            keep = max_chars // 2
            document_text = (
                document_text[:keep] +
                f"\n\n[... middle section truncated ...]\n\n" +
                document_text[-keep:]
            )

        return f"""Document to extract from:

{document_text}

Extract the requested information and return as JSON."""

    def estimate_cost(
        self,
        input_tokens: int,
        output_tokens: int,
        model_name: Optional[str] = None
    ) -> float:
        """Estimate cost for token usage"""
        price_model = model_name or self.model

        if price_model not in self.PRICING:
            logger.warning(f"Unknown model {price_model}, using gpt-5.1-mini pricing")
            pricing = self.PRICING["gpt-5.1-mini"]
        else:
            pricing = self.PRICING[price_model]

        cost = (
            input_tokens * pricing["input"] +
            output_tokens * pricing["output"]
        )

        return round(cost, 6)

    def count_tokens(self, text: str) -> int:
        """
        Count tokens using tiktoken.

        Approximate count for cost estimation.
        """
        try:
            import tiktoken

            # Get encoding for model
            if "gpt-5" in self.model:
                # gpt-5 tokenizer is compatible with the cl100k family
                encoding = tiktoken.encoding_for_model("gpt-4o-mini")
            elif "gpt-4" in self.model:
                encoding = tiktoken.encoding_for_model("gpt-4")
            elif "gpt-3.5" in self.model:
                encoding = tiktoken.encoding_for_model("gpt-3.5-turbo")
            else:
                encoding = tiktoken.get_encoding("cl100k_base")

            tokens = encoding.encode(text)
            return len(tokens)

        except ImportError:
            # Fallback: rough approximation
            logger.warning("tiktoken not installed, using rough token estimate")
            return len(text) // 4  # Rough approximation: 1 token ≈ 4 chars

        except Exception as e:
            logger.warning(f"Token counting error: {e}, using rough estimate")
            return len(text) // 4


def get_openai_provider() -> OpenAIProvider:
    """
    Factory function to get configured OpenAI provider.

    Returns:
        OpenAIProvider instance

    Raises:
        AIProviderError: If AI not enabled or not configured
    """
    if not settings.AI_ENABLED:
        raise AIProviderError("AI extraction not enabled (AI_ENABLED=false)")

    return OpenAIProvider()
