"""
Base AI Provider Interface

All AI providers must implement this interface to ensure consistent
provenance tracking, cost management, and output validation.
"""

from abc import ABC, abstractmethod
from typing import Type, Dict, Any
from pydantic import BaseModel


class AIProvider(ABC):
    """
    Abstract base class for AI providers.

    Ensures all providers support:
    - Structured output extraction
    - Token/cost tracking
    - Prompt versioning
    - Graceful error handling
    """

    @abstractmethod
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
        Extract structured data from document text.

        Args:
            document_text: Raw text to process
            task_name: Extraction task identifier (e.g., 'cip_capital_commitment')
            schema: Pydantic model class for output validation
            prompt_version: Prompt version ID for tracking
            system_prompt: Optional system prompt override
            max_tokens: Maximum output tokens

        Returns:
            dict with:
                - extracted_facts: Validated data (instance of schema)
                - confidence: Model-reported confidence (0-1) if available
                - model: Model name/version used
                - tokens_input: Input token count
                - tokens_output: Output token count
                - cost_estimate: Cost in USD
                - validation_status: 'valid', 'failed', or 'manual_review'
                - error_message: Error description if validation failed
                - raw_output: Raw model response (for debugging)

        Raises:
            AIProviderError: If extraction fails unrecoverably
        """
        pass

    @abstractmethod
    def estimate_cost(
        self,
        input_tokens: int,
        output_tokens: int
    ) -> float:
        """
        Estimate cost for token usage.

        Args:
            input_tokens: Number of input tokens
            output_tokens: Number of output tokens

        Returns:
            Estimated cost in USD
        """
        pass

    @abstractmethod
    def count_tokens(self, text: str) -> int:
        """
        Count tokens in text (provider-specific tokenization).

        Args:
            text: Text to tokenize

        Returns:
            Token count
        """
        pass


class AIProviderError(Exception):
    """Base exception for AI provider errors"""
    pass


class ValidationError(AIProviderError):
    """Raised when output validation fails"""
    pass


class CostLimitExceededError(AIProviderError):
    """Raised when cost limit is exceeded"""
    pass


class RateLimitError(AIProviderError):
    """Raised when API rate limit is hit"""
    pass
