"""Chat API routes for Atlas."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from src.ai.providers.base import AIProviderError
from src.api.services.chat_service import ChatService
from src.utils.logging import get_logger

router = APIRouter()
logger = get_logger(__name__)


class ChatTurn(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=500)
    context: Optional[Dict[str, Any]] = None
    history: List[ChatTurn] = Field(default_factory=list)
    previous_response_id: Optional[str] = None


class ChatTokens(BaseModel):
    input: int
    output: int
    total: int


class ChatResponse(BaseModel):
    response: str
    response_id: Optional[str] = None
    model: str
    tokens: ChatTokens
    cost: Optional[float] = None


@router.post("/chat", response_model=ChatResponse)
async def post_chat(request: ChatRequest) -> ChatResponse:
    """Handle Atlas chat prompts and return grounded responses."""
    try:
        service = ChatService()
        result = service.reply(
            message=request.message,
            context=request.context,
            history=[turn.model_dump() for turn in request.history],
            previous_response_id=request.previous_response_id,
        )

        return ChatResponse(
            response=result.get("response", ""),
            response_id=result.get("response_id"),
            model=result.get("model", "gpt-5.1-mini"),
            tokens=ChatTokens(
                input=int(result.get("tokens_input", 0) or 0),
                output=int(result.get("tokens_output", 0) or 0),
                total=int(result.get("tokens_total", 0) or 0),
            ),
            cost=result.get("cost_estimate"),
        )

    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except AIProviderError as exc:
        logger.error(f"Chat provider failure: {exc}")
        raise HTTPException(status_code=503, detail=f"Chat API call failed: {exc}") from exc
    except Exception as exc:
        logger.error(f"Unexpected chat error: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail="Chat service unavailable") from exc
