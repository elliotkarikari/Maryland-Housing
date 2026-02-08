"""
Ask Atlas — Chat API endpoint.

POST /api/v1/chat   (uses OpenAI Responses API)
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional

from config.settings import get_settings
from src.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()

chat_router = APIRouter(tags=["chat"])


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=1000)
    context: Dict[str, Any] = Field(default_factory=dict)
    history: Optional[List[ChatMessage]] = Field(default_factory=list)
    previous_response_id: Optional[str] = None


class ChatResponse(BaseModel):
    response: str
    response_id: Optional[str] = None
    model: str = ""
    tokens_input: int = 0
    tokens_output: int = 0
    cost_estimate: float = 0.0


@chat_router.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """Ask Atlas — answer questions about the selected area."""
    if not settings.AI_ENABLED or not settings.OPENAI_API_KEY:
        raise HTTPException(
            status_code=503,
            detail="AI chat is not enabled. Set AI_ENABLED=true and OPENAI_API_KEY in .env",
        )

    try:
        from src.ai.chat.service import ChatService

        svc = ChatService()
        history = [{"role": m.role, "content": m.content} for m in (request.history or [])]
        result = svc.reply(
            user_message=request.message,
            context=request.context,
            history=history,
            previous_response_id=request.previous_response_id,
        )
        return ChatResponse(
            response=result["response"],
            response_id=result.get("response_id"),
            model=result.get("model", ""),
            tokens_input=result.get("tokens_input", 0),
            tokens_output=result.get("tokens_output", 0),
            cost_estimate=result.get("cost_estimate", 0.0),
        )
    except Exception as exc:
        logger.error(f"Chat error: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))
