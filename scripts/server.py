"""
MULTI-AGENT API SERVER (ASYNC VERSION)
OpenAI-compatible API exposing three specialized agents.
Optimized for concurrent requests from multiple users.
"""
import sys, os, logging, asyncio
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import AGENT_PORT
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("server")

app = FastAPI(
    title="Company AI Agent Server",
    docs_url=None,       # Disable Swagger UI for security
    redoc_url=None,      # Disable ReDoc
    openapi_url=None     # Disable OpenAPI schema endpoint
)

# --- Agent Loading ---
# Note: drawings and financial are commented out until they are updated to async
# import drawings_agent  as drawings  
# import financial_agent as financial 
import documents_agent as documents

AGENTS = {
    # "agent-drawings":  drawings,    
    # "agent-financial": financial,   
    "agent-documents": documents,
}

AGENT_DESCRIPTIONS = {
    "agent-drawings":  "Technical drawings: DXF, STP, IFC, SVG, STL — geometry, layers, materials",
    "agent-financial": "Financial documents: Excel, CSV, PDF — balances, invoices, budgets",
    "agent-documents": "Company documents: PDF, PPTX, Word — reports, presentations, manuals",
}

# --- Pydantic models ---

class Message(BaseModel):
    role: str
    content: str

class ChatRequest(BaseModel):
    model: str
    messages: List[Message]
    stream: bool = False

# --- Endpoints ---

@app.get("/health")
async def health():
    """Async health check."""
    stats = {}
    for name, agent in AGENTS.items():
        try:
            # We keep to_thread only for the count() which is a standard DB call
            count = await asyncio.to_thread(agent.collection.count)
            stats[name] = count
        except Exception:
            stats[name] = "error"
    return {"status": "ok", "indexed_chunks": stats}

@app.get("/v1/models")
async def list_models():
    """List agents as OpenAI-compatible models."""
    return {
        "object": "list",
        "data": [
            {
                "id":          name,
                "object":      "model",
                "description": desc
            }
            for name, desc in AGENT_DESCRIPTIONS.items()
        ]
    }

@app.post("/v1/chat/completions")
async def chat(req: ChatRequest):
    """
    Main chat endpoint.
    Handles search and generation for multiple concurrent users.
    """
    if not req.messages:
        raise HTTPException(status_code=400, detail="No messages provided")

    agent = AGENTS.get(req.model)
    if not agent:
        raise HTTPException(
            status_code=404,
            detail=f"Agent '{req.model}' not found. Available: {list(AGENTS.keys())}"
        )

    question = req.messages[-1].content
    logger.info(f"[{req.model}] Query received: {question[:80]}...")

    try:
        # 1. Search (still uses to_thread because ChromaDB is synchronous)
        context = await asyncio.to_thread(agent.search, question)
        
        # 2. Answer (NOW DIRECTLY AWAITED because it is an async function)
        response = await agent.answer(question, context)
        
    except Exception as e:
        logger.error(f"Error during agent processing {req.model}: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Agent Error: {str(e)}")

    logger.info(f"[{req.model}] Response generated successfully.")

    return {
        "id":      f"{req.model}-response",
        "object":  "chat.completion",
        "model":   req.model,
        "choices": [{
            "message":       {"role": "assistant", "content": response},
            "finish_reason": "stop",
            "index":         0
        }]
    }

if __name__ == "__main__":
    import uvicorn
    logger.info("=" * 50)
    logger.info("Company AI Agent Server (Async) starting...")
    logger.info("=" * 50)
    uvicorn.run(
        "server:app", 
        host="0.0.0.0",
        port=AGENT_PORT,
        access_log=True,
        workers=4 
    )