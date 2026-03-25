"""
MULTI-AGENT API SERVER
OpenAI-compatible API exposing three specialized agents:
  - agent-drawings  : DXF, STP, IFC, SVG, STL, technical PDFs
  - agent-financial : Excel, CSV, financial PDFs
  - agent-documents : PDF, PPTX, Word documents

Security:
  - No public API docs (openapi_url=None)
  - Binds to 0.0.0.0 so Open WebUI (Docker) can reach it
  - Port 8000 should NOT be exposed externally (firewall rule)
  - All data stays local — no external API calls
"""
import sys, os, logging
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
    docs_url=None,       # disable Swagger UI
    redoc_url=None,      # disable ReDoc
    openapi_url=None     # disable OpenAPI schema endpoint
)

# Load all three agents at startup
import drawings_agent  as drawings
import financial_agent as financial
import documents_agent as documents

AGENTS = {
    "agent-drawings":  drawings,
    "agent-financial": financial,
    "agent-documents": documents,
}

AGENT_DESCRIPTIONS = {
    "agent-drawings":  "Technical drawings: DXF, STP, IFC, SVG, STL — geometry, layers, materials",
    "agent-financial": "Financial documents: Excel, CSV, PDF — balances, invoices, budgets",
    "agent-documents": "Company documents: PDF, PPTX, Word — reports, presentations, manuals",
}

# ── Pydantic models ──────────────────────────────────────────

class Message(BaseModel):
    role: str
    content: str

class ChatRequest(BaseModel):
    model: str
    messages: List[Message]
    stream: bool = False

# ── Endpoints ────────────────────────────────────────────────

@app.get("/health")
def health():
    """Health check — returns indexed chunk counts per agent."""
    stats = {}
    for name, agent in AGENTS.items():
        try:
            stats[name] = agent.collection.count()
        except Exception:
            stats[name] = "error"
    return {"status": "ok", "indexed_chunks": stats}

@app.get("/v1/models")
def list_models():
    """List available agents as OpenAI-compatible models."""
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
def chat(req: ChatRequest):
    """
    Main chat endpoint.
    Routes the request to the correct agent based on the model name.
    Returns OpenAI-compatible response format.
    """
    if not req.messages:
        raise HTTPException(status_code=400, detail="No messages provided")

    agent = AGENTS.get(req.model)
    if not agent:
        raise HTTPException(
            status_code=404,
            detail=f"Agent '{req.model}' not found. "
                   f"Available: {list(AGENTS.keys())}"
        )

    question = req.messages[-1].content
    context  = agent.search(question)
    response = agent.answer(question, context)

    logger.info(f"[{req.model}] Q: {question[:80]}...")

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
    logger.info("Company AI Agent Server starting...")
    for name, agent in AGENTS.items():
        logger.info(f"  {name}: {agent.collection.count()} chunks indexed")
    logger.info("=" * 50)
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=AGENT_PORT,
        access_log=True
    )