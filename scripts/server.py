"""
MULTI-AGENT API SERVER v2
OpenAI-compatible API — tre agenti specializzati:
  - agent-drawings  : DXF, STP, IFC, SVG, STL, PDF tecnici
  - agent-financial : Excel, CSV, PDF finanziari
  - agent-documents : PDF, PPTX, Word, Markdown

Miglioramenti v2:
  - SSE streaming nativo (Open WebUI riceve i token in tempo reale)
  - ThreadPoolExecutor: le chiamate bloccanti all'LLM non bloccano il server
  - Conversation history: tutta la storia viene passata all'agente come contesto
  - CORS corretto per Docker / Open WebUI
  - Caricamento agenti resiliente: un agente rotto non abbatte il server
  - Validazione input: lunghezza messaggi, ruolo, profondità storia
  - Error handling dettagliato con log strutturato per ogni richiesta
  - Health endpoint esteso con stato per agente
  - Request ID univoco (tracciamento nei log)

Sicurezza:
  - Nessuna doc pubblica (openapi_url=None)
  - Bind su 0.0.0.0 — porta 8000 NON esposta esternamente (firewall)
  - Tutto locale — zero chiamate API esterne
"""

import sys, os, logging, uuid, asyncio, time, json
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import AGENT_PORT

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel, validator
from typing import List, Optional

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("server")

# ── App ────────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Company AI Agent Server",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

# CORS — indispensabile per Open WebUI (Docker su porta 3000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # il firewall blocca l'accesso esterno
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# Thread pool per chiamate bloccanti all'LLM
# 4 thread = 4 richieste LLM parallele senza bloccare l'event loop FastAPI
_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="llm-worker")

# ── Limiti di sicurezza / performance ─────────────────────────────────────────
MAX_MSG_CHARS    = 8_000    # max caratteri per singolo messaggio utente
MAX_HISTORY_MSGS = 20       # max messaggi di history da includere nel contesto
MAX_HISTORY_CHARS = 10_000  # max caratteri totali per la history

# ── Descrizioni agenti ─────────────────────────────────────────────────────────
AGENT_DESCRIPTIONS = {
    "agent-drawings":  "Disegni tecnici: DXF, STP, IFC, SVG, STL — geometria, layer, materiali",
    "agent-financial": "Documenti finanziari: Excel, CSV, PDF — bilanci, fatture, budget",
    "agent-documents": "Documenti aziendali: PDF, PPTX, Word — report, presentazioni, manuali",
}

# ── Caricamento agenti — resiliente ───────────────────────────────────────────
# Se un agente fallisce, gli altri continuano a funzionare normalmente.
AGENTS: dict = {}

def _load_agents():
    import importlib
    specs = [
        ("agent-drawings",  "drawings_agent"),
        ("agent-financial", "financial_agent"),
        ("agent-documents", "documents_agent"),
    ]
    for agent_id, module_name in specs:
        try:
            mod   = importlib.import_module(module_name)
            count = mod.collection.count()
            AGENTS[agent_id] = mod
            logger.info(f"  ✓ {agent_id}: {count:,} chunk indicizzati")
        except Exception as exc:
            logger.error(f"  ✗ {agent_id}: ERRORE caricamento — {exc}", exc_info=True)

_load_agents()

# ── Modelli Pydantic ───────────────────────────────────────────────────────────

class Message(BaseModel):
    role: str
    content: str

    @validator("role")
    def check_role(cls, v):
        if v not in ("user", "assistant", "system"):
            raise ValueError(f"Ruolo non valido: '{v}' — usa user/assistant/system")
        return v

    @validator("content")
    def check_and_truncate_content(cls, v):
        if len(v) > MAX_MSG_CHARS:
            logger.warning(f"Messaggio troncato: {len(v):,} → {MAX_MSG_CHARS:,} chars")
            return v[:MAX_MSG_CHARS]
        return v


class ChatRequest(BaseModel):
    model: str
    messages: List[Message]
    stream: bool = False
    temperature: Optional[float] = None  # accettato ma ignorato (gestito dall'agente)
    max_tokens:  Optional[int]   = None  # idem

# ── History builder ────────────────────────────────────────────────────────────

def extract_question_and_history(messages: List[Message]) -> tuple[str, str]:
    """
    Separa la domanda corrente dalla history della conversazione.

    Returns:
        question : testo dell'ultimo messaggio utente
        history  : stringa formattata dei turni precedenti (troncata ai limiti)
    """
    # Ultima domanda = ultimo messaggio utente
    question = ""
    for msg in reversed(messages):
        if msg.role == "user":
            question = msg.content
            break

    if not question:
        return "", ""

    # History = tutto tranne l'ultimo messaggio utente
    prior = messages[:-1] if messages and messages[-1].role == "user" else messages[:]

    # Teniamo solo gli ultimi MAX_HISTORY_MSGS messaggi
    recent = prior[-MAX_HISTORY_MSGS:]

    parts       = []
    total_chars = 0
    for msg in recent:
        label = "Utente" if msg.role == "user" else "Assistente"
        line  = f"[{label}]: {msg.content}"

        if total_chars + len(line) > MAX_HISTORY_CHARS:
            break  # stop se superiamo il budget di caratteri

        parts.append(line)
        total_chars += len(line)

    history = "\n".join(parts)
    return question, history


def build_full_question(question: str, history: str) -> str:
    """Combina history + domanda attuale in un'unica stringa coerente."""
    if history:
        return (
            "=== STORIA CONVERSAZIONE ===\n"
            f"{history}\n\n"
            "=== DOMANDA ATTUALE ===\n"
            f"{question}"
        )
    return question

# ── SSE helpers ────────────────────────────────────────────────────────────────

def _sse_chunk(content: str, model: str, req_id: str, finish: bool = False) -> str:
    """Formatta un singolo chunk SSE nel formato OpenAI chat.completion.chunk."""
    payload = {
        "id":      req_id,
        "object":  "chat.completion.chunk",
        "model":   model,
        "choices": [{
            "delta":         {"content": content} if not finish else {},
            "finish_reason": "stop" if finish else None,
            "index":         0,
        }],
    }
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"


async def _run_agent(agent, full_question: str, question: str) -> tuple[str, str]:
    """
    Esegue search() e answer() dell'agente in un thread del pool.
    Non blocca l'event loop FastAPI durante la lunga chiamata all'LLM.

    Returns:
        (context, response) — entrambe stringhe
    """
    loop = asyncio.get_event_loop()

    # search usa la domanda "pura" (senza history) per la similarità vettoriale
    context  = await loop.run_in_executor(_EXECUTOR, agent.search, question)

    # answer riceve la domanda completa con history per il contesto conversazionale
    response = await loop.run_in_executor(_EXECUTOR, agent.answer, full_question, context)

    return context, response

# ── Endpoint: health ───────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    """
    Health check esteso.
    Restituisce: stato per agente, chunk indicizzati, thread disponibili.
    """
    agent_stats = {}
    for name, agent in AGENTS.items():
        try:
            agent_stats[name] = {
                "status": "ok",
                "chunks": agent.collection.count(),
            }
        except Exception as exc:
            agent_stats[name] = {
                "status": "error",
                "detail": str(exc),
            }

    unloaded = [k for k in AGENT_DESCRIPTIONS if k not in AGENTS]

    overall = "ok" if not unloaded and all(
        v["status"] == "ok" for v in agent_stats.values()
    ) else "degraded"

    return {
        "status":           overall,
        "agents":           agent_stats,
        "unloaded_agents":  unloaded,
        "executor_threads": _EXECUTOR._max_workers,
    }

# ── Endpoint: models ───────────────────────────────────────────────────────────

@app.get("/v1/models")
async def list_models():
    """Elenca gli agenti disponibili nel formato OpenAI-compatibile."""
    return {
        "object": "list",
        "data": [
            {
                "id":          name,
                "object":      "model",
                "description": AGENT_DESCRIPTIONS.get(name, ""),
                "status":      "ready" if name in AGENTS else "unavailable",
            }
            for name in AGENT_DESCRIPTIONS
        ],
    }

# ── Endpoint: chat/completions ─────────────────────────────────────────────────

@app.post("/v1/chat/completions")
async def chat(req: ChatRequest, request: Request):
    """
    Endpoint principale — OpenAI-compatibile.

    Routing:   sceglie l'agente dal campo `model`
    History:   passa tutta la conversazione come contesto all'agente
    Streaming: SSE word-by-word se stream=True (default di Open WebUI)
    Blocking:  risposta JSON completa se stream=False
    """
    req_id  = f"chatcmpl-{uuid.uuid4().hex[:16]}"
    t_start = time.perf_counter()

    # ── Validazione ────────────────────────────────────────────────────────────
    if not req.messages:
        raise HTTPException(status_code=400, detail="Nessun messaggio fornito")

    agent = AGENTS.get(req.model)
    if not agent:
        known     = list(AGENT_DESCRIPTIONS.keys())
        available = list(AGENTS.keys())
        detail    = f"Agente '{req.model}' non trovato."
        if req.model in known and req.model not in AGENTS:
            detail += " L'agente è noto ma non è stato caricato correttamente all'avvio (controlla i log)."
        detail += f" Agenti disponibili: {available}"
        raise HTTPException(status_code=404, detail=detail)

    # ── Estrazione domanda + history ──────────────────────────────────────────
    question, history = extract_question_and_history(req.messages)

    if not question:
        raise HTTPException(status_code=400, detail="Nessun messaggio utente trovato")

    full_question = build_full_question(question, history)

    # ── STREAMING ─────────────────────────────────────────────────────────────
    if req.stream:
        async def event_generator():
            try:
                _, response = await _run_agent(agent, full_question, question)

                elapsed = time.perf_counter() - t_start
                logger.info(
                    f"[{req_id[:12]}] [{req.model}] {elapsed:.1f}s | "
                    f"history={len(history)} chars | "
                    f"Q: {question[:60]}..."
                )

                # Stream parola per parola — Open WebUI mostra i token in arrivo
                words = response.split(" ")
                for i, word in enumerate(words):
                    chunk = word if i == 0 else f" {word}"
                    yield _sse_chunk(chunk, req.model, req_id)
                    # Yield all'event loop ogni 15 parole per mantenere la reattività
                    if i % 15 == 0:
                        await asyncio.sleep(0)

                # Chunk di chiusura + segnale DONE
                yield _sse_chunk("", req.model, req_id, finish=True)
                yield "data: [DONE]\n\n"

            except Exception as exc:
                logger.error(
                    f"[{req_id[:12]}] [{req.model}] ERRORE streaming: {exc}",
                    exc_info=True,
                )
                # Segnala l'errore via SSE prima di chiudere lo stream
                err_payload = json.dumps({
                    "error": {"message": str(exc), "type": "agent_error"}
                }, ensure_ascii=False)
                yield f"data: {err_payload}\n\n"
                yield "data: [DONE]\n\n"

        return StreamingResponse(
            event_generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control":     "no-cache",
                "X-Accel-Buffering": "no",      # disabilita buffering nginx
                "Connection":        "keep-alive",
                "X-Request-Id":      req_id,
            },
        )

    # ── NON-STREAMING ─────────────────────────────────────────────────────────
    try:
        _, response = await _run_agent(agent, full_question, question)

        elapsed = time.perf_counter() - t_start
        logger.info(
            f"[{req_id[:12]}] [{req.model}] {elapsed:.1f}s | "
            f"history={len(history)} chars | "
            f"Q: {question[:60]}..."
        )

        return {
            "id":      req_id,
            "object":  "chat.completion",
            "model":   req.model,
            "choices": [{
                "message":       {"role": "assistant", "content": response},
                "finish_reason": "stop",
                "index":         0,
            }],
            "usage": {
                "prompt_tokens":     len(full_question.split()),
                "completion_tokens": len(response.split()),
                "total_tokens":      len(full_question.split()) + len(response.split()),
            },
        }

    except Exception as exc:
        logger.error(
            f"[{req_id[:12]}] [{req.model}] ERRORE: {exc}",
            exc_info=True,
        )
        raise HTTPException(
            status_code=500,
            detail=f"Errore agente '{req.model}': {exc}",
        )

# ── Handler globale per eccezioni non gestite ──────────────────────────────────

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Eccezione non gestita su {request.url}: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"error": {"message": str(exc), "type": "internal_error"}},
    )

# ── Avvio ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    logger.info("=" * 60)
    logger.info("Company AI Agent Server v2 — avvio")
    logger.info(f"  Agenti caricati : {list(AGENTS.keys())}")
    if len(AGENTS) < len(AGENT_DESCRIPTIONS):
        missing = [k for k in AGENT_DESCRIPTIONS if k not in AGENTS]
        logger.warning(f"  Agenti mancanti : {missing}")
    logger.info(f"  Executor threads: {_EXECUTOR._max_workers}")
    logger.info(f"  Porta           : {AGENT_PORT}")
    logger.info("=" * 60)

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=AGENT_PORT,
        access_log=True,
        loop="asyncio",
    )
