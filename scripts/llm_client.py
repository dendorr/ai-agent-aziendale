"""
LLM CLIENT — singleton AsyncOpenAI condiviso da tutti gli agenti

Punto unico di configurazione per le chiamate al modello.
Funziona out-of-the-box con qualsiasi backend OpenAI-compatibile:

  Dev    : Ollama         → LLM_BASE_URL=http://localhost:11434/v1
  Prod A : vLLM           → LLM_BASE_URL=http://localhost:8000/v1
  Prod B : SGLang         → LLM_BASE_URL=http://localhost:30000/v1

Espone funzioni helper async per:
  - chat_complete()        : risposta intera (non-streaming)
  - chat_complete_stream() : streaming async dei token
  - chat_complete_json()   : risposta forzata in JSON (per routing/SQL)

Uso da un agente:

    from llm_client import chat_complete
    text = await chat_complete(
        model=ANSWER_MODEL,
        system="Sei un assistente...",
        user="Domanda dell'utente",
    )
"""

import sys
import os
import logging
from typing import AsyncIterator, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import (
    LLM_BASE_URL,
    LLM_API_KEY,
    LLM_TIMEOUT_SECONDS,
    LLM_ROUTING_TIMEOUT_SECONDS,
)

from openai import AsyncOpenAI
from openai import APIConnectionError, APITimeoutError, APIError

logger = logging.getLogger("llm_client")

# ── Singleton client ──────────────────────────────────────────────────────────
# Un'unica istanza condivisa: il connection pool sottostante riusa le
# connessioni HTTP, riducendo latenza e overhead per richieste multiple.

_client: Optional[AsyncOpenAI] = None


def get_client() -> AsyncOpenAI:
    """Restituisce il client AsyncOpenAI condiviso (lazy-init)."""
    global _client
    if _client is None:
        _client = AsyncOpenAI(
            base_url=LLM_BASE_URL,
            api_key=LLM_API_KEY,
            timeout=LLM_TIMEOUT_SECONDS,
            max_retries=1,
        )
        logger.info(f"LLM client inizializzato → {LLM_BASE_URL}")
    return _client


# ── Chat completion (non-streaming) ───────────────────────────────────────────

async def chat_complete(
    model: str,
    system: str,
    user: str,
    temperature: float = 0.2,
    max_tokens: Optional[int] = None,
    timeout: Optional[float] = None,
) -> str:
    """
    Genera una risposta completa (non-streaming).

    Restituisce il testo della risposta o un messaggio di errore leggibile.
    Non solleva eccezioni: gli errori vengono catturati e ritornati come stringa
    in modo che il chiamante (agent) possa decidere se mostrarli o meno.
    """
    client = get_client()

    try:
        response = await client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user",   "content": user},
            ],
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout if timeout is not None else LLM_TIMEOUT_SECONDS,
        )
        return response.choices[0].message.content or ""

    except APITimeoutError:
        logger.warning(f"Timeout LLM ({model})")
        return "Timeout — prova una domanda più specifica."
    except APIConnectionError as e:
        logger.error(f"Connessione LLM fallita ({model}): {e}")
        return f"Errore di connessione al modello LLM: {e}"
    except APIError as e:
        logger.error(f"Errore API LLM ({model}): {e}")
        return f"Errore del modello: {e}"
    except Exception as e:
        logger.error(f"Errore imprevisto LLM ({model}): {e}", exc_info=True)
        return f"Errore: {e}"


# ── Chat completion streaming ─────────────────────────────────────────────────

async def chat_complete_stream(
    model: str,
    system: str,
    user: str,
    temperature: float = 0.2,
    max_tokens: Optional[int] = None,
) -> AsyncIterator[str]:
    """
    Genera una risposta in streaming, yielding chunk di testo non appena
    arrivano dal modello. Usato dall'endpoint /v1/chat/completions con
    stream=True per inoltrare i token a Open WebUI in tempo reale.

    In caso di errore, yield un singolo messaggio testuale e termina.
    """
    client = get_client()

    try:
        stream = await client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user",   "content": user},
            ],
            temperature=temperature,
            max_tokens=max_tokens,
            stream=True,
            timeout=LLM_TIMEOUT_SECONDS,
        )

        async for chunk in stream:
            try:
                delta = chunk.choices[0].delta
                content = getattr(delta, "content", None)
                if content:
                    yield content
            except (IndexError, AttributeError):
                # Chunk senza delta valido (es: ruolo iniziale) → ignoriamo
                continue

    except APITimeoutError:
        logger.warning(f"Timeout streaming LLM ({model})")
        yield "\n\n[Timeout del modello — prova una domanda più specifica.]"
    except APIConnectionError as e:
        logger.error(f"Connessione streaming LLM fallita ({model}): {e}")
        yield f"\n\n[Errore di connessione al modello: {e}]"
    except APIError as e:
        logger.error(f"Errore API streaming LLM ({model}): {e}")
        yield f"\n\n[Errore del modello: {e}]"
    except Exception as e:
        logger.error(f"Errore imprevisto streaming LLM ({model}): {e}", exc_info=True)
        yield f"\n\n[Errore: {e}]"


# ── Chat completion JSON (per routing) ────────────────────────────────────────

async def chat_complete_json(
    model: str,
    system: str,
    user: str,
    temperature: float = 0.0,
    timeout: Optional[float] = None,
) -> str:
    """
    Variante usata per la generazione di output JSON (routing model).

    Timeout più stretto rispetto a chat_complete (default = LLM_ROUTING_TIMEOUT_SECONDS).
    Temperature = 0 per output deterministico.

    NB: non forziamo response_format={"type": "json_object"} perché Ollama
        non lo supporta su tutti i modelli; ci affidiamo al prompt + a
        un parsing tollerante nel chiamante.
    """
    return await chat_complete(
        model=model,
        system=system,
        user=user,
        temperature=temperature,
        timeout=timeout if timeout is not None else LLM_ROUTING_TIMEOUT_SECONDS,
    )


# ── Cleanup ───────────────────────────────────────────────────────────────────

async def close_client():
    """Chiude il client (chiamare allo shutdown del server)."""
    global _client
    if _client is not None:
        try:
            await _client.close()
        except Exception:
            pass
        _client = None