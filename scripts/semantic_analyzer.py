"""
SEMANTIC ANALYZER — Lazy evaluation, full async

Le schede semantiche vengono generate SOLO alla prima query (non durante
l'indicizzazione, per non rallentare il watcher). Una volta generate sono
cached per sempre (su disco + dentro ChromaDB).

Tutte le chiamate LLM sono async via llm_client.AsyncOpenAI.
ChromaDB è sincrono per natura → wrappato in run_in_executor.
"""

import sys
import os
import json
import asyncio
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import LLM_MODEL_FAST, MEMORY_PATH

from llm_client import chat_complete

MEMORY_FILE = Path(MEMORY_PATH) / "semantic_cards.json"


# ── Persistenza schede su disco ───────────────────────────────────────────────

def load_cards() -> dict:
    if MEMORY_FILE.exists():
        try:
            return json.loads(MEMORY_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def save_cards(cards: dict):
    MEMORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    MEMORY_FILE.write_text(
        json.dumps(cards, indent=2, ensure_ascii=False), encoding="utf-8"
    )


# ── Prompt schede semantiche per agent ────────────────────────────────────────

_SYSTEM_PROMPT = (
    "Sei un assistente che produce schede semantiche strutturate "
    "per descrivere documenti aziendali. Rispondi in italiano, "
    "compilando ESATTAMENTE i campi richiesti, uno per riga."
)


def _user_prompt(filename: str, raw_text: str, agent_type: str) -> str:
    """Genera il prompt utente specifico per il tipo di agente."""
    preview = raw_text[:3000]

    if agent_type == "financial":
        fields = """TIPO_DOCUMENTO: [foglio spese / fattura / bilancio / preventivo / lista pagamenti / report di mercato]
CONTESTO_BUSINESS: [descrivi in 1-2 frasi cosa rappresenta questo file]
ENTITA_PRINCIPALI: [persone, aziende, prodotti menzionati]
VALORI_CHIAVE: [importi, totali, prezzi unitari importanti]
PERIODO: [anno, mese, data se presente]
STRUTTURA: [descrivi le colonne/campi principali]
COLORI_EXCEL: [spiega cosa significano i colori in questo file specifico, se presenti]
REGOLE_CALCOLO: [regole inferite dal contenuto, se evidenti]
NOTE_IMPORTANTI: [qualsiasi cosa utile per rispondere a domande su questo file]"""

    elif agent_type == "drawings":
        fields = """TIPO_FILE: [DXF/STP/IFC/SVG/STL/PDF tecnico]
TIPO_OGGETTO: [componente meccanico / edificio / struttura / assemblaggio]
FUNZIONE_PROBABILE: [a cosa potrebbe servire basandoti sulla geometria]
GEOMETRIA: [numero solidi, facce, superfici - complessità]
MATERIALI: [se presenti]
UNITA_MISURA: [mm/cm/m/pollici]
LAYER_PRINCIPALI: [se DXF/SVG]
ELEMENTI_BIM: [se IFC: piani, pareti, porte, ecc.]
NOTE_TECNICHE: [qualsiasi dettaglio tecnico rilevante]"""

    else:  # documents (default)
        fields = """TIPO_DOCUMENTO: [relazione/presentazione/manuale/contratto/procedura]
ARGOMENTO_PRINCIPALE: [di cosa tratta in 1-2 frasi]
STRUTTURA: [capitoli/sezioni/slide principali]
ENTITA_CHIAVE: [persone, aziende, prodotti, luoghi]
DATE_IMPORTANTI: [date rilevanti se presenti]
PUNTI_CHIAVE: [3-5 informazioni più importanti]"""

    return (
        f"FILE: {filename}\n"
        f"CONTENUTO ESTRATTO:\n{preview}\n\n"
        f"Produci una scheda semantica con ESATTAMENTE questo formato:\n\n"
        f"{fields}"
    )


# ── Generazione card (async) ──────────────────────────────────────────────────

async def generate_semantic_card(filename: str, raw_text: str, agent_type: str) -> str:
    """Chiede al modello fast di analizzare il file e produrre una scheda."""
    user_prompt = _user_prompt(filename, raw_text, agent_type)

    card = await chat_complete(
        model=LLM_MODEL_FAST,
        system=_SYSTEM_PROMPT,
        user=user_prompt,
        temperature=0.1,
        timeout=60,
    )

    return f"=== SCHEDA SEMANTICA: {filename} ===\n{card}\n"


# ── Get-or-create con cache ChromaDB + disco ──────────────────────────────────

async def get_or_create_card(filepath: str, raw_text: str,
                              agent_type: str, collection) -> str:
    """
    Ottiene la scheda dalla cache (ChromaDB) o la genera ora.
    Tutte le operazioni ChromaDB (sincrone) sono delegate a un executor.
    """
    filename = Path(filepath).name
    card_id  = f"{filepath}__semantic_card"
    loop     = asyncio.get_running_loop()

    # ── Check cache su ChromaDB ──────────────────────────────────────────────
    try:
        existing = await loop.run_in_executor(
            None, lambda: collection.get(ids=[card_id])
        )
        if existing and existing.get("ids") and existing.get("documents"):
            return existing["documents"][0]
    except Exception:
        pass

    # ── Generazione (lazy, prima volta) ──────────────────────────────────────
    print(f"  [AI] Generazione scheda semantica per {filename}...", flush=True)
    card = await generate_semantic_card(filename, raw_text, agent_type)

    # ── Salva su ChromaDB (sync → executor) ──────────────────────────────────
    try:
        await loop.run_in_executor(
            None,
            lambda: collection.upsert(
                documents=[card],
                ids=[card_id],
                metadatas=[{
                    "filename": filename,
                    "path":     str(filepath),
                    "chunk":    -1,
                    "agent":    agent_type,
                    "type":     "semantic_card",
                }],
            ),
        )
    except Exception as e:
        print(f"  [warn] save card → ChromaDB: {e}", flush=True)

    # ── Salva su disco (file JSON) — anche questo via executor ───────────────
    try:
        await loop.run_in_executor(None, _persist_card_to_disk, str(filepath), card)
    except Exception as e:
        print(f"  [warn] save card → disk: {e}", flush=True)

    return card


def _persist_card_to_disk(filepath: str, card: str):
    """Helper sync per scrivere la card su disco (chiamato da executor)."""
    cards = load_cards()
    cards[filepath] = card
    save_cards(cards)


# ── Search con generazione lazy delle card ────────────────────────────────────

async def search_with_cards(collection, query: str, agent_type: str,
                             n_results: int = 6) -> str:
    """
    Esegue similarity search su ChromaDB e arricchisce il contesto con
    le schede semantiche dei file rilevanti (generate al volo se mancanti).

    Tutte le operazioni ChromaDB sono in executor.
    """
    loop = asyncio.get_running_loop()

    count = await loop.run_in_executor(None, collection.count)
    if count == 0:
        return "[No documents indexed yet]"

    n = min(n_results, count)
    r = await loop.run_in_executor(
        None,
        lambda: collection.query(query_texts=[query], n_results=n),
    )

    cards = []
    chunks = []
    seen_files = set()

    # Separa schede da chunk grezzi
    for doc, meta in zip(r["documents"][0], r["metadatas"][0]):
        fname = meta.get("filename", "")
        chunk_type = meta.get("type", "chunk")
        if chunk_type == "semantic_card":
            if fname not in seen_files:
                cards.append((doc, meta))
                seen_files.add(fname)
        else:
            chunks.append((doc, meta))

    # Per ogni file nei risultati, ottieni la card (genera se serve) — in parallelo
    async def _fetch_card_for(meta):
        fname = meta.get("filename", "")
        fpath = meta.get("path", "")
        if fname in seen_files or not fpath:
            return None

        card_id = f"{fpath}__semantic_card"
        try:
            existing = await loop.run_in_executor(
                None, lambda: collection.get(ids=[card_id])
            )
            if existing and existing.get("ids") and existing.get("documents"):
                return (existing["documents"][0], {"filename": fname})

            # Card mancante → genera ora (lazy)
            raw_chunks = await loop.run_in_executor(
                None, lambda: collection.get(where={"path": fpath})
            )
            if raw_chunks and raw_chunks.get("documents"):
                raw_text = " ".join(raw_chunks["documents"][:3])
                card = await get_or_create_card(fpath, raw_text, agent_type, collection)
                return (card, {"filename": fname})
        except Exception:
            return None
        return None

    # Lancia in parallelo tutte le richieste di card mancanti.
    # asyncio.gather con return_exceptions=True per resilienza.
    tasks = [_fetch_card_for(meta) for _, meta in chunks
             if meta.get("filename") not in seen_files]
    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for res in results:
            if isinstance(res, tuple):
                doc, meta = res
                if meta["filename"] not in seen_files:
                    cards.append(res)
                    seen_files.add(meta["filename"])

    # Costruisci il contesto: prima le schede, poi i chunk grezzi
    context = ""
    if cards:
        context += "=== DOCUMENT UNDERSTANDING ===\n"
        for doc, _ in cards:
            context += doc + "\n"
        context += "\n=== RAW DATA ===\n"
    for doc, meta in chunks:
        context += f"\n--- {meta.get('filename', 'unknown')} ---\n{doc[:2000]}\n"

    return context if context.strip() else "[No relevant content found]"