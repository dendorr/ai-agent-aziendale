"""
SEMANTIC ANALYZER — Lazy evaluation
Semantic cards are generated ONLY when first queried, not during indexing.
Once generated, cards are cached forever (disk + ChromaDB).
"""
import sys, os, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import OLLAMA_URL, LLM_MODEL_FAST, MEMORY_PATH
from pathlib import Path

MEMORY_FILE = Path(MEMORY_PATH) / "semantic_cards.json"

def load_cards():
    if MEMORY_FILE.exists():
        try: return json.loads(MEMORY_FILE.read_text(encoding="utf-8"))
        except Exception: return {}
    return {}

def save_cards(cards):
    MEMORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    MEMORY_FILE.write_text(json.dumps(cards, indent=2, ensure_ascii=False), encoding="utf-8")

def generate_semantic_card(filename, raw_text, agent_type):
    """Ask Ollama to analyze the file and produce a structured semantic card."""
    import requests
    preview = raw_text[:3000]
    prompts = {
        "financial": f"""Analizza questo documento finanziario aziendale e produci una scheda semantica strutturata.

FILE: {filename}
CONTENUTO ESTRATTO:
{preview}

Produci una scheda con ESATTAMENTE questo formato:

TIPO_DOCUMENTO: [foglio spese / fattura / bilancio / preventivo / lista pagamenti]
CONTESTO_BUSINESS: [descrivi in 1-2 frasi cosa rappresenta questo file]
ENTITA_PRINCIPALI: [persone, aziende, prodotti menzionati]
VALORI_CHIAVE: [importi, totali, prezzi unitari importanti]
PERIODO: [anno, mese, data se presente]
STRUTTURA: [descrivi le colonne/campi principali]
COLORI_EXCEL: [spiega cosa significano i colori in questo file]
REGOLE_CALCOLO: [es: ogni persona deve 15 euro, il resto è la differenza]
NOTE_IMPORTANTI: [qualsiasi cosa utile per rispondere a domande su questo file]""",

        "drawings": f"""Analizza questo file di disegno tecnico e produci una scheda semantica strutturata.

FILE: {filename}
CONTENUTO ESTRATTO:
{preview}

Produci una scheda con ESATTAMENTE questo formato:

TIPO_FILE: [DXF/STP/IFC/SVG/STL/PDF tecnico]
TIPO_OGGETTO: [componente meccanico / edificio / struttura / assemblaggio]
FUNZIONE_PROBABILE: [a cosa potrebbe servire basandoti sulla geometria]
GEOMETRIA: [numero solidi, facce, superfici - complessità]
MATERIALI: [se presenti]
UNITA_MISURA: [mm/cm/m/pollici]
LAYER_PRINCIPALI: [se DXF/SVG]
ELEMENTI_BIM: [se IFC: piani, pareti, porte, ecc.]
NOTE_TECNICHE: [qualsiasi dettaglio tecnico rilevante]""",

        "documents": f"""Analizza questo documento aziendale e produci una scheda semantica strutturata.

FILE: {filename}
CONTENUTO ESTRATTO:
{preview}

Produci una scheda con ESATTAMENTE questo formato:

TIPO_DOCUMENTO: [relazione/presentazione/manuale/contratto/procedura]
ARGOMENTO_PRINCIPALE: [di cosa tratta in 1-2 frasi]
STRUTTURA: [capitoli/sezioni/slide principali]
ENTITA_CHIAVE: [persone, aziende, prodotti, luoghi]
DATE_IMPORTANTI: [date rilevanti se presenti]
PUNTI_CHIAVE: [3-5 informazioni più importanti]"""
    }
    prompt = prompts.get(agent_type, prompts["documents"])
    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": LLM_MODEL_FAST, "prompt": prompt, "stream": False},
            timeout=60
        )
        r.raise_for_status()
        card = r.json().get("response", "")
        return f"=== SCHEDA SEMANTICA: {filename} ===\n{card}\n"
    except Exception as e:
        return f"=== SCHEDA SEMANTICA: {filename} ===\nFile indicizzato. Tipo: {agent_type}\n"

def get_or_create_card(filepath, raw_text, agent_type, collection):
    """Get existing card from cache or generate a new one."""
    filename = Path(filepath).name
    card_id  = f"{filepath}__semantic_card"
    # Check ChromaDB cache
    try:
        existing = collection.get(ids=[card_id])
        if existing["ids"]:
            return existing["documents"][0]
    except Exception:
        pass
    # Generate new card
    print(f"  [AI] Generating semantic card for {filename}...", flush=True)
    card = generate_semantic_card(filename, raw_text, agent_type)
    # Save to ChromaDB
    collection.upsert(
        documents=[card],
        ids=[card_id],
        metadatas=[{"filename": filename, "path": str(filepath),
                    "chunk": -1, "agent": agent_type, "type": "semantic_card"}]
    )
    # Save to disk
    cards = load_cards()
    cards[str(filepath)] = card
    save_cards(cards)
    return card

def search_with_cards(collection, query, agent_type, n_results=6):
    """
    Search with lazy semantic card generation.
    Cards are created on first query, then cached forever.
    """
    if collection.count() == 0:
        return "[No documents indexed yet]"

    r = collection.query(
        query_texts=[query],
        n_results=min(n_results, collection.count())
    )

    cards      = []
    chunks     = []
    seen_files = set()

    # Separate cards from chunks
    for doc, meta in zip(r["documents"][0], r["metadatas"][0]):
        fname      = meta.get("filename", "")
        chunk_type = meta.get("type", "chunk")
        if chunk_type == "semantic_card":
            if fname not in seen_files:
                cards.append((doc, meta))
                seen_files.add(fname)
        else:
            chunks.append((doc, meta))

    # For each file in results, get or generate its semantic card (LAZY)
    for _, meta in chunks:
        fname = meta.get("filename", "")
        fpath = meta.get("path", "")
        if fname not in seen_files and fpath:
            card_id = f"{fpath}__semantic_card"
            try:
                existing = collection.get(ids=[card_id])
                if existing["ids"] and existing["documents"]:
                    # Card exists in cache
                    cards.append((existing["documents"][0], existing["metadatas"][0]))
                    seen_files.add(fname)
                else:
                    # Card does NOT exist — generate now (lazy, first time only)
                    raw_chunks = collection.get(where={"path": fpath})
                    if raw_chunks["documents"]:
                        raw_text = " ".join(raw_chunks["documents"][:3])
                        card = get_or_create_card(fpath, raw_text, agent_type, collection)
                        cards.append((card, {"filename": fname}))
                        seen_files.add(fname)
            except Exception:
                pass

    # Build context: cards first, then raw data
    context = ""
    if cards:
        context += "=== DOCUMENT UNDERSTANDING ===\n"
        for doc, _ in cards:
            context += doc + "\n"
        context += "\n=== RAW DATA ===\n"
    for doc, meta in chunks:
        context += f"\n--- {meta.get('filename', 'unknown')} ---\n{doc[:2000]}\n"

    return context if context.strip() else "[No relevant content found]"