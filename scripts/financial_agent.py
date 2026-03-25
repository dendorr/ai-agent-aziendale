"""
FINANCIAL AGENT — with Lazy Semantic Cards + Color Support
Handles: Excel (with colors), CSV, PDF invoices/reports
Semantic cards generated on first query, cached forever.
"""
import sys, os, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import (CHROMA_PATHS, OLLAMA_URL, LLM_MODEL,
                            CHUNK_SIZE, CHUNK_OVERLAP, EXTENSIONS, MEMORY_PATH)
from pathlib import Path
import chromadb, fitz
import semantic_analyzer as analyzer

client     = chromadb.PersistentClient(path=CHROMA_PATHS["financial"])
collection = client.get_or_create_collection("financial")

# ── Persistent memory ────────────────────────────────────────
MEMORY_FILE = Path(MEMORY_PATH) / "financial_memory.json"

def load_memory():
    if MEMORY_FILE.exists():
        try: return json.loads(MEMORY_FILE.read_text(encoding="utf-8"))
        except Exception: return {}
    return {}

def save_memory(memory):
    MEMORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    MEMORY_FILE.write_text(json.dumps(memory, indent=2, ensure_ascii=False), encoding="utf-8")

def add_to_memory(key, value):
    """Manually add a stable business fact to persistent memory."""
    memory = load_memory()
    memory[key] = value
    save_memory(memory)
    print(f"[Memory] Saved: {key} -> {value}")

# ── Color reading ─────────────────────────────────────────────
COLOR_MEANINGS = {
    "FF00FF00": "PAGATO/CONFERMATO", "FF92D050": "PAGATO/CONFERMATO",
    "FF00B050": "PAGATO/CONFERMATO", "FFC6EFCE": "PAGATO/CONFERMATO",
    "FFFF0000": "NON PAGATO/RIFIUTATO", "FFFFC7CE": "NON PAGATO/RIFIUTATO",
    "FF9C0006": "NON PAGATO/RIFIUTATO", "FFFFFF00": "IN ATTESA/PENDENTE",
    "FFFFEB9C": "IN ATTESA/PENDENTE", "FFFFC000": "IN ATTESA/PENDENTE",
}

def get_color_meaning(hex_color):
    if not hex_color or hex_color in ("00000000", "FF000000"): return None
    upper = hex_color.upper()
    if upper in COLOR_MEANINGS: return COLOR_MEANINGS[upper]
    try:
        r = int(upper[2:4], 16)
        g = int(upper[4:6], 16)
        b = int(upper[6:8], 16)
        if g > 150 and r < 100 and b < 100: return "PAGATO/CONFERMATO"
        elif r > 150 and g < 100 and b < 100: return "NON PAGATO/RIFIUTATO"
        elif r > 150 and g > 150 and b < 100: return "IN ATTESA/PENDENTE"
    except Exception: pass
    return None

# ── File readers ─────────────────────────────────────────────

def read_excel(f):
    """Read Excel with full color support and structured row labeling."""
    try:
        import openpyxl
        wb   = openpyxl.load_workbook(f, data_only=True)
        text = f"=== EXCEL FILE: {Path(f).name} ===\nSheets: {', '.join(wb.sheetnames)}\n\n"
        for sheet_name in wb.sheetnames:
            ws      = wb[sheet_name]
            text   += f"[Sheet: {sheet_name}]\n"
            headers = []
            first   = True
            for row in ws.iter_rows():
                row_vals  = []
                row_color = []
                for cell in row:
                    val = str(cell.value) if cell.value is not None else ""
                    row_vals.append(val)
                    try:
                        if cell.fill and cell.fill.fgColor:
                            m = get_color_meaning(cell.fill.fgColor.rgb)
                            row_color.append(f"{val}[{m}]" if m else val)
                        else:
                            row_color.append(val)
                    except Exception:
                        row_color.append(val)
                if not any(v.strip() for v in row_vals): continue
                if first:
                    headers = row_vals
                    text   += "HEADERS: " + " | ".join(row_vals) + "\n\n"
                    first   = False
                else:
                    for h, v in zip(headers, row_color):
                        if v.strip(): text += f"  {h}: {v}\n"
                    text += "\n"
        return text
    except Exception as e:
        try:
            import pandas as pd
            dfs  = pd.read_excel(f, sheet_name=None)
            text = f"=== EXCEL FILE: {Path(f).name} ===\n"
            for name, df in dfs.items():
                text += f"\n[Sheet: {name}]\n{df.to_string(index=False)}\n"
            return text
        except Exception as e2:
            return f"[Error reading Excel: {e2}]"

def read_pdf_financial(f):
    """Multi-strategy PDF reader: PyMuPDF text + pdfplumber tables."""
    results = []
    try:
        doc  = fitz.open(f)
        text = f"=== FINANCIAL PDF: {Path(f).name} ===\nPages: {len(doc)}\n\n"
        for i, page in enumerate(doc):
            t = page.get_text("text")
            if t.strip(): text += f"[Page {i+1}]\n{t}\n"
        if len(text) > 200: results.append(text)
    except Exception: pass
    try:
        import pdfplumber
        with pdfplumber.open(f) as pdf:
            text = f"=== PDF TABLES: {Path(f).name} ===\n"
            for i, page in enumerate(pdf.pages):
                tables = page.extract_tables()
                if tables:
                    text += f"\n[Page {i+1} Tables]\n"
                    for table in tables:
                        for row in table:
                            clean = [str(c).strip() if c else "" for c in row]
                            if any(c for c in clean):
                                text += " | ".join(clean) + "\n"
            if len(text) > 200: results.append(text)
    except Exception: pass
    if not results: return f"[Could not extract text from {Path(f).name}]"
    return "\n".join(results)

def read_csv(f):
    try:
        import pandas as pd
        for enc in ["utf-8", "latin-1", "iso-8859-1", "cp1252"]:
            try:
                df   = pd.read_csv(f, encoding=enc)
                text = f"=== CSV: {Path(f).name} ===\n"
                text += f"Rows: {len(df)}, Columns: {', '.join(df.columns.astype(str))}\n\n"
                text += df.to_string(index=False) + "\n"
                return text
            except UnicodeDecodeError: continue
    except Exception as e:
        return f"[Error reading CSV: {e}]"

def read_file(f):
    ext = Path(f).suffix.lower()
    if ext in (".xlsx", ".xls"):  return read_excel(f)
    elif ext == ".csv":           return read_csv(f)
    elif ext == ".pdf":           return read_pdf_financial(f)
    elif ext == ".txt":           return open(f, encoding="utf-8", errors="ignore").read()
    return f"[Format {ext} not supported]"

# ── Chunking ─────────────────────────────────────────────────
def chunk_text(text):
    words = text.split()
    chunks, i = [], 0
    while i < len(words):
        chunks.append(" ".join(words[i:i+CHUNK_SIZE]))
        i += CHUNK_SIZE - CHUNK_OVERLAP
    return chunks or [""]

# ── Indexing — fast, no AI ───────────────────────────────────
def index_file(filepath):
    """Index file — fast, no AI, only raw text extraction."""
    p   = Path(filepath)
    ext = p.suffix.lower()
    if ext not in EXTENSIONS["financial"]: return 0
    text = read_file(filepath)
    if not text.strip(): return 0
    chunks = chunk_text(text)
    for i, chunk in enumerate(chunks):
        collection.upsert(
            documents=[chunk],
            ids=[f"{filepath}__c{i}"],
            metadatas=[{
                "filename": p.name,
                "path":     str(filepath),
                "chunk":    i,
                "agent":    "financial",
                "type":     "chunk"
            }]
        )
    return len(chunks)

def index_folder(folder):
    files = [f for f in Path(folder).rglob("*")
             if f.is_file() and f.suffix.lower() in EXTENSIONS["financial"]]
    print(f"[Financial] Found {len(files)} files...")
    total = 0
    for f in files:
        n = index_file(str(f))
        if n: print(f"  [OK] {f.name} -> {n} chunks")
        else: print(f"  [--] {f.name} -> skipped")
        total += n
    print(f"[Financial] Done — {total} total chunks")

# ── Search with lazy semantic cards ──────────────────────────
def search(query):
    """Search with lazy semantic card generation."""
    return analyzer.search_with_cards(collection, query, "financial", n_results=6)

# ── Answer ───────────────────────────────────────────────────
SYSTEM_PROMPT = """You are a precise financial assistant for a company.
Use DOCUMENT UNDERSTANDING (semantic cards) to understand context FIRST,
then use RAW DATA to find specific values.

EXCEL COLOR LEGEND:
- [PAGATO/CONFERMATO] = green = confirmed payment
- [NON PAGATO/RIFIUTATO] = red = unpaid/rejected
- [IN ATTESA/PENDENTE] = yellow = pending

RULES:
- Always reply in Italian
- ONLY use data from the provided documents
- Always cite source file and sheet
- If not found: "Non ho trovato questo dato nei documenti"
- Never invent figures
- Show all calculation steps
- Format: € 1.234,56"""

def answer(question, context):
    import requests
    memory     = load_memory()
    memory_txt = ""
    if memory:
        memory_txt = "\nKNOWLEDGE BASE:\n"
        memory_txt += "\n".join(f"  - {k}: {v}" for k, v in memory.items()) + "\n"
    prompt = f"""{SYSTEM_PROMPT}
{memory_txt}
{context}

QUESTION: {question}

ANSWER (in Italian):"""
    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": LLM_MODEL, "prompt": prompt, "stream": False},
            timeout=180
        )
        r.raise_for_status()
        return r.json().get("response", "Error")
    except requests.exceptions.Timeout:
        return "Timeout — riprova con una domanda più specifica."
    except Exception as e:
        return f"Error: {e}"

if __name__ == "__main__":
    import sys
    folder = sys.argv[1] if len(sys.argv) > 1 else "."
    index_folder(folder)