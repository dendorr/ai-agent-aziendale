"""
FINANCIAL AGENT
Handles: Excel (.xlsx, .xls), CSV, financial PDFs, text reports
Extracts: tables, values, summaries, trends
Always responds in Italian based exclusively on company documents
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import (CHROMA_PATHS, OLLAMA_URL, LLM_MODEL,
                            CHUNK_SIZE, CHUNK_OVERLAP, EXTENSIONS)
from pathlib import Path
import chromadb, fitz

client     = chromadb.PersistentClient(path=CHROMA_PATHS["financial"])
collection = client.get_or_create_collection("financial")

SYSTEM_PROMPT = """You are a professional financial assistant for a company.
You analyze financial documents: balance sheets, invoices, budgets, Excel reports, CSV files.

STRICT RULES:
- Always reply in Italian
- Base answers EXCLUSIVELY on data from the provided documents
- Always cite the source file and sheet when referencing numbers
- If a value is not found in the documents, say explicitly: "Non ho trovato questo dato nei documenti"
- Never invent figures, percentages or forecasts not present in the files
- When showing calculations, show all steps clearly
- Format currency values clearly (e.g. € 1.234,56)
- For trends, compare values only if multiple periods are available in the documents"""

def read_excel(f):
    """Read Excel file extracting all sheets, rows and values."""
    try:
        import openpyxl
        wb   = openpyxl.load_workbook(f, data_only=True)
        text = f"=== EXCEL FILE: {Path(f).name} ===\n"
        text += f"Sheets: {', '.join(wb.sheetnames)}\n\n"
        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            text += f"[Sheet: {sheet_name}]\n"
            # Find actual data range
            rows_with_data = 0
            for row in ws.iter_rows(values_only=True):
                cells = [str(c) if c is not None else "" for c in row]
                if any(c.strip() for c in cells):
                    text += " | ".join(cells) + "\n"
                    rows_with_data += 1
            text += f"(Total rows with data: {rows_with_data})\n\n"
        return text
    except Exception as e:
        # Fallback to pandas
        try:
            import pandas as pd
            sheets = pd.read_excel(f, sheet_name=None)
            text   = f"=== EXCEL FILE: {Path(f).name} ===\n"
            for name, df in sheets.items():
                text += f"\n[Sheet: {name}]\n"
                text += df.to_string(index=False) + "\n"
            return text
        except Exception as e2:
            return f"[Error reading Excel {Path(f).name}: {e2}]"

def read_csv(f):
    """Read CSV file using pandas for robust parsing."""
    try:
        import pandas as pd
        # Try different encodings
        for enc in ["utf-8", "latin-1", "iso-8859-1"]:
            try:
                df   = pd.read_csv(f, encoding=enc)
                text = f"=== CSV FILE: {Path(f).name} ===\n"
                text += f"Rows: {len(df)}, Columns: {len(df.columns)}\n"
                text += f"Columns: {', '.join(df.columns.astype(str))}\n\n"
                text += df.to_string(index=False) + "\n"
                return text
            except UnicodeDecodeError:
                continue
        return f"[Error: could not decode CSV file {Path(f).name}]"
    except Exception as e:
        return f"[Error reading CSV: {e}]"

def read_pdf(f):
    """Read financial PDF extracting text and tables."""
    try:
        doc  = fitz.open(f)
        text = f"=== FINANCIAL PDF: {Path(f).name} ===\n"
        for i, page in enumerate(doc):
            t = page.get_text()
            if t.strip():
                text += f"\n[Page {i+1}]\n{t}\n"
        return text
    except Exception as e:
        return f"[Error reading PDF: {e}]"

def read_file(f):
    """Route file to the correct reader."""
    ext = Path(f).suffix.lower()
    if ext in (".xlsx", ".xls"):  return read_excel(f)
    elif ext == ".csv":           return read_csv(f)
    elif ext == ".pdf":           return read_pdf(f)
    elif ext == ".txt":
        return f"=== TEXT FILE: {Path(f).name} ===\n" + \
               open(f, encoding="utf-8", errors="ignore").read()
    return f"[Format {ext} not supported by financial agent]"

def chunk_text(text):
    """Split text into overlapping chunks."""
    words  = text.split()
    chunks = []
    i      = 0
    while i < len(words):
        chunks.append(" ".join(words[i:i + CHUNK_SIZE]))
        i += CHUNK_SIZE - CHUNK_OVERLAP
    return chunks or [""]

def index_file(filepath):
    """Index a single file into the financial vector database."""
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
                "agent":    "financial"
            }]
        )
    return len(chunks)

def index_folder(folder):
    """Index all financial files in a folder recursively."""
    files = [f for f in Path(folder).rglob("*")
             if f.is_file() and f.suffix.lower() in EXTENSIONS["financial"]]
    print(f"[Financial] Found {len(files)} files...")
    total = 0
    for f in files:
        n = index_file(str(f))
        if n:
            print(f"  [OK] {f.name} -> {n} chunks")
        else:
            print(f"  [--] {f.name} -> skipped")
        total += n
    print(f"[Financial] Done — {total} total chunks")

def search(query):
    """Search the financial vector database."""
    if collection.count() == 0:
        return "[No financial documents indexed yet]"
    r = collection.query(
        query_texts=[query],
        n_results=min(6, collection.count())
    )
    context = ""
    for doc, meta in zip(r["documents"][0], r["metadatas"][0]):
        context += f"\n--- {meta['filename']} ---\n{doc[:2000]}\n"
    return context

def answer(question, context):
    """Send question + financial context to Ollama."""
    import requests
    prompt = f"""{SYSTEM_PROMPT}

COMPANY FINANCIAL DOCUMENTS:
{context}

QUESTION: {question}

DETAILED ANSWER (in Italian):"""
    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": LLM_MODEL, "prompt": prompt, "stream": False},
            timeout=180
        )
        r.raise_for_status()
        return r.json().get("response", "Error in model response")
    except requests.exceptions.Timeout:
        return "Il modello ha impiegato troppo tempo. Riprova con una domanda più specifica."
    except Exception as e:
        return f"Error communicating with model: {e}"

if __name__ == "__main__":
    import sys
    folder = sys.argv[1] if len(sys.argv) > 1 else "."
    print(f"Indexing financial documents in: {folder}")
    index_folder(folder)