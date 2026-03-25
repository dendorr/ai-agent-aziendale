"""
DOCUMENTS AGENT — with Lazy Semantic Cards
Handles: PDF, PPTX, Word (.docx), Markdown, plain text
Semantic cards generated on first query, cached forever.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import (CHROMA_PATHS, OLLAMA_URL, LLM_MODEL,
                            CHUNK_SIZE, CHUNK_OVERLAP, EXTENSIONS)
from pathlib import Path
import chromadb, fitz
import semantic_analyzer as analyzer

client     = chromadb.PersistentClient(path=CHROMA_PATHS["documents"])
collection = client.get_or_create_collection("documents")

SYSTEM_PROMPT = """You are a professional document assistant for a company.
You analyze: reports, presentations, manuals, procedures, contracts.

Always reply in Italian. Base answers EXCLUSIVELY on the content of the provided documents.
Always cite the document name and section/page.
For summaries, structure the answer clearly.
Never add external information beyond the documents.
For presentations, reference the specific slide number.
For Word documents, reference the section or heading."""

def read_pdf(f):
    try:
        doc  = fitz.open(f)
        text = f"=== PDF: {Path(f).name} ===\nPages: {len(doc)}\n"
        for i, page in enumerate(doc):
            t = page.get_text()
            if t.strip(): text += f"\n[Page {i+1}]\n{t}\n"
        return text
    except Exception as e:
        return f"[Error reading PDF: {e}]"

def read_pptx(f):
    try:
        from pptx import Presentation
        prs  = Presentation(f)
        text = f"=== PRESENTATION: {Path(f).name} ===\nSlides: {len(prs.slides)}\n"
        for i, slide in enumerate(prs.slides):
            text += f"\n[Slide {i+1}]"
            title = ""
            if slide.shapes.title and slide.shapes.title.text.strip():
                title = slide.shapes.title.text.strip()
                text += f" — {title}"
            text += "\n"
            for shape in slide.shapes:
                if not shape.has_text_frame: continue
                for para in shape.text_frame.paragraphs:
                    t = para.text.strip()
                    if t and t != title:
                        indent = "  " * (para.level + 1)
                        text += f"{indent}{t}\n"
            if slide.has_notes_slide:
                notes = slide.notes_slide.notes_text_frame.text.strip()
                if notes: text += f"  [Notes]: {notes}\n"
        return text
    except ImportError:
        return f"[python-pptx not installed]"
    except Exception as e:
        return f"[Error reading PPTX: {e}]"

def read_docx(f):
    try:
        from docx import Document
        doc  = Document(f)
        text = f"=== WORD DOCUMENT: {Path(f).name} ===\n"
        for para in doc.paragraphs:
            if not para.text.strip(): continue
            style = para.style.name
            if "Heading 1" in style:   text += f"\n# {para.text.strip()}\n"
            elif "Heading 2" in style: text += f"\n## {para.text.strip()}\n"
            elif "Heading 3" in style: text += f"\n### {para.text.strip()}\n"
            else:                      text += f"{para.text.strip()}\n"
        if doc.tables:
            text += "\n[TABLES]\n"
            for i, table in enumerate(doc.tables):
                text += f"\n[Table {i+1}]\n"
                for row in table.rows:
                    cells = [c.text.strip() for c in row.cells if c.text.strip()]
                    if cells: text += " | ".join(cells) + "\n"
        return text
    except ImportError:
        return f"[python-docx not installed]"
    except Exception as e:
        return f"[Error reading DOCX: {e}]"

def read_file(f):
    ext = Path(f).suffix.lower()
    if ext == ".pdf":              return read_pdf(f)
    elif ext in (".pptx", ".ppt"): return read_pptx(f)
    elif ext in (".docx", ".doc"): return read_docx(f)
    elif ext in (".txt", ".md"):   return open(f, encoding="utf-8", errors="ignore").read()
    return f"[Format {ext} not supported]"

def chunk_text(text):
    words  = text.split()
    chunks = []
    i      = 0
    while i < len(words):
        chunks.append(" ".join(words[i:i + CHUNK_SIZE]))
        i += CHUNK_SIZE - CHUNK_OVERLAP
    return chunks or [""]

def index_file(filepath):
    """Index file — fast, no AI, only raw text extraction."""
    p   = Path(filepath)
    ext = p.suffix.lower()
    if ext not in EXTENSIONS["documents"]: return 0
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
                "agent":    "documents",
                "type":     "chunk"
            }]
        )
    return len(chunks)

def index_folder(folder):
    files = [f for f in Path(folder).rglob("*")
             if f.is_file() and f.suffix.lower() in EXTENSIONS["documents"]]
    print(f"[Documents] Found {len(files)} files...")
    total = 0
    for f in files:
        n = index_file(str(f))
        if n: print(f"  [OK] {f.name} -> {n} chunks")
        else: print(f"  [--] {f.name} -> skipped")
        total += n
    print(f"[Documents] Done — {total} total chunks")

def search(query):
    """Search with lazy semantic card generation."""
    return analyzer.search_with_cards(collection, query, "documents", n_results=5)

def answer(question, context):
    import requests
    prompt = f"""{SYSTEM_PROMPT}

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
    print(f"Indexing documents in: {folder}")
    index_folder(folder)