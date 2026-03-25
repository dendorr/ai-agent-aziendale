"""
DOCUMENTS AGENT
Handles: PDF, PPTX, Word (.docx), Markdown, plain text
Extracts: text, slide content, tables, headings, speaker notes
Always responds in Italian based exclusively on company documents
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import (CHROMA_PATHS, OLLAMA_URL, LLM_MODEL,
                            CHUNK_SIZE, CHUNK_OVERLAP, EXTENSIONS)
from pathlib import Path
import chromadb, fitz

client     = chromadb.PersistentClient(path=CHROMA_PATHS["documents"])
collection = client.get_or_create_collection("documents")

SYSTEM_PROMPT = """You are a professional document assistant for a company.
You analyze company documents: reports, presentations, manuals, procedures, contracts.

STRICT RULES:
- Always reply in Italian
- Base answers EXCLUSIVELY on the content of the provided documents
- Always cite the document name and section/page when referencing content
- For summaries, structure the answer clearly with key points
- If a document is only partially readable, state what was extracted
- Never add external information beyond the documents
- For presentations, reference the specific slide number
- For Word documents, reference the section or heading"""

def read_pdf(f):
    """Read PDF extracting text page by page."""
    try:
        doc  = fitz.open(f)
        text = f"=== PDF DOCUMENT: {Path(f).name} ===\n"
        text += f"Total pages: {len(doc)}\n"
        for i, page in enumerate(doc):
            t = page.get_text()
            if t.strip():
                text += f"\n[Page {i+1}]\n{t}\n"
        return text
    except Exception as e:
        return f"[Error reading PDF: {e}]"

def read_pptx(f):
    """Read PowerPoint extracting slide content, titles, and speaker notes."""
    try:
        from pptx import Presentation
        prs  = Presentation(f)
        text = f"=== PRESENTATION: {Path(f).name} ===\n"
        text += f"Total slides: {len(prs.slides)}\n"

        for i, slide in enumerate(prs.slides):
            text += f"\n[Slide {i+1}]"

            # Slide title
            title = ""
            if slide.shapes.title and slide.shapes.title.text.strip():
                title = slide.shapes.title.text.strip()
                text += f" — {title}"
            text += "\n"

            # All text from shapes
            for shape in slide.shapes:
                if not shape.has_text_frame: continue
                for para in shape.text_frame.paragraphs:
                    t = para.text.strip()
                    if t and t != title:
                        # Preserve bullet hierarchy
                        indent = "  " * (para.level + 1)
                        text += f"{indent}{t}\n"

            # Speaker notes
            if slide.has_notes_slide:
                notes = slide.notes_slide.notes_text_frame.text.strip()
                if notes:
                    text += f"  [Speaker notes]: {notes}\n"

        return text
    except ImportError:
        return f"[python-pptx not installed — install with: pip install python-pptx]"
    except Exception as e:
        return f"[Error reading PPTX: {e}]"

def read_docx(f):
    """Read Word document extracting paragraphs, headings and tables."""
    try:
        from docx import Document
        doc  = Document(f)
        text = f"=== WORD DOCUMENT: {Path(f).name} ===\n"

        for para in doc.paragraphs:
            if not para.text.strip(): continue
            style = para.style.name
            if "Heading 1" in style:
                text += f"\n# {para.text.strip()}\n"
            elif "Heading 2" in style:
                text += f"\n## {para.text.strip()}\n"
            elif "Heading 3" in style:
                text += f"\n### {para.text.strip()}\n"
            else:
                text += f"{para.text.strip()}\n"

        # Tables
        if doc.tables:
            text += "\n[TABLES]\n"
            for i, table in enumerate(doc.tables):
                text += f"\n[Table {i+1}]\n"
                for row in table.rows:
                    cells = [c.text.strip() for c in row.cells if c.text.strip()]
                    if cells:
                        text += " | ".join(cells) + "\n"

        return text
    except ImportError:
        return f"[python-docx not installed — install with: pip install python-docx]"
    except Exception as e:
        return f"[Error reading DOCX: {e}]"

def read_file(f):
    """Route file to the correct reader."""
    ext = Path(f).suffix.lower()
    if ext == ".pdf":                  return read_pdf(f)
    elif ext in (".pptx", ".ppt"):     return read_pptx(f)
    elif ext in (".docx", ".doc"):     return read_docx(f)
    elif ext in (".txt", ".md"):
        return f"=== TEXT FILE: {Path(f).name} ===\n" + \
               open(f, encoding="utf-8", errors="ignore").read()
    return f"[Format {ext} not supported by documents agent]"

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
    """Index a single document into the vector database."""
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
                "agent":    "documents"
            }]
        )
    return len(chunks)

def index_folder(folder):
    """Index all document files in a folder recursively."""
    files = [f for f in Path(folder).rglob("*")
             if f.is_file() and f.suffix.lower() in EXTENSIONS["documents"]]
    print(f"[Documents] Found {len(files)} files...")
    total = 0
    for f in files:
        n = index_file(str(f))
        if n:
            print(f"  [OK] {f.name} -> {n} chunks")
        else:
            print(f"  [--] {f.name} -> skipped")
        total += n
    print(f"[Documents] Done — {total} total chunks")

def search(query):
    """Search the documents vector database."""
    if collection.count() == 0:
        return "[No documents indexed yet]"
    r = collection.query(
        query_texts=[query],
        n_results=min(5, collection.count())
    )
    context = ""
    for doc, meta in zip(r["documents"][0], r["metadatas"][0]):
        context += f"\n--- {meta['filename']} ---\n{doc[:2000]}\n"
    return context

def answer(question, context):
    """Send question + document context to Ollama."""
    import requests
    prompt = f"""{SYSTEM_PROMPT}

COMPANY DOCUMENT CONTENTS:
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
        return r.json().get("response", "Error in model response")
    except requests.exceptions.Timeout:
        return "Il modello ha impiegato troppo tempo. Riprova con una domanda più specifica."
    except Exception as e:
        return f"Error communicating with model: {e}"

if __name__ == "__main__":
    import sys
    folder = sys.argv[1] if len(sys.argv) > 1 else "."
    print(f"Indexing documents in: {folder}")
    index_folder(folder)