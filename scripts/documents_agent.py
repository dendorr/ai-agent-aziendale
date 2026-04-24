"""
DOCUMENTS AGENT v3 — Universal document intelligence

Converts ALL documents to structured Markdown before indexing.
Supports: PDF, PPTX (with full image OCR), DOCX, Markdown, TXT.

Architecture:
  - Markdown cache:  documents converted once, reused on restart (mtime-based invalidation)
  - Image OCR:       pytesseract (fast) → easyocr (accurate) — singleton, not re-initialized
  - Number/measure:  exact values and units preserved, never transformed
  - Multi-model:     routing model (qwen3:0.6b) selects documents,
                     answer model (LLM_MODEL) generates the response
  - Persistent mem:  document index + user annotations across sessions
  - Zero hardcoded:  no assumptions about file content, language or domain
"""

import sys, os, json, re, hashlib
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import (CHROMA_PATHS, OLLAMA_URL, LLM_MODEL,
                            CHUNK_SIZE, CHUNK_OVERLAP, EXTENSIONS, MEMORY_PATH)
import chromadb
import semantic_analyzer as analyzer

client     = chromadb.PersistentClient(path=CHROMA_PATHS["documents"])
collection = client.get_or_create_collection("documents")

MEMORY_FILE    = Path(MEMORY_PATH) / "documents_memory.json"
MARKDOWN_CACHE = Path(MEMORY_PATH).parent / "markdown_cache"

# ── Models ─────────────────────────────────────────────────────────────────────
ROUTING_MODEL = "qwen3:0.6b"  # fast: selects relevant documents
ANSWER_MODEL  = LLM_MODEL      # accurate: final answer

# ── OCR — singleton to avoid re-initializing easyocr every call ───────────────
_easyocr_reader = None


def _get_easyocr_reader():
    """Lazy-initialize the easyocr Reader once per process."""
    global _easyocr_reader
    if _easyocr_reader is None:
        try:
            import easyocr
            _easyocr_reader = easyocr.Reader(
                ["it", "en"], gpu=False, verbose=False
            )
        except Exception:
            pass
    return _easyocr_reader


def ocr_image_bytes(image_bytes: bytes, source_hint: str = "") -> str:
    """
    Run OCR on raw image bytes.
    Level 1: pytesseract — fast, accurate for printed/scanned text
    Level 2: easyocr     — neural-network, better for stylized/handwritten text
    Returns the extracted text block, or an empty string if nothing is readable.
    """
    if not image_bytes:
        return ""

    from PIL import Image
    import io

    try:
        img = Image.open(io.BytesIO(image_bytes))
        if img.mode not in ("RGB", "L"):
            img = img.convert("RGB")
    except Exception:
        return ""

    # Level 1: pytesseract
    try:
        import pytesseract
        text = pytesseract.image_to_string(
            img, lang="ita+eng", config="--psm 3 --oem 3"
        ).strip()
        if len(text) > 15:
            return f"[OCR]\n{text}"
    except Exception:
        pass

    # Level 2: easyocr
    try:
        import numpy as np
        reader = _get_easyocr_reader()
        if reader:
            results = reader.readtext(np.array(img))
            lines   = [r[1] for r in results if len(r) > 2 and r[2] > 0.3]
            text    = "\n".join(lines).strip()
            if len(text) > 15:
                return f"[OCR]\n{text}"
    except Exception:
        pass

    return source_hint  # return caller hint (e.g. "[Immagine slide 3]") if OCR fails


def ocr_image_file(filepath) -> str:
    """OCR on a file path."""
    try:
        return ocr_image_bytes(Path(filepath).read_bytes())
    except Exception:
        return ""

# ── Shared helpers ─────────────────────────────────────────────────────────────

def _table_to_markdown(table) -> str:
    """
    Convert a python-pptx or python-docx Table object to a Markdown table.
    Preserves all cell content exactly — no rounding or transformation of numbers/units.
    """
    rows = []
    for i, row in enumerate(table.rows):
        cells = [cell.text.strip().replace("\n", " ") for cell in row.cells]
        rows.append("| " + " | ".join(cells) + " |")
        if i == 0:  # separator after header row
            rows.append("|" + "|".join(["---"] * len(cells)) + "|")
    return "\n".join(rows)

# ── PPTX → Markdown ────────────────────────────────────────────────────────────

def convert_pptx_to_markdown(filepath) -> str:
    """
    Convert a PPTX file to structured Markdown.

    Per slide:
      - Slide number + title as heading
      - All text boxes (level-aware indentation)
      - Tables → Markdown tables (numbers/units exact)
      - Embedded images → OCR text block
      - Speaker notes → blockquote at end of slide

    Returns a single Markdown string.
    """
    from pptx import Presentation
    from pptx.enum.shapes import MSO_SHAPE_TYPE

    p   = Path(filepath)
    prs = Presentation(str(filepath))

    lines = [f"# {p.stem}", f"*File: {p.name} — {len(prs.slides)} slide*", ""]

    for slide_num, slide in enumerate(prs.slides, 1):
        # Slide title
        title = ""
        try:
            if slide.shapes.title and slide.shapes.title.text.strip():
                title = slide.shapes.title.text.strip()
        except Exception:
            pass

        slide_heading = f"## Slide {slide_num}"
        if title:
            slide_heading += f": {title}"
        lines.append(slide_heading)
        lines.append("")

        # Process shapes in z-order
        for shape in slide.shapes:

            # ── Tables ───────────────────────────────────────────────────────
            if shape.has_table:
                lines.append(_table_to_markdown(shape.table))
                lines.append("")
                continue

            # ── Text frames ───────────────────────────────────────────────────
            if shape.has_text_frame:
                for para in shape.text_frame.paragraphs:
                    text = para.text.strip()
                    if not text or text == title:
                        continue
                    level  = getattr(para, "level", 0)
                    indent = "  " * level
                    prefix = f"{'#' * (level + 3)} " if level > 0 else ""
                    lines.append(f"{indent}{prefix}{text}")
                lines.append("")
                continue

            # ── Pictures → OCR ───────────────────────────────────────────────
            try:
                if shape.shape_type == MSO_SHAPE_TYPE.PICTURE:
                    img_bytes = shape.image.blob
                    hint      = f"[Immagine — slide {slide_num}]"
                    ocr_text  = ocr_image_bytes(img_bytes, source_hint=hint)
                    if ocr_text:
                        # Format as blockquote for visual distinction
                        quoted = ocr_text.replace("\n", "\n> ")
                        lines.append(f"> **{hint}**")
                        lines.append(f"> {quoted}")
                        lines.append("")
            except Exception:
                pass

        # Speaker notes
        try:
            notes_tf   = slide.notes_slide.notes_text_frame
            notes_text = notes_tf.text.strip()
            if notes_text:
                quoted = notes_text.replace("\n", "\n> ")
                lines.append("**Note relatore:**")
                lines.append(f"> {quoted}")
                lines.append("")
        except Exception:
            pass

        lines.append("---")
        lines.append("")

    return "\n".join(lines)

# ── DOCX → Markdown ────────────────────────────────────────────────────────────

def convert_docx_to_markdown(filepath) -> str:
    """
    Convert a DOCX file to structured Markdown.

    Strategy:
      Level 1 — markitdown (Microsoft): preserves headings, tables, lists perfectly
      Level 2 — python-docx manual: iterates body children in document order
                 (paragraphs AND tables interleaved), then extracts and OCRs images

    Numbers, units, tolerances are always preserved exactly as written.
    """
    p = Path(filepath)

    # Level 1: markitdown
    try:
        from markitdown import MarkItDown
        result = MarkItDown().convert(str(filepath))
        text   = result.text_content.strip()
        if len(text) > 100:
            md = f"# {p.stem}\n*File: {p.name}*\n\n{text}"
            # Also append OCR of embedded images (markitdown skips them)
            img_blocks = _docx_extract_image_ocr(filepath)
            if img_blocks:
                md += "\n\n## Immagini estratte dal documento\n\n" + "\n\n".join(img_blocks)
            return md
    except ImportError:
        pass
    except Exception as e:
        print(f"  [markitdown warning] {p.name}: {e}", flush=True)

    # Level 2: python-docx manual (preserves paragraph + table order)
    try:
        from docx import Document
        from docx.oxml.ns import qn
        from docx.text.paragraph import Paragraph
        from docx.table import Table as DocxTable

        doc   = Document(str(filepath))
        lines = [f"# {p.stem}", f"*File: {p.name}*", ""]

        _HEADING_MAP = {
            "heading 1": "#", "heading 2": "##", "heading 3": "###",
            "heading 4": "####", "heading 5": "#####",
        }

        # Iterate body children in document order (paragraphs interleaved with tables)
        for child in doc.element.body.iterchildren():
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag

            if tag == "p":
                para  = Paragraph(child, doc)
                text  = para.text.strip()
                style = (para.style.name or "").lower() if para.style else ""

                if not text:
                    lines.append("")
                    continue

                prefix = ""
                for key, md_prefix in _HEADING_MAP.items():
                    if key in style:
                        prefix = md_prefix
                        break

                if prefix:
                    lines.append(f"{prefix} {text}")
                elif "list" in style:
                    level  = getattr(para, "style", None)
                    indent = "  " * (int(re.search(r'\d', style).group()) - 1
                                     if re.search(r'\d', style) else 0)
                    lines.append(f"{indent}- {text}")
                else:
                    lines.append(text)

            elif tag == "tbl":
                tbl = DocxTable(child, doc)
                lines.append("")
                lines.append(_table_to_markdown(tbl))
                lines.append("")

        lines.append("")

        # Append OCR of all embedded images
        img_blocks = _docx_extract_image_ocr(filepath)
        if img_blocks:
            lines.append("## Immagini estratte dal documento")
            lines.extend(img_blocks)

        return "\n".join(lines)

    except Exception as e:
        return f"[DOCX error: {e}]"


def _docx_extract_image_ocr(filepath) -> list:
    """
    Extract all embedded images from a DOCX and run OCR on each.
    Returns a list of Markdown blockquote strings.
    """
    blocks = []
    try:
        from docx import Document
        doc = Document(str(filepath))
        for i, rel in enumerate(doc.part.rels.values(), 1):
            if "image" in rel.reltype:
                try:
                    img_bytes = rel.target_part.blob
                    ocr_text  = ocr_image_bytes(
                        img_bytes, source_hint=f"[Immagine {i}]"
                    )
                    if ocr_text and len(ocr_text) > 10:
                        quoted = ocr_text.replace("\n", "\n> ")
                        blocks.append(f"> **Immagine {i}:**\n> {quoted}")
                except Exception:
                    pass
    except Exception:
        pass
    return blocks

# ── PDF → Markdown ─────────────────────────────────────────────────────────────

def convert_pdf_to_markdown(filepath) -> str:
    """
    Convert a PDF to structured Markdown.

    Level 1 — markitdown:   best layout preservation
    Level 2 — fitz:         page-by-page text + image OCR + table detection
    Level 3 — pdfplumber:   supplementary table extraction

    All numeric values, units and tolerances are preserved exactly.
    """
    import fitz as _fitz

    p      = Path(filepath)
    header = f"# {p.stem}\n*File: {p.name}*\n\n"

    # Level 1: markitdown
    try:
        from markitdown import MarkItDown
        result = MarkItDown().convert(str(filepath))
        text   = result.text_content.strip()
        if len(text) > 100:
            return header + text
    except ImportError:
        pass
    except Exception as e:
        print(f"  [markitdown warning] {p.name}: {e}", flush=True)

    # Level 2: fitz — text + images with OCR
    fitz_parts = []
    try:
        doc = _fitz.open(str(filepath))
        for page_num, page in enumerate(doc, 1):
            page_lines = [f"## Pagina {page_num}", ""]

            # Raw text
            text = page.get_text("text")
            if text.strip():
                page_lines.append(text.strip())
                page_lines.append("")

            # Tables via fitz (available in PyMuPDF >= 1.23)
            try:
                for tab in page.find_tables().tables:
                    import pandas as pd
                    df = tab.to_pandas()
                    md = df.to_markdown(index=False)
                    if md:
                        page_lines.append(f"**Tabella:**\n{md}")
                        page_lines.append("")
            except Exception:
                pass

            # Embedded images → OCR
            for img_info in page.get_images(full=True):
                try:
                    xref      = img_info[0]
                    base_img  = doc.extract_image(xref)
                    img_bytes = base_img.get("image", b"")
                    ocr_text  = ocr_image_bytes(
                        img_bytes, source_hint=f"[Immagine pag. {page_num}]"
                    )
                    if ocr_text and len(ocr_text) > 15:
                        quoted = ocr_text.replace("\n", "\n> ")
                        page_lines.append(f"> **Immagine pag. {page_num}:**\n> {quoted}")
                        page_lines.append("")
                except Exception:
                    pass

            fitz_parts.append("\n".join(page_lines))
        doc.close()

    except Exception as e:
        fitz_parts.append(f"[fitz error: {e}]")

    # Level 3: pdfplumber — supplementary table extraction
    plumber_parts = []
    try:
        import pdfplumber
        with pdfplumber.open(str(filepath)) as pdf:
            for page_num, page in enumerate(pdf.pages, 1):
                for table in page.extract_tables() or []:
                    rows = []
                    for i, row in enumerate(table):
                        cells = [str(c).strip() if c else "" for c in row]
                        rows.append("| " + " | ".join(cells) + " |")
                        if i == 0:
                            rows.append("|" + "|".join(["---"] * len(cells)) + "|")
                    if rows:
                        plumber_parts.append(
                            f"**Tabella pag. {page_num}:**\n" + "\n".join(rows)
                        )
    except Exception:
        pass

    result = header
    if fitz_parts:
        result += "\n\n---\n\n".join(fitz_parts)
    if plumber_parts:
        result += "\n\n## Tabelle aggiuntive\n\n" + "\n\n".join(plumber_parts)

    return result if len(result) > len(header) + 20 else f"[Impossibile estrarre {p.name}]"

# ── Markdown cache ─────────────────────────────────────────────────────────────

def _cache_path(filepath) -> Path:
    """Deterministic cache file path for a given source file."""
    h    = hashlib.md5(str(filepath).encode()).hexdigest()[:8]
    stem = re.sub(r"[^a-zA-Z0-9]", "_", Path(filepath).stem[:40]).strip("_")
    return MARKDOWN_CACHE / f"{stem}_{h}.md"


def _cache_is_valid(filepath, cache_file: Path) -> bool:
    """Cache hit: cache file exists AND is newer than (or same age as) the source."""
    if not cache_file.exists():
        return False
    try:
        return cache_file.stat().st_mtime >= Path(filepath).stat().st_mtime
    except Exception:
        return False


def get_or_create_markdown(filepath) -> str:
    """
    Return the Markdown representation of a document.
    Reads from disk cache if valid; otherwise converts and writes the cache.
    Invalidation is automatic: cache is regenerated whenever the source file changes.
    """
    MARKDOWN_CACHE.mkdir(parents=True, exist_ok=True)
    cache_file = _cache_path(filepath)

    if _cache_is_valid(filepath, cache_file):
        try:
            return cache_file.read_text(encoding="utf-8")
        except Exception:
            pass  # corrupted cache → regenerate

    ext      = Path(filepath).suffix.lower()
    markdown = ""

    if ext == ".pdf":
        markdown = convert_pdf_to_markdown(filepath)
    elif ext in (".pptx", ".ppt"):
        markdown = convert_pptx_to_markdown(filepath)
    elif ext in (".docx", ".doc"):
        markdown = convert_docx_to_markdown(filepath)
    elif ext == ".md":
        markdown = Path(filepath).read_text(encoding="utf-8", errors="ignore")
    elif ext == ".txt":
        content  = Path(filepath).read_text(encoding="utf-8", errors="ignore")
        markdown = f"# {Path(filepath).stem}\n\n{content}"

    if markdown and len(markdown.strip()) > 50:
        try:
            cache_file.write_text(markdown, encoding="utf-8")
            print(f"  [cache] Scritto {cache_file.name} "
                  f"({len(markdown):,} caratteri)", flush=True)
        except Exception as e:
            print(f"  [cache warning] {e}", flush=True)

    return markdown

# ── Memory ─────────────────────────────────────────────────────────────────────

def load_memory() -> dict:
    if MEMORY_FILE.exists():
        try:
            return json.loads(MEMORY_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"documents": {}, "annotations": {}}


def save_memory(memory: dict):
    MEMORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    MEMORY_FILE.write_text(
        json.dumps(memory, indent=2, ensure_ascii=False), encoding="utf-8"
    )


def update_document_memory(filepath, markdown: str):
    """
    Store per-document metadata in persistent memory after indexing.
    Stored: file path, timestamp, word count, size in KB.
    No content assumptions — all fields are derived from the file itself.
    """
    from datetime import datetime
    memory = load_memory()
    if "documents" not in memory:
        memory["documents"] = {}

    p = Path(filepath)
    memory["documents"][p.name] = {
        "filepath":   str(filepath),
        "indexed_at": datetime.now().isoformat(timespec="seconds"),
        "words":      len(markdown.split()),
        "chars":      len(markdown),
        "size_kb":    round(p.stat().st_size / 1024, 1),
        "ext":        p.suffix.lower(),
    }
    save_memory(memory)


def add_annotation(filename: str, note: str):
    """Attach a user annotation to a document (persisted across sessions)."""
    memory = load_memory()
    if "annotations" not in memory:
        memory["annotations"] = {}
    if filename not in memory["annotations"]:
        memory["annotations"][filename] = []
    memory["annotations"][filename].append(note)
    save_memory(memory)

# ── Chunking + indexing ────────────────────────────────────────────────────────

def chunk_text(text: str) -> list:
    words = text.split()
    chunks, i = [], 0
    while i < len(words):
        chunks.append(" ".join(words[i:i + CHUNK_SIZE]))
        i += CHUNK_SIZE - CHUNK_OVERLAP
    return chunks or [""]


def index_file(filepath) -> int:
    """
    Convert a document to Markdown (or use cache), chunk it and store in ChromaDB.
    Returns the number of chunks indexed, or 0 if the file was skipped.
    """
    p   = Path(filepath)
    ext = p.suffix.lower()

    if ext not in EXTENSIONS["documents"]:
        return 0

    print(f"  [docs] Elaborazione {p.name}...", flush=True)
    markdown = get_or_create_markdown(filepath)

    if not markdown or not markdown.strip():
        return 0

    update_document_memory(filepath, markdown)

    # Remove all stale chunks for this file
    try:
        existing = collection.get(where={"path": str(filepath)})
        if existing and existing["ids"]:
            collection.delete(ids=existing["ids"])
    except Exception:
        pass

    chunks = chunk_text(markdown)
    for i, chunk in enumerate(chunks):
        collection.upsert(
            documents=[chunk],
            ids=[f"{filepath}__c{i}"],
            metadatas=[{
                "filename": p.name,
                "path":     str(filepath),
                "chunk":    i,
                "agent":    "documents",
                "type":     "chunk",
                "ext":      ext,
            }]
        )
    return len(chunks)


def index_folder(folder):
    files = [f for f in Path(folder).rglob("*")
             if f.is_file() and f.suffix.lower() in EXTENSIONS["documents"]]
    print(f"[Documents] Trovati {len(files)} file...")
    total = 0
    for f in files:
        n = index_file(str(f))
        if n: print(f"  [OK] {f.name} → {n} chunk")
        else: print(f"  [--] {f.name} → saltato")
        total += n
    print(f"[Documents] Completato — {total} chunk totali")

# ── Search ─────────────────────────────────────────────────────────────────────

def search(query: str):
    return analyzer.search_with_cards(collection, query, "documents", n_results=6)

# ── Routing model ──────────────────────────────────────────────────────────────

def route_documents(query: str, memory: dict) -> list:
    """
    Use the fast ROUTING_MODEL to identify which indexed documents are most
    likely relevant to the user's query.
    Returns a list of filenames (used to prioritize ChromaDB results).
    Falls back to empty list on any error.
    """
    docs = memory.get("documents", {})
    if not docs:
        return []

    import requests

    doc_lines = "\n".join(
        f"  - {name}  ({info.get('words', 0):,} parole | "
        f"{info.get('size_kb', 0)} KB | {info.get('ext', '')})"
        for name, info in list(docs.items())[:25]
    )

    prompt = f"""Sei un assistente che seleziona i documenti più rilevanti per rispondere a una domanda.

DOCUMENTI DISPONIBILI:
{doc_lines}

DOMANDA: {query}

Elenca SOLO i nomi file più rilevanti (massimo 4), uno per riga.
Solo nomi file esatti, nessun altro testo."""

    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": ROUTING_MODEL, "prompt": prompt, "stream": False},
            timeout=20,
        )
        r.raise_for_status()
        raw      = r.json().get("response", "")
        all_docs = set(docs.keys())
        relevant = [
            line.strip().strip("-").strip()
            for line in raw.strip().split("\n")
            if line.strip() and any(d in line for d in all_docs)
        ]
        return relevant[:4]
    except Exception:
        return []

# ── System prompt ──────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """Sei un esperto analista documentale con accesso completo ai documenti aziendali.

REGOLE ASSOLUTE:
- Rispondi SEMPRE e SOLO in italiano, qualunque sia la lingua del documento
- Usa ESCLUSIVAMENTE le informazioni presenti nei documenti forniti nel contesto
- Non inventare, non stimare, non integrare con conoscenze esterne
- Cita sempre la fonte (nome file, numero slide, numero pagina) per ogni dato riportato
- Se l'informazione non è presente nei documenti, dichiaralo esplicitamente

NUMERI, MISURE E SPECIFICHE TECNICHE — REGOLA CRITICA:
- Riporta i valori ESATTAMENTE come appaiono nel documento: nessuna trasformazione
- Preserva le unità di misura (mm, cm, m, µm, kg, g, ml, l, %, °C, bar, N, pz, €, $, ecc.)
- Non arrotondare mai, non convertire unità, non cambiare il formato
- Tolleranze (±0,5; ±5%; max 3 mm) vanno sempre riportate insieme al valore principale
- Tabelle con misure vanno riportate COMPLETE e FEDELI all'originale
- Se il documento riporta un range (es. 10–15 kg), riporta il range intero

TESTO ESTRATTO DA IMMAGINI ([OCR]):
- Il testo tra [OCR] proviene da immagini, grafici o schemi nel documento
- Trattalo come dato attendibile; segnala la fonte OCR se utile al contesto

STRUTTURA DELLE RISPOSTE:
- Specifiche tecniche → tabella o lista ordinata con unità
- Confronti tra documenti → colonne affiancate con fonte per ogni valore
- Riassunti → struttura gerarchica (sezioni → punti chiave)
- Ricerca di valori specifici → cita il contesto esatto del documento

CAPACITÀ:
- Analisi di presentazioni, relazioni, manuali, capitolati, specifiche tecniche
- Estrazione precisa di dati da tabelle, grafici e immagini (via OCR)
- Confronto tra più documenti sullo stesso argomento
- Ricerca di termini, misure, codici o specifiche esatte
- Sintesi di documenti lunghi mantenendo tutti i dati numerici"""

# ── Answer ─────────────────────────────────────────────────────────────────────

def answer(question: str, context: str) -> str:
    import requests

    memory = load_memory()

    # Build document index block for the prompt
    docs     = memory.get("documents", {})
    mem_txt  = ""

    if docs:
        doc_lines = "\n".join(
            f"  - {name} ({info.get('words', 0):,} parole | {info.get('ext', '')})"
            for name, info in list(docs.items())[:20]
        )
        mem_txt += f"\nDOCUMENTI INDICIZZATI:\n{doc_lines}\n"

    annotations = memory.get("annotations", {})
    if annotations:
        ann_lines = [
            f"  [{fname}] {note}"
            for fname, notes in list(annotations.items())[:5]
            for note in notes[:3]
        ]
        if ann_lines:
            mem_txt += "\nANNOTAZIONI UTENTE:\n" + "\n".join(ann_lines) + "\n"

    # Use routing model to suggest relevant docs (informational, not filtering)
    relevant = route_documents(question, memory)
    if relevant:
        mem_txt += "\nDOCUMENTI PROBABILMENTE RILEVANTI PER QUESTA DOMANDA:\n"
        mem_txt += "\n".join(f"  - {d}" for d in relevant) + "\n"

    prompt = f"""{SYSTEM_PROMPT}
{mem_txt}
=== CONTENUTO DOCUMENTI — FONTE PRIMARIA ===
{context}

DOMANDA: {question}

RISPOSTA (obbligatoriamente in italiano — dati solo dalla FONTE PRIMARIA,
numeri e misure ESATTAMENTE come nel documento):"""

    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": ANSWER_MODEL, "prompt": prompt, "stream": False},
            timeout=180,
        )
        r.raise_for_status()
        return r.json().get("response", "Error")
    except requests.exceptions.Timeout:
        return "Timeout — prova una domanda più specifica."
    except Exception as e:
        return f"Error: {e}"


if __name__ == "__main__":
    folder = sys.argv[1] if len(sys.argv) > 1 else "."
    index_folder(folder)