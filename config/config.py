# ============================================================
# GLOBAL CONFIGURATION — Company AI Agent System
# ============================================================
# PHASE 1: uses local test folders
# PHASE 2: change FOLDERS values to point to company server mount
# ============================================================

import os

# ── Base path ────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HOME_DIR = os.path.expanduser("~")

# ── Ollama settings ──────────────────────────────────────────
OLLAMA_URL      = "http://localhost:11434"

# Main model — use qwen2.5:32b on office server (RTX 4090)
# Use qwen2.5:7b or llama3.2 on laptop for testing
LLM_MODEL       = "qwen2.5:7b"       # change to qwen2.5:32b on office server
LLM_MODEL_FAST  = "llama3.2:latest"
EMBED_MODEL     = "nomic-embed-text"

# ── Server settings ──────────────────────────────────────────
AGENT_PORT      = 8000
KIWIX_PORT      = 8080

# ── Chunking settings ────────────────────────────────────────
CHUNK_SIZE      = 600   # words per chunk
CHUNK_OVERLAP   = 60    # overlap between chunks

# ── Document folders ─────────────────────────────────────────
# PHASE 1 — local test folders (laptop)
# PHASE 2 — change these to the SMB mount point on company server
#   e.g. "/mnt/company_server/financial"

_TEST_BASE = "/mnt/c/Users/ferra/OneDrive - Università di Pavia/Desktop/ai-test"

FOLDERS = {
    "financial": _TEST_BASE,   # Excel, CSV, financial PDFs
    "drawings":  _TEST_BASE,   # DXF, STP, IFC, SVG, STL
    "documents": _TEST_BASE,   # PDF, PPTX, Word
}

# On office server — uncomment and set correct paths:
# FOLDERS = {
#     "financial": "/mnt/company_server/financial",
#     "drawings":  "/mnt/company_server/drawings",
#     "documents": "/mnt/company_server/documents",
# }

# ── Vector database paths ────────────────────────────────────
_CHROMA_BASE = os.path.join(HOME_DIR, "ai-agent", "chroma")

CHROMA_PATHS = {
    "financial": os.path.join(_CHROMA_BASE, "financial"),
    "drawings":  os.path.join(_CHROMA_BASE, "drawings"),
    "documents": os.path.join(_CHROMA_BASE, "documents"),
}

# Create chroma directories if they don't exist
for path in CHROMA_PATHS.values():
    os.makedirs(path, exist_ok=True)

# ── Supported file extensions per agent ──────────────────────
EXTENSIONS = {
    "financial": [".xlsx", ".xls", ".csv", ".pdf", ".txt"],
    "drawings":  [".dxf", ".dwg", ".svg", ".ifc",
                  ".stp", ".step", ".stl", ".obj", ".3dm", ".pdf"],
    "documents": [".pdf", ".pptx", ".ppt", ".docx", ".doc", ".txt", ".md"],
}