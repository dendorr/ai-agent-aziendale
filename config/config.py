# ============================================================
# GLOBAL CONFIGURATION — Company AI Agent System
# ============================================================
import os

HOME_DIR = os.path.expanduser("~")
BASE_DIR = os.path.join(HOME_DIR, "ai-agent")

OLLAMA_URL     = "http://localhost:11434"
LLM_MODEL      = "mistral-nemo:latest"
LLM_MODEL_FAST = "llama3.2:latest"
EMBED_MODEL    = "nomic-embed-text"
AGENT_PORT     = 8000
KIWIX_PORT     = 8080
CHUNK_SIZE     = 600
CHUNK_OVERLAP  = 60

_TEST_BASE = "/mnt/c/Users/ferra/OneDrive - Università di Pavia/Desktop/ai-test"

FOLDERS = {
    "financial": _TEST_BASE,
    "drawings":  _TEST_BASE,
    "documents": _TEST_BASE,
}

# PHASE 2 — office server:
# FOLDERS = {
#     "financial": "/mnt/company_server/financial",
#     "drawings":  "/mnt/company_server/drawings",
#     "documents": "/mnt/company_server/documents",
# }

_CHROMA_BASE = os.path.join(BASE_DIR, "chroma")

CHROMA_PATHS = {
    "financial": os.path.join(_CHROMA_BASE, "financial"),
    "drawings":  os.path.join(_CHROMA_BASE, "drawings"),
    "documents": os.path.join(_CHROMA_BASE, "documents"),
}

for path in CHROMA_PATHS.values():
    os.makedirs(path, exist_ok=True)

MEMORY_PATH = os.path.join(BASE_DIR, "memory")
os.makedirs(MEMORY_PATH, exist_ok=True)

EXTENSIONS = {
    "financial": [".xlsx", ".xls", ".csv", ".pdf", ".txt"],
    "drawings":  [".dxf", ".dwg", ".svg", ".ifc",
                  ".stp", ".step", ".stl", ".obj", ".3dm", ".pdf"],
    "documents": [".pdf", ".pptx", ".ppt", ".docx", ".doc", ".txt", ".md"],
}