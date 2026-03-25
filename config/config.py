# ============================================================
# GLOBAL CONFIGURATION — AI Agent System
# ============================================================

# Folders to monitor — PHASE 1: local test files
# In Phase 2, these will point to the company server
WATCH_FOLDER = "/mnt/c/Users/ferra/OneDrive - Università di Pavia/Desktop/ai-test"

# Vector databases — one per agent
CHROMA_DB_PATH   = "/home/ferra/ai-agent/chroma_db"
CHROMA_DRAWINGS  = "/home/ferra/ai-agent/chroma_disegni"

# Ollama settings
OLLAMA_URL       = "http://localhost:11434"
LLM_MODEL        = "qwen2.5:7b"
LLM_MODEL_FAST   = "llama3.2:latest"
EMBED_MODEL      = "nomic-embed-text"

# Server settings
AGENT_PORT       = 8000
KIWIX_PORT       = 8080

# Chunking settings
CHUNK_SIZE       = 600
CHUNK_OVERLAP    = 60

# Supported file extensions per agent
SUPPORTED_EXTENSIONS = [
    ".pdf", ".xlsx", ".xls", ".csv",
    ".dxf", ".dwg", ".svg", ".ifc",
    ".stp", ".step", ".stl", ".obj",
    ".3dm", ".txt", ".md"
]

EXTENSIONS = {
    "financial": [".xlsx", ".xls", ".csv", ".pdf", ".txt"],
    "drawings":  [".dxf", ".dwg", ".svg", ".ifc", ".stp",
                  ".step", ".stl", ".obj", ".3dm", ".pdf"],
    "documents": [".pdf", ".pptx", ".ppt", ".docx", ".doc", ".txt", ".md"],
}