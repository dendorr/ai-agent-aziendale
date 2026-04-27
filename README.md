# AI Agent Aziendale

A fully local, air-gapped AI system for company document analysis. No data ever leaves the local network. No cloud APIs. No training on company data.

---

## Architecture

```
Open WebUI (port 3000) → FastAPI server.py (port 8000) → agent router
                                                          ├─ agent-drawings  → ChromaDB (async)
                                                          ├─ agent-financial → ChromaDB + SQLite (async)
                                                          ├─ agent-documents → ChromaDB + Markdown cache (async)
                                                          │
                                                          └─ llm_client.py → AsyncOpenAI → Ollama / vLLM / SGLang

watcher.py (separate process, sync) → monitors data/{financial,drawings,documents}
                                     → calls index_file() per agent
```

All agents use `AsyncOpenAI` via a shared client in `scripts/llm_client.py`. The LLM endpoint is configurable: Ollama for development, vLLM or SGLang for production. ChromaDB operations (synchronous by nature) are wrapped in `asyncio.run_in_executor`.

The server supports both streaming (SSE token-by-token) and blocking responses, with conversation history management and per-request UUID tracking.

The watcher runs as a separate sync process — it polls the data folders every 10 seconds, debounces file changes, and dispatches indexing to agent-specific `index_file()` functions in a thread pool.

---

## Three Agents

| Agent | Model name in WebUI | Handles |
|-------|---------------------|---------|
| Drawings  | `agent-drawings`  | DXF, STP, IFC, SVG, STL, technical PDFs |
| Financial | `agent-financial` | Excel, CSV, financial PDFs → auto SQLite |
| Documents | `agent-documents` | PDF, PPTX, Word, Markdown → Markdown cache + OCR |

**Financial Agent** — Reads any Excel or CSV file and figures out the structure automatically: column names, data types, numeric statistics, color patterns. Builds a local SQLite database from the file so queries are fast and precise. Uses a two-model pipeline: a fast small model generates the SQL, the main model writes the answer.

**Drawings Agent** — Reads technical CAD files: DXF, STP/STEP, IFC, SVG, STL. Extracts geometry, layers, materials, dimensions, and component lists. DWG files are automatically converted to DXF via LibreCAD before processing.

**Documents Agent** — Reads PDFs, PowerPoint presentations, Word documents, and Markdown files. Converts everything to structured Markdown before indexing, which is cached to disk so re-indexing is fast. Runs OCR on embedded images (including images inside PPTX slides) using pytesseract with easyocr as fallback. Extracts speaker notes, tables, headings, and all text content.

---

## Tech stack

| Component | Technology |
|---|---|
| LLM client | AsyncOpenAI (OpenAI-compatible, works with any backend) |
| LLM runtime (dev) | Ollama |
| LLM runtime (prod) | SGLang or vLLM (recommended for multi-user) |
| Models (dev) | qwen2.5:7b (answers), qwen3:0.6b (routing/SQL) |
| Models (prod) | qwen3:30b-a3b MoE (answers), qwen3:0.6b (routing/SQL) |
| Chat interface | Open WebUI via Docker |
| API server | FastAPI + uvicorn (full async, SSE streaming) |
| Vector database | ChromaDB |
| Structured database | SQLite (auto-built from Excel/CSV) |
| OCR | pytesseract + easyocr |
| Language | Python 3.12 |

---

## Hardware

| Environment | Specs |
|---|---|
| Development | Samsung Galaxy Book 4 Ultra, RTX 4050, 16GB RAM, Windows 11 + WSL2 Ubuntu 24.04 |
| Production | Office server, RTX 4090, 24GB VRAM |

---

## Project structure

```
ai-agent/
├── config/
│   └── config.py               # all paths, models, chunk sizes — env var driven
├── scripts/
│   ├── server.py               # FastAPI server v3 (async, SSE streaming, history)
│   ├── watcher.py              # file watcher v2.1 (sync, separate process)
│   ├── llm_client.py           # shared AsyncOpenAI singleton + helpers
│   ├── financial_agent.py      # Excel/CSV/PDF agent with auto SQLite
│   ├── drawings_agent.py       # CAD files agent
│   ├── documents_agent.py      # PDF/PPTX/DOCX agent with OCR + Markdown cache
│   ├── semantic_analyzer.py    # lazy semantic card engine (async)
│   └── convert_dwg.py          # DWG to DXF converter
├── data/                       # company files — monitored by watcher
│   ├── financial/              # Excel, CSV, PDF (invoices, budgets, reports)
│   ├── drawings/               # DXF, STP, IFC, SVG, STL (CAD files)
│   └── documents/              # PDF, PPTX, DOCX, Markdown
├── memory/                     # JSON memory + SQLite (git-ignored)
├── markdown_cache/             # converted documents cache (git-ignored)
├── chroma/                     # vector databases (git-ignored)
│   ├── financial/
│   ├── drawings/
│   └── documents/
├── logs/                       # runtime logs (git-ignored)
├── setup.sh                    # automated setup script
└── requirements.txt            # Python dependencies
```

Drop files into `data/{financial,drawings,documents}` and the watcher picks them up automatically.

From Windows Explorer: `\\wsl$\Ubuntu\home\<user>\ai-agent\data`

---

## Environment variables

Everything is configurable via environment variables. Defaults work out of the box for development with Ollama.

| Variable | Default | Description |
|---|---|---|
| `LLM_BASE_URL` | `http://localhost:11434/v1` | LLM endpoint (Ollama dev, vLLM/SGLang prod) |
| `LLM_API_KEY` | `ollama-no-key` | API key (ignored by Ollama, required by OpenAI client) |
| `LLM_MODEL_MAIN` | `qwen2.5:7b` | Model for answers |
| `LLM_MODEL_FAST` | `qwen3:0.6b` | Model for routing and SQL generation |
| `COMPANY_DATA_DIR` | `~/ai-agent/data` | Root folder for company files |
| `AGENT_PORT` | `8000` | FastAPI server port |
| `CHUNK_SIZE` | `600` | Words per chunk for vector indexing |
| `CHUNK_OVERLAP` | `60` | Word overlap between chunks |

---

## Installation

### Quick setup

```bash
git clone https://github.com/dendorr/ai-agent-aziendale.git
cd ai-agent-aziendale
bash setup.sh
```

### Manual setup

```bash
# system dependencies (Ubuntu/WSL2)
sudo apt install tesseract-ocr tesseract-ocr-ita tesseract-ocr-eng libgl1

# Python environment
python3.12 -m venv ~/ai-env
source ~/ai-env/bin/activate
pip install -r requirements.txt

# Ollama models (development)
ollama pull qwen2.5:7b
ollama pull qwen3:0.6b

# Production (RTX 4090)
ollama pull qwen3:30b-a3b

# Open WebUI (first time)
docker run -d -p 3000:3000 \
  --add-host=host.docker.internal:host-gateway \
  -v open-webui:/app/backend/data \
  --name open-webui \
  ghcr.io/open-webui/open-webui:main
```

---

## Starting the system

```bash
source ~/ai-env/bin/activate
sudo systemctl restart ollama
docker start open-webui
cd ~/ai-agent/scripts

# Terminal 1 — API server
python server.py

# Terminal 2 — file watcher
python watcher.py
```

Or with nohup for background:

```bash
nohup python server.py  > ~/ai-agent/logs/server.log  2>&1 &
nohup python watcher.py > ~/ai-agent/logs/watcher.log 2>&1 &
```

Then open `http://localhost:3000` and connect to `http://127.0.0.1:8000/v1` in Open WebUI settings. You will see 3 models: `agent-drawings`, `agent-financial`, `agent-documents`.

---

## Production deployment with SGLang

SGLang offers significantly better throughput than Ollama for multi-user scenarios thanks to RadixAttention (KV cache reuse across requests with shared system prompts).

```bash
# Install SGLang
pip install sglang[all]

# Start SGLang server with optimizations for RTX 4090
python -m sglang.launch_server \
  --model Qwen/Qwen3-30B-A3B \
  --port 30000 \
  --kv-cache-dtype fp8 \
  --enable-prefix-caching \
  --chunked-prefill-size 8192

# Point the agent system to SGLang
export LLM_BASE_URL="http://localhost:30000/v1"
export LLM_MODEL_MAIN="Qwen/Qwen3-30B-A3B"
```

vLLM is also supported as an alternative backend — same OpenAI-compatible API.

---

## Port forwarding (WSL2 to Windows)

If the WSL2 IP resets and services aren't reachable from other machines:

```powershell
netsh interface portproxy add v4tov4 listenport=3000 listenaddress=0.0.0.0 connectport=3000 connectaddress=<WSL_IP>
netsh interface portproxy add v4tov4 listenport=8000 listenaddress=0.0.0.0 connectport=8000 connectaddress=<WSL_IP>
```

Find WSL2 IP with: `hostname -I` inside Ubuntu.

---

## Remote access

```bash
cloudflared tunnel --protocol http2 --url http://localhost:3000
```

Note: Cloudflare tunnels may be blocked on mobile hotspots (port 7844).

---

## Security

- All data stays local — zero internet after initial setup
- ChromaDB databases are stored locally, never synced to cloud
- API server binds to 0.0.0.0 — firewall controls external access
- Open WebUI handles authentication for all workstations
- Company files are git-ignored — never committed to the repo
- No company data is ever used for model training
- Swagger/OpenAPI docs disabled in production

---

## License

Private project — all rights reserved.