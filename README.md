# AI Agent Aziendale

A fully local AI agent system for small businesses. No cloud, no subscriptions, no data ever leaves the network.

---

## What it does

Three specialized agents, each focused on a specific type of company document.

**Financial Agent**

Reads any Excel or CSV file and figures out the structure automatically — column names, data types, numeric statistics, color patterns. Builds a local SQLite database from the file so queries are fast and precise. Answers natural language questions about invoices, payments, budgets, and market reports. Uses a two-model pipeline: a fast small model generates the SQL, the main model writes the answer.

**Drawings Agent**

Reads technical CAD files: DXF, STP/STEP, IFC, SVG, STL. Extracts geometry, layers, materials, dimensions, and component lists. DWG files are automatically converted to DXF via LibreCAD before processing.

**Documents Agent**

Reads PDFs, PowerPoint presentations, Word documents, and Markdown files. Converts everything to structured Markdown before indexing, which is cached to disk so re-indexing is fast. Runs OCR on embedded images (including images inside PPTX slides) using pytesseract with easyocr as fallback. Extracts speaker notes, tables, headings, and all text content. Always cites the source file and page or slide number.

---

## Architecture

```
Open WebUI (port 3000) -> FastAPI server (port 8000) -> agent router
                                                         |- Financial Agent  -> ChromaDB + SQLite
                                                         |- Drawings Agent   -> ChromaDB
                                                         |- Documents Agent  -> ChromaDB + Markdown cache
                                                                |
                                                         Ollama (local LLM)
```

---

## Tech stack

| Component | Technology |
|---|---|
| LLM runtime | Ollama |
| Models | qwen2.5:7b (main), qwen3:0.6b (routing), qwen2.5:32b (production server) |
| Chat interface | Open WebUI via Docker |
| API server | FastAPI |
| Vector database | ChromaDB |
| Structured database | SQLite |
| OCR | pytesseract + easyocr |
| Language | Python 3.12 |

---

## Hardware

| Environment | Specs |
|---|---|
| Development | Samsung Galaxy Book 4 Ultra, RTX 4050, 16GB RAM, Windows 11 + WSL2 Ubuntu 24.04 |
| Production | Office server, RTX 4090 |

---

## Project structure

```
ai-agent/
├── config/
│   └── config.py               # paths, models, chunk sizes
├── scripts/
│   ├── server.py               # FastAPI multi-agent server
│   ├── watcher.py              # file watcher, re-indexes on change
│   ├── financial_agent.py      # Excel/CSV/PDF agent with SQLite
│   ├── drawings_agent.py       # CAD files agent
│   ├── documents_agent.py      # PDF/PPTX/DOCX agent with OCR
│   ├── semantic_analyzer.py    # lazy semantic card engine
│   └── convert_dwg.py          # DWG to DXF converter
├── memory/                     # JSON memory files (git-ignored)
├── markdown_cache/             # converted documents cache (git-ignored)
├── chroma/                     # vector databases (git-ignored)
│   ├── financial/
│   ├── drawings/
│   └── documents/
└── logs/                       # runtime logs (git-ignored)
```

---

## Setup

The easiest way is to run the setup script, which installs all system and Python dependencies and creates the directory structure.

```bash
bash setup.sh
```

Or manually:

```bash
# system dependencies (Ubuntu/WSL2)
sudo apt install tesseract-ocr tesseract-ocr-ita tesseract-ocr-eng libgl1

# Python environment
python3.12 -m venv ~/ai-env
source ~/ai-env/bin/activate
pip install -r requirements.txt

# Ollama models
ollama pull qwen2.5:7b
ollama pull qwen3:0.6b
ollama pull llama3.2

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
nohup python server.py  > ~/ai-agent/logs/server.log  2>&1 &
nohup python watcher.py > ~/ai-agent/logs/watcher.log 2>&1 &
```

Then open `http://localhost:3000` and select an agent as the model.

---

## Remote access

```bash
cloudflared tunnel --protocol http2 --url http://localhost:3000
```

Note: Cloudflare tunnels may be blocked on mobile hotspots (port 7844).

---

## Privacy

Everything runs locally. No data is sent to external servers. The system works fully air-gapped. Company documents, financial files, and databases are all git-ignored.

---

## License

Private project — all rights reserved.
