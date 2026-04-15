# Company AI Agent — Local Multi-Agent System

A fully local, air-gapped AI agent system designed for small and medium businesses.
No cloud, no subscriptions, no data ever leaves your network.

---

## What It Does

Three specialized AI agents, each expert in a specific document domain:

### Financial Agent
- Reads and analyzes company Excel files (invoices, payments, budgets, market reports)
- Understands color-coded cells (e.g. green = paid, red = unpaid, yellow = pending)
- Handles complex multi-sheet Excel files with hundreds of rows
- Builds a local SQLite database from large Excel files for fast structured queries
- Answers natural language questions about financial data in Italian

### Drawings Agent
- Reads and analyzes technical CAD files: DXF, STP/STEP, IFC, SVG, STL
- Extracts geometric information: solids, faces, layers, materials, units of measure
- Provides dimensions, component lists, and structural descriptions
- Automatically converts DWG to DXF via LibreCAD
- Supports BIM files (IFC) with floors, walls, doors, windows

### Documents Agent
- Reads company documents: PDF, PowerPoint (PPTX), Word (DOCX), Markdown
- Extracts text, tables, slide notes, headings, and structured content
- Answers questions about reports, manuals, procedures, and presentations
- Always cites the source document and page/slide number

---

## Internal Memory System

All three agents use a lazy semantic card system:
- On first query, the AI generates a structured summary for each file
- Summaries are cached locally — no re-analysis on subsequent queries
- The system remembers what is in your files across sessions
- Memory is stored in JSON and ChromaDB vector databases

---

## Architecture

```
User (Open WebUI) -> FastAPI Server (port 8000) -> Agent Router
                                                    |- Financial Agent -> ChromaDB + SQLite
                                                    |- Drawings Agent  -> ChromaDB
                                                    |- Documents Agent -> ChromaDB
                                                           |
                                                    Ollama (local LLM)
```

---

## Tech Stack

| Component | Technology |
|---|---|
| LLM Runtime | Ollama |
| Models | qwen2.5:7b (dev) / qwen2.5:32b (production) |
| Chat Interface | Open WebUI via Docker |
| API Server | FastAPI (OpenAI-compatible) |
| Vector Database | ChromaDB (one collection per agent) |
| Structured Database | SQLite (for large tabular data) |
| Offline Knowledge | Kiwix Wikipedia (port 8080) |
| Language | Python 3.12 |

---

## Hardware

| Environment | Specs |
|---|---|
| Development | Samsung Galaxy Book 4 Ultra — RTX 4050, 16GB RAM, Windows 11 + WSL2/Ubuntu 24.04 |
| Production | Office server — RTX 4090 |

---

## Project Structure

```
ai-agent/
├── config/
│   └── config.py               # Central configuration
├── scripts/
│   ├── server.py               # FastAPI multi-agent server
│   ├── watcher.py              # Auto file watcher & indexer
│   ├── financial_agent.py      # Financial documents agent
│   ├── drawings_agent.py       # Technical drawings agent
│   ├── documents_agent.py      # Company documents agent
│   ├── semantic_analyzer.py    # Lazy semantic card engine
│   ├── db_builder.py           # SQLite builder for large Excel files
│   └── convert_dwg.py          # DWG to DXF converter
├── memory/
│   ├── semantic_cards.json     # Cached file summaries
│   └── data.db                 # SQLite structured data (git-ignored)
├── chroma/                     # Vector databases (git-ignored)
│   ├── financial/
│   ├── drawings/
│   └── documents/
└── logs/                       # Runtime logs (git-ignored)
```

---

## Quick Start

### 1. Prerequisites
```bash
# Install Ollama
curl -fsSL https://ollama.com/install.sh | sh

# Pull models
ollama pull qwen2.5:7b
ollama pull llama3.2

# Start Open WebUI
docker run -d -p 3000:3000 --name open-webui ghcr.io/open-webui/open-webui:main
```

### 2. Install dependencies
```bash
python3 -m venv ~/ai-env
source ~/ai-env/bin/activate
pip install -r requirements.txt
```

### 3. Start the system
```bash
source ~/ai-env/bin/activate
sudo systemctl restart ollama
docker start open-webui
cd ~/ai-agent/scripts
nohup python server.py  > ~/ai-agent/logs/server.log  2>&1 &
nohup python watcher.py > ~/ai-agent/logs/watcher.log 2>&1 &
```

### 4. Open the chat
Navigate to `http://localhost:3000` and select one of the three agents as your model.

---

## Privacy and Security

- 100% local — no data ever sent to external servers
- Air-gapped ready — works with no internet connection
- Company documents, financial data, and databases are git-ignored
- API server is not publicly exposed (firewall protected)
- OpenAPI docs disabled in production

---

## Remote Access (optional)

```bash
cloudflared tunnel --protocol http2 --url http://localhost:3000
```

---

## License

Private project — all rights reserved.
