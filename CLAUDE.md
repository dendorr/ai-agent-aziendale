# ai-agent-aziendale — Contesto progetto

## Scopo
Sistema di AI agent completamente locale (air-gapped) per un'azienda.
Nessuna chiamata a API cloud — tutto gira su Ollama in locale.

## Stack
- **LLM**: Ollama — `qwen2.5:7b` (testo), `llama3.2` (disegni), `nomic-embed-text` (embedding)
- **Vector DB**: ChromaDB (una collection per agente)
- **API**: FastAPI porta 8000, formato OpenAI-compatibile
- **File watcher**: watchdog, polling ogni 10s

## Architettura
```
File System → Watcher → Router estensione → Agente → Chunking 600 parole → ChromaDB
Query → Server → Agent.search() → ChromaDB → Semantic Card (lazy) → Ollama → Risposta IT
```

## Struttura file
- `config/config.py` — configurazione globale (modelli, path, ChromaDB)
- `memory/semantic_cards.json` — cache persistente semantic card
- `scripts/server.py` — server FastAPI
- `scripts/watcher.py` — daemon monitoraggio file
- `scripts/financial_agent.py` — agente Excel/CSV/PDF finanziari
- `scripts/documents_agent.py` — agente PDF/Word/PPTX
- `scripts/drawings_agent.py` — agente CAD/BIM (DXF, STP, IFC, STL)
- `scripts/semantic_analyzer.py` — generatore lazy semantic card, doppia cache
- `scripts/convert_dwg.py` — convertitore DWG→DXF via LibreCAD headless

## Tre agenti
| Agente | Estensioni | Feature |
|---|---|---|
| financial | .xlsx .xls .csv .pdf .txt | Colori Excel (verde=pagato, rosso=non pagato) |
| documents | .pdf .pptx .docx .txt .md | Citazioni con slide/sezione |
| drawings | .dxf .dwg .stp .ifc .svg .stl .pdf | Analisi geometrica, parsing BIM/IFC |

## Regole importanti
- Rispondi sempre in italiano
- Il sistema è air-gapped: non suggerire mai API cloud (OpenAI, Azure, ecc.)
- Le semantic card sono lazy e mai rigenerate — rispetta questa logica
- Deployment target: RTX 4090 su server aziendale (dev su RTX 4050 laptop)
- Quando modifichi un agente, verifica compatibilità con `semantic_analyzer.py`
