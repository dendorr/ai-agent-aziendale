import sys, os, logging
sys.path.insert(0, os.path.expanduser("~/ai-agent"))
from config.config import CHROMA_DB_PATH, OLLAMA_URL
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import requests
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("server")
app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
import agente_disegni as dis
import chromadb
client_rag = chromadb.PersistentClient(path=CHROMA_DB_PATH)
try:
    col_rag = client_rag.get_collection("documenti_aziendali")
except Exception:
    col_rag = client_rag.get_or_create_collection("documenti_aziendali")
def cerca_rag(domanda):
    if col_rag.count() == 0:
        return "[Nessun documento indicizzato]"
    r = col_rag.query(query_texts=[domanda], n_results=min(5, col_rag.count()))
    contesto = ""
    for doc, meta in zip(r["documents"][0], r["metadatas"][0]):
        contesto += f"\n--- {meta['filename']} ---\n{doc[:1500]}\n"
    return contesto
def rispondi_rag(domanda, contesto):
    prompt = f"""Sei un assistente aziendale. Rispondi SEMPRE in italiano.
Basati ESCLUSIVAMENTE sui documenti forniti.
DOCUMENTI:\n{contesto}\nDOMANDA: {domanda}\nRISPOSTA:"""
    try:
        r = requests.post(f"{OLLAMA_URL}/api/generate",
            json={"model": "llama3.2:latest", "prompt": prompt, "stream": False}, timeout=120)
        return r.json().get("response", "Errore")
    except Exception as e:
        return f"Errore: {e}"
class Message(BaseModel):
    role: str
    content: str
class ChatRequest(BaseModel):
    model: str
    messages: List[Message]
    stream: bool = False
@app.get("/health")
def health():
    return {"status": "ok", "agente-aziendale": col_rag.count(), "agente-disegni": dis.collection.count()}
@app.get("/v1/models")
def models():
    return {"object": "list", "data": [
        {"id": "agente-aziendale", "object": "model"},
        {"id": "agente-disegni", "object": "model"},
    ]}
@app.post("/v1/chat/completions")
def chat(req: ChatRequest):
    if not req.messages:
        raise HTTPException(status_code=400, detail="Nessun messaggio")
    domanda = req.messages[-1].content
    if req.model == "agente-disegni":
        risposta = dis.rispondi(domanda, dis.cerca(domanda))
    elif req.model == "agente-aziendale":
        risposta = rispondi_rag(domanda, cerca_rag(domanda))
    else:
        raise HTTPException(status_code=404, detail=f"Agente non trovato")
    logger.info(f"[{req.model}] {domanda[:60]}...")
    return {"id": f"{req.model}-001", "object": "chat.completion", "model": req.model,
            "choices": [{"message": {"role": "assistant", "content": risposta}, "finish_reason": "stop", "index": 0}]}
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
