import sys
import os
sys.path.append(os.path.expanduser("~/ai-agent"))

from config.config import CHROMA_DB_PATH
import chromadb
import requests
import json

client = chromadb.PersistentClient(path=CHROMA_DB_PATH)
collection = client.get_collection("documenti_aziendali")

def cerca_documenti(domanda, n_risultati=3):
    risultati = collection.query(
        query_texts=[domanda],
        n_results=min(n_risultati, collection.count())
    )
    return risultati

def chiedi_a_ollama(domanda, contesto):
    prompt = f"""Sei un assistente aziendale. Rispondi SEMPRE in italiano.
Usa solo le informazioni fornite nel contesto per rispondere.

CONTESTO DAI DOCUMENTI AZIENDALI:
{contesto}

DOMANDA: {domanda}

RISPOSTA:"""

    response = requests.post(
        "http://localhost:11434/api/generate",
        json={
            "model": "qwen2.5:7b",
            "prompt": prompt,
            "stream": False
        }
    )
    return response.json()["response"]

def agente(domanda):
    print(f"\nDomanda: {domanda}")
    print("Cerco nei documenti...")
    
    risultati = cerca_documenti(domanda)
    
    if not risultati["documents"][0]:
        print("Nessun documento trovato.")
        return
    
    contesto = ""
    for i, (doc, meta) in enumerate(zip(risultati["documents"][0], risultati["metadatas"][0])):
        contesto += f"\n--- File: {meta['filename']} ---\n{doc[:2000]}\n"
    
    print("Risposta:")
    risposta = chiedi_a_ollama(domanda, contesto)
    print(risposta)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        domanda = " ".join(sys.argv[1:])
    else:
        domanda = input("Fai una domanda sui tuoi documenti: ")
    agente(domanda)
