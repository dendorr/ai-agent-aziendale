import sys, os, re
from pathlib import Path
import chromadb, fitz

client = chromadb.PersistentClient(path="/home/ferra/ai-agent/chroma_disegni")
collection = client.get_or_create_collection("disegni")

SYSTEM_PROMPT = """Sei un assistente tecnico esperto in disegno architettonico e industriale.
Rispondi SEMPRE in italiano. Basati ESCLUSIVAMENTE sulle informazioni estratte dai file.
Indica sempre il file e l elemento di riferimento.
Non inventare mai dimensioni o specifiche non presenti nei file."""

def leggi_stp(f):
    try:
        testo = f"=== FILE STEP/STP: {Path(f).name} ===\n"
        contenuto = open(f, encoding="utf-8", errors="ignore").read()
        desc = re.findall(r"FILE_DESCRIPTION\s*\(\s*\((.*?)\)", contenuto)
        if desc: testo += f"Descrizione: {desc[0]}\n"
        schema = re.findall(r"FILE_SCHEMA\s*\(\s*\((.*?)\)\s*\)", contenuto)
        if schema: testo += f"Schema: {schema[0]}\n"
        entita = {
            "PRODUCT": "Prodotti",
            "MANIFOLD_SOLID_BREP": "Solidi",
            "ADVANCED_FACE": "Facce",
            "EDGE_CURVE": "Spigoli",
            "CYLINDRICAL_SURFACE": "Superfici cilindriche",
            "PLANE": "Piani",
            "AXIS2_PLACEMENT_3D": "Sistemi riferimento 3D",
        }
        testo += "\n[ENTITA GEOMETRICHE]\n"
        for codice, nome in entita.items():
            n = contenuto.count(codice + "(") + contenuto.count(codice + " (")
            if n > 0: testo += f"  {nome}: {n}\n"
        nomi = re.findall(r"PRODUCT\s*\(\s*'([^']*)'", contenuto)
        if nomi:
            testo += "\n[COMPONENTI]\n"
            for nome in list(set(nomi))[:30]:
                if nome.strip(): testo += f"  - {nome}\n"
        materiali = re.findall(r"MATERIAL\s*\(\s*'([^']*)'", contenuto)
        if materiali:
            testo += "\n[MATERIALI]\n"
            for mat in list(set(materiali))[:20]:
                if mat.strip(): testo += f"  - {mat}\n"
        if "MILLIMETRE" in contenuto or "MILLIMETER" in contenuto:
            testo += "\nUnita di misura: millimetri\n"
        elif "METRE" in contenuto:
            testo += "\nUnita di misura: metri\n"
        elif "INCH" in contenuto:
            testo += "\nUnita di misura: pollici\n"
        testo += f"\nDimensione file: {Path(f).stat().st_size / 1024:.1f} KB\n"
        testo += f"Righe totali: {contenuto.count(chr(10))}\n"
        return testo
    except Exception as e:
        return f"[Errore STP: {e}]"

def leggi_dxf(f):
    try:
        import ezdxf
        doc = ezdxf.readfile(f)
        info = f"=== FILE DXF: {Path(f).name} ===\n"
        info += f"Versione: {doc.dxfversion}\n\n[LAYER]\n"
        for layer in doc.layers:
            info += f"  - {layer.dxf.name}\n"
        testi = []
        for e in doc.modelspace():
            if e.dxftype() == "TEXT" and e.dxf.text.strip():
                testi.append(e.dxf.text.strip())
            elif e.dxftype() == "MTEXT":
                t = e.plain_mtext().strip()
                if t: testi.append(t)
        if testi:
            info += "\n[TESTI E ANNOTAZIONI]\n"
            for t in testi[:50]: info += f"  {t}\n"
        return info
    except Exception as e:
        return f"[Errore DXF: {e}]"

def leggi_svg(f):
    try:
        from lxml import etree
        tree = etree.parse(f)
        root = tree.getroot()
        info = f"=== FILE SVG: {Path(f).name} ===\n"
        info += f"Larghezza: {root.get('width','N/D')}\n"
        info += f"Altezza: {root.get('height','N/D')}\n"
        testi = root.findall('.//{http://www.w3.org/2000/svg}text')
        if testi:
            info += "\n[TESTI]\n"
            for t in testi[:50]:
                if t.text and t.text.strip():
                    info += f"  {t.text.strip()}\n"
        return info
    except Exception as e:
        return f"[Errore SVG: {e}]"

def leggi_ifc(f):
    try:
        import ifcopenshell
        ifc = ifcopenshell.open(f)
        info = f"=== FILE IFC: {Path(f).name} ===\n"
        info += f"Schema: {ifc.schema}\n"
        progetti = ifc.by_type("IfcProject")
        if progetti:
            info += f"Progetto: {progetti[0].Name or 'N/D'}\n"
        piani = ifc.by_type("IfcBuildingStorey")
        if piani:
            info += "\n[PIANI]\n"
            for p in piani:
                info += f"  - {p.Name or 'Piano'}\n"
        for tipo, nome in [
            ("IfcWall","Pareti"), ("IfcSlab","Solai"),
            ("IfcDoor","Porte"), ("IfcWindow","Finestre"),
            ("IfcColumn","Colonne"), ("IfcBeam","Travi"),
        ]:
            els = ifc.by_type(tipo)
            if els: info += f"\n[{nome}]: {len(els)} elementi\n"
        materiali = ifc.by_type("IfcMaterial")
        if materiali:
            info += "\n[MATERIALI]\n"
            for m in materiali[:20]:
                info += f"  - {m.Name}\n"
        return info
    except ImportError:
        return f"[ifcopenshell non installato]"
    except Exception as e:
        return f"[Errore IFC: {e}]"

def leggi_stl(f):
    try:
        import numpy as np
        from stl import mesh
        m = mesh.Mesh.from_file(f)
        info = f"=== FILE STL: {Path(f).name} ===\n"
        info += f"Triangoli: {len(m.vectors)}\n"
        minc = m.vectors.min(axis=(0,1))
        maxc = m.vectors.max(axis=(0,1))
        info += f"Dimensioni bounding box:\n"
        info += f"  X: {minc[0]:.2f} -> {maxc[0]:.2f}\n"
        info += f"  Y: {minc[1]:.2f} -> {maxc[1]:.2f}\n"
        info += f"  Z: {minc[2]:.2f} -> {maxc[2]:.2f}\n"
        return info
    except Exception as e:
        return f"[Errore STL: {e}]"

def leggi_pdf(f):
    try:
        doc = fitz.open(f)
        info = f"=== PDF TECNICO: {Path(f).name} ===\n"
        for i, p in enumerate(doc):
            t = p.get_text()
            if t.strip(): info += f"\n[Pagina {i+1}]\n{t}\n"
        return info
    except Exception as e:
        return f"[Errore PDF: {e}]"

def leggi_file(f):
    ext = Path(f).suffix.lower()
    if ext in (".stp", ".step"):  return leggi_stp(f)
    elif ext == ".dxf":           return leggi_dxf(f)
    elif ext == ".svg":           return leggi_svg(f)
    elif ext == ".ifc":           return leggi_ifc(f)
    elif ext == ".stl":           return leggi_stl(f)
    elif ext == ".pdf":           return leggi_pdf(f)
    elif ext in (".txt", ".md"):  return open(f, encoding="utf-8", errors="ignore").read()
    return f"[Formato {ext} non supportato]"

ESTENSIONI = [".stp", ".step", ".dxf", ".svg", ".ifc", ".stl", ".pdf"]

def chunk_testo(testo):
    parole = testo.split()
    chunks, i = [], 0
    while i < len(parole):
        chunks.append(" ".join(parole[i:i+500]))
        i += 450
    return chunks or [""]

def indicizza_file(filepath):
    p = Path(filepath)
    if p.suffix.lower() not in ESTENSIONI: return 0
    testo = leggi_file(filepath)
    if not testo.strip(): return 0
    chunks = chunk_testo(testo)
    for i, chunk in enumerate(chunks):
        collection.upsert(
            documents=[chunk],
            ids=[f"{filepath}__c{i}"],
            metadatas=[{"filename": p.name, "path": str(filepath), "chunk": i}]
        )
    return len(chunks)

def indicizza_cartella(cartella):
    files = [f for f in Path(cartella).rglob("*")
             if f.is_file() and f.suffix.lower() in ESTENSIONI]
    print(f"[Disegni] Trovati {len(files)} file...")
    for f in files:
        n = indicizza_file(str(f))
        if n: print(f"  [OK] {f.name} -> {n} chunk")
        else: print(f"  [--] {f.name} -> saltato")
    print(f"[Disegni] Completato -- {collection.count()} chunk totali")

def cerca(domanda):
    if collection.count() == 0:
        return "[Nessun disegno tecnico indicizzato]"
    r = collection.query(
        query_texts=[domanda],
        n_results=min(5, collection.count())
    )
    contesto = ""
    for doc, meta in zip(r["documents"][0], r["metadatas"][0]):
        contesto += f"\n--- {meta['filename']} ---\n{doc[:2000]}\n"
    return contesto

def rispondi(domanda, contesto):
    import requests
    prompt = f"""{SYSTEM_PROMPT}

CONTENUTO FILE TECNICI:
{contesto}

DOMANDA: {domanda}

RISPOSTA TECNICA DETTAGLIATA:"""
    try:
        r = requests.post(
            "http://localhost:11434/api/generate",
            json={"model": "llama3.2:latest", "prompt": prompt, "stream": False},
            timeout=120
        )
        return r.json().get("response", "Errore risposta")
    except Exception as e:
        return f"Errore: {e}"

if __name__ == "__main__":
    cartella = "/mnt/c/Users/ferra/OneDrive - Università di Pavia/Desktop/ai-test"
    print(f"Indicizzando: {cartella}")
    indicizza_cartella(cartella)
    print("\nTest lettura diretta del file STP:")
    for f in Path(cartella).rglob("*.stp"):
        print(leggi_stp(str(f)))
    for f in Path(cartella).rglob("*.step"):
        print(leggi_stp(str(f)))
