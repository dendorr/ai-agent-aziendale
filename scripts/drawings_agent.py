"""
DRAWINGS AGENT — with Lazy Semantic Cards
Handles: STP, STEP, DXF, SVG, IFC, STL, PDF (technical)
Semantic cards generated on first query, cached forever.
"""
import sys, os, re
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import (CHROMA_PATHS, OLLAMA_URL, LLM_MODEL_FAST,
                            CHUNK_SIZE, CHUNK_OVERLAP, EXTENSIONS)
from pathlib import Path
import chromadb, fitz
import semantic_analyzer as analyzer

client     = chromadb.PersistentClient(path=CHROMA_PATHS["drawings"])
collection = client.get_or_create_collection("drawings")

SYSTEM_PROMPT = """You are a technical assistant specialized in architectural and industrial drawing.
You analyze CAD, BIM, and technical documents: DXF, IFC, SVG, STP, STL, technical PDFs.

Always reply in Italian. Base your answers EXCLUSIVELY on information extracted from the files.
Always cite the source file and the relevant element.
Never invent dimensions or specifications not present in the files.

When identifying components:
- Use geometric data (solids, faces, cylindrical surfaces) to infer the object type
- Cylindrical surfaces suggest rotational parts (screws, pins, shafts, bushings)
- Planar surfaces suggest prismatic parts (plates, brackets, frames)
- High face count = complex geometry
- IFC files contain building elements — describe floors, walls, materials
- DXF files contain 2D drawings — describe layers and annotations"""

def read_stp(f):
    try:
        text    = f"=== STEP/STP FILE: {Path(f).name} ===\n"
        content = open(f, encoding="utf-8", errors="ignore").read()
        desc = re.findall(r"FILE_DESCRIPTION\s*\(\s*\((.*?)\)", content)
        if desc: text += f"Description: {desc[0]}\n"
        schema = re.findall(r"FILE_SCHEMA\s*\(\s*\((.*?)\)\s*\)", content)
        if schema: text += f"Schema: {schema[0]}\n"
        entities = {
            "PRODUCT":             "Products/Components",
            "MANIFOLD_SOLID_BREP": "Solids",
            "ADVANCED_FACE":       "Faces",
            "EDGE_CURVE":          "Edges",
            "CYLINDRICAL_SURFACE": "Cylindrical surfaces",
            "CONICAL_SURFACE":     "Conical surfaces",
            "SPHERICAL_SURFACE":   "Spherical surfaces",
            "TOROIDAL_SURFACE":    "Toroidal surfaces",
            "PLANE":               "Planes",
            "AXIS2_PLACEMENT_3D":  "3D reference frames",
            "B_SPLINE_SURFACE":    "B-spline surfaces",
        }
        text += "\n[GEOMETRIC ENTITIES]\n"
        for code, name in entities.items():
            n = content.count(code + "(") + content.count(code + " (")
            if n > 0: text += f"  {name}: {n}\n"
        names = re.findall(r"PRODUCT\s*\(\s*'([^']*)'", content)
        if names:
            text += "\n[COMPONENTS/PRODUCTS]\n"
            for name in list(set(names))[:30]:
                if name.strip(): text += f"  - {name}\n"
        materials = re.findall(r"MATERIAL\s*\(\s*'([^']*)'", content)
        if materials:
            text += "\n[MATERIALS]\n"
            for mat in list(set(materials))[:20]:
                if mat.strip(): text += f"  - {mat}\n"
        if "MILLIMETRE" in content or "MILLIMETER" in content:
            text += "\nUnit of measure: millimeters\n"
        elif "METRE" in content or "METER" in content:
            text += "\nUnit of measure: meters\n"
        elif "INCH" in content:
            text += "\nUnit of measure: inches\n"
        solids   = content.count("MANIFOLD_SOLID_BREP(") + content.count("MANIFOLD_SOLID_BREP (")
        faces    = content.count("ADVANCED_FACE(") + content.count("ADVANCED_FACE (")
        cyl_surf = content.count("CYLINDRICAL_SURFACE(") + content.count("CYLINDRICAL_SURFACE (")
        planes   = content.count("PLANE(") + content.count("PLANE (")
        text += "\n[GEOMETRIC ANALYSIS]\n"
        if solids > 0: text += f"  Distinct solids: {solids}\n"
        if faces > 0 and solids > 0: text += f"  Avg faces per solid: {faces // solids}\n"
        if faces > 0:
            if cyl_surf > 0:
                text += f"  Cylindrical ratio: {round(cyl_surf/faces*100,1)}% — likely rotational/machined part\n"
            if planes > 0:
                text += f"  Planar ratio: {round(planes/faces*100,1)}% — likely prismatic/structural part\n"
        text += f"\nFile size: {Path(f).stat().st_size / 1024:.1f} KB\n"
        text += f"Total lines: {content.count(chr(10))}\n"
        return text
    except Exception as e:
        return f"[Error reading STP {Path(f).name}: {e}]"

def read_dxf(f):
    try:
        import ezdxf
        doc  = ezdxf.readfile(f)
        info = f"=== DXF FILE: {Path(f).name} ===\n"
        info += f"AutoCAD version: {doc.dxfversion}\n"
        units = {0:"Unitless",1:"Inches",2:"Feet",4:"mm",5:"cm",6:"m"}
        if doc.header.get('$INSUNITS'):
            info += f"Units: {units.get(doc.header['$INSUNITS'], 'unknown')}\n"
        info += "\n[LAYERS]\n"
        for layer in doc.layers:
            info += f"  - {layer.dxf.name}\n"
        msp   = doc.modelspace()
        texts, dims, blocks = [], [], []
        for e in msp:
            if e.dxftype() == "TEXT" and e.dxf.text.strip():
                texts.append(e.dxf.text.strip())
            elif e.dxftype() == "MTEXT":
                t = e.plain_mtext().strip()
                if t: texts.append(t)
            elif e.dxftype() == "DIMENSION":
                try: dims.append(str(e.dxf.text or ""))
                except Exception: pass
            elif e.dxftype() == "INSERT":
                try: blocks.append(e.dxf.name)
                except Exception: pass
        if texts:
            info += "\n[TEXTS AND ANNOTATIONS]\n"
            for t in texts[:100]: info += f"  {t}\n"
        if dims:
            info += "\n[DIMENSIONS]\n"
            for d in dims[:50]: info += f"  {d}\n"
        if blocks:
            info += "\n[BLOCKS/COMPONENTS]\n"
            for b in list(set(blocks))[:30]: info += f"  - {b}\n"
        return info
    except Exception as e:
        return f"[Error reading DXF: {e}]"

def read_svg(f):
    try:
        from lxml import etree
        tree = etree.parse(f)
        root = tree.getroot()
        info = f"=== SVG FILE: {Path(f).name} ===\n"
        info += f"Width: {root.get('width','N/A')}\n"
        info += f"Height: {root.get('height','N/A')}\n"
        info += f"ViewBox: {root.get('viewBox','N/A')}\n"
        texts = root.findall('.//{http://www.w3.org/2000/svg}text')
        if texts:
            info += "\n[TEXTS]\n"
            for t in texts[:50]:
                if t.text and t.text.strip(): info += f"  {t.text.strip()}\n"
        groups = root.findall('.//{http://www.w3.org/2000/svg}g')
        layer_ids = [g.get('id') for g in groups if g.get('id')]
        if layer_ids:
            info += "\n[LAYERS/GROUPS]\n"
            for lid in layer_ids[:30]: info += f"  - {lid}\n"
        return info
    except Exception as e:
        return f"[Error reading SVG: {e}]"

def read_ifc(f):
    try:
        import ifcopenshell
        ifc  = ifcopenshell.open(f)
        info = f"=== IFC FILE: {Path(f).name} ===\n"
        info += f"IFC Schema: {ifc.schema}\n"
        projects = ifc.by_type("IfcProject")
        if projects:
            p = projects[0]
            info += f"Project: {p.Name or 'N/A'}\n"
            if p.Description: info += f"Description: {p.Description}\n"
        floors = ifc.by_type("IfcBuildingStorey")
        if floors:
            info += "\n[FLOORS]\n"
            for floor in floors:
                info += f"  - {floor.Name or 'Unnamed'}"
                if hasattr(floor, 'Elevation') and floor.Elevation:
                    info += f" — elevation: {floor.Elevation:.2f}m"
                info += "\n"
        for ifc_type, name in [
            ("IfcWall","Walls"),("IfcSlab","Slabs"),("IfcColumn","Columns"),
            ("IfcBeam","Beams"),("IfcDoor","Doors"),("IfcWindow","Windows"),
            ("IfcStair","Stairs"),("IfcRoof","Roofs"),("IfcSpace","Spaces"),
            ("IfcFurniture","Furniture"),
        ]:
            els = ifc.by_type(ifc_type)
            if els:
                info += f"\n[{name.upper()}] — {len(els)} elements\n"
                for el in els[:15]: info += f"  - {el.Name or ifc_type}\n"
        materials = ifc.by_type("IfcMaterial")
        if materials:
            info += "\n[MATERIALS]\n"
            for mat in materials[:30]: info += f"  - {mat.Name}\n"
        return info
    except ImportError:
        return f"[ifcopenshell not installed]"
    except Exception as e:
        return f"[Error reading IFC: {e}]"

def read_stl(f):
    try:
        from stl import mesh
        m    = mesh.Mesh.from_file(f)
        info = f"=== STL FILE: {Path(f).name} ===\n"
        info += f"Triangles: {len(m.vectors)}\n"
        min_c = m.vectors.min(axis=(0,1))
        max_c = m.vectors.max(axis=(0,1))
        dims  = max_c - min_c
        info += f"Bounding box: X={dims[0]:.2f}, Y={dims[1]:.2f}, Z={dims[2]:.2f}\n"
        try:
            vol, _, _ = m.get_mass_properties()
            info += f"Approximate volume: {vol:.4f}\n"
        except Exception: pass
        return info
    except Exception as e:
        return f"[Error reading STL: {e}]"

def read_pdf(f):
    try:
        doc  = fitz.open(f)
        info = f"=== TECHNICAL PDF: {Path(f).name} ===\n"
        for i, page in enumerate(doc):
            t = page.get_text()
            if t.strip(): info += f"\n[Page {i+1}]\n{t}\n"
        return info
    except Exception as e:
        return f"[Error reading PDF: {e}]"

def read_file(f):
    ext = Path(f).suffix.lower()
    if ext in (".stp", ".step"):  return read_stp(f)
    elif ext == ".dxf":           return read_dxf(f)
    elif ext == ".svg":           return read_svg(f)
    elif ext == ".ifc":           return read_ifc(f)
    elif ext == ".stl":           return read_stl(f)
    elif ext == ".pdf":           return read_pdf(f)
    elif ext in (".txt", ".md"):  return open(f, encoding="utf-8", errors="ignore").read()
    return f"[Format {ext} not supported by drawings agent]"

def chunk_text(text):
    words  = text.split()
    chunks = []
    i      = 0
    while i < len(words):
        chunks.append(" ".join(words[i:i + CHUNK_SIZE]))
        i += CHUNK_SIZE - CHUNK_OVERLAP
    return chunks or [""]

def index_file(filepath):
    """Index file — fast, no AI, only raw text extraction."""
    p   = Path(filepath)
    ext = p.suffix.lower()
    if ext not in EXTENSIONS["drawings"]: return 0
    text = read_file(filepath)
    if not text.strip(): return 0
    chunks = chunk_text(text)
    for i, chunk in enumerate(chunks):
        collection.upsert(
            documents=[chunk],
            ids=[f"{filepath}__c{i}"],
            metadatas=[{
                "filename": p.name,
                "path":     str(filepath),
                "chunk":    i,
                "agent":    "drawings",
                "type":     "chunk"
            }]
        )
    return len(chunks)

def index_folder(folder):
    files = [f for f in Path(folder).rglob("*")
             if f.is_file() and f.suffix.lower() in EXTENSIONS["drawings"]]
    print(f"[Drawings] Found {len(files)} files...")
    total = 0
    for f in files:
        n = index_file(str(f))
        if n: print(f"  [OK] {f.name} -> {n} chunks")
        else: print(f"  [--] {f.name} -> skipped")
        total += n
    print(f"[Drawings] Done — {total} total chunks")

def search(query):
    """Search with lazy semantic card generation."""
    return analyzer.search_with_cards(collection, query, "drawings", n_results=5)

def answer(question, context):
    import requests
    prompt = f"""{SYSTEM_PROMPT}

{context}

QUESTION: {question}

DETAILED TECHNICAL ANSWER (in Italian):"""
    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": LLM_MODEL_FAST, "prompt": prompt, "stream": False},
            timeout=180
        )
        r.raise_for_status()
        return r.json().get("response", "Error in model response")
    except requests.exceptions.Timeout:
        return "Timeout — riprova con una domanda più specifica."
    except Exception as e:
        return f"Error: {e}"

if __name__ == "__main__":
    import sys
    folder = sys.argv[1] if len(sys.argv) > 1 else "."
    print(f"Indexing drawings in: {folder}")
    index_folder(folder)