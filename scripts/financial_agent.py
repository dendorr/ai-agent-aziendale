"""
FINANCIAL AGENT v9 — Universal financial document intelligence

Zero hardcoded assumptions about file structure, content or domain.
Multi-model pipeline:
  - ROUTING_MODEL (qwen3:0.6b)  → selects relevant tables, generates SQL
  - ANSWER_MODEL  (qwen2.5:7b)  → produces the final answer in Italian

Excel enhancements over v8:
  - Auto-detect header row (handles title rows before actual headers)
  - Date cells converted to ISO strings (no more serial-number floats)
  - Merged cells: master value propagated to all child cells
  - Color sampling limited to first 500 rows (speed)
  - Single workbook open — no more double-open

PDF: markitdown → fitz → pdfplumber (three-level fallback)
"""

import sys, os, json, sqlite3, re, hashlib
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import (CHROMA_PATHS, OLLAMA_URL, LLM_MODEL,
                            CHUNK_SIZE, CHUNK_OVERLAP, EXTENSIONS, MEMORY_PATH)
from pathlib import Path
import chromadb, fitz
import semantic_analyzer as analyzer

client     = chromadb.PersistentClient(path=CHROMA_PATHS["financial"])
collection = client.get_or_create_collection("financial")

MEMORY_FILE = Path(MEMORY_PATH) / "financial_memory.json"
GENERIC_DB  = Path(MEMORY_PATH) / "financial_files.db"

# ── Models ─────────────────────────────────────────────────────────────────────
ROUTING_MODEL = "qwen3:0.6b"   # fast: selects tables, generates SQL
ANSWER_MODEL  = LLM_MODEL       # accurate: final answer

# RGB values treated as "no background" (default/white/black)
_IGNORE_RGB = {"00000000", "FFFFFFFF", "00FFFFFF", "FF000000"}

# ── Low-level helpers ──────────────────────────────────────────────────────────

def sanitize_col(name: str) -> str:
    """Convert any string to a safe SQLite column name (max 50 chars)."""
    s = re.sub(r"[^a-zA-Z0-9_]", "_", str(name).strip()).strip("_")
    return (s or "col")[:50]


def table_name_for(filepath: str, sheet: str = "") -> str:
    """Generate a deterministic, collision-free SQLite table name."""
    h    = hashlib.md5(f"{filepath}_{sheet}".encode()).hexdigest()[:8]
    stem = re.sub(r"[^a-zA-Z0-9]", "_", Path(filepath).stem[:20]).strip("_")
    return f"f_{stem}_{h}".lower()


def query_db(db_path, sql: str, params=()):
    """Execute a read-only query on any SQLite database. Returns list of dicts."""
    if not Path(db_path).exists():
        return []
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
    except Exception as e:
        return [{"error": str(e)}]
    finally:
        conn.close()

# ── Generic SQLite (one DB, one table per file + sheet) ────────────────────────

def _open_generic_db():
    GENERIC_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(GENERIC_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _file_schemas (
            schema_id  TEXT PRIMARY KEY,
            filepath   TEXT,
            filename   TEXT,
            sheet      TEXT,
            table_name TEXT,
            col_names  TEXT,
            col_raw    TEXT,
            col_types  TEXT,
            col_stats  TEXT,
            color_info TEXT,
            row_count  INTEGER,
            indexed_at TEXT
        )
    """)
    conn.commit()
    return conn

# ── Excel cell helpers ─────────────────────────────────────────────────────────

def _cell_value(cell):
    """
    Extract a clean Python value from an openpyxl cell.
    - Formulas  → None  (data_only=True resolves them; raw = strings starting with '=')
    - datetime  → ISO string YYYY-MM-DD
    - Everything else → raw value
    """
    import datetime as _dt
    v = cell.value
    if v is None:
        return None
    if isinstance(v, str) and v.strip().startswith("="):
        return None
    if isinstance(v, (_dt.datetime, _dt.date)):
        return v.strftime("%Y-%m-%d")
    return v


def _cell_rgb(cell):
    """Return the hex background RGB of a cell, or None if default/white/black."""
    try:
        rgb = cell.fill.fgColor.rgb
        if rgb and rgb.upper() not in _IGNORE_RGB:
            return rgb.upper()
    except Exception:
        pass
    return None

# ── Excel structure helpers ────────────────────────────────────────────────────

def _detect_header_row(ws, max_scan: int = 5) -> int:
    """
    Scan the first max_scan rows and return the 1-based index of the row
    with the highest number of non-null cells.
    Handles files with one or more title / logo rows above the actual column headers.
    """
    best_row, best_count = 1, 0
    for i, row in enumerate(ws.iter_rows(min_row=1, max_row=max_scan), 1):
        count = sum(1 for c in row if c.value is not None)
        if count > best_count:
            best_count, best_row = count, i
    return best_row


def _build_merge_map(ws) -> dict:
    """
    Return {(row, col): value} for every non-master cell in every merge range.
    The value is inherited from the top-left (master) cell of the range.
    Silently skips if the workbook was opened in read_only mode.
    """
    merge_map = {}
    try:
        for rng in ws.merged_cells.ranges:
            master_val = ws.cell(row=rng.min_row, column=rng.min_col).value
            for row in range(rng.min_row, rng.max_row + 1):
                for col in range(rng.min_col, rng.max_col + 1):
                    if (row, col) != (rng.min_row, rng.min_col):
                        merge_map[(row, col)] = master_val
    except Exception:
        pass
    return merge_map

# ── Column type detection ──────────────────────────────────────────────────────

def _detect_col_stats(df) -> tuple:
    """
    Auto-detect column types (numeric / text / empty) and compute statistics.
    A column is numeric when >= 70% of its non-null values parse as numbers.
    No assumptions made about column names or content domain.
    """
    import pandas as pd
    col_types, col_stats = {}, {}

    for col in df.columns:
        series = df[col].dropna()
        if series.empty:
            col_types[col] = "empty"
            continue

        num = pd.to_numeric(series, errors="coerce").dropna()
        if len(num) / len(series) >= 0.7:
            col_types[col] = "numeric"
            col_stats[col] = {
                "sum":     round(float(num.sum()),  4),
                "mean":    round(float(num.mean()), 4),
                "min":     round(float(num.min()),  4),
                "max":     round(float(num.max()),  4),
                "count":   int(num.count()),
                "nonzero": int((num != 0).sum()),
            }
        else:
            col_types[col] = "text"
            vc = series.astype(str).value_counts()
            col_stats[col] = {
                "unique":     int(series.nunique()),
                "top_values": vc.head(10).index.tolist(),
                "count":      int(series.count()),
            }

    return col_types, col_stats

# ── Sheet processor ────────────────────────────────────────────────────────────

def _process_sheet(ws, sname: str, color_sample: int = 500):
    """
    Process a single openpyxl worksheet.
    Returns a data dict or None if the sheet should be skipped.
    """
    import pandas as pd

    if ws.max_row is None or ws.max_row < 2:
        return None

    # Auto-detect the header row
    hdr_idx  = _detect_header_row(ws)
    hdr_row  = list(ws.iter_rows(min_row=hdr_idx, max_row=hdr_idx))[0]
    raw_hdrs = [c.value for c in hdr_row]

    if not any(h for h in raw_hdrs if h is not None):
        return None

    n_cols = len(raw_hdrs)

    # Sanitize column names, handle duplicates
    seen, san_hdrs = {}, []
    for i, h in enumerate(raw_hdrs):
        base = sanitize_col(h) if h is not None else f"col_{i}"
        cnt  = seen.get(base, 0)
        seen[base] = cnt + 1
        san_hdrs.append(f"{base}_{cnt}" if cnt else base)

    # Build merge map for value propagation
    merge_map  = _build_merge_map(ws)
    rows       = []
    color_info = {}   # sanitized_col -> {rgb: count}
    color_rows = 0

    for ri, row in enumerate(ws.iter_rows(min_row=hdr_idx + 1), hdr_idx + 1):
        vals = []
        for ci, cell in enumerate(row[:n_cols]):
            # Prefer merged-cell master value if this cell is a slave
            val = merge_map.get((ri, cell.column), _cell_value(cell))
            vals.append(val)

            # Sample colors only for the first color_sample rows (speed)
            if color_rows < color_sample and ci < len(san_hdrs):
                rgb = _cell_rgb(cell)
                if rgb:
                    col = san_hdrs[ci]
                    if col not in color_info:
                        color_info[col] = {}
                    color_info[col][rgb] = color_info[col].get(rgb, 0) + 1

        rows.append(vals + [None] * max(0, n_cols - len(vals)))
        if ri - hdr_idx <= color_sample:
            color_rows += 1

    if not rows:
        return None

    import pandas as pd
    df = pd.DataFrame(rows, columns=san_hdrs)
    col_types, col_stats = _detect_col_stats(df)

    return {
        "df":         df,
        "col_names":  san_hdrs,
        "col_raw":    [str(h) if h is not None else "" for h in raw_hdrs],
        "col_types":  col_types,
        "col_stats":  col_stats,
        "color_info": color_info,
    }


def _sheet_summary_lines(sname: str, data: dict) -> list:
    """Build human-readable summary lines for one sheet (stored in ChromaDB chunks)."""
    san_hdrs   = data["col_names"]
    raw_hdrs   = data["col_raw"]
    col_types  = data["col_types"]
    col_stats  = data["col_stats"]
    color_info = data["color_info"]
    df         = data["df"]

    raw_map  = dict(zip(san_hdrs, raw_hdrs))
    raw_str  = ", ".join(h for h in raw_hdrs if h)
    num_cols = [c for c in san_hdrs if col_types.get(c) == "numeric"]
    txt_cols = [c for c in san_hdrs if col_types.get(c) == "text"]

    lines = [f"[Foglio: {sname}] {len(df):,} righe | Colonne: {raw_str[:150]}"]

    if num_cols:
        lines.append("  Colonne numeriche:")
        for col in num_cols[:10]:
            s = col_stats[col]
            lines.append(
                f"    {raw_map.get(col, col)}: "
                f"sum={s['sum']:,.2f} | min={s['min']:,.2f} | "
                f"max={s['max']:,.2f} | n={s['count']}"
            )

    if txt_cols:
        lines.append("  Colonne testo:")
        for col in txt_cols[:10]:
            s   = col_stats[col]
            top = ", ".join(str(v) for v in s["top_values"][:5])
            lines.append(
                f"    {raw_map.get(col, col)}: {s['unique']} valori unici (es: {top})"
            )

    if color_info:
        lines.append("  Pattern colori (sfondo celle — significato da inferire dal contesto):")
        for col, colors in list(color_info.items())[:6]:
            lbl = raw_map.get(col, col)
            for rgb, count in sorted(colors.items(), key=lambda x: -x[1])[:4]:
                hex_c = f"#{rgb[2:] if len(rgb) == 8 else rgb}"
                lines.append(f"    {lbl}: {count} celle con sfondo {hex_c}")

    return lines

# ── Excel / CSV readers ────────────────────────────────────────────────────────

def read_excel_smart(filepath, max_rows: int = 50_000, color_sample: int = 500):
    """
    Read any Excel file.
    - Single workbook open (data_only=True)
    - Auto-header detection per sheet
    - Date handling, merge propagation, color sampling
    Returns (sheets_data, text_summary). Zero hardcoded assumptions.
    """
    import openpyxl
    p  = Path(filepath)
    wb = openpyxl.load_workbook(filepath, data_only=True)

    sheets_data   = {}
    summary_lines = [
        f"=== EXCEL: {p.name} ===",
        f"Fogli ({len(wb.sheetnames)}): {', '.join(wb.sheetnames[:30])}",
        "",
    ]

    for sname in wb.sheetnames:
        ws = wb[sname]
        try:
            data = _process_sheet(ws, sname, color_sample=color_sample)
        except Exception as e:
            summary_lines.append(f"[Foglio: {sname}] ERRORE: {e}")
            continue

        if data is None:
            continue

        sheets_data[sname] = data
        summary_lines.extend(_sheet_summary_lines(sname, data))
        summary_lines.append("")

    wb.close()
    return sheets_data, "\n".join(summary_lines)


def read_csv_smart(filepath, max_rows: int = 50_000):
    """
    Read any CSV with auto-detected encoding and fully dynamic schema.
    Returns (sheets_data, text_summary).
    """
    import pandas as pd
    p  = Path(filepath)
    df = None

    for enc in ["utf-8", "latin-1", "cp1252"]:
        try:
            df = pd.read_csv(filepath, encoding=enc, nrows=max_rows)
            break
        except UnicodeDecodeError:
            continue
        except Exception as e:
            return {}, f"[CSV error: {e}]"

    if df is None:
        return {}, f"[Impossibile leggere {p.name}]"

    seen, san_map = {}, {}
    for h in df.columns:
        base = sanitize_col(h)
        cnt  = seen.get(base, 0)
        seen[base] = cnt + 1
        san_map[h] = f"{base}_{cnt}" if cnt else base

    raw_names = list(df.columns)
    df        = df.rename(columns=san_map)
    col_types, col_stats = _detect_col_stats(df)

    sheets_data = {"CSV": {
        "df":         df,
        "col_names":  list(df.columns),
        "col_raw":    raw_names,
        "col_types":  col_types,
        "col_stats":  col_stats,
        "color_info": {},
    }}

    raw_map  = dict(zip(list(df.columns), raw_names))
    num_cols = [c for c in df.columns if col_types.get(c) == "numeric"]
    txt_cols = [c for c in df.columns if col_types.get(c) == "text"]

    lines = [
        f"=== CSV: {p.name} ===",
        f"Righe: {len(df):,} | Colonne: {', '.join(raw_names[:30])}",
        "",
    ]
    if num_cols:
        lines.append("Colonne numeriche:")
        for col in num_cols[:10]:
            s = col_stats[col]
            lines.append(
                f"  {raw_map.get(col, col)}: "
                f"sum={s['sum']:,.2f} | min={s['min']:.2f} | max={s['max']:.2f}"
            )
    if txt_cols:
        lines.append("Colonne testo:")
        for col in txt_cols[:10]:
            s   = col_stats[col]
            top = ", ".join(str(v) for v in s["top_values"][:5])
            lines.append(
                f"  {raw_map.get(col, col)}: {s['unique']} valori unici (es: {top})"
            )

    return sheets_data, "\n".join(lines)

# ── Generic DB builder ─────────────────────────────────────────────────────────

def build_file_db(filepath, sheets_data) -> list:
    """
    Persist every sheet of an Excel/CSV into the generic SQLite DB.
    Each sheet gets its own table; schema metadata saved to _file_schemas.
    Returns list of (table_name, sheet_name, row_count).
    """
    import pandas as pd
    conn  = _open_generic_db()
    p     = Path(filepath)
    fhash = hashlib.md5(str(filepath).encode()).hexdigest()[:12]
    created = []

    for sname, data in sheets_data.items():
        df         = data["df"]
        col_names  = data["col_names"]
        col_raw    = data["col_raw"]
        col_types  = data["col_types"]
        col_stats  = data["col_stats"]
        color_info = data.get("color_info", {})

        if df is None or df.empty:
            continue

        tname     = table_name_for(filepath, sname)
        schema_id = f"{fhash}_{sname}"

        # Drop + recreate table
        conn.execute(f'DROP TABLE IF EXISTS "{tname}"')
        col_defs = [
            f'"{col}" {"REAL" if col_types.get(col) == "numeric" else "TEXT"}'
            for col in col_names
        ]
        conn.execute(f'CREATE TABLE "{tname}" ({", ".join(col_defs)})')

        # Coerce types
        for col in col_names:
            if col_types.get(col) == "numeric":
                df[col] = pd.to_numeric(df[col], errors="coerce")
            else:
                df[col] = (df[col].fillna("")
                                  .astype(str)
                                  .str.strip()
                                  .replace({"nan": "", "None": "", "NaT": ""}))

        # Batch insert
        placeholders = ", ".join(["?"] * len(col_names))
        insert_sql   = f'INSERT INTO "{tname}" VALUES ({placeholders})'
        batch = []
        for row in df.itertuples(index=False):
            vals = []
            for col, v in zip(col_names, row):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    vals.append(None)
                elif col_types.get(col) == "numeric":
                    try:    vals.append(float(v))
                    except: vals.append(None)
                else:
                    vals.append(str(v) if v is not None else "")
            batch.append(vals)
            if len(batch) >= 1000:
                conn.executemany(insert_sql, batch)
                batch = []
        if batch:
            conn.executemany(insert_sql, batch)

        # Index low-cardinality text columns
        for col in col_names:
            if col_types.get(col) == "text":
                if col_stats.get(col, {}).get("unique", 9999) < 500:
                    idx = f"idx_{tname[:18]}_{col[:12]}"
                    try:
                        conn.execute(
                            f'CREATE INDEX IF NOT EXISTS "{idx}" ON "{tname}"("{col}")'
                        )
                    except Exception:
                        pass

        # Persist schema metadata
        conn.execute("""
            INSERT OR REPLACE INTO _file_schemas
            (schema_id, filepath, filename, sheet, table_name,
             col_names, col_raw, col_types, col_stats, color_info,
             row_count, indexed_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
        """, (
            schema_id, str(filepath), p.name, sname, tname,
            json.dumps(col_names,  ensure_ascii=False),
            json.dumps(col_raw,    ensure_ascii=False),
            json.dumps(col_types,  ensure_ascii=False),
            json.dumps(col_stats,  ensure_ascii=False),
            json.dumps(color_info, ensure_ascii=False),
            len(df),
        ))
        created.append((tname, sname, len(df)))

    conn.commit()
    conn.close()
    return created

# ── Schema retrieval ───────────────────────────────────────────────────────────

def get_all_file_schemas() -> list:
    """Return all indexed file schemas from the generic DB, with parsed JSON fields."""
    if not GENERIC_DB.exists():
        return []
    rows = query_db(GENERIC_DB, "SELECT * FROM _file_schemas ORDER BY indexed_at DESC")
    result = []
    for r in rows:
        s = dict(r)
        for key in ("col_names", "col_raw", "col_types", "col_stats", "color_info"):
            try:    s[f"_{key}"] = json.loads(s.get(key) or "null") or {}
            except: s[f"_{key}"] = {}
        result.append(s)
    return result

# ── Routing model ──────────────────────────────────────────────────────────────

def route_query(query: str, schemas: list) -> list:
    """
    Use the fast ROUTING_MODEL to select relevant tables and generate SQL queries
    tailored to the user's question.

    Returns a list of {"table": str, "sql": str} dicts.
    Falls back to empty list on any error — adaptive_sql_context handles the fallback.
    """
    if not schemas:
        return []

    import requests

    schema_lines = []
    for s in schemas[:8]:
        col_names = s.get("_col_names") or []
        col_raw   = s.get("_col_raw")   or col_names
        col_types = s.get("_col_types") or {}
        raw_map   = dict(zip(col_names, col_raw)) if len(col_names) == len(col_raw) else {}

        num_cols = [raw_map.get(c, c) for c in col_names if col_types.get(c) == "numeric"][:6]
        txt_cols = [raw_map.get(c, c) for c in col_names if col_types.get(c) == "text"][:6]

        schema_lines.append(
            f"  tabella={s['table_name']} | file={s['filename']} | "
            f"foglio={s['sheet']} | righe={s['row_count']} | "
            f"colonne_numeriche=[{', '.join(str(c) for c in num_cols)}] | "
            f"colonne_testo=[{', '.join(str(c) for c in txt_cols)}]"
        )

    prompt = f"""Sei un analista SQL esperto. Seleziona le tabelle rilevanti e genera query SQL precise.

TABELLE DISPONIBILI:
{chr(10).join(schema_lines)}

DOMANDA UTENTE: {query}

Genera SOLO un JSON array (massimo 3 query). Ogni elemento deve avere:
{{"table": "nome_tabella_esatto", "sql": "SELECT ... FROM \\"nome_tabella\\" ..."}}

Regole SQL obbligatorie:
- Nomi tabella e colonna ESATTAMENTE come indicati, tra doppi apici
- Per totali/somme: SUM("colonna")
- Per distribuzioni: GROUP BY "colonna" ORDER BY totale DESC
- Per ricerche testuali: WHERE "colonna" LIKE '%valore%'
- LIMIT 30 in ogni query
- Solo JSON valido, zero testo fuori dal JSON"""

    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": ROUTING_MODEL, "prompt": prompt, "stream": False},
            timeout=30,
        )
        r.raise_for_status()
        raw   = r.json().get("response", "[]")
        match = re.search(r'\[.*?\]', raw, re.DOTALL)
        if match:
            parsed = json.loads(match.group())
            if isinstance(parsed, list):
                return parsed
    except Exception as e:
        print(f"  [routing] {e}", flush=True)

    return []

# ── Adaptive SQL context ───────────────────────────────────────────────────────

def adaptive_sql_context(query: str, schemas: list) -> str:
    """
    Build rich analytical context from the actual file schemas stored in SQLite.

    Two-phase approach:
      Phase 1 — Routing model generates and runs targeted SQL queries
      Phase 2 — Generic fallback: pre-computed stats + group-by for all tables
                (supplementary, always shown so the LLM has full schema visibility)
    """
    if not schemas:
        return ""

    parts         = []
    routed_tables = set()

    # ── Phase 1: routing model SQL ────────────────────────────────────────────
    routed = route_query(query, schemas)

    for item in routed:
        tname = item.get("table", "")
        sql   = item.get("sql", "")
        if not tname or not sql:
            continue

        rows = query_db(GENERIC_DB, sql)
        if not rows or "error" in rows[0]:
            continue

        sch   = next((s for s in schemas if s["table_name"] == tname), None)
        fname = sch["filename"] if sch else tname
        sheet = sch["sheet"]    if sch else ""

        header = f"\n[Query → {fname}"
        if sheet and sheet != "CSV":
            header += f" | {sheet}"
        header += "]"
        parts.append(header)
        parts.append(f"SQL: {sql[:250]}")

        col_keys = list(rows[0].keys())
        parts.append(" | ".join(col_keys[:10]))
        for row in rows[:30]:
            parts.append("  " + " | ".join(str(row.get(k, "")) for k in col_keys[:10]))

        routed_tables.add(tname)

    # ── Phase 2: generic stats for all tables ─────────────────────────────────
    for sch in schemas[:6]:
        tname      = sch["table_name"]
        fname      = sch["filename"]
        sheet      = sch["sheet"]
        col_names  = sch.get("_col_names") or []
        col_raw    = sch.get("_col_raw")   or col_names
        col_types  = sch.get("_col_types") or {}
        col_stats  = sch.get("_col_stats") or {}
        color_info = sch.get("_color_info") or {}

        if not col_names:
            continue

        raw_map  = dict(zip(col_names, col_raw)) if len(col_names) == len(col_raw) else {}
        num_cols = [c for c in col_names if col_types.get(c) == "numeric"]
        txt_cols = [c for c in col_names if col_types.get(c) == "text"]
        cat_cols = [c for c in txt_cols
                    if col_stats.get(c, {}).get("unique", 9999) < 50]

        hdr = f"\n[{fname}"
        if sheet and sheet != "CSV":
            hdr += f" | Foglio: {sheet}"
        hdr += f" | {sch['row_count']:,} righe]"
        parts.append(hdr)

        raw_col_list = ", ".join(raw_map.get(c, c) for c in col_names[:20])
        parts.append(f"Colonne: {raw_col_list}")

        # Numeric statistics (always shown)
        if num_cols:
            parts.append("Statistiche numeriche:")
            for col in num_cols[:10]:
                s = col_stats.get(col, {})
                if s:
                    lbl = raw_map.get(col, col)
                    parts.append(
                        f"  {lbl}: sum={s.get('sum', 0):,.2f} | "
                        f"min={s.get('min', 0):,.2f} | max={s.get('max', 0):,.2f} | "
                        f"n={s.get('count', 0)}"
                    )

        # Color patterns — LLM infers meaning from the specific file's context
        if color_info:
            parts.append("Pattern colori (significato da inferire dal contesto del file):")
            for col, colors in list(color_info.items())[:5]:
                lbl = raw_map.get(col, col)
                for rgb, count in sorted(colors.items(), key=lambda x: -x[1])[:4]:
                    hex_c = f"#{rgb[2:] if len(rgb) == 8 else rgb}"
                    parts.append(f"  {lbl}: {count} celle con sfondo {hex_c}")

        # Group-by breakdown for tables not already answered by routing
        if tname not in routed_tables:
            for cat_col in cat_cols[:2]:
                for num_col in num_cols[:2]:
                    rows = query_db(GENERIC_DB, f"""
                        SELECT "{cat_col}",
                               COUNT(*)         AS righe,
                               SUM("{num_col}") AS totale,
                               AVG("{num_col}") AS media
                        FROM "{tname}"
                        WHERE "{cat_col}" IS NOT NULL AND "{cat_col}" != ''
                        GROUP BY "{cat_col}"
                        ORDER BY totale DESC
                        LIMIT 25
                    """)
                    if rows and "error" not in rows[0] and len(rows) > 1:
                        cat_lbl = raw_map.get(cat_col, cat_col)
                        num_lbl = raw_map.get(num_col, num_col)
                        parts.append(f"\nBreakdown {cat_lbl} → {num_lbl}:")
                        for r in rows:
                            parts.append(
                                f"  {r[cat_col]}: "
                                f"totale={r['totale']:,.2f} | n={r['righe']} | "
                                f"media={r['media']:,.2f}"
                            )

        # Sample rows (always useful for contextual grounding)
        sample = query_db(GENERIC_DB, f'SELECT * FROM "{tname}" LIMIT 5')
        if sample and "error" not in sample[0]:
            parts.append("Righe di esempio:")
            for row in sample:
                line = " | ".join(
                    f"{raw_map.get(k, k)}={v}"
                    for k, v in list(row.items())[:8]
                    if v is not None and v != ""
                )
                parts.append(f"  {line}")

    return "\n".join(parts)

# ── PDF → Markdown ─────────────────────────────────────────────────────────────

def read_pdf_as_markdown(filepath) -> str:
    """
    Convert a PDF to structured Markdown for better LLM comprehension.
    Three-level fallback:
      1. markitdown (Microsoft) — preserves headings, tables, lists
      2. fitz (PyMuPDF)        — raw text page by page
      3. pdfplumber            — table extraction
    """
    p      = Path(filepath)
    header = f"=== PDF: {p.name} ===\n"

    # Level 1: markitdown
    try:
        from markitdown import MarkItDown
        result = MarkItDown().convert(str(filepath))
        text   = result.text_content.strip()
        if len(text) > 100:
            return header + "[Formato: Markdown]\n\n" + text
    except ImportError:
        pass
    except Exception as e:
        print(f"  [markitdown warning] {p.name}: {e}", flush=True)

    # Level 2: fitz
    fitz_text = ""
    try:
        doc = fitz.open(filepath)
        for i, page in enumerate(doc):
            t = page.get_text()
            if t.strip():
                fitz_text += f"[Pagina {i+1}]\n{t}\n"
        doc.close()
    except Exception:
        pass

    # Level 3: pdfplumber (tables)
    plumber_text = ""
    try:
        import pdfplumber
        with pdfplumber.open(filepath) as pdf:
            for page in pdf.pages:
                for table in page.extract_tables() or []:
                    for row in table:
                        r = [str(c).strip() if c else "" for c in row]
                        if any(r):
                            plumber_text += " | ".join(r) + "\n"
    except Exception:
        pass

    parts = []
    if fitz_text:
        parts.append("[Formato: testo grezzo]\n" + fitz_text)
    if plumber_text:
        parts.append("[Tabelle estratte]\n" + plumber_text)

    return header + "\n".join(parts) if parts else f"[Impossibile estrarre {p.name}]"

# ── Memory ─────────────────────────────────────────────────────────────────────

def load_memory() -> dict:
    if MEMORY_FILE.exists():
        try:    return json.loads(MEMORY_FILE.read_text(encoding="utf-8"))
        except: return {}
    return {}


def save_memory(memory: dict):
    MEMORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    MEMORY_FILE.write_text(json.dumps(memory, indent=2, ensure_ascii=False), encoding="utf-8")

# ── Chunking + indexing ────────────────────────────────────────────────────────

def chunk_text(text: str) -> list:
    words = text.split()
    chunks, i = [], 0
    while i < len(words):
        chunks.append(" ".join(words[i:i + CHUNK_SIZE]))
        i += CHUNK_SIZE - CHUNK_OVERLAP
    return chunks or [""]


def index_file(filepath) -> int:
    """Index a single file into ChromaDB. Returns the number of chunks created."""
    p   = Path(filepath)
    ext = p.suffix.lower()

    if ext not in EXTENSIONS["financial"]:
        return 0

    if ext in (".xlsx", ".xls"):
        try:
            sheets_data, text = read_excel_smart(filepath)
            if sheets_data:
                tables = build_file_db(filepath, sheets_data)
                rows   = sum(r for _, _, r in tables)
                text  += f"\n[SQLite: {rows:,} righe in {len(tables)} tabella/e]"
        except Exception as e:
            text = f"[Excel error: {e}]"

    elif ext == ".csv":
        try:
            sheets_data, text = read_csv_smart(filepath)
            if sheets_data:
                tables = build_file_db(filepath, sheets_data)
                rows   = sum(r for _, _, r in tables)
                text  += f"\n[SQLite: {rows:,} righe]"
        except Exception as e:
            text = f"[CSV error: {e}]"

    elif ext == ".pdf":
        text = read_pdf_as_markdown(filepath)

    elif ext == ".txt":
        text = open(filepath, encoding="utf-8", errors="ignore").read()

    else:
        return 0

    if not text or not text.strip():
        return 0

    # Remove all stale chunks for this file before reindexing
    try:
        existing = collection.get(where={"path": str(filepath)})
        if existing and existing["ids"]:
            collection.delete(ids=existing["ids"])
    except Exception:
        pass

    chunks = chunk_text(text)
    for i, chunk in enumerate(chunks):
        collection.upsert(
            documents=[chunk],
            ids=[f"{filepath}__c{i}"],
            metadatas=[{
                "filename": p.name,
                "path":     str(filepath),
                "chunk":    i,
                "agent":    "financial",
                "type":     "chunk",
            }]
        )
    return len(chunks)


def index_folder(folder):
    files = [f for f in Path(folder).rglob("*")
             if f.is_file() and f.suffix.lower() in EXTENSIONS["financial"]]
    print(f"[Financial] Trovati {len(files)} file...")
    total = 0
    for f in files:
        n = index_file(str(f))
        if n: print(f"  [OK] {f.name} → {n} chunk")
        else: print(f"  [--] {f.name} → saltato")
        total += n
    print(f"[Financial] Completato — {total} chunk totali")

# ── Search ─────────────────────────────────────────────────────────────────────

def search(query: str):
    return analyzer.search_with_cards(collection, query, "financial", n_results=4)

# ── System prompt ──────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """Sei un esperto analista finanziario aziendale con accesso diretto ai dati strutturati estratti dai file.

REGOLE ASSOLUTE:
- Rispondi SEMPRE e SOLO in italiano, qualunque sia la lingua della domanda o dei dati
- Usa ESCLUSIVAMENTE i dati forniti nel contesto — mai inventare, stimare o integrare con conoscenze esterne
- Riporta TUTTI i dati presenti nell'ordine in cui compaiono nel contesto
- Non scartare mai righe che sembrano anomale: se sono nel contesto sono dati reali e verificati
- Se un dato non è disponibile nel contesto, dichiaralo esplicitamente
- Cita sempre il file e il foglio sorgente quando disponibili

FORMATTAZIONE NUMERI:
- Separatore migliaia: punto  (es: 1.234.567)
- Separatore decimali: virgola (es: 1.234,56)
- Percentuali: 15,3%
- Valute: € 1.234,56

COLORI NELLE CELLE EXCEL:
- I colori delle celle non hanno un significato predefinito universale
- Interpreta il loro significato DAL CONTESTO SPECIFICO del documento in esame
  (es: se la colonna si chiama "Stato" e ha celle verdi/rosse, inferisci il significato)
- Descrivi i pattern rilevati e proponi un'interpretazione contestuale motivata
- Non assumere mai un significato fisso per nessun colore senza evidenza nel file

STRUTTURA MULTI-FOGLIO:
- Fogli diversi possono rappresentare periodi, categorie, o viste diverse dello stesso dataset
- Cerca relazioni tra fogli quando rilevante per la domanda
- Segnala se i dati richiesti sono distribuiti su più fogli

CAPACITÀ:
- Qualsiasi file finanziario: fatture, pagamenti, budget, bilanci, report di mercato, dashboard
- Aggregazioni precise: somme, medie, ranking, breakdown per categoria
- Confronti temporali, trend, analisi per segmento o canale
- Interpretazione di strutture dati complesse con fogli multipli e celle colorate"""

# ── Answer ─────────────────────────────────────────────────────────────────────

def answer(question: str, context: str) -> str:
    import requests

    schemas      = get_all_file_schemas()
    generic_data = adaptive_sql_context(question, schemas) if schemas else ""

    # DB summary header
    db_info = ""
    if schemas:
        db_info = f"[File DB: {len(schemas)} tabelle indicizzate]\n"
        for s in schemas[:6]:
            raw     = s.get("_col_raw") or []
            db_info += (
                f"  {s['filename']} | {s['sheet']} "
                f"({s['row_count']:,} righe) — "
                f"{', '.join(str(c) for c in raw[:12])}\n"
            )

    # Persistent memory
    memory  = load_memory()
    mem_txt = ""
    if memory:
        mem_txt = "\nMEMORIA:\n" + "\n".join(f"  {k}: {v}" for k, v in memory.items()) + "\n"

    has_db_data = bool(generic_data and generic_data.strip())
    raw_context = (
        "[Contesto semantico disponibile — usa i dati strutturati sopra per i valori numerici]"
        if has_db_data
        else (context[:3000] if len(context) > 3000 else context)
    )

    prompt = f"""{SYSTEM_PROMPT}
{mem_txt}
=== DATABASE ATTIVI ===
{db_info or "Nessun database indicizzato"}

=== DATI STRUTTURATI — FONTE PRIMARIA (usa SOLO questi per i valori numerici) ===
{generic_data or "—"}

=== CONTESTO SEMANTICO (solo per comprensione del documento, NON per i numeri) ===
{raw_context}

DOMANDA: {question}

RISPOSTA (obbligatoriamente in italiano — valori numerici solo dalla FONTE PRIMARIA):"""

    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": ANSWER_MODEL, "prompt": prompt, "stream": False},
            timeout=180,
        )
        r.raise_for_status()
        return r.json().get("response", "Error")
    except requests.exceptions.Timeout:
        return "Timeout — prova una domanda più specifica."
    except Exception as e:
        return f"Error: {e}"


if __name__ == "__main__":
    folder = sys.argv[1] if len(sys.argv) > 1 else "."
    index_folder(folder)