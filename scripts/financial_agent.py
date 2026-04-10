"""
FINANCIAL AGENT v6 — Universal financial document intelligence
Auto-discovers schema from any file, builds per-file SQLite tables,
runs adaptive SQL queries. No hardcoded assumptions about content.
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
KB_FILE     = Path(MEMORY_PATH) / "knowledge_base.json"
GENERIC_DB  = Path(MEMORY_PATH) / "financial_files.db"
NIELSEN_DB  = Path(MEMORY_PATH) / "nielsen.db"

_IGNORE_RGB = {"00000000", "FFFFFFFF", "00FFFFFF", "FFFFFFFF"}

# ── Low-level helpers ─────────────────────────────────────────

def sanitize_col(name):
    s = re.sub(r"[^a-zA-Z0-9_]", "_", str(name).strip()).strip("_")
    return (s or "col")[:50]

def table_name_for(filepath, sheet=""):
    h    = hashlib.md5(f"{filepath}_{sheet}".encode()).hexdigest()[:8]
    stem = re.sub(r"[^a-zA-Z0-9]", "_", Path(filepath).stem[:20]).strip("_")
    return f"f_{stem}_{h}".lower()

def _is_formula(v):
    return isinstance(v, str) and v.strip().startswith("=")

def query_db(db_path, sql, params=()):
    if not Path(db_path).exists(): return []
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
    except Exception as e:
        return [{"error": str(e)}]
    finally:
        conn.close()

# ── Generic SQLite (one DB, one table per file+sheet) ─────────

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
            col_names  TEXT,   -- JSON list of sanitized names
            col_raw    TEXT,   -- JSON list of original names
            col_types  TEXT,   -- JSON {name: numeric|text|empty}
            col_stats  TEXT,   -- JSON {name: {sum,min,max,...}}
            color_info TEXT,   -- JSON {col: {rgb: count}}
            row_count  INTEGER,
            indexed_at TEXT
        )
    """)
    conn.commit()
    return conn

# ── Excel reading ─────────────────────────────────────────────

def _cell_rgb(cell):
    try:
        rgb = cell.fill.fgColor.rgb
        if rgb and rgb.upper() not in _IGNORE_RGB:
            return rgb.upper()
    except Exception:
        pass
    return None

def read_excel_smart(filepath, max_rows=50_000, color_sample=2000):
    """
    Read any Excel file. Returns (sheets_data, text_summary).
    sheets_data = {sheet_name: {df, col_names, col_raw, col_types, col_stats, color_info}}
    No assumptions about content — schema fully auto-detected.
    """
    import openpyxl, pandas as pd

    p  = Path(filepath)
    wb = openpyxl.load_workbook(filepath, data_only=True)

    sheets_data   = {}
    summary_lines = [f"=== EXCEL: {p.name} ===",
                     f"Fogli: {', '.join(wb.sheetnames[:25])}", ""]

    for sname in wb.sheetnames:
        ws = wb[sname]
        if ws.max_row < 2:
            continue

        # Header row
        hdr_row  = list(ws.iter_rows(min_row=1, max_row=1))[0]
        raw_hdrs = [c.value for c in hdr_row]
        if not any(h for h in raw_hdrs if h is not None):
            continue

        # Sanitize column names (handle duplicates)
        seen, san_hdrs = {}, []
        for i, h in enumerate(raw_hdrs):
            base = sanitize_col(h) if h is not None else f"col_{i}"
            cnt  = seen.get(base, 0)
            seen[base] = cnt + 1
            san_hdrs.append(f"{base}_{cnt}" if cnt else base)

        n_cols     = len(san_hdrs)
        rows       = []
        color_info = {}   # san_col -> {rgb: count}
        color_rows = 0

        for row in ws.iter_rows(min_row=2, max_row=min(ws.max_row, max_rows + 1)):
            vals = []
            for c in row[:n_cols]:
                v = c.value
                vals.append(None if _is_formula(v) else v)
            rows.append(vals + [None] * max(0, n_cols - len(vals)))

            if color_rows < color_sample:
                for ci, cell in enumerate(row[:n_cols]):
                    rgb = _cell_rgb(cell)
                    if rgb:
                        col = san_hdrs[ci]
                        if col not in color_info:
                            color_info[col] = {}
                        color_info[col][rgb] = color_info[col].get(rgb, 0) + 1
                color_rows += 1

        if not rows:
            continue

        df = pd.DataFrame(rows, columns=san_hdrs)

        # Detect column types and compute stats
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
                    "sum":     round(float(num.sum()), 4),
                    "mean":    round(float(num.mean()), 4),
                    "min":     round(float(num.min()), 4),
                    "max":     round(float(num.max()), 4),
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

        sheets_data[sname] = {
            "df":         df,
            "col_names":  san_hdrs,
            "col_raw":    [str(h) if h is not None else "" for h in raw_hdrs],
            "col_types":  col_types,
            "col_stats":  col_stats,
            "color_info": color_info,
        }

        # Build text summary for ChromaDB
        raw_str  = ", ".join(str(h) for h in raw_hdrs if h)
        num_cols = [c for c in san_hdrs if col_types.get(c) == "numeric"]
        txt_cols = [c for c in san_hdrs if col_types.get(c) == "text"]
        raw_map  = dict(zip(san_hdrs, [str(h) if h else "" for h in raw_hdrs]))

        summary_lines.append(f"[Foglio: {sname}] {len(rows):,} righe | Colonne: {raw_str[:120]}")
        if num_cols:
            summary_lines.append("  Colonne numeriche:")
            for col in num_cols[:8]:
                s = col_stats[col]
                summary_lines.append(
                    f"    {raw_map.get(col,col)}: "
                    f"sum={s['sum']:,.2f} | min={s['min']:,.2f} | "
                    f"max={s['max']:,.2f} | n={s['count']}"
                )
        if txt_cols:
            summary_lines.append("  Colonne testo:")
            for col in txt_cols[:8]:
                s   = col_stats[col]
                top = ", ".join(str(v) for v in s["top_values"][:5])
                summary_lines.append(
                    f"    {raw_map.get(col,col)}: {s['unique']} valori unici (es: {top})"
                )
        if color_info:
            summary_lines.append("  Pattern colori (sfondo celle):")
            for col, colors in list(color_info.items())[:5]:
                for rgb, count in sorted(colors.items(), key=lambda x: -x[1])[:3]:
                    hex_c = f"#{rgb[2:] if len(rgb) == 8 else rgb}"
                    summary_lines.append(
                        f"    {raw_map.get(col,col)}: {count} celle con sfondo {hex_c}"
                    )
        summary_lines.append("")

    wb.close()
    return sheets_data, "\n".join(summary_lines)


def read_csv_smart(filepath, max_rows=50_000):
    """Read CSV, auto-detect encoding + schema. Returns (sheets_data, text_summary)."""
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

    # Sanitize column names
    seen, san_map = {}, {}
    for h in df.columns:
        base = sanitize_col(h)
        cnt  = seen.get(base, 0)
        seen[base] = cnt + 1
        san_map[h] = f"{base}_{cnt}" if cnt else base
    raw_names = list(df.columns)
    df        = df.rename(columns=san_map)

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
                "sum":   round(float(num.sum()), 4),
                "mean":  round(float(num.mean()), 4),
                "min":   round(float(num.min()), 4),
                "max":   round(float(num.max()), 4),
                "count": int(num.count()),
            }
        else:
            col_types[col] = "text"
            vc = series.astype(str).value_counts()
            col_stats[col] = {
                "unique":     int(series.nunique()),
                "top_values": vc.head(10).index.tolist(),
                "count":      int(series.count()),
            }

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

    lines = [f"=== CSV: {p.name} ===",
             f"Righe: {len(df):,} | Colonne: {', '.join(raw_names[:30])}", ""]
    if num_cols:
        lines.append("Colonne numeriche:")
        for col in num_cols[:8]:
            s = col_stats[col]
            lines.append(f"  {raw_map.get(col,col)}: sum={s['sum']:,.2f} | min={s['min']:.2f} | max={s['max']:.2f}")
    if txt_cols:
        lines.append("Colonne testo:")
        for col in txt_cols[:8]:
            s   = col_stats[col]
            top = ", ".join(str(v) for v in s["top_values"][:5])
            lines.append(f"  {raw_map.get(col,col)}: {s['unique']} valori unici (es: {top})")

    return sheets_data, "\n".join(lines)


# ── Nielsen file detection ────────────────────────────────────

def is_nielsen_file(filepath):
    try:
        import openpyxl
        wb    = openpyxl.load_workbook(filepath, read_only=True)
        count = sum(1 for s in wb.sheetnames if s.startswith("Share"))
        wb.close()
        return count >= 3
    except Exception:
        return False

def _nielsen_summary_text(filepath):
    try:
        import openpyxl
        wb   = openpyxl.load_workbook(filepath, data_only=True, read_only=True)
        segs = [s for s in wb.sheetnames if s.startswith("Share")]
        txt  = f"=== NIELSEN DASHBOARD: {Path(filepath).name} ===\n"
        txt += f"Fogli: {len(wb.sheetnames)} | Segmenti: {', '.join(segs)}\n"
        wb.close()
        return txt
    except Exception as e:
        return f"[Nielsen summary error: {e}]"


# ── Generic DB builder ────────────────────────────────────────

def build_file_db(filepath, sheets_data):
    """
    Store every sheet of an Excel/CSV into the generic SQLite DB.
    Each sheet gets its own table; schema saved to _file_schemas.
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

        # Create table
        conn.execute(f'DROP TABLE IF EXISTS "{tname}"')
        col_defs = []
        for col in col_names:
            sql_t = "REAL" if col_types.get(col) == "numeric" else "TEXT"
            col_defs.append(f'"{col}" {sql_t}')
        conn.execute(f'CREATE TABLE "{tname}" ({", ".join(col_defs)})')

        # Coerce types before insertion
        for col in col_names:
            if col_types.get(col) == "numeric":
                df[col] = pd.to_numeric(df[col], errors="coerce")
            else:
                df[col] = (df[col].fillna("")
                                  .astype(str)
                                  .str.strip()
                                  .replace({"nan": "", "None": "", "NaT": ""}))

        # Insert using itertuples (fast)
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

        # Index on low-cardinality text columns
        for col in col_names:
            if col_types.get(col) == "text":
                uq = col_stats.get(col, {}).get("unique", 9999)
                if uq < 500:
                    idx = f"idx_{tname[:18]}_{col[:12]}"
                    try:
                        conn.execute(f'CREATE INDEX IF NOT EXISTS "{idx}" ON "{tname}"("{col}")')
                    except Exception:
                        pass

        # Persist schema
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


# ── Schema retrieval ──────────────────────────────────────────

def get_all_file_schemas():
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


# ── Adaptive SQL context ──────────────────────────────────────

def adaptive_sql_context(query, schemas):
    """
    Build analytical SQL context from real file schemas.
    Runs stats + group-by queries using actual column names.
    """
    if not schemas:
        return ""

    q     = query.lower()
    parts = []

    for sch in schemas[:4]:
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
        # Category columns: text with few unique values
        cat_cols = [c for c in txt_cols
                    if col_stats.get(c, {}).get("unique", 9999) < 50]

        hdr = f"\n[{fname}"
        if sheet != "CSV":
            hdr += f" | Foglio: {sheet}"
        hdr += f" | {sch['row_count']:,} righe]"
        parts.append(hdr)
        raw_col_list = ", ".join(raw_map.get(c, c) for c in col_names[:20])
        parts.append(f"Colonne: {raw_col_list}")

        # Numeric column statistics (always shown)
        if num_cols:
            parts.append("Statistiche numeriche:")
            for col in num_cols[:8]:
                s = col_stats.get(col, {})
                if s:
                    lbl = raw_map.get(col, col)
                    parts.append(
                        f"  {lbl}: sum={s.get('sum',0):,.2f} | "
                        f"min={s.get('min',0):,.2f} | max={s.get('max',0):,.2f} | "
                        f"n={s.get('count',0)}"
                    )

        # Color patterns (descriptive — LLM infers meaning from context)
        if color_info:
            parts.append("Pattern colori rilevati nelle celle:")
            for col, colors in list(color_info.items())[:4]:
                lbl = raw_map.get(col, col)
                for rgb, count in sorted(colors.items(), key=lambda x: -x[1])[:3]:
                    hex_c = f"#{rgb[2:] if len(rgb) == 8 else rgb}"
                    parts.append(f"  {lbl}: {count} celle con sfondo {hex_c}")

        # Group-by: category col × numeric col
        for cat_col in cat_cols[:2]:
            for num_col in num_cols[:2]:
                rows = query_db(GENERIC_DB, f"""
                    SELECT "{cat_col}",
                           COUNT(*)          AS righe,
                           SUM("{num_col}")  AS totale,
                           AVG("{num_col}")  AS media
                    FROM "{tname}"
                    WHERE "{cat_col}" IS NOT NULL AND "{cat_col}" != ''
                    GROUP BY "{cat_col}"
                    ORDER BY totale DESC
                    LIMIT 20
                """)
                if rows and "error" not in rows[0] and len(rows) > 1:
                    cat_lbl = raw_map.get(cat_col, cat_col)
                    num_lbl = raw_map.get(num_col, num_col)
                    parts.append(f"\nBreakdown {cat_lbl} → {num_lbl}:")
                    for r in rows[:20]:
                        parts.append(
                            f"  {r[cat_col]}: "
                            f"totale={r['totale']:,.2f} | n={r['righe']} | media={r['media']:,.2f}"
                        )

        # Sample rows
        sample = query_db(GENERIC_DB, f'SELECT * FROM "{tname}" LIMIT 4')
        if sample and "error" not in sample[0]:
            parts.append("Righe di esempio:")
            for row in sample:
                line = " | ".join(
                    f"{raw_map.get(k,k)}={v}"
                    for k, v in list(row.items())[:8]
                    if v is not None and v != ""
                )
                parts.append(f"  {line}")

    return "\n".join(parts)


# ── Nielsen context ───────────────────────────────────────────

def nielsen_context_for_query(query):
    """Generate context from nielsen.db using only dimensions actually present in the DB."""
    if not NIELSEN_DB.exists():
        return ""

    q = query.lower()

    segs    = query_db(NIELSEN_DB, "SELECT DISTINCT segment FROM nielsen_data ORDER BY segment")
    periods = query_db(NIELSEN_DB, "SELECT DISTINCT period   FROM nielsen_data ORDER BY period")
    metrics = query_db(NIELSEN_DB, "SELECT DISTINCT metric   FROM nielsen_data ORDER BY metric")
    markets = query_db(NIELSEN_DB, "SELECT DISTINCT market   FROM nielsen_data ORDER BY market")

    if not segs:
        return ""

    seg_list    = [r["segment"] for r in segs]
    period_list = [r["period"]  for r in periods]
    metric_list = [r["metric"]  for r in metrics]
    market_list = [r["market"]  for r in markets]

    # Match segments
    matched_segs = [s for s in seg_list if s.lower().replace("_", "") in q] or seg_list[:3]

    # Match period
    matched_period = next((p for p in period_list if p.lower() in q), None)
    if not matched_period:
        matched_period = "L12M" if "L12M" in period_list else period_list[0]

    # Match metric
    metric_kw = {
        "volume": "VOLUME_KGS", "kg": "VOLUME_KGS",
        "valore": "VALUE_EUR",  "euro": "VALUE_EUR", "fatturato": "VALUE_EUR",
        "quota":  "VOLUME_SOM", "share": "VOLUME_SOM",
        "prezzo": "PRICE_PER_KG",
        "distribuzione": "WEIGHTED_DIST",
    }
    matched_metric = next((metric_kw[k] for k in metric_kw if k in q), "VOLUME_KGS")
    if matched_metric not in metric_list:
        matched_metric = "VOLUME_KGS" if "VOLUME_KGS" in metric_list else metric_list[0]

    # Match market
    matched_market = next((m for m in market_list if m.lower() in q), None)
    if not matched_market:
        matched_market = "Omnichannel" if "Omnichannel" in market_list else market_list[0]

    parts = []

    for seg in matched_segs[:3]:
        # Fetch both volume and value together — LLM always has the full picture
        vol_rows = query_db(NIELSEN_DB, """
            SELECT brand, value FROM nielsen_data
            WHERE segment=? AND metric='VOLUME_KGS' AND period=? AND market=?
              AND level=1 AND (is_aggregate=0 OR is_aggregate IS NULL)
            ORDER BY value DESC LIMIT 15
        """, (seg, matched_period, matched_market))

        eur_rows = query_db(NIELSEN_DB, """
            SELECT brand, value FROM nielsen_data
            WHERE segment=? AND metric='VALUE_EUR' AND period=? AND market=?
              AND level=1 AND (is_aggregate=0 OR is_aggregate IS NULL)
            ORDER BY value DESC LIMIT 15
        """, (seg, matched_period, matched_market))

        # Also fetch the explicitly requested metric if different
        extra_rows = []
        if matched_metric not in ("VOLUME_KGS", "VALUE_EUR"):
            extra_rows = query_db(NIELSEN_DB, """
                SELECT brand, value FROM nielsen_data
                WHERE segment=? AND metric=? AND period=? AND market=?
                  AND level=1 AND (is_aggregate=0 OR is_aggregate IS NULL)
                ORDER BY value DESC LIMIT 15
            """, (seg, matched_metric, matched_period, matched_market))

        if vol_rows and "error" not in vol_rows[0]:
            parts.append(
                f"\n[Nielsen | {seg} | {matched_period} | {matched_market}]"
                f" — brand reali, ordinati dal 1° (più alto) all'ultimo:"
            )
            eur_map = {r["brand"]: r["value"] for r in eur_rows if not r.get("error")}
            for rank, r in enumerate(vol_rows, 1):
                brand   = r["brand"]
                vol_val = r["value"]
                eur_val = eur_map.get(brand)
                eur_str = f" | € {eur_val:,.0f}" if eur_val else ""
                parts.append(f"  #{rank} {brand}: {vol_val:,.0f} kg{eur_str}")

        if extra_rows and "error" not in extra_rows[0]:
            parts.append(f"  [{matched_metric}]")
            for rank, r in enumerate(extra_rows, 1):
                v = r["value"]
                if matched_metric in ("VOLUME_SOM","VALUE_SOM","WEIGHTED_DIST","CUP_SOM"):
                    parts.append(f"  #{rank} {r['brand']}: {v:.1%}")
                elif matched_metric in ("PRICE_PER_KG","PRICE_PER_CUP","PRICE_INDEX","PRICE_INDEX_CUP"):
                    parts.append(f"  #{rank} {r['brand']}: € {v:.2f}")
                else:
                    parts.append(f"  #{rank} {r['brand']}: {v:,.0f}")

    # YoY if requested
    if any(k in q for k in ["crescita","trend","variazione","vs","anno precedente","yoy"]):
        prev_period = next((p for p in period_list if "-1" in p and "L12" in p), None)
        if prev_period:
            for seg in matched_segs[:2]:
                curr = query_db(NIELSEN_DB, """
                    SELECT brand, value FROM nielsen_data
                    WHERE segment=? AND metric=? AND period=? AND market=?
                      AND level=1 AND (is_aggregate=0 OR is_aggregate IS NULL)
                    ORDER BY value DESC LIMIT 10
                """, (seg, matched_metric, matched_period, matched_market))
                prev = query_db(NIELSEN_DB, """
                    SELECT brand, value FROM nielsen_data
                    WHERE segment=? AND metric=? AND period=? AND market=?
                      AND level=1 AND (is_aggregate=0 OR is_aggregate IS NULL)
                    ORDER BY value DESC LIMIT 10
                """, (seg, matched_metric, prev_period, matched_market))

                if curr and prev and "error" not in curr[0]:
                    prev_d = {r["brand"]: r["value"] for r in prev}
                    parts.append(f"\n[Nielsen | {seg} | YoY {matched_period} vs {prev_period}] — ordinati per volume attuale:")
                    for rank, r in enumerate(curr, 1):
                        brand = r["brand"]
                        c_val = r["value"]
                        p_val = prev_d.get(brand)
                        if p_val and p_val > 0:
                            pct   = (c_val - p_val) / p_val * 100
                            arrow = "▲" if pct > 0 else "▼"
                            parts.append(f"  #{rank} {brand}: {c_val:,.0f} ({arrow}{abs(pct):.1f}%)")

    return "\n".join(parts)


# ── PDF reader ────────────────────────────────────────────────

def read_pdf(filepath):
    results = []
    try:
        doc  = fitz.open(filepath)
        text = f"=== PDF: {Path(filepath).name} ===\n"
        for i, page in enumerate(doc):
            t = page.get_text()
            if t.strip():
                text += f"[Pagina {i+1}]\n{t}\n"
        if len(text) > 200:
            results.append(text)
    except Exception:
        pass
    try:
        import pdfplumber
        with pdfplumber.open(filepath) as pdf:
            text = f"=== PDF TABLES: {Path(filepath).name} ===\n"
            for page in pdf.pages:
                for table in page.extract_tables() or []:
                    for row in table:
                        r = [str(c).strip() if c else "" for c in row]
                        if any(r):
                            text += " | ".join(r) + "\n"
            if len(text) > 200:
                results.append(text)
    except Exception:
        pass
    return "\n".join(results) if results else f"[Impossibile estrarre {Path(filepath).name}]"


# ── Memory / KB ───────────────────────────────────────────────

def load_memory():
    if MEMORY_FILE.exists():
        try:    return json.loads(MEMORY_FILE.read_text(encoding="utf-8"))
        except: return {}
    return {}

def save_memory(memory):
    MEMORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    MEMORY_FILE.write_text(json.dumps(memory, indent=2, ensure_ascii=False), encoding="utf-8")

def load_kb():
    if KB_FILE.exists():
        try:    return json.loads(KB_FILE.read_text(encoding="utf-8"))
        except: return {}
    return {}


# ── Chunking + indexing ───────────────────────────────────────

def chunk_text(text):
    words = text.split()
    chunks, i = [], 0
    while i < len(words):
        chunks.append(" ".join(words[i:i + CHUNK_SIZE]))
        i += CHUNK_SIZE - CHUNK_OVERLAP
    return chunks or [""]

def index_file(filepath):
    p   = Path(filepath)
    ext = p.suffix.lower()
    if ext not in EXTENSIONS["financial"]:
        return 0

    if ext in (".xlsx", ".xls"):
        if is_nielsen_file(filepath):
            text = _nielsen_summary_text(filepath)
            try:
                import nielsen_db_builder as ndb
                print(f"  [DB] Building Nielsen SQLite for {p.name}...", flush=True)
                ndb.build_db(str(filepath))
                text += f"\n[Nielsen DB: {ndb.get_db_summary()[:200]}]"
            except Exception as e:
                text += f"\n[Nielsen DB error: {e}]"
        else:
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
        text = read_pdf(filepath)

    elif ext == ".txt":
        text = open(filepath, encoding="utf-8", errors="ignore").read()

    else:
        return 0

    if not text or not text.strip():
        return 0

    # Remove ALL stale chunks and semantic cards for this file before reindexing
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
            metadatas=[{"filename": p.name, "path": str(filepath),
                        "chunk": i, "agent": "financial", "type": "chunk"}]
        )
    return len(chunks)

def index_folder(folder):
    files = [f for f in Path(folder).rglob("*")
             if f.is_file() and f.suffix.lower() in EXTENSIONS["financial"]]
    print(f"[Financial] Trovati {len(files)} file...")
    total = 0
    for f in files:
        n = index_file(str(f))
        if n: print(f"  [OK] {f.name} -> {n} chunk")
        else: print(f"  [--] {f.name} -> saltato")
        total += n
    print(f"[Financial] Completato — {total} chunk totali")


# ── Search ────────────────────────────────────────────────────

def search(query):
    return analyzer.search_with_cards(collection, query, "financial", n_results=4)


# ── Answer ────────────────────────────────────────────────────

SYSTEM_PROMPT = """Sei un esperto analista finanziario aziendale con accesso a dati strutturati estratti dai file aziendali.

REGOLE GENERALI:
- Rispondi SEMPRE e SOLO in italiano, qualunque sia la lingua della domanda
- NON usare mai cinese, inglese o altre lingue — SOLO ITALIANO
- Usa ESCLUSIVAMENTE i dati forniti nel contesto — non inventare, non stimare, non integrare
- Riporta TUTTI i dati presenti nell'ordine esatto in cui compaiono (sono già ordinati per valore)
- Non scartare mai righe che ti sembrano anomale: se sono nel contesto, sono dati reali e verificati
- Valori numerici: separatore migliaia con punto (es: 1.234.567)
- Valori monetari: € 1.234,56
- Percentuali: 15,3%
- Se un dato non è disponibile: dichiaralo esplicitamente
- Cita sempre il file e il foglio sorgente quando rilevante

STRUTTURA DATI NIELSEN (quando presenti):
- I dati sono già filtrati per livello gerarchico: ricevi solo i brand diretti (level=1)
- "PL" = Private Label (prodotto a marchio del distributore) — è un competitor reale, va sempre riportato
- Il primo risultato è sempre quello con il valore più alto
- Periodi: L12M = ultimi 12 mesi, LM12-1 = stesso periodo anno precedente, L6M/L3M = semestre/trimestre
- Segmenti: NCC=capsule Nespresso compat., RG=macinato, NDG=Dolce Gusto, LAMM=Lavazza A Modo Mio,
  Pods=cialde ESE, Beans=grani, Instant_Tot=solubile totale, Tot_Coffee=totale mercato caffè

PATTERN COLORI EXCEL:
- I colori nelle celle non hanno un significato universale
- Interpretali dal contesto specifico del documento in esame

CAPACITÀ:
- Qualsiasi file finanziario: fatture, pagamenti, budget, bilanci, dashboard di mercato
- Aggregazioni precise: somme, medie, ranking, breakdown per categoria
- Confronti YoY, trend temporali, analisi per segmento o canale"""

def answer(question, context):
    import requests

    # Structured data from DB
    nielsen_data = nielsen_context_for_query(question)
    schemas      = get_all_file_schemas()
    generic_data = adaptive_sql_context(question, schemas) if schemas else ""

    # DB summary header
    db_info = ""
    if NIELSEN_DB.exists():
        rows = query_db(NIELSEN_DB, "SELECT COUNT(*) AS n FROM nielsen_data")
        segs = query_db(NIELSEN_DB, "SELECT DISTINCT segment FROM nielsen_data")
        if rows and not rows[0].get("error"):
            seg_str  = ", ".join(r["segment"] for r in segs)
            db_info += f"[Nielsen DB: {rows[0]['n']:,} record | Segmenti: {seg_str}]\n"
    if schemas:
        db_info += f"[File DB: {len(schemas)} tabelle indicizzate]\n"
        for s in schemas[:5]:
            raw = s.get("_col_raw") or []
            db_info += f"  {s['filename']} ({s['row_count']:,} righe) — {', '.join(str(c) for c in raw[:10])}\n"

    # Knowledge base
    kb     = load_kb()
    kb_txt = ""
    if kb:
        kb_txt = "\n=== KNOWLEDGE BASE ===\n"
        for section, content in kb.items():
            kb_txt += f"[{section}]\n"
            if isinstance(content, dict):
                for k, v in content.items():
                    kb_txt += f"  {k}: {', '.join(v) if isinstance(v, list) else v}\n"
            kb_txt += "\n"

    # Persistent memory
    memory  = load_memory()
    mem_txt = ""
    if memory:
        mem_txt = "\nMEMORIA:\n" + "\n".join(f"  {k}: {v}" for k, v in memory.items()) + "\n"

    # When structured DB data exists, the raw ChromaDB context is supplementary only.
    # Truncate it to avoid the LLM reading stale/confusing raw spreadsheet dumps.
    has_db_data = bool(nielsen_data and nielsen_data.strip() != "—") or \
                  bool(generic_data and generic_data.strip() != "—")
    if has_db_data:
        raw_context = "[Contesto semantico disponibile ma non necessario — usa i dati DB sopra]"
    else:
        raw_context = context[:3000] if len(context) > 3000 else context

    prompt = f"""{SYSTEM_PROMPT}
{kb_txt}{mem_txt}
=== DATABASE ATTIVI ===
{db_info or "Nessun database indicizzato"}

⚠️ FONTE PRIMARIA — usa SOLO questi dati per i valori numerici:

=== DATI NIELSEN (fonte autorevole) ===
{nielsen_data or "—"}

=== DATI FILE STRUTTURATI (fonte autorevole) ===
{generic_data or "—"}

ℹ️ CONTESTO SUPPLEMENTARE — solo per capire il documento, NON per i numeri:
{raw_context}

DOMANDA: {question}

RISPOSTA (OBBLIGATORIAMENTE IN ITALIANO — non usare mai altre lingue — usa SOLO i valori dalla FONTE PRIMARIA):"""

    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": LLM_MODEL, "prompt": prompt, "stream": False},
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
