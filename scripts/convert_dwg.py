"""
DWG → DXF Converter v2 — Multi-backend con fallback automatico

Backend in ordine di priorità:
  1. ODA File Converter  — qualità industriale, supporta tutte le versioni DWG
  2. ezdxf.addons.odafc  — wrapper Python per ODA (stesso binario, API più pulita)
  3. LibreCAD CLI         — fallback open-source (qualità inferiore)

Configurazione via variabili d'ambiente:
  DWG_CONVERTER_BACKEND  — forzare un backend: "oda", "ezdxf", "librecad", "auto" (default)
  ODA_CONVERTER_PATH     — path assoluto all'eseguibile ODA (se non in PATH)

Uso:
  python convert_dwg.py file.dwg                 # converte singolo file
  python convert_dwg.py /path/to/folder           # converte tutti i DWG nella cartella
  python convert_dwg.py --check                   # verifica backend disponibili
"""

import subprocess
import os
import sys
import shutil
import tempfile
from pathlib import Path

# ── Configurazione ────────────────────────────────────────────────────────────

BACKEND_PREFERENCE = os.environ.get("DWG_CONVERTER_BACKEND", "auto").lower()
ODA_CONVERTER_PATH = os.environ.get("ODA_CONVERTER_PATH", "")

# Versione DXF di output (R2018 = massima compatibilità moderna)
ODA_OUTPUT_VERSION = os.environ.get("ODA_OUTPUT_VERSION", "ACAD2018")
CONVERSION_TIMEOUT = int(os.environ.get("DWG_CONVERSION_TIMEOUT", "120"))


# ── Backend detection ─────────────────────────────────────────────────────────

def _find_oda_executable() -> str | None:
    """Cerca l'eseguibile ODA File Converter nel sistema."""
    # Path esplicito da env var
    if ODA_CONVERTER_PATH and Path(ODA_CONVERTER_PATH).is_file():
        return ODA_CONVERTER_PATH

    # Nomi comuni per ODA File Converter
    candidates = [
        "ODAFileConverter",
        "ODAFileConverter_QT5_lnxX64_8.3dll",
    ]

    for name in candidates:
        path = shutil.which(name)
        if path:
            return path

    # Percorsi comuni su Linux
    common_paths = [
        "/usr/bin/ODAFileConverter",
        "/usr/local/bin/ODAFileConverter",
        os.path.expanduser("~/ODAFileConverter"),
    ]
    # Cerca anche AppImage nella home
    home = Path.home()
    for appimage in home.glob("ODAFileConverter*.AppImage"):
        common_paths.append(str(appimage))

    for p in common_paths:
        if Path(p).is_file():
            return p

    return None


def _check_ezdxf_odafc() -> bool:
    """Verifica se ezdxf.addons.odafc è disponibile e funzionante."""
    try:
        from ezdxf.addons import odafc
        return odafc.is_installed()
    except (ImportError, Exception):
        return False


def _check_librecad() -> bool:
    """Verifica se LibreCAD è installato."""
    return shutil.which("librecad") is not None


def check_backends() -> dict:
    """Restituisce lo stato di tutti i backend disponibili."""
    oda_path = _find_oda_executable()
    return {
        "oda": {
            "available": oda_path is not None,
            "path": oda_path or "non trovato",
            "note": "Qualità industriale — raccomandato",
        },
        "ezdxf": {
            "available": _check_ezdxf_odafc(),
            "note": "Wrapper Python per ODA (richiede ODA installato)",
        },
        "librecad": {
            "available": _check_librecad(),
            "path": shutil.which("librecad") or "non trovato",
            "note": "Fallback open-source — qualità inferiore",
        },
    }


def _select_backend() -> str | None:
    """Seleziona il miglior backend disponibile."""
    if BACKEND_PREFERENCE != "auto":
        return BACKEND_PREFERENCE if BACKEND_PREFERENCE in ("oda", "ezdxf", "librecad") else None

    if _find_oda_executable():
        return "oda"
    if _check_ezdxf_odafc():
        return "ezdxf"
    if _check_librecad():
        return "librecad"
    return None


# ── Conversione per backend ───────────────────────────────────────────────────

def _convert_via_oda(dwg_path: Path, dxf_path: Path) -> bool:
    """
    Conversione tramite ODA File Converter CLI.

    ODA accetta: <input_folder> <output_folder> <output_version> <output_format>
                 <recursive> <audit> [filter]

    Strategia: copia il DWG in una cartella temp, esegui ODA, recupera il DXF.
    Questo evita di processare altri file nella stessa cartella.
    """
    oda_exe = _find_oda_executable()
    if not oda_exe:
        return False

    with tempfile.TemporaryDirectory(prefix="oda_in_") as in_dir, \
         tempfile.TemporaryDirectory(prefix="oda_out_") as out_dir:

        # Copia il singolo DWG nella cartella di input
        temp_dwg = Path(in_dir) / dwg_path.name
        shutil.copy2(dwg_path, temp_dwg)

        cmd = [
            oda_exe,
            in_dir,                 # input folder
            out_dir,                # output folder
            ODA_OUTPUT_VERSION,     # output version (ACAD2018)
            "DXF",                  # output format
            "0",                    # recursive: no
            "1",                    # audit: yes
            f"*.DWG",               # filter
        ]

        try:
            # Su Linux potrebbe servire xvfb per sopprimere la GUI
            env = os.environ.copy()
            env["DISPLAY"] = ""  # previene tentativo di aprire GUI

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=CONVERSION_TIMEOUT,
                env=env,
            )

            # Cerca il DXF generato nella cartella di output
            expected_name = dwg_path.stem + ".dxf"
            output_file = Path(out_dir) / expected_name

            if not output_file.exists():
                # ODA potrebbe usare case diverso
                for f in Path(out_dir).glob("*.dxf"):
                    output_file = f
                    break
                for f in Path(out_dir).glob("*.DXF"):
                    output_file = f
                    break

            if output_file.exists():
                shutil.move(str(output_file), str(dxf_path))
                return True

            print(f"  [ODA] Nessun DXF generato. stdout: {result.stdout[:200]}", flush=True)
            print(f"  [ODA] stderr: {result.stderr[:200]}", flush=True)
            return False

        except FileNotFoundError:
            print(f"  [ODA] Eseguibile non trovato: {oda_exe}", flush=True)
            return False
        except subprocess.TimeoutExpired:
            print(f"  [ODA] Timeout ({CONVERSION_TIMEOUT}s) per: {dwg_path.name}", flush=True)
            return False
        except Exception as e:
            print(f"  [ODA] Errore: {e}", flush=True)
            return False


def _convert_via_ezdxf(dwg_path: Path, dxf_path: Path) -> bool:
    """Conversione tramite ezdxf.addons.odafc (wrapper Python per ODA)."""
    try:
        from ezdxf.addons import odafc
        odafc.convert(str(dwg_path), str(dxf_path), version="R2018", audit=True)
        return dxf_path.exists()
    except ImportError:
        print("  [ezdxf] Modulo ezdxf.addons.odafc non disponibile", flush=True)
        return False
    except Exception as e:
        print(f"  [ezdxf] Errore: {e}", flush=True)
        return False


def _convert_via_librecad(dwg_path: Path, dxf_path: Path) -> bool:
    """Conversione tramite LibreCAD CLI (fallback)."""
    try:
        result = subprocess.run(
            ["librecad", "dxf2dxf", "--output", str(dxf_path), str(dwg_path)],
            capture_output=True,
            text=True,
            timeout=CONVERSION_TIMEOUT,
        )
        return dxf_path.exists()
    except FileNotFoundError:
        print("  [LibreCAD] Non installato. Installa con: sudo apt install librecad", flush=True)
        return False
    except subprocess.TimeoutExpired:
        print(f"  [LibreCAD] Timeout ({CONVERSION_TIMEOUT}s) per: {dwg_path.name}", flush=True)
        return False
    except Exception as e:
        print(f"  [LibreCAD] Errore: {e}", flush=True)
        return False


# ── API pubblica ──────────────────────────────────────────────────────────────

def convert_dwg_to_dxf(dwg_path: str | Path) -> str | None:
    """
    Converte un file DWG in DXF usando il miglior backend disponibile.

    Ritorna:
      str  — path del DXF generato (successo)
      None — conversione fallita su tutti i backend

    Se il DXF esiste già e non è più vecchio del DWG, salta la conversione.
    """
    dwg = Path(dwg_path)
    dxf = dwg.with_suffix(".dxf")

    if not dwg.exists():
        print(f"  [ERROR] File non trovato: {dwg}", flush=True)
        return None

    # Skip se DXF è già aggiornato
    if dxf.exists():
        try:
            if dxf.stat().st_mtime >= dwg.stat().st_mtime:
                print(f"  [SKIP] DXF già aggiornato: {dxf.name}", flush=True)
                return str(dxf)
        except OSError:
            pass
        # DXF esiste ma è più vecchio → riconverti
        print(f"  [UPDATE] DXF più vecchio del DWG, riconversione...", flush=True)

    backend = _select_backend()
    if not backend:
        print("  [ERROR] Nessun backend di conversione disponibile!", flush=True)
        print("  Installa uno tra: ODA File Converter (raccomandato), LibreCAD", flush=True)
        return None

    # Mappa backend → funzione
    converters = {
        "oda":     _convert_via_oda,
        "ezdxf":   _convert_via_ezdxf,
        "librecad": _convert_via_librecad,
    }

    # Tenta il backend selezionato
    print(f"  [DWG→DXF] {dwg.name} via {backend}...", flush=True)
    if converters[backend](dwg, dxf):
        size_kb = dxf.stat().st_size / 1024
        print(f"  [OK] {dxf.name} ({size_kb:.1f} KB) via {backend}", flush=True)
        return str(dxf)

    # Fallback: prova gli altri backend
    for fallback_name, fallback_fn in converters.items():
        if fallback_name == backend:
            continue
        print(f"  [FALLBACK] Provo {fallback_name}...", flush=True)
        if fallback_fn(dwg, dxf):
            size_kb = dxf.stat().st_size / 1024
            print(f"  [OK] {dxf.name} ({size_kb:.1f} KB) via {fallback_name}", flush=True)
            return str(dxf)

    print(f"  [FAIL] Conversione fallita su tutti i backend: {dwg.name}", flush=True)
    return None


def convert_folder(folder: str | Path) -> dict:
    """
    Converte tutti i DWG in una cartella. Ritorna statistiche.
    """
    folder = Path(folder)
    dwg_files = list(folder.rglob("*.dwg")) + list(folder.rglob("*.DWG"))

    if not dwg_files:
        print(f"[DWG] Nessun file DWG trovato in {folder}", flush=True)
        return {"total": 0, "converted": 0, "skipped": 0, "failed": 0}

    stats = {"total": len(dwg_files), "converted": 0, "skipped": 0, "failed": 0}
    print(f"[DWG] Trovati {len(dwg_files)} file DWG in {folder}", flush=True)

    for dwg in dwg_files:
        result = convert_dwg_to_dxf(dwg)
        if result:
            if "SKIP" in str(result):  # heuristic
                stats["skipped"] += 1
            else:
                stats["converted"] += 1
        else:
            stats["failed"] += 1

    print(f"[DWG] Completato: {stats['converted']} convertiti, "
          f"{stats['skipped']} saltati, {stats['failed']} falliti", flush=True)
    return stats


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso:")
        print("  python convert_dwg.py file.dwg          # converte un file")
        print("  python convert_dwg.py /path/to/folder    # converte una cartella")
        print("  python convert_dwg.py --check            # verifica backend")
        sys.exit(1)

    arg = sys.argv[1]

    if arg == "--check":
        print("=== Backend disponibili ===")
        backends = check_backends()
        for name, info in backends.items():
            status = "✓" if info["available"] else "✗"
            print(f"  {status} {name:10} — {info['note']}")
            if "path" in info:
                print(f"    Path: {info['path']}")
        sys.exit(0)

    path = Path(arg)
    if path.is_dir():
        convert_folder(path)
    elif path.is_file():
        result = convert_dwg_to_dxf(path)
        if result:
            print(f"Risultato: {result}")
        else:
            print("Conversione fallita.")
            sys.exit(1)
    else:
        print(f"Percorso non trovato: {arg}")
        sys.exit(1)