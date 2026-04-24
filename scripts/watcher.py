"""
GLOBAL FILE WATCHER v2
Monitora le tre cartelle agenti ogni 10 secondi.
Instrada ogni file nuovo o modificato all'agente corretto per l'indicizzazione.

Miglioramenti v2 rispetto alla versione precedente:
  - Debounce (1 ciclo): aspetta che l'mtime sia stabile prima di indicizzare
    → evita di indicizzare file ancora in scrittura / copia
  - Rilevamento eliminazioni: file rimossi dal disco vengono cancellati da ChromaDB
    con query mirata (where={"path": ...}) — nessun caricamento di tutti i chunk
  - ThreadPoolExecutor (2 worker): l'indicizzazione avviene in background
    → il loop di polling non si blocca su file grandi
  - In-progress tracking: nessuna doppia indicizzazione dello stesso file
  - Registro persistente JSON: sopravvive ai riavvii — i file già indicizzati
    non vengono re-indicizzati se non sono cambiati
  - Caricamento agenti resiliente: un agente rotto non abbatte il watcher
  - Log strutturato in italiano con livelli INFO/WARNING/ERROR
"""

import sys, os, time, json, logging
from concurrent.futures import ThreadPoolExecutor, Future
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import FOLDERS, EXTENSIONS, MEMORY_PATH

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("watcher")

# ── Costanti ───────────────────────────────────────────────────────────────────
POLL_INTERVAL     = 10   # secondi tra ogni scan
DEBOUNCE_CYCLES   = 1    # cicli con mtime stabile prima di indicizzare (~10s di grazia)
MAX_INDEX_WORKERS = 2    # thread paralleli per indicizzazione
REGISTRY_FILE     = Path(MEMORY_PATH) / "watcher_registry.json"
REGISTRY_SAVE_EVERY = 6  # salva il registro ogni N cicli (~60s)

# ── Caricamento agenti — resiliente ────────────────────────────────────────────
# Se un agente fallisce, gli altri continuano a funzionare normalmente.
AGENTS: dict = {}   # agent_name -> (module, extensions_set)

def _load_agents():
    import importlib
    specs = [
        ("financial", "financial_agent"),
        ("drawings",  "drawings_agent"),
        ("documents", "documents_agent"),
    ]
    for agent_name, module_name in specs:
        try:
            mod = importlib.import_module(module_name)
            AGENTS[agent_name] = (mod, EXTENSIONS[agent_name])
            logger.info(f"  ✓ [{agent_name}] {mod.collection.count():,} chunk in DB")
        except Exception as exc:
            logger.error(f"  ✗ [{agent_name}] ERRORE caricamento — {exc}", exc_info=True)

_load_agents()

# ── Registro persistente ───────────────────────────────────────────────────────

def load_registry() -> dict:
    """
    Carica il registro {path: mtime} dal disco.
    All'avvio i file già indicizzati (mtime invariato) vengono saltati,
    risparmiando decine di secondi di re-indicizzazione inutile.
    """
    if REGISTRY_FILE.exists():
        try:
            return json.loads(REGISTRY_FILE.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning(f"  [registry] Impossibile caricare, si riparte da zero: {exc}")
    return {}


def save_registry(registry: dict):
    """Salva il registro su disco in modo sicuro (write → rename)."""
    try:
        REGISTRY_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = REGISTRY_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(registry, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.replace(REGISTRY_FILE)
    except Exception as exc:
        logger.warning(f"  [registry] Impossibile salvare: {exc}")

# ── Routing ────────────────────────────────────────────────────────────────────

def get_agent_for(filepath: str) -> tuple | None:
    """
    Restituisce (agent_name, module) per il file dato basandosi sull'estensione.
    Ritorna None se nessun agente supporta quel formato.
    """
    ext = Path(filepath).suffix.lower()
    for agent_name, (mod, exts) in AGENTS.items():
        if ext in exts:
            return agent_name, mod
    return None

# ── Indicizzazione ─────────────────────────────────────────────────────────────

def index_file_task(filepath: str) -> int:
    """
    Eseguito in un thread del pool.
    Chiama index_file() dell'agente corretto.
    Restituisce il numero di chunk creati (0 = saltato o errore).
    """
    result = get_agent_for(filepath)
    if result is None:
        logger.debug(f"  [skip] {Path(filepath).name} — formato non supportato")
        return 0

    agent_name, mod = result
    try:
        n = mod.index_file(filepath)
        if n:
            logger.info(f"  [+] [{agent_name.upper()}] {Path(filepath).name} → {n} chunk")
        else:
            logger.debug(f"  [~] [{agent_name.upper()}] {Path(filepath).name} → saltato")
        return n
    except Exception as exc:
        logger.error(
            f"  [!] [{agent_name.upper()}] Errore indicizzando {Path(filepath).name}: {exc}",
            exc_info=True,
        )
        return 0

# ── Rimozione ──────────────────────────────────────────────────────────────────

def remove_file_from_db(filepath: str):
    """
    Rimuove tutti i chunk di un file eliminato da ChromaDB.
    Usa where={"path": filepath} — nessun caricamento dell'intero DB.
    Chiamato direttamente nel loop principale (operazione veloce).
    """
    p = Path(filepath)
    for agent_name, (mod, _) in AGENTS.items():
        try:
            existing = mod.collection.get(where={"path": filepath})
            ids = existing.get("ids", [])
            if ids:
                mod.collection.delete(ids=ids)
                logger.info(
                    f"  [-] [{agent_name.upper()}] {p.name} rimosso "
                    f"({len(ids)} chunk)"
                )
        except Exception as exc:
            logger.warning(
                f"  [!] [{agent_name.upper()}] Rimozione {p.name} fallita: {exc}"
            )

# ── Loop principale ────────────────────────────────────────────────────────────

def run():
    logger.info("=" * 55)
    logger.info("Global File Watcher v2 — avvio")

    # Crea cartelle monitorate se non esistono
    for agent_name, folder in FOLDERS.items():
        Path(folder).mkdir(parents=True, exist_ok=True)
        logger.info(f"  [*] Monitoraggio [{agent_name}]: {folder}")

    # Stato iniziale
    registry: dict[str, float] = load_registry()   # path -> mtime dell'ultima indicizzazione
    pending:  dict[str, tuple] = {}                 # path -> (mtime, cicli_stabili)
    in_progress: set[str]      = set()              # file in indicizzazione ora

    logger.info(f"  [*] File nel registro: {len(registry)}")
    logger.info("=" * 55)

    executor     = ThreadPoolExecutor(max_workers=MAX_INDEX_WORKERS, thread_name_prefix="idx")
    futures:     dict[str, Future] = {}
    save_counter = 0

    while True:
        try:
            seen_paths: set[str] = set()

            # ── 1. Scan cartelle ────────────────────────────────────────────
            for agent_name, folder in FOLDERS.items():
                if agent_name not in AGENTS:
                    continue
                _, exts = AGENTS[agent_name]

                for f in Path(folder).rglob("*"):
                    if not f.is_file():
                        continue
                    if f.suffix.lower() not in exts:
                        continue

                    try:
                        mtime = f.stat().st_mtime
                    except Exception:
                        continue   # file rimosso durante lo scan

                    key = str(f)
                    seen_paths.add(key)

                    # File già in indicizzazione → skip
                    if key in in_progress:
                        continue

                    # File non cambiato rispetto all'ultima indicizzazione → skip
                    if registry.get(key) == mtime:
                        pending.pop(key, None)
                        continue

                    # ── 2. Debounce ─────────────────────────────────────────
                    # Aspetta che l'mtime sia stabile per almeno DEBOUNCE_CYCLES scan
                    # prima di avviare l'indicizzazione.
                    prev_mtime, cycles = pending.get(key, (None, 0))

                    if prev_mtime == mtime:
                        cycles += 1
                        pending[key] = (mtime, cycles)

                        if cycles >= DEBOUNCE_CYCLES:
                            # mtime stabile → avvia indicizzazione in background
                            pending.pop(key, None)
                            in_progress.add(key)

                            captured_key   = key
                            captured_mtime = mtime

                            def _on_done(
                                fut: Future,
                                _path=captured_key,
                                _mtime=captured_mtime
                            ):
                                in_progress.discard(_path)
                                futures.pop(_path, None)
                                if fut.exception() is None and fut.result():
                                    # Aggiorna il registro solo se l'indicizzazione è riuscita
                                    registry[_path] = _mtime

                            fut = executor.submit(index_file_task, key)
                            fut.add_done_callback(_on_done)
                            futures[key] = fut
                    else:
                        # mtime cambiato (o prima volta) → reset debounce
                        pending[key] = (mtime, 0)

            # ── 3. Rilevamento eliminazioni ─────────────────────────────────
            # Qualunque path nel registro che non è più presente sul disco
            # è stato eliminato — va rimosso anche da ChromaDB.
            deleted = set(registry.keys()) - seen_paths - in_progress
            for key in deleted:
                logger.info(f"  [~] File eliminato: {Path(key).name}")
                remove_file_from_db(key)
                registry.pop(key, None)
                pending.pop(key, None)

            # ── 4. Salvataggio periodico registro ──────────────────────────
            save_counter += 1
            if save_counter >= REGISTRY_SAVE_EVERY or deleted:
                save_registry(registry)
                save_counter = 0

            time.sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            logger.info("\n[*] Watcher fermato — salvataggio registro...")
            executor.shutdown(wait=True)
            save_registry(registry)
            logger.info("[*] Uscita.")
            break

        except Exception as exc:
            logger.error(f"[!] Errore loop watcher: {exc}", exc_info=True)
            time.sleep(15)   # pausa più lunga in caso di errore persistente


if __name__ == "__main__":
    run()
