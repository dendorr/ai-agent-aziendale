"""
GLOBAL FILE WATCHER
Monitors all three agent folders every 10 seconds.
Routes each new or modified file to the correct agent for indexing.
Detects file modifications via mtime (modification timestamp).
"""
import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import FOLDERS, EXTENSIONS
from pathlib import Path

import financial_agent as financial
import drawings_agent  as drawings
import documents_agent as documents

# Map each agent name to its module and supported extensions
AGENTS = {
    "financial": (financial, EXTENSIONS["financial"]),
    "drawings":  (drawings,  EXTENSIONS["drawings"]),
    "documents": (documents, EXTENSIONS["documents"]),
}

def route_and_index(filepath):
    """Determine which agent handles this file and index it."""
    p   = Path(filepath)
    ext = p.suffix.lower()
    for agent_name, (agent_module, extensions) in AGENTS.items():
        if ext in extensions:
            n = agent_module.index_file(filepath)
            if n:
                print(f"[{agent_name.upper()}] {p.name} -> {n} chunks", flush=True)
            return
    print(f"[SKIP] {Path(filepath).name} — unsupported format", flush=True)

def remove_from_index(filepath):
    """Remove all chunks of a deleted file from all agent databases."""
    p = Path(filepath)
    for agent_name, (agent_module, _) in AGENTS.items():
        try:
            all_ids = agent_module.collection.get()["ids"]
            ids_to_delete = [i for i in all_ids if i.startswith(str(filepath) + "__c")]
            if ids_to_delete:
                agent_module.collection.delete(ids=ids_to_delete)
                print(f"[{agent_name.upper()}] Removed {p.name} ({len(ids_to_delete)} chunks)", flush=True)
        except Exception:
            pass

def run():
    """Main watcher loop — polls all folders every 10 seconds."""
    print("[*] Global file watcher started", flush=True)
    for agent_name, folder in FOLDERS.items():
        Path(folder).mkdir(parents=True, exist_ok=True)
        print(f"[*] Monitoring [{agent_name}]: {folder}", flush=True)

    # Print initial database stats
    for agent_name, (agent_module, _) in AGENTS.items():
        print(f"[*] {agent_name}: {agent_module.collection.count()} chunks in DB", flush=True)

    # Track files by path -> last modification time
    file_registry = {}

    while True:
        try:
            for agent_name, folder in FOLDERS.items():
                _, extensions = AGENTS[agent_name]
                for f in Path(folder).rglob("*"):
                    if not f.is_file(): continue
                    if f.suffix.lower() not in extensions: continue
                    try:
                        mtime = f.stat().st_mtime
                    except Exception:
                        continue
                    key = str(f)
                    if file_registry.get(key) != mtime:
                        file_registry[key] = mtime
                        route_and_index(str(f))

            time.sleep(10)

        except KeyboardInterrupt:
            print("\n[*] Watcher stopped.", flush=True)
            break
        except Exception as e:
            print(f"[!] Watcher error: {e}", flush=True)
            time.sleep(15)

if __name__ == "__main__":
    run()