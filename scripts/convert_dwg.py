"""
DWG to DXF converter
Runs LibreCAD in headless mode to convert DWG files to DXF
Called automatically by the watcher when a .dwg file is detected
"""
import subprocess, os, sys
from pathlib import Path

def convert_dwg_to_dxf(dwg_path):
    """Convert a DWG file to DXF using LibreCAD CLI."""
    dwg  = Path(dwg_path)
    dxf  = dwg.with_suffix(".dxf")
    if dxf.exists():
        print(f"  [SKIP] DXF already exists: {dxf.name}")
        return str(dxf)
    try:
        result = subprocess.run(
            ["librecad", "dxf2dxf", "--output", str(dxf), str(dwg)],
            capture_output=True, text=True, timeout=60
        )
        if dxf.exists():
            print(f"  [OK] Converted: {dwg.name} -> {dxf.name}")
            return str(dxf)
        else:
            print(f"  [FAIL] Conversion failed: {dwg.name}")
            return None
    except FileNotFoundError:
        print("  [ERROR] LibreCAD not installed. Install with: sudo apt install librecad")
        return None
    except subprocess.TimeoutExpired:
        print(f"  [TIMEOUT] Conversion timeout for: {dwg.name}")
        return None
    except Exception as e:
        print(f"  [ERROR] {e}")
        return None

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python convert_dwg.py file.dwg")
        sys.exit(1)
    result = convert_dwg_to_dxf(sys.argv[1])
    print(f"Result: {result}")
