#!/usr/bin/env python3
"""
Gemeinsames Modul: Lädt PMB-Gesamtlisten von pmb.acdh.oeaw.ac.at/media/
in ein lokales Verzeichnis (data/indices-pmb/), wenn sie dort noch nicht
vorhanden sind.

Verwendung als Modul:
    from download_pmb_lists import ensure_pmb_lists, PMB_DIR
    ensure_pmb_lists()

Verwendung als Skript:
    uv run pyscripts/download_pmb_lists.py
"""

import sys
import requests
from pathlib import Path

PMB_DIR = Path("data/indices-pmb")

PMB_URLS = {
    "listperson.xml": "https://pmb.acdh.oeaw.ac.at/media/listperson.xml",
    "listplace.xml":  "https://pmb.acdh.oeaw.ac.at/media/listplace.xml",
    "listorg.xml":    "https://pmb.acdh.oeaw.ac.at/media/listorg.xml",
    "listbibl.xml":   "https://pmb.acdh.oeaw.ac.at/media/listbibl.xml",
    "listevent.xml":  "https://pmb.acdh.oeaw.ac.at/media/listevent.xml",
}


def ensure_pmb_lists(out_dir: Path = PMB_DIR, force: bool = False) -> Path:
    """Stellt sicher, dass alle PMB-Listen lokal vorhanden sind.

    Bereits vorhandene Dateien werden übersprungen (außer force=True).
    Gibt den Pfad zum Verzeichnis zurück.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    headers = {
        "Content-type": "application/xml; charset=utf-8",
        "Accept-Charset": "utf-8",
    }
    errors = 0
    for filename, url in PMB_URLS.items():
        out_path = out_dir / filename
        if out_path.exists() and not force:
            print(f"📂 {filename} bereits vorhanden, wird übersprungen")
            sys.stdout.flush()
            continue
        print(f"🌐 Lade {filename} von {url} ...")
        sys.stdout.flush()
        try:
            r = requests.get(url, headers=headers, timeout=120)
            if r.status_code != 200:
                print(f"❌ HTTP {r.status_code} für {url}")
                errors += 1
                continue
            out_path.write_bytes(r.content)
            print(f"✅ Gespeichert: {out_path} ({len(r.content) // 1024} KB)")
        except Exception as e:
            print(f"❌ Fehler beim Laden von {filename}: {e}")
            errors += 1
        sys.stdout.flush()

    if errors:
        raise RuntimeError(f"{errors} PMB-Datei(en) konnten nicht geladen werden")

    return out_dir


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="PMB-Listen herunterladen")
    parser.add_argument("--force", action="store_true",
                        help="Bereits vorhandene Dateien überschreiben")
    args = parser.parse_args()
    ensure_pmb_lists(force=args.force)
    print(f"\n✅ Alle PMB-Listen in {PMB_DIR}/ bereit")
