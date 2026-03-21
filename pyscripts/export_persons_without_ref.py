#!/usr/bin/env python3
"""
export_persons_without_ref.py

Durchsucht alle Editions-XML-Dateien nach <person>-Elementen ohne PMB-Referenz
und exportiert die persName-Texte als CSV mit den Spalten:
  nachname, vorname, beruf, ort, original

Nutzung:
    python3 export_persons_without_ref.py [--dir PFAD] [--out DATEI]
"""

import argparse
import csv
import glob
import os
import re
import unicodedata

from lxml import etree

TEI_NS = "http://www.tei-c.org/ns/1.0"
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EDITIONS = os.path.join(BASE_DIR, "data", "editions")
DEFAULT_OUT = os.path.join(BASE_DIR, "persons_without_ref_split.csv")


def tei(local):
    return f"{{{TEI_NS}}}{local}"


def has_pmb(el):
    ref = el.get("ref", "")
    source = el.get("source", "")
    return "pmb" in ref.lower() or "pmb.acdh.oeaw.ac.at" in source


def parse_persname(text):
    """
    Zerlegt einen persName-Text in Spalten.

    Beispiele:
      "Karpp, J. (frz. Übersetzer, Cannes)"
          -> nachname="Karpp", vorname="J.", beruf="frz. Übersetzer", ort="Cannes"
      "Bianchini (Mitarbeiter Société des Auteurs, Paris)"
          -> nachname="Bianchini", vorname="", beruf="Mitarbeiter …", ort="Paris"
      "Beer-Hofmann, Richard"
          -> nachname="Beer-Hofmann", vorname="Richard", beruf="", ort=""
      "??lkner (Theaterdirektor)"
          -> nachname="??lkner", vorname="", beruf="Theaterdirektor", ort=""
    """
    text = text.strip()
    nachname = vorname = beruf = ort = ""

    # Klammerinhalt extrahieren
    paren_content = ""
    m = re.search(r"\(([^)]*)\)\s*$", text)
    if m:
        paren_content = m.group(1).strip()
        base = text[: m.start()].strip().rstrip(",").strip()
    else:
        base = text

    # Nachname / Vorname aus dem Basisteil
    if "," in base:
        parts = base.split(",", 1)
        nachname = parts[0].strip()
        vorname = parts[1].strip()
    else:
        nachname = base.strip()

    # Beruf / Ort aus dem Klammerinhalt
    if paren_content:
        if "," in paren_content:
            b, o = paren_content.split(",", 1)
            beruf = b.strip()
            ort = o.strip()
        else:
            beruf = paren_content

    return nachname, vorname, beruf, ort


def collect_persons(editions_dir):
    """
    Gibt eine sortierte, deduplizierte Liste von (nachname, vorname, beruf, ort, original)
    für alle Personen ohne PMB-Referenz zurück.
    """
    seen = set()
    rows = []

    parser = etree.XMLParser(recover=True)
    for path in sorted(glob.glob(os.path.join(editions_dir, "*.xml"))):
        try:
            tree = etree.parse(path, parser)
        except etree.XMLSyntaxError:
            continue

        root = tree.getroot()
        for person in root.findall(f".//{tei('person')}"):
            if has_pmb(person):
                continue
            pn = person.find(tei("persName"))
            if pn is None or not (pn.text or "").strip():
                continue

            original = pn.text.strip()
            if original in seen:
                continue
            seen.add(original)

            nachname, vorname, beruf, ort = parse_persname(original)
            rows.append((nachname, vorname, beruf, ort, original))

    rows.sort(key=lambda r: (
        unicodedata.normalize("NFC", r[0]).lower(),
        unicodedata.normalize("NFC", r[1]).lower(),
    ))
    return rows


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--dir", default=EDITIONS,
        help=f"Editions-Verzeichnis (Standard: {EDITIONS})",
    )
    ap.add_argument(
        "--out", default=DEFAULT_OUT,
        help=f"Ausgabedatei (Standard: {DEFAULT_OUT})",
    )
    args = ap.parse_args()

    print("Durchsuche Editions-Dateien …")
    rows = collect_persons(args.dir)

    with open(args.out, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["nachname", "vorname", "beruf", "ort", "original"])
        writer.writerows(rows)

    print(f"  {len(rows)} Personen ohne PMB-Referenz")
    print(f"  Ausgabe: {args.out}")


if __name__ == "__main__":
    main()
