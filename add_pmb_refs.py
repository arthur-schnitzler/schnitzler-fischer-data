#!/usr/bin/env python3
"""
add_pmb_refs.py

Durchsucht alle Editions-XML-Dateien nach Entitaeten (bibl, person, org,
place) in sourceDesc und profileDesc, die noch keine PMB-Nummer haben, und
ergaenzt ref="#pmbXXXX" sowie source="https://pmb.acdh.oeaw.ac.at/entity/X/",
soweit ein Treffer in den indices-pmb-Dateien gefunden wird.

Sonderfall: orgName-Elemente, deren Text als Python-Dict-Literal vorliegt
(z.B. {'name': '...', 'id': '...', 'type': 'PMB', 'url': '...'}), werden
korrekt aufgeloest und der Text-Inhalt auf den reinen Namen reduziert.

Nutzung:
    python3 add_pmb_refs.py [--dry-run] [--verbose] [--file PFAD]
"""

import ast
import glob
import os
import re
import unicodedata
import argparse
from lxml import etree

# ── Konstanten ────────────────────────────────────────────────────────────────
TEI_NS = "http://www.tei-c.org/ns/1.0"
NS = {"tei": TEI_NS}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EDITIONS = os.path.join(BASE_DIR, "editions")
INDICES = os.path.join(BASE_DIR, "indices-pmb")


# ── Hilfsfunktionen ──────────────────────────────────────────────────────────
def tei(local):
    return f"{{{TEI_NS}}}{local}"


def normalize(s):
    """Normalisiert fuer Vergleiche: NFC, Kleinschreibung, Whitespace."""
    if not s:
        return ""
    s = unicodedata.normalize("NFC", s)
    return " ".join(s.lower().split())


def strip_parens(s):
    """Entfernt Klammerhinweise: 'Name (Hinweis)' -> 'Name'."""
    return re.sub(r"\s*\([^)]*\)", "", s).strip()


def pmb_id_from_url(url):
    """Extrahiert PMB-Nummer aus https://pmb.acdh.oeaw.ac.at/entity/12345/"""
    m = re.search(r"/entity/(\d+)/", url or "")
    return m.group(1) if m else None


def has_pmb(el):
    """True wenn das Element bereits eine PMB-Nummer traegt."""
    ref = el.get("ref", "")
    source = el.get("source", "")
    return "pmb" in ref.lower() or "pmb.acdh.oeaw.ac.at" in source


def try_parse_dict_text(text):
    """
    Versucht, einen Python-Dict-Literal-String zu parsen, wie er in manchen
    orgName-Elementen vorkommt:
        {'name': 'Foo Bar', 'id': '123', 'type': 'GND', 'url': '...'}
    Gibt das Dict zurueck oder None.
    """
    if not text or not text.strip().startswith("{"):
        return None
    try:
        d = ast.literal_eval(text.strip())
        if isinstance(d, dict) and "name" in d:
            return d
    except Exception:
        pass
    return None


# ── Index laden ──────────────────────────────────────────────────────────────
def _load_index(path, entity_tag, name_tags):
    """
    Generischer Index-Loader.
    entity_tag : lokaler Element-Name im Index (person/bibl/org/place)
    name_tags  : Liste lokaler Name-Tags als Schluessel
    Gibt Dict: normalisierter_Name -> {'pmb_id': '...', 'url': '...'}
    """
    parser = etree.XMLParser(recover=True)
    tree = etree.parse(path, parser)
    root = tree.getroot()
    index = {}

    for entity in root.findall(f".//{tei(entity_tag)}"):
        pmb_url = None
        for idno in entity.findall(tei("idno")):
            if idno.get("subtype") == "pmb":
                pmb_url = idno.text
                break
        if not pmb_url:
            continue
        pmb_id = pmb_id_from_url(pmb_url)
        if not pmb_id:
            continue

        entry = {"pmb_id": pmb_id, "url": pmb_url.rstrip("/") + "/"}

        for ntag in name_tags:
            for name_el in entity.findall(f".//{tei(ntag)}"):
                forename = name_el.find(tei("forename"))
                surname = name_el.find(tei("surname"))
                if forename is not None or surname is not None:
                    parts = []
                    if surname is not None and surname.text:
                        parts.append(surname.text.strip())
                    if forename is not None and forename.text:
                        parts.append(forename.text.strip())
                    full = ", ".join(parts) if parts else None
                    if full:
                        index.setdefault(normalize(full), entry)
                elif name_el.text:
                    index.setdefault(normalize(name_el.text), entry)

    return index


def load_all_indices():
    print("Indizes werden geladen ...")
    persons = _load_index(
        os.path.join(INDICES, "listperson.xml"), "person", ["persName"]
    )
    bibls = _load_index(
        os.path.join(INDICES, "listbibl.xml"), "bibl", ["title"]
    )
    orgs = _load_index(
        os.path.join(INDICES, "listorg.xml"), "org", ["orgName"]
    )
    places = _load_index(
        os.path.join(INDICES, "listplace.xml"), "place", ["placeName"]
    )
    print(
        f"  Personen: {len(persons):,}  Werke: {len(bibls):,}  "
        f"Orgs: {len(orgs):,}  Orte: {len(places):,}"
    )
    return persons, bibls, orgs, places


# ── Namens-Lookup ─────────────────────────────────────────────────────────────
def lookup_person(text, index):
    """Versucht mehrere Namensvarianten."""
    bare = strip_parens(text)
    candidates = [text, bare]

    if "," in bare:
        surname, forenames = bare.split(",", 1)
        surname = surname.strip()
        forenames = forenames.strip()

        candidates.append(f"{forenames} {surname}")

        # Nur erster Vorname: "Bölsche, Wilhelm Karl Eduard" -> "Bölsche, Wilhelm"
        first = forenames.split()[0] if forenames else ""
        if first and first != forenames:
            candidates.append(f"{surname}, {first}")
            candidates.append(f"{first} {surname}")

    for c in candidates:
        entry = index.get(normalize(c))
        if entry:
            return entry
    return None


def lookup_bibl(text, index):
    """Versucht Titel-Matching mit verschiedenen Normalisierungen."""
    candidates = [text, strip_parens(text)]
    if ":" in text:
        after = text.split(":", 1)[1].strip()
        candidates += [after, strip_parens(after)]

    for c in candidates:
        entry = index.get(normalize(c))
        if entry:
            return entry
    return None


def lookup_generic(text, index):
    """Einfaches Lookup mit optionalem Klammerentfernen."""
    for c in [text, strip_parens(text)]:
        entry = index.get(normalize(c))
        if entry:
            return entry
    return None


# ── Entitaets-Verarbeitung ────────────────────────────────────────────────────
def get_text(el, local_tag):
    child = el.find(tei(local_tag))
    return child.text if child is not None else None


def process_entity(el, el_type, name_tag, lookup_fn, index, changes):
    """
    Prueft ein einzelnes Element; fuegt PMB-Ref hinzu wenn gefunden.
    Gibt True zurueck wenn geaendert.
    """
    if has_pmb(el):
        return False

    # Sonderfall: orgName mit Python-Dict-Literal
    if el_type == "org":
        name_el = el.find(tei("orgName"))
        if name_el is not None and name_el.text:
            d = try_parse_dict_text(name_el.text)
            if d is not None:
                clean_name = d.get("name", "").strip()
                id_type = d.get("type", "").upper()
                id_val = str(d.get("id", ""))
                id_url = d.get("url", "")

                if id_type == "PMB":
                    pmb_id = pmb_id_from_url(id_url)
                    if pmb_id:
                        el.set("ref", f"#pmb{pmb_id}")
                        el.set("source", id_url.rstrip("/") + "/")
                        name_el.text = clean_name
                        changes.append(
                            (el_type, clean_name, pmb_id, "dict-pmb")
                        )
                        return True
                elif id_type == "GND":
                    match = lookup_fn(clean_name, index)
                    if match:
                        el.set("ref", f"#pmb{match['pmb_id']}")
                        el.set("source", match["url"])
                        name_el.text = clean_name
                        changes.append(
                            (el_type, clean_name, match["pmb_id"],
                             "dict-gnd->pmb")
                        )
                        return True
                    else:
                        el.set("ref", id_val)
                        el.set("source", id_url)
                        name_el.text = clean_name
                        changes.append(
                            (el_type, clean_name, None, "dict-gnd-only")
                        )
                        return True
                name_el.text = clean_name
                return True

    name = get_text(el, name_tag)
    if not name:
        changes.append((el_type, "", None, "no-name"))
        return False

    match = lookup_fn(name, index)
    if match:
        el.set("ref", f"#pmb{match['pmb_id']}")
        el.set("source", match["url"])
        changes.append((el_type, name, match["pmb_id"], "matched"))
        return True
    else:
        changes.append((el_type, name, None, "no-match"))
        return False


# ── Datei-Verarbeitung ───────────────────────────────────────────────────────
def process_file(xml_file, persons, bibls, orgs, places,
                 dry_run=False, verbose=False):
    """Verarbeitet eine Editions-Datei. Gibt (modified, changes) zurueck."""
    parser = etree.XMLParser(recover=True, remove_blank_text=False)
    try:
        tree = etree.parse(xml_file, parser)
    except etree.XMLSyntaxError as e:
        print(f"  XML-Fehler in {xml_file}: {e}")
        return False, []

    root = tree.getroot()
    changes = []
    modified = False

    sections = (
        root.findall(".//tei:sourceDesc", NS)
        + root.findall(".//tei:profileDesc", NS)
    )

    entity_cfg = [
        ("person", "persName", lookup_person, persons),
        ("bibl", "title", lookup_bibl, bibls),
        ("org", "orgName", lookup_generic, orgs),
        ("place", "placeName", lookup_generic, places),
    ]

    for section in sections:
        for el_type, name_tag, lookup_fn, index in entity_cfg:
            for el in section.findall(f".//{tei(el_type)}"):
                if process_entity(
                    el, el_type, name_tag, lookup_fn, index, changes
                ):
                    modified = True

    if modified and not dry_run:
        tree.write(
            xml_file,
            encoding="UTF-8",
            xml_declaration=True,
            pretty_print=True,
        )

    if verbose and changes:
        fname = os.path.basename(xml_file)
        for el_type, name, pmb_id, status in changes:
            arrow = f"-> pmb{pmb_id}" if pmb_id else "KEIN TREFFER"
            print(f"  [{fname}] {el_type}: \"{name}\" {arrow} ({status})")

    return modified, changes


# ── Hauptprogramm ─────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--dry-run", action="store_true",
        help="Nur auswerten, nichts schreiben"
    )
    ap.add_argument(
        "--verbose", action="store_true",
        help="Jede Aenderung ausgeben"
    )
    ap.add_argument("--file", help="Nur eine einzelne Datei verarbeiten")
    ap.add_argument(
        "--dir", default=EDITIONS,
        help=f"Editions-Verzeichnis (Standard: {EDITIONS})",
    )
    args = ap.parse_args()

    persons, bibls, orgs, places = load_all_indices()
    print()

    if args.file:
        files = [args.file]
    else:
        files = sorted(glob.glob(os.path.join(args.dir, "*.xml")))

    total_files = len(files)
    modified_files = 0
    all_changes = []

    for xml_file in files:
        modified, changes = process_file(
            xml_file, persons, bibls, orgs, places,
            dry_run=args.dry_run,
            verbose=args.verbose,
        )
        if modified:
            modified_files += 1
        for c in changes:
            all_changes.append((os.path.basename(xml_file),) + c)

    matched = [
        c for c in all_changes
        if c[3] is not None
        and c[4] not in ("no-match", "dict-gnd-only", "no-name")
    ]
    unmatched = [
        c for c in all_changes
        if c[3] is None or c[4] in ("no-match", "no-name")
    ]
    gnd_only = [c for c in all_changes if c[4] == "dict-gnd-only"]

    action = "wuerden geaendert werden" if args.dry_run else "wurden geaendert"
    print(f"\nErgebnis:")
    print(f"  Dateien {action}:        {modified_files} / {total_files}")
    print(f"  Entitaeten mit PMB:       {len(matched)}")
    print(f"  Entitaeten ohne Treffer:  {len(unmatched)}")
    if gnd_only:
        print(f"  Nur GND, kein PMB:       {len(gnd_only)}")

    log_path = os.path.join(BASE_DIR, "pmb_refs_log.txt")
    with open(log_path, "w", encoding="utf-8") as f:
        f.write("=== Gefundene PMB-Treffer ===\n")
        for fname, el_type, name, pmb_id, status in matched:
            f.write(
                f"{fname}  [{el_type}]  \"{name}\"  "
                f"-> pmb{pmb_id}  ({status})\n"
            )
        f.write("\n=== Kein PMB-Treffer ===\n")
        for fname, el_type, name, pmb_id, status in unmatched:
            f.write(f"{fname}  [{el_type}]  \"{name}\"  ({status})\n")
        if gnd_only:
            f.write("\n=== Nur GND (kein PMB-Treffer) ===\n")
            for fname, el_type, name, pmb_id, status in gnd_only:
                f.write(f"{fname}  [{el_type}]  \"{name}\"  ({status})\n")

    print(f"\nProtokoll: {log_path}")


if __name__ == "__main__":
    main()
