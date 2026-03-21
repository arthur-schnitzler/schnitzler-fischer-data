#!/usr/bin/env python3
"""
Findet Briefverweise in note[@type='description'] und div[@type='letter']/p
und ersetzt sie durch <ref target="ID">text</ref> Elemente.

Beispiel:
  Vorher: Antwort auf: 1931-09-05 AS (Wien) an SF (Berlin)
  Nachher: Antwort auf: <ref target="11700">1931-09-05 AS (Wien) an SF (Berlin)</ref>
"""

import os
import re
import glob
import argparse
from lxml import etree

TEI_NS = "http://www.tei-c.org/ns/1.0"
NS = {"tei": TEI_NS}

# Vollnamen → Abkürzungen
PERSON_TO_ABBR = {
    "Arthur Schnitzler": "AS",
    "Samuel Fischer": "SF",
    "Oskar Bie": "OB",
    "Gottfried Bermann": "GB",
    "Gottfried Bermann Fischer": "GB",
    "Rudolf Kayser": "RK",
    "Lothar Schmidt": "LS",
    "Carl Schur": "CS",
    "Ernst Peter Tal": "EPT",
    "Peter Paul Schmitt": "PPS",
    "Moritz Heimann": "MH",
    "Leo Greiner": "LG",
    "Hans Jacob": "HJ",
    "Heinrich Eippner": "HE",
    "Heinrich Simon": "HS",
    "Konrad Maril": "KM",
    "Regina Rosenbaum": "RR",
    "Norbert Hoffmann": "NH",
    "Otto Greiß": "OG",
    "Paul Eipper": "PE",
    "Otto Rublack": "OR",
    "NN": "NN",
}

# Alternative Abkürzungen (Fehler/Varianten in den Daten)
ABBR_ALIASES = {
    "FS": "SF",  # Tippfehler für SF in einzelnen Dateien
    "GB": "GB",  # Gottfried Bermann (manchmal ohne Fischer)
}


def names_to_abbr(names_str):
    """'Samuel Fischer / Gottfried Bermann' → 'SF/GB'"""
    names = [n.strip() for n in names_str.split("/")]
    abbrs = []
    for name in names:
        abbr = PERSON_TO_ABBR.get(name)
        if abbr is None:
            return None  # Unbekannte Person
        abbrs.append(abbr)
    return "/".join(abbrs)


def parse_main_title(title):
    """
    Parst 'YYYY-MM-DD Name(s) (Stadt) an Name(s) (Stadt)' in Komponenten.
    Gibt None zurück wenn nicht parsbar.
    """
    m = re.match(r'^(\d{4}-\d{2}-\d{2}[a-z]?)\s+(.+?)\s+an\s+(.+)$', title)
    if not m:
        return None

    date = m.group(1)
    sender_part = m.group(2).strip()
    receiver_part = m.group(3).strip()

    def extract_names_city(part):
        city_m = re.match(r'^(.+?)\s*\(([^)]+)\)\s*$', part)
        if city_m:
            return city_m.group(1).strip(), city_m.group(2).strip()
        return part.strip(), None

    sender_names, sender_city = extract_names_city(sender_part)
    receiver_names, receiver_city = extract_names_city(receiver_part)

    sender_abbr = names_to_abbr(sender_names)
    receiver_abbr = names_to_abbr(receiver_names)

    if sender_abbr is None or receiver_abbr is None:
        return None

    return {
        "date": date,
        "sender_abbr": sender_abbr,
        "sender_city": sender_city,
        "receiver_abbr": receiver_abbr,
        "receiver_city": receiver_city,
    }


def make_keys(info):
    """Erzeugt alle möglichen Lookup-Schlüssel für einen Brief (mit/ohne Städte)."""
    d = info["date"]
    sc = info["sender_city"]
    rc = info["receiver_city"]

    # Abkürzungs-Varianten: volle Gruppe und nur erste Person
    sender_variants = {info["sender_abbr"]}
    first_s = info["sender_abbr"].split("/")[0]
    if first_s != info["sender_abbr"]:
        sender_variants.add(first_s)

    receiver_variants = {info["receiver_abbr"]}
    first_r = info["receiver_abbr"].split("/")[0]
    if first_r != info["receiver_abbr"]:
        receiver_variants.add(first_r)

    keys = []
    for sa in sender_variants:
        for ra in receiver_variants:
            if sc and rc:
                keys.append(f"{d} {sa} ({sc}) an {ra} ({rc})")
            if sc:
                keys.append(f"{d} {sa} ({sc}) an {ra}")
            if rc:
                keys.append(f"{d} {sa} an {ra} ({rc})")
            keys.append(f"{d} {sa} an {ra}")

    return keys


def build_index(xml_dir):
    """Baut Index: Referenz-Schlüssel → xml:id"""
    index = {}
    unresolved = []

    for xml_file in sorted(glob.glob(os.path.join(xml_dir, "*.xml"))):
        try:
            parser = etree.XMLParser(recover=True)
            tree = etree.parse(xml_file, parser)
        except etree.XMLSyntaxError as e:
            print(f"  XML-Fehler in {xml_file}: {e}")
            continue

        root = tree.getroot()

        # xml:id aus dem Root-Element
        xml_id = root.get("{http://www.w3.org/XML/1998/namespace}id")
        if not xml_id:
            xml_id = os.path.basename(xml_file).replace(".xml", "")

        # Haupttitel (erster titleStmt/title)
        title_elem = root.find(".//tei:titleStmt/tei:title", NS)
        if title_elem is None or not title_elem.text:
            continue

        title = title_elem.text.strip()

        # Nur Einträge mit Datumsformat
        if not re.match(r'\d{4}-\d{2}-\d{2}', title):
            continue

        info = parse_main_title(title)
        if info is None:
            unresolved.append((xml_id, title))
            continue

        for key in make_keys(info):
            if key in index and index[key] != xml_id:
                print(f"  WARNUNG: Doppelter Schlüssel '{key}': {index[key]} und {xml_id}")
            else:
                index[key] = xml_id

    return index, unresolved


def find_ref_match(text, pos, index):
    """
    Versucht ab Position pos einen Briefverweis im Index zu finden.
    Gibt (end_pos, ref_text, target_id) zurück oder None.
    """
    remainder = text[pos:]

    # Datum (mit optionalem Buchstaben-Suffix wie a, b)
    date_m = re.match(r'\d{4}-\d{2}-\d{2}[a-z]?', remainder)
    if not date_m:
        return None
    date = date_m.group(0)
    p = len(date)

    # Absender-Abkürzung
    sender_m = re.match(r'\s+([A-Z]{2}(?:/[A-Z]{2})*)', remainder[p:])
    if not sender_m:
        return None
    sender = sender_m.group(1)
    p += len(sender_m.group(0))

    # Optionale Absender-Stadt
    sender_city_m = re.match(r'\s+\(([^)]+)\)', remainder[p:])
    sender_city = None
    p_after_sc = p
    if sender_city_m:
        sender_city = sender_city_m.group(1)
        p_after_sc = p + len(sender_city_m.group(0))

    # " an "
    an_m = re.match(r'\s+an\s+', remainder[p_after_sc:])
    if not an_m:
        return None
    p2 = p_after_sc + len(an_m.group(0))

    # Empfänger-Abkürzung
    receiver_m = re.match(r'[A-Z]{2}(?:/[A-Z]{2})*', remainder[p2:])
    if not receiver_m:
        return None
    receiver = receiver_m.group(0)
    p3 = p2 + len(receiver_m.group(0))

    # Optionale Empfänger-Stadt
    receiver_city_m = re.match(r'\s+\(([^)]+)\)', remainder[p3:])
    receiver_city = None
    p_after_rc = p3
    if receiver_city_m:
        receiver_city = receiver_city_m.group(1)
        p_after_rc = p3 + len(receiver_city_m.group(0))

    # Kandidaten in Reihenfolge der Spezifizität prüfen
    candidates = []
    if sender_city and receiver_city:
        candidates.append((f"{date} {sender} ({sender_city}) an {receiver} ({receiver_city})", pos + p_after_rc))
        candidates.append((f"{date} {sender} ({sender_city}) an {receiver}", pos + p3))
        candidates.append((f"{date} {sender} an {receiver} ({receiver_city})", pos + p_after_rc))
    elif sender_city:
        candidates.append((f"{date} {sender} ({sender_city}) an {receiver}", pos + p3))
    elif receiver_city:
        candidates.append((f"{date} {sender} an {receiver} ({receiver_city})", pos + p_after_rc))
    candidates.append((f"{date} {sender} an {receiver}", pos + p3))

    for key, end_pos in candidates:
        if key in index:
            ref_text = text[pos:end_pos]
            return end_pos, ref_text, index[key]

    # Aliases auflösen (z.B. FS → SF) und erneut suchen
    def resolve_aliases(abbr):
        parts = abbr.split("/")
        resolved = [ABBR_ALIASES.get(p, p) for p in parts]
        return "/".join(resolved)

    sender_r = resolve_aliases(sender)
    receiver_r = resolve_aliases(receiver)
    if sender_r != sender or receiver_r != receiver:
        for key, end_pos in candidates:
            key_r = key.replace(sender, sender_r).replace(
                receiver, receiver_r
            )
            if key_r in index:
                ref_text = text[pos:end_pos]
                return end_pos, ref_text, index[key_r]

    return None


def split_text_with_refs(text, index):
    """
    Zerlegt Text in Segmente: [(text, target_or_None), ...]
    Segmente mit target sollen als <ref> gerendert werden.
    """
    result = []
    last_end = 0

    # Suche nach potenziellen Startpunkten (Datum-Pattern)
    for date_m in re.finditer(r'\d{4}-\d{2}-\d{2}', text):
        pos = date_m.start()
        if pos < last_end:
            continue  # bereits verarbeitet

        match = find_ref_match(text, pos, index)
        if match is None:
            continue

        end_pos, ref_text, target = match

        # Text vor dem Verweis
        if pos > last_end:
            result.append((text[last_end:pos], None))

        result.append((ref_text, target))
        last_end = end_pos

    # Resttext
    if last_end < len(text):
        result.append((text[last_end:], None))

    return result


def apply_refs_to_element(elem, index):
    """
    Ersetzt Briefverweise in einem Text-Element durch <ref>-Kindelemente.
    Gibt True zurück wenn Änderungen vorgenommen wurden.
    """
    # Nur wenn das Element reinen Text enthält (noch keine Kinder)
    if len(elem) > 0:
        return False
    if not elem.text:
        return False

    segments = split_text_with_refs(elem.text, index)

    # Änderungen nötig?
    if not any(target is not None for _, target in segments):
        return False

    # Element neu aufbauen
    elem.text = ""
    last_ref = None

    for seg_text, target in segments:
        if target is None:
            if last_ref is None:
                elem.text = (elem.text or "") + seg_text
            else:
                last_ref.tail = (last_ref.tail or "") + seg_text
        else:
            ref = etree.SubElement(elem, f"{{{TEI_NS}}}ref")
            ref.set("target", target)
            ref.text = seg_text
            ref.tail = ""
            last_ref = ref

    return True


def process_file(xml_file, index, dry_run=False, verbose=False):
    """
    Verarbeitet eine XML-Datei.
    Gibt True zurück wenn Änderungen vorgenommen wurden.
    """
    try:
        parser = etree.XMLParser(remove_blank_text=False, recover=True)
        tree = etree.parse(xml_file, parser)
    except etree.XMLSyntaxError as e:
        print(f"  XML-Fehler: {e}")
        return False

    root = tree.getroot()
    modified = False

    # note[@type='description']
    for note in root.findall(".//tei:note[@type='description']", NS):
        if apply_refs_to_element(note, index):
            modified = True
            if verbose:
                xml_id = root.get("{http://www.w3.org/XML/1998/namespace}id", "?")
                print(f"  [{xml_id}] note[@type='description'] geändert")

    # div[@type='letter']/p
    for p in root.findall(".//tei:div[@type='letter']/tei:p", NS):
        if apply_refs_to_element(p, index):
            modified = True
            if verbose:
                xml_id = root.get("{http://www.w3.org/XML/1998/namespace}id", "?")
                print(f"  [{xml_id}] div[@type='letter']/p geändert")

    if modified and not dry_run:
        tree.write(
            xml_file,
            encoding="UTF-8",
            xml_declaration=True,
            pretty_print=True,
        )

    return modified


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dir",
        default="/Users/oldfiche/git/schnitzler-fischer-data/editions",
        help="Verzeichnis mit TEI-XML-Dateien",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Nur zählen, nichts schreiben",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Jede Änderung ausgeben",
    )
    parser.add_argument(
        "--file",
        help="Nur eine einzelne Datei verarbeiten",
    )
    args = parser.parse_args()

    print("Index wird aufgebaut...")
    index, unresolved = build_index(args.dir)
    print(f"  {len(index)} Schlüssel für {len(set(index.values()))} Briefe")

    if unresolved:
        print(f"  {len(unresolved)} Titel konnten nicht aufgelöst werden:")
        for xml_id, title in unresolved[:10]:
            print(f"    {xml_id}: {title}")
        if len(unresolved) > 10:
            print(f"    ... und {len(unresolved) - 10} weitere")

    print()

    if args.file:
        files = [args.file]
    else:
        files = sorted(glob.glob(os.path.join(args.dir, "*.xml")))

    modified_count = 0
    total = len(files)

    for xml_file in files:
        if process_file(xml_file, index, dry_run=args.dry_run, verbose=args.verbose):
            modified_count += 1

    action = "würden geändert werden" if args.dry_run else "wurden geändert"
    print(f"{modified_count} von {total} Dateien {action}.")


if __name__ == "__main__":
    main()
