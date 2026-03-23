#!/usr/bin/env python3
"""
Transforms listPerson, listPlace, listBibl, listOrg in profileDesc
into a <note> element inside correspDesc, then removes the list elements.
"""
import os
import sys
from lxml import etree

TEI_NS = "http://www.tei-c.org/ns/1.0"
T = f"{{{TEI_NS}}}"

BASE_URL = (
    "https://raw.githubusercontent.com/arthur-schnitzler/"
    "schnitzler-fischer-data/refs/heads/main/data/editions/"
)

LOD = "https://lod.academy/cmif/vocab/terms#"


def strip_hash(val):
    if val and val.startswith("#"):
        return val[1:]
    return val or ""


def process_file(filepath):
    filename = os.path.basename(filepath)

    parser = etree.XMLParser(remove_blank_text=True)
    try:
        tree = etree.parse(filepath, parser)
    except etree.XMLSyntaxError as e:
        print(f"  ERROR parsing {filename}: {e}")
        return False

    root = tree.getroot()

    profile_desc = root.find(f".//{T}profileDesc")
    if profile_desc is None:
        return False

    corresp_desc = profile_desc.find(f"{T}correspDesc")
    if corresp_desc is None:
        return False

    # Skip if already has note in correspDesc
    if corresp_desc.find(f"{T}note") is not None:
        return False

    list_person = profile_desc.find(f"{T}listPerson")
    list_place = profile_desc.find(f"{T}listPlace")
    list_bibl = profile_desc.find(f"{T}listBibl")
    list_org = profile_desc.find(f"{T}listOrg")

    if not any(x is not None for x in [list_person, list_place, list_bibl, list_org]):
        return False

    note = etree.Element(f"{T}note")

    if list_person is not None:
        for person in list_person.findall(f"{T}person"):
            target = strip_hash(person.get("ref", ""))
            name_el = person.find(f"{T}persName")
            name = (name_el.text or "") if name_el is not None else ""
            ref = etree.SubElement(note, f"{T}ref")
            ref.set("type", f"{LOD}mentionsPerson")
            ref.set("target", target)
            ref.text = name

    if list_bibl is not None:
        for bibl in list_bibl.findall(f"{T}bibl"):
            target = strip_hash(bibl.get("ref", ""))
            title_el = bibl.find(f"{T}title")
            title = (title_el.text or "") if title_el is not None else ""
            ref = etree.SubElement(note, f"{T}ref")
            ref.set("type", f"{LOD}mentionsBibl")
            ref.set("target", target)
            ref.text = title

    if list_place is not None:
        for place in list_place.findall(f"{T}place"):
            target = strip_hash(place.get("ref", ""))
            name_el = place.find(f"{T}placeName")
            name = (name_el.text or "") if name_el is not None else ""
            ref = etree.SubElement(note, f"{T}ref")
            ref.set("type", f"{LOD}mentionsPlace")
            ref.set("target", target)
            ref.text = name

    if list_org is not None:
        for org in list_org.findall(f"{T}org"):
            target = strip_hash(org.get("ref", ""))
            name_el = org.find(f"{T}orgName")
            name = (name_el.text or "") if name_el is not None else ""
            ref = etree.SubElement(note, f"{T}ref")
            ref.set("type", f"{LOD}mentionsOrg")
            ref.set("target", target)
            ref.text = name

    lang_ref = etree.SubElement(note, f"{T}ref")
    lang_ref.set("type", f"{LOD}hasLanguage")
    lang_ref.set("target", "de")
    lang_ref.text = "German"

    tei_ref = etree.SubElement(note, f"{T}ref")
    tei_ref.set("type", f"{LOD}isAvailableAsTEIfile")
    tei_ref.set("target", f"{BASE_URL}{filename}")

    corresp_desc.append(note)

    for lst in [list_person, list_place, list_bibl, list_org]:
        if lst is not None:
            profile_desc.remove(lst)

    etree.indent(tree, space="  ")
    tree.write(
        filepath,
        pretty_print=True,
        xml_declaration=True,
        encoding="UTF-8",
    )
    return True


if __name__ == "__main__":
    editions_dir = sys.argv[1] if len(sys.argv) > 1 else "data/editions"

    processed = 0
    skipped = 0
    errors = 0

    files = sorted(f for f in os.listdir(editions_dir) if f.endswith(".xml"))
    total = len(files)

    for i, filename in enumerate(files, 1):
        filepath = os.path.join(editions_dir, filename)
        result = process_file(filepath)
        if result:
            processed += 1
            if processed % 100 == 0:
                print(f"  [{i}/{total}] {processed} processed so far...")
        elif result is False:
            skipped += 1

    print(f"\nDone: {processed} transformed, {skipped} skipped (no lists or already done)")
