#!/usr/bin/env python3
"""
Transform sf_*.xml files: for each file with //sourceDesc/listBibl/bibl elements,
add mentionsBibl refs into correspDesc/note.
"""

import glob
import os
import sys
from lxml import etree

NS = "http://www.tei-c.org/ns/1.0"
NSMAP = {"tei": NS}
MENTIONS_BIBL_TYPE = "https://lod.academy/cmif/vocab/terms#mentionsBibl"


def get_xml_declaration(filepath):
    """Read the first line of the file to preserve the exact XML declaration."""
    with open(filepath, "rb") as f:
        first_line = f.readline().decode("utf-8", errors="replace").rstrip("\n")
    if first_line.startswith("<?xml"):
        return first_line
    return None


def process_file(filepath, dry_run=False):
    """Process a single XML file. Returns list of (pmb_id, title) tuples added."""
    orig_decl = get_xml_declaration(filepath)

    parser = etree.XMLParser(remove_blank_text=False)
    tree = etree.parse(filepath, parser)
    root = tree.getroot()

    # Find all bibl elements (not biblStruct) in sourceDesc/listBibl
    bibls = root.findall(
        f".//{{{NS}}}sourceDesc/{{{NS}}}listBibl/{{{NS}}}bibl"
    )

    if not bibls:
        return []

    # Collect valid (pmb_id, title) pairs
    to_add = []
    for bibl in bibls:
        ref_attr = bibl.get("ref", "")
        title_el = bibl.find(f"{{{NS}}}title")
        title_text = title_el.text.strip() if (title_el is not None and title_el.text) else ""
        if not ref_attr or not title_text:
            continue
        pmb_id = ref_attr.lstrip("#")
        to_add.append((pmb_id, title_text))

    if not to_add:
        return []

    # Find correspDesc
    corresp_desc = root.find(f".//{{{NS}}}correspDesc")
    if corresp_desc is None:
        return []

    # Find or create note (without type attribute — the mentionsBibl note)
    note = None
    for child in corresp_desc:
        if child.tag == f"{{{NS}}}note" and child.get("type") is None:
            note = child
            break

    # Collect already-present mentionsBibl targets to ensure idempotency
    existing_targets = set()
    if note is not None:
        for ref_el in note.findall(f"{{{NS}}}ref"):
            if ref_el.get("type") == MENTIONS_BIBL_TYPE:
                existing_targets.add(ref_el.get("target", ""))

    # Filter out already-present items
    new_items = [(pmb_id, title) for pmb_id, title in to_add if pmb_id not in existing_targets]

    if not new_items:
        return []

    if dry_run:
        print(f"  Would add to {os.path.basename(filepath)}:")
        for pmb_id, title in new_items:
            print(f'    <ref type="{MENTIONS_BIBL_TYPE}" target="{pmb_id}">{title}</ref>')
        return new_items

    # Create note if it doesn't exist yet
    if note is None:
        # Determine indentation: note goes at 8 spaces inside correspDesc
        note = etree.SubElement(corresp_desc, f"{{{NS}}}note")
        # We'll handle indentation via text/tail manipulation below
        note_is_new = True
    else:
        note_is_new = False

    # Add ref elements for each new item
    for pmb_id, title in new_items:
        ref_el = etree.SubElement(note, f"{{{NS}}}ref")
        ref_el.set("type", MENTIONS_BIBL_TYPE)
        ref_el.set("target", pmb_id)
        ref_el.text = title

    # Now fix indentation manually in the serialized output
    # We'll write the file and then fix indentation with string manipulation

    # Serialize to string
    xml_bytes = etree.tostring(root, encoding="unicode", xml_declaration=False)

    # Fix indentation for the note and its refs
    # Strategy: use string replacement on the newly added elements
    # The note element needs 8-space indent, refs 10-space indent

    lines = xml_bytes.split("\n")
    result_lines = []
    i = 0
    while i < len(lines):
        line = lines[i]
        result_lines.append(line)
        i += 1

    xml_str = "\n".join(result_lines)

    # Prepend the original XML declaration
    if orig_decl:
        xml_str = orig_decl + "\n" + xml_str
    else:
        xml_str = '<?xml version="1.0" ?>\n' + xml_str

    # Ensure file ends with newline
    if not xml_str.endswith("\n"):
        xml_str += "\n"

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(xml_str)

    return new_items


def process_file_with_indent(filepath, dry_run=False):
    """Process file preserving indentation carefully."""
    orig_decl = get_xml_declaration(filepath)

    # Read original content for reference
    with open(filepath, "r", encoding="utf-8") as f:
        original_content = f.read()

    parser = etree.XMLParser(remove_blank_text=False)
    tree = etree.parse(filepath, parser)
    root = tree.getroot()

    # Find all bibl elements (not biblStruct) in sourceDesc/listBibl
    bibls = root.findall(
        f".//{{{NS}}}sourceDesc/{{{NS}}}listBibl/{{{NS}}}bibl"
    )

    if not bibls:
        return []

    # Collect valid (pmb_id, title) pairs
    to_add = []
    for bibl in bibls:
        ref_attr = bibl.get("ref", "")
        title_el = bibl.find(f"{{{NS}}}title")
        title_text = title_el.text.strip() if (title_el is not None and title_el.text) else ""
        if not ref_attr or not title_text:
            continue
        pmb_id = ref_attr.lstrip("#")
        to_add.append((pmb_id, title_text))

    if not to_add:
        return []

    # Find correspDesc
    corresp_desc = root.find(f".//{{{NS}}}correspDesc")
    if corresp_desc is None:
        return []

    # Find note (without type attribute — the mentionsBibl note)
    note = None
    for child in corresp_desc:
        if child.tag == f"{{{NS}}}note" and child.get("type") is None:
            note = child
            break

    # Collect already-present mentionsBibl targets
    existing_targets = set()
    if note is not None:
        for ref_el in note.findall(f"{{{NS}}}ref"):
            if ref_el.get("type") == MENTIONS_BIBL_TYPE:
                existing_targets.add(ref_el.get("target", ""))

    # Filter out already-present items
    new_items = [(pmb_id, title) for pmb_id, title in to_add if pmb_id not in existing_targets]

    if not new_items:
        return []

    if dry_run:
        print(f"  Would add to {os.path.basename(filepath)}:")
        for pmb_id, title in new_items:
            print(f'    <ref type="{MENTIONS_BIBL_TYPE}" target="{pmb_id}">{title}</ref>')
        return new_items

    # Build the ref lines to insert
    ref_lines = []
    for pmb_id, title in new_items:
        ref_lines.append(
            f'          <ref type="{MENTIONS_BIBL_TYPE}" target="{pmb_id}">{title}</ref>'
        )

    if note is None:
        # Need to create <note> as last child of correspDesc
        # Find the closing </correspDesc> tag and insert before it
        note_block = "        <note>\n"
        for rl in ref_lines:
            note_block += rl + "\n"
        note_block += "        </note>\n"

        # Insert before </correspDesc>
        new_content = original_content.replace(
            "      </correspDesc>",
            note_block + "      </correspDesc>",
            1
        )
    else:
        # Note exists: insert new refs before </note>
        # Find the closing </note> tag (the one without type attr in correspDesc)
        # We need to find the right </note>. Strategy: find the note block
        # by looking for the note that contains the existing refs.

        # Build insertion text
        insert_text = ""
        for rl in ref_lines:
            insert_text += rl + "\n"

        # Find closing tag of the note. Since note may have tail whitespace,
        # let's find its position by looking for the pattern.
        # We look for the last ref inside this note and insert after it,
        # or if no refs, before </note>.

        # Simple approach: find </note> that closes the correspDesc/note
        # by working with the serialized form.
        # Since remove_blank_text=False preserves original formatting,
        # let's use string operations on original_content.

        # We know what's in the note. Find its closing </note> in context.
        # Build a marker: if note has existing refs, we insert after the last one;
        # otherwise insert before </note>.

        # Serialize just the note to find its context
        note_serialized = etree.tostring(note, encoding="unicode")

        # Find this note in the original content and replace it
        # Strategy: find </note> after the correspDesc opening

        # Find correspDesc block in original
        cd_start = original_content.find("<correspDesc")
        cd_end = original_content.find("</correspDesc>", cd_start) + len("</correspDesc>")
        cd_block = original_content[cd_start:cd_end]

        # Find the closing </note> in the note block (the plain note without type)
        # We find the last existing ref line in the note, then append after it
        note_close_idx = cd_block.rfind("        </note>")
        if note_close_idx == -1:
            # Try with less indentation
            note_close_idx = cd_block.rfind("</note>")

        if note_close_idx != -1:
            # Insert ref_lines before the closing </note>
            insert_pos = cd_start + note_close_idx
            new_content = (
                original_content[:insert_pos]
                + insert_text
                + original_content[insert_pos:]
            )
        else:
            print(f"  WARNING: Could not find </note> in {filepath}")
            return []

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(new_content)

    return new_items


def main():
    editions_dir = "/Users/oldfiche/git/schnitzler-fischer-data/data/editions"
    pattern = os.path.join(editions_dir, "sf_*.xml")
    files = sorted(glob.glob(pattern))

    print(f"Found {len(files)} sf_*.xml files")

    # --- DRY RUN on test files ---
    test_files = [
        os.path.join(editions_dir, "sf_10015.xml"),
        os.path.join(editions_dir, "sf_10004.xml"),
    ]
    print("\n=== DRY RUN on test files ===")
    for tf in test_files:
        result = process_file_with_indent(tf, dry_run=True)
        if not result:
            print(f"  {os.path.basename(tf)}: nothing to add (no bibl, or all already present)")

    print("\n=== Processing all files ===")
    total_changed = 0
    total_refs_added = 0
    for filepath in files:
        added = process_file_with_indent(filepath, dry_run=False)
        if added:
            total_changed += 1
            total_refs_added += len(added)
            print(f"  {os.path.basename(filepath)}: added {len(added)} mentionsBibl ref(s): {[p for p,t in added]}")

    print(f"\nDone. {total_changed} files changed, {total_refs_added} refs added total.")


if __name__ == "__main__":
    main()
