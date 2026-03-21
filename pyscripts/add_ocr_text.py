"""
Fügt in alle TEI-Dateien in editions/ ein <div type="OCR"> ein,
wenn eine <graphic url="https://schnitzler-mikrofilme.acdh.oeaw.ac.at/..."> vorhanden ist.
Der OCR-Text wird von GitHub geholt.
"""

import os
import re
import urllib.request
import xml.etree.ElementTree as ET

EDITIONS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'editions')
GITHUB_BASE = (
    'https://raw.githubusercontent.com/arthur-schnitzler/'
    'schnitzler-mikrofilme-static/refs/heads/main/exports'
)
PAGE_NS = 'http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15'
TEI_NS = 'http://www.tei-c.org/ns/1.0'

GRAPHIC_RE = re.compile(
    r'<graphic url="(https://schnitzler-mikrofilme\.acdh\.oeaw\.ac\.at/([^"]+)\.html)"'
)


def fetch_ocr_lines(page_id: str) -> list[str] | None:
    """Holt die OCR-Zeilen für eine Seiten-ID wie '1416744_0169'."""
    prefix = page_id.split('_')[0]
    url = f'{GITHUB_BASE}/{prefix}/page/{page_id}.xml'
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            data = resp.read()
    except Exception as e:
        print(f"  FEHLER beim Abrufen von {url}: {e}")
        return None

    root = ET.fromstring(data)
    ns = {'p': PAGE_NS}
    lines = []
    for unicode_elem in root.findall('.//p:TextLine/p:TextEquiv/p:Unicode', ns):
        text = (unicode_elem.text or '').strip()
        if text:
            lines.append(text)
    return lines if lines else None


def process_file(filepath: str) -> bool:
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # Schon vorhanden?
    if 'type="OCR"' in content:
        return False

    match = GRAPHIC_RE.search(content)
    if not match:
        return False

    page_id = match.group(2)  # z.B. '1416744_0169'
    print(f"  Seite: {page_id}")

    ocr_lines = fetch_ocr_lines(page_id)
    if not ocr_lines:
        print("  Kein OCR-Text gefunden.")
        return False

    def esc(t):
        return t.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

    inner = '<lb/>'.join(esc(line) for line in ocr_lines)
    ocr_div = f'      <div type="OCR"><p>{inner}</p></div>\n    '
    new_content = content.replace('</body>', ocr_div + '</body>', 1)

    if new_content == content:
        print("  </body> nicht gefunden, übersprungen.")
        return False

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(new_content)
    return True


def main():
    files = sorted(
        f for f in os.listdir(EDITIONS_DIR) if f.endswith('.xml')
    )
    changed = 0
    for filename in files:
        filepath = os.path.join(EDITIONS_DIR, filename)
        print(f"{filename}")
        if process_file(filepath):
            print(f"  → OCR eingefügt")
            changed += 1
        else:
            print(f"  → übersprungen")

    print(f"\n{changed} von {len(files)} Dateien geändert.")


if __name__ == '__main__':
    main()
