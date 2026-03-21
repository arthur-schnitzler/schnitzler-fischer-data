"""
Fügt in alle TEI-Dateien in editions/ ein <div type="OCR"> ein,
wenn eine <graphic url="https://schnitzler-mikrofilme.acdh.oeaw.ac.at/..."> vorhanden ist.
Die Seitenanzahl wird aus note[@type='description'] gelesen.
Jede Seite wird in ein eigenes <p> mit <pb facs="..."/> geschrieben.
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

GRAPHIC_RE = re.compile(
    r'<graphic url="https://schnitzler-mikrofilme\.acdh\.oeaw\.ac\.at/([^"]+)\.html"'
)
# Matches "2 Seiten", "1 Seite", "3 Seiten beschrieben" etc.
PAGE_COUNT_RE = re.compile(r'(\d+)\s+Seite')
# Bestehenden OCR-Block inkl. umliegender Leerzeichen/Newlines entfernen
OCR_DIV_RE = re.compile(r'\s*<div type="OCR">.*?</div>', re.DOTALL)


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


def make_page_ids(first_page_id: str, count: int) -> list[str]:
    """Erzeugt fortlaufende Seiten-IDs ausgehend von der ersten ID.

    Z.B. ('1416741_0173', 3) → ['1416741_0173', '1416741_0174', '1416741_0175']
    """
    prefix, num_str = first_page_id.rsplit('_', 1)
    width = len(num_str)
    start = int(num_str)
    return [f"{prefix}_{str(start + i).zfill(width)}" for i in range(count)]


def process_file(filepath: str) -> bool:
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    match = GRAPHIC_RE.search(content)
    if not match:
        return False

    # Bestehenden OCR-Block entfernen (wird neu generiert)
    content = OCR_DIV_RE.sub('', content)

    first_page_id = match.group(1)  # z.B. '1416744_0169'

    # Seitenanzahl aus note[@type='description'] lesen
    count_match = PAGE_COUNT_RE.search(content)
    page_count = int(count_match.group(1)) if count_match else 1

    page_ids = make_page_ids(first_page_id, page_count)
    print(f"  Seiten ({page_count}): {', '.join(page_ids)}")

    def esc(t):
        return t.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

    pages_xml = ''
    any_ocr = False
    for page_id in page_ids:
        ocr_lines = fetch_ocr_lines(page_id)
        if ocr_lines:
            inner = '<lb/>\n'.join(esc(line) for line in ocr_lines)
            pages_xml += f'\n        <p><pb facs="{page_id}"/>{inner}</p>'
            any_ocr = True
        else:
            print(f"  Kein OCR-Text für {page_id}.")
            pages_xml += f'\n        <p><pb facs="{page_id}"/></p>'

    if not any_ocr:
        print("  Kein OCR-Text gefunden.")
        return False

    ocr_div = f'      <div type="OCR">{pages_xml}\n      </div>\n    '
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
