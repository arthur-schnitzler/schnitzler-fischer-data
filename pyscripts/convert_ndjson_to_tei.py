import json
import os
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom
import re

# Sigle → (Vollname, PMB-ID)
SIGLEN = {
    'AS':  ('Arthur Schnitzler', 'pmb2121'),
    'SF':  ('Samuel Fischer',    'pmb11324'),
    'CS':  ('Carl Schur',        'pmb335601'),
    'EPT': ('Ernst Peter Tal',   'pmb4621'),
    'GB':  ('Gottfried Bermann', 'pmb10889'),
    'HE':  ('Heinrich Eippner',  'pmb335603'),
    'HJ':  ('Hans Jacob',        'pmb4445'),
    'HS':  ('Heinrich Simon',    'pmb3186'),
    'KM':  ('Konrad Maril',      'pmb4374'),
    'LG':  ('Leo Greiner',       'pmb11515'),
    'LS':  ('Lothar Schmidt',    'pmb12681'),
    'MH':  ('Moritz Heimann',    'pmb11632'),
    'NH':  ('Norbert Hoffmann',  'pmb4401'),
    'OB':  ('Oskar Bie',         'pmb2914'),
    'OG':  ('Otto Greiß',        'pmb335606'),
    'OR':  ('Otto Rublack',      'pmb335607'),
    'PE':  ('Paul Eipper',       'pmb6960'),
    'PPS': ('Peter Paul Schmitt','pmb267651'),
    'RK':  ('Rudolf Kayser',     'pmb11891'),
    'RR':  ('Regina Rosenbaum',  'pmb335608'),
}
# Vollname → PMB-Referenz (für XML-Attribute), inkl. "Nachname, Vorname"-Format
NAME_TO_PMB = {name: pmb for name, pmb in SIGLEN.values()}
NAME_TO_PMB['Schnitzler, Arthur'] = 'pmb2121'
NAME_TO_PMB['Fischer, Samuel']    = 'pmb11324'

def set_persname_ref(elem, name):
    """Setzt ref='#pmb...' auf einem persName-Element, falls bekannt."""
    pmb = NAME_TO_PMB.get(name)
    if pmb:
        elem.set('ref', f'#{pmb}')

def expand_abbreviations(text):
    # Ersetze SF mit Samuel Fischer, AS mit Arthur Schnitzler
    text = text.replace('SF', 'Samuel Fischer').replace('AS', 'Arthur Schnitzler')
    # Ersetze /Abk mit / Vollname
    def replace_sigle(match):
        sigle = match.group(1)
        name, _ = SIGLEN.get(sigle, (sigle, None))
        return f" / {name}"
    text = re.sub(r'/(\w+)', replace_sigle, text)
    return text

def expand_type(text):
    abbrevs = {
        'Ms': 'Handschrift',
        'T': 'Typoskript',
        'U': 'Unterschrift',
        'ehKorr': 'eigenhändige Korrektur(en) (ggf. mit Urheber-Monogramm)',
        'ehN': 'eigenhändige Notiz(en) (ggf. mit Urheber-Monogramm)',
        'ehU': 'eigenhändige Unterschrift',
        'U-Stempel': 'Unterschriftsstempel'
    }
    for abbr, full in abbrevs.items():
        text = re.sub(r'\b' + re.escape(abbr) + r'\b', full, text)
    return text

def set_ref_and_source(elem, item):
    if 'id' in item:
        if 'url' in item and item['url'].startswith('https://pmb.acdh.oeaw.ac.at'):
            elem.set('ref', "#pmb" + str(item['id']))
        else:
            elem.set('ref', item['id'])
    if 'url' in item:
        elem.set('source', item['url'])

def is_certain_date(date_str):
    """Überprüft, ob ein Datum sicher ist (keine Fragezeichen/Unsicherheitszeichen)"""
    return date_str and '?' not in date_str

def add_date_element(parent, date_str, n=None):
    """Fügt ein date-Element mit optionalem when- und n-Attribut hinzu"""
    date_elem = SubElement(parent, 'date')
    date_elem.text = date_str
    if is_certain_date(date_str):
        date_elem.set('when', date_str)
    if n is not None:
        date_elem.set('n', n)
    return date_elem

def extract_places_from_letterhead(letterhead):
    """Extrahiert Absende- und Empfangsort aus dem Letterhead-Titel.
    Format: '... SIGLE (Ort) an SIGLE (Ort)'
    Gibt (sent_place, received_place) zurück, jeweils None wenn nicht gefunden."""
    if not letterhead:
        return None, None
    # Empfangsort: letztes (Ort) nach dem Wort 'an'
    rec_match = re.search(r'\ban\s+\S+\s+\(([^)]+)\)', letterhead)
    received_place = rec_match.group(1) if rec_match else None
    # Absende-Ort: erstes (Ort) im String
    sent_match = re.search(r'\(([^)]+)\)', letterhead)
    sent_place = sent_match.group(1) if sent_match else None
    # Wenn nur ein Ort gefunden und er gleich dem Empfangsort ist → kein Absende-Ort
    if sent_place and received_place and sent_place == received_place:
        sent_place = None
    return sent_place, received_place

def add_list_to_element(parent, list_data, element_name, sub_element_name, attr_key='name'):
    if list_data:
        list_elem = SubElement(parent, element_name)
        for item in list_data:
            item_elem = SubElement(list_elem, sub_element_name)
            if isinstance(item, dict):
                item_elem.text = item.get(attr_key, '')
                set_ref_and_source(item_elem, item)
            else:
                item_elem.text = str(item)

def create_tei_xml(entry, n=None):
    # TEI Root
    tei = Element('TEI', {'xmlns': 'http://www.tei-c.org/ns/1.0', 'xml:id': f"sf_{entry['id']}"})
    
    # TEI Header
    teiHeader = SubElement(tei, 'teiHeader')
    fileDesc = SubElement(teiHeader, 'fileDesc')
    
    # Title Statement
    titleStmt = SubElement(fileDesc, 'titleStmt')
    title = SubElement(titleStmt, 'title')
    title.text = expand_abbreviations(entry.get('letterhead', f"Brief von {entry.get('author', 'Unbekannt')} an {entry.get('recipient', 'Unbekannt')}"))
    author_elem = SubElement(titleStmt, 'author')
    author_elem.text = entry.get('author', '')
    set_persname_ref(author_elem, entry.get('author', ''))
    editor = SubElement(titleStmt, 'editor')
    for name in ('Aurnhammer, Achim', 'Martin, Dieter', 'Neubrand, Susanne'):
        name_elem = SubElement(editor, 'name')
        name_elem.text = name

    # Publication Statement
    editionStmt = SubElement(fileDesc, 'editionStmt')
    edition = SubElement(editionStmt, 'edition')
    edition.text = 'schnitzler-fischer'
    respStmt = SubElement(editionStmt, 'respStmt')
    resp = SubElement(respStmt, 'resp')
    resp.text = 'Regestausgabe'

    publicationStmt = SubElement(fileDesc, 'publicationStmt')
    publisher = SubElement(publicationStmt, 'publisher')
    publisher.text = "Arthur Schnitzler Archiv"
    pubPlace = SubElement(publicationStmt, 'pubPlace')
    pubPlace.text = "Freiburg im Breisgau"
    date = SubElement(publicationStmt, 'date')
    date.set('when', '2026')
    date.text = '2026'
    availability = SubElement(publicationStmt, 'availability')
    p_avail = SubElement(availability, 'p')
    p_avail.text = (
        'Die hier präsentierten Daten wurden übernommen von: '
        'Arthur Schnitzler-Archiv Freiburg (Hg.): Arthur Schnitzler und der S. Fischer Verlag. '
        'Briefdatenbank 1888\u20131931. https://biblio.ub.uni-freiburg.de/sf/ '
        '(Zugriff: 2026-03-20). '
        'Alle Rechte bei den ursprünglichen Ersteller_innen der Datenbank.'
    )

    # Source Description
    sourceDesc = SubElement(fileDesc, 'sourceDesc')
    
    # List of Witnesses
    if entry.get('archive_location'):
        listWit = SubElement(sourceDesc, 'listWit')
        archives = [a.strip() for a in entry['archive_location'].split(',')]
        for i, arch in enumerate(archives, 1):
            witness = SubElement(listWit, 'witness', {'n': str(i)})
            msDesc = SubElement(witness, 'msDesc')
            msIdentifier = SubElement(msDesc, 'msIdentifier')
            if ': ' in arch:
                prefix, idno_text = arch.split(': ', 1)
                if prefix == 'ASAF':
                    country = SubElement(msIdentifier, 'country')
                    country.text = 'D'
                    settlement = SubElement(msIdentifier, 'settlement')
                    settlement.text = 'Freiburg im Breisgau'
                    repository = SubElement(msIdentifier, 'repository')
                    repository.text = 'Arthur Schnitzler Archiv'
                    idno = SubElement(msIdentifier, 'idno')
                    idno.text = idno_text
                elif prefix == 'CUL':
                    country = SubElement(msIdentifier, 'country')
                    country.text = 'GB'
                    settlement = SubElement(msIdentifier, 'settlement')
                    settlement.text = 'Cambridge'
                    repository = SubElement(msIdentifier, 'repository')
                    repository.text = 'Cambridge University Library'
                    idno = SubElement(msIdentifier, 'idno')
                    idno.text = idno_text
                else:
                    repository = SubElement(msIdentifier, 'repository')
                    repository.text = arch
            else:
                repository = SubElement(msIdentifier, 'repository')
                repository.text = arch
            
            # Add zotero idno to the first witness
            if i == 1:
                idno_zotero = SubElement(msIdentifier, 'idno', {'type': 'zotero'})
                idno_zotero.text = entry.get('zotero_id', '')
    
    # Notes Statement
    notesStmt = SubElement(fileDesc, 'notesStmt')
    note_desc = SubElement(notesStmt, 'note', {'type': 'description'})
    note_desc.text = entry.get('description', '')
    if entry.get('published_in'):
        note_pub = SubElement(notesStmt, 'note', {'type': 'published_in'})
        note_pub.text = entry['published_in']
    if entry.get('type'):
        note_type = SubElement(notesStmt, 'note', {'type': 'type'})
        note_type.text = expand_type(entry['type'])
    if entry.get('arthur_schnitzler_chronik_entry'):
        note_chronik = SubElement(notesStmt, 'note', {'type': 'chronik_entry'})
        note_chronik.text = entry['arthur_schnitzler_chronik_entry']
    
    # Profile Description
    profileDesc = SubElement(teiHeader, 'profileDesc')
    correspDesc = SubElement(profileDesc, 'correspDesc')
    
    sent_place, received_place = extract_places_from_letterhead(entry.get('letterhead', ''))

    # Sent
    correspAction_sent = SubElement(correspDesc, 'correspAction', {'type': 'sent'})
    persName_sent = SubElement(correspAction_sent, 'persName')
    persName_sent.text = entry.get('author', '')
    set_persname_ref(persName_sent, entry.get('author', ''))
    if entry.get('author_coworker'):
        for coworker in entry['author_coworker']:
            persName_cow = SubElement(correspAction_sent, 'persName', {'type': 'coworker'})
            persName_cow.text = coworker
            set_persname_ref(persName_cow, coworker)
    if sent_place:
        placeName_sent = SubElement(correspAction_sent, 'placeName')
        placeName_sent.text = sent_place
    add_date_element(correspAction_sent, entry.get('date', ''), n=n)

    # Received
    correspAction_rec = SubElement(correspDesc, 'correspAction', {'type': 'received'})
    persName_rec = SubElement(correspAction_rec, 'persName')
    persName_rec.text = entry.get('recipient', '')
    set_persname_ref(persName_rec, entry.get('recipient', ''))
    if entry.get('recipient_coworker'):
        for coworker in entry['recipient_coworker']:
            persName_cow_rec = SubElement(correspAction_rec, 'persName', {'type': 'coworker'})
            persName_cow_rec.text = coworker
            set_persname_ref(persName_cow_rec, coworker)
    if received_place:
        placeName_rec = SubElement(correspAction_rec, 'placeName')
        placeName_rec.text = received_place
    
    # Text Class
    textClass = SubElement(profileDesc, 'textClass')
    if entry.get('subject_areas'):
        keywords = SubElement(textClass, 'keywords')
        for subject in entry['subject_areas']:
            term = SubElement(keywords, 'term')
            term.text = subject
    
    # Lists
    if entry.get('persons'):
        listPerson = SubElement(profileDesc, 'listPerson')
        for person in entry['persons']:
            person_elem = SubElement(listPerson, 'person')
            persName_p = SubElement(person_elem, 'persName')
            if isinstance(person, dict):
                persName_p.text = person.get('name', '')
                if 'id' in person:
                    if 'url' in person and person['url'].startswith('https://pmb.acdh.oeaw.ac.at'):
                        person_elem.set('ref', 'pmb' + str(person['id']))
                    else:
                        person_elem.set('ref', str(person['id']))
                if 'url' in person:
                    person_elem.set('source', person['url'])
            else:
                persName_p.text = str(person)
    
    if entry.get('places'):
        listPlace = SubElement(profileDesc, 'listPlace')
        for place in entry['places']:
            place_elem = SubElement(listPlace, 'place')
            placeName = SubElement(place_elem, 'placeName')
            if isinstance(place, dict):
                placeName.text = place.get('name', '')
                set_ref_and_source(place_elem, place)
            else:
                placeName.text = str(place)
    
    if entry.get('theaters'):
        listOrg = SubElement(profileDesc, 'listOrg', {'type': 'theaters'})
        for theater in entry['theaters']:
            org = SubElement(listOrg, 'org')
            orgName = SubElement(org, 'orgName')
            if isinstance(theater, dict):
                orgName.text = theater.get('name', '')
                set_ref_and_source(org, theater)
            else:
                orgName.text = str(theater)
    
    # Source Desc Lists
    if entry.get('works') or entry.get('journals_periodicals') or entry.get('third_party_works'):
        listBibl = SubElement(sourceDesc, 'listBibl')
        for work in entry.get('works', []):
            bibl = SubElement(listBibl, 'bibl')
            title = SubElement(bibl, 'title')
            if isinstance(work, dict):
                title.text = work.get('name', '')
                set_ref_and_source(bibl, work)
            else:
                title.text = str(work)
        for journal in entry.get('journals_periodicals', []):
            bibl = SubElement(listBibl, 'bibl', {'type': 'journal'})
            title = SubElement(bibl, 'title')
            if isinstance(journal, dict):
                title.text = journal.get('name', '')
                set_ref_and_source(bibl, journal)
            else:
                title.text = str(journal)
        for third in entry.get('third_party_works', []):
            bibl = SubElement(listBibl, 'bibl', {'type': 'third_party'})
            title = SubElement(bibl, 'title')
            if isinstance(third, dict):
                title.text = third.get('name', '')
                set_ref_and_source(bibl, third)
            else:
                title.text = str(third)
    
    if entry.get('publishers') or entry.get('other_institutions'):
        listOrg_source = SubElement(sourceDesc, 'listOrg')
        for pub in entry.get('publishers', []):
            org = SubElement(listOrg_source, 'org', {'type': 'publisher'})
            orgName = SubElement(org, 'orgName')
            if isinstance(pub, dict):
                orgName.text = pub.get('name', '')
                set_ref_and_source(org, pub)
            else:
                orgName.text = str(pub)
        for inst in entry.get('other_institutions', []):
            org = SubElement(listOrg_source, 'org', {'type': 'institution'})
            orgName = SubElement(org, 'orgName')
            orgName.text = str(inst)
    
    # Journal Entry
    if entry.get('journal_entry'):
        ref = SubElement(sourceDesc, 'ref', {'type': 'journal_entry'})
        ref.text = entry['journal_entry'].get('title', '')
        if 'url' in entry['journal_entry']:
            ref.set('target', entry['journal_entry']['url'])
    
    # Facsimile (between teiHeader and text)
    if entry.get('digitized_version'):
        facsimile = SubElement(tei, 'facsimile')
        graphic = SubElement(facsimile, 'graphic')
        graphic.set('url', entry['digitized_version'])
    
    # Text Body
    text = SubElement(tei, 'text')
    body = SubElement(text, 'body')
    div = SubElement(body, 'div', {'type': 'letter'})
    p = SubElement(div, 'p')
    p.text = entry.get('description', 'Inhalt nicht verfügbar.')
    
    # Pretty print XML
    rough_string = tostring(tei, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ")

def main():
    _base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    input_file = os.path.join(_base, 'website-download', 'export.ndjson')
    output_dir = os.path.join(_base, 'data', 'editions')
    os.makedirs(output_dir, exist_ok=True)

    # Erster Pass: alle Einträge laden und n-Wert pro Datum bestimmen
    from collections import defaultdict
    entries = []
    date_counters = defaultdict(int)
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            entry = json.loads(line.strip())
            entries.append(entry)
            date_counters[entry.get('date', '')] += 1

    # Für Daten mit mehreren Briefen: laufende Nummern vergeben
    date_seen = defaultdict(int)
    id_to_n = {}
    for entry in entries:
        date = entry.get('date', '')
        date_seen[date] += 1
        id_to_n[entry['id']] = f"{date_seen[date]:02d}"

    # Zweiter Pass: XML erzeugen
    for entry in entries:
        n = id_to_n[entry['id']]
        xml_content = create_tei_xml(entry, n=n)
        filename = f"sf_{entry['id']}.xml"
        with open(os.path.join(output_dir, filename), 'w', encoding='utf-8') as out_f:
            out_f.write(xml_content)
        print(f"Erstellt: {filename}")

if __name__ == "__main__":
    main()