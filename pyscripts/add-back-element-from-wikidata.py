#!/usr/bin/env python3
"""
Python implementation for creating back elements with Wikidata references:
- Creates back element with empty placeholders
- Populates placeholders with Wikidata data
- Adds persons from bibliography authors
- Normalizes and cleans up the data
"""

import sys
import time
import json
from lxml import etree as ET
from pathlib import Path
from typing import Set, Dict, Optional
import requests


class WikidataProcessor:
    # Configuration: XPath patterns for extracting entity references
    # Format: entity_type -> list of (xpath_pattern, attribute_name, description)
    ENTITY_EXTRACTION_CONFIG = {
        "person": [
            (".//tei:persName[@ref]", "ref", "persName with @ref"),
            (".//tei:persName[@key]", "key", "persName with @key"),
            (".//tei:rs[@type='person'][@ref]", "ref", "rs[@type='person'] with @ref"),
            (".//tei:rs[@type='person'][@key]", "key", "rs[@type='person'] with @key"),
            (".//tei:author[@ref]", "ref", "author with @ref"),
            (".//tei:author[@key]", "key", "author with @key"),
            (".//tei:item[@ana='person']", "corresp", "item with @corresp"),
            (".//tei:handShift[@scribe]", "scribe", "handShift with @scribe"),
            (".//tei:handNote[@corresp]", "corresp", "handNote with @corresp (excluding 'schreibkraft')"),
        ],
        "bibl": [
            (".//tei:rs[@type='work'][@ref]", "ref", "rs[@type='work'] with @ref"),
            (".//tei:rs[@type='work'][@key]", "key", "rs[@type='work'] with @key"),
            (".//tei:biblStruct//tei:title[@ref]", "ref", "title in biblStruct with @ref"),
            (".//tei:teiHeader//tei:title[@ref]", "ref", "title in teiHeader with @ref"),
            (".//tei:item[@ana='work']", "corresp", "item with @corresp"),

        ],
        "place": [
            (".//tei:placeName[@ref]", "ref", "placeName with @ref"),
            (".//tei:placeName[@key]", "key", "placeName with @key"),
            (".//tei:rs[@type='place'][@ref]", "ref", "rs[@type='place'] with @ref"),
            (".//tei:rs[@type='place'][@key]", "key", "rs[@type='place'] with @key"),
        ],
        "org": [
            (".//tei:orgName[@ref]", "ref", "orgName with @ref"),
            (".//tei:orgName[@key]", "key", "orgName with @key"),
            (".//tei:rs[@type='org'][@ref]", "ref", "rs[@type='org'] with @ref"),
            (".//tei:rs[@type='org'][@key]", "key", "rs[@type='org'] with @key"),
        ],
        "event": [
            (".//tei:eventName[@ref]", "ref", "eventName with @ref"),
            (".//tei:eventName[@key]", "key", "eventName with @key"),
            (".//tei:rs[@type='event'][@ref]", "ref", "rs[@type='event'] with @ref"),
            (".//tei:rs[@type='event'][@key]", "key", "rs[@type='event'] with @key"),
        ]
    }

    def __init__(self, wikidata_lists_dir: str = "python-temp"):
        print(f"🔧 Initializing WikidataProcessor...")
        sys.stdout.flush()

        self.wikidata_lists_dir = Path(wikidata_lists_dir)
        self.tei_ns = "http://www.tei-c.org/ns/1.0"
        self.xml_ns = "http://www.w3.org/XML/1998/namespace"
        self.ns = {"tei": self.tei_ns, "xml": self.xml_ns}

        # Statistics for debugging
        self.stats = {
            "wikidata_lookups": 0,
            "wikidata_found": 0,
            "wikidata_not_found": 0,
            "api_calls": 0,
            "api_success": 0,
            "api_failures": 0
        }

        print(f"📥 Initializing Wikidata system...")
        sys.stdout.flush()

        # Cache for recently accessed entities
        self.wikidata_cache = {}
        self.api_session = requests.Session()
        self.max_cache_size = 100000
        self._load_cache()

        print(f"✅ WikidataProcessor initialized")
        print(f"📋 Entity extraction configuration:")
        for entity_type, patterns in self.ENTITY_EXTRACTION_CONFIG.items():
            print(f"   {entity_type}: {len(patterns)} patterns")
            for _, _, desc in patterns:
                print(f"      - {desc}")
        sys.stdout.flush()


    def _fetch_wikidata_entity(self, wikidata_id: str) -> Optional[ET.Element]:
        """Fetch entity data from Wikidata API with retry and backoff"""
        # Check cache first
        if wikidata_id in self.wikidata_cache:
            print(f"📋 Cache hit for {wikidata_id}")
            sys.stdout.flush()
            return self.wikidata_cache[wikidata_id]

        print(f"🌐 Fetching {wikidata_id} from Wikidata...")
        sys.stdout.flush()

        url = f"https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json"
        headers = {
            'User-Agent': 'Chezy-Brockhaus TEI Processor/1.0 (https://github.com/selmajahnke/Chezy-Brockhaus)'
        }

        max_retries = 5
        backoff = 5  # seconds

        for attempt in range(max_retries):
            try:
                response = self.api_session.get(url, headers=headers, timeout=10)

                if response.status_code == 200:
                    data = response.json()
                    entity_data = data.get("entities", {}).get(wikidata_id, {})

                    if entity_data:
                        print(f"✅ Successfully fetched {wikidata_id}")
                        sys.stdout.flush()
                        self._add_to_cache(wikidata_id, entity_data)
                        time.sleep(0.5)  # polite delay after each request
                        return entity_data

                elif response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', backoff))
                    print(f"⏳ Rate limited (429) for {wikidata_id}. Waiting {retry_after}s (attempt {attempt + 1}/{max_retries})...")
                    sys.stdout.flush()
                    time.sleep(retry_after)
                    backoff = min(backoff * 2, 60)
                    continue

                else:
                    print(f"❌ Failed to fetch {wikidata_id}: HTTP {response.status_code}")
                    sys.stdout.flush()
                    return None

            except Exception as e:
                print(f"❌ Error fetching {wikidata_id}: {e}")
                sys.stdout.flush()
                if attempt < max_retries - 1:
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 60)
                else:
                    return None

        print(f"❌ Failed to fetch {wikidata_id} after {max_retries} attempts")
        sys.stdout.flush()
        return None

    def _fetch_wikidata_entities_batch(self, wikidata_ids: list) -> dict:
        """Fetch multiple Wikidata entities in one API call (max 50 per batch)"""
        if not wikidata_ids:
            return {}

        ids_str = "|".join(wikidata_ids)
        url = "https://www.wikidata.org/w/api.php"
        params = {
            'action': 'wbgetentities',
            'ids': ids_str,
            'format': 'json',
            'languages': 'de|en',
        }
        headers = {
            'User-Agent': 'Chezy-Brockhaus TEI Processor/1.0 (https://github.com/selmajahnke/Chezy-Brockhaus)'
        }

        max_retries = 5
        backoff = 5

        for attempt in range(max_retries):
            try:
                preview = ', '.join(wikidata_ids[:5]) + ('...' if len(wikidata_ids) > 5 else '')
                print(f"🌐 Batch fetching {len(wikidata_ids)} entities: {preview}")
                sys.stdout.flush()

                response = self.api_session.get(url, params=params, headers=headers, timeout=30)

                if response.status_code == 200:
                    data = response.json()
                    entities = data.get('entities', {})
                    result = {}
                    for wid, entity_data in entities.items():
                        if entity_data and 'missing' not in entity_data:
                            self._add_to_cache(wid, entity_data)
                            result[wid] = entity_data
                    print(f"✅ Batch fetched {len(result)}/{len(wikidata_ids)} entities")
                    sys.stdout.flush()
                    time.sleep(1)  # polite delay after each batch request
                    return result

                elif response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', backoff))
                    print(f"⏳ Rate limited (429) on batch. Waiting {retry_after}s (attempt {attempt + 1}/{max_retries})...")
                    sys.stdout.flush()
                    time.sleep(retry_after)
                    backoff = min(backoff * 2, 60)
                    continue

                else:
                    print(f"❌ Batch fetch failed: HTTP {response.status_code}")
                    sys.stdout.flush()
                    return {}

            except Exception as e:
                print(f"❌ Error in batch fetch: {e}")
                sys.stdout.flush()
                if attempt < max_retries - 1:
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 60)
                else:
                    return {}

        print(f"❌ Batch fetch failed after {max_retries} attempts")
        sys.stdout.flush()
        return {}

    def _add_to_cache(self, wikidata_id: str, entity_data: dict) -> None:
        """Add entity to cache with size management"""
        if len(self.wikidata_cache) >= self.max_cache_size:
            oldest_key = next(iter(self.wikidata_cache))
            del self.wikidata_cache[oldest_key]
        self.wikidata_cache[wikidata_id] = entity_data

    def _load_cache(self) -> None:
        """Load persistent Wikidata cache from disk"""
        cache_file = self.wikidata_lists_dir / "wikidata_cache.json"
        if cache_file.exists():
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    self.wikidata_cache = json.load(f)
                print(
                    f"📦 Loaded {len(self.wikidata_cache)} entries from cache"
                )
            except Exception as e:
                print(f"⚠️ Could not load cache: {e}")
                self.wikidata_cache = {}
        else:
            print("📦 No cache file found, starting fresh")
        sys.stdout.flush()

    def _save_cache(self) -> None:
        """Save persistent Wikidata cache to disk"""
        cache_file = self.wikidata_lists_dir / "wikidata_cache.json"
        try:
            self.wikidata_lists_dir.mkdir(parents=True, exist_ok=True)
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.wikidata_cache, f, ensure_ascii=False)
            print(
                f"💾 Saved {len(self.wikidata_cache)} entries to cache"
            )
        except Exception as e:
            print(f"⚠️ Could not save cache: {e}")
        sys.stdout.flush()

    def _clone_element(self, elem: ET.Element) -> ET.Element:
        """Create a deep clone of an element with all attributes, text, and children"""
        # Create new element with same tag
        new_elem = ET.Element(elem.tag, elem.attrib)
        new_elem.text = elem.text
        new_elem.tail = elem.tail
        
        # Recursively clone children
        for child in elem:
            new_elem.append(self._clone_element(child))
        
        return new_elem


    def _is_in_back_section(self, elem: ET.Element, root: ET.Element) -> bool:
        """Check if element is in the back section by checking path"""
        # Simple approach: convert element to string and check if it's in back section
        back_elements = root.findall(".//tei:back", self.ns)
        for back in back_elements:
            for descendant in back.iter():
                if descendant is elem:
                    return True
        return False

    def _extract_refs(self, root: ET.Element) -> Dict[str, Dict[str, Set[str]]]:
        """Extract all references from the TEI document using configuration (Step 1)
        Returns: Dict with entity types, each containing 'in_text', 'in_commentary', and 'implied' sets
        """
        refs = {
            "person": {"in_text": set(), "in_commentary": set(), "implied": set()},
            "bibl": {"in_text": set(), "in_commentary": set(), "implied": set()},
            "place": {"in_text": set(), "in_commentary": set(), "implied": set()},
            "org": {"in_text": set(), "in_commentary": set(), "implied": set()},
            "event": {"in_text": set(), "in_commentary": set(), "implied": set()}
        }

        # Check if document uses '#' format
        has_hash = any("#" in ref for ref in self._get_all_refs(root))

        # Helper function to check if element is in commentary
        def is_in_commentary(elem):
            """Check if element is inside a note[@type='commentary']"""
            current = elem
            while current is not None:
                if current.tag == f"{{{self.tei_ns}}}note" and current.get("type") == "commentary":
                    return True
                current = current.getparent() if hasattr(current, 'getparent') else None
            return False

        # Helper function to clean and split references
        def extract_refs_from_attribute(ref_value, has_hash):
            """Extract individual references from an attribute value"""
            if not ref_value:
                return []
            if has_hash:
                return [token.replace("#", "").strip() for token in ref_value.split("#") if token.strip()]
            else:
                return [token.strip() for token in ref_value.split() if token.strip()]

        # Process each entity type using the configuration
        for entity_type, patterns in self.ENTITY_EXTRACTION_CONFIG.items():
            for xpath, attr_name, description in patterns:
                # Find all matching elements
                elements = root.findall(xpath, self.ns)

                for elem in elements:
                    # Skip elements in back section
                    if self._is_in_back_section(elem, root):
                        continue

                    # Get the reference attribute
                    ref_value = elem.get(attr_name)

                    # Special handling for handNote: exclude 'schreibkraft'
                    if "handNote" in description and ref_value == "schreibkraft":
                        continue

                    if not ref_value:
                        continue

                    # Determine which set to add to
                    in_commentary = is_in_commentary(elem)
                    is_implied = elem.get("subtype") == "implied"

                    # Special handling: handShift and handNote are always in text
                    if "handShift" in description or "handNote" in description:
                        ref_set = refs[entity_type]["in_text"]
                    # Special handling: title in biblStruct or teiHeader is always in text
                    elif "biblStruct" in description or "teiHeader" in description:
                        ref_set = refs[entity_type]["in_text"]
                    elif in_commentary:
                        ref_set = refs[entity_type]["in_commentary"]
                    elif is_implied:
                        ref_set = refs[entity_type]["implied"]
                    else:
                        ref_set = refs[entity_type]["in_text"]

                    # Extract and add references
                    extracted_refs = extract_refs_from_attribute(ref_value, has_hash)
                    ref_set.update(extracted_refs)

        return refs

    def _get_all_refs(self, root: ET.Element) -> list:
        """Helper to get all ref attributes for hash detection"""
        refs = []
        for elem in root.findall(".//*[@ref]", self.ns):
            refs.append(elem.get("ref"))
        for elem in root.findall(".//*[@key]", self.ns):
            refs.append(elem.get("key"))
        return refs

    def _create_back_element(self, root: ET.Element, refs: Dict[str, Dict[str, Set[str]]]) -> ET.Element:
        """Create back element with placeholder lists (Step 1)"""
        # Find or create text element
        text_elem = root.find(".//tei:text", self.ns)
        if text_elem is None:
            return root

        # Remove existing back element
        existing_back = text_elem.find("tei:back", self.ns)
        if existing_back is not None:
            text_elem.remove(existing_back)

        # Create new back element
        back_elem = ET.SubElement(text_elem, f"{{{self.tei_ns}}}back")

        # Create list elements with empty placeholders
        for list_type, ref_dict in refs.items():
            in_text_refs = ref_dict.get("in_text", set())
            in_commentary_refs = ref_dict.get("in_commentary", set())
            implied_refs = ref_dict.get("implied", set())
            all_refs = in_text_refs | in_commentary_refs | implied_refs

            if all_refs:  # Only create if there are references
                if list_type == "person":
                    list_elem = ET.SubElement(back_elem, f"{{{self.tei_ns}}}listPerson")
                    # First add refs from text (no ana attribute)
                    for ref_id in sorted(in_text_refs):
                        if ref_id.strip():
                            person_elem = ET.SubElement(list_elem, f"{{{self.tei_ns}}}person")
                            person_elem.set("{http://www.w3.org/XML/1998/namespace}id", ref_id.replace("#", ""))
                    # Then add refs only in commentary (with ana="comment")
                    for ref_id in sorted(in_commentary_refs - in_text_refs):
                        if ref_id.strip():
                            person_elem = ET.SubElement(list_elem, f"{{{self.tei_ns}}}person")
                            person_elem.set("{http://www.w3.org/XML/1998/namespace}id", ref_id.replace("#", ""))
                            person_elem.set("ana", "comment")
                    # Then add refs only implied (with ana="implied", but not if also in in_text)
                    for ref_id in sorted(implied_refs - in_text_refs - in_commentary_refs):
                        if ref_id.strip():
                            person_elem = ET.SubElement(list_elem, f"{{{self.tei_ns}}}person")
                            person_elem.set("{http://www.w3.org/XML/1998/namespace}id", ref_id.replace("#", ""))
                            person_elem.set("ana", "implied")

                elif list_type == "bibl":
                    list_elem = ET.SubElement(back_elem, f"{{{self.tei_ns}}}listBibl")
                    # First add refs from text (no ana attribute)
                    for ref_id in sorted(in_text_refs):
                        if ref_id.strip():
                            bibl_elem = ET.SubElement(list_elem, f"{{{self.tei_ns}}}bibl")
                            bibl_elem.set("{http://www.w3.org/XML/1998/namespace}id", ref_id.replace("#", ""))
                    # Then add refs only in commentary (with ana="comment")
                    for ref_id in sorted(in_commentary_refs - in_text_refs):
                        if ref_id.strip():
                            bibl_elem = ET.SubElement(list_elem, f"{{{self.tei_ns}}}bibl")
                            bibl_elem.set("{http://www.w3.org/XML/1998/namespace}id", ref_id.replace("#", ""))
                            bibl_elem.set("ana", "comment")
                    # Then add refs only implied (with ana="implied", but not if also in in_text)
                    for ref_id in sorted(implied_refs - in_text_refs - in_commentary_refs):
                        if ref_id.strip():
                            bibl_elem = ET.SubElement(list_elem, f"{{{self.tei_ns}}}bibl")
                            bibl_elem.set("{http://www.w3.org/XML/1998/namespace}id", ref_id.replace("#", ""))
                            bibl_elem.set("ana", "implied")

                elif list_type == "place":
                    list_elem = ET.SubElement(back_elem, f"{{{self.tei_ns}}}listPlace")
                    # First add refs from text (no ana attribute)
                    for ref_id in sorted(in_text_refs):
                        if ref_id.strip():
                            place_elem = ET.SubElement(list_elem, f"{{{self.tei_ns}}}place")
                            place_elem.set("{http://www.w3.org/XML/1998/namespace}id", ref_id.replace("#", ""))
                    # Then add refs only in commentary (with ana="comment")
                    for ref_id in sorted(in_commentary_refs - in_text_refs):
                        if ref_id.strip():
                            place_elem = ET.SubElement(list_elem, f"{{{self.tei_ns}}}place")
                            place_elem.set("{http://www.w3.org/XML/1998/namespace}id", ref_id.replace("#", ""))
                            place_elem.set("ana", "comment")
                    # Then add refs only implied (with ana="implied", but not if also in in_text)
                    for ref_id in sorted(implied_refs - in_text_refs - in_commentary_refs):
                        if ref_id.strip():
                            place_elem = ET.SubElement(list_elem, f"{{{self.tei_ns}}}place")
                            place_elem.set("{http://www.w3.org/XML/1998/namespace}id", ref_id.replace("#", ""))
                            place_elem.set("ana", "implied")

                elif list_type == "org":
                    list_elem = ET.SubElement(back_elem, f"{{{self.tei_ns}}}listOrg")
                    # First add refs from text (no ana attribute)
                    for ref_id in sorted(in_text_refs):
                        if ref_id.strip():
                            org_elem = ET.SubElement(list_elem, f"{{{self.tei_ns}}}org")
                            org_elem.set("{http://www.w3.org/XML/1998/namespace}id", ref_id.replace("#", ""))
                    # Then add refs only in commentary (with ana="comment")
                    for ref_id in sorted(in_commentary_refs - in_text_refs):
                        if ref_id.strip():
                            org_elem = ET.SubElement(list_elem, f"{{{self.tei_ns}}}org")
                            org_elem.set("{http://www.w3.org/XML/1998/namespace}id", ref_id.replace("#", ""))
                            org_elem.set("ana", "comment")
                    # Then add refs only implied (with ana="implied", but not if also in in_text)
                    for ref_id in sorted(implied_refs - in_text_refs - in_commentary_refs):
                        if ref_id.strip():
                            org_elem = ET.SubElement(list_elem, f"{{{self.tei_ns}}}org")
                            org_elem.set("{http://www.w3.org/XML/1998/namespace}id", ref_id.replace("#", ""))
                            org_elem.set("ana", "implied")

                elif list_type == "event":
                    list_elem = ET.SubElement(back_elem, f"{{{self.tei_ns}}}listEvent")
                    # First add refs from text (no ana attribute)
                    for ref_id in sorted(in_text_refs):
                        if ref_id.strip():
                            event_elem = ET.SubElement(list_elem, f"{{{self.tei_ns}}}event")
                            event_elem.set("{http://www.w3.org/XML/1998/namespace}id", ref_id.replace("#", ""))
                    # Then add refs only in commentary (with ana="comment")
                    for ref_id in sorted(in_commentary_refs - in_text_refs):
                        if ref_id.strip():
                            event_elem = ET.SubElement(list_elem, f"{{{self.tei_ns}}}event")
                            event_elem.set("{http://www.w3.org/XML/1998/namespace}id", ref_id.replace("#", ""))
                            event_elem.set("ana", "comment")
                    # Then add refs only implied (with ana="implied", but not if also in in_text)
                    for ref_id in sorted(implied_refs - in_text_refs - in_commentary_refs):
                        if ref_id.strip():
                            event_elem = ET.SubElement(list_elem, f"{{{self.tei_ns}}}event")
                            event_elem.set("{http://www.w3.org/XML/1998/namespace}id", ref_id.replace("#", ""))
                            event_elem.set("ana", "implied")

        return root

    def _populate_from_wikidata(self, root: ET.Element) -> ET.Element:
        """Populate placeholders with Wikidata data (Step 2)"""
        back_elem = root.find(".//tei:back", self.ns)
        if back_elem is None:
            return root

        # Collect all Q-IDs not yet in cache and pre-fetch them in batches
        all_ids = []
        for list_type in ["Person", "Bibl", "Place", "Org", "Event"]:
            list_elem = back_elem.find(f"tei:list{list_type}", self.ns)
            if list_elem is not None:
                entity_type = list_type.lower()
                for entity in list_elem.findall(f"tei:{entity_type}", self.ns):
                    xml_id = entity.get("{http://www.w3.org/XML/1998/namespace}id")
                    if xml_id and xml_id.startswith('Q') and xml_id not in self.wikidata_cache:
                        all_ids.append(xml_id)

        unique_ids = list(dict.fromkeys(all_ids))  # deduplicate, preserve order
        if unique_ids:
            print(f"🔄 Pre-fetching {len(unique_ids)} unique Wikidata entities in batches of 50...")
            sys.stdout.flush()
            batch_size = 50
            for i in range(0, len(unique_ids), batch_size):
                self._fetch_wikidata_entities_batch(unique_ids[i:i + batch_size])

        # Process each list type (all entities now loaded from cache)
        for list_type in ["Person", "Bibl", "Place", "Org", "Event"]:
            list_elem = back_elem.find(f"tei:list{list_type}", self.ns)
            if list_elem is not None:
                self._populate_list(list_elem, list_type.lower())

        return root

    def _populate_list(self, list_elem: ET.Element, entity_type: str):
        """Populate a specific list with Wikidata data"""
        for entity in list_elem.findall(f"tei:{entity_type}", self.ns):
            xml_id = entity.get("{http://www.w3.org/XML/1998/namespace}id")
            if not xml_id:
                continue

            # Save ana attribute if it exists
            ana_attribute = entity.get("ana")

            # Check if this is a Wikidata ID (starts with Q)
            if not xml_id.startswith('Q'):
                print(f"⚠️ Skipping non-Wikidata ID: {xml_id}")
                continue

            print(f"🔍 Looking up Wikidata: {xml_id} (entity_type: {entity_type})")
            sys.stdout.flush()

            # Fetch from Wikidata
            self.stats["wikidata_lookups"] += 1
            wikidata_data = self._fetch_wikidata_entity(xml_id)

            if wikidata_data:
                # Convert Wikidata JSON to TEI XML
                self.stats["wikidata_found"] += 1
                print(f"✅ Found {xml_id} in Wikidata")
                sys.stdout.flush()

                # Clear entity and rebuild with Wikidata data
                entity.clear()
                entity.set("{http://www.w3.org/XML/1998/namespace}id", xml_id)

                # Restore ana attribute if it existed
                if ana_attribute:
                    entity.set("ana", ana_attribute)

                # Convert based on entity type
                if entity_type == 'person':
                    self._add_person_from_wikidata(entity, wikidata_data)
                elif entity_type == 'place':
                    self._add_place_from_wikidata(entity, wikidata_data)
                elif entity_type == 'org':
                    self._add_org_from_wikidata(entity, wikidata_data)
                elif entity_type == 'bibl':
                    self._add_work_from_wikidata(entity, wikidata_data)
                elif entity_type == 'event':
                    self._add_event_from_wikidata(entity, wikidata_data)
            else:
                self.stats["wikidata_not_found"] += 1
                print(f"❌ {xml_id} NOT FOUND in Wikidata")
                sys.stdout.flush()

    def _get_wikidata_label(self, data: dict, lang: str = 'de') -> str:
        """Get label from Wikidata entity in specified language"""
        labels = data.get('labels', {})
        if lang in labels:
            return labels[lang].get('value', '')
        # Fallback to English
        if 'en' in labels:
            return labels['en'].get('value', '')
        # Fallback to first available label
        if labels:
            return next(iter(labels.values())).get('value', '')
        return ''

    def _get_wikidata_claim_value(self, data: dict, property_id: str) -> Optional[str]:
        """Get the value of a Wikidata claim, preferring 'preferred' rank if available"""
        claims = data.get('claims', {})
        if property_id not in claims:
            return None
        if not claims[property_id]:
            return None

        # Look for preferred rank first
        preferred_claims = [c for c in claims[property_id] if c.get('rank') == 'preferred']

        # If we have preferred claims, use the first one
        if preferred_claims:
            claim = preferred_claims[0]
        else:
            # Otherwise, use the first normal claim (skip deprecated)
            normal_claims = [c for c in claims[property_id] if c.get('rank') != 'deprecated']
            if not normal_claims:
                return None
            claim = normal_claims[0]

        mainsnak = claim.get('mainsnak', {})
        datavalue = mainsnak.get('datavalue', {})

        if datavalue.get('type') == 'string':
            return datavalue.get('value')
        elif datavalue.get('type') == 'time':
            return datavalue.get('value', {}).get('time', '').lstrip('+')
        elif datavalue.get('type') == 'wikibase-entityid':
            return datavalue.get('value', {}).get('id')

        return None

    def _get_wikidata_given_names(self, data: dict) -> list:
        """Get all given names (P735) with their series ordinal (P1545) qualifier, sorted by ordinal"""
        claims = data.get('claims', {})
        if 'P735' not in claims:
            return []
        if not claims['P735']:
            return []

        # Filter out deprecated claims
        valid_claims = [c for c in claims['P735'] if c.get('rank') != 'deprecated']

        # Extract given name IDs with their ordinal
        given_names = []
        for claim in valid_claims:
            mainsnak = claim.get('mainsnak', {})
            datavalue = mainsnak.get('datavalue', {})

            if datavalue.get('type') == 'wikibase-entityid':
                given_name_id = datavalue.get('value', {}).get('id')
                if given_name_id:
                    # Check for series ordinal qualifier (P1545)
                    ordinal = None
                    qualifiers = claim.get('qualifiers', {})
                    if 'P1545' in qualifiers:
                        ordinal_claim = qualifiers['P1545'][0]
                        ordinal_value = ordinal_claim.get('datavalue', {})
                        if ordinal_value.get('type') == 'string':
                            try:
                                ordinal = int(ordinal_value.get('value', '0'))
                            except ValueError:
                                pass

                    given_names.append({
                        'id': given_name_id,
                        'ordinal': ordinal if ordinal is not None else 999  # Put items without ordinal at the end
                    })

        # Sort by ordinal
        given_names.sort(key=lambda x: x['ordinal'])

        return [gn['id'] for gn in given_names]

    def _get_wikidata_occupations(self, data: dict) -> list:
        """Get all occupations (P106) with their labels, including female form (P2521)
        Returns: List of tuples (occupation_id, occupation_label)
        """
        claims = data.get('claims', {})
        if 'P106' not in claims:
            return []
        if not claims['P106']:
            return []

        # Filter out deprecated claims
        valid_claims = [c for c in claims['P106'] if c.get('rank') != 'deprecated']

        occupations = []
        for claim in valid_claims:
            mainsnak = claim.get('mainsnak', {})
            datavalue = mainsnak.get('datavalue', {})

            if datavalue.get('type') == 'wikibase-entityid':
                occupation_id = datavalue.get('value', {}).get('id')
                if occupation_id:
                    # Fetch occupation entity to get label
                    occupation_data = self._fetch_wikidata_entity(occupation_id)
                    if occupation_data:
                        occupation_label = self._get_wikidata_label(occupation_data)
                        if occupation_label:
                            # Check for female form (P2521) on the occupation entity
                            # P2521 is a monolingualtext property, not handled by _get_wikidata_claim_value
                            female_label = None
                            p2521_claims = occupation_data.get('claims', {}).get('P2521', [])
                            # Prefer German, fallback to any language
                            fallback = None
                            for p2521_claim in p2521_claims:
                                if p2521_claim.get('rank') == 'deprecated':
                                    continue
                                dv = p2521_claim.get('mainsnak', {}).get('datavalue', {})
                                if dv.get('type') == 'monolingualtext':
                                    val = dv.get('value', {})
                                    if val.get('language') == 'de':
                                        female_label = val.get('text')
                                        break
                                    elif fallback is None:
                                        fallback = val.get('text')
                            if female_label is None:
                                female_label = fallback
                            if female_label:
                                occupation_label = f"{occupation_label}/{female_label}"
                            occupations.append((occupation_id, occupation_label))

        return occupations

    def _get_wikidata_family_names(self, data: dict) -> dict:
        """Get all family names (P734), returning preferred and additional names separately"""
        claims = data.get('claims', {})
        if 'P734' not in claims:
            return {'preferred': None, 'additional': []}
        if not claims['P734']:
            return {'preferred': None, 'additional': []}

        # Filter out deprecated claims
        valid_claims = [c for c in claims['P734'] if c.get('rank') != 'deprecated']
        if not valid_claims:
            return {'preferred': None, 'additional': []}

        # Look for preferred rank first
        preferred_claims = [c for c in valid_claims if c.get('rank') == 'preferred']

        preferred_id = None
        additional_ids = []

        if preferred_claims:
            # Use first preferred claim as main surname
            mainsnak = preferred_claims[0].get('mainsnak', {})
            datavalue = mainsnak.get('datavalue', {})
            if datavalue.get('type') == 'wikibase-entityid':
                preferred_id = datavalue.get('value', {}).get('id')

            # All other claims (including other preferred ones) are additional
            for claim in preferred_claims[1:] + [c for c in valid_claims if c.get('rank') != 'preferred']:
                mainsnak = claim.get('mainsnak', {})
                datavalue = mainsnak.get('datavalue', {})
                if datavalue.get('type') == 'wikibase-entityid':
                    family_id = datavalue.get('value', {}).get('id')
                    if family_id and family_id != preferred_id:
                        additional_ids.append(family_id)
        else:
            # No preferred claim, use first normal claim as main
            first_claim = valid_claims[0]
            mainsnak = first_claim.get('mainsnak', {})
            datavalue = mainsnak.get('datavalue', {})
            if datavalue.get('type') == 'wikibase-entityid':
                preferred_id = datavalue.get('value', {}).get('id')

            # Rest are additional
            for claim in valid_claims[1:]:
                mainsnak = claim.get('mainsnak', {})
                datavalue = mainsnak.get('datavalue', {})
                if datavalue.get('type') == 'wikibase-entityid':
                    family_id = datavalue.get('value', {}).get('id')
                    if family_id and family_id != preferred_id:
                        additional_ids.append(family_id)

        return {'preferred': preferred_id, 'additional': additional_ids}

    def _add_person_from_wikidata(self, person_elem: ET.Element, data: dict):
        """Convert Wikidata person data to TEI"""
        # Add persName with surname and forename(s)
        given_name_ids = self._get_wikidata_given_names(data)  # all given names with order
        family_names = self._get_wikidata_family_names(data)  # family names (preferred + additional)

        # Create main persName element
        persname = ET.SubElement(person_elem, f"{{{self.tei_ns}}}persName")

        # Fetch labels for given names and family name if we have IDs
        forename_texts = []
        surname_text = None

        # Fetch all given names in order
        for given_name_id in given_name_ids:
            given_name_data = self._fetch_wikidata_entity(given_name_id)
            if given_name_data:
                forename_label = self._get_wikidata_label(given_name_data)
                if forename_label:
                    forename_texts.append(forename_label)

        # Fetch preferred family name
        if family_names['preferred']:
            family_name_data = self._fetch_wikidata_entity(family_names['preferred'])
            if family_name_data:
                surname_text = self._get_wikidata_label(family_name_data)

        # Add surname and forename elements if available
        if surname_text:
            surname = ET.SubElement(persname, f"{{{self.tei_ns}}}surname")
            surname.text = surname_text

        # Add all forenames in a single forename element, space-separated
        if forename_texts:
            forename = ET.SubElement(persname, f"{{{self.tei_ns}}}forename")
            forename.text = ' '.join(forename_texts)

        # Fallback: if we don't have surname/forename, use the label as plain text
        if not surname_text and not forename_texts:
            label = self._get_wikidata_label(data)
            if label:
                persname.text = label

        # Add additional family names as separate persName elements
        for additional_family_id in family_names['additional']:
            additional_family_data = self._fetch_wikidata_entity(additional_family_id)
            if additional_family_data:
                additional_surname_text = self._get_wikidata_label(additional_family_data)
                if additional_surname_text:
                    additional_persname = ET.SubElement(person_elem, f"{{{self.tei_ns}}}persName")
                    additional_surname = ET.SubElement(additional_persname, f"{{{self.tei_ns}}}surname")
                    additional_surname.text = additional_surname_text

        # Add birth date (P569) and place (P19)
        birth_date_info = self._get_wikidata_date_info(data, 'P569')
        birth_place_id = self._get_wikidata_claim_value(data, 'P19')

        if birth_date_info or birth_place_id:
            birth = ET.SubElement(person_elem, f"{{{self.tei_ns}}}birth")

            if birth_date_info:
                date_elem = ET.SubElement(birth, f"{{{self.tei_ns}}}date")
                self._apply_date_info_to_elem(date_elem, birth_date_info)

            if birth_place_id:
                # Fetch place data to get the place name
                place_data = self._fetch_wikidata_entity(birth_place_id)
                if place_data:
                    place_name = self._get_wikidata_label(place_data)
                    if place_name:
                        settlement = ET.SubElement(birth, f"{{{self.tei_ns}}}settlement")
                        settlement.set("ref", birth_place_id)
                        placename = ET.SubElement(settlement, f"{{{self.tei_ns}}}placeName")
                        placename.set("type", "pref")
                        placename.text = place_name

        # Add death date (P570) and place (P20)
        death_date_info = self._get_wikidata_date_info(data, 'P570')
        death_place_id = self._get_wikidata_claim_value(data, 'P20')

        if death_date_info or death_place_id:
            death = ET.SubElement(person_elem, f"{{{self.tei_ns}}}death")

            if death_date_info:
                date_elem = ET.SubElement(death, f"{{{self.tei_ns}}}date")
                self._apply_date_info_to_elem(date_elem, death_date_info)

            if death_place_id:
                # Fetch place data to get the place name
                place_data = self._fetch_wikidata_entity(death_place_id)
                if place_data:
                    place_name = self._get_wikidata_label(place_data)
                    if place_name:
                        settlement = ET.SubElement(death, f"{{{self.tei_ns}}}settlement")
                        settlement.set("ref", death_place_id)
                        placename = ET.SubElement(settlement, f"{{{self.tei_ns}}}placeName")
                        placename.set("type", "pref")
                        placename.text = place_name

        # Add sex/gender (P21)
        sex_id = self._get_wikidata_claim_value(data, 'P21')
        if sex_id:
            # Fetch sex entity to get label
            sex_data = self._fetch_wikidata_entity(sex_id)
            if sex_data:
                sex_label = self._get_wikidata_label(sex_data, 'en')  # Use English for standard values
                # Map Wikidata values to TEI values
                sex_value = None
                if sex_label.lower() in ['female', 'weiblich']:
                    sex_value = 'female'
                elif sex_label.lower() in ['male', 'männlich']:
                    sex_value = 'male'
                elif sex_label.lower() in ['intersex', 'intersexuell']:
                    sex_value = 'intersex'

                if sex_value:
                    sex_elem = ET.SubElement(person_elem, f"{{{self.tei_ns}}}sex")
                    sex_elem.set("value", sex_value)

        # Add occupations (P106)
        occupations = self._get_wikidata_occupations(data)
        for occupation_id, occupation_label in occupations:
            occupation_elem = ET.SubElement(person_elem, f"{{{self.tei_ns}}}occupation")
            occupation_elem.set("key", occupation_id)
            occupation_elem.text = occupation_label

        # Add Wikidata ID (from xml:id)
        wikidata_id = person_elem.get("{http://www.w3.org/XML/1998/namespace}id")
        if wikidata_id and wikidata_id.startswith('Q'):
            idno_wikidata = ET.SubElement(person_elem, f"{{{self.tei_ns}}}idno")
            idno_wikidata.set("type", "wikidata")
            idno_wikidata.text = f"https://www.wikidata.org/wiki/{wikidata_id}"

        # Add GND ID (P227)
        gnd = self._get_wikidata_claim_value(data, 'P227')
        if gnd:
            idno_gnd = ET.SubElement(person_elem, f"{{{self.tei_ns}}}idno")
            idno_gnd.set("type", "gnd")
            idno_gnd.text = f"https://d-nb.info/gnd/{gnd}/"

        # Add PMB ID (P12483) if available
        pmb = self._get_wikidata_claim_value(data, 'P12483')
        if pmb:
            idno_pmb = ET.SubElement(person_elem, f"{{{self.tei_ns}}}idno")
            idno_pmb.set("type", "pmb")
            idno_pmb.text = f"https://pmb.acdh.oeaw.ac.at/entity/{pmb}/"

    def _add_place_from_wikidata(self, place_elem: ET.Element, data: dict):
        """Convert Wikidata place data to TEI"""
        # Add placeName
        label = self._get_wikidata_label(data)
        if label:
            placename = ET.SubElement(place_elem, f"{{{self.tei_ns}}}placeName")
            placename.text = label

        # Add coordinates (P625)
        coords = self._get_wikidata_claim_value(data, 'P625')
        if coords:
            location = ET.SubElement(place_elem, f"{{{self.tei_ns}}}location")
            geo = ET.SubElement(location, f"{{{self.tei_ns}}}geo")
            # Wikidata coordinates need to be extracted from the claim structure
            claims = data.get('claims', {}).get('P625', [])
            if claims:
                coord_value = claims[0].get('mainsnak', {}).get('datavalue', {}).get('value', {})
                lat = coord_value.get('latitude')
                lon = coord_value.get('longitude')
                if lat and lon:
                    geo.text = f"{lat} {lon}"

        # Add Wikidata ID (from xml:id)
        wikidata_id = place_elem.get("{http://www.w3.org/XML/1998/namespace}id")
        if wikidata_id and wikidata_id.startswith('Q'):
            idno_wikidata = ET.SubElement(place_elem, f"{{{self.tei_ns}}}idno")
            idno_wikidata.set("type", "wikidata")
            idno_wikidata.text = f"https://www.wikidata.org/wiki/{wikidata_id}"

        # Add GND ID (P227)
        gnd = self._get_wikidata_claim_value(data, 'P227')
        if gnd:
            idno_gnd = ET.SubElement(place_elem, f"{{{self.tei_ns}}}idno")
            idno_gnd.set("type", "gnd")
            idno_gnd.text = f"https://d-nb.info/gnd/{gnd}/"

        # Add PMB ID (P12483) if available
        pmb = self._get_wikidata_claim_value(data, 'P12483')
        if pmb:
            idno_pmb = ET.SubElement(place_elem, f"{{{self.tei_ns}}}idno")
            idno_pmb.set("type", "pmb")
            idno_pmb.text = f"https://pmb.acdh.oeaw.ac.at/entity/{pmb}/"

    def _add_org_from_wikidata(self, org_elem: ET.Element, data: dict):
        """Convert Wikidata organization data to TEI"""
        # Add orgName
        label = self._get_wikidata_label(data)
        if label:
            orgname = ET.SubElement(org_elem, f"{{{self.tei_ns}}}orgName")
            orgname.text = label

        # Add Wikidata ID (from xml:id)
        wikidata_id = org_elem.get("{http://www.w3.org/XML/1998/namespace}id")
        if wikidata_id and wikidata_id.startswith('Q'):
            idno_wikidata = ET.SubElement(org_elem, f"{{{self.tei_ns}}}idno")
            idno_wikidata.set("type", "wikidata")
            idno_wikidata.text = f"https://www.wikidata.org/wiki/{wikidata_id}"

        # Add GND ID (P227)
        gnd = self._get_wikidata_claim_value(data, 'P227')
        if gnd:
            idno_gnd = ET.SubElement(org_elem, f"{{{self.tei_ns}}}idno")
            idno_gnd.set("type", "gnd")
            idno_gnd.text = f"https://d-nb.info/gnd/{gnd}/"

        # Add PMB ID (P12483) if available
        pmb = self._get_wikidata_claim_value(data, 'P12483')
        if pmb:
            idno_pmb = ET.SubElement(org_elem, f"{{{self.tei_ns}}}idno")
            idno_pmb.set("type", "pmb")
            idno_pmb.text = f"https://pmb.acdh.oeaw.ac.at/entity/{pmb}/"

    def _add_work_from_wikidata(self, bibl_elem: ET.Element, data: dict):
        """Convert Wikidata work data to TEI"""
        # Add title
        label = self._get_wikidata_label(data)
        if label:
            title = ET.SubElement(bibl_elem, f"{{{self.tei_ns}}}title")
            title.text = label

        # Add author (P50)
        author_id = self._get_wikidata_claim_value(data, 'P50')
        if author_id:
            author = ET.SubElement(bibl_elem, f"{{{self.tei_ns}}}author")
            author.set("key", author_id)
            author.set("role", "hat-geschaffen")

            # Fetch author data to get name
            author_data = self._fetch_wikidata_entity(author_id)
            if author_data:
                # Get author name from Wikidata
                author_name = self._get_wikidata_label(author_data)
                if author_name:
                    author.text = author_name

        # Add publication date (P577)
        pub_date = self._get_wikidata_claim_value(data, 'P577')
        if pub_date:
            date_elem = ET.SubElement(bibl_elem, f"{{{self.tei_ns}}}date")
            # Normalize date: extract year-month-day, handle year-only dates
            normalized_date = self._normalize_iso_date(pub_date)
            if normalized_date:
                date_elem.set("when", normalized_date)

        # Add Wikidata ID (from xml:id)
        wikidata_id = bibl_elem.get("{http://www.w3.org/XML/1998/namespace}id")
        if wikidata_id and wikidata_id.startswith('Q'):
            idno_wikidata = ET.SubElement(bibl_elem, f"{{{self.tei_ns}}}idno")
            idno_wikidata.set("type", "wikidata")
            idno_wikidata.text = f"https://www.wikidata.org/wiki/{wikidata_id}"

        # Add GND ID (P227) if available
        gnd = self._get_wikidata_claim_value(data, 'P227')
        if gnd:
            idno_gnd = ET.SubElement(bibl_elem, f"{{{self.tei_ns}}}idno")
            idno_gnd.set("type", "gnd")
            idno_gnd.text = f"https://d-nb.info/gnd/{gnd}/"

        # Add PMB ID (P12483) if available
        pmb = self._get_wikidata_claim_value(data, 'P12483')
        if pmb:
            idno_pmb = ET.SubElement(bibl_elem, f"{{{self.tei_ns}}}idno")
            idno_pmb.set("type", "pmb")
            idno_pmb.text = f"https://pmb.acdh.oeaw.ac.at/entity/{pmb}/"

    def _add_event_from_wikidata(self, event_elem: ET.Element, data: dict):
        """Convert Wikidata event data to TEI"""
        # Add event label
        label = self._get_wikidata_label(data)
        if label:
            label_elem = ET.SubElement(event_elem, f"{{{self.tei_ns}}}label")
            label_elem.text = label

        # Add start time (P580)
        start_date = self._get_wikidata_claim_value(data, 'P580')
        if start_date:
            date_elem = ET.SubElement(event_elem, f"{{{self.tei_ns}}}date")
            date_elem.set("from", start_date[:10])

            # Add end time (P582)
            end_date = self._get_wikidata_claim_value(data, 'P582')
            if end_date:
                date_elem.set("to", end_date[:10])

        # Add Wikidata ID (from xml:id)
        wikidata_id = event_elem.get("{http://www.w3.org/XML/1998/namespace}id")
        if wikidata_id and wikidata_id.startswith('Q'):
            idno_wikidata = ET.SubElement(event_elem, f"{{{self.tei_ns}}}idno")
            idno_wikidata.set("type", "wikidata")
            idno_wikidata.text = f"https://www.wikidata.org/wiki/{wikidata_id}"

        # Add GND ID (P227) if available
        gnd = self._get_wikidata_claim_value(data, 'P227')
        if gnd:
            idno_gnd = ET.SubElement(event_elem, f"{{{self.tei_ns}}}idno")
            idno_gnd.set("type", "gnd")
            idno_gnd.text = f"https://d-nb.info/gnd/{gnd}/"

        # Add PMB ID (P12483) if available
        pmb = self._get_wikidata_claim_value(data, 'P12483')
        if pmb:
            idno_pmb = ET.SubElement(event_elem, f"{{{self.tei_ns}}}idno")
            idno_pmb.set("type", "pmb")
            idno_pmb.text = f"https://pmb.acdh.oeaw.ac.at/entity/{pmb}/"

    def _add_persons_from_bibliography(self, root: ET.Element) -> ET.Element:
        """Add persons from bibliography authors to listPerson (Step 3)"""
        back_elem = root.find(".//tei:back", self.ns)
        if back_elem is None:
            return root

        listbibl = back_elem.find("tei:listBibl", self.ns)
        if listbibl is None:
            return root

        # Check if listBibl has bibl elements with author elements (ref or key, since normalization runs later)
        bibl_authors_ref = listbibl.findall(".//tei:bibl/tei:author[@ref]", self.ns)
        bibl_authors_key = listbibl.findall(".//tei:bibl/tei:author[@key]", self.ns)
        bibl_authors = bibl_authors_ref + bibl_authors_key
        if not bibl_authors:
            print("No author elements with @ref or @key found in listBibl")
            return root

        # Get or create listPerson
        listperson = back_elem.find("tei:listPerson", self.ns)
        if listperson is None:
            listperson = ET.SubElement(back_elem, f"{{{self.tei_ns}}}listPerson")

        # Get existing person IDs
        existing_person_ids = set()
        for person in listperson.findall("tei:person", self.ns):
            xml_id = person.get("{http://www.w3.org/XML/1998/namespace}id")
            if xml_id:
                existing_person_ids.add(xml_id)

        # Get unique author refs from bibliography (Wikidata IDs)
        author_refs = set()
        for author in bibl_authors:
            ref = author.get("ref") or author.get("key")
            if ref:
                # Clean the reference
                clean_ref = ref.replace("#", "")
                if clean_ref.startswith("Q"):
                    author_refs.add(clean_ref)

        print(f"Found {len(author_refs)} unique Wikidata author references")
        print(f"Existing persons: {len(existing_person_ids)}")

        # Add missing persons with ana="implied"
        added_count = 0
        for ref in author_refs:
            if ref not in existing_person_ids:
                print(f"Adding person: {ref}")

                # Fetch from Wikidata
                wikidata_data = self._fetch_wikidata_entity(ref)

                if wikidata_data:
                    person_elem = ET.SubElement(listperson, f"{{{self.tei_ns}}}person")
                    person_elem.set("{http://www.w3.org/XML/1998/namespace}id", ref)
                    person_elem.set("ana", "implied")
                    self._add_person_from_wikidata(person_elem, wikidata_data)
                    added_count += 1
                    print(f"✅ Added {ref} from Wikidata (implied)")

        print(f"Added {added_count} persons to listPerson")
        return root

    def _normalize_data(self, root: ET.Element) -> ET.Element:
        """Apply normalizations (Step 4)"""
        # Convert when-iso to when attributes
        for elem in root.findall(".//*[@when-iso]"):
            when_iso = elem.get("when-iso")
            elem.set("when", self._normalize_date(when_iso))
            del elem.attrib["when-iso"]
        
        # Convert notAfter-iso to notAfter
        for elem in root.findall(".//*[@notAfter-iso]"):
            notafter_iso = elem.get("notAfter-iso")
            elem.set("notAfter", self._normalize_date(notafter_iso))
            del elem.attrib["notAfter-iso"]
        
        # Convert notBefore-iso to notBefore
        for elem in root.findall(".//*[@notBefore-iso]"):
            notbefore_iso = elem.get("notBefore-iso")
            elem.set("notBefore", self._normalize_date(notbefore_iso))
            del elem.attrib["notBefore-iso"]
        
        # Convert from-iso to from
        for elem in root.findall(".//*[@from-iso]"):
            from_iso = elem.get("from-iso")
            elem.set("from", from_iso)
            del elem.attrib["from-iso"]
        
        # Convert to-iso to to
        for elem in root.findall(".//*[@to-iso]"):
            to_iso = elem.get("to-iso")
            elem.set("to", to_iso)
            del elem.attrib["to-iso"]
        
        # Convert key to ref attributes (for Wikidata IDs)
        for elem in root.findall(".//*[@key]"):
            key = elem.get("key")
            # Keep Wikidata IDs as-is if they start with Q
            if not key.startswith('Q'):
                # You may want to handle other ID formats here
                pass
            elem.set("ref", key)
            del elem.attrib["key"]

        # Normalize XML IDs in back section (ensure they're valid Wikidata IDs)
        back_elem = root.find(".//tei:back", self.ns)
        if back_elem is not None:
            for entity_type in ["person", "bibl", "place", "org", "event"]:
                for entity in back_elem.findall(f".//tei:{entity_type}[@xml:id]", self.ns):
                    xml_id = entity.get("{http://www.w3.org/XML/1998/namespace}id")
                    # Keep Wikidata IDs as-is
                    if xml_id and xml_id.startswith('Q'):
                        continue
        
        # Handle special URL conversions and cleanups
        self._normalize_urls(root)
        self._cleanup_duplicates(root)
        
        return root

    def _format_date_by_precision(self, time_str: str, precision: int) -> str:
        """Format a Wikidata time string to an ISO date string based on its precision.

        Wikidata precision levels relevant here:
          11 = Tag (YYYY-MM-DD)
          10 = Monat (YYYY-MM)
           9 = Jahr (YYYY)
           8 = Jahrzehnt
           7 = Jahrhundert
        """
        if not time_str:
            return ""

        is_bce = time_str.startswith('-')
        clean = time_str.lstrip('+-')

        if 'T' in clean:
            clean = clean.split('T')[0]

        parts = clean.split('-')
        year_str = parts[0].zfill(4)
        month_str = parts[1] if len(parts) > 1 else '01'
        day_str = parts[2] if len(parts) > 2 else '01'

        prefix = '-' if is_bce else ''

        if precision >= 11:
            return f"{prefix}{year_str}-{month_str}-{day_str}"
        elif precision == 10:
            return f"{prefix}{year_str}-{month_str}"
        else:
            # precision 9 (Jahr) und ungenauer: nur Jahr zurückgeben
            return f"{prefix}{year_str}"

    def _get_wikidata_date_info(self, data: dict, property_id: str) -> Optional[dict]:
        """Extrahiert strukturierte Datumsinformation aus einem Wikidata-Time-Claim.

        Berücksichtigt Präzision sowie folgende Qualifikatoren:
          P1319 – frühestmöglicher Zeitpunkt  → notBefore
          P1326 – spätestmöglicher Zeitpunkt  → notAfter
          P1480 – Nachweisumstände (Q5727902 = circa) → is_circa
        """
        claims = data.get('claims', {})
        if property_id not in claims or not claims[property_id]:
            return None

        preferred = [c for c in claims[property_id] if c.get('rank') == 'preferred']
        normal = [c for c in claims[property_id] if c.get('rank') != 'deprecated']
        claim = preferred[0] if preferred else (normal[0] if normal else None)
        if not claim:
            return None

        mainsnak = claim.get('mainsnak', {})
        datavalue = mainsnak.get('datavalue', {})
        if datavalue.get('type') != 'time':
            return None

        value = datavalue.get('value', {})
        time_str = value.get('time', '').lstrip('+')
        precision = value.get('precision', 11)

        qualifiers = claim.get('qualifiers', {})

        def _extract_qualifier_date(qual_list):
            snak = qual_list[0]
            qval = snak.get('datavalue', {})
            if qval.get('type') == 'time':
                qt = qval['value'].get('time', '').lstrip('+')
                qp = qval['value'].get('precision', 9)
                return self._format_date_by_precision(qt, qp)
            return None

        not_before = _extract_qualifier_date(qualifiers['P1319']) if 'P1319' in qualifiers else None
        not_after = _extract_qualifier_date(qualifiers['P1326']) if 'P1326' in qualifiers else None

        is_circa = False
        if 'P1480' in qualifiers:
            qval = qualifiers['P1480'][0].get('datavalue', {})
            if qval.get('type') == 'wikibase-entityid':
                if qval.get('value', {}).get('id') == 'Q5727902':
                    is_circa = True

        return {
            'formatted': self._format_date_by_precision(time_str, precision),
            'precision': precision,
            'not_before': not_before,
            'not_after': not_after,
            'is_circa': is_circa,
        }

    def _apply_date_info_to_elem(self, date_elem, date_info: dict):
        """Setzt TEI-Attribute und deutschen Lesetext auf einem <date>-Element.

        Logik:
          - Bereich (notBefore/notAfter vorhanden) → @notBefore / @notAfter
          - Sonst → @when mit präzisionsgerechtem Datum
          - ca. → @cert="low"
          - Elementtext (deutsch): "1761", "ca. 1761", "nach 1741", "vor 1800", "1741–1800"
        """
        nb = date_info.get('not_before')
        na = date_info.get('not_after')
        fmt = date_info.get('formatted', '')
        circa = date_info.get('is_circa', False)

        if nb or na:
            if nb:
                date_elem.set("notBefore", nb)
            if na:
                date_elem.set("notAfter", na)
        elif fmt:
            date_elem.set("when", fmt)

        if circa:
            date_elem.set("cert", "low")

        # Lesetext auf Deutsch
        if nb and na:
            text = f"{nb}–{na}"
        elif nb:
            text = f"nach {nb}"
        elif na:
            text = f"vor {na}"
        else:
            text = fmt

        if circa:
            text = f"ca. {text}"

        if text:
            date_elem.text = text

    def _normalize_date(self, date_str: str) -> str:
        """Normalize date format by padding year"""
        if not date_str or '-' not in date_str:
            return date_str

        year = date_str.split('-')[0]
        if len(year) == 4:
            return date_str
        elif len(year) == 3:
            return f"0{date_str}"
        elif len(year) == 2:
            return f"00{date_str}"
        elif len(year) == 1:
            return f"000{date_str}"

        return date_str

    def _normalize_iso_date(self, date_str: str) -> str:
        """Normalize ISO date to valid YYYY-MM-DD format, handling year-only and invalid dates"""
        if not date_str:
            return ""

        # Remove leading + if present
        date_str = date_str.lstrip('+')

        # Extract date part (before 'T' if timestamp)
        if 'T' in date_str:
            date_str = date_str.split('T')[0]

        # Split into components
        parts = date_str.split('-')
        if len(parts) < 1:
            return ""

        year = parts[0]
        month = parts[1] if len(parts) > 1 else None
        day = parts[2] if len(parts) > 2 else None

        # Validate and fix components
        # Year: pad to 4 digits
        if year:
            year = year.zfill(4)
        else:
            return ""

        # Month: must be 01-12, if 00 or invalid, use year-only format
        if month and month != '00':
            try:
                month_int = int(month)
                if 1 <= month_int <= 12:
                    month = month.zfill(2)
                else:
                    # Invalid month, use year only
                    return year
            except ValueError:
                return year
        else:
            # Month is 00 or missing, use year only
            return year

        # Day: must be 01-31, if 00 or invalid, use year-month format
        if day and day != '00':
            try:
                day_int = int(day)
                if 1 <= day_int <= 31:
                    day = day.zfill(2)
                    return f"{year}-{month}-{day}"
                else:
                    # Invalid day, use year-month
                    return f"{year}-{month}"
            except ValueError:
                return f"{year}-{month}"
        else:
            # Day is 00 or missing, use year-month format
            return f"{year}-{month}"

    def _normalize_urls(self, root: ET.Element):
        """Convert various URL elements to proper idno elements"""
        back_elem = root.find(".//tei:back", self.ns)
        if back_elem is None:
            return
        
        # Convert note[@type='IDNO'] to idno elements
        for note in back_elem.findall(".//tei:note[@type='IDNO']", self.ns):
            # Find parent and replace
            for parent in back_elem.iter():
                if note in parent:
                    idno = ET.Element(f"{{{self.tei_ns}}}idno")
                    idno.set("type", "URL")
                    idno.set("subtype", self._get_url_subtype(note.text or ""))
                    idno.text = note.text
                    parent.remove(note)
                    parent.append(idno)
                    break

    def _get_url_subtype(self, url: str) -> str:
        """Determine URL subtype based on domain"""
        if "wikipedia" in url:
            return "wikipedia"
        elif "wikidata" in url:
            return "wikidata"
        elif "geonames" in url:
            return "geonames"
        elif url.startswith("https://www."):
            return url.split("https://www.")[1].split(".")[0]
        elif url.startswith("http://www."):
            return url.split("http://www.")[1].split(".")[0]
        elif url.startswith("https://"):
            return url.split("https://")[1].split(".")[0]
        elif url.startswith("http://"):
            return url.split("http://")[1].split(".")[0]
        else:
            return url.split(".")[0] if "." in url else "unknown"

    def _indent_element(self, elem: ET.Element, level: int = 0, indent_str: str = "   "):
        """Add proper indentation to XML elements for pretty printing"""
        # Add newline + indent before element
        indent = "\n" + (indent_str * level)

        # If element has children
        if len(elem):
            # Set indent before first child
            if not elem.text or not elem.text.strip():
                elem.text = indent + indent_str

            # Process all children
            for i, child in enumerate(elem):
                self._indent_element(child, level + 1, indent_str)

                # Add indent after each child (except last)
                if i < len(elem) - 1:
                    if not child.tail or not child.tail.strip():
                        child.tail = indent + indent_str
                else:
                    # Last child: indent back to parent level
                    if not child.tail or not child.tail.strip():
                        child.tail = indent

        # If this is not the root and has no tail yet
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = indent

    def _cleanup_duplicates(self, root: ET.Element):
        """Remove duplicate placeName elements and empty lists"""
        back_elem = root.find(".//tei:back", self.ns)
        if back_elem is None:
            return
        
        # Remove listBibl elements with bibl[@type='collections'] and note[@type='collections']
        elements_to_remove = []
        
        # Find all listBibl elements that contain bibl[@type='collections']
        for listbibl in back_elem.findall(".//tei:listBibl", self.ns):
            bibls_with_collections = listbibl.findall("tei:bibl[@type='collections']", self.ns)
            if bibls_with_collections:
                elements_to_remove.append(listbibl)
        
        # Find all note[@type='collections'] elements
        for note in back_elem.findall(".//tei:note[@type='collections']", self.ns):
            elements_to_remove.append(note)
        
        # Remove found elements
        for element in elements_to_remove:
            parent = None
            for potential_parent in back_elem.iter():
                if element in potential_parent:
                    parent = potential_parent
                    break
            if parent is not None:
                parent.remove(element)
        
        # Remove empty lists
        for list_elem in back_elem.findall("tei:list*", self.ns):
            if len(list_elem) == 0:
                back_elem.remove(list_elem)

    def process_file(self, input_file: str, output_file: Optional[str] = None) -> str:
        """Process a TEI XML file through all three steps"""
        try:
            print(f"🔄 Starting processing of {Path(input_file).name}")
            sys.stdout.flush()
            
            # Read original file to preserve processing instructions
            print(f"📖 Reading file: {Path(input_file).name}")
            sys.stdout.flush()
            with open(input_file, 'r', encoding='utf-8') as f:
                original_content = f.read()
            
            print(f"📝 File size: {len(original_content):,} characters")
            sys.stdout.flush()
            
            # Extract processing instructions from original file
            print(f"🔍 Extracting processing instructions...")
            processing_instructions = []
            lines = original_content.split('\n')
            for line in lines:
                line = line.strip()
                if line.startswith('<?') and line.endswith('?>') and not line.startswith('<?xml '):
                    processing_instructions.append(line)
                elif line.startswith('<') and not line.startswith('<?'):
                    break  # Stop at first actual XML element
            
            print(f"⚙️ Parsing XML...")
            sys.stdout.flush()
            tree = ET.parse(input_file)
            root = tree.getroot()
            print(f"✅ XML parsed successfully")
            sys.stdout.flush()

            # Remove existing back element before processing
            text_elem = root.find(".//tei:text", self.ns)
            if text_elem is not None:
                existing_back = text_elem.find("tei:back", self.ns)
                if existing_back is not None:
                    text_elem.remove(existing_back)
                    print(f"🗑️ Existing tei:back element removed")
                    sys.stdout.flush()

            # Step 1: Extract references and create back element
            print(f"🔍 Step 1: Extracting references...")
            sys.stdout.flush()
            refs = self._extract_refs(root)
            print(f"📊 Found references: {sum(len(v) for v in refs.values())} total")
            sys.stdout.flush()
            root = self._create_back_element(root, refs)
            print(f"✅ Step 1 completed")
            sys.stdout.flush()
            
            # Step 2: Populate with Wikidata data
            print(f"🌐 Step 2: Populating from Wikidata...")
            sys.stdout.flush()
            root = self._populate_from_wikidata(root)
            print(f"✅ Step 2 completed")
            sys.stdout.flush()

            # Step 3: Add persons from bibliography authors
            print(f"👤 Step 3: Adding persons from bibliography authors...")
            sys.stdout.flush()
            root = self._add_persons_from_bibliography(root)
            print(f"✅ Step 3 completed")
            sys.stdout.flush()

            # Step 4: Normalize data
            print(f"⚙️ Step 4: Normalizing data...")
            sys.stdout.flush()
            root = self._normalize_data(root)
            print(f"✅ Step 4 completed")
            sys.stdout.flush()

            # Step 5: Add proper indentation to back element
            print(f"⚙️ Step 5: Formatting output...")
            sys.stdout.flush()
            back_elem = root.find(".//tei:back", self.ns)
            if back_elem is not None:
                self._indent_element(back_elem, level=2)  # level 2 because back is inside text
            print(f"✅ Step 5 completed")
            sys.stdout.flush()

            # Write output
            if output_file is None:
                output_file = input_file

            # Write with lxml with pretty printing for better formatting
            tree.write(output_file, encoding="utf-8", xml_declaration=True, pretty_print=True)
            
            # Print statistics
            print(f"\n📊 Wikidata Processing Statistics:")
            print(f"   Total lookups: {self.stats['wikidata_lookups']}")
            print(f"   Found in Wikidata: {self.stats['wikidata_found']}")
            print(f"   Not found: {self.stats['wikidata_not_found']}")
            if self.stats['wikidata_lookups'] > 0:
                print(f"   Success rate: {100 * self.stats['wikidata_found'] / self.stats['wikidata_lookups']:.1f}%")
            
            # Re-read the written file and insert processing instructions
            with open(output_file, 'r', encoding='utf-8') as f:
                content = f.read()

            # Insert processing instructions after XML declaration
            lines = content.split('\n')

            # Find where to insert: after <?xml ... ?> but before any other PIs or root element
            insert_index = 0
            if lines[0].startswith('<?xml '):
                insert_index = 1

            # Remove any existing processing instructions that we're about to re-add
            # to prevent duplicates when running multiple times
            cleaned_lines = lines[:insert_index]
            existing_pis = set()

            for i in range(insert_index, len(lines)):
                line = lines[i].strip()
                # Skip existing PIs that match our extracted ones
                if line.startswith('<?') and line.endswith('?>') and not line.startswith('<?xml '):
                    if line in processing_instructions:
                        existing_pis.add(line)
                        continue  # Skip this line to avoid duplication
                cleaned_lines.append(lines[i])

            # Now insert our processing instructions after the XML declaration
            final_lines = cleaned_lines[:insert_index] + processing_instructions + cleaned_lines[insert_index:]
            content = '\n'.join(final_lines)

            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(content)

            self._save_cache()
            return output_file

        except Exception as e:
            print(
                f"❌ ERROR processing {input_file}: {e}",
                file=sys.stderr
            )
            import traceback
            traceback.print_exc()
            self._save_cache()
            sys.stdout.flush()
            sys.stderr.flush()
            return ""


def main():
    print(f"🚀 Starting Wikidata back-element processor")
    sys.stdout.flush()

    if len(sys.argv) < 2:
        print("Usage: python add-back-element-from-pmb.py <input_file> [output_file]")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None

    print(f"📝 Processing file: {input_file}")
    sys.stdout.flush()

    print(f"🔧 Creating WikidataProcessor instance...")
    sys.stdout.flush()

    processor = WikidataProcessor()

    print(f"▶️ Starting file processing...")
    sys.stdout.flush()

    result = processor.process_file(input_file, output_file)

    print(f"✅ Script completed: {result}")
    sys.stdout.flush()

    if result:
        print(f"Successfully processed: {result}")
        sys.exit(0)
    else:
        print("❌ Processing failed - no output file was created", file=sys.stderr)
        sys.stderr.flush()
        sys.exit(1)


if __name__ == "__main__":
    main()