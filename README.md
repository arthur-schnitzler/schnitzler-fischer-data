# Schnitzler–Fischer Data

## English

### About this Repository

This is a **test repository**. The data was originally retrieved using the export function of Arthur Schnitzler-Archiv Freiburg (Hg.): Arthur Schnitzler and the publishing house S. Fischer. 
Correspondence database 1888–1931. https://biblio.ub.uni-freiburg.de/sf/ at [https://biblio.ub.uni-freiburg.de/sf/](https://biblio.ub.uni-freiburg.de/sf/).

### Rights

All rights to the data remain with the original data creators. This repository does not claim any ownership over the source material.

### Disclaimer

No guarantee is given for the accuracy, completeness, or continued availability of the data. The data is provided as-is, without warranty of any kind.

### Data Conversion

The exported data was converted into valid [TEI](https://tei-c.org/) (Text Encoding Initiative) XML documents using a set of Python scripts:

- `convert_ndjson_to_tei.py` — converts the NDJSON export into TEI XML files
- `add_letter_refs.py` — adds letter references to the TEI documents
- `add_pmb_refs.py` — enriches the data with references to the [PMB (Personen der Moderne Basis)](https://pmb.acdh.oeaw.ac.at/)
- `export_persons_without_ref.py` — exports persons not yet matched to a PMB entry

### OCR Text Retrieval

The full text content of the letters was automatically fetched from [https://schnitzler-mikrofilme.acdh.oeaw.ac.at](https://schnitzler-mikrofilme.acdh.oeaw.ac.at) using the script `add_ocr_text.py`.

---

## Deutsch

### Über dieses Repository

Dies ist ein **Test-Repository**. Die Daten wurden ursprünglich mithilfe der Export-Funktion von Arthur Schnitzler-Archiv Freiburg (Hg.): Arthur Schnitzler und der S. Fischer Verlag. 
Briefdatenbank 1888–1931. https://biblio.ub.uni-freiburg.de/sf/ unter [https://biblio.ub.uni-freiburg.de/sf/](https://biblio.ub.uni-freiburg.de/sf/) bezogen.

### Rechte

Alle Rechte an den Daten verbleiben bei den ursprünglichen Datenerstellern. Dieses Repository erhebt keinen Eigentumsanspruch auf das Quellmaterial.

### Haftungsausschluss

Für die Richtigkeit, Vollständigkeit oder den fortlaufenden Bestand der Daten wird keine Gewähr übernommen. Die Daten werden ohne jegliche Garantie bereitgestellt.

### Datenkonvertierung

Die exportierten Daten wurden mithilfe einer Reihe von Python-Skripten in valide [TEI](https://tei-c.org/)-Dokumente (Text Encoding Initiative) umgewandelt:

- `convert_ndjson_to_tei.py` — konvertiert den NDJSON-Export in TEI-XML-Dateien
- `add_letter_refs.py` — fügt Briefreferenzen in die TEI-Dokumente ein
- `add_pmb_refs.py` — reichert die Daten mit Verweisen auf die [PMB (Personen der Moderne Basis)](https://pmb.acdh.oeaw.ac.at/) an
- `export_persons_without_ref.py` — exportiert Personen, die noch keinem PMB-Eintrag zugeordnet wurden

### Bezug der OCR-Textinhalte

Die Textinhalte der Briefe wurden automatisch von [https://schnitzler-mikrofilme.acdh.oeaw.ac.at](https://schnitzler-mikrofilme.acdh.oeaw.ac.at) bezogen. Hierfür wurde das Skript `add_ocr_text.py` verwendet.
