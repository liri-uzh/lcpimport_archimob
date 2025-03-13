# Input files

 - in `docs`: the XML/TEI files
 - in `meta`: Metadata.txt for info on the docs, person_file.xml for info on the people
 - in `audio`: the audio files referenced in the `start` attribute of `u` in the XML/TEI files

# Output files

Run

```python
from tei_to_tables import run
run()
```