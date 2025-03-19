import csv
import io
import os
import wave

from collections import defaultdict
from json import dumps
from lxml import etree
from tqdm import tqdm
from uuid import uuid4

json_template: dict[str, dict] = {
    "meta": {
        "name": "ArchiMob Release 2 (2019) (Version 1.0.0)",
        "authors": "Scherrer, Y., Samardzic, T., & Glaser, E.",
        "url": "https://doi.org/10.48656/496p-3w34",
        "date": "2023",
        "version": 1,
        "revision": "1.0",
        "license": "cc-by-nc-sa",
        "corpusDescription": "The ArchiMob corpus represents German linguistic varieties spoken within the territory of Switzerland. This corpus is the first electronic resource containing long samples of transcribed text in Swiss German, intended for studying the spatial distribution of morphosyntactic features and for natural language processing.",
        "mediaSlots": {"audio": {"mediaType": "audio", "isOptional": True}},
        "sample_query": 'Segment s\n    # Restrict the search to segments that have an audio file\n    file_present = "yes"\n\nIncident@s i\n    # The segment should overlap (operator @) with an incident\n    # whose description contains "auf den"\n    description = /auf den/\n\n# The segment should contain a sequence of 2 tokens\nsequence@s npause\n    Token\n        # The first token in the sequence should be a noun\n        xpos = "NN"\n    Token\n        # The next token should be preceded by a pause\n        pause_before = "yes"\n\n# Show that sequence in the context of its segment\nres => plain\n    context\n        s\n    entities\n        npause\n\n# Show which dialects the speakers of the hits use\nstats => analysis\n    attributes\n        s.who.dialect\n    functions\n        frequency',
    },
    "firstClass": {"document": "Document", "segment": "Segment", "token": "Token"},
    "layer": {
        "Token": {
            "abstract": False,
            "layerType": "unit",
            "anchoring": {"location": False, "stream": True, "time": True},
            "attributes": {
                "form": {"isGlobal": False, "type": "text", "nullable": False},
                "lemma": {"isGlobal": False, "type": "text", "nullable": False},
                "xpos": {
                    "isGlobal": False,
                    "type": "categorical",
                    "values": [],
                    "nullable": True,
                },
                "unclear": {
                    "isGlobal": False,
                    "type": "categorical",
                    "values": ["no", "yes"],
                    "nullable": False,
                },
                "truncated": {
                    "isGlobal": False,
                    "type": "categorical",
                    "values": ["no", "yes"],
                    "nullable": False,
                },
                "vocal": {
                    "isGlobal": False,
                    "type": "categorical",
                    "values": ["no", "yes"],
                    "nullable": False,
                },
                "pause_before": {
                    "isGlobal": False,
                    "type": "categorical",
                    "values": ["no", "yes"],
                    "nullable": False,
                },
                "pause_after": {
                    "isGlobal": False,
                    "type": "categorical",
                    "values": ["no", "yes"],
                    "nullable": False,
                },
                "unintelligible": {
                    "isGlobal": False,
                    "type": "categorical",
                    "values": ["no", "yes"],
                    "nullable": False,
                },
            },
        },
        "Segment": {
            "abstract": False,
            "layerType": "span",
            "contains": "Token",
            "attributes": {
                "who": {"ref": "who"},
                "meta": {
                    "audio_file": {"type": "text", "nullable": False},
                    "file_present": {
                        "type": "categorical",
                        "values": ["yes", "no"],
                        "nullable": False,
                    },
                },
            },
        },
        "Document": {
            "abstract": False,
            "contains": "Segment",
            "layerType": "span",
            "attributes": {
                "title": {
                    "isGlobal": False,
                    "type": "categorical",
                    "values": [],
                    "nullable": False,
                },
                "transcriptor": {
                    "isGlobal": False,
                    "type": "categorical",
                    "values": [],
                    "nullable": True,
                },
                "tool": {
                    "isGlobal": False,
                    "type": "categorical",
                    "values": [],
                    "nullable": True,
                },
                "transcription_phase": {
                    "isGlobal": False,
                    "type": "categorical",
                    "values": [],
                    "nullable": True,
                },
                "normalisation": {
                    "isGlobal": False,
                    "type": "categorical",
                    "values": [],
                    "nullable": True,
                },
                "who": {"ref": "who"},
            },
        },
        "Incident": {
            "abstract": False,
            "contains": "Token",  # Not exactly true, but will make it appear in the details tab
            "layerType": "unit",
            "anchoring": {"location": False, "stream": True, "time": False},
            "attributes": {
                "meta": {"description": {"type": "text"}},
            },
        },
    },
    "globalAttributes": {
        "who": {
            "type": "dict",
            "keys": {
                "sex": {"type": "text"},
                "birth": {"type": "date"},
                "occupation": {"type": "text"},
                "residence": {"type": "text"},
                "dialect": {"type": "text"},
            },
        }
    },
    "tracks": {"layers": {"Segment": {"split": ["who"]}}},
}

DOC_DB_FILE = "./meta/Metadata.txt"
PERSON_DB_FILE = "./meta/person_file.xml"
FOLDER = "./docs/"
AUDIO_FOLDER = "./audio/"

SENTENCE_TAG = "u"
TOKEN_TAG = "w"
TOKEN_ATTRIBUTES = {"lemma": "normalised", "xpos": "tag"}

ANNOTATION_TAGS = ("incident", "pause")
NON_ANNOTATION_TAGS = ("vocal", "del", "gap")

char_cursor = 1
token_id = 1
incident_id = 1
document_id = 1
audio_cursor = 1

person_db: dict[str, dict] = {}
doc_db: dict[str, dict] = {}
token_forms: dict[str, int] = {}
token_lemmas: dict[str, int] = {}

skip_doc_cols = ("Year of birth", "Sex", "Profession")


def esc_fts(value: str) -> str | int:
    if isinstance(value, int):
        return value
    return value.replace("'", "''").replace("\\", "\\\\")


def parse_range(range_str: str) -> tuple[int, int]:
    return tuple(map(int, range_str.strip("[]()").split(",")))  # type: ignore


def to_range(lower: str | int, upper: str | int) -> str:
    return f"[{str(lower)},{str(upper)})"


def get_audio_length(filename):
    """
    Get the length of an audio file in frames, assuming there are 25 frames per second.
    if `frames_to_integers==True`, seconds get expressed as integers, otherwise as floats.
    """
    with wave.open(filename, "rb") as audio:
        seconds = audio.getnframes() / audio.getframerate()
        # frames = seconds * 25

        return seconds


def seconds_to_frame_range(start_cursor, end_cursor):
    start_frame = int(round(start_cursor * 25, 0))
    end_frame = int(round(end_cursor * 25, 0))
    if end_frame <= start_frame:
        end_frame = start_frame + 1
    return to_range(start_frame, end_frame)


def concatenate_audio_files(folder_path, output_path, processed_segs=[]):
    """
    Concatenates several audio files into one audio file using Python's built-in wav module\n
    and save it to `output_path`. Note that extension (wav) must be added to `output_path`.\n
    If `processed_segs` is provided, it will skip the files in the list; some docs have more audios than segments in doc.\n
    """
    data = []
    for clip_name in processed_segs:
        clip = f"{folder_path}/{clip_name}"
        if not os.path.isfile(clip):
            continue
        w = wave.open(clip, "rb")
        data.append([w.getparams(), w.readframes(w.getnframes())])
        w.close()
    output = wave.open(output_path, "wb")
    output.setparams(data[0][0])
    for i in range(len(data)):
        output.writeframes(data[i][1])
    output.close()


def load_docs(input_file):
    header = []
    with open(input_file, "r") as metadata:
        reader = csv.reader(metadata, delimiter="\t")
        for values in reader:
            if not header:
                header = values
                continue
            cols = {
                header[n]: x
                for n, x in enumerate(values)
                if header[n] not in skip_doc_cols
            }
            person_db[cols.get("SpeakerID", "")]["dialect"] = cols.pop("Dialect area")
            doc_db[cols.pop("DocID")] = cols


def load_people(input_file):
    tree = etree.parse(io.BytesIO(open(input_file, "rb").read()))  # type: ignore
    root = tree.getroot()
    people = root.findall(".//person")
    for person in people:
        id, sex = person.values()
        id = id.strip()
        person_db[id] = {"sex": sex.strip()}
        for x in person.getchildren():
            unclear = x.find("unclear")
            person_db[id][x.tag] = unclear.text if unclear is not None else x.text


def write_forms_lemmas():
    with open("./output/token_form.csv", "w", encoding="utf-8") as forms, open(
        "./output/token_lemma.csv", "w", encoding="utf-8"
    ) as lemmas:
        forms_csv = csv.writer(forms)
        lemmas_csv = csv.writer(lemmas)
        forms_csv.writerow(["form_id", "form"])
        lemmas_csv.writerow(["lemma_id", "lemma"])
        for form, i in token_forms.items():
            forms_csv.writerow([str(i), form])
        for lemma, i in token_lemmas.items():
            lemmas_csv.writerow([str(i), lemma])


def write_speakers():
    header = ["who_id", "who"]
    with open("./output/global_attribute_who.csv", "w", encoding="utf-8") as speakers:
        speakers_csv = csv.writer(speakers)
        speakers_csv.writerow(header)
        for speaker_id, props in person_db.items():
            speakers_csv.writerow([speaker_id, dumps(props)])


def parse_file(input_file, doc_name):

    global char_cursor
    global token_id
    global document_id
    global incident_id
    global person_db
    global audio_cursor

    start_char_doc = char_cursor
    start_audio_doc = audio_cursor

    doc_audio_folder = input_file.removesuffix(".xml").split("/")[
        -1
    ]  # Get the name of the audio folder
    audio_doc = f"{AUDIO_FOLDER}{doc_audio_folder}"  # the whole folder corresponds to one document

    audio_frame_dict = defaultdict(lambda: defaultdict(str))

    doc_media_name = f"{doc_audio_folder}.wav"
    if not os.path.exists("./output/media"):
        os.makedirs("./output/media")

    tree = etree.parse(io.BytesIO(open(input_file, "rb").read()))  # type: ignore
    root = tree.getroot()

    doc_title = root.xpath(f".//*[local-name()='title']")[0].text

    processed_segs = []

    segs_xpath = f".//*[local-name()='{SENTENCE_TAG}']"

    with open("./output/document.csv", "a", encoding="utf-8") as doc_output, open(
        "./output/segment.csv", "a", encoding="utf-8"
    ) as seg_output, open(
        "./output/fts_vector.csv", "a", encoding="utf-8"
    ) as fts_output, open(
        "./output/token.csv", "a", encoding="utf-8"
    ) as tok_output:

        doc_csv = csv.writer(doc_output)
        seg_csv = csv.writer(seg_output)
        fts_csv = csv.writer(fts_output)
        tok_csv = csv.writer(tok_output)

        for seg in root.xpath(
            segs_xpath
        ):  # TODO: sort root.xpath(segs_xpath) by audio_name
            seg_id = str(uuid4())
            token_vector = []
            start_char_seg = char_cursor
            start_audio_tok = audio_cursor
            start_audio_seg = audio_cursor
            audio_name = seg.get("start").split("#")[1].replace("-", "_") + ".wav"

            # these two specific audio folders deviate from the naming convention
            if "d1082_2_TLI" in audio_name:
                audio_name = audio_name.replace("d1082_2_TLI", "1082_2d1082_2_TLI")
            elif "d1082_3_TLI" in audio_name:
                audio_name = audio_name.replace("d1082_3_TLI", "1082_3d1082_3_TLI")

            ### NOTE: if the audio file is not found, the audio length is set to 0
            if audio_name not in processed_segs:
                try:
                    segment_audio_length = get_audio_length(f"{audio_doc}/{audio_name}")
                    processed_segs.append(audio_name)
                except FileNotFoundError:
                    segment_audio_length = 0
            else:
                segment_audio_length = 0

            audio_cursor += segment_audio_length

            segment_char_length = 0  # Calculate total character length of the segment

            seg_children = seg.getchildren()
            # First pass: compute segment_char_length
            for x in seg_children:
                tag = x.tag.split("}")[-1]
                unclear = tag == "unclear"
                truncated = tag == "del"
                vocal = tag == "vocal"
                if unclear:
                    tag = TOKEN_TAG
                    x = x.getchildren()[0]
                if tag == TOKEN_TAG or tag in NON_ANNOTATION_TAGS:
                    x_children = x.getchildren()
                    form = ((x_children[0] if x_children else x).text or "").strip()
                    segment_char_length += len(form)

            # Second pass: actually process the nodes
            for n, x in enumerate(seg_children):
                start_char_tok = char_cursor
                tag = x.tag.split("}")[-1]
                unclear = "yes" if tag == "unclear" else "no"
                truncated = "yes" if tag == "del" else "no"
                vocal = "yes" if tag == "vocal" else "no"
                x_children = x.getchildren()
                if unclear == "yes":
                    tag = TOKEN_TAG
                    x = x_children[0]
                if tag == TOKEN_TAG or tag in NON_ANNOTATION_TAGS:
                    form = ((x_children[0] if x_children else x).text or "").strip()
                    lemma = x.get(TOKEN_ATTRIBUTES["lemma"], "").strip()
                    xpos = x.get(TOKEN_ATTRIBUTES["xpos"], "").strip()
                    if (
                        xpos
                        and xpos
                        not in json_template["layer"]["Token"]["attributes"]["xpos"][
                            "values"
                        ]
                    ):
                        json_template["layer"]["Token"]["attributes"]["xpos"][
                            "values"
                        ].append(xpos)
                    form_id = token_forms.get(form, len(token_forms) + 1)
                    token_forms[form] = form_id
                    lemma_id = token_lemmas.get(lemma, len(token_lemmas) + 1)
                    token_lemmas[lemma] = lemma_id
                    char_cursor += max(len(form) - 1, 1)

                    # token_frame_length = len(form) * math.ceil(frame_per_char_ratio)

                    if start_audio_tok > audio_cursor:
                        raise ValueError(
                            f"Audio cursor is less than start_audio_tok\n {audio_cursor} < {start_audio_tok}"
                        )

                    if audio_name not in audio_frame_dict["tokens"].keys():
                        audio_frame_range = seconds_to_frame_range(
                            start_audio_tok, audio_cursor
                        )
                        audio_frame_dict["tokens"][audio_name] = audio_frame_range
                    else:
                        audio_frame_range = audio_frame_dict["tokens"][audio_name]

                    pauseBefore = (
                        "yes"
                        if (n > 0 and seg_children[n - 1].tag.split("}")[-1] == "pause")
                        else "no"
                    )
                    pauseAfter = (
                        "yes"
                        if (
                            n + 1 < len(seg_children)
                            and seg_children[n + 1].tag.split("}")[-1] == "pause"
                        )
                        else "no"
                    )
                    unintelligible = "yes" if tag == "gap" else "no"

                    tok_csv.writerow(
                        [
                            token_id,
                            form_id,
                            lemma_id,
                            xpos,
                            unclear,
                            truncated,
                            vocal,
                            pauseBefore,
                            pauseAfter,
                            unintelligible,
                            to_range(start_char_tok, char_cursor),
                            seg_id,
                            audio_frame_range,
                        ]
                    )
                    start_audio_tok = audio_cursor
                    token_vector.append(
                        (
                            form,
                            lemma,
                            xpos,
                            unclear,
                            truncated,
                            vocal,
                            pauseBefore,
                            pauseAfter,
                            unintelligible,
                        )
                    )
                    token_id += 1
                    char_cursor += 1

                elif tag in ANNOTATION_TAGS:
                    if tag != "incident":
                        continue  # We used to have multiple types of annotation, now they're only "incidents"
                    with open(
                        "./output/incident.csv", "a", encoding="utf-8"
                    ) as ann_output:
                        aid = str(incident_id)
                        incident_id += 1
                        meta = "{}"
                        if tag == "gap":
                            meta = '{"reason": "' + x.get("reason") + '"}'
                        if tag == "incident":
                            desc = x.xpath(".//*[local-name()='desc']")[0]
                            meta = '{"description": "' + desc.text + '"}'
                        csv.writer(ann_output).writerow(
                            [aid, meta, to_range(char_cursor, char_cursor + 1)]
                        )
                else:
                    pass
            start_audio_tok = audio_cursor
            who = seg.get("who").removeprefix("person_db#").strip()
            if who not in person_db:
                person_db[who] = {}
            if char_cursor - start_char_seg < 2:
                char_cursor += 2

            if seg == root.xpath(segs_xpath)[-1]:
                concatenate_audio_files(
                    audio_doc, f"./output/media/{doc_media_name}", processed_segs
                )
                doc_frame_len = get_audio_length(f"./output/media/{doc_media_name}")

                assert round(doc_frame_len, 0) == round(
                    audio_cursor - start_audio_doc, 0
                ), f"Audio length mismatch: {doc_frame_len} != {audio_cursor - start_audio_doc}"

            if audio_name not in audio_frame_dict["segments"].keys():
                audio_frame_range = seconds_to_frame_range(
                    start_audio_seg, audio_cursor
                )
                audio_frame_dict["segments"][audio_name] = audio_frame_range
            else:
                audio_frame_range = audio_frame_dict["segments"][audio_name]

            file_present = "yes" if audio_name in processed_segs else "no"
            audio_meta_json = (
                '{"audio_file": "'
                + audio_name
                + '", '
                + '"file_present": "'
                + file_present
                + '"}'
            )
            seg_csv.writerow(
                [
                    seg_id,
                    who,
                    to_range(start_char_seg, char_cursor - 1),
                    audio_frame_range,
                    audio_meta_json,
                ]
            )
            # TODO: include all 9 token attributes
            vector = " ".join(
                " ".join(
                    [f"'{i}{esc_fts(x)}':{n}" for i, x in enumerate(vector, start=1)]
                )
                for n, vector in enumerate(token_vector, start=1)
            )
            fts_csv.writerow([seg_id, vector])

        doc_char_range = to_range(start_char_doc, char_cursor)

        doc_frame_range = seconds_to_frame_range(start_audio_doc, audio_cursor)

        doc_csv.writerow(
            [
                document_id,
                doc_title,  # retrieved from the <title> node
                doc_name,  # filename without .xml
                *doc_db[doc_name].values(),
                doc_char_range,
                doc_frame_range,
                '{"audio": "' + doc_media_name + '"}',
            ]
        )
        document_id += 1
        for attribute_name, attribute_props in json_template["layer"]["Document"][
            "attributes"
        ].items():
            if attribute_props.get("type") != "categorical":
                continue
            aname = attribute_name.replace("_", " ").lower()
            aname = aname[0].upper() + aname[1:]
            value = doc_db[doc_name].get(aname)
            if attribute_name == "title":
                value = doc_title
            if attribute_name == "name":
                value = doc_name
            if (
                value
                and value
                not in json_template["layer"]["Document"]["attributes"][attribute_name][
                    "values"
                ]
            ):
                json_template["layer"]["Document"]["attributes"][attribute_name][
                    "values"
                ].append(value)


def run():
    load_people(PERSON_DB_FILE)
    load_docs(DOC_DB_FILE)
    with open("./output/document.csv", "w", encoding="utf-8") as doc_output, open(
        "./output/segment.csv", "w", encoding="utf-8"
    ) as seg_output, open(
        "./output/fts_vector.csv", "w", encoding="utf-8"
    ) as fts_output, open(
        "./output/token.csv", "w", encoding="utf-8"
    ) as tok_output, open(
        "./output/incident.csv", "w", encoding="utf-8"
    ) as ann_output:

        doc_csv = csv.writer(doc_output)
        seg_csv = csv.writer(seg_output)
        fts_csv = csv.writer(fts_output)
        tok_csv = csv.writer(tok_output)
        ann_csv = csv.writer(ann_output)

        doc_csv.writerow(
            [
                "document_id",
                "title",  # corresponds to <title> in the document
                "name",  # filename without .xml
                *[
                    ("who_id" if k == "SpeakerID" else k.replace(" ", "_").lower())
                    for k in next(x for x in doc_db.values()).keys()
                ],
                "char_range",
                "frame_range",
                "media",
            ]
        )
        seg_csv.writerow(["segment_id", "who_id", "char_range", "frame_range", "meta"])
        fts_csv.writerow(["segment_id", "vector"])
        tok_csv.writerow(
            [
                "token_id",
                "form_id",
                "lemma_id",
                "xpos",
                "unclear",
                "truncated",
                "vocal",
                "pause_before",  # do not use camelCase: postgres only supports it when quoted
                "pause_after",  # do not use camelCase: postgres only supports it when quoted
                "unintelligible",
                "char_range",
                "segment_id",
                "frame_range",
            ]
        )
        ann_csv.writerow(["incident_id", "meta", "char_range"])

    for file in tqdm(os.listdir(FOLDER)):
        if not file.endswith(".xml"):
            continue
        doc_name = file.removesuffix(".xml").split("_")[0]
        try:
            parse_file(FOLDER + file, doc_name=doc_name)
        except Exception as e:
            # for now it should never be triggered, as I excluded problematic files
            print(f"Error processing file {file}: {e}")
            continue
    write_forms_lemmas()
    write_speakers()
    with open("./output/meta.json", "w", encoding="utf-8") as json_file:
        json_file.write(dumps(json_template, indent="\t"))
