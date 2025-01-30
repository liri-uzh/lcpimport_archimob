from json import dumps
from lxml import etree
from uuid import uuid4
import io
import os
import wave
from tqdm import tqdm
import shutil

DOC_DB_FILE = "./meta/Metadata.txt"
PERSON_DB_FILE = "./meta/person_file.xml"
FOLDER = "./docs/"
AUDIO_FOLDER = "./audio/"

SENTENCE_TAG = "u"
TOKEN_TAG = "w"
TOKEN_ATTRIBUTES = {"lemma": "normalised", "xpos": "tag"}

ANNOTATION_TAGS = ("incident", "pause", "gap")
NON_ANNOTATION_TAGS = ("vocal", "del")

char_cursor = 1
token_id = 1
annotation_id = 1
document_id = 1
audio_cursor = 1

person_db: dict[str, dict] = {}
doc_db: dict[str, dict] = {}
token_forms: dict[str, int] = {}
token_lemmas: dict[str, int] = {}

skip_doc_cols = ("Year of birth", "Sex", "Profession")

json_template: dict[str, dict] = {
    "meta": {
        "name": "ArchiMob Release 2 (2019) (Version 1.0.0)",
        "author": "Scherrer, Y., Samardzic, T., & Glaser, E.",
        "url": "https://doi.org/10.48656/496p-3w34",
        "date": "2023",
        "version": 1,
        "corpusDescription": "The ArchiMob corpus represents German linguistic varieties spoken within the territory of Switzerland. This corpus is the first electronic resource containing long samples of transcribed text in Swiss German, intended for studying the spatial distribution of morphosyntactic features and for natural language processing.",
        "mediaSlots": {"audio": {"mediaType": "audio", "isOptional": True}},
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
                    "values": ["0", "1"],
                    "nullable": False,
                },
                "truncated": {
                    "isGlobal": False,
                    "type": "categorical",
                    "values": ["0", "1"],
                    "nullable": False,
                },
                "vocal": {
                    "isGlobal": False,
                    "type": "categorical",
                    "values": ["0", "1"],
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
                "meta": {"audio_file": {"type": "text", "nullable": False}},
            },
        },
        "Document": {
            "abstract": False,
            "contains": "Segment",
            "layerType": "span",
            "attributes": {
                "id": {
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
        "Annotation": {
            "abstract": False,
            "layerType": "unit",
            "anchoring": {"location": False, "stream": True, "time": False},
            "attributes": {
                "type": {
                    "isGlobal": False,
                    "type": "categorical",
                    "values": [],
                    "nullable": False,
                },
                "meta": {"reason": {"type": "text"}, "description": {"type": "text"}},
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


def get_audio_length(filename, seconds_to_integers=True):
    """
    Get the length of an audio file in frames, assuming there are 25 frames per second.
    if `seconds_to_integers==True`, seconds get expressed as integers, otherwise as floats.
    """
    with wave.open(filename, "rb") as audio:
        seconds = audio.getnframes() / audio.getframerate()

        return int(seconds * 25) if seconds_to_integers else seconds * 25


def concatenate_audio_files(folder_path, output_path):
    """Concatenates several audio files into one audio file using Python's built-in wav module
    and save it to `output_path`. Note that extension (wav) must be added to `output_path`
    """
    audio_clip_paths = os.listdir(folder_path)
    data = []
    for clip_name in audio_clip_paths:
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
        while line := metadata.readline():
            values = [x.strip() for x in line.split("\t")]
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
        forms.write("\t".join(["form_id", "form"]))
        lemmas.write("\t".join(["lemma_id", "lemma"]))
        for form, i in token_forms.items():
            forms.write("\n" + "\t".join([str(i), form]))
        for lemma, i in token_lemmas.items():
            lemmas.write("\n" + "\t".join([str(i), lemma]))


def write_speakers():
    header = ["who_id", "who"]
    with open("./output/global_attribute_who.csv", "w", encoding="utf-8") as speakers:
        speakers.write("\t".join(header))
        for speaker_id, props in person_db.items():
            json_string = (
                "{"
                + ",".join(
                    f'"{prop_name}": "{prop_value}"'
                    for prop_name, prop_value in props.items()
                )
                + "}"
            )
            speakers.write("\n" + "\t".join([speaker_id, json_string]))


def parse_file(input_file, doc_name):

    global char_cursor
    global token_id
    global document_id
    global annotation_id
    global person_db
    global audio_cursor

    start_char_doc = char_cursor
    start_audio_doc = audio_cursor

    doc_audio_folder = input_file.removesuffix(".xml").split("/")[
        -1
    ]  # Get the name of the audio folder
    audio_doc = f"{AUDIO_FOLDER}{doc_audio_folder}"  # the whole folder corresponds to one document

    doc_media_name = f"{doc_audio_folder}.wav"
    if not os.path.exists("./media"):
        os.makedirs("./media")
    concatenate_audio_files(audio_doc, f"./media/{doc_media_name}")

    tree = etree.parse(io.BytesIO(open(input_file, "rb").read()))  # type: ignore
    root = tree.getroot()

    doc_title = root.xpath(f".//*[local-name()='title']")[0].text

    with open("./output/document.csv", "a", encoding="utf-8") as doc_output, open(
        "./output/segment.csv", "a", encoding="utf-8"
    ) as seg_output, open(
        "./output/fts_vector.csv", "a", encoding="utf-8"
    ) as fts_output, open(
        "./output/token.csv", "a", encoding="utf-8"
    ) as tok_output:
        for seg in root.xpath(f".//*[local-name()='{SENTENCE_TAG}']"):
            seg_id = str(uuid4())
            token_vector = []
            start_char_seg = char_cursor
            start_audio_tok = audio_cursor
            start_audio_seg = audio_cursor
            audio_name = seg.get("start").split("#")[1].replace("-", "_") + ".wav"

            # these two specific audio folders deviate from the naming convention
            if "d1082_2_TLI" in audio_name:
                audio_name.replace("d1082_2_TLI", "1082_2d1082_2_TLI")
            elif "d1082_3_TLI" in audio_name:
                audio_name.replace("d1082_3_TLI", "1082_3d1082_3_TLI")

            ### NOTE: if the audio file is not found, the audio length is set to 0
            try:
                segment_audio_length = get_audio_length(f"{audio_doc}/{audio_name}")
            except FileNotFoundError:
                segment_audio_length = 0

            # Having a null length would produce empty frame ranges,
            # which messes with SQL queries down the road
            if segment_audio_length < 1:
                segment_audio_length = 1

            audio_cursor += segment_audio_length

            segment_char_length = 0  # Calculate total character length of the segment

            for x in seg.getchildren():
                tag = x.tag.split("}")[-1]
                unclear = tag == "unclear"
                truncated = tag == "del"
                vocal = tag == "vocal"
                if unclear:
                    tag = TOKEN_TAG
                    x = x.getchildren()[0]
                if tag == TOKEN_TAG or tag in NON_ANNOTATION_TAGS:
                    form = (x.text or "").strip()
                    segment_char_length += len(form)

            # Calculate frame-to-character ratio for the segment
            frame_per_char_ratio = (
                segment_audio_length / segment_char_length
                if segment_char_length > 0
                else 0
            )

            for x in seg.getchildren():
                start_char_tok = char_cursor
                tag = x.tag.split("}")[-1]
                unclear = tag == "unclear"
                truncated = tag == "del"
                vocal = tag == "vocal"
                if unclear:
                    tag = TOKEN_TAG
                    x = x.getchildren()[0]
                if tag == TOKEN_TAG or tag in NON_ANNOTATION_TAGS:
                    form = (x.text or "").strip()
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

                    # Calculate frame length for the token
                    token_frame_length = int(round(len(form) * frame_per_char_ratio, 0))
                    if token_frame_length < 1:
                        token_frame_length = 1

                    tok_output.write(
                        "\n"
                        + "\t".join(
                            [
                                str(token_id),
                                str(form_id),
                                str(lemma_id),
                                str(xpos),
                                "1" if unclear else "0",
                                "1" if truncated else "0",
                                "1" if vocal else "0",
                                f"[{start_char_tok},{char_cursor})",
                                seg_id,
                                f"[{start_audio_tok},{start_audio_tok + token_frame_length})",
                            ]
                        )
                    )
                    start_audio_tok += token_frame_length  # Update audio position
                    token_vector.append((form, lemma, xpos))
                    token_id += 1
                    char_cursor += 1
                elif tag in ANNOTATION_TAGS:
                    with open(
                        "./output/annotation.csv", "a", encoding="utf-8"
                    ) as ann_output:
                        aid = str(annotation_id)
                        annotation_id += 1
                        meta = "{}"
                        if tag == "gap":
                            meta = '{"reason": "' + x.get("reason") + '"}'
                        if tag == "incident":
                            desc = x.xpath(".//*[local-name()='desc']")[0]
                            meta = '{"description": "' + desc.text + '"}'
                        ann_output.write(
                            "\n"
                            + "\t".join(
                                [aid, tag, meta, f"[{char_cursor},{char_cursor+1})"]
                            )
                        )
                        char_cursor += 1
                        if (
                            tag
                            not in json_template["layer"]["Annotation"]["attributes"][
                                "type"
                            ]["values"]
                        ):
                            json_template["layer"]["Annotation"]["attributes"]["type"][
                                "values"
                            ].append(tag)
                else:
                    pass
            who = seg.get("who").removeprefix("person_db#").strip()
            if who not in person_db:
                person_db[who] = {}
            if char_cursor - start_char_seg < 2:
                char_cursor += 2
            seg_output.write(
                "\n"
                + "\t".join(
                    [
                        seg_id,
                        who,
                        f"[{start_char_seg},{char_cursor-1})",
                        f"[{start_audio_seg},{audio_cursor})",
                        '{"audio_file": "' + audio_name + '"}',
                    ]
                )
            )
            vector = " ".join(
                f"'1{f}':{n} '2{l}':{n} '3{x}':{n}"
                for n, (f, l, x) in enumerate(token_vector, start=1)
            )
            fts_output.write("\n" + "\t".join([seg_id, vector]))

        doc_char_range = f"[{start_char_doc},{char_cursor})"
        doc_frame_range = f"[{start_audio_doc},{audio_cursor})"
        doc_output.write(
            "\n"
            + "\t".join(
                [
                    str(document_id),
                    doc_title,
                    *doc_db[doc_name].values(),
                    doc_char_range,
                    doc_frame_range,
                    '{"audio": "' + doc_media_name + '"}',
                ]
            )
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
            if attribute_name == "id":
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
        "./output/annotation.csv", "w", encoding="utf-8"
    ) as ann_output:
        doc_output.write(
            "\t".join(
                [
                    "document_id",
                    "name",
                    *[
                        ("who_id" if k == "SpeakerID" else k.replace(" ", "_").lower())
                        for k in next(x for x in doc_db.values()).keys()
                    ],
                    "char_range",
                    "frame_range",
                    "media",
                ]
            )
        )
        seg_output.write(
            "\t".join(["segment_id", "who_id", "char_range", "frame_range", "meta"])
        )
        fts_output.write("\t".join(["segment_id", "vector"]))
        tok_output.write(
            "\t".join(
                [
                    "token_id",
                    "form_id",
                    "lemma_id",
                    "xpos",
                    "unclear",
                    "truncated",
                    "vocal",
                    "char_range",
                    "segment_id",
                    "frame_range",
                ]
            )
        )
        ann_output.write("\t".join(["annotation_id", "type", "meta", "char_range"]))

    for file in tqdm(os.listdir(FOLDER)):
        if not file.endswith(".xml"):
            continue
        doc_name = file.removesuffix(".xml").split("_")[0]
        parse_file(FOLDER + file, doc_name=doc_name)
    write_forms_lemmas()
    write_speakers()
    with open("./output/meta.json", "w", encoding="utf-8") as json_file:
        json_file.write(dumps(json_template, indent="\t"))


run()
