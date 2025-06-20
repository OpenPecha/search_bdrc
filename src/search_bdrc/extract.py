import json
import re
from pathlib import Path


def write_json(data: dict | list, output_path: str | Path):
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def extract_instances_id(text: str) -> list[str]:
    regex = r"<a\shref=\"/show/bdr:([A-Z0-9_]+)\?"

    ids = re.findall(regex, text)

    # Remove duplicate
    ids = list(set(ids))
    return ids


if __name__ == "__main__":

    for page_no in range(1, 45):
        input_path = f"ཤེས་རབ་ཀྱི་ཕ་རོལ་ཏུ་ཕྱིན་པའི་སྙིང་པོ།/{page_no}.txt"
        output_path = f"ཤེས་རབ་ཀྱི་ཕ་རོལ་ཏུ་ཕྱིན་པའི་སྙིང་པོ།_jsons/{page_no}.json"

        text = Path(input_path).read_text(encoding="utf-8")
        ids = extract_instances_id(text)

        if len(ids) != 20:
            print(f"Page no {page_no} has {len(ids)} ids.")

        write_json(ids, output_path)
