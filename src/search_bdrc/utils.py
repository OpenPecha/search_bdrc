import json
from pathlib import Path


def write_json(data: dict, output_path: str | Path):
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
