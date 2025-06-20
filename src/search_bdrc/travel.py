import json
from pathlib import Path

import requests
from rdflib import Graph


def write_json(data: dict, output_path: str | Path):
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def extract_workhasinstance(g) -> list[str]:
    works = []

    for subj, pred, obj in g:
        if str(pred) == "http://purl.bdrc.io/ontology/core/workHasInstance":
            work_link = str(obj)
            work = work_link.split("/")[-1]
            works.append(work)

    return works


def get_work_metadata(work_id: str):
    url = f"https://purl.bdrc.io/resource/{work_id}.ttl"
    headers = {"Accept": "text/turtle"}  # Requesting Turtle format
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.text
        g = Graph()
        g.parse(data=data, format="turtle")

        works = extract_workhasinstance(g)
        return works
    else:
        print(f"Failed to retrieve data: {response.status_code}")


if __name__ == "__main__":
    work_id = "WA0RK0529"
    related_works = get_work_metadata(work_id)

    write_json(related_works, "res.json")
    print(related_works)
