# %%
import json
import multiprocessing
import pathlib
from typing import List

import pandas as pd


# %%
def get(data: dict, path: str | List[str]):
    if type(path) is not list:
        path = path.split(".")
    for property in path:
        if type(data) is not dict or property not in data:
            return None
        data = data[property]
    return data


# %%
def get_data(paths: List[pathlib.Path]):
    data_list = []
    for path in paths:
        try:
            with open(path, encoding="UTF-8") as file:
                data = json.load(file)
            filtered_data = {}
            filtered_data["filename"] = path.name
            filtered_data["article_title"] = get(
                data, ["abstracts-retrieval-response", "coredata", "dc:title"]
            )
            filtered_data["publish_title"] = get(
                data,
                ["abstracts-retrieval-response", "coredata", "prism:publicationName"],
            )
            filtered_data["abstract"] = get(
                data,
                [
                    "abstracts-retrieval-response",
                    "item",
                    "bibrecord",
                    "head",
                    "abstracts",
                ],
            )
            filtered_data["publish_date"] = get(
                data, ["abstracts-retrieval-response", "coredata", "prism:coverDate"]
            )
            filtered_data["cited_by_count"] = get(
                data, ["abstracts-retrieval-response", "coredata", "citedby-count"]
            )
            filtered_data["reference_count"] = get(
                data,
                [
                    "abstracts-retrieval-response",
                    "item",
                    "bibrecord",
                    "tail",
                    "bibliography",
                    "@refcount",
                ],
            )
            data_list.append(filtered_data)
            print(f"Success: {path}")
        except Exception as e:
            print(f"Failed: {path}\nException: {repr(e)}")
    return data_list


# %%
data_list = []

# %%
buckets = {i: [] for i in range(12)}

for path in pathlib.Path("Data 2018-2023/Project").glob("**/*"):
    buckets[path.__hash__() % 12].append(path)

# %%
if __name__ == "__main__":
    cpu_count = multiprocessing.cpu_count()
    with multiprocessing.Pool(cpu_count) as p:
        for data_part in p.map(get_data, buckets.values()):
            data_list += data_part
    df = pd.DataFrame(data_list)
    df["publish_date"] = pd.to_datetime(df["publish_date"])
    df.to_csv("papers.csv")
