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
def ensure_list_data(x):
    return x if isinstance(x, list) else [x]


# %%
def get_data_list(paths: List[pathlib.Path]):
    data_list = []
    for path in paths:
        try:
            with open(path, encoding="UTF-8") as file:
                data = json.load(file)
            filtered_data = {}
            filtered_data["id"] = get(
                data, ["abstracts-retrieval-response", "coredata", "eid"]
            )
            filtered_data["title"] = get(
                data, ["abstracts-retrieval-response", "coredata", "dc:title"]
            )
            filtered_data["publication_name"] = get(
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
            filtered_data["classification_codes"] = get(
                data, ["abstracts-retrieval-response", "subject-areas", "subject-area"]
            )
            filtered_data["affiliations"] = get(
                data, ["abstracts-retrieval-response", "affiliation"]
            )
            filtered_data["references"] = get(
                data,
                [
                    "abstracts-retrieval-response",
                    "item",
                    "bibrecord",
                    "tail",
                    "bibliography",
                    "reference",
                ],
            )
            filtered_data["keywords"] = get(
                data, ["abstracts-retrieval-response", "authkeywords", "author-keyword"]
            )
            data_list.append(filtered_data)
        except Exception as e:
            print(f"Failed: {path}\nException: {repr(e)}")
    print("Reading files done.")

    return data_list


# %%
def get_df(data_list):
    df = pd.DataFrame(data_list)

    df["publish_date"] = pd.to_datetime(df["publish_date"])

    list_columns = df.loc[
        :, ["classification_codes", "affiliations", "references", "keywords"]
    ]
    list_columns = list_columns.apply(ensure_list_data)

    return df


# # %%
# def get_papers_df(df):
#     papers_df = df.drop(
#         columns=["classification_codes", "affiliations", "references", "keywords"]
#     )
#     return papers_df


# def get_classification_codes_df(df):
#     classification_codes_df = df.explode("classification_codes", ignore_index=True)[
#         ["id", "classification_codes"]
#     ]
#     classification_codes_df.rename(
#         columns={"id": "paper_id", "classification_codes": "classification_code"},
#         inplace=True,
#     )
#     classification_codes_df = classification_codes_df[["paper_id"]].join(
#         pd.json_normalize(classification_codes_df["classification_code"])
#     )
#     classification_codes_df.drop(columns=["@_fa"], inplace=True)
#     classification_codes_df.rename(
#         columns={"$": "name", "@code": "code", "@abbrev": "abbreviation"}, inplace=True
#     )

#     paper_to_classification_code_df = classification_codes_df.loc[
#         :, ["paper_id", "code"]
#     ]

#     classification_codes_df.drop(columns=["paper_id"], inplace=True)

#     return (classification_codes_df, paper_to_classification_code_df)


# # %%
# def get_affiliations_df(df):
#     affiliations_df = df.explode("affiliations", ignore_index=True)[
#         ["id", "affiliations"]
#     ]
#     affiliations_df.rename(
#         columns={"id": "paper_id", "affiliations": "affiliation"}, inplace=True
#     )
#     affiliations_df = affiliations_df[["paper_id"]].join(
#         pd.json_normalize(affiliations_df["affiliation"])
#     )

#     paper_to_affiliations_df = affiliations_df.loc[:, ["paper_id", "@id"]]
#     paper_to_affiliations_df.rename(columns={"@id": "affiliation_id"}, inplace=True)

#     affiliations_df = (
#         affiliations_df.drop(columns=["paper_id"])
#         .drop_duplicates()
#         .reset_index(drop=True)
#     )

#     return (affiliations_df, paper_to_affiliations_df)


# # %%
# def get_references_df(df):
#     references_df = df.explode("references", ignore_index=True)[["id", "references"]]
#     references_df.rename(
#         columns={"id": "paper_id", "references": "reference"}, inplace=True
#     )
#     references_df = references_df.join(
#         pd.json_normalize(references_df["reference"])
#     ).drop(columns=["reference"])
#     return references_df


# # %%
# def get_keywords_df(df):
#     keywords_df = df.explode("keywords", ignore_index=True)[["id", "keywords"]]
#     keywords_df.rename(columns={"id": "paper_id", "keywords": "keyword"}, inplace=True)
#     keywords_df = keywords_df.join(pd.json_normalize(keywords_df["keyword"])).drop(
#         columns=["keyword"]
#     )
#     keywords_df = keywords_df.loc[:, ["paper_id", "$"]]
#     keywords_df.rename(columns={"$": "keyword"}, inplace=True)

#     paper_to_keywords_df = keywords_df

#     keywords_df = keywords_df.drop(columns=["paper_id"]).drop_duplicates()

#     return (keywords_df, paper_to_keywords_df)


# %%
if __name__ == "__main__":
    cpu_count = multiprocessing.cpu_count()
    data_list = []

    buckets = {i: [] for i in range(cpu_count)}
    for path in pathlib.Path("Data 2018-2023/Project").glob("*/*"):
        buckets[path.__hash__() % cpu_count].append(path)

    with multiprocessing.Pool(cpu_count) as p:
        for data_part in p.map(get_data_list, buckets.values()):
            data_list += data_part
    merged_df = get_df(data_list)
    merged_df.to_csv("data/processed/merged.csv")

    # papers_df = get_papers_df(merged_df)
    # papers_df.to_csv("data/processed/papers.csv")

    # classification_codes_df, paper_to_classification_code_df = (
    #     get_classification_codes_df(merged_df)
    # )
    # classification_codes_df.to_csv("data/processed/classification_codes.csv")
    # paper_to_classification_code_df.to_csv(
    #     "data/processed/paper_to_classification_code.csv"
    # )

    # affiliations_df, paper_to_affiliations_df = get_affiliations_df(merged_df)
    # affiliations_df.to_csv("data/processed/affiliations.csv")
    # paper_to_affiliations_df.to_csv("data/processed/paper_to_affiliations.csv")

    # references_df = get_references_df(merged_df)
    # references_df.to_csv("data/processed/references.csv")

    # keywords_df, paper_to_keywords_df = get_keywords_df(merged_df)
    # keywords_df.to_csv("data/processed/keywords.csv")
    # paper_to_keywords_df.to_csv("data/processed/paper_to_keywords.csv")
