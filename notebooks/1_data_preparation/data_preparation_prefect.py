from prefect import task, flow
import pandas as pd
import json
from pathlib import Path
from genson import SchemaBuilder
from typing import List

@flow(log_prints=True)
def data_preparation():
    # %% [markdown]
    # # Data Preparation

    # %% [markdown]
    # ## 0. Setup

    # %%
    # %pip install genson

    # %%

    # %%
    DATA_FOLDER_PATH = Path("../../data")

    RAW_DATA_FOLDER_PATH = DATA_FOLDER_PATH.joinpath("raw/Data 2018-2023/Project")
    RAW_DATA_SAMPLE_PATH = RAW_DATA_FOLDER_PATH.joinpath("2023/202302889")
    RAW_DATA_SCHEMA_PATH = DATA_FOLDER_PATH.joinpath("schema/raw_data_schema.json")

    PROCESSED_DATA_FOLDER_PATH = DATA_FOLDER_PATH.joinpath("processed")

    # %% [markdown]
    # ## 1. Explore given data from Scopus

    # %%
    with open(RAW_DATA_SAMPLE_PATH) as file:
        data = json.load(file)

    # %%
    print(json.dumps(data, indent=2))

    # %% [markdown]
    # ## 2. Build schema

    # %%
    schema = {}

    for path in RAW_DATA_FOLDER_PATH.glob("*/*"):
        print(path)
        with open(path.absolute()) as file:
            data = json.load(file)
        schema_builder = SchemaBuilder()
        schema_builder.add_schema(schema)
        schema_builder.add_object(data)
        schema = schema_builder.to_schema()

    # %%
    with open(RAW_DATA_SCHEMA_PATH, "w") as file:
        json.dump(schema, file, indent=2)

    # %% [markdown]
    # ## 3. Explore fields

    # %%
    def is_path_required(schema: dict, path: str | List[str]):
        if type(path) is not list:
            path = path.split(".")
        if len(path) == 0:
            return True

        if "items" in schema:
            return is_path_required(schema["items"], path)
        
        if "anyOf" in schema:
            for schema_part in schema["anyOf"]:
                if not is_path_required(schema_part, path):
                    return False
            return True
        
        if "type" in schema and schema["type"] != "object":
            return False
        
        property = path[0]
        if property not in schema["required"]:
            return False
        else:
            schema_part = schema["properties"][property]
            if not is_path_required(schema_part, path[1:]):
                return False
        return True

    # %%
    with open(RAW_DATA_SAMPLE_PATH) as file:
        data = json.load(file)

    # %%
    with open(RAW_DATA_SCHEMA_PATH) as file:
        schema = json.load(file)

    # %% [markdown]
    # ### 3.1 ID

    # %%
    data["abstracts-retrieval-response"]["coredata"]["eid"]

    # %%
    is_path_required(schema, "abstracts-retrieval-response.coredata.eid")

    # %% [markdown]
    # ### 3.2 Author

    # %%
    pd.json_normalize(
        data,
        record_path=["abstracts-retrieval-response", "authors", "author"],
        errors="ignore",
    )

    # %%
    is_path_required(schema, "abstracts-retrieval-response.authors.author")

    # %% [markdown]
    # ### 3.3 Article title

    # %%
    data["abstracts-retrieval-response"]["coredata"]["dc:title"]

    # %%
    is_path_required(schema, "abstracts-retrieval-response.coredata.dc:title")

    # %% [markdown]
    # ### 3.4 Publication name
    # (background of the problem)

    # %%
    data["abstracts-retrieval-response"]["coredata"]["prism:publicationName"]

    # %%
    is_path_required(schema, "abstracts-retrieval-response.coredata.prism:publicationName")

    # %% [markdown]
    # ### 3.5 Abstract

    # %%
    print(data["abstracts-retrieval-response"]["item"]["bibrecord"]["head"]["abstracts"])

    # %%
    is_path_required(schema, "abstracts-retrieval-response.item.bibrecord.head.abstracts")

    # %% [markdown]
    # ### 3.6 Classification codes

    # %%
    # Classification codes (1) (Not used)
    pd.json_normalize(
        data,
        record_path=["abstracts-retrieval-response", "item", "bibrecord", "head", "enhancement", "classificationgroup", "classifications"],
    )

    # %%
    is_path_required(schema, "abstracts-retrieval-response.item.bibrecord.head.enhancement.classificationgroup.classifications")

    # %%
    # Classification codes (2) (ASJC)
    pd.json_normalize(
        data,
        record_path=["abstracts-retrieval-response", "subject-areas", "subject-area"],
    )

    # %%
    is_path_required(schema, "abstracts-retrieval-response.subject-areas.subject-area")

    # %% [markdown]
    # ### 3.7 Publication date

    # %%
    # # Publication date (1)
    # data["abstracts-retrieval-response"]["item"]["bibrecord"]["head"]["source"]["publicationdate"]

    # %%
    is_path_required(schema, "abstracts-retrieval-response.item.bibrecord.head.source.publicationdate")

    # %%
    # Publication date (2)
    data["abstracts-retrieval-response"]["coredata"]["prism:coverDate"]

    # %%
    is_path_required(schema, "abstracts-retrieval-response.coredata.prism:coverDate")

    # %% [markdown]
    # ### 3.8 Creation date

    # %%
    # # Creation date
    # data["abstracts-retrieval-response"]["item"]["bibrecord"]["item-info"]["history"]["date-created"]

    # %%
    is_path_required(schema, "abstracts-retrieval-response.item.bibrecord.item-info.history.date-created")

    # %% [markdown]
    # ### 3.9 Affiliations

    # %%
    # Affiliations (1)
    affiliations_1 = pd.json_normalize(
        data,
        ["abstracts-retrieval-response", "item", "bibrecord", "head", "author-group"]
    )

    affiliations_1

    # %%
    is_path_required(schema, "abstracts-retrieval-response.item.bibrecord.head.author-group")

    # %%
    # Affiliations (2)
    pd.json_normalize(
        data,
        ["abstracts-retrieval-response", "affiliation"]
    )

    # %%
    is_path_required(schema, "abstracts-retrieval-response.affiliation")

    # %% [markdown]
    # ### 3.10 Citation info

    # %%
    # Citation info
    data["abstracts-retrieval-response"]["item"]["bibrecord"]["head"]["citation-info"]

    # %%
    is_path_required(schema, "abstracts-retrieval-response.item.bibrecord.head.citation-info")

    # %% [markdown]
    # ### 3.11 Cited by count

    # %%
    # Cited by count
    data["abstracts-retrieval-response"]["coredata"]["citedby-count"]

    # %%
    is_path_required(schema, "abstracts-retrieval-response.coredata.citedby-count")

    # %% [markdown]
    # ### 3.12 Reference count

    # %%
    # Reference count
    data["abstracts-retrieval-response"]["item"]["bibrecord"]["tail"]["bibliography"]["@refcount"]

    # %%
    is_path_required(schema, "abstracts-retrieval-response.item.bibrecord.tail.bibliography.@refcount")

    # %% [markdown]
    # ### 3.13 References

    # %%
    # References
    references = pd.json_normalize(
        data,
        ["abstracts-retrieval-response", "item", "bibrecord", "tail", "bibliography", "reference"],
    )
    references["paper_id"] = data["abstracts-retrieval-response"]["coredata"]["eid"]
    references.head(5)

    # %%
    is_path_required(schema, "abstracts-retrieval-response.item.bibrecord.tail.bibliography.reference")

    # %% [markdown]
    # ### 3.14 Keywords

    # %%
    # Keywords (2)
    keywords = pd.json_normalize(
        data,
        ["abstracts-retrieval-response", "authkeywords", "author-keyword"],
    )
    keywords = keywords[["$"]]
    keywords

    # %%
    is_path_required(schema, "abstracts-retrieval-response.authkeywords.author-keyword")

    # %% [markdown]
    # ## 4. Aggregate data

    # %% [markdown]
    # ### 4.0 Setup

    # %%
    def get_from_path_or_none(data: dict, path: str | List[str]):
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

    # %% [markdown]
    # ### 4.1 Reading data

    # %%
    data_list = []
    for path in RAW_DATA_FOLDER_PATH.glob("*/*"):
        try:
            print(path)
            with open(path) as file:
                data = json.load(file)
            filtered_data = {}
            filtered_data["id"] = get_from_path_or_none(data, ["abstracts-retrieval-response", "coredata", "eid"])
            filtered_data["title"] = get_from_path_or_none(data, ["abstracts-retrieval-response", "coredata", "dc:title"])
            filtered_data["publication_name"] = get_from_path_or_none(data, ["abstracts-retrieval-response", "coredata", "prism:publicationName"],)
            # filtered_data["abstract"] = get_from_path_or_none(data, ["abstracts-retrieval-response", "item", "bibrecord", "head", "abstracts"])
            filtered_data["publish_date"] = get_from_path_or_none(data, ["abstracts-retrieval-response", "coredata", "prism:coverDate"])
            filtered_data["cited_by_count"] = get_from_path_or_none(data, ["abstracts-retrieval-response", "coredata", "citedby-count"])
            # filtered_data["reference_count"] = get_from_path_or_none(data, ["abstracts-retrieval-response", "item", "bibrecord", "tail", "bibliography", "@refcount"])
            # filtered_data["classification_codes"] = get_from_path_or_none(data, ["abstracts-retrieval-response", "subject-areas", "subject-area"])
            filtered_data["affiliations"] = get_from_path_or_none(data, ["abstracts-retrieval-response", "affiliation"])
            # filtered_data["references"] = get_from_path_or_none(data, ["abstracts-retrieval-response", "item", "bibrecord", "tail", "bibliography", "reference"])
            # filtered_data["keywords"] = get_from_path_or_none(data, ["abstracts-retrieval-response", "authkeywords", "author-keyword"])
            data_list.append(filtered_data)
        except Exception as e:
            print(f"Failed: {path}\nException: {repr(e)}")
    print("Reading files done.")
    merged_df = pd.DataFrame(data_list)
    merged_df.head(5)

    # %%
    merged_df["publish_date"] = pd.to_datetime(merged_df["publish_date"])

    # %%
    list_columns = ["classification_codes", "affiliations", "references", "keywords"]
    for list_column in list_columns:
        merged_df[list_column] = merged_df[list_column].apply(ensure_list_data)

    # %%
    json_columns = ["classification_codes", "affiliations", "references", "keywords"]
    for json_column in json_columns:
        merged_df[json_column] = merged_df[json_column].apply(json.dumps)

    # %%
    merged_df.head(5)

    # %%
    merged_df.info()

    # %%
    with open(PROCESSED_DATA_FOLDER_PATH.joinpath("merged.csv"), "w") as file:
        merged_df.to_csv(file, index=False)

    # %% [markdown]
    # ## 5. Segregate data

    # %% [markdown]
    # ### 5.0 Setup

    # %%
    merged_df = pd.read_csv(PROCESSED_DATA_FOLDER_PATH.joinpath("merged.csv"))
    merged_df.head(5)

    # %%
    merged_df.info()

    # %%
    merged_df = merged_df.astype({
        "cited_by_count": pd.Int64Dtype(),
        "reference_count": pd.Int64Dtype(),
    })

    # %%
    list_columns = ["classification_codes", "affiliations", "references", "keywords"]
    for list_column in list_columns:
        merged_df[list_column] = merged_df[list_column].apply(json.loads)

    # %% [markdown]
    # ### 5.1 Papers

    # %%
    selected_columns = ["id", "title", "publication_name", "abstract", "publish_date", "cited_by_count", "reference_count"]
    papers_df = merged_df[selected_columns]
    papers_df.head(5)

    # %%
    papers_df["id"].is_unique

    # %%
    with open(PROCESSED_DATA_FOLDER_PATH.joinpath("papers.csv"), "w") as file:
        papers_df.to_csv(file, index=False)

    # %% [markdown]
    # ### 5.2 Classification codes

    # %%
    paper_to_classification_code_df = merged_df[["id", "classification_codes"]].explode("classification_codes", ignore_index=True)
    paper_to_classification_code_df.dropna(subset=["classification_codes"], inplace=True)
    paper_to_classification_code_df.head(5)

    # %%
    classification_codes_df = pd.json_normalize(paper_to_classification_code_df["classification_codes"])
    classification_codes_df.head(5)

    # %%
    classification_codes_df.drop(columns=["@_fa"], inplace=True)
    classification_codes_df.rename(columns={
        "$": "name",
        "@code": "code",
        "@abbrev": "abbreviation"
    }, inplace=True)
    classification_codes_df.head(5)

    # %%
    paper_to_classification_code_df.rename(columns={"id": "paper_id"}, inplace=True)
    paper_to_classification_code_df = paper_to_classification_code_df[["paper_id"]].join(classification_codes_df["code"], how="left")
    paper_to_classification_code_df.head(5)

    # %%
    paper_to_classification_code_df.drop_duplicates(ignore_index=True, inplace=True)

    # %%
    classification_codes_df.drop_duplicates(subset=["code"], ignore_index=True, inplace=True)

    # %%
    with open(PROCESSED_DATA_FOLDER_PATH.joinpath("classification_codes.csv"), "w") as file:
        classification_codes_df.to_csv(file, index=False)

    # %%
    with open(PROCESSED_DATA_FOLDER_PATH.joinpath("paper_to_classification_code.csv"), "w") as file:
        paper_to_classification_code_df.to_csv(file, index=False)

    # %% [markdown]
    # ### 5.3 Affiliations

    # %%
    paper_to_affiliation_df = merged_df[["id", "affiliations"]].explode("affiliations", ignore_index=True)
    paper_to_affiliation_df.dropna(subset=["affiliations"], inplace=True)
    paper_to_affiliation_df.head(5)

    # %%
    affiliations_df = pd.json_normalize(paper_to_affiliation_df["affiliations"])
    paper_to_affiliation_df.drop(columns=["affiliations"], inplace=True)
    affiliations_df.head(5)

    # %%
    affiliations_df.rename(columns={
        "@id": "id",
        "affiliation-city": "city",
        "affilname": "name",
        "@href": "href",
        "affiliation-country": "country",
    }, inplace=True)
    affiliations_df.head(5)

    # %%
    paper_to_affiliation_df.rename(columns={"id": "paper_id"}, inplace=True)
    paper_to_affiliation_df = paper_to_affiliation_df.join(affiliations_df["id"], how="left")
    paper_to_affiliation_df.head(5)

    # %%
    paper_to_affiliation_df.drop_duplicates(ignore_index=True, inplace=True)

    # %%
    affiliations_df.drop_duplicates(subset=["id"], ignore_index=True, inplace=True)

    # %%
    with open(PROCESSED_DATA_FOLDER_PATH.joinpath("affiliations.csv"), "w") as file:
        affiliations_df.to_csv(file, index=False)

    # %%
    with open(PROCESSED_DATA_FOLDER_PATH.joinpath("paper_to_affiliation.csv"), "w") as file:
        paper_to_affiliation_df.to_csv(file, index=False)

    # %% [markdown]
    # ### 5.4 References

    # %%
    references_df = merged_df[["id", "references"]].explode("references", ignore_index=True)
    references_df.dropna(subset=["references"], inplace=True)
    references_df.head(5)

    # %%
    references_df = references_df[["id"]].join(pd.json_normalize(references_df["references"]), how="left")
    references_df.head(5)

    # %%
    references_df.info()

    # %%
    references_df[references_df.columns[:15]].head(5)

    # %%
    references_df[references_df.columns[15:]].head(5)

    # %%
    references_df = references_df[[
        "id", "@id", "ref-fulltext", "ref-info.ref-title.ref-titletext", "ref-info.ref-authors.author",
        "ref-info.ref-sourcetitle", "ref-info.ref-text"
    ]]
    references_df.rename(columns={
        "id": "paper_id",
        "@id": "reference_id",
        "ref-fulltext": "full_text",
        "ref-info.ref-title.ref-titletext": "title",
        "ref-info.ref-authors.author": "authors",
        "ref-info.ref-sourcetitle": "source_title",
        "ref-info.ref-text": "text",
    }, inplace=True)
    references_df.head(5)

    # %% [markdown]
    # #### 5.4.1 Authors

    # %%
    paper_reference_author_df = references_df[["paper_id", "reference_id", "authors"]].explode("authors", ignore_index=True)
    paper_reference_author_df.dropna(subset=["authors"], inplace=True)
    paper_reference_author_df.head(5)

    # %%
    paper_reference_author_df = paper_reference_author_df[["paper_id", "reference_id"]].join(pd.json_normalize(paper_reference_author_df["authors"]), how="left")
    paper_reference_author_df.head(5)

    # %%
    paper_reference_author_df = paper_reference_author_df[["paper_id", "reference_id", "ce:indexed-name"]]
    paper_reference_author_df.rename(columns={"ce:indexed-name": "name"}, inplace=True)
    paper_reference_author_df.head(5)

    # %%
    references_df.drop(columns=["authors"], inplace=True)
    references_df.head(5)

    # %%
    with open(PROCESSED_DATA_FOLDER_PATH.joinpath("references.csv"), "w") as file:
        references_df.to_csv(file, index=False)

    # %%
    with open(PROCESSED_DATA_FOLDER_PATH.joinpath("paper_reference_author.csv"), "w") as file:
        paper_reference_author_df.to_csv(file, index=False)

    # %% [markdown]
    # ### 5.5 Keywords

    # %%
    paper_to_keyword_df = merged_df[['id', 'keywords']].explode('keywords', ignore_index=True)
    paper_to_keyword_df.dropna(subset=['keywords'], inplace=True)
    paper_to_keyword_df.head(5)

    # %%
    paper_to_keyword_df = paper_to_keyword_df[['id']].join(pd.json_normalize(paper_to_keyword_df['keywords']), how='left')
    paper_to_keyword_df.head(5)

    # %%
    paper_to_keyword_df.drop(columns=['@_fa'], inplace=True)
    paper_to_keyword_df.rename(columns={'$': 'keyword'}, inplace=True)
    paper_to_keyword_df.head(5)

    # %%
    paper_to_keyword_df.dropna(subset=['keyword'], inplace=True)
    paper_to_keyword_df.drop_duplicates(ignore_index=True, inplace=True)
    paper_to_keyword_df.head(5)

    # %%
    with open(PROCESSED_DATA_FOLDER_PATH.joinpath('paper_to_keyword.csv'), 'w') as file:
        paper_to_keyword_df.to_csv(file, index=False)

    # %%
    keywords_df = paper_to_keyword_df[['keyword']].drop_duplicates(ignore_index=True)
    keywords_df.head(5)

    # %%
    with open(PROCESSED_DATA_FOLDER_PATH.joinpath('keywords.csv'), 'w') as file:
        keywords_df.to_csv(file, index=False)


if __name__ == "__main__":
    data_preparation.serve(
        name="data-preparation-test",
        tags=["test"],
        parameters={},
        interval=60,
    )
