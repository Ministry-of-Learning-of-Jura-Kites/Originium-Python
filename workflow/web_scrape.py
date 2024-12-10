from prefect import task, flow
from prefect.blocks.system import Secret
import requests
import time
import os
import json

# Task to perform API search
@task
def api_search(year, offset, count):
    secret_block = Secret.load("scopus-apikey")
    api_key = secret_block.get()
    header = {"X-ELS-APIKey": api_key, "Accept": "application/json"}

    URI = "https://api.elsevier.com/content/search/scopus"
    AFF_NAME = "Chulalongkorn"

    res = requests.get(
        url=URI,
        headers=header,
        params={
            "start": offset,
            "count": count,
            "query": f"AFFIL({AFF_NAME}) AND PUBYEAR = {year}",
            "apiKey": api_key,
        },
    )
    time.sleep(3)

    if not res.ok:
        print(f"Error in search API call: {res.content}")
        return None

    try:
        return res.json().get("search-results", {}).get("entry", [])
    except Exception as e:
        print(f"Error parsing search response: {e}, content: {res.content}")
        return None


# Task to retrieve abstracts
@task
def api_abstracts_retrieve(eid):
    secret_block = Secret.load("scopus-apikey")
    api_key = secret_block.get()
    header = {"X-ELS-APIKey": api_key, "Accept": "application/json"}

    URI = f"https://api.elsevier.com/content/abstract/eid/{eid}"

    res = requests.get(url=URI, headers=header, params={"apiKey": api_key})
    time.sleep(3)

    if not res.ok:
        print(f"Error in abstract retrieval: {res.content}")
        return None

    try:
        return json.loads(res.text)
    except Exception as e:
        print(f"Error parsing abstract response: {e}, content: {res.content}")
        return None


# Task to write JSON
@task
def write_json(path, data):
    directory = os.path.dirname(path)
    if not os.path.exists(directory):
        os.makedirs(directory)

    with open(path, "w") as json_file:
        json.dump(data, json_file, indent=4)


# Main flow
@flow
def main_flow(first_year, last_year, each_year, each_chunk):
    start_time = time.time()
    total = 0

    years = range(first_year, last_year + 1)
    for year in years:
        scopus_root_path = f"../data/raw/fetched_papers/scopus/{year}"
        abstract_root_path = f"../data/raw/fetched_papers/abstract/{year}"
        for offset in range(0, each_year, each_chunk):
            elapsed_time = time.time() - start_time
            print(
                f"[{elapsed_time:.2f}] Searching with year:{year}, offset:{offset}, count:{each_chunk}"
            )

            # Perform API search
            search_result = api_search(year, offset, each_chunk)
            if not search_result:
                print(
                    f"[{elapsed_time:.2f}] No results for year:{year}, offset:{offset}"
                )
                break
            else:
                print(
                    f"[{elapsed_time:.2f}] Found {len(search_result)} papers for year:{year}, offset:{offset}"
                )
                # Save to JSON
                path = os.path.join(abstract_root_path, f"{year}-{offset}.json")
                write_json(path, search_result)

            # for paper in search_result:
            #     eid = paper.get("eid")
            #     if not eid:
            #         print(f"Missing EID in search result: {paper}")
            #         continue

            #     # Retrieve abstract
            #     abs_response = api_abstracts_retrieve(eid)
            #     if abs_response is None:
            #         print(f"[{elapsed_time:.2f}] Paper with eid:{eid} not found")
            #         continue

            #     # Save to JSON
            #     path = os.path.join(abstract_root_path, f"{eid}.json")
            #     write_json(path, abs_response)

            total += len(search_result)
            print(f"[{elapsed_time:.2f}] Loaded {total} papers")


# Run the flow
if __name__ == "__main__":
    main_flow(first_year=2000, last_year=2007, each_year=3000, each_chunk=10)
