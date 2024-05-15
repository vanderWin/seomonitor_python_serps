import os
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import requests
import numpy as np
from datetime import datetime
import time

api_key = os.getenv("API_KEY")
campaign_id = os.getenv("CAMPAIGN_ID")
project_id = os.getenv("PROJECT_ID")
bucket_name = os.getenv("BUCKET_NAME")
dest_file_name = os.getenv("DEST_FILE_NAME")

current_date = datetime.now().strftime("%Y-%m-%d")

def main(request):

    # Step one, fetch keyword data
    # Initialise variables
    offset = 0
    limit = 1000
    status_code = 200  # Assume initial status code to start the loop

    # Initialize an empty DataFrame for keywords data
    keywords_df = pd.DataFrame()

    while status_code == 200:
        url = f"https://apigw.seomonitor.com/v3/rank-tracker/v3.0/keywords?campaign_id={campaign_id}&start_date={current_date}&end_date={current_date}&limit={limit}&offset={offset}&include_all_groups=true"

        headers = {"Accept": "*/*", "Authorization": api_key}
        print(f"Requesting URL: {url}")
        response = requests.get(url, headers=headers, timeout=120)
        status_code = response.status_code

        if status_code == 200:
            json_content = response.json()  # Directly parse the JSON response
            kwds = pd.json_normalize(json_content)  # Convert JSON to DataFrame
            kwds["variant_flag"] = ~kwds[
                "main_keyword_id"
            ].isna()  # Create variant_flag

            # Select specific columns to form 'reference_list'
            reference_list = kwds[
                [
                    "keyword_id",
                    "keyword",
                    "main_keyword_id",
                    "groups",
                    "search_data.search_volume",
                    "variant_flag",
                ]
            ]

            # Append the reference_list DataFrame to the main keywords DataFrame
            keywords_df = pd.concat([keywords_df, reference_list], ignore_index=True)

            offset += limit
            print(f"Fetched and appended keywords with offset {offset}")
        else:
            print(f"Received status code {status_code}, stopping...")

    # Convert 'None' values to np.nan in 'main_keyword_id' for the entire DataFrame
    keywords_df["main_keyword_id"] = keywords_df["main_keyword_id"].apply(
        lambda x: np.nan if x is None else x
    )

    # Step 2, fetch group data
    url = f"https://apigw.seomonitor.com/v3/rank-tracker/v3.0/groups?campaign_id={campaign_id}"
    headers = {"Accept": "*/*", "Authorization": api_key}

    response = requests.get(url, headers=headers, timeout=120)
    groups = response.json()  # Directly get the JSON response

    # Recursive function to process groups
    def process_groups(groups, parent_id=pd.NA):
        rows_list = []

        for group in groups:
            current = {
                "group_id": group["group_id"],
                "group_name": group["name"],
                "group_type": group["type"],
                "parent_id": parent_id,
            }

            rows_list.append(current)

            # If there are subgroups, process them recursively
            if "subgroups" in group and len(group["subgroups"]) > 0:
                children = process_groups(group["subgroups"], group["group_id"])
                rows_list.extend(children)

        return rows_list

    # Running loop and converting to df
    df_groups_list = process_groups(groups)
    df_groups = pd.DataFrame(df_groups_list)

    # Step 3, creating a big keyword list
    keywords_expanded = keywords_df.assign(
        groups=keywords_df["groups"].str.split(",")
    ).explode("groups")
    keywords_expanded["groups"] = keywords_expanded["groups"].astype(str)

    # Ensure that the 'group_id' in group_info is of type string for the join
    df_groups["group_id"] = df_groups["group_id"].astype(str)

    # Perform the left join
    joined_data = pd.merge(
        keywords_expanded, df_groups, left_on="groups", right_on="group_id", how="left"
    )

    # Aggregate the joined data
    keywords_augmented = (
        joined_data.groupby("keyword_id")
        .agg(
            {
                "keyword": "first",
                "main_keyword_id": "first",
                "search_data.search_volume": "first",
                "variant_flag": "first",
                "group_name": lambda x: ", ".join(map(str, x.dropna().unique())),
                "parent_id": lambda x: ", ".join(map(str, x.dropna().unique())),
            }
        )
        .reset_index()
    )

    # Prepare a DataFrame for the join
    main_keywords = keywords_augmented[["keyword_id", "keyword"]].rename(
        columns={"keyword": "main_keyword"}
    )

    keywords_augmented["main_keyword_id"] = keywords_augmented[
        "main_keyword_id"
    ].astype(str)
    main_keywords["keyword_id"] = main_keywords["keyword_id"].astype(str)

    # Perform the left join
    keywords_augmented = pd.merge(
        keywords_augmented,
        main_keywords,
        left_on="main_keyword_id",
        right_on="keyword_id",
        how="left",
        suffixes=("", "_main"),
    )

    # Clean up the resulting DataFrame if necessary
    keywords_augmented.drop(columns=["keyword_id_main"], inplace=True)
    # Convert 'None' values to np.nan in 'main_keyword_id' for the entire DataFrame
    keywords_augmented["main_keyword_id"] = keywords_augmented[
        "main_keyword_id"
    ].replace("None", np.nan)

    # Exporting DataFrame to CSV with default handling of NaN values (empty strings) for testing purposes, remove this step.
    keywords_augmented.to_csv("keywords_augmented.csv", index=False)

    # Step 4, fetch SERP data
    date_str = datetime.now().strftime("%Y-%m-%d")

    def fetch_and_process_serp_data(
        device_type,
        campaign_id,
        date_str,
        api_key,
        keywords_augmented,
        file_path,
        append=False,
    ):
        offset = 0
        limit = 100
        status_code = 200
        max_offset = 10000  # Ridiculously high offset cap, just to be safe

        # Initialize an empty DataFrame to accumulate results
        all_serp_flat = pd.DataFrame()

        while offset < max_offset and (status_code == 200 or status_code == 524):
            url = f"https://apigw.seomonitor.com/v3/rank-tracker/v3.0/keywords/top-results?campaign_id={campaign_id}&device={device_type}&date={date_str}&limit={limit}&offset={offset}"
            print(f"Requesting URL: {url}")
            response = requests.get(
                url, headers={"Accept": "*/*", "Authorization": api_key}, timeout=120
            )
            status_code = response.status_code

            if status_code == 200:
                json_content = response.json()
                serp_flat = pd.json_normalize(
                    data=json_content,
                    record_path=["top_100_results"],
                    meta=["keyword_id", "keyword"],
                    errors="ignore",
                )

                # Accumulate results
                all_serp_flat = pd.concat([all_serp_flat, serp_flat], ignore_index=True)

                offset += limit
                print(f"Fetched and saved {device_type} SERP data with offset {offset}")
                time.sleep(1)  # Throttle requests to avoid hitting rate limits
            elif status_code == 524:
                print("Received status code 524, waiting 30 seconds before retrying...")
                time.sleep(30)
            else:
                print(f"Received status code {status_code}, stopping...")
                break

        # Process and join this data with keywords_augmented
        final_df = pd.merge(
            all_serp_flat, keywords_augmented, how="left", on="keyword_id"
        )
        final_df["campaign_id"] = campaign_id
        final_df["date"] = date_str
        final_df["device"] = device_type.capitalize()
        # Drop the 'keyword_y' column
        final_df = final_df.drop(columns=['keyword_y'])

        # Determine whether to append or write new
        if append:
            final_df.to_csv(file_path, mode="a", index=False, header=False, na_rep="")
        else:
            final_df.to_csv(file_path, index=False, header=False, na_rep="")
        print(f"Completed fetching and saving all {device_type} data.")

    # Path for the CSV file, assuming it's the same for both desktop and mobile
    file_path = f"{dest_file_name}.csv"

    # Fetch and process desktop data, write to CSV without appending (creating the file or overwriting if it exists)
    fetch_and_process_serp_data(
        "desktop",
        campaign_id,
        current_date,
        api_key,
        keywords_augmented,
        file_path,
        append=False,
    )

    # Fetch and process mobile data, append to the existing CSV
    fetch_and_process_serp_data(
        "mobile",
        campaign_id,
        current_date,
        api_key,
        keywords_augmented,
        file_path,
        append=True,
    )

    # Step 5, moving the data to GCS
    from google.cloud import storage

    storage_client = storage.Client(project="organic-data")

    bucket_name = "rankflux"
    destination_blob_name = f"{dest_file_name}.csv"

    # Get the bucket
    bucket = storage_client.bucket(bucket_name)

    # Create a blob and upload the file
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)

    print(
        f"File {file_path} uploaded to {destination_blob_name} in bucket {bucket_name}."
    )

    # Final step, move to BQ 
    from google.cloud import bigquery

    project_id = "organic-data-361613"
    bucket_name = "rankflux"
    destination_blob_name = f"{dest_file_name}.csv"
    dataset_id = "rankflux_data"
    table_id = f"{campaign_id}_serps"
    uri = f"gs://{bucket_name}/{destination_blob_name}"

    # Initialize a BigQuery client
    client = bigquery.Client(project=project_id)

    # Manually define the schema
    schema = [
        bigquery.SchemaField("domain", "STRING"),
        bigquery.SchemaField("rank", "INTEGER"),
        bigquery.SchemaField("landing_page", "STRING"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("search_intent", "STRING"),
        bigquery.SchemaField("keyword_id", "STRING"),
        bigquery.SchemaField("keyword", "STRING"),
        bigquery.SchemaField("main_keyword_id", "STRING"),
        bigquery.SchemaField("search_volume", "INTEGER"),
        bigquery.SchemaField("variant_flag", "BOOL"),
        bigquery.SchemaField("group_name", "STRING"),
        bigquery.SchemaField("parent_group_id", "STRING"),
        bigquery.SchemaField("main_keyword", "STRING"),
        bigquery.SchemaField("campaign_id", "STRING"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("device", "STRING"),
    ]

    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        autodetect=False,
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=0,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        max_bad_records=10
    )

    # Start the load job
    load_job = client.load_table_from_uri(
        uri, f"{dataset_id}.{table_id}", job_config=job_config
    )

    try:
        load_job.result()  # Waits for the job to complete
        print(f"Loaded data from {uri} into {dataset_id}.{table_id} in BigQuery.")
    except Exception as e:
        print(f"Failed to load data from {uri} into BigQuery: {e}")
        return "Process encountered an error"
