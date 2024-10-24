import os
import json
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Load environment variables
if os.path.exists('.env'):
    from dotenv import load_dotenv
    load_dotenv()

# Set the path to your service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "organic-data-361613-c0202d1cabeb.json"

# Initialize BigQuery client
project_id = os.getenv("PROJECT_ID")
dataset_id = os.getenv("DATASET_ID")
table_id = os.getenv("TABLE_ID")
client = bigquery.Client(project=project_id)

# Campaigns data
campaigns = [
    {"Name": "United Kingdom", "ID": 313717}
    # {"Name": "Belfast", "ID": 314477},
    # {"Name": "Birmingham", "ID": 314470},
    # {"Name": "Bristol", "ID": 314479},
    # {"Name": "Edinburgh", "ID": 314474},
    # {"Name": "Liverpool", "ID": 314478},
    # {"Name": "London", "ID": 314469},
    # {"Name": "Milton Keynes", "ID": 314472},
    # {"Name": "Newcastle upon Tyne", "ID": 314476},
    # {"Name": "Norwich", "ID": 314473},
    # {"Name": "Plymouth", "ID": 314475},
    # {"Name": "Wolverhampton", "ID": 314471}
]

# BigQuery table schema (adjust types as needed)
schema = [
    bigquery.SchemaField("campaign_id", "STRING"),
    bigquery.SchemaField("location_name", "STRING"),
    bigquery.SchemaField("date", "DATE"),
    bigquery.SchemaField("keyword_id", "STRING"),
    bigquery.SchemaField("keyword", "STRING"),
    bigquery.SchemaField("main_keyword_id", "STRING"),
    bigquery.SchemaField("search_intent", "STRING"),
    bigquery.SchemaField("labels", "STRING"),
    bigquery.SchemaField("groups", "STRING"),
    bigquery.SchemaField("search_volume", "INTEGER"),
    bigquery.SchemaField("year_over_year", "FLOAT"),

    # Landing Pages
    bigquery.SchemaField("current_desktop_landing_page", "STRING"),
    bigquery.SchemaField("desired_desktop_landing_page", "STRING"),
    bigquery.SchemaField("current_mobile_landing_page", "STRING"),
    bigquery.SchemaField("desired_mobile_landing_page", "STRING"),

    # Ranking Data
    bigquery.SchemaField("rank_desktop", "INTEGER"),
    bigquery.SchemaField("rank_mobile", "INTEGER"),

    # Traffic Data
    bigquery.SchemaField("traffic_sessions", "INTEGER"),
    bigquery.SchemaField("transactions", "INTEGER"),
    bigquery.SchemaField("ecommerce_revenue", "FLOAT"),
    bigquery.SchemaField("goal_completions", "INTEGER"),
    bigquery.SchemaField("goal_revenue", "FLOAT"),

    # Opportunity Data
    bigquery.SchemaField("opportunity_score", "FLOAT"),
    bigquery.SchemaField("opportunity_difficulty", "STRING"),
    bigquery.SchemaField("opportunity_avg_cpc", "FLOAT"),
    bigquery.SchemaField("additional_monthly_sessions", "INTEGER")
]

# Define the specific date for the API call
specified_date = "2024-10-01"

# Create or update the BigQuery table
def create_bigquery_table(client, dataset_id, table_id, schema):
    table_ref = client.dataset(dataset_id).table(table_id)
    try:
        client.get_table(table_ref)  # Check if table exists
        print(f"Table {table_id} already exists.")
    except NotFound:
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(field="date")
        client.create_table(table)
        print(f"Table {table_id} created.")

# Function to load data into BigQuery
def load_data_to_bigquery(client, dataset_id, table_id, data):
    table_ref = client.dataset(dataset_id).table(table_id)
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_APPEND",  # Append to the table if it exists
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    
    # Load the data
    load_job = client.load_table_from_json(data, table_ref, job_config=job_config)
    load_job.result()  # Wait for the job to complete
    print(f"Loaded {len(data)} rows into {dataset_id}.{table_id}.")

# Flatten and process JSON data with cleaning
def process_json_file(file_path, location_name, campaign_id):
    with open(file_path, 'r', encoding='utf-8') as file:
        json_data = json.load(file)
        
    # Helper function to clean the data
    def clean_value(value):
        return None if value == "N/A" else value

    # Process each entry in JSON
    flattened_data = []
    for entry in json_data:
        # Extract values from nested keys and clean them
        search_data = entry.get("search_data", {})
        landing_pages = entry.get("landing_pages", {})
        traffic_data = entry.get("traffic_data", {})
        opportunity_data = entry.get("opportunity", {})

        flattened_entry = {
            # Campaign details first
            "campaign_id": campaign_id,
            "location_name": location_name,
            "date": "2024-10-01",  # Static date for this test

            # Keyword information
            "keyword_id": clean_value(entry.get("keyword_id")),
            "keyword": clean_value(entry.get("keyword")),
            "main_keyword_id": clean_value(entry.get("main_keyword_id")),
            "search_intent": clean_value(entry.get("search_intent")),
            "labels": clean_value(entry.get("labels")),
            "groups": clean_value(entry.get("groups")),

            # Search data
            "search_volume": clean_value(search_data.get("search_volume", 0)),
            "year_over_year": clean_value(search_data.get("year_over_year", 0)),

            # Landing Pages data
            "current_desktop_landing_page": clean_value(landing_pages.get("desktop", {}).get("current", "")),
            "desired_desktop_landing_page": clean_value(landing_pages.get("desktop", {}).get("desired", "")),
            "current_mobile_landing_page": clean_value(landing_pages.get("mobile", {}).get("current", "")),
            "desired_mobile_landing_page": clean_value(landing_pages.get("mobile", {}).get("desired", "")),

            # Ranking data
            "rank_desktop": clean_value(entry.get("ranking_data", {}).get("desktop", {}).get("rank", None)),
            "rank_mobile": clean_value(entry.get("ranking_data", {}).get("mobile", {}).get("rank", None)),

            # Traffic data extraction
            "traffic_sessions": clean_value(traffic_data.get("sessions", 0)),
            "transactions": clean_value(traffic_data.get("ecommerce", {}).get("transactions", 0)),
            "ecommerce_revenue": clean_value(traffic_data.get("ecommerce", {}).get("revenue", 0)),
            "goal_completions": clean_value(traffic_data.get("goals", {}).get("completions", 0)),
            "goal_revenue": clean_value(traffic_data.get("goals", {}).get("revenue", 0)),

            # Opportunity data extraction
            "opportunity_score": clean_value(opportunity_data.get("score", 0)),
            "opportunity_difficulty": clean_value(opportunity_data.get("difficulty", "")),
            "opportunity_avg_cpc": clean_value(opportunity_data.get("avg_cpc", 0)),
            "additional_monthly_sessions": clean_value(opportunity_data.get("additional_monthly_sessions", 0))
        }
        flattened_data.append(flattened_entry)
    
    return flattened_data

# Main function
def main():
    # Ensure the BigQuery table exists
    create_bigquery_table(client, dataset_id, table_id, schema)
    
    all_data = []
    
    # Iterate over campaigns and load data
    for campaign in campaigns:
        location_name = campaign["Name"]
        campaign_id = campaign["ID"]
        
        # File path based on location name
        file_path = f"keywords_{location_name.replace(' ', '_')}_2024-10-01.json"
        
        if os.path.exists(file_path):
            print(f"Processing file: {file_path}")
            flattened_data = process_json_file(file_path, location_name, campaign_id)
            all_data.extend(flattened_data)
        else:
            print(f"File not found: {file_path}")
    
    # Load all collected data to BigQuery
    if all_data:
        load_data_to_bigquery(client, dataset_id, table_id, all_data)
    else:
        print("No data to load.")

if __name__ == "__main__":
    main()
