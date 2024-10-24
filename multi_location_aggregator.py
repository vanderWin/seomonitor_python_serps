import os
import json
import requests
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from datetime import datetime, timedelta

# Helper function to generate date range
def date_range(start_date, end_date):
    current_date = start_date
    while current_date <= end_date:
        yield current_date.strftime("%Y-%m-%d")
        current_date += timedelta(days=1)

# Load environment variables from .env file if present
if os.path.exists('.env'):
    from dotenv import load_dotenv
    load_dotenv()

# Set the path to your service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "organic-data-361613-c0202d1cabeb.json"

# Initialize BigQuery client
project_id = os.getenv("PROJECT_ID")
dataset_id = os.getenv("DATASET_ID")
table_id = os.getenv("TABLE_ID")
api_key = os.getenv("API_KEY")
client = bigquery.Client(project=project_id)

# Campaigns data to iterate over
campaigns = [
    {"Name": "United Kingdom", "ID": 313717},
    {"Name": "Belfast", "ID": 314477},
    {"Name": "Birmingham", "ID": 314470},
    {"Name": "Bristol", "ID": 314479},
    {"Name": "Edinburgh", "ID": 314474},
    {"Name": "Liverpool", "ID": 314478},
    {"Name": "London", "ID": 314469},
    {"Name": "Milton Keynes", "ID": 314472},
    {"Name": "Newcastle upon Tyne", "ID": 314476},
    {"Name": "Norwich", "ID": 314473},
    {"Name": "Plymouth", "ID": 314475},
    {"Name": "Wolverhampton", "ID": 314471}
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

# Function to create or update the BigQuery table
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

# Function to fetch data for each campaign and save it to a JSON file
def fetch_data(campaigns, specified_date):
    for campaign in campaigns:
        campaign_name = campaign["Name"].replace(" ", "_")
        campaign_id = campaign["ID"]
        
        # URL for the API call
        url = f"https://apigw.seomonitor.com/v3/rank-tracker/v3.0/keywords?campaign_id={campaign_id}&start_date={specified_date}&end_date={specified_date}&include_all_groups=true&limit=1000"
        
        # Headers for the request
        headers = {
            "Accept": "*/*",
            "Authorization": api_key
        }
        
        # Making the request
        response = requests.get(url, headers=headers, timeout=120)
        
        # Check the status code and save the response as JSON
        if response.status_code == 200:
            print(f"API call successful for {campaign_name}")
            json_content = response.json()
            
            # Save JSON data to a file named after the campaign
            output_file = f"keywords_{campaign_name}_{specified_date}.json"
            with open(output_file, 'w', encoding='utf-8') as json_file:
                json.dump(json_content, json_file, ensure_ascii=False, indent=4)
            print(f"Data saved to {output_file}")
        else:
            print(f"API call failed for {campaign_name} with status code {response.status_code}")
            print("Response content:", response.text)

# Flatten and process JSON data with cleaning
def process_json_file(file_path, location_name, campaign_id, specified_date):
    with open(file_path, 'r', encoding='utf-8') as file:
        json_data = json.load(file)
        
    def clean_value(value):
        return None if value == "N/A" else value

    flattened_data = []
    for entry in json_data:
        search_data = entry.get("search_data", {})
        landing_pages = entry.get("landing_pages", {})
        traffic_data = entry.get("traffic_data", {})
        opportunity_data = entry.get("opportunity", {})

        flattened_entry = {
            "campaign_id": campaign_id,
            "location_name": location_name,
            "date": specified_date,  # Use the dynamic date

            "keyword_id": clean_value(entry.get("keyword_id")),
            "keyword": clean_value(entry.get("keyword")),
            "main_keyword_id": clean_value(entry.get("main_keyword_id")),
            "search_intent": clean_value(entry.get("search_intent")),
            "labels": clean_value(entry.get("labels")),
            "groups": clean_value(entry.get("groups")),

            "search_volume": clean_value(search_data.get("search_volume", 0)),
            "year_over_year": clean_value(search_data.get("year_over_year", 0)),

            "current_desktop_landing_page": clean_value(landing_pages.get("desktop", {}).get("current", "")),
            "desired_desktop_landing_page": clean_value(landing_pages.get("desktop", {}).get("desired", "")),
            "current_mobile_landing_page": clean_value(landing_pages.get("mobile", {}).get("current", "")),
            "desired_mobile_landing_page": clean_value(landing_pages.get("mobile", {}).get("desired", "")),

            "rank_desktop": clean_value(entry.get("ranking_data", {}).get("desktop", {}).get("rank", None)),
            "rank_mobile": clean_value(entry.get("ranking_data", {}).get("mobile", {}).get("rank", None)),

            "traffic_sessions": clean_value(traffic_data.get("sessions", 0)),
            "transactions": clean_value(traffic_data.get("ecommerce", {}).get("transactions", 0)),
            "ecommerce_revenue": clean_value(traffic_data.get("ecommerce", {}).get("revenue", 0)),
            "goal_completions": clean_value(traffic_data.get("goals", {}).get("completions", 0)),
            "goal_revenue": clean_value(traffic_data.get("goals", {}).get("revenue", 0)),

            "opportunity_score": clean_value(opportunity_data.get("score", 0)),
            "opportunity_difficulty": clean_value(opportunity_data.get("difficulty", "")),
            "opportunity_avg_cpc": clean_value(opportunity_data.get("avg_cpc", 0)),
            "additional_monthly_sessions": clean_value(opportunity_data.get("additional_monthly_sessions", 0))
        }
        flattened_data.append(flattened_entry)
    
    return flattened_data


# Function to load data into BigQuery
def load_data_to_bigquery(client, dataset_id, table_id, data):
    table_ref = client.dataset(dataset_id).table(table_id)
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_APPEND",
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    
    load_job = client.load_table_from_json(data, table_ref, job_config=job_config)
    load_job.result()
    print(f"Loaded {len(data)} rows into {dataset_id}.{table_id}.")

# Main function to handle both fetch and load
# Main function to handle both fetch and load
def main():
    # Define date range
    start_date = datetime.strptime("2024-09-25", "%Y-%m-%d") # Backdating
    end_date = datetime.now()

    create_bigquery_table(client, dataset_id, table_id, schema)
    
    # Iterate over each date in the range
    for specified_date in date_range(start_date, end_date):
        print(f"Processing data for date: {specified_date}")

        # Fetch data for all campaigns for the current date
        fetch_data(campaigns, specified_date)
        
        # Load fetched data to BigQuery
        all_data = []
        for campaign in campaigns:
            location_name = campaign["Name"]
            campaign_id = campaign["ID"]
            file_path = f"keywords_{location_name.replace(' ', '_')}_{specified_date}.json"
            
            if os.path.exists(file_path):
                print(f"Processing file: {file_path}")
                # Pass the dynamic date to the function
                flattened_data = process_json_file(file_path, location_name, campaign_id, specified_date)
                all_data.extend(flattened_data)

                # Remove the JSON file after processing to save space
                os.remove(file_path)
                print(f"File {file_path} deleted after processing.")
            else:
                print(f"File not found: {file_path}")
        
        if all_data:
            load_data_to_bigquery(client, dataset_id, table_id, all_data)
        else:
            print(f"No data to load for date: {specified_date}")

if __name__ == "__main__":
    main()
