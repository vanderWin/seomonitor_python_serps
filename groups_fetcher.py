import os
import json
import requests
import pandas as pd
from datetime import datetime

# Load environment variables from .env file if present
if os.path.exists('.env'):
    from dotenv import load_dotenv
    load_dotenv()

# Initialize necessary variables
api_key = os.getenv("API_KEY")
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

# File to store the results
output_file = "groups.csv"

# Define today's date
specified_date = datetime.now().strftime("%Y-%m-%d")

# Function to fetch and process group data
def fetch_group_data(campaign_id, specified_date):
    url = f"https://apigw.seomonitor.com/v3/rank-tracker/v3.0/groups/data?campaign_id={campaign_id}&start_date={specified_date}&end_date={specified_date}"
    
    headers = {
        "Accept": "*/*",
        "Authorization": api_key
    }
    
    # Making the request
    response = requests.get(url, headers=headers, timeout=120)
    
    if response.status_code == 200:
        print(f"API call successful for campaign ID {campaign_id} on {specified_date}")
        json_content = response.json()
        
        # Flatten the JSON and return as DataFrame
        flattened_df = pd.json_normalize(json_content)
        return flattened_df
    else:
        print(f"API call failed for campaign ID {campaign_id} with status code {response.status_code}")
        return pd.DataFrame()  # Return an empty DataFrame if there's an issue

# Main function
def main():
    # Initialize all_data list here
    all_data = []

    # Fetch data for all campaigns for the current date
    for campaign in campaigns:
        campaign_id = campaign["ID"]
        
        # Fetch and flatten group data
        flattened_df = fetch_group_data(campaign_id, specified_date)
        
        if not flattened_df.empty:
            # Append the flattened data to the list
            all_data.append(flattened_df)

    # Concatenate all data from different campaigns into a single DataFrame
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)

        # Append to the CSV file
        if not os.path.exists(output_file):
            # If the file doesn't exist, write headers
            combined_df.to_csv(output_file, mode='w', index=False, header=True)
        else:
            # If the file exists, append without headers
            combined_df.to_csv(output_file, mode='a', index=False, header=False)
            
        print(f"Data for {specified_date} appended to {output_file}.")
    else:
        print(f"No data to append for {specified_date}.")

if __name__ == "__main__":
    main()
