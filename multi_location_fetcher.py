# Script to fetch specified data and store them as local json objects

import os
import requests
import json
from datetime import datetime

# Load environment variables from .env file if present
if os.path.exists('.env'):
    from dotenv import load_dotenv
    load_dotenv()

# Fetch API key from environment variables
api_key = os.getenv("API_KEY")

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

# Define the specific date for the API call
specified_date = "2024-10-01"

# Iterate over each campaign and make an API call
for campaign in campaigns:
    campaign_name = campaign["Name"].replace(" ", "_")  # Replace spaces with underscores for file naming
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
        json_content = response.json()  # Parse the JSON response
        
        # Save JSON data to a file named after the campaign
        output_file = f"keywords_{campaign_name}_2024-10-01.json"
        with open(output_file, 'w', encoding='utf-8') as json_file:
            json.dump(json_content, json_file, ensure_ascii=False, indent=4)
        print(f"Data saved to {output_file}")
    else:
        print(f"API call failed for {campaign_name} with status code {response.status_code}")
        print("Response content:", response.text)
