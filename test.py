import requests
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import ElasticsearchException
import json

# Configuration
api_url = 'https://api.example.com/data'  # Replace with your API endpoint
cloud_id = 'your-cloud-id'  # Replace with your Elasticsearch Cloud ID
username = 'your-username'  # Replace with your Elasticsearch username
password = 'your-password'  # Replace with your Elasticsearch password
es_index = 'my_index'  # Replace with your desired Elasticsearch index name

# Initialize Elasticsearch client
def create_es_client():
    es = Elasticsearch(
        cloud_id=cloud_id,
        basic_auth=(username, password),
        # Uncomment the following line if you are using a self-signed certificate
        # verify_certs=False
    )
    return es

# Function to fetch data from API
def fetch_data_from_api(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an HTTPError for bad responses
        return response.json()  # Assuming the API returns JSON data
    except requests.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return None

# Function to send data to Elasticsearch
def send_data_to_elasticsearch(es, index, data):
    try:
        # Prepare data for bulk indexing
        actions = [
            {
                "_index": index,
                "_source": item
            }
            for item in data
        ]
        
        # Perform bulk indexing
        response, failed = helpers.bulk(es, actions, raise_on_error=False)
        
        # Check for errors
        if failed > 0:
            print(f"{failed} documents failed to index.")
            # Print out the detailed error information
            for item in response:
                if isinstance(item, dict) and 'error' in item:
                    print(f"Failed to index document: {item['error']}")
        else:
            print(f"Data successfully indexed to {index}")
    except ElasticsearchException as e:
        print(f"Error sending data to Elasticsearch: {e}")

# Main function to orchestrate the process
def main():
    es = create_es_client()
    data = fetch_data_from_api(api_url)
    if data:
        # Ensure data is in the correct format for Elasticsearch
        if isinstance(data, dict):  # Check if data is a dictionary
            data = [data]  # Convert to list with one item
        elif not isinstance(data, list):  # Check if data is neither a list nor a dict
            print("Unexpected data format from API.")
            return
        
        send_data_to_elasticsearch(es, es_index, data)

if __name__ == "__main__":
    main()
