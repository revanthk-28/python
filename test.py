import requests
from elasticsearch import Elasticsearch, helpers
import json

# Configuration
api_url = 'http://localhost:9200/_cluster/health?pretty'  # Replace with the actual API endpoint
es_host = 'http://localhost:9200'  # Replace with your Elasticsearch host URL
es_index = 'test_index'  # Replace with your desired Elasticsearch index name

# Initialize Elasticsearch client
es = Elasticsearch([es_host])

# Function to fetch data from API
def fetch_data_from_api(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Check for HTTP errors
        return response.json()  # Assuming the API returns JSON data
    except requests.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return None

# Function to send data to Elasticsearch
def send_data_to_elasticsearch(index, data):
    try:
        # Bulk indexing data
        actions = [
            {
                "_index": index,
                "_source": item
            }
            for item in data
        ]
        helpers.bulk(es, actions)
        print(f"Data successfully indexed to {index}")
    except Exception as e:
        print(f"Error sending data to Elasticsearch: {e}")

# Main function to orchestrate the process
def main():
    data = fetch_data_from_api(api_url)
    if data:
        send_data_to_elasticsearch(es_index, data)

if __name__ == "__main__":
    main()

