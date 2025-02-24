import pydgraph
import json
import os
from dotenv import load_dotenv
import weaviate
from weaviate.classes.init import Auth
import requests

load_dotenv()

# Query your data from dgraph
def query_transactions():
    query = """
    query {
        queryMember {
            id
            collaborators {
                companyPublicKey
                dataOfBirth
            }
            companyName
            contactDetails
        }
    }
    """
    
    headers = {
        'Content-Type': 'application/json',
        'X-Auth-Token': os.environ["DGRAPH_API_KEY"]
    }
    response = requests.post(
        os.environ["DGRAPH_URL"],
        json={'query': query},
        headers=headers
    )
    print("Response:", response.text)  # Debug print
    return response.json()

def main():
    weaviate_client = None
    
    try:
        # Get data from dgraph
        data = query_transactions()
        members = data["data"]["queryMember"]  # Fix the data access
        
        # Initialize Weaviate client
        weaviate_client = weaviate.connect_to_weaviate_cloud(
            cluster_url=os.environ["WCD_URL"],
            auth_credentials=Auth.api_key(os.environ["WCD_API_KEY"]),
            headers={"X-Cohere-Api-Key": os.environ["COHERE_API_KEY"]},
        )
        
        # Import to Weaviate
        transactions = weaviate_client.collections.get("HavonaTest")
        
        with transactions.batch.dynamic() as batch:
            for member in members:
                # Remove id and prepare object for Weaviate
                weaviate_object = {
                    'companyName': member['companyName'],
                    'contactDetails': member['contactDetails'] if member['contactDetails'] else "",
                    'collaborators': []  # Empty array since current data has null values
                }
                batch.add_object(weaviate_object)
                if batch.number_errors > 0:
                    print("Batch import stopped due to errors.")
                    break
                    
        failed_objects = transactions.batch.failed_objects
        if failed_objects:
            print(f"Number of failed imports: {len(failed_objects)}")
            print(f"First failed object: {failed_objects[0]}")
            
    finally:
        if weaviate_client:
            weaviate_client.close()

if __name__ == "__main__":
    main() 