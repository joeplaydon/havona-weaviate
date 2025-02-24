import weaviate
from weaviate.classes.init import Auth
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get credentials from environment variables
wcd_url = os.environ["WCD_URL"]
wcd_api_key = os.environ["WCD_API_KEY"]
cohere_api_key = os.environ["COHERE_API_KEY"]

client = weaviate.connect_to_weaviate_cloud(
    cluster_url=wcd_url,                                    # Replace with your Weaviate Cloud URL
    auth_credentials=Auth.api_key(wcd_api_key),             # Replace with your Weaviate Cloud key
    headers={"X-Cohere-Api-Key": cohere_api_key}
)

# Get the collection config
collection = client.collections.get("HavonaMasterJSON")

print("Collection Configuration:")
print(f"Name: {collection.name}")


# Check if collection exists and is ready
print(f"\nCollection exists: {client.collections.exists('HavonaMasterJSON')}")
print(f"Client is ready: {client.is_ready()}")

client.close()  # Free up resources