import weaviate
from weaviate.classes.init import Auth
import os
from dotenv import load_dotenv

load_dotenv()

# Best practice: store your credentials in environment variables
wcd_url = os.environ["WCD_URL"]
wcd_api_key = os.environ["WCD_API_KEY"]
cohere_api_key = os.environ["COHERE_API_KEY"]

client = weaviate.connect_to_weaviate_cloud(
    cluster_url=wcd_url,                                    # Replace with your Weaviate Cloud URL
    auth_credentials=Auth.api_key(wcd_api_key),             # Replace with your Weaviate Cloud key
    headers={"X-Cohere-Api-Key": cohere_api_key},           # Replace with your Cohere API key
)

questions = client.collections.get("HavonaTestCohere")

response = questions.generate.near_text(
    query="deal",
    limit=4,
    grouped_task="What currency is being used for this transaction and what are the payment terms? Is transhipment allowed for this shipment from Baltimore to Cebu City?"
)

print(response.generated)  # Inspect the generated text

client.close()  # Free up resources

