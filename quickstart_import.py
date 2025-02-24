import weaviate
from weaviate.classes.init import Auth
import json, os
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

# Read the local havona_schema.json file
with open('havona_schema.json', 'r') as file:
    data = json.load(file)

questions = client.collections.get("HavonaTestCohere")

with questions.batch.dynamic() as batch:
    deal = data["DttMasterJsonDeal"]
    # Create object matching the schema structure, without 'id' fields
    trade_object = {
        "reference": deal["id"],  # renamed from id
        "tradeContract": {
            "reference": deal["id"],  # renamed from id
            "dtt": deal["dtt"]["DigitalTradeTransaction"],
            "contractNo": deal["contractNo"],
            "contractDate": deal["contractDate"],
            "status": deal["status"]["DigitalTradeTransactionStatus"],
            "incoTerms": deal["incoTerms"]["IncoTerm"]
        },
        "productGoods": {
            "name": deal["productGoods"]["name"],
            "quantity": deal["productGoods"]["quantity"],
            "price": deal["productGoods"]["price"]["MoneyAmount"],
            "subtotal": deal["productGoods"]["subtotal"],
            "weightUnit": deal["productGoods"]["weightUnit"],
            "hsCode": deal["productGoods"]["hsCode"],
            "originCountry": deal["productGoods"]["originCountry"],
            "originLocation": deal["productGoods"]["originLocation"]
        },
        "delivery": {
            "partialShipment": deal["delivery"]["partialShipment"],
            "transhipment": deal["delivery"]["transhipment"],
            "packaging": deal["delivery"]["packaging"],
            "latestShipmentDate": deal["delivery"]["latestShipmentDate"],
            "latestDeliveryDate": deal["delivery"]["latestDeliveryDate"]
        },
        "transports": {
            "loadLocation": deal["transports"]["loadLocation"]["Location"],
            "UNLocationCode": deal["transports"]["UNLocationCode"],
            "dischargeLocation": deal["transports"]["dischargeLocation"]["Location"],
            "plannedDepartureDate": deal["transports"]["plannedDepartureDate"],
            "plannedArrivalDate": deal["transports"]["plannedArrivalDate"]
        }
    }
    batch.add_object(trade_object)

failed_objects = questions.batch.failed_objects
if failed_objects:
    print(f"Number of failed imports: {len(failed_objects)}")
    print(f"First failed object: {failed_objects[0]}")

client.close()  # Free up resources