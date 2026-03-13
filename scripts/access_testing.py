import requests
import os
from dotenv import load_dotenv

load_dotenv()
CLIENT_ID = os.getenv("SALESFORCE_CLIENT_ID")
CLIENT_SECRET = os.getenv("SALESFORCE_CLIENT_SECRET")
INSTANCE_URL = "https://tonal--full.sandbox.my.salesforce.com"
TOKEN_URL = f"{INSTANCE_URL}/services/oauth2/token"

payload = {
    "grant_type": "client_credentials",
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET
}

headers_auth = {
    "Content-Type": "application/x-www-form-urlencoded"
}

token_response = requests.post(TOKEN_URL, data=payload, headers=headers_auth)

if token_response.status_code == 200:
    access_token = token_response.json().get("access_token")
    print("Successfully authenticated!")

    headers_query = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    objects = ["Account", "Contact", "Opportunity", "Case"]

    for obj in objects:
        query_url = f"{INSTANCE_URL}/services/data/v60.0/query?q=SELECT+COUNT()+FROM+{obj}"
        data_response = requests.get(query_url, headers=headers_query)

        if data_response.status_code == 200:
            result = data_response.json()
            print(f"{obj}:", result["totalSize"])
        else:
            print(f"{obj} query failed:", data_response.status_code)
            print(data_response.text)

else:
    print(f"Auth Failed: {token_response.status_code}")
    print(token_response.text)