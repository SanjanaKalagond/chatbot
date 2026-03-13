import requests
import os
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("SALESFORCE_CLIENT_ID")
CLIENT_SECRET = os.getenv("SALESFORCE_CLIENT_SECRET")
url = "https://tonal--full.sandbox.my.salesforce.com/services/oauth2/token"

payload = {
    "grant_type": "client_credentials",
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
}

headers = {
    "Content-Type": "application/x-www-form-urlencoded"
}

response = requests.post(url, data=payload, headers=headers)

print(f"Status Code: {response.status_code}")
print(response.text)