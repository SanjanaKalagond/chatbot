import requests
from app.config import (
    SALESFORCE_CLIENT_ID,
    SALESFORCE_CLIENT_SECRET
)

INSTANCE_URL = "https://tonal--full.sandbox.my.salesforce.com"
TOKEN_URL = f"{INSTANCE_URL}/services/oauth2/token"

def get_salesforce_token():
    payload = {
        "grant_type": "client_credentials",
        "client_id": SALESFORCE_CLIENT_ID,
        "client_secret": SALESFORCE_CLIENT_SECRET
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    response = requests.post(TOKEN_URL, data=payload, headers=headers).json()
    if "access_token" not in response:
        raise Exception(f"Salesforce auth failed: {response}")
    return response["access_token"], INSTANCE_URL