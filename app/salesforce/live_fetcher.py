import requests
from app.salesforce.auth import get_salesforce_token

def fetch_live_from_sf(soql):
    access_token, instance_url = get_salesforce_token()
    url = f"{instance_url}/services/data/v59.0/query"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"q": soql}
    response = requests.get(url, headers=headers, params=params)
    data = response.json()
    return data.get("records", [])