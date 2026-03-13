import requests
import sys

def run_query_stream(instance_url, access_token, soql):
    url = f"{instance_url}/services/data/v59.0/query"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"q": soql}
    
    response = requests.get(url, headers=headers, params=params).json()
    
    if "records" in response:
        yield response["records"]
        
        while not response.get("done", True):
            next_url = instance_url + response["nextRecordsUrl"]
            response = requests.get(next_url, headers=headers).json()
            if "records" in response:
                yield response["records"]
            else:
                break