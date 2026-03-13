from app.salesforce.auth import get_salesforce_token
import requests
import sys

OBJECTS = [
    "Account",
    "Contact",
    "Opportunity",
    "Order",
    "OrderItem",
    "Case",
    "Task",
    "Activity",
    "ContentVersion",
    "ContentDocument",
    "ContentDocumentLink"
]


def check_all_counts():

    access_token, instance_url = get_salesforce_token()

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    query_url = f"{instance_url}/services/data/v59.0/query"

    print("\nFetching Salesforce Object Counts\n")
    sys.stdout.flush()

    for obj in OBJECTS:

        try:

            soql = f"SELECT COUNT() FROM {obj}"

            response = requests.get(
                query_url,
                headers=headers,
                params={"q": soql},
                timeout=30
            )

            if response.status_code != 200:
                print(f"{obj}: ERROR {response.status_code} → {response.text}")
                continue

            data = response.json()

            if "totalSize" in data:
                print(f"{obj}: {data['totalSize']}")
            else:
                print(f"{obj}: Unexpected response → {data}")

        except Exception as e:
            print(f"{obj}: Failed → {str(e)}")

        sys.stdout.flush()


if __name__ == "__main__":
    check_all_counts()