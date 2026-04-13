#!/usr/bin/env python3
"""
One-off: fetch ONE Business_Account row from Salesforce via REST query and print raw JSON.
Does not connect to Postgres or insert anything. Safe to delete after you inspect output.

SOQL fields should match app/ingestion/b2b_accounts_pipeline.py (B2B_ACCOUNT_SOQL_FIELDS).
"""
from __future__ import annotations

import json
import sys

import requests

from app.salesforce.auth import get_salesforce_token

# Keep in sync with b2b_accounts_pipeline.B2B_ACCOUNT_SOQL_FIELDS
_SOQL = (
    "SELECT Id, Name, Type, Industry, AnnualRevenue, Phone, Fax, Website, AccountSource, Description, "
    "NumberOfEmployees, OwnerId, ParentId, "
    "BillingStreet, BillingCity, BillingState, BillingPostalCode, BillingCountry, "
    "ShippingStreet, ShippingCity, ShippingState, ShippingPostalCode, ShippingCountry, "
    "RecordTypeId, RecordType.DeveloperName, CreatedDate, LastModifiedDate "
    "FROM Account WHERE RecordType.DeveloperName = 'Business_Account' LIMIT 1"
)

_API_VERSION = "v59.0"


def main() -> None:
    access_token, instance_url = get_salesforce_token()
    url = f"{instance_url}/services/data/{_API_VERSION}/query"
    resp = requests.get(
        url,
        headers={"Authorization": f"Bearer {access_token}"},
        params={"q": _SOQL},
        timeout=120,
    )
    try:
        data = resp.json()
    except Exception:
        print(resp.text, file=sys.stderr)
        sys.exit(1)

    if resp.status_code != 200:
        print(json.dumps(data, indent=2), file=sys.stderr)
        sys.exit(1)

    records = data.get("records") or []
    if not records:
        print("No records returned (empty list).", file=sys.stderr)
        sys.exit(0)

    row = records[0]
    print(json.dumps(row, indent=2, default=str))


if __name__ == "__main__":
    main()

#python scripts/preview_one_b2b_account_from_salesforce.py