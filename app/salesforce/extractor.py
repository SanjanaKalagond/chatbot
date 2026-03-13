from app.salesforce.auth import get_salesforce_token
from app.database.sync_metadata import get_last_sync

def extract_object_soql(object_name):
    last_sync = get_last_sync(object_name)
    
    field_map = {
        "Account": "Id, Name, Type, Industry, AnnualRevenue, Phone, Website, BillingCity, LastModifiedDate",
        "Contact": "Id, FirstName, LastName, Email, Phone, AccountId, LastModifiedDate",
        "Opportunity": "Id, Name, Amount, StageName, CloseDate, AccountId, LastModifiedDate",
        "Case": "Id, Subject, Status, Priority, Description, AccountId, LastModifiedDate",
        "Order": "Id, AccountId, EffectiveDate, Status, TotalAmount, LastModifiedDate",
        "OrderItem": "Id, OrderId, Quantity, UnitPrice, TotalPrice, LastModifiedDate",
        "ContentVersion": "Id, VersionData, FirstPublishLocationId, FileExtension, Title, LastModifiedDate",
        "Task": "Id, Subject, Description, WhoId, WhatId, Status, ActivityDate, LastModifiedDate",
        "Activity": "Id, Subject, Description, WhoId, WhatId, LastModifiedDate"
    }

    fields = field_map.get(object_name, "Id, LastModifiedDate")
    soql = f"SELECT {fields} FROM {object_name}"
    
    if last_sync:
        sync_str = last_sync.strftime("%Y-%m-%dT%H:%M:%SZ")
        soql += f" WHERE LastModifiedDate > {sync_str}"
        
    return soql