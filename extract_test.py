from app.salesforce.extractor import extract_object
accounts = extract_object("Account")
print("Total accounts extracted:", len(accounts))