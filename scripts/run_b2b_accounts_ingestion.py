import argparse
import sys

from app.ingestion.b2b_accounts_pipeline import ingest_b2b_accounts


def main():
    p = argparse.ArgumentParser(description="Ingest B2B Salesforce accounts into b2b_accounts.")
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Ingest at most N rows (test); does not update sync_metadata.",
    )
    p.add_argument(
        "--full-refresh",
        action="store_true",
        help="TRUNCATE b2b_accounts, reset Account_B2B sync cursor, then load all B2B accounts (upsert by Id).",
    )
    args = p.parse_args()

    print("Starting B2B Account ingestion (RecordType = Business_Account only)")
    sys.stdout.flush()
    ingest_b2b_accounts(limit=args.limit, full_refresh=args.full_refresh)


if __name__ == "__main__":
    main()
