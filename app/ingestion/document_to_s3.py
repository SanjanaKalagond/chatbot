import boto3
from app.config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_BUCKET_NAME

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

def upload_document(local_path, s3_key):
    s3.upload_file(local_path, S3_BUCKET_NAME, s3_key)
    return f"s3://{S3_BUCKET_NAME}/{s3_key}"