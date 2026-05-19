import boto3
import os

def get_s3_client():
    return boto3.client("s3", region_name=os.getenv("AWS_REGION", "ap-south-1"))

s3 = get_s3_client()