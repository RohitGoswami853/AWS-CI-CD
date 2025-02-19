import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# Initialize Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = SparkSession(sc)
job = Job(glueContext)

# S3 Buckets
SOURCE_BUCKET = "nct-inbound-bucket-1235"
TARGET_BUCKET = "nct-outbound-bucket-1234"

# Initialize S3 client
s3 = boto3.client("s3")

# List objects in the source bucket
objects = s3.list_objects_v2(Bucket=SOURCE_BUCKET)

if "Contents" in objects:
    for obj in objects["Contents"]:
        copy_source = {"Bucket": SOURCE_BUCKET, "Key": obj["Key"]}
        s3.copy_object(Bucket=TARGET_BUCKET, Key=obj["Key"], CopySource=copy_source)
        print(f"✅ Copied {obj['Key']} from {SOURCE_BUCKET} to {TARGET_BUCKET}")

print("🚀 Job T1 completed successfully.")
