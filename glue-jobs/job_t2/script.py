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
SOURCE_BUCKET = "nct-outbound-bucket-1234"  # Output from job_t1
TARGET_BUCKET = "nct-final-bucket-1236"  # Final processed data

# Initialize S3 client
s3 = boto3.client("s3")

# List objects in the source bucket
objects = s3.list_objects_v2(Bucket=SOURCE_BUCKET)

if "Contents" in objects:
    for obj in objects["Contents"]:
        copy_source = {"Bucket": SOURCE_BUCKET, "Key": obj["Key"]}
        s3.copy_object(Bucket=TARGET_BUCKET, Key=obj["Key"], CopySource=copy_source)
        print(f"✅ Processed and copied {obj['Key']} from {SOURCE_BUCKET} to {TARGET_BUCKET}")

print("🚀 Job T2 completed successfully.")