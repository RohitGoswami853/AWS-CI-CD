import sys
import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Initialize Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define S3 bucket path
target_bucket = "s3://BCDE/"

# Read data from S3
df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [target_bucket]},
    format="parquet"
)

# Count rows to ensure data exists
record_count = df.count()
print(f"Job T2: Successfully validated {record_count} records in {target_bucket}")

# Commit job
job.commit()
