import sys
import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Get arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Glue and Spark Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define source and target S3 buckets
source_bucket = "s3://ABCD/"
target_bucket = "s3://BCDE/"

# Read data from source S3 bucket
df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [source_bucket]},
    format="parquet"
)

# Write data to target S3 bucket
glueContext.write_dynamic_frame.from_options(
    frame=df,
    connection_type="s3",
    connection_options={"path": target_bucket},
    format="parquet"
)

# Function to update or create Job T2
def update_or_create_job_t2():
    glue_client = boto3.client("glue")
    job_name = "T2"
    script_location = "s3://your-bucket/glue-jobs/job_t2/script.py"

    # Check if Job T2 exists
    try:
        glue_client.get_job(JobName=job_name)
        print(f"Updating {job_name}...")
        glue_client.update_job(
            JobName=job_name,
            JobUpdate={
                "Command": {"Name": "glueetl", "ScriptLocation": script_location}
            }
        )
    except glue_client.exceptions.EntityNotFoundException:
        print(f"Creating {job_name}...")
        glue_client.create_job(
            Name=job_name,
            Role="AWSGlueServiceRole",
            Command={"Name": "glueetl", "ScriptLocation": script_location},
            GlueVersion="3.0"
        )

# Call function to update or create Job T2
update_or_create_job_t2()

# Commit job
job.commit()
