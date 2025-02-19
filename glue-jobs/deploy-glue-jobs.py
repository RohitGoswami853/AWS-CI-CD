import boto3
import json

glue = boto3.client("glue")

# Load T1 job config
with open("glue-jobs/job_t1/job-config.json", "r") as file:
    job_t1_config = json.load(file)

# Create/Update T1 job
glue.create_job(**job_t1_config)
print("✅ Glue Job T1 updated successfully.")

# Modify config for T2
job_t2_config = job_t1_config.copy()
job_t2_config["JobName"] = "T2"
job_t2_config["Command"]["ScriptLocation"] = "s3://nct-inbound-bucket-1235/glue-jobs/job_t2/script.py"

# Create/Update T2 job
glue.create_job(**job_t2_config)
print("✅ Glue Job T2 updated successfully.")
