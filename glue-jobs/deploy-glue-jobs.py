import boto3

glue_client = boto3.client("glue")

def update_glue_job(job_name, script_location):
    response = glue_client.update_job(
        JobName=job_name,
        JobUpdate={
            "Command": {"Name": "glueetl", "ScriptLocation": script_location}
        }
    )
    print(f"Updated {job_name}: {response}")

# Update Job T1
update_glue_job("T1", "s3://your-bucket/glue-jobs/job_t1/script.py")

# Update Job T2 (Triggered automatically)
update_glue_job("T2", "s3://your-bucket/glue-jobs/job_t2/script.py")
