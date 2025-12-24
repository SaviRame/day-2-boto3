import boto3
import json


s3_client = boto3.client('s3')

#creating an s3 bucket
def bucket_creation():
   response = s3_client.create_bucket(Bucket = 'savitha-labs-boto3-day2')
   print(response)

#uploading files to the s3 bucket
def file_upload():
    s3_client.upload_file(r"C:\Users\jmsav\oubt\day-1\yellow_tripdata_2025-08.parquet", 'savitha-labs-boto3-day2', 'yellow_tripdata_2025-08.parquet')
    s3_client.upload_file(r"C:\Users\jmsav\oubt\day-1\taxi_zone_lookup.csv", 'savitha-labs-boto3-day2', 'taxi_zone_lookup.csv')

#policy to allow all access 
def policy():
    bucket_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "allow specific user full access",
                "Effect": "Allow",
                "Principal": {"AWS": "arn:aws:iam::765017559809:user/boto3"},
                "Action": "s3:*",
                "Resource": "arn:aws:s3:::savitha-labs-boto3-day2/*"
            }
        ]
    }

    policy = json.dumps(bucket_policy)
    s3_client.put_bucket_policy(Bucket='savitha-labs-boto3-day2', Policy=policy)

#calling the functions
#bucket_creation()
#file_upload()
#policy()

resp = s3_client.get_bucket_policy(Bucket="savitha-labs-boto3-day2")
print(json.loads(resp["Policy"]))

# adding tagging to the bucket
BUCKET = "savitha-labs-boto3-day2"

def tag_existing_object(key: str, owner: str, classification: str, domain: str):
    s3_client.put_object_tagging(
        Bucket=BUCKET,
        Key=key,
        Tagging={
            "TagSet": [
                {"Key": "Owner", "Value": owner},
                {"Key": "Classification", "Value": classification},
                {"Key": "Domain", "Value": domain},
            ]
        },
    )
    print(f"Tagged: {key}")

def show_tags(key: str):
    resp = s3_client.get_object_tagging(Bucket=BUCKET, Key=key)
    print(f"Tags for {key}: {resp['TagSet']}")

# Tag your two already-uploaded objects

tag_existing_object(
    "yellow_tripdata_2023-08.parquet",
    "savitha",
    "Internal",
    "Transport"
)

tag_existing_object(
    "taxi_zone_lookup.csv",
    "savitha",
    "Internal",
    "Transport"
)


# Verify
show_tags("yellow_tripdata_2023-08.parquet")
show_tags("taxi_zone_lookup.csv")