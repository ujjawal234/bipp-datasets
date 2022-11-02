# import json

# import boto3
from botocore.exceptions import NoCredentialsError


def wasabi_upload(s3, bucket_name, wasabi_path, local_file_path):
    """
    function to upload files from local to wasabi, it will create folder path for the dataset that we
    are wroking on and add the files from local path.

    params:
    s3 : Element which returns after authentication using wasabi_auth function
    bucket_name: dev-data or prod-data
    wasabi_path: dataset_name + (data path from monorepo) | Ex: agcensus/raw/andhra_pradesh/anantapur/data.csv
    local_file_path: path of file in the system in which processing is being done.
    """
    for bucket in s3.list_buckets()["Buckets"]:
        if bucket["Name"] == bucket_name:
            print(bucket_name + " bucket already exists")
            wasabi_bucket = bucket_name
            pass
        else:
            wasabi_bucket = s3.create_bucket(Bucket=bucket_name)

    def upload_to_wasabi(file_name, bucket, data):
        """
        Function to upload a dataset on to the wasabi cloud
        """
        try:
            s3.put_object(Bucket=bucket_name, Key=file_name, Body=data)
            print("Upload Successful")
            return True
        except FileNotFoundError:
            print("The file was not found")
            return False
        except NoCredentialsError:
            print("Credentials not available")
            return False

    data = open(local_file_path, "rb")
    wasabi_bucket = bucket_name
    # invoking the upload function to wasabi or amazon s3.
    upload_to_wasabi(wasabi_path, wasabi_bucket, data)
    print("file uploaded to wasabi on this path: ", wasabi_path)
