# import json

# import boto3
# from logging import logger

from botocore.exceptions import ClientError


def wasabi_download(s3, bucket_name, objects, local_storage_path):
    """
    Function to read and download the files from wasabi cloud storage to local

    Params:
    s3 : s3 client which is invoked from wasabi_auth function
    bucket_name : bucket from which data needs to be downloaded
    objects : list of file paths from bucket_list function
    local_storage_path : path to store the file on local
    """

    pass


def bucket_list(s3, bucket_name, prefix=None):
    """
    Lists the objects in a bucket, optionally filtered by a prefix.

    :param bucket_name: The bucket to query.
    :param prefix: When specified, only objects that start with this prefix are listed.
    :return: The list of objects.
    """
    bucket = s3.Bucket(bucket_name)
    try:
        if not prefix:
            objects = list(bucket.objects.all())
        else:
            objects = list(bucket.objects.filter(Prefix=prefix))
        # logger.info("Got objects %s from bucket '%s'",
        #             [o.key for o in objects], bucket_name.name)
    except ClientError:
        # logger.exception("Couldn't get objects for bucket '%s'.", bucket_name.name)
        raise
    else:
        return objects


def read_data_codeboook(s3, bucket_name, object_list):
    """
    Function to read the csv file and its corresponding codebook
    """
    for each in object_list:
        file_key = s3.get_object(Bucket=bucket_name, key=each)
        print(file_key)
    pass
