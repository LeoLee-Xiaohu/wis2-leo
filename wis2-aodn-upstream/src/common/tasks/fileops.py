import os
import io
import botocore
from prefect import task, get_run_logger
from prefect_aws.s3 import S3Bucket

from common.lib.util import validate_absolute_path, mkdir_p


def s3_url(bucket: S3Bucket, key: str = '') -> str:
    """Return the complete S3 URL for a key in a bucket, including any folder prefix"""
    return f"s3://{os.path.join(bucket.bucket_name, bucket.bucket_folder, key)}"


@task
def copy_file(src_bucket: S3Bucket, src_key: str, dest_bucket: S3Bucket, dest_key: str, delete_src: bool = False):
    """Copy a file from src to dest, both specified as S3Bucket blocks and keys.
    If delete_src=True, the src file is deleted after successful copy.
    """
    src_bucket.copy_object(src_key, dest_key, dest_bucket)

    # Optionally delete the source file
    if delete_src:
        delete_file(src_bucket, src_key)


@task
def download_file(src_bucket: S3Bucket, src_key: str, dest: str):
    """Download a file from an S3 bucket to a local path

    :param S3Bucket src_bucket: An open S3Bucket block for the source bucket.
    :param str src_key: The key of the file to download
    :param str dest: The full local path to store the file. Must be absolute path.
    """

    logger = get_run_logger()

    # make sure dest is an absolute path, the directory exists, but the file itself doesn't
    validate_absolute_path(dest)
    assert not os.path.exists(dest), f"Download destination '{dest}' already exists!"
    dest_dir = os.path.dirname(dest)
    if not os.path.isdir(dest_dir):
        logger.debug(f"Creating download directory '{dest_dir}'")
        mkdir_p(dest_dir)

    try:
        src_bucket.download_object_to_path(src_key, dest)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            raise FileNotFoundError(f"File '{s3_url(src_bucket, src_key)}' does not exist.")
        else:
            raise

@task
def download_file_to_memory(src_bucket: S3Bucket, src_key: str) -> io.BytesIO:
    """
    Download a file from an S3 bucket into memory.

    :param S3Bucket src_bucket: An S3Bucket block already loaded for the source bucket.
    :param str src_key: The key/path of the file to download (e.g. "emails/123.eml").
    :return: io.BytesIO containing the file’s raw bytes.
    """
    logger = get_run_logger()

    if not src_key:
        raise ValueError("`src_key` must be a non-empty S3 key string")

    logger.debug(f"Downloading '{src_key}' into memory")
    # read_path returns raw bytes
    raw_bytes = src_bucket.read_path(src_key)
    return io.BytesIO(raw_bytes)

@task
def upload_file(src: str, dest_bucket: S3Bucket, dest_key: str):
    """Upload a file from a local path to an S3 bucket

    :param str src: The full local path to upload
    :param S3Bucket dest_bucket: An open S3Bucket block for the destination bucket
    :param str dest_key: The S3 key to upload to
     """

    # Special case where the ACL must be set for objects uploaded to imos-data
    if dest_bucket.bucket_name == "imos-data":
        dest_bucket.upload_from_path(src, dest_key, ExtraArgs={"ACL": "bucket-owner-full-control"})
    else:
        dest_bucket.upload_from_path(src, dest_key)


@task
def delete_file(bucket: S3Bucket, key: str):
    """Delete an object from an S3 bucket

    :param S3Bucket bucket: An open S3Bucket block
    :param str key: The key of the file to delete, this is appended to the bucket's `bucket_folder` prefix
    """

    logger = get_run_logger()
    logger.info(f"Deleting '{s3_url(bucket, key)}'")

    bucket_path = os.path.join(bucket.bucket_folder, key)
    client = bucket.credentials.get_s3_client()

    client.delete_object(Bucket=bucket.bucket_name, Key=bucket_path)


@task
def object_exists(bucket: S3Bucket, key:str):
    """Check if an object exists in an S3 bucket

    :param S3Bucket bucket: An open S3Bucket block
    :param str key: The key of the file to check
    :return: True if the object exists, False otherwise
    """

    try:
        client = bucket.credentials.get_s3_client()
        key = os.path.join(bucket.bucket_folder, key)
        client.head_object(Bucket=bucket.bucket_name, Key=key)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            # Something else has gone wrong.
            raise
    else:
        return True


@task
def prefix_exists(bucket: S3Bucket, prefix:str):
    """Check if a key prefix exists in an S3 bucket

    :param S3Bucket bucket: An open S3Bucket block
    :param str prefix: The key prefix to check
    :return: True if the prefix exists, False otherwise
    """

    response = bucket.list_objects(prefix, max_items=1)
    if len(response) > 0:
        return True
    else:
        return False


@task
def traceable_put_object(bucket: S3Bucket, key:str, file:str):
    """Upload a file to an S3 bucket and return the x-amz-request-id for the put operation
    :param str bucket_name: The name of the S3 bucket
    :param str key: The key to upload the file to
    :param str file: The path to the file to upload
    """

    client = bucket.credentials.get_s3_client()
    with open(file, 'rb') as body:
        key = os.path.join(bucket.bucket_folder, key)
        result = client.put_object(Bucket=bucket.bucket_name, Key=key, Body=body)

    if result['ResponseMetadata']['HTTPStatusCode'] == 200:
        request_id = result['ResponseMetadata']['RequestId']
    else:
        request_id = None

    return request_id


@task
def list_keys(bucket: S3Bucket, prefix: str = '') -> list[str]:
    """List keys in an S3 bucket with a given prefix

    :param S3Bucket bucket: An open S3Bucket block
    :param str prefix: The prefix to filter keys by
    :return: A list of keys in the bucket with the given prefix
    """

    objects = bucket.list_objects(folder=prefix)

    # Remove the bucket folder prefix from the keys to make them usable with
    # the original S3Bucket block
    return [obj["Key"].removeprefix(bucket.bucket_folder + "/") for obj in objects]

@task
def head_object(bucket: S3Bucket, key: str) -> dict:
    """Get object metadata from an S3 bucket with a given key

    :param S3Bucket bucket: An open S3Bucket block
    :param str key: The object key
    :return: The object metadata
    """

    client = bucket.credentials.get_s3_client()
    head = client.head_object(Bucket=bucket.bucket_name, Key=bucket._resolve_path(key))

    return head
