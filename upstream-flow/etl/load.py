from prefect import get_run_logger, task
from minio import Minio
from prefect.blocks.system import Secret
from prefect.variables import Variable
import os


@task(retries=3, retry_delay_seconds=2)
def load_to_minio(
    local_file: str,
    minio_path: str,
    incoming_bucket: str = 'wis2box-incoming'
) -> None:
    """Upload a local file to MinIO storage.

    Parameters
    ----------
    local_file : str
        The path to the local file to be uploaded.
    minio_path : str
        The path in MinIO storage where the file will be uploaded.
        e.g., 'urn:wmo:md:au-bom-imos:apollo-bay'.
    incoming_bucket : str, optional
        The name of the bucket in MinIO where the file will be uploaded.
        Default is 'wis2box-incoming'.
    """

    logger = get_run_logger()
    # Get WIS2 MinIO service endpoint and user name from prefect variables
    MINIO_STORAGE_ENDPOINT = Variable.get("wis2_minio_storage_endpoint")
    MINIO_STORAGE_USER = Variable.get("wis2_minio_storage_username")
    # Get the Minio storage password from prefect blocks
    secret_block = Secret.load("wis2-minio-storage-password")
    MINIO_STORAGE_PASSWORD = secret_block.get()


    if MINIO_STORAGE_ENDPOINT.startswith('https://'):
        is_secure = True
        MINIO_STORAGE_ENDPOINT = MINIO_STORAGE_ENDPOINT.replace('https://', '')
    else:
        is_secure = False
        MINIO_STORAGE_ENDPOINT = MINIO_STORAGE_ENDPOINT.replace('http://', '')

    client = Minio(
        endpoint=MINIO_STORAGE_ENDPOINT,
        access_key=MINIO_STORAGE_USER,
        secret_key=MINIO_STORAGE_PASSWORD,
        secure=is_secure)

    identifier = os.path.join(minio_path, os.path.basename(local_file))

    logger.info(f"Putting into {incoming_bucket} : {local_file} as {identifier}")
    client.fput_object(incoming_bucket, identifier, local_file)
