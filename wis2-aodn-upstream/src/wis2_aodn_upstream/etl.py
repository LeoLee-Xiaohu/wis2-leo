from prefect import get_run_logger, task
import pandas as pd
import xarray as xr
import os

from minio import Minio
from prefect.blocks.system import Secret
from prefect.variables import Variable


@task
def convert_buoy_nc_to_csv(
    nc_file_path: str,
    wigos_id: str,
    temp_dir: str
) -> str:
    """Convert a NetCDF file of buoy data to a CSV file.

    The CSV file will include WIGOS metadata and time information.

    Parameters
    ----------
    nc_file_path : str
        The path of the NetCDF file containing buoy data.
    wigos_id : str
        The WMO WIGOS ID of the buoy.
    temp_dir : str
        The temporary directory to store the output CSV file.

    Returns
    -------
    str
        The path for the output CSV file.
    """
    logger = get_run_logger()
    logger.info(f"Processing NetCDF file: {nc_file_path}")

    with xr.open_dataset(nc_file_path, engine="netcdf4") as ds:
        # Only keep the last time step
        ds = ds.isel(TIME=-1)

        df = ds.to_dataframe()
        df = df.reset_index()

        # Prepare the WIGOS metadata and add to dataframe
        buoy_or_platform_identifier = wigos_id[-5:]
        df["regionNumber"] = buoy_or_platform_identifier[0]
        df["wmoRegionSubArea"] = buoy_or_platform_identifier[1]
        df["buoyOrPlatformIdentifier"] = buoy_or_platform_identifier
        df["blockNumber"] = buoy_or_platform_identifier[0:2]
        df["stationNumber"] = buoy_or_platform_identifier[2:5]
        df["wigos_station_identifier"] = wigos_id

        # Add time components
        df["year"] = df["TIME"].dt.year
        df["month"] = df["TIME"].dt.month
        df["day"] = df["TIME"].dt.day
        df["hour"] = df["TIME"].dt.hour
        df["minute"] = df["TIME"].dt.minute
        df["second"] = df["TIME"].dt.second

        df = df.rename(columns={"LATITUDE": "latitude", "LONGITUDE": "longitude"})

        # Construct the output CSV file path
        data_timestamp = pd.to_datetime(ds.TIME.item())
        data_time_str = data_timestamp.strftime("%Y%m%dT%H%M%S")
        output_file_path = os.path.join(
            temp_dir, f"WIGOS_{wigos_id}_{data_time_str}.csv"
        )

        logger.info(f"Saving DataFrame to CSV file: {output_file_path}")
        df.to_csv(output_file_path, index=False)

        return output_file_path


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
