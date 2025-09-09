"""
Prefect flow to process WIS2 buoy data.

This flow retrieves buoy data in NetCDF format from an S3 bucket,
converts the latest data point to a CSV file with WIGOS metadata,
and uploads the resulting CSV to a MinIO bucket for WIS2box processing.

The flow is triggered with a path to a specific NetCDF file and a
dataset configuration name, which specifies details like the WIGOS ID.
"""
import argparse
import os
from tempfile import TemporaryDirectory
from prefect import flow, get_run_logger
from prefect_aws import S3Bucket

from .common.lib.config import lazy_load_config
from .common.tasks.fileops import download_file
from .etl import load_to_minio, convert_buoy_nc_to_csv


@flow(flow_run_name="WIS2-buoys-{dataset_config}")
def wis2_buoys_upstream_flow(
    path: str,
    dataset_config: str,
) -> None:
    """Flow to process WIS2 buoy data from S3, convert NetCDF to CSV, and upload to MinIO.
    Parameters
    ----------
    path : str
        The S3 path to the NetCDF file to be processed.
        Example: 'IMOS/COASTAL-WAVE-BUOYS/WAVE-BUOYS/REALTIME/WAVE-PARAMETERS/file.nc'
    dataset_config : str
        The name of the dataset configuration.
        Example: 'APOLLO-BAY'
    """

    logger = get_run_logger()
    logger.info(f"Processing file: {path}")

    config = lazy_load_config(dataset_config)
    logger.info(f"Loaded config_id: {config['config_id']}")
    logger.debug(f"Config:\n{config}")

    wigos_id = config.get("wigos_id")
    minio_path = config.get("minio_path")
    if not wigos_id:
        raise ValueError("WIGOS ID is required to process the buoy data.")
    if not minio_path:
        raise ValueError("MinIO path is required to upload the processed data.")

    logger.info(f"WIGOS ID: {wigos_id}")
    logger.info(f"MinIO path: {minio_path}")

    public_bucket = S3Bucket.load("public-bucket")

    with TemporaryDirectory(prefix="wis2_") as temp_dir:
        logger.debug(f"Using temporary directory '{temp_dir}'")
        local_path = os.path.join(temp_dir, os.path.basename(path))
        download_file(public_bucket, path, local_path)

        csv_path = convert_buoy_nc_to_csv(local_path, wigos_id, temp_dir)

        load_to_minio(csv_path, minio_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process S3 paths and wis2 datasets.",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument(
        "--path",
        required=True,
        help="S3 path to process. Example: 'IMOS/COASTAL-WAVE-BUOYS/WAVE-BUOYS/REALTIME/WAVE-PARAMETERS/APOLLO-BAY/2025/IMOS_COASTAL-WAVE-BUOYS_20250801_APOLLO-BAY_RT_WAVE-PARAMETERS_monthly.nc'",
    )
    parser.add_argument(
        "--dataset-config",
        required=True,
        type=str,
        help="Name of the dataset configuration. Example: 'config/APOLLO_BAY'",
    )
    args = parser.parse_args()

    wis2_buoys_upstream_flow(path=args.path, dataset_config=args.dataset_config)
