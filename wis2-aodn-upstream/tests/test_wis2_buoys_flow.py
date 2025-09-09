import os
import pandas as pd
from unittest.mock import patch, MagicMock
from tempfile import TemporaryDirectory

from prefect import flow
from prefect.testing.utilities import prefect_test_harness

from wis2_aodn_upstream.etl import convert_buoy_nc_to_csv, load_to_minio
from wis2_aodn_upstream.wis2_buoys_flow import wis2_buoys_upstream_flow



def test_convert_buoy_nc_to_csv(dummy_nc_file):
    """Test the convert_buoy_nc_to_csv task."""
    with TemporaryDirectory() as temp_dir:
        wigos_id = "0-20000-0-58422"
        csv_path = convert_buoy_nc_to_csv(dummy_nc_file, wigos_id, temp_dir)

        assert os.path.exists(csv_path)
        assert os.path.basename(csv_path) == "WIGOS_0-20000-0-58422_20230101T010000.csv"

        df = pd.read_csv(csv_path)
        assert df["wigos_station_identifier"][0] == wigos_id
        assert df["regionNumber"][0] == 5
        assert df["blockNumber"][0] == 58
        assert df["stationNumber"][0] == 422
        assert df["latitude"][0] == -38.5
        assert df["longitude"][0] == 143.5
        assert df["WSSH"][0] == 21.0


@patch("wis2_aodn_upstream.etl.Minio")
@patch("wis2_aodn_upstream.etl.Variable")
@patch("wis2_aodn_upstream.etl.Secret")
@patch("wis2_aodn_upstream.etl.get_run_logger")
def test_load_to_minio(mock_get_run_logger, mock_secret, mock_variable, mock_minio):
    mock_logger = MagicMock()
    mock_get_run_logger.return_value = mock_logger

    mock_variable.get.side_effect = [
        "http://minio:9000",
        "minio-user"
    ]
    mock_secret.load.return_value.get.return_value = "minio-password"

    mock_minio_client = MagicMock()
    mock_minio.return_value = mock_minio_client

    with open("test.csv", "w") as f:
        f.write("test,data")

    load_to_minio("test.csv", "test-path")

    mock_minio.assert_called_with(
        endpoint="minio:9000",
        access_key="minio-user",
        secret_key="minio-password",
        secure=False
    )
    mock_minio_client.fput_object.assert_called_with(
        "wis2box-incoming",
        "test-path/test.csv",
        "test.csv"
    )
    os.remove("test.csv")



@patch("wis2_aodn_upstream.wis2_buoys_flow.lazy_load_config")
@patch("wis2_aodn_upstream.wis2_buoys_flow.S3Bucket")
@patch("wis2_aodn_upstream.wis2_buoys_flow.download_file")
@patch("wis2_aodn_upstream.wis2_buoys_flow.convert_buoy_nc_to_csv")
@patch("wis2_aodn_upstream.wis2_buoys_flow.load_to_minio")
@patch("wis2_aodn_upstream.wis2_buoys_flow.get_run_logger")
def test_wis2_buoys_upstream_flow(
    mock_get_run_logger,
    mock_load_to_minio,
    mock_convert_buoy_nc_to_csv,
    mock_download_file,
    mock_s3_bucket,
    mock_lazy_load_config,
):
    """Test the main wis2_buoys_upstream_flow end-to-end."""
    mock_get_run_logger.return_value = MagicMock()

    mock_lazy_load_config.return_value = {
        "config_id": "test-config",
        "wigos_id": "0-20000-0-58422",
        "minio_path": "test-minio-path",
    }

    mock_s3_bucket.load.return_value = MagicMock()
    mock_convert_buoy_nc_to_csv.return_value = "/tmp/fake.csv"

    with prefect_test_harness():
        wis2_buoys_upstream_flow(path="IMOS/APOLLO-BAY/test-path.nc", dataset_config="test-config")

    mock_lazy_load_config.assert_called_with("test-config")
    mock_s3_bucket.load.assert_called_with("public-bucket")
    mock_download_file.assert_called_once()
    mock_convert_buoy_nc_to_csv.assert_called_once()
    mock_load_to_minio.assert_called_once_with("/tmp/fake.csv", "test-minio-path")