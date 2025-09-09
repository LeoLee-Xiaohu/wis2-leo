import os
import pandas as pd
from unittest.mock import patch, MagicMock
from tempfile import TemporaryDirectory

from src.wis2_aodn_upstream.etl.load import load_to_minio
from src.wis2_aodn_upstream.etl.transform import convert_buoy_nc_to_csv

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


@patch("projects.wis2.wis2_buoys_flow.Minio")
@patch("projects.wis2.wis2_buoys_flow.Variable")
@patch("projects.wis2.wis2_buoys_flow.Secret")
@patch("projects.wis2.wis2_buoys_flow.get_run_logger")
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
