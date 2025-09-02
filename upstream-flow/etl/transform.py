from prefect import get_run_logger, task
import pandas as pd
import xarray as xr
import os


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
