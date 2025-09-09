import pytest
import xarray as xr
import pandas as pd
import sys

@pytest.fixture(scope="session")
def dummy_nc_file(tmpdir_factory):
    """Create a dummy NetCDF file for testing."""
    # Create a dummy dataset
    time = pd.to_datetime(['2023-01-01T00:00:00', '2023-01-01T01:00:00'])
    lat = [-38.5]
    lon = [143.5]
    # Wave significant height
    wssh = [20.0, 21.0]

    ds = xr.Dataset(
        {
            'WSSH': (('TIME',), wssh)
        },
        coords={
            'TIME': time,
            'LATITUDE': lat,
            'LONGITUDE': lon,
            "timeSeries": (("TIME"), [1, 2])
        }
    )
    ds.attrs['geospatial_lat_min'] = -38.5
    ds.attrs['geospatial_lat_max'] = -38.5
    ds.attrs['geospatial_lon_min'] = 143.5
    ds.attrs['geospatial_lon_max'] = 143.5

    # Save to a temporary file
    fn = tmpdir_factory.mktemp("data").join("test.nc")
    file_path = str(fn)
    ds.to_netcdf(file_path, engine="netcdf4")
    return file_path

