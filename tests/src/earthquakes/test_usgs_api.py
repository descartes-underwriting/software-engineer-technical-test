import asyncio
from datetime import datetime
import numpy as np
import pandas as pd
import os
import sys

sys.path.append(os.path.abspath('./src/'))
from earthquakes.tools import (
    LATITUDE_COLUMN,
    LONGITUDE_COLUMN,
)
from earthquakes.usgs_api import (get_earthquake_data,
                      build_api_url,
                      get_earthquake_data_for_multiple_locations
)


def test_ok_earthquake_api():
    latitude = 35.025
    longitude = 25.763
    radius = 200
    minimum_magnitude = 4.5

    earthquake_data = get_earthquake_data(
        latitude=latitude,
        longitude=longitude,
        radius=radius,
        minimum_magnitude=minimum_magnitude,
        end_date=datetime(year=2021, month=10, day=21)
    )

    assert isinstance(earthquake_data, pd.DataFrame)
    assert len(earthquake_data) == 734


def test_url_validity():
    end_date=datetime(year=2021, month=10, day=21)
    url = build_api_url(35.025, 25.763, 200, end_date, 4.5)

    expected_url = ("https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv"
    "&latitude=35.025&longitude=25.763&starttime=1823-01-01"
    "&endtime=2021-10-21&minmagnitude=4.5&maxradiuskm=200")

    assert expected_url == url


def test_empty_earthquake_api():
    latitude = 35.025
    longitude = 25.763
    radius = 50
    minimum_magnitude = 4.5

    earthquake_data = get_earthquake_data(
        latitude=35.025,
        longitude=25.763,
        radius=50,
        minimum_magnitude=4.5,
        end_date=datetime(year=1900, month=10, day=21)
    )

    assert earthquake_data.empty


def test_get_multiple_location_data():
    number_of_assets = 10
    random_state = np.random.RandomState(0)
    random_values = random_state.random(2*number_of_assets)

    latitudes = random_values[::2] * 20 + 35.0
    longitudes = random_values[1::2] * 25 + 3.0
    assets = pd.DataFrame({LATITUDE_COLUMN: latitudes,
                           LONGITUDE_COLUMN: longitudes})

    end_date = datetime(year=2021, month=10, day=21)
    radius=200
    minimum_magnitude=4.5

    earthquake_data = asyncio.run(
        get_earthquake_data_for_multiple_locations(assets, radius, minimum_magnitude, end_date))

    assert isinstance(earthquake_data, pd.DataFrame)
    assert len(earthquake_data) == 1151
