import asyncio
from io import StringIO
import io
from typing import Optional
from math import isclose
from urllib.request import Request, urlopen
from urllib.parse import urlencode, urljoin
import aiohttp
from dateutil.relativedelta import relativedelta
from datetime import datetime
import pandas as pd
import numpy as np
import csv

from earthquakes.tools import (
    TIME_COLUMN,
    PAYOUT_COLUMN,
    MAGNITUDE_COLUMN,
    DISTANCE_COLUMN,
    LATITUDE_COLUMN,
    LONGITUDE_COLUMN,
)


DATA_ENCODING = "utf-8"

# Earthquake data

# The US Geological Service (USGS) provides CSV data through their [API](https://earthquake.usgs.gov/fdsnws/event/1/).

# Use it to retrieve earthquake information.

# In the module `earthquakes.usgs_api`:
# + Implement the function `get_earthquake_data`,
# + The function will retrieve the earthquake data of the area of interest for the past 200 years,
# + The implementation must use the `urllib` python package,
# + The API request url must be build in a dedicated function `build_api_url`,
# + Tests should be provided for `build_api_url`.

# Note: Earthquakes after the 21-10-2021 should not be considered.


def value_in_range(value: float, min_val: float, max_val: float) -> bool:
    """
    Check if a value is within a given range.

    Args:
        value (float): Value to check
        min_val (float): The minimum value of the range.
        max_val (float): The maximum value of the range.

    Returns:
        bool: True if the value is within the range, False otherwise.
    """

    lower_than_max = not isclose(value, max_val) and value < max_val
    greater_than_min = not isclose(value, min_val) and value > min_val

    return lower_than_max and greater_than_min


def build_api_url(
    latitude: float,
    longitude: float,
    radius: float,
    end_date: datetime,
    min_magnitude: float,
) -> Optional[Request]:
    """
    A function to build the API URL for calling Earthquake Catalog API.
    The API pattern is https://earthquake.usgs.gov/fdsnws/event/1/[METHOD[?PARAMETERS]]
    API documentation is here: https://earthquake.usgs.gov/fdsnws/event/1/

    Args:
        latitude (float): The latitude in decimal degrees [-90, 90].
        longitude (float): The longitude in decimal degrees [-180, 180].
        radius (float): The radius in kilometers [0, 20001.6].

    In this case the METHOD is `query`.
    Query parameters:
        - format: csv (for easier conversion to dataframe)
        - endtime: 21-10-2021 (later events should not be considered) TODO: this conflicts with end_date input parameter
        - latitude: Decimal [-90,90] degrees
        - longitude: Decimal [-180,180] degrees
        - maxradiuskm: Decimal [0, 20001.6] km
        Use the input parameters for the 3 above
        - eventtype: earthquake
        - minsig ? TODO: is this useful
        - reviewstatus ? Same as above
        - limit: limit the request: Integer [1,20000]. TODO: implement repeat call functionality
        TODO: check the other parameters
    NOTE: Query method Parameters should be submitted as key=value pairs using the HTTP GET method
    and may not be specified more than once; if a parameter is submitted multiple times the result is undefined.

    Returns:
        request (Request): an instance of the Request class containing all the information needed to
        call the API or None if input paraneters are invalid
    """

    API_URL = "https://earthquake.usgs.gov/fdsnws/event/1/"
    API_METHOD = "query"

    if (
        value_in_range(latitude, -90.0, 90.0) == False
        or value_in_range(longitude, -180.0, 180.0) == False
        or value_in_range(radius, 0.0, 20001.6) == False
    ):
        # TODO: add logging
        return None

    # Get the data for the last 200 years
    start_date = end_date - relativedelta(years=200)

    # build parameters
    params = {
        "format": "csv",
        "endtime": end_date.isoformat(),
        "starttime": start_date.isoformat(),
        "latitude": str(latitude),
        "longitude": str(longitude),
        "maxradiuskm": str(radius),
        "minmagnitude": str(min_magnitude),
        "eventtype": "earthquake",
    }

    url_params = urlencode(params)
    full_url = urljoin(API_URL, API_METHOD) + "?" + url_params

    request = Request(url=full_url, method="GET")

    return request


def get_earthquake_data(
    latitude: float,
    longitude: float,
    radius: float,
    minimum_magnitude: float,
    end_date: datetime.date,
) -> pd.DataFrame:
    """
    Generate a function comment for the given function body in a markdown code block with the correct language syntax.

    Args:
        latitude (float): The latitude of the location. Range [-90, 90]
        longitude (float): The longitude of the location. Range [-180, 180]
        radius (float): The radius in kilometers. Range [0, 20001.6]
        minimum_magnitude (float): The minimum magnitude of earthquakes to include.
        end_date (datetime): The end date of the data in the format "YYYY-MM-DD".

    Returns:
        pd.DataFrame: The result of the API call packaged as pandas DataFrame.
    """

    request = build_api_url(
        latitude=latitude,
        longitude=longitude,
        radius=radius,
        end_date=end_date,
        min_magnitude=minimum_magnitude,
    )

    if request is None:
        return pd.DataFrame()

    try:
        response = urlopen(request)

        # urlopen will not return the whole data in one go.

        csv_data = ""

        for line in response:
            csv_data += line.decode(DATA_ENCODING)

        response.close()

        # Convert the csv data to a pandas dataframe
        df = pd.read_csv(io.StringIO(csv_data), header=0)

    except Exception as e:
        # TODO: targeted exception handling
        # TODO: logging the exception for debugging: log.debug ...
        print(f"An error occurred: {e}")
        return pd.DataFrame()

    return df


# Large asset portfolio - async requests

# Our client also whishes to cover a large amount of properties all over Europe.

# In order to speed-up the requests to the USGS API, in the module `earthquakes.usgs_api`:
# - Implement the `async` function `get_earthquake_data_for_multiple_locations`,
# - The implementation should use the `asyncio` and `aiohttp` libraries,
# - The solution should re-use some of the functions already written,
# - Tests are not required for any of the functions.


async def get_earthquake_data_for_location(
    latitude: float,
    longitude: float,
    radius: float,
    minimum_magnitude: float,
    end_date: datetime,
) -> pd.DataFrame:
    """
    Retrieves earthquake data asynchronously for a given location within a specified radius and minimum magnitude.

    Parameters:
        latitude (float): The latitude of the location.
        longitude (float): The longitude of the location.
        radius (float): The radius within which to search for earthquakes.
        minimum_magnitude (float): The minimum magnitude of the earthquakes.
        end_date (datetime): The end date of the search.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the retrieved earthquake data.
    """
    request = build_api_url(
        latitude=latitude,
        longitude=longitude,
        radius=radius,
        end_date=end_date,
        min_magnitude=minimum_magnitude,
    )

    if request is None:
        return pd.DataFrame()

    url = request.full_url

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            csv_data = await response.text(encoding=DATA_ENCODING)

            # Convert the csv data to a pandas dataframe
            df = pd.read_csv(io.StringIO(csv_data))

            return df


async def get_earthquake_data_for_multiple_locations_async(
    assets: pd.DataFrame, radius: float, minimum_magnitude: float, end_date: datetime
) -> pd.DataFrame:
    """
    Retrieves earthquake data for multiple locations asynchronously.

    Args:
        assets (pd.DataFrame): A DataFrame containing asset information.
        radius (float): The radius within which to search for earthquakes.
        minimum_magnitude (float): The minimum magnitude of earthquakes to retrieve.
        end_date (datetime): The end date for the earthquake data retrieval.

    Returns:
        pd.DataFrame: A DataFrame containing the earthquake data for the specified locations.
    """
    location_requests = []

    for asset in assets.itertuples():
        location_requests.append(
            get_earthquake_data_for_location(
                latitude=asset.latitude,
                longitude=asset.longitude,
                radius=radius,
                minimum_magnitude=minimum_magnitude,
                end_date=end_date,
            )
        )

    # Use asyncio.gather to concurrently execute all coroutines
    data_list = await asyncio.gather(*location_requests)

    # Remove empty DataFrames from the list
    non_empty_data = [df for df in data_list if not df.empty]

    if not non_empty_data:
        return pd.DataFrame()  # Return an empty DataFrame if all were empty

    # Reset index of each DataFrame before concatenating
    for df in non_empty_data:
        df.reset_index(drop=True, inplace=True)

    return pd.concat(non_empty_data, ignore_index=True)


def get_earthquake_data_for_multiple_locations(
    assets: pd.DataFrame, radius: float, minimum_magnitude: float, end_date: datetime
) -> pd.DataFrame:
    """
    Get earthquake data for multiple locations.

    Args:
        assets (pd.DataFrame): The asset data.
        radius (float): The radius for searching earthquake data.
        minimum_magnitude (float): The minimum magnitude of the earthquakes to include.
        end_date (datetime): The end date for the earthquake data search.

    Returns:
        pd.DataFrame: The earthquake data for multiple locations.
    """

    df = get_earthquake_data_for_multiple_locations_async(
        assets=assets,
        radius=radius,
        minimum_magnitude=minimum_magnitude,
        end_date=end_date,
    )

    return df
