from typing import Optional
from math import isclose
from urllib.request import Request
from datetime import datetime
import pandas as pd

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
    latitude: float, longitude: float, radius: float
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

    # build parameters
    params = {
        "format": "csv",
        "endtime": "21-10-2021",  # TODO: this need to be checked
        "latitude": str(latitude),
        "longitude": str(longitude),
        "maxradiuskm": str(radius),
        "eventtype": "earthquake",
    }

    request = Request(url=API_URL + API_METHOD, data=params, method="GET")

    return request


def get_earthquake_data(
    latitude: float,
    longitude: float,
    radius: float,
    minimum_magnitude: float,
    end_date: datetime,
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
        pd.DataFrame: The result of the API call packaged as pandas DataFrame or None in case of error
    """

    request = build_api_url(latitude=latitude, longitude=longitude, radius=radius)

    if request is None:
        return pd.DataFrame()

    df = pd.DataFrame()
    return df
