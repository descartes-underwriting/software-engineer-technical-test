"""
    Extract Eartquake data from USGS api
"""
import urllib
import urllib.request
import aiohttp
import asyncio
import numpy as np
import pandas as pd
import csv
from io import StringIO
from datetime import datetime


def get_earthquake_data(latitude, longitude, radius, minimum_magnitude, end_date):
    """
    Function used to get the earthquake data for the US Geological Service (USGS) api
    param latitude: Latitude of the position on Earth to be studied
    type: float
    param longitude: Longitude of the position on Earth to be studied
    type: float
    param radius: Radius to be studied aroud the given position
    type: float
    param minimum_magnitude: Minimum earthquake magnitude to be considered
    type: float
    param end_date: Timestamp of the end of the studied period
    type: integer

    :return expected_earthquake_data: a DataFrame containing the data fetched from the URL.
                                      If empty data or an error, returne an empty df.
    :rtype: pd.DataFrame
    """

    # Earthquakes after the 21-10-2021 should not be considered
    max_date = datetime(year=2021, month=10, day=21)
    end_date = min(end_date, max_date)

    api_url = build_api_url(latitude, longitude, radius, end_date, minimum_magnitude)
    expected_earthquake_data = pd.DataFrame()

    try:
        # Send an HTTP GET request to the API
        with urllib.request.urlopen(api_url) as response:
            # Check if the request was successful (status code 200)
            if response.status == 200:
                # Read the CSV data from the response
                csv_data = response.read().decode('utf-8')
                expected_earthquake_data = pd.read_csv(StringIO(csv_data))
            else:
                print(f"Error: Unable to fetch data from API. Status code: {response.status}")
    except urllib.error.URLError as e:
        print(f"URL error: {e}")
    except pd.errors.EmptyDataError as e:
        print(f"CSV data is empty or invalid: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    return expected_earthquake_data


def build_api_url(latitude, longitude, radius, end_date, minimum_magnitude):
    """
    Function used define the URL in order to get data from the US Geological Service (USGS) api
    param latitude: Latitude of the position on Earth to be studied
    type: float
    param longitude: Longitude of the position on Earth to be studied
    type: float
    param radius: Radius to be studied aroud the given position
    type: float
    param end_date: date of the end of the studied period
    type: datetime
    param minimum_magnitude: Minimum earthquake magnitude to be considered: Set by default to 0
    type: float

    :return: api_url
    :rtype: string
    """

    # The start time should be 200 years from now.
    start_date = datetime(year=1823, month=1, day=1).strftime("%Y-%m-%d")
    end_date   = end_date.strftime("%Y-%m-%d")

    url = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&"
    params = {'latitude': latitude,
              'longitude': longitude,
              'starttime': start_date,
              'endtime': end_date,
              'minmagnitude':minimum_magnitude,
              'maxradiuskm':radius}

    api_url = url + urllib.parse.urlencode(params)
    return api_url


async def get_for_session_df_from_api(url, session):
    """
    Function the get the earthquakes data
        from a given URL asynchronously using aiohttp
        and saving them in a dataFrame

    param url: The URL from which to fetch the CSV data.
    type: string
    param: session The aiohttp client session to use for the request.
    type: aiohttp.ClientSession

    :return df: : a DataFrame containing the data fetched from the URL.
                  If empty data or an error, returne an empty df.
    :rtype: pd.DataFrame
    """
    try:
        async with session.get(url) as response:
            # Raise an exception if the request is not successful (e.g., status code 4xx or 5xx)
            response.raise_for_status()
            csv_data = await response.text()
            df = pd.read_csv(StringIO(csv_data))
            return df

    except aiohttp.ClientError as e:
        print(f'Error fetching data from {url}: {e}')
        return pd.DataFrame()


async def get_earthquake_data_for_multiple_locations(assets,
                                                     radius,
                                                     minimum_magnitude,
                                                     end_date):
    """
    Function used to get Earthquakes data for different positions

    param assets: Dataframe containing the longitude and latitude of the studied positions
    type: pd.DataFrame
    param radius: Radius to be studied aroud the given position
    type: float
    param minimum_magnitude: Minimum earthquake magnitude to be considered
    type: float
    param end_date: datetime of the end of the studied period
    type: datetime

    :return df: : a DataFrame containing the earthquake data fetched
                  from all given positions.
    :rtype: pd.DataFrame
    """

    # Create the URLS based on the given assets: longitude, latitude
    # save them in the api_urls list
    api_urls = []
    for _, row in assets.iterrows():
        latitude = row["latitude"]
        longitude = row["longitude"]
        api_urls.append(
            build_api_url(latitude, longitude, radius, end_date, minimum_magnitude)
        )

    # Retreive the df from each given url using async
    async with aiohttp.ClientSession() as session:
        tasks = [get_for_session_df_from_api(url, session) for url in api_urls]
        df_list = await asyncio.gather(*tasks)


    # Filter out empty DataFrames from the list
    non_empty_df_list = [x for x in df_list if not x.empty]

    # Concat the retreived df in a single df
    earthquakes_data = pd.concat(
        non_empty_df_list, ignore_index=True).drop_duplicates(keep="first")
    return earthquakes_data
