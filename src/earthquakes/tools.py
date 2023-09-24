from math import radians, sin, asin, sqrt
import numpy as np
import pandas as pd

EARTH_RADIUS = 6378

TIME_COLUMN = "time"
PAYOUT_COLUMN = "payout"
MAGNITUDE_COLUMN = "mag"
DISTANCE_COLUMN = "distance"
LATITUDE_COLUMN = "latitude"
LONGITUDE_COLUMN = "longitude"


def haversine_distance(
    latitude_from: float, latitude_to: float, longitude_from: float, longitude_to: float
) -> float:
    """
    Calculate the haversine distance between two points on the Earth's surface.
    Not vectorized, used as starting point for vectorization

    Args:
        point_1 (Tuple): A tuple containing the latitude and longitude of the first point in degrees.
        point_2 (Tuple): A tuple containing the latitude and longitude of the second point in degrees.

    Returns:
        float: The haversine distance between the two points in kilometers.
    """
    lat = [radians(latitude_from), radians(latitude_to)]
    long = [radians(longitude_from), radians(longitude_to)]
    lat_diff = lat[1] - lat[0]
    lat_sum = lat[1] + lat[0]
    long_diff = long[1] - long[0]

    ret = (
        sin(lat_diff / 2) ** 2
        + (1 - sin(lat_diff / 2) ** 2 - sin(lat_sum / 2) ** 2) * sin(long_diff / 2) ** 2
    )
    ret = 2 * EARTH_RADIUS * asin(sqrt(ret))

    return ret


def get_haversine_distance(
    latitude_column: pd.Series,
    longitude_column: pd.Series,
    latitude: float,
    longitude: float,
) -> pd.Series:
    """
    Calculate the Haversine distance between each coordinate in the latitude_column and longitude_column
    and the given latitude and longitude.

    Args:
        latitude_column (pd.Series): A pandas Series containing the latitude values.
        longitude_column (pd.Series): A pandas Series containing the longitude values.
        latitude (float): The latitude value to calculate the distance from.
        longitude (float): The longitude value to calculate the distance from.

    Returns:
        pd.Series: A pandas Series containing the calculated distances.
    """

    # Convert degrees to radians
    latitude_column = np.radians(latitude_column)
    longitude_column = np.radians(longitude_column)
    latitude = np.radians(latitude)
    longitude = np.radians(longitude)

    # Calculate differences and sums
    lat_diff = latitude_column - latitude
    lat_sum = latitude_column + latitude
    long_diff = longitude_column - longitude

    # Apply Haversine formula using vectorized operations
    ret = (
        np.sin(lat_diff / 2) ** 2
        + (1 - np.sin(lat_diff / 2) ** 2 - np.sin(lat_sum / 2) ** 2)
        * np.sin(long_diff / 2) ** 2
    )
    ret = 2 * EARTH_RADIUS * np.arcsin(np.sqrt(ret))

    return ret


def compute_payouts(earthquake_data: pd.DataFrame, payout_structure: list) -> pd.Series:
    """
    Compute payouts based on earthquake data and a payout structure.

    Parameters:
    - earthquake_data (pd.DataFrame): A DataFrame containing earthquake data.
    - payout_structure (list): A list defining the payout structure.

    Returns:
    - pd.Series: A Series containing the computed payouts per year.
    """

    # order the data by event time

    df = earthquake_data[[TIME_COLUMN, MAGNITUDE_COLUMN, DISTANCE_COLUMN]].sort_values(
        by=TIME_COLUMN
    )

    # Compute potential payout per event
    # Initialize the payout column to 0
    df["potential_payout"] = 0

    for payout in payout_structure:
        payout_column = (df[MAGNITUDE_COLUMN] >= payout["magnitude"]).astype(int)
        payout_column *= (df[DISTANCE_COLUMN] <= payout["radius"]).astype(int)
        payout_column *= payout["payout"]

        df["potential_payout"] = np.maximum(df["potential_payout"], payout_column)

    # Select only payouts greater than 0
    payouts = df[[TIME_COLUMN, "potential_payout"]][df["potential_payout"] > 0]

    # Convert the TIME_COLUMN to just the year
    payouts[TIME_COLUMN] = pd.to_datetime(payouts[TIME_COLUMN]).dt.year

    # Select just maximum payout per year
    payouts = payouts.groupby(TIME_COLUMN)["potential_payout"].max()

    # Return data is a series where the index is the year
    return payouts


def compute_burning_cost(payouts: pd.Series, start_year: int, end_year: int) -> float:
    """
    Compute the burning cost for a given range of years.

    Args:
        payouts (pd.Series): A series of payouts with the year as index
        start_year (int): The start year of the range.
        end_year (int): The end year of the range.

    Returns:
        float: The average burning cost for the given range of years.
    """
    df = payouts.loc[start_year:end_year]

    return df.sum() / (end_year - start_year + 1)
