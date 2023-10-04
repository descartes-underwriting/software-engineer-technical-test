from math import cos, asin, sqrt, sin, pi
import numpy as np
import pandas as pd

EARTH_RADIUS = 6378
PI_180 = pi / 180

MAGNITUDE_COLUMN = "mag"
DISTANCE_COLUMN = "distance"
LATITUDE_COLUMN = "latitude"
LONGITUDE_COLUMN = "longitude"


def get_haversine_distance(latitude_col, longitude_col, latitude, longitude):
    """
    Function to compute the distance between a given coordinate and
        a series of positions acquired from a database
    params latitude_col: Series containing the database latitude positions
    type: pandas Series
    params longitude_col: Series containing the database longitude positions
    type: pandas Series
    params latitude: Latitude of the studied position
    type: float
    params longitude: Longitude of the studied position
    type: float

    return harvestine_distance: Series of the distance between the studied position and the database
    rtype: pandas Series

    """

    def compute_harvestine_distance_between_two_points(pos, lat2, lon2):
        """
        Function to compute the Harvestine distance between 2 points: (lat1, lon1) and (lat2, lon2)
        https://en.wikipedia.org/wiki/Haversine_formula

        params lat1: Latitude of the first point
        type: pandas float
        params lon1: Longitude of the first point
        type: pandas float
        params lat2: Latitude of the second point
        type: pandas float
        params lon2: Longitude of the second point
        type: pandas float

        return harvestine_distance: Harvestine distance between the 2 given coordonates
        rtype: float

        """
        # ° To radian
        lat1 = pos.iloc[0] * PI_180
        lon1 = pos.iloc[1] * PI_180
        lat2 = lat2 * PI_180
        lon2 = lon2 * PI_180

        # distance calculus
        a = sin((lat1 - lat2) / 2) * sin((lat1 - lat2) / 2)
        b = sin((lon1 - lon2) / 2) * sin((lon1 - lon2) / 2)
        c = a + b * cos(lat1) * cos(lat2)
        harvestine_distance = 2 * EARTH_RADIUS * asin(sqrt(c))
        return harvestine_distance

    df = pd.concat([latitude_col, longitude_col], axis=1)
    harvestine_distance = df.apply(
        compute_harvestine_distance_between_two_points,
        args=(latitude, longitude),
        axis=1,
    )
    return harvestine_distance
