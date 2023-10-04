from math import cos, asin, sqrt, sin, pi
import numpy as np
import pandas as pd

EARTH_RADIUS = 6378
PI_180 = pi / 180

TIME_COLUMN = "time"
PAYOUT_COLUMN = "payout"
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


def compute_payouts(earthquake_data, payout_structure):
    """
    Function to compute the payout for a given year given:
        the earthquakes event dataframe
        the payout_structure
    params earthquake_data: A DataFrame containing the earthquakes magnitude and distance
    type: pd.DataFrame
    params payout_structure: predefined payout amounts
    type: dict

    return payout: payout amount given
    rtype: float

    """

    def compute_payout_by_event(dist, mag, payout_structure):
        """
        Function to compute the payout for a given event:
            the distance and magnitude of the earthquake
        params dist: Distance from eht earthquake center
        type: float
        params mag: earthquake magnitude
        type: float

        return payout: payout amount corresponding to the event
        rtype: float
        """
        for index, row in payout_structure.iterrows():
            if dist <= row[DISTANCE_COLUMN] and mag >= row[MAGNITUDE_COLUMN]:
                return row[PAYOUT_COLUMN]
        return 0

    payout_dict = dict()
    df = earthquake_data[[TIME_COLUMN, MAGNITUDE_COLUMN, DISTANCE_COLUMN]].copy()
    df[TIME_COLUMN] = pd.PeriodIndex(df[TIME_COLUMN], freq="Y").astype(str)
    for year, df_by_year in df.groupby(TIME_COLUMN):
        df_by_year[PAYOUT_COLUMN] = df_by_year.apply(
            lambda x: compute_payout_by_event(x[DISTANCE_COLUMN],
                                             x[MAGNITUDE_COLUMN],
                                             payout_structure), axis=1)

        # Consider the first payout event as the payout of the year
        # as their is only payout by year
        df_payout_not_0 = df_by_year[PAYOUT_COLUMN].loc[df_by_year[PAYOUT_COLUMN] != 0]
        payout_dict[int(year)] = df_payout_not_0.iloc[0] if df_payout_not_0.shape[0] else 0

    return payout_dict


def compute_burning_cost(payout_dict, start_year, end_year):
    """
    Function to compute the burning cost:
    The average of payouts over a time range.
    In this project, the burning cost should be expressed in %.

    params payout_dict: a dict containing the payout by year
    type: dict
    params start_year: start year of the studied period
    type: int
    params start_year: start year of the studied period
    type: int

    return payout: payout amount given
    rtype: float
    """

    if start_year >= end_year:
        print("Start year is higher than end year => burning_cost is 0")
        return 0

    payout_df = pd.DataFrame.from_dict(payout_dict, orient="index")
    burning_cost = (
        payout_df[0]
        .loc[(payout_df.index >= start_year) & (payout_df.index < end_year)]
        .mean()
    )
    return burning_cost
