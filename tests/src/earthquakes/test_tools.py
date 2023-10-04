import numpy as np
import pandas as pd
import os
import sys

sys.path.append(os.path.abspath('./src/'))
from earthquakes.tools import (
    TIME_COLUMN,
    PAYOUT_COLUMN,
    MAGNITUDE_COLUMN,
    DISTANCE_COLUMN,
    LATITUDE_COLUMN,
    LONGITUDE_COLUMN
)
from earthquakes.tools import (
    get_haversine_distance,
    compute_payouts,
    compute_burning_cost
)


def test_payout_function():
    earthquake_data_df = pd.DataFrame(
        [["2021-10-12T09:24:05.099Z", 35.1691, 26.2152, 6, 50],
         ["2021-10-03T14:31:27.622Z", 35.1442, 25.2375, 5, 250],
         ["2021-09-29T11:54:48.885Z", 35.0268, 25.1561, 4.5, 150],
         ["2021-09-28T15:13:16.867Z", 35.2054, 25.2791, 6, 1],
         ["2021-09-28T04:48:08.650Z", 35.0817, 25.2018, 9, 150],
         ["2020-10-12T09:24:05.099Z", 35.1691, 26.2152, 6, 50],
         ["2020-10-03T14:31:27.622Z", 35.1442, 25.2375, 5, 250],
         ["2020-09-29T11:54:48.885Z", 35.0268, 25.1561, 4.5, 150],
         ["2020-09-28T15:13:16.867Z", 35.2054, 25.2791, 4, 100],
         ["2020-09-28T04:48:08.650Z", 35.0817, 25.2018, 3, 150]
         ], columns=[
             TIME_COLUMN,
             LATITUDE_COLUMN,
             LONGITUDE_COLUMN,
             MAGNITUDE_COLUMN,
             DISTANCE_COLUMN
             ]
         )

    payout_structure_dict = {DISTANCE_COLUMN: [10, 50, 200], MAGNITUDE_COLUMN: [4.5, 5.5, 6.5], PAYOUT_COLUMN: [100, 75, 50]}
    payout_structure = pd.DataFrame.from_dict(payout_structure_dict)

    payouts = compute_payouts(earthquake_data_df, payout_structure)

    # payouts is a dict transform it into numpy
    payout_values = np.array(list(payouts.values()))
    assert np.max(payout_values) > 1
    assert np.max(payout_values) <= 100
    assert payout_values.tolist() == [75.0, 75.0]


def test_harvestine_distance():
    earthquake_data_df = pd.DataFrame([["2021-10-12T09:24:05.099Z", 35.1691, 26.2152],
                                       ["2021-10-03T14:31:27.622Z", 35.1442, 25.2375],
                                       ["2021-09-29T11:54:48.885Z", 35.0268, 25.1561],
                                       ["2021-09-28T15:13:16.867Z", 35.2054, 25.2791],
                                       ["2021-09-28T04:48:08.650Z", 35.0817, 25.2018]
                                       ], columns=['time', LATITUDE_COLUMN, LONGITUDE_COLUMN])
    distance = get_haversine_distance(earthquake_data_df[LATITUDE_COLUMN], earthquake_data_df[LONGITUDE_COLUMN], 35, 25)
    distance = distance.tolist()
    distance = [round(num, 6) for num in distance]
    expected_distance = [112.282482, 26.941499, 14.541066, 34.188531, 20.517793]
    assert distance == expected_distance


def test_compute_burning_cost():
    payout_dict = {1906: 0, 1908: 0, 1910: 75.0, 1911: 0, 1913: 0, 1915: 0, 1917: 0, 1918: 0,
     1919: 0, 1920: 0, 1922: 50.0, 1923: 0, 1926: 0, 1927: 0, 1928: 0, 1929: 0,
     1930: 0, 1931: 0, 1932: 0, 1933: 0, 1934: 0, 1935: 0, 1936: 0, 1937: 0, 1938: 75.0,
     1939: 0, 1940: 75.0, 1942: 0, 1944: 0, 1946: 0, 1947: 0, 1948: 50.0, 1949: 0, 1950: 0,
     1951: 0, 1952: 50.0, 1953: 0, 1954: 0, 1955: 0, 1956: 50.0, 1957: 0, 1958: 0, 1959: 0,
     1962: 0, 1965: 0, 1966: 0, 1968: 0, 1969: 0, 1973: 0, 1974: 0, 1975: 0, 1976: 0,
     1977: 0, 1978: 0, 1979: 0, 1980: 0, 1981: 0, 1982: 0, 1983: 75.0, 1984: 0, 1985: 0,
     1986: 0, 1987: 0, 1988: 0, 1989: 75.0, 1990: 0, 1991: 0, 1992: 0, 1993: 0, 1994: 0,
     1995: 0, 1996: 0, 1997: 0, 1998: 100.0, 1999: 0, 2000: 0, 2001: 0, 2002: 0, 2003: 0,
     2004: 0, 2005: 0, 2006: 0, 2007: 0, 2008: 75.0, 2009: 0, 2010: 0, 2011: 75.0, 2012: 0,
     2013: 0, 2014: 0, 2015: 0, 2016: 100.0, 2017: 0, 2018: 0, 2019: 0, 2020: 75.0, 2021: 75.0}

    burning_cost = compute_burning_cost(payout_dict, start_year=1952, end_year=2021)
    burning_cost = round(burning_cost, 1)
    assert burning_cost == 11.1
    burning_cost_equal_years = compute_burning_cost(payout_dict, start_year=1953, end_year=1953)
    assert burning_cost_equal_years == 0
    burning_cost_start_after_end = compute_burning_cost(payout_dict, start_year=1952, end_year=1950)
    assert burning_cost_start_after_end == 0


