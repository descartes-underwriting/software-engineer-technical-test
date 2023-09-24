import pytest
from datetime import datetime
from dateutil.relativedelta import relativedelta
from urllib.error import URLError
from urllib.parse import urlparse, parse_qs
import urllib.request
from pandas import DataFrame

from earthquakes.usgs_api import build_api_url, get_earthquake_data


@pytest.fixture
def valid_parameters() -> tuple:
    """
    Generates a fixture that returns valid parameters for testing.

    Returns:
        Tuple: A tuple containing the latitude, longitude, radius, max distance, and date for testing.
    """
    return (37.7749, -122.4194, 100.0, 5.0, datetime(2022, 1, 1))


@pytest.fixture
def invalid_parameters() -> tuple:
    """
    A fixture that returns a tuple of invalid parameters.

    Returns:
        tuple: A tuple containing invalid parameters.
    """
    return (100, -200, 30000, -1, datetime(1877, 1, 1))


@pytest.fixture
def csv_data():
    data = """time,latitude,longitude,depth,mag,magType,nst,gap,dmin,rms,net,id,updated,place,type,horizontalError,depthError,magError,magNst,status,locationSource,magSource
2023-09-08T01:06:12.975Z,36.0627,25.0264,10,4.2,mb,32,83,0.781,0.7,us,us7000ku2q,2023-09-08T11:45:08.235Z,"49 km SW of Emporeío, Greece",earthquake,5.41,1.925,0.136,15,reviewed,us,us
2023-08-28T22:10:19.406Z,35.7724,25.4996,11.932,4.5,mb,74,76,0.693,0.61,us,us7000krp7,2023-09-12T01:10:33.926Z,"50 km N of Limín Khersonísou, Greece",earthquake,2.6,4.358,0.066,67,reviewed,us,us
"""
    return data.strip()


def test_valid_parameters(valid_parameters: tuple) -> None:
    """
    Test the validity of the given parameters for the build_api_url function.

    Args:
        valid_parameters (tuple): A tuple containing the latitude, longitude, radius, minimum_magnitude, and end_date.

    Returns:
        None: This function does not return anything.

    Raises:
        AssertionError: If the result is not an instance of Request or the full_url does not start with
        "https://earthquake.usgs.gov/fdsnws/event/1/query".
        AssertionError: If the parsed query string does not match the expected values.
    """
    latitude, longitude, radius, minimum_magnitude, end_date = valid_parameters
    result = build_api_url(
        latitude=latitude,
        longitude=longitude,
        radius=radius,
        min_magnitude=minimum_magnitude,
        end_date=end_date,
    )
    assert isinstance(result, urllib.request.Request)
    assert result.full_url.startswith(
        "https://earthquake.usgs.gov/fdsnws/event/1/query"
    )

    parse_result = urlparse(result.full_url)

    start_date = end_date - relativedelta(years=200)

    if end_date > datetime(2021, 10, 21):
        end_date = datetime(2021, 10, 21)

    assert parse_qs(parse_result.query) == {
        "format": ["csv"],
        "starttime": [start_date.isoformat()],
        "endtime": [end_date.isoformat()],
        "latitude": [str(latitude)],
        "longitude": [str(longitude)],
        "maxradiuskm": [str(radius)],
        "minmagnitude": [str(minimum_magnitude)],
        "eventtype": ["earthquake"],
    }
    assert result.method == "GET"


def test_invalid_parameters(invalid_parameters):
    """
    Test the behavior of the function when invalid parameters are provided.
    TODO: test each parameter by making just one of the paraneters invalid

    Args:
        invalid_parameters (tuple): A tuple containing the latitude, longitude, radius,
            minimum_magnitude, and end_date parameters.

    Returns:
        None

    Raises:
        AssertionError: If the result of build_api_url is not None.
    """
    latitude, longitude, radius, minimum_magnitude, end_date = invalid_parameters
    result = build_api_url(
        latitude=latitude,
        longitude=longitude,
        radius=radius,
        min_magnitude=minimum_magnitude,
        end_date=end_date,
    )
    assert result is None


# Test cases

import earthquakes


def test_get_earthquake_data_valid_request(
    monkeypatch, valid_parameters: tuple, csv_data: str
):
    """
    Test the get_earthquake_data function with valid request parameters and valid CSV data.
    Mock the urlopen function to return a response

    Args:
        monkeypatch: A pytest fixture that allows us to modify the behavior of
            functions or objects during test execution.
        valid_parameters (tuple): A tuple containing the latitude, longitude,
            radius, minimum_magnitude, and end_date parameters for the function
            under test.
        csv_data (str): A string representing the CSV data that will be returned
            by the mocked urlopen function.

    Returns:
        None
    """

    class MockResponse:
        def __init__(self, data):
            self.data = data.splitlines()
            self.index = 0

        def __iter__(self):
            return self

        def __next__(self):
            if self.index >= len(self.data):
                raise StopIteration
            else:
                line = self.data[self.index] + "\n"
                self.index += 1
                return line.encode()

        def close(self):
            # This is a mocked method
            pass

    def mock_urlopen(request):
        """
        A function that mocks the `urlopen` function from the `urllib.request` module used
        in the `earthquakes.usgs_api` module.

        Parameters:
            request (object): The request object to be passed to the `urlopen` function.

        Returns:
            object: A `MockResponse` object.

        """
        return MockResponse(csv_data)

    monkeypatch.setattr(earthquakes.usgs_api, "urlopen", mock_urlopen)

    latitude, longitude, radius, minimum_magnitude, end_date = valid_parameters
    # Call the function under test
    result = get_earthquake_data(
        latitude, longitude, radius, minimum_magnitude, end_date
    )

    # Check that the function returns a 2 line DataFrame
    assert len(result) == 2


def test_get_earthquake_data_invalid_request(monkeypatch, valid_parameters: tuple):
    """
    Test the behavior of the get_earthquake_data function when an invalid request is made.

    Args:
        monkeypatch (fixture): The monkeypatch fixture used to mock the urlopen function.
        valid_parameters (tuple): The valid parameters for the get_earthquake_data function.

    Returns:
        None
    """

    # Mock the urlopen function to raise a URLError
    def mock_urlopen(request):
        raise URLError(f"Mock URLError for request: {request}")

    monkeypatch.setattr(earthquakes.usgs_api, "urlopen", mock_urlopen)

    # Destructure the valid parameters tuple
    latitude, longitude, radius, minimum_magnitude, end_date = valid_parameters

    # Call the function under test and assign the result to a variable
    result = get_earthquake_data(
        latitude, longitude, radius, minimum_magnitude, end_date
    )

    # Check that the function returns an empty DataFrame using the `empty` attribute
    assert result.empty
