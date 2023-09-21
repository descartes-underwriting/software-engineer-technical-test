import pytest
from datetime import datetime
from urllib.error import URLError
from urllib.request import Request
from urllib.parse import urlparse, parse_qs
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
    assert isinstance(result, Request)
    assert result.full_url.startswith(
        "https://earthquake.usgs.gov/fdsnws/event/1/query"
    )

    parse_result = urlparse(result.full_url)

    assert parse_qs(parse_result.query) == {
        "format": ["csv"],
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


# # Test cases
# def test_get_earthquake_data_valid_request(monkeypatch, test_data):
#     # Mock the urlopen function to return a response
#     def mock_urlopen(request):
#         class MockResponse:
#             def read(self):
#                 return b"Mock response data"

#             def close(self):
#                 # This is a mocked method
#                 pass

#         return MockResponse()

#     monkeypatch.setattr("urllib.request.urlopen", mock_urlopen)

#     latitude, longitude, radius, minimum_magnitude, end_date = test_data
#     # Call the function under test
#     result = get_earthquake_data(
#         latitude, longitude, radius, minimum_magnitude, end_date
#     )

#     # Check that the function returns a non-empty DataFrame
#     assert not result.empty


# def test_get_earthquake_data_invalid_request(monkeypatch):
#     # Mock the urlopen function to raise a URLError
#     def mock_urlopen(request):
#         raise URLError("Mock URLError")

#     monkeypatch.setattr("urllib.request.urlopen", mock_urlopen)

#     latitude, longitude, radius, minimum_magnitude, end_date = test_data

#     # Call the function under test
#     result = get_earthquake_data(
#         latitude, longitude, radius, minimum_magnitude, end_date
#     )

#     # Check that the function returns an empty DataFrame
#     assert result.empty
