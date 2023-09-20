import pytest
from datetime import datetime
from urllib.error import URLError
from urllib.request import Request
from urllib.parse import urlparse, parse_qs
from earthquakes.usgs_api import build_api_url, get_earthquake_data


@pytest.fixture
def valid_parameters():
    return (37.7749, -122.4194, 100)


@pytest.fixture
def invalid_parameters():
    return (100, -200, 30000)


@pytest.fixture
def test_data(valid_parameters):
    latitude, longitude, radius = valid_parameters
    minimum_magnitude = 5.0
    end_date = datetime(2022, 1, 1)
    return (latitude, longitude, radius, minimum_magnitude, end_date)


def test_valid_parameters(valid_parameters):
    latitude, longitude, radius = valid_parameters
    result = build_api_url(latitude, longitude, radius)
    assert isinstance(result, Request)
    assert result.full_url.startswith(
        "https://earthquake.usgs.gov/fdsnws/event/1/query"
    )

    parse_result = urlparse(result.full_url)

    assert parse_qs(parse_result.query) == {
        "format": ["csv"],
        "endtime": ["21-10-2021"],
        "latitude": [str(latitude)],
        "longitude": [str(longitude)],
        "maxradiuskm": [str(radius)],
        "eventtype": ["earthquake"],
    }
    assert result.method == "GET"


def test_invalid_parameters(invalid_parameters):
    latitude, longitude, radius = invalid_parameters
    result = build_api_url(latitude, longitude, radius)
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
