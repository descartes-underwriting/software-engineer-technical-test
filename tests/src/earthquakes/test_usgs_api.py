import pytest
from urllib.request import Request
from typing import Optional
from earthquakes.usgs_api import build_api_url


@pytest.fixture
def valid_parameters():
    return (37.7749, -122.4194, 100)


@pytest.fixture
def invalid_parameters():
    return (100, -200, 30000)


def test_valid_parameters(valid_parameters):
    latitude, longitude, radius = valid_parameters
    result = build_api_url(latitude, longitude, radius)
    assert isinstance(result, Request)
    assert result.full_url == "https://earthquake.usgs.gov/fdsnws/event/1/query"
    assert result.data == {
        "format": "csv",
        "endtime": "21-10-2021",
        "latitude": str(latitude),
        "longitude": str(longitude),
        "maxradiuskm": str(radius),
        "eventtype": "earthquake",
    }
    assert result.method == "GET"


def test_invalid_parameters(invalid_parameters):
    latitude, longitude, radius = invalid_parameters
    result = build_api_url(latitude, longitude, radius)
    assert result is None
