# [ Single Test Function ]

from ex1 import get_weather


def test_get_weather():
    assert get_weather(31) == "Hot"
