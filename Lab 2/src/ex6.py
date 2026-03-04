import requests


def get_weather(city):
    # I want to mock this api,
    # so if the link changed later, I won't have to change my tests
    response = requests.get(f"http://api.weatherapi.com/v1/{city}")
    if response.status_code == 200:
        return response.json()
    else:
        raise ValueError("Couldn't fetch weather data")
