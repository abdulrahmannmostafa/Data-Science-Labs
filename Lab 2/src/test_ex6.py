# [ Mocks : Mocking External API Calls ]:

from ex6 import get_weather


# You have to call the parameter: "mocker"
def test_get_weather(mocker):
    # Mock requests.get to return a fake response:
    # Pass the path as a STRING
    # We patch 'requests.get' inside the 'ex6' module
    mock_get = mocker.patch('ex6.requests.get')

    # Set return values
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {'temperature': 25, 'condition': 'Sunny'}

    # Call the function
    result = get_weather("Dubai")

    # Assert that the mocked response is returned
    assert result == {'temperature': 25, 'condition': 'Sunny'}
    # Assert that requests.get was called with the correct URL
    mock_get.assert_called_once_with("http://api.weatherapi.com/v1/Dubai")
