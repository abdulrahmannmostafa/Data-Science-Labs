# [ Mocks : Mocking Database ]:

from ex7 import save_user


def test_save_user(mocker):
    # Mock the 'sqlite3.connect' inside the 'ex7' module
    # to avoid creating an actual database
    mock_conn = mocker.patch('ex7.sqlite3.connect')
    mock_cursor = mock_conn.return_value.cursor.return_value

    # Call the function we want to test
    save_user('Alice', 30)

    # Assert that the connect function was called with the correct database name
    mock_conn.assert_called_once_with('users.db')

    # Assert that the execute method was called with the correct SQL command
    expected_sql = 'INSERT INTO users (name, age) VALUES ("Alice", 30)'
    mock_cursor.execute.assert_called_once_with(expected_sql)
