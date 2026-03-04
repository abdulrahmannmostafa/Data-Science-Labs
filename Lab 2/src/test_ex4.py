# [ Fixtures ]

import pytest
from ex4 import UserManager


# The fixture is something that you run before every single test
@pytest.fixture
def user_manager():
    """Creates a fresh instance of UserManager before each test."""
    return UserManager()


# user_manager is the name of the fixture,
# and it will be passed as an argument to the test functions that need it.
# The fixture will be executed before each test function that uses it,
# ensuring that each test gets a fresh instance of UserManager to work with.
def test_add_user(user_manager):
    assert user_manager.add_user("john_doe", "john@example.com") == True
    assert user_manager.get_user("john_doe") == "john@example.com"


# This test uses a fresh instance of UserManager provided by the user_manager fixture
# Fixtures enable fresh instances, so each test runs in isolation
def test_add_duplicate_user(user_manager):
    user_manager.add_user("john_doe", "john@example.com")
    with pytest.raises(ValueError):
        user_manager.add_user("john_doe", "another@example.com")
