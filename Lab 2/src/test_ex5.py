# [ Parameterization ]:
# To avoid writing a ton of duplicated code,
# we can use parameterized testing to run the same test function
# with different sets of inputs and expected outputs.

import pytest
from ex5 import is_prime


# Takes a list of tubles, of multiple inputs and expected outputs
# NOTE: num and expected are the names of the parameters
# that will be passed to the test function
@pytest.mark.parametrize(
    "num, expected",
    [
        (1, False),
        (2, True),
        (3, True),
        (4, False),
        (5, True),
        (10, False),
        (13, True),
    ],
)
def test_is_prime(num, expected):
    # This test will run once for each tuple in the parameter list
    assert is_prime(num) == expected
    # NOTE: when you pytest this file, 7 tests will run, one for each tuple in the parameter list
