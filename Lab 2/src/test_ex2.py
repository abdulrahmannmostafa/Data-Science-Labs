# [ Multiple Test Functions ]

import pytest
from ex2 import add, divide


def test_add():
    assert add(2, 3) == 5, "Expected add(2, 3) to be 5"
    assert add(-1, 1) == 0, "Expected add(-1, 1) to be 0"
    assert add(0, 0) == 0, "Expected add(0, 0) to be 0"


def test_divide():
    assert divide(10, 2) == 5
    assert divide(-6, 3) == -2
    # The keyword argument match to assert that the exception matches a text or regex
    with pytest.raises(ValueError, match="Cannot divide by zero"):
        divide(5, 0)

    # Try: pytest -s
    # to captrure the print statement in the function
    print("All tests passed for divide function.")
