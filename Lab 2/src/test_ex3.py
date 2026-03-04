# [ Test for Data Cleaning Function ]

import pandas as pd
from ex3 import clean_data


def test_clean_data_removes_nulls():
    df = pd.DataFrame({'A': [1, None, 3], 'B': [4, 5, 6]})
    result = clean_data(df)
    assert result.isnull().sum().sum() == 0

    # When you call .isnull() on a DataFrame,
    # Pandas returns a DataFrame of Booleans (True for null, False for not null).

    # First .sum(): By default, summing a DataFrame operates across rows for each column.
    # This returns a Series where the index is your column names
    # and the values are the count of nulls in that specific column.

    # Second .sum(): This sums the values of that Series,
    # collapsing the column-wise counts into a single scalar integer
    # representing the total number of nulls in the entire DataFrame.

    # Steps:
    # Any row containing at least one None or NaN is deleted.
    # so, the second row is removed, leaving only the first and third rows.
    # result.isnull() returns all False.
    # The first .sum() returns A: 0, B: 0.
    # The second .sum() returns 0.
