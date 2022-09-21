from bigquery_frame.dataframe import strip_margin
from bigquery_frame.utils import number_lines


def test_number_lines():
    s = "\n".join([str(i) for i in range(1, 10)])
    expected = strip_margin(
        """
    |1: 1
    |2: 2
    |3: 3
    |4: 4
    |5: 5
    |6: 6
    |7: 7
    |8: 8
    |9: 9"""
    )
    assert number_lines(s) == expected

    s = "\n".join([str(i) for i in range(1, 11)])
    expected = strip_margin(
        """
    |01: 1
    |02: 2
    |03: 3
    |04: 4
    |05: 5
    |06: 6
    |07: 7
    |08: 8
    |09: 9
    |10: 10"""
    )
    assert number_lines(s) == expected
