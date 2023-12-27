import warnings

from bigquery_frame import DataFrame
from bigquery_frame.transformations_impl.sort_all_arrays import sort_all_arrays


def normalize_arrays(df: DataFrame) -> DataFrame:
    """Given a DataFrame, sort all columns of type arrays (even nested ones) in a canonical order,
    making them comparable

    !!! warning
    This method is deprecated since version 0.5.0 and will be removed in version 0.6.0.
    Please use transformations.sort_all_arrays instead.

    >>> from bigquery_frame import BigQueryBuilder
    >>> bq = BigQueryBuilder()
    >>> df = bq.sql('SELECT [3, 2, 1] as a')

    >>> df.show()
    +-----------+
    |         a |
    +-----------+
    | [3, 2, 1] |
    +-----------+
    >>> normalize_arrays(df).show()
    +-----------+
    |         a |
    +-----------+
    | [1, 2, 3] |
    +-----------+

    >>> df = bq.sql('SELECT [STRUCT(2 as a, 1 as b), STRUCT(1 as a, 2 as b), STRUCT(1 as a, 1 as b)] as s')
    >>> df.show()
    +--------------------------------------------------------+
    |                                                      s |
    +--------------------------------------------------------+
    | [{'a': 2, 'b': 1}, {'a': 1, 'b': 2}, {'a': 1, 'b': 1}] |
    +--------------------------------------------------------+
    >>> normalize_arrays(df).show()
    +--------------------------------------------------------+
    |                                                      s |
    +--------------------------------------------------------+
    | [{'a': 1, 'b': 1}, {'a': 1, 'b': 2}, {'a': 2, 'b': 1}] |
    +--------------------------------------------------------+

    >>> df = bq.sql('''SELECT [
    ...         STRUCT([STRUCT(2 as a, 2 as b), STRUCT(2 as a, 1 as b)] as l2),
    ...         STRUCT([STRUCT(1 as a, 2 as b), STRUCT(1 as a, 1 as b)] as l2)
    ...     ] as l1
    ... ''')
    >>> df.show()
    +----------------------------------------------------------------------------------------------+
    |                                                                                           l1 |
    +----------------------------------------------------------------------------------------------+
    | [{'l2': [{'a': 2, 'b': 2}, {'a': 2, 'b': 1}]}, {'l2': [{'a': 1, 'b': 2}, {'a': 1, 'b': 1}]}] |
    +----------------------------------------------------------------------------------------------+
    >>> normalize_arrays(df).show()
    +----------------------------------------------------------------------------------------------+
    |                                                                                           l1 |
    +----------------------------------------------------------------------------------------------+
    | [{'l2': [{'a': 1, 'b': 1}, {'a': 1, 'b': 2}]}, {'l2': [{'a': 2, 'b': 1}, {'a': 2, 'b': 2}]}] |
    +----------------------------------------------------------------------------------------------+

    >>> df = bq.sql('''SELECT [
    ...         STRUCT(STRUCT(2 as a, 2 as b) as s),
    ...         STRUCT(STRUCT(1 as a, 2 as b) as s)
    ...     ] as l1
    ... ''')
    >>> df.show()
    +----------------------------------------------------+
    |                                                 l1 |
    +----------------------------------------------------+
    | [{'s': {'a': 2, 'b': 2}}, {'s': {'a': 1, 'b': 2}}] |
    +----------------------------------------------------+
    >>> normalize_arrays(df).show()
    +----------------------------------------------------+
    |                                                 l1 |
    +----------------------------------------------------+
    | [{'s': {'a': 1, 'b': 2}}, {'s': {'a': 2, 'b': 2}}] |
    +----------------------------------------------------+

    :return:
    """
    warning_message = (
        "The method bigquery_frame.transformations.normalize_arrays is deprecated since version 0.5.0 "
        "and will be removed in version 0.6.0. "
        "Please use transformations.sort_all_arrays instead."
    )
    warnings.warn(warning_message, category=DeprecationWarning)
    return sort_all_arrays(df)
