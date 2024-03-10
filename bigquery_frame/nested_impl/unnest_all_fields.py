from typing import Dict, List, Optional

from bigquery_frame import DataFrame, nested
from bigquery_frame.field_utils import is_sub_field_or_equal_to_any
from bigquery_frame.nested_impl.package import unnest_fields


def unnest_all_fields(df: DataFrame, keep_columns: Optional[List[str]] = None) -> Dict[str, DataFrame]:
    """Given a DataFrame, return a dict of {granularity: DataFrame} where all arrays have been recursively
    unnested (a.k.a. exploded).
    This produce one DataFrame for each possible granularity.

    For instance, given a DataFrame with the following flattened schema:
        id
        s1.a
        s2!.b
        s2!.c
        s2!.s3!.d
        s4!.e
        s4!.f

    This will produce a dict with four granularity - DataFrames entries:
        - '': DataFrame[id, s1.a] ('' corresponds to the root granularity)
        - 's2': DataFrame[s2!.b, s2!.c]
        - 's2!.s3': DataFrame[s2!.s3!.d]
        - 's4': DataFrame[s4!.e, s4!.f]

    !!! warning "Limitation: BigQuery does not support dots and exclamation marks in column names"
        For this reason, dots are replaced with the string "__ARRAY__" and exclamation mark are replaced
        with "__STRUCT__". When manipulating column names, you can use the utils methods from the
        module `bigquery_frame.special_characters` to reverse the replacement.

    Args:
        df: A Spark DataFrame
        keep_columns: Names of columns that should be kept while unnesting

    Returns:
        A list of DataFrames

    Examples:
        >>> from bigquery_frame import BigQueryBuilder
        >>> from bigquery_frame import nested
        >>> bq = BigQueryBuilder()
        >>> df = bq.sql('''
        ...     SELECT
        ...         1 as id,
        ...         STRUCT(2 as a) as s1,
        ...         [STRUCT(3 as b, 4 as c, [STRUCT(5 as d), STRUCT(6 as d)] as s3)] as s2,
        ...         [STRUCT(7 as e, 8 as f), STRUCT(9 as e, 10 as f)] as s4
        ... ''')
        >>> df.show(simplify_structs=True)
        +----+-----+----------------------+-------------------+
        | id |  s1 |                   s2 |                s4 |
        +----+-----+----------------------+-------------------+
        |  1 | {2} | [{3, 4, [{5}, {6}]}] | [{7, 8}, {9, 10}] |
        +----+-----+----------------------+-------------------+

        >>> nested.fields(df)
        ['id', 's1.a', 's2!.b', 's2!.c', 's2!.s3!.d', 's4!.e', 's4!.f']
        >>> result_df_list = nested.unnest_all_fields(df, keep_columns=["id"])
        >>> for cols, result_df in result_df_list.items():
        ...     print(cols)
        ...     result_df.show()
        <BLANKLINE>
        +----+---------------+
        | id | s1__STRUCT__a |
        +----+---------------+
        |  1 |             2 |
        +----+---------------+
        s2!
        +----+------------------------+------------------------+
        | id | s2__ARRAY____STRUCT__b | s2__ARRAY____STRUCT__c |
        +----+------------------------+------------------------+
        |  1 |                      3 |                      4 |
        +----+------------------------+------------------------+
        s2!.s3!
        +----+---------------------------------------------+
        | id | s2__ARRAY____STRUCT__s3__ARRAY____STRUCT__d |
        +----+---------------------------------------------+
        |  1 |                                           5 |
        |  1 |                                           6 |
        +----+---------------------------------------------+
        s4!
        +----+------------------------+------------------------+
        | id | s4__ARRAY____STRUCT__e | s4__ARRAY____STRUCT__f |
        +----+------------------------+------------------------+
        |  1 |                      7 |                      8 |
        |  1 |                      9 |                     10 |
        +----+------------------------+------------------------+
    """
    if keep_columns is None:
        keep_columns = []
    fields_to_unnest = [field for field in nested.fields(df) if not is_sub_field_or_equal_to_any(field, keep_columns)]
    return unnest_fields(df, fields_to_unnest, keep_fields=keep_columns)
