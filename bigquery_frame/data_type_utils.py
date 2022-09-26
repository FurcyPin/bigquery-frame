from typing import Generator, List, Optional, Tuple

from google.cloud.bigquery import SchemaField

from bigquery_frame.dataframe import is_repeated
from bigquery_frame.utils import assert_true

BIGQUERY_TYPE_ALIASES = {
    "INT64": "INT64",
    "INT": "INT64",
    "SMALLINT": "INT64",
    "INTEGER": "INT64",
    "BIGINT": "INT64",
    "TINYINT": "INT64",
    "BYTEINT": "INT64",
    "NUMERIC": "NUMERIC",
    "DECIMAL": "NUMERIC",
    "FLOAT": "FLOAT64",
    "BIGNUMERIC": "BIGNUMERIC",
    "BIGDECIMAL": "BIGNUMERIC",
}
"""This dict gives for each type the most commonly use alias
Source: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#numeric_types
"""

BIGQUERY_TYPE_CONVERSIONS = {
    "BOOL": "STRING",
    "INT64": "NUMERIC",
    "NUMERIC": "BIGNUMERIC",
    "BIGNUMERIC": "FLOAT64",
    "FLOAT64": "STRING",
    "DATE": "DATETIME",
    "DATETIME": "TIMESTAMP",
    "TIMESTAMP": "STRING",
    "TIME": "STRING",
    "BYTES": "STRING",
    "GEOGRAPHY": "STRING",
}
"""This dict gives for each type the smallest supertype that it can be converted to.
Source: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_rules
"""


def resolve_type_alias(tpe: str) -> str:
    """Return the most commonly used alias for this type, if any. Else, return the input.

    Based on: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_rules

    :param tpe: a BigQuery type
    :return: the most common alias for this type
    """
    return BIGQUERY_TYPE_ALIASES.get(tpe, tpe)


def list_wider_types(tpe: str) -> Generator[str, None, None]:
    """Return a generator of all wider types into which the given type can be cast.

    Based on: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_rules

    >>> list(list_wider_types("INT64"))
    ['INT64', 'NUMERIC', 'BIGNUMERIC', 'FLOAT64', 'STRING']
    >>> list(list_wider_types("INTEGER"))
    ['INT64', 'NUMERIC', 'BIGNUMERIC', 'FLOAT64', 'STRING']
    >>> list(list_wider_types("STRING"))
    ['STRING']
    >>> list(list_wider_types("FOO"))
    ['FOO']

    :param tpe: input type
    :return: a generator of wider types
    """
    current_type = resolve_type_alias(tpe)
    max_loop = 10
    counter = 0
    next_type = BIGQUERY_TYPE_CONVERSIONS.get(current_type, None)
    while current_type != next_type:
        counter += 1
        yield current_type
        assert_true(
            counter < max_loop, "This should not happen. Please check thatBIGQUERY_CONVERSIONS does not contain a loop."
        )
        if next_type is None:
            break
        else:
            current_type = next_type
            next_type = BIGQUERY_TYPE_CONVERSIONS.get(current_type, None)


def find_wider_type_for_string_types(t1: str, t2: str) -> Optional[str]:
    """Find the smallest common type into which the two given type can both be cast.
    Returns None if no such type exists.

    Based on: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_rules

    >>> find_wider_type_for_string_types("INT64", "DECIMAL")
    'NUMERIC'
    >>> find_wider_type_for_string_types("TINYINT", "INTEGER")
    'INT64'
    >>> find_wider_type_for_string_types("DECIMAL", "FLOAT64")
    'FLOAT64'
    >>> find_wider_type_for_string_types("DATE", "TIMESTAMP")
    'TIMESTAMP'
    >>> find_wider_type_for_string_types("DATE", "TIME")
    'STRING'
    >>> find_wider_type_for_string_types("WRONG_TYPE", "INT")

    :param t1: a BigQuery type
    :param t2: another BigQuery type
    :return: the smallest common type for the two
    """
    l1 = list(list_wider_types(t1))
    for t in list_wider_types(t2):
        if t in l1:
            return t
    return None


def find_common_type_for_fields(left_field: SchemaField, right_field: SchemaField):
    if is_repeated(right_field) != is_repeated(left_field):
        return None
    elif right_field.field_type == left_field.field_type:
        return None
    else:
        return find_wider_type_for_string_types(left_field.field_type, right_field.field_type)


def get_common_columns(
    left_schema: List[SchemaField], right_schema: List[SchemaField]
) -> List[Tuple[str, Optional[str]]]:
    """Return a list of common Columns between two DataFrame schemas, along with the widest common type
    of for the two columns.

    When columns already have the same type or have incompatible types, they are simply not cast.

    >>> from bigquery_frame import BigQueryBuilder
    >>> bq = BigQueryBuilder()
    >>> df1 = bq.sql('''SELECT 'A' as id, CAST(1 as BIGINT) as d, 'a' as a, 'x' as b''')
    >>> df2 = bq.sql('''SELECT 'A' as id, CAST(1 as FLOAT64) as d, ['a'] as a, 'x' as c''')
    >>> get_common_columns(df1.schema, df2.schema)
    [('id', None), ('d', 'FLOAT64'), ('a', None)]

    :param left_schema:
    :param right_schema:
    :return:
    """
    left_fields = {field.name: field for field in left_schema}
    right_fields = {field.name: field for field in right_schema}

    def get_columns():
        for name, left_field in left_fields.items():
            if name in right_fields:
                right_field: SchemaField = right_fields[name]
                yield name, find_common_type_for_fields(left_field, right_field)

    return list(get_columns())
