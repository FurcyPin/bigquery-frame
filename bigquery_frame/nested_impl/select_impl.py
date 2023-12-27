from typing import Mapping

from bigquery_frame import DataFrame
from bigquery_frame.nested_impl.package import ColumnTransformation, resolve_nested_fields


# Workaround: This file is temporarily called "select_impl.py" instead of "select.py" to work around a bug in PyCharm.
# https://youtrack.jetbrains.com/issue/PY-58068
def select(df: DataFrame, fields: Mapping[str, ColumnTransformation]) -> DataFrame:
    """Project a set of expressions and returns a new [DataFrame][bigquery_frame.DataFrame].

    This method is similar to the [DataFrame.select][bigquery_frame.DataFrame.select] method, with the extra
    capability of working on nested and repeated fields (structs and arrays).

    The syntax for field names works as follows:

    - "." is the separator for struct elements
    - "!" must be appended at the end of fields that are repeated (arrays)

    The following types of transformation are allowed:

    - String and column expressions can be used on any non-repeated field, even nested ones.
    - When working on repeated fields, transformations must be expressed as higher order functions
      (e.g. lambda expressions). String and column expressions can be used on repeated fields as well,
      but their value will be repeated multiple times.
    - When working on multiple levels of nested arrays, higher order functions may take multiple arguments,
      corresponding to each level of repetition (See Example 5.).
    - `None` can also be used to represent the identity transformation, this is useful to select a field without
       changing and without having to repeat its name.

    Args:
        df: A DataFrame
        fields: A Dict(field_name, transformation_to_apply)

    Returns:
        A new DataFrame where only the specified field have been selected and the corresponding
        transformations were applied to each of them.

    Examples:

        *Example 1: non-repeated fields*

        >>> from bigquery_frame import BigQueryBuilder
        >>> from bigquery_frame import functions as f
        >>> from bigquery_frame import nested
        >>> bq = BigQueryBuilder()
        >>> df = bq.sql('''SELECT 1 as id, STRUCT(2 as a, 3 as b) as s''')
        >>> df.printSchema()
        root
         |-- id: INTEGER (NULLABLE)
         |-- s: RECORD (NULLABLE)
         |    |-- a: INTEGER (NULLABLE)
         |    |-- b: INTEGER (NULLABLE)
        <BLANKLINE>
        >>> df.show(simplify_structs=True)
        +----+--------+
        | id |      s |
        +----+--------+
        |  1 | {2, 3} |
        +----+--------+

        Transformations on non-repeated fields may be expressed as a string representing a column name,
        a Column expression or None.
        (In this example the column "id" will be dropped because it was not selected)
        >>> new_df = df.transform(nested.select, {
        ...     "s.a": "s.a",                        # Column name (string)
        ...     "s.b": None,                         # None: use to keep a column without having to repeat its name
        ...     "s.c": f.col("s.a") + f.col("s.b")   # Column expression
        ... })
        >>> new_df.printSchema()
        root
         |-- s: RECORD (NULLABLE)
         |    |-- a: INTEGER (NULLABLE)
         |    |-- b: INTEGER (NULLABLE)
         |    |-- c: INTEGER (NULLABLE)
        <BLANKLINE>

        >>> new_df.show(simplify_structs=True)
        +-----------+
        |         s |
        +-----------+
        | {2, 3, 5} |
        +-----------+

        *Example 2: repeated fields*

        >>> df = bq.sql('SELECT 1 as id, [STRUCT(1 as a, 2 as b), STRUCT(3 as a, 4 as b)] as s')
        >>> nested.print_schema(df)
        root
         |-- id: INTEGER (nullable = true)
         |-- s!.a: INTEGER (nullable = true)
         |-- s!.b: INTEGER (nullable = true)
        <BLANKLINE>
        >>> df.show(simplify_structs=True)
        +----+------------------+
        | id |                s |
        +----+------------------+
        |  1 | [{1, 2}, {3, 4}] |
        +----+------------------+

        Transformations on repeated fields must be expressed as higher-order
        functions (lambda expressions or named functions).
        The value passed to this function will correspond to the last repeated element.
        >>> df.transform(nested.select, {
        ...     "s!.a": lambda s: s["a"],
        ...     "s!.b": None,
        ...     "s!.c": lambda s: s["a"] + s["b"]
        ... }).show(simplify_structs=True)
        +------------------------+
        |                      s |
        +------------------------+
        | [{1, 2, 3}, {3, 4, 7}] |
        +------------------------+

        String and column expressions can be used on repeated fields as well,
        but their value will be repeated multiple times.
        >>> df.transform(nested.select, {
        ...     "id": None,
        ...     "s!.a": "id",
        ...     "s!.b": f.lit(2)
        ... }).show(simplify_structs=True)
        +----+------------------+
        | id |                s |
        +----+------------------+
        |  1 | [{1, 2}, {1, 2}] |
        +----+------------------+

        *Example 3: field repeated twice*
        >>> df = bq.sql('''
        ...     SELECT
        ...         1 as id,
        ...         [STRUCT([1, 2, 3] as e)] as s1,
        ...         [STRUCT([4, 5, 6] as e)] as s2
        ... ''')
        >>> nested.print_schema(df)
        root
         |-- id: INTEGER (nullable = true)
         |-- s1!.e!: INTEGER (nullable = false)
         |-- s2!.e!: INTEGER (nullable = false)
        <BLANKLINE>
        >>> df.show(simplify_structs=True)
        +----+---------------+---------------+
        | id |            s1 |            s2 |
        +----+---------------+---------------+
        |  1 | [{[1, 2, 3]}] | [{[4, 5, 6]}] |
        +----+---------------+---------------+

        Here, the lambda expression will be applied to the last repeated element `e`.
        >>> new_df = df.transform(nested.select, {
        ...  "s1!.e!": None,
        ...  "s2!.e!": lambda e : e.cast("FLOAT64")
        ... })
        >>> nested.print_schema(new_df)
        root
         |-- s1!.e!: INTEGER (nullable = false)
         |-- s2!.e!: FLOAT (nullable = false)
        <BLANKLINE>
        >>> new_df.show(simplify_structs=True)
        +---------------+---------------------+
        |            s1 |                  s2 |
        +---------------+---------------------+
        | [{[1, 2, 3]}] | [{[4.0, 5.0, 6.0]}] |
        +---------------+---------------------+

        *Example 4: Accessing multiple repetition levels*
        >>> df = bq.sql('''
        ...     SELECT
        ...         1 as id,
        ...         [
        ...             STRUCT(2 as average, [1, 2, 3] as values),
        ...             STRUCT(3 as average, [1, 2, 3, 4, 5] as values)
        ...         ] as s1
        ... ''')
        >>> nested.print_schema(df)
        root
         |-- id: INTEGER (nullable = true)
         |-- s1!.average: INTEGER (nullable = true)
         |-- s1!.values!: INTEGER (nullable = false)
        <BLANKLINE>
        >>> df.show(simplify_structs=True)
        +----+----------------------------------------+
        | id |                                     s1 |
        +----+----------------------------------------+
        |  1 | [{2, [1, 2, 3]}, {3, [1, 2, 3, 4, 5]}] |
        +----+----------------------------------------+

        Here, the transformation applied to "s1!.values!" takes two arguments.
        >>> new_df = df.transform(nested.select, {
        ...  "id": None,
        ...  "s1!.average": None,
        ...  "s1!.values!": lambda s1, value : value - s1["average"]
        ... })
        >>> new_df.show(simplify_structs=True)
        +----+-------------------------------------------+
        | id |                                        s1 |
        +----+-------------------------------------------+
        |  1 | [{2, [-1, 0, 1]}, {3, [-2, -1, 0, 1, 2]}] |
        +----+-------------------------------------------+

        Extra arguments can be added to the left for each repetition level, up to the root level.
        >>> new_df = df.transform(nested.select, {
        ...  "id": None,
        ...  "s1!.average": None,
        ...  "s1!.values!": lambda root, s1, value : value - s1["average"] + root["id"]
        ... })
        >>> new_df.show(simplify_structs=True)
        +----+-----------------------------------------+
        | id |                                      s1 |
        +----+-----------------------------------------+
        |  1 | [{2, [0, 1, 2]}, {3, [-1, 0, 1, 2, 3]}] |
        +----+-----------------------------------------+

    """
    return df.select(*resolve_nested_fields(fields, starting_level=df))
