from collections.abc import Mapping

from bigquery_frame import DataFrame, nested
from bigquery_frame.nested_impl.package import AnyKindOfTransformation, resolve_nested_fields


def with_fields(df: DataFrame, fields: Mapping[str, AnyKindOfTransformation]) -> DataFrame:
    """Return a new [DataFrame][pyspark.sql.DataFrame] by adding or replacing (when they already exist) columns.

    This method is similar to the [DataFrame.withColumn][bigquery_frame.DataFrame.withColumn] method, with the extra
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
        A new DataFrame with the same fields as the input DataFrame, where the specified transformations have been
        applied to the corresponding fields. If a field name did not exist in the input DataFrame,
        it will be added to the output DataFrame. If it did exist, the original value will be replaced with the new one.

    Examples:
        *Example 1: non-repeated fields*
        >>> from bigquery_frame import BigQueryBuilder
        >>> from bigquery_frame import functions as f
        >>> from bigquery_frame import nested
        >>> bq = BigQueryBuilder()
        >>> df = bq.sql('''SELECT 1 as id, STRUCT(2 as a, 3 as b) as s''')
        >>> nested.print_schema(df)
        root
         |-- id: INTEGER (nullable = true)
         |-- s.a: INTEGER (nullable = true)
         |-- s.b: INTEGER (nullable = true)
        <BLANKLINE>
        >>> df.show(simplify_structs=True)
        +----+--------+
        | id |      s |
        +----+--------+
        |  1 | {2, 3} |
        +----+--------+

        Transformations on non-repeated fields may be expressed as a string representing a column name
        or a Column expression.
        >>> new_df = nested.with_fields(df, {
        ...     "s.id": "id",                                 # column name (string)
        ...     "s.c": f.col("s.a") + f.col("s.b")            # Column expression
        ... })
        >>> new_df.printSchema()
        root
         |-- id: INTEGER (NULLABLE)
         |-- s: RECORD (NULLABLE)
         |    |-- a: INTEGER (NULLABLE)
         |    |-- b: INTEGER (NULLABLE)
         |    |-- id: INTEGER (NULLABLE)
         |    |-- c: INTEGER (NULLABLE)
        <BLANKLINE>
        >>> new_df.show(simplify_structs=True)
        +----+--------------+
        | id |            s |
        +----+--------------+
        |  1 | {2, 3, 1, 5} |
        +----+--------------+

        *Example 2: repeated fields*
        >>> df = bq.sql('''
        ...     SELECT
        ...         1 as id,
        ...         [STRUCT(1 as a, STRUCT(2 as c) as b), STRUCT(3 as a, STRUCT(4 as c) as b)] as s
        ... ''')
        >>> nested.print_schema(df)
        root
         |-- id: INTEGER (nullable = true)
         |-- s!.a: INTEGER (nullable = true)
         |-- s!.b.c: INTEGER (nullable = true)
        <BLANKLINE>
        >>> df.show(simplify_structs=True)
        +----+----------------------+
        | id |                    s |
        +----+----------------------+
        |  1 | [{1, {2}}, {3, {4}}] |
        +----+----------------------+

        Transformations on repeated fields must be expressed as
        higher-order functions (lambda expressions or named functions).
        The value passed to this function will correspond to the last repeated element.
        >>> new_df = df.transform(nested.with_fields, {
        ...     "s!.b.d": lambda s: s["a"] + s["b"]["c"]}
        ... )
        >>> nested.print_schema(new_df)
        root
         |-- id: INTEGER (nullable = true)
         |-- s!.a: INTEGER (nullable = true)
         |-- s!.b.c: INTEGER (nullable = true)
         |-- s!.b.d: INTEGER (nullable = true)
        <BLANKLINE>
        >>> new_df.show(simplify_structs=True)
        +----+----------------------------+
        | id |                          s |
        +----+----------------------------+
        |  1 | [{1, {2, 3}}, {3, {4, 7}}] |
        +----+----------------------------+

        String and column expressions can be used on repeated fields as well,
        but their value will be repeated multiple times.
        >>> df.transform(nested.with_fields, {
        ...     "id": None,
        ...     "s!.a": "id",
        ...     "s!.b.c": f.lit(2)
        ... }).show(simplify_structs=True)
        +----+----------------------+
        | id |                    s |
        +----+----------------------+
        |  1 | [{1, {2}}, {1, {2}}] |
        +----+----------------------+

        *Example 3: field repeated twice*
        >>> df = bq.sql('SELECT 1 as id, [STRUCT([1, 2, 3] as e)] as s')
        >>> nested.print_schema(df)
        root
         |-- id: INTEGER (nullable = true)
         |-- s!.e!: INTEGER (nullable = false)
        <BLANKLINE>
        >>> df.show(simplify_structs=True)
        +----+---------------+
        | id |             s |
        +----+---------------+
        |  1 | [{[1, 2, 3]}] |
        +----+---------------+

        Here, the lambda expression will be applied to the last repeated element `e`.
        >>> df.transform(nested.with_fields, {"s!.e!": lambda e : e.cast("FLOAT64")}).show(simplify_structs=True)
        +----+---------------------+
        | id |                   s |
        +----+---------------------+
        |  1 | [{[1.0, 2.0, 3.0]}] |
        +----+---------------------+

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
        >>> new_df = df.transform(nested.with_fields, {
        ...  "s1!.values!": lambda s1, value : value - s1["average"]
        ... })
        >>> new_df.show(simplify_structs=True)
        +----+-------------------------------------------+
        | id |                                        s1 |
        +----+-------------------------------------------+
        |  1 | [{2, [-1, 0, 1]}, {3, [-2, -1, 0, 1, 2]}] |
        +----+-------------------------------------------+

        Extra arguments can be added to the left for each repetition level, up to the root level.
        >>> new_df = df.transform(nested.with_fields, {
        ...  "s1!.values!": lambda root, s1, value : value - s1["average"] + root["id"]
        ... })
        >>> new_df.show(simplify_structs=True)
        +----+-----------------------------------------+
        | id |                                      s1 |
        +----+-----------------------------------------+
        |  1 | [{2, [0, 1, 2]}, {3, [-1, 0, 1, 2, 3]}] |
        +----+-----------------------------------------+
    """
    default_columns = {field: None for field in nested.fields(df)}
    fields = {**default_columns, **fields}
    return df.select(*resolve_nested_fields(fields, starting_level=df))
