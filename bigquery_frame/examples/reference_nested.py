from bigquery_frame import DataFrame


def fields() -> None:
    """First, let's distinguish the notion of `Column` and `Field`.
    Both terms are already used in Spark, but we chose here to make the following distinction:

    - A `Column` is a root-level column of a DataFrame.
    - A `Field` is any column or sub-column inside a struct of the DataFrame.

    Examples: Example: let's consider the following DataFrame

        >>> from bigquery_frame.examples.reference_nested import _get_sample_data
        >>> df = _get_sample_data()
        >>> df.show(simplify_structs=True)  # noqa: E501 # doctest: +NORMALIZE_WHITESPACE
        +----+-------------------------+-----------------+
        | id |                    name |           types |
        +----+-------------------------+-----------------+
        |  1 | {Bulbasaur, Bulbizarre} | [Grass, Poison] |
        +----+-------------------------+-----------------+

        >>> df.printSchema()
        root
         |-- id: INTEGER (NULLABLE)
         |-- name: RECORD (NULLABLE)
         |    |-- english: STRING (NULLABLE)
         |    |-- french: STRING (NULLABLE)
         |-- types: STRING (REPEATED)
        <BLANKLINE>

        This DataFrame has 3 columns:

        ```
        id
        name
        types
        ```

        But it has 4 fields:

        ```
        id
        name.english
        name.french
        types!
        ```

        This can be seen by using the method
        [`bigquery_frame.nested.print_schema`][bigquery_frame.nested_impl.print_schema.print_schema]

        >>> from bigquery_frame import nested
        >>> nested.print_schema(df)
        root
         |-- id: INTEGER (nullable = true)
         |-- name.english: STRING (nullable = true)
         |-- name.french: STRING (nullable = true)
         |-- types!: STRING (nullable = false)
        <BLANKLINE>

        As we can see, some field names contain dots `.` or exclamation marks `!`, they convey the following
        meaning:

        - A dot `.` represents a struct.
        - An exclamation mark `!` represents an array.

        While the *dot* syntax for *structs* should feel familiar to users, the exclamation mark `!` should feel new.
        It is used as a *repetition marker* indicating that this field is repeated.

        !!! tip "Tip"
            It is important to not forget to use exclamation marks `!` when mentionning a field.
            For instance:

            - `types` designates the root-level field which is of type `ARRAY<STRING>`
            - `types!` designates the elements inside this array

            In particular, if a field `"my_field"` is of type `ARRAY<ARRAY<STRING>>`, the innermost elements of the
            arrays will be designated as `"my_field!!"` with two exclamation marks.

        !!! warning "Limitation: Do not use dots, exclamation marks or percents in field names"
            Given the syntax used, every method defined in the `bigquery_frame.nested` module assumes that all field
            names in DataFrames do not contain any dot `.`, exclamation mark `!` or percents `%`.
            This can be worked around using the transformation
            [`bigquery_frame.transformations.transform_all_field_names`]
            [bigquery_frame.transformations_impl.transform_all_field_names.transform_all_field_names].
    """
    # This is a hacky way to have doctests that runs in the pipeline and are usable in the doc thanks to mkdocstrings


def _get_sample_data() -> DataFrame:
    from bigquery_frame import BigQueryBuilder

    bq = BigQueryBuilder()
    df = bq.sql(
        """
        SELECT
            1 as id,
            STRUCT("Bulbasaur" as english, "Bulbizarre" as french) as name,
            ["Grass", "Poison"] as types
    """,
    )
    return df
