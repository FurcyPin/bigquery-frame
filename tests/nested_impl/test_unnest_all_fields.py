from bigquery_frame import BigQueryBuilder, nested
from bigquery_frame.utils import strip_margin


def test_unnest_fields_with_fields_having_same_name_inside_structs(bq: BigQueryBuilder):
    """GIVEN a DataFrame with fields in structs having the same name as root-level columns
    WHEN we apply unnest_fields on it
    THEN the result should be correct
    """
    df = bq.sql(
        """
        SELECT
            1 as id,
            STRUCT(2 as id) as s1
    """,
    )
    assert df.show_string(simplify_structs=True) == strip_margin(
        """
        |+----+-----+
        || id |  s1 |
        |+----+-----+
        ||  1 | {2} |
        |+----+-----+""",
    )
    assert nested.fields(df) == ["id", "s1.id"]
    result_df_list = nested.unnest_all_fields(df, keep_columns=["id"])
    assert list(result_df_list.keys()) == [""]
    result_df_list[""].show()
    assert result_df_list[""].show_string(simplify_structs=True) == strip_margin(
        """
        |+----+----------------+
        || id | s1__STRUCT__id |
        |+----+----------------+
        ||  1 |              2 |
        |+----+----------------+""",
    )


def test_unnest_fields_with_fields_having_same_name_inside_array_structs(bq: BigQueryBuilder):
    """GIVEN a DataFrame with fields in array of struct having the same name as root-level columns
    WHEN we apply unnest_fields on it
    THEN the result should be correct
    """
    df = bq.sql(
        """
        SELECT
            1 as id,
            STRUCT(2 as id) as s1,
            [STRUCT(3 as id, [STRUCT(5 as id), STRUCT(6 as id)] as s3)] as s2,
    """,
    )
    assert df.show_string(simplify_structs=True) == strip_margin(
        """
        |+----+-----+-------------------+
        || id |  s1 |                s2 |
        |+----+-----+-------------------+
        ||  1 | {2} | [{3, [{5}, {6}]}] |
        |+----+-----+-------------------+""",
    )

    assert nested.fields(df) == ["id", "s1.id", "s2!.id", "s2!.s3!.id"]
    result_df_list = nested.unnest_all_fields(df, keep_columns=["id"])
    assert result_df_list[""].show_string() == strip_margin(
        """
        |+----+----------------+
        || id | s1__STRUCT__id |
        |+----+----------------+
        ||  1 |              2 |
        |+----+----------------+""",
    )
    assert result_df_list["s2!"].show_string() == strip_margin(
        """
        |+----+-------------------------+
        || id | s2__ARRAY____STRUCT__id |
        |+----+-------------------------+
        ||  1 |                       3 |
        |+----+-------------------------+""",
    )
    assert result_df_list["s2!.s3!"].show_string() == strip_margin(
        """
        |+----+----------------------------------------------+
        || id | s2__ARRAY____STRUCT__s3__ARRAY____STRUCT__id |
        |+----+----------------------------------------------+
        ||  1 |                                            5 |
        ||  1 |                                            6 |
        |+----+----------------------------------------------+""",
    )


def test_unnest_fields_with_fields_having_same_name_inside_array_structs_and_names_are_keywords(bq: BigQueryBuilder):
    """GIVEN a DataFrame with fields in array of struct having the same name as root-level columns
      AND if the names are reserved keywords
    WHEN we apply unnest_fields on it
    THEN the result should be correct
    """
    df = bq.sql(
        """
        SELECT
            1 as `group`,
            STRUCT(2 as `group`) as s1,
            [STRUCT(3 as `group`, [STRUCT(5 as `group`), STRUCT(6 as `group`)] as s3)] as s2,
    """,
    )
    assert df.show_string(simplify_structs=True) == strip_margin(
        """
        |+-------+-----+-------------------+
        || group |  s1 |                s2 |
        |+-------+-----+-------------------+
        ||     1 | {2} | [{3, [{5}, {6}]}] |
        |+-------+-----+-------------------+""",
    )

    assert nested.fields(df) == ["group", "s1.group", "s2!.group", "s2!.s3!.group"]
    result_df_list = nested.unnest_all_fields(df, keep_columns=["group"])
    assert result_df_list[""].show_string() == strip_margin(
        """
        |+-------+-------------------+
        || group | s1__STRUCT__group |
        |+-------+-------------------+
        ||     1 |                 2 |
        |+-------+-------------------+""",
    )
    assert result_df_list["s2!"].show_string() == strip_margin(
        """
        |+-------+----------------------------+
        || group | s2__ARRAY____STRUCT__group |
        |+-------+----------------------------+
        ||     1 |                          3 |
        |+-------+----------------------------+""",
    )
    assert result_df_list["s2!.s3!"].show_string() == strip_margin(
        """
        |+-------+-------------------------------------------------+
        || group | s2__ARRAY____STRUCT__s3__ARRAY____STRUCT__group |
        |+-------+-------------------------------------------------+
        ||     1 |                                               5 |
        ||     1 |                                               6 |
        |+-------+-------------------------------------------------+""",
    )
