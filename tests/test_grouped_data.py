from bigquery_frame import BigQueryBuilder
from bigquery_frame.utils import strip_margin


def test_groupBy_with_struct_columns(bq: BigQueryBuilder):
    """GIVEN a DataFrame with nested fields
    WHEN we use a pivot.agg statement
    THEN the result should be correct
    """
    df = bq.sql(
        """
        SELECT * FROM UNNEST([
            STRUCT(STRUCT(2 as age, "Alice" as name, 80 as height) as child),
            STRUCT(STRUCT(3 as age, "Alice" as name, 100 as height) as child),
            STRUCT(STRUCT(5 as age, "Bob" as name, 120 as height) as child),
            STRUCT(STRUCT(10 as age, "Bob" as name, 140 as height) as child)
        ])
    """,
    )
    assert df.groupBy("child.name").avg("child.age").sort("name").show_string() == strip_margin(
        """
        |+-------+---------+
        ||  name | avg_age |
        |+-------+---------+
        || Alice |     2.5 |
        ||   Bob |     7.5 |
        |+-------+---------+""",
    )

    assert df.groupBy().avg("child.age", "child.height").show_string() == strip_margin(
        """
        |+---------+------------+
        || avg_age | avg_height |
        |+---------+------------+
        ||     5.0 |      110.0 |
        |+---------+------------+""",
    )


def test_pivot_with_struct_columns(bq: BigQueryBuilder):
    """GIVEN a DataFrame with nested fields
    WHEN we use a pivot.agg statement
    THEN the result should be correct
    """
    df = bq.sql(
        """
        SELECT * FROM UNNEST([
            STRUCT("expert" as training, STRUCT("dotNET" as course, 2012 as year, 10000 as earnings) as sales) ,
            STRUCT("junior" as training, STRUCT("Java" as course, 2012 as year, 20000 as earnings) as sales) ,
            STRUCT("expert" as training, STRUCT("dotNET" as course, 2012 as year, 5000 as earnings) as sales) ,
            STRUCT("junior" as training, STRUCT("dotNET" as course, 2013 as year, 48000 as earnings) as sales) ,
            STRUCT("expert" as training, STRUCT("Java" as course, 2013 as year, 30000 as earnings) as sales)
        ])
    """,
    )
    assert df.show_string(simplify_structs=True) == strip_margin(
        """
        |+----------+-----------------------+
        || training |                 sales |
        |+----------+-----------------------+
        ||   expert | {dotNET, 2012, 10000} |
        ||   junior |   {Java, 2012, 20000} |
        ||   expert |  {dotNET, 2012, 5000} |
        ||   junior | {dotNET, 2013, 48000} |
        ||   expert |   {Java, 2013, 30000} |
        |+----------+-----------------------+""",
    )
    df1 = df.groupBy("training").pivot("sales.course", ["dotNET", "Java"]).sum("sales.earnings")
    assert df1.show_string(simplify_structs=True) == strip_margin(
        """
    |+----------+--------+-------+
    || training | dotNET |  Java |
    |+----------+--------+-------+
    ||   expert |  15000 | 30000 |
    ||   junior |  48000 | 20000 |
    |+----------+--------+-------+""",
    )
    df2 = df.groupBy().pivot("sales.course", ["dotNET", "Java"]).sum("sales.earnings")
    assert df2.show_string(simplify_structs=True) == strip_margin(
        """
    |+--------+-------+
    || dotNET |  Java |
    |+--------+-------+
    ||  15000 | 30000 |
    ||  48000 | 20000 |
    |+--------+-------+""",
    )
    df3 = df.groupBy("training").pivot("sales.course").sum("sales.earnings")
    assert df3.show_string(simplify_structs=True) == strip_margin(
        """
    |+----------+-------+--------+
    || training |  Java | dotNET |
    |+----------+-------+--------+
    ||   expert | 30000 |  15000 |
    ||   junior | 20000 |  48000 |
    |+----------+-------+--------+""",
    )
    df4 = df.groupBy().pivot("sales.course").sum("sales.earnings")
    assert df4.show_string(simplify_structs=True) == strip_margin(
        """
    |+-------+--------+
    ||  Java | dotNET |
    |+-------+--------+
    || 30000 |  15000 |
    || 20000 |  48000 |
    |+-------+--------+""",
    )
