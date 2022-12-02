import pytest
from google.api_core.exceptions import BadRequest
from google.cloud.bigquery import SchemaField

from bigquery_frame import BigQueryBuilder
from bigquery_frame import functions as f
from bigquery_frame.dataframe import strip_margin
from tests.utils import captured_output


def test_createOrReplaceTempView(bq: BigQueryBuilder):
    df = bq.sql("""SELECT 1 as id, "Bulbasaur" as name, ["Grass", "Poison"] as types, NULL as other_col""")
    df.createOrReplaceTempView("pokedex")
    df2 = bq.sql("""SELECT * FROM pokedex""")

    expected = [
        SchemaField(name="id", field_type="INTEGER", mode="NULLABLE"),
        SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
        SchemaField(name="types", field_type="STRING", mode="REPEATED"),
        SchemaField(name="other_col", field_type="INTEGER", mode="NULLABLE"),
    ]

    assert df2.schema == expected


def test_createOrReplaceTempView_with_reserved_keyword_alias(bq: BigQueryBuilder):
    """Some words like 'ALL' are reserved by BigQuery
    and may not be used as table names without being backticked."""
    bq.sql("""SELECT 1 as id""").createOrReplaceTempView("all")
    df = bq.table("all")

    expected = [SchemaField(name="id", field_type="INTEGER", mode="NULLABLE")]

    assert df.schema == expected


def test_createOrReplaceTempView_multiple_times(bq: BigQueryBuilder):
    """When we call df.createOrReplaceTempView with the same view name multiple times,
    it should overwrite the first view"""
    df1 = bq.sql("""SELECT 1 as id""")
    df2 = bq.sql("""SELECT 2 as id""")
    df1.createOrReplaceTempView("T")
    df2.createOrReplaceTempView("T")
    df = bq.table("T")
    assert [r["id"] for r in df.collect()] == [2]


def test_createOrReplaceTempView_cyclic_dependency(bq: BigQueryBuilder):
    """When we call df.createOrReplaceTempView with the same view name multiple times,
    it should overwrite the first view, but beware of cyclic dependencies!"""
    # This should work
    bq.sql("""SELECT 1 as id""").createOrReplaceTempView("T")
    bq.sql("""SELECT * FROM T""").createOrReplaceTempView("T2")
    bq.table("T2").collect()

    # But this should not
    bq.sql("""SELECT 1 as id""").createOrReplaceTempView("T")
    bq.sql("""SELECT * FROM T""").createOrReplaceTempView("T")
    with pytest.raises(BadRequest):
        bq.table("T").show()


def test_createOrReplaceTempTable(bq: BigQueryBuilder):
    df = bq.sql("""SELECT 1 as id, "Bulbasaur" as name, ["Grass", "Poison"] as types, NULL as other_col""")
    df.createOrReplaceTempTable("pokedex")
    df2 = bq.sql("""SELECT * FROM pokedex""")

    expected = [
        SchemaField(name="id", field_type="INTEGER", mode="NULLABLE"),
        SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
        SchemaField(name="types", field_type="STRING", mode="REPEATED"),
        SchemaField(name="other_col", field_type="INTEGER", mode="NULLABLE"),
    ]

    assert df2.schema == expected


def test_createOrReplaceTempTable_with_reserved_keyword_alias(bq: BigQueryBuilder):
    """Some words like 'ALL' are reserved by BigQuery
    and may not be used as table names without being backticked."""
    bq.sql("""SELECT 1 as id""").createOrReplaceTempTable("all")
    df = bq.table("all")

    expected = [SchemaField(name="id", field_type="INTEGER", mode="NULLABLE")]

    assert df.schema == expected


def test_show_with_keyword_alias(bq: BigQueryBuilder):
    """Some words like 'ALL' are reserved by BigQuery
    and may not be used as table names without being backticked."""
    df = bq.sql("""SELECT 1 as id""").alias("all")
    df.show()


def test_select(bq: BigQueryBuilder):
    """Select should work with either multiple args or a single argument which is a list"""
    df = bq.sql("""SELECT 1 as c1, 2 as c2""").select("c1", "c2").select(["c1", "c2"])

    expected = [
        SchemaField(name="c1", field_type="INTEGER", mode="NULLABLE"),
        SchemaField(name="c2", field_type="INTEGER", mode="NULLABLE"),
    ]

    assert df.schema == expected

    with pytest.raises(TypeError):
        df.select(["c1"], ["c2"])


def test_bare_strings(bq: BigQueryBuilder):
    df = bq.sql("""SELECT 1 as id, "Bulbasaur" as name, ["Grass", "Poison"] as types, NULL as other_col""")
    df2 = df.select("id", "name", "types")
    df2.createOrReplaceTempView("pokedex")
    df3 = bq.sql("""SELECT * FROM pokedex""")
    df4 = df3.withColumn("nb_types", "ARRAY_LENGTH(types)")
    df5 = df4.withColumn("name", "LOWER(name)", replace=True)

    expected = [
        SchemaField(name="id", field_type="INTEGER", mode="NULLABLE"),
        SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
        SchemaField(name="types", field_type="STRING", mode="REPEATED"),
        SchemaField(name="nb_types", field_type="INTEGER", mode="NULLABLE"),
    ]

    assert df5.schema == expected


def test_count(bq: BigQueryBuilder):
    df = bq.sql("""SELECT 1 as id UNION ALL SELECT 2 as id""")
    expected = 2
    assert df.count() == expected


def test_sort_with_aliased_column(bq: BigQueryBuilder):
    """
    GIVEN a DataFrame
    WHEN we sort it on an aliased column
    THEN the query should be valid
    """
    df = bq.sql("""SELECT id FROM UNNEST([3, 2, 1]) as id""")
    with captured_output() as (stdout, stderr):
        df.sort(df["id"].alias("_")).show()
        expected = strip_margin(
            """
            |+----+
            || id |
            |+----+
            ||  1 |
            ||  2 |
            ||  3 |
            |+----+
            |"""
        )
        assert stdout.getvalue() == expected


def test_take(bq: BigQueryBuilder):
    df = bq.sql("""SELECT id FROM UNNEST(GENERATE_ARRAY(1, 10, 1)) as id""")
    expected = 5
    assert len(df.take(5)) == expected


def test_union_with_common_dependency(bq: BigQueryBuilder):
    """Corner case that requires to use `_dedup_key_value_list` in `_apply_query`"""
    df1 = bq.sql("""SELECT id FROM UNNEST(GENERATE_ARRAY(1, 10, 1)) as id""")
    df2 = df1.where("MOD(id, 2) = 0")
    df3 = df1.where("MOD(id, 2) = 1")
    df4 = df2.union(df3)

    assert df4.count() == 10


def test_drop(bq: BigQueryBuilder):
    df = bq.sql("""SELECT 1 as id, "Bulbasaur" as name, ["Grass", "Poison"] as types, NULL as other_col""")
    df2 = df.drop("other_col", "col_that_does_not_exists")

    expected = [
        SchemaField(name="id", field_type="INTEGER", mode="NULLABLE"),
        SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
        SchemaField(name="types", field_type="STRING", mode="REPEATED"),
    ]

    assert df2.schema == expected


def test_sql_syntax_error(bq: BigQueryBuilder):
    df = bq.sql("""SELECT *""")

    with pytest.raises(Exception) as e:
        df.show()

    expected = "400 SELECT * must have a FROM clause at"
    assert expected in str(e.value)


def test_show_limit(bq: BigQueryBuilder):
    """When df.show() does not display all rows, a message should be printed"""
    df = bq.sql("""SELECT * FROM UNNEST([1, 2, 3]) as a""")

    with captured_output() as (stdout, stderr):
        df.show(1)
        expected = strip_margin(
            """
            |+---+
            || a |
            |+---+
            || 1 |
            |+---+
            |only showing top 1 row
            |"""
        )
        assert stdout.getvalue() == expected

    with captured_output() as (stdout, stderr):
        df.show(2)
        expected = strip_margin(
            """
            |+---+
            || a |
            |+---+
            || 1 |
            || 2 |
            |+---+
            |only showing top 2 rows
            |"""
        )
        assert stdout.getvalue() == expected

    with captured_output() as (stdout, stderr):
        df.show(3)
        expected = strip_margin(
            """
            |+---+
            || a |
            |+---+
            || 1 |
            || 2 |
            || 3 |
            |+---+
            |"""
        )
        assert stdout.getvalue() == expected


def test_determinism(bq: BigQueryBuilder):
    df1 = bq.sql("""SELECT 1 as id""").withColumn("a", f.lit("a"))
    df2 = bq.sql("""SELECT 1 as id""").withColumn("a", f.lit("a"))
    assert df1.compile() == df2.compile()


def test_determinism_with_temp_view(bq: BigQueryBuilder):
    bq.sql("""SELECT 1 as id""").withColumn("a", f.lit("a")).createOrReplaceTempView("T1")
    df1 = bq.sql("""SELECT * FROM T1""").withColumn("b", f.lit("b"))
    df2 = bq.sql("""SELECT * FROM T1""").withColumn("b", f.lit("b"))
    assert df1.compile() == df2.compile()
