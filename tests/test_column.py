import pytest

from bigquery_frame import BigQueryBuilder
from bigquery_frame import functions as f
from bigquery_frame.dataframe import strip_margin
from tests.utils import captured_output


def test_when_otherwise(bq: BigQueryBuilder):
    df = bq.sql(
        """
        SELECT
            *
        FROM UNNEST ([
            STRUCT(false as a),
            STRUCT(true as a),
            STRUCT(null as a)
        ])
    """
    )
    expected = strip_margin(
        """
        |+-------+---+
        ||     a | b |
        |+-------+---+
        || False | 1 |
        ||  True | 1 |
        ||  null | 0 |
        |+-------+---+
        |"""
    )
    with captured_output() as (stdout, stderr):
        a = f.col("a").alias("a")
        # Operators must be compatible with literals, hence the "True & a"
        df.withColumn("b", f.when(a.isNull(), f.lit(0)).otherwise(f.lit(1)).cast("STRING")).show()
        assert stdout.getvalue() == expected


def test_alias_with_keyword(bq: BigQueryBuilder):
    """
    GIVEN a column
    WHEN we alias is with a name which is a reserved SQL keyword
    THEN the query should remain valid
    """
    df = bq.sql("SELECT 1 as a")
    expected = strip_margin(
        """
        |+-------+
        || group |
        |+-------+
        ||     1 |
        |+-------+
        |"""
    )
    with captured_output() as (stdout, stderr):
        df.select(f.col("a").alias("group")).show()
        assert stdout.getvalue() == expected


def test_and(bq: BigQueryBuilder):
    df = bq.sql(
        """
        SELECT
            *
        FROM UNNEST ([
            STRUCT(false as a, false as b),
            STRUCT(true as a, false as b),
            STRUCT(null as a, false as b),
            STRUCT(false as a, true as b),
            STRUCT(true as a, true as b),
            STRUCT(null as a, true as b),
            STRUCT(false as a, null as b),
            STRUCT(true as a, null as b),
            STRUCT(null as a, null as b)
        ])
    """
    )
    expected = strip_margin(
        """
        |+-------+-------+-------+
        ||     a |     b |     c |
        |+-------+-------+-------+
        || False | False | False |
        ||  True | False | False |
        ||  null | False | False |
        || False |  True | False |
        ||  True |  True |  True |
        ||  null |  True |  null |
        || False |  null | False |
        ||  True |  null |  null |
        ||  null |  null |  null |
        |+-------+-------+-------+
        |"""
    )
    with captured_output() as (stdout, stderr):
        a = f.col("a").alias("a")
        b = f.col("b").alias("b")
        # Operators must be compatible with literals, hence the "True & a"
        df.withColumn("c", True & a & b & True).show()
        assert stdout.getvalue() == expected


def test_or(bq: BigQueryBuilder):
    df = bq.sql(
        """
        SELECT
            *
        FROM UNNEST ([
            STRUCT(false as a, false as b),
            STRUCT(true as a, false as b),
            STRUCT(null as a, false as b),
            STRUCT(false as a, true as b),
            STRUCT(true as a, true as b),
            STRUCT(null as a, true as b),
            STRUCT(false as a, null as b),
            STRUCT(true as a, null as b),
            STRUCT(null as a, null as b)
        ])
    """
    )
    expected = strip_margin(
        """
        |+-------+-------+-------+
        ||     a |     b |     c |
        |+-------+-------+-------+
        || False | False | False |
        ||  True | False |  True |
        ||  null | False |  null |
        || False |  True |  True |
        ||  True |  True |  True |
        ||  null |  True |  True |
        || False |  null |  null |
        ||  True |  null |  True |
        ||  null |  null |  null |
        |+-------+-------+-------+
        |"""
    )
    with captured_output() as (stdout, stderr):
        a = f.col("a").alias("a")
        b = f.col("b").alias("b")
        # Operators must be compatible with literals, hence the "False | a"
        df.withColumn("c", False | a | b | False).show()
        assert stdout.getvalue() == expected


def test_invert(bq: BigQueryBuilder):
    df = bq.sql(
        """
        SELECT
            *
        FROM UNNEST ([
            STRUCT(false as a),
            STRUCT(true as a),
            STRUCT(null as a)
        ])
    """
    )
    expected = strip_margin(
        """
        |+-------+-------+
        ||     a |     b |
        |+-------+-------+
        || False |  True |
        ||  True | False |
        ||  null |  null |
        |+-------+-------+
        |"""
    )
    with captured_output() as (stdout, stderr):
        a = f.col("a").alias("a")
        # Operators must be compatible with literals, hence the "False | a"
        df.withColumn("b", ~a).show()
        assert stdout.getvalue() == expected


def test_mod(bq: BigQueryBuilder):
    df = bq.sql(
        """
        SELECT
            *
        FROM UNNEST ([
            STRUCT(1 as a),
            STRUCT(2 as a),
            STRUCT(3 as a)
        ])
    """
    )
    expected = strip_margin(
        """
        |+---+---+
        || a | b |
        |+---+---+
        || 1 | 1 |
        || 2 | 0 |
        || 3 | 1 |
        |+---+---+
        |"""
    )
    with captured_output() as (stdout, stderr):
        a = f.col("a").alias("a")
        df.withColumn("b", a % 2).show()
        assert stdout.getvalue() == expected


def test_add(bq: BigQueryBuilder):
    df = bq.sql(
        """
        SELECT
            *
        FROM UNNEST ([
            STRUCT(2 as a, 2 as b),
            STRUCT(2 as a, null as b),
            STRUCT(null as a, 2 as b)
        ])
    """
    )
    expected = strip_margin(
        """
        |+------+------+------+
        ||    a |    b |    c |
        |+------+------+------+
        ||    2 |    2 |    4 |
        ||    2 | null | null |
        || null |    2 | null |
        |+------+------+------+
        |"""
    )
    with captured_output() as (stdout, stderr):
        a = f.col("a").alias("a")
        b = f.col("b").alias("b")
        # Operators must be compatible with literals, hence the "0 + a"
        df.withColumn("c", 0 + a + b + 0).show()
        assert stdout.getvalue() == expected


def test_sub(bq: BigQueryBuilder):
    df = bq.sql(
        """
        SELECT
            *
        FROM UNNEST ([
            STRUCT(2 as a, 2 as b),
            STRUCT(2 as a, null as b),
            STRUCT(null as a, 2 as b)
        ])
    """
    )
    expected = strip_margin(
        """
        |+------+------+------+
        ||    a |    b |    c |
        |+------+------+------+
        ||    2 |    2 |    0 |
        ||    2 | null | null |
        || null |    2 | null |
        |+------+------+------+
        |"""
    )
    with captured_output() as (stdout, stderr):
        a = f.col("a").alias("a")
        b = f.col("b").alias("b")
        # Operators must be compatible with literals, hence the "0 + a"
        df.withColumn("c", -(1 - (a - b) - 1)).show()
        assert stdout.getvalue() == expected


def test_mul(bq: BigQueryBuilder):
    df = bq.sql(
        """
        SELECT
            *
        FROM UNNEST ([
            STRUCT(2 as a, 2 as b),
            STRUCT(2 as a, null as b),
            STRUCT(null as a, 2 as b)
        ])
    """
    )
    expected = strip_margin(
        """
        |+------+------+------+
        ||    a |    b |    c |
        |+------+------+------+
        ||    2 |    2 |    4 |
        ||    2 | null | null |
        || null |    2 | null |
        |+------+------+------+
        |"""
    )
    with captured_output() as (stdout, stderr):
        a = f.col("a").alias("a")
        b = f.col("b").alias("b")
        # Operators must be compatible with literals, hence the "1 * a"
        df.withColumn("c", 1 * a * b * 1).show()
        assert stdout.getvalue() == expected


def test_div(bq: BigQueryBuilder):
    df = bq.sql(
        """
        SELECT
            *
        FROM UNNEST ([
            STRUCT(2 as a, 2 as b),
            STRUCT(2 as a, null as b),
            STRUCT(null as a, 2 as b)
        ])
    """
    )
    expected = strip_margin(
        """
        |+------+------+------+
        ||    a |    b |    c |
        |+------+------+------+
        ||    2 |    2 |  1.0 |
        ||    2 | null | null |
        || null |    2 | null |
        |+------+------+------+
        |"""
    )
    with captured_output() as (stdout, stderr):
        a = f.col("a").alias("a")
        b = f.col("b").alias("b")
        # Operators must be compatible with literals, hence the "1 * a"
        df.withColumn("c", 1 / (a / b / 1)).show()
        assert stdout.getvalue() == expected


def test_eq(bq: BigQueryBuilder):
    df = bq.sql(
        """
        SELECT
            *
        FROM UNNEST ([
            STRUCT("a" as a, "a" as b),
            STRUCT("a" as a, "b" as b),
            STRUCT("a" as a, null as b),
            STRUCT(null as a, "b" as b)
        ])
    """
    )
    expected = strip_margin(
        """
        |+------+------+-------+-------+-------+
        ||    a |    b |     c |     d |     e |
        |+------+------+-------+-------+-------+
        ||    a |    a |  True |  True |  True |
        ||    a |    b | False | False | False |
        ||    a | null |  null |  null |  null |
        || null |    b |  null | False | False |
        |+------+------+-------+-------+-------+
        |"""
    )
    with captured_output() as (stdout, stderr):
        a = f.col("a").alias("a")
        b = f.col("b").alias("b")
        df.withColumn("c", a == b).withColumn("d", "a" == b).withColumn("e", b == "a").show()
        assert stdout.getvalue() == expected
    with pytest.raises(ValueError):
        a = f.col("a").alias("a")
        print(a == a == a)


def test_neq(bq: BigQueryBuilder):
    df = bq.sql(
        """
        SELECT
            *
        FROM UNNEST ([
            STRUCT("a" as a, "a" as b),
            STRUCT("a" as a, "b" as b),
            STRUCT("a" as a, null as b),
            STRUCT(null as a, "b" as b)
        ])
    """
    )
    expected = strip_margin(
        """
        |+------+------+-------+-------+-------+
        ||    a |    b |     c |     d |     e |
        |+------+------+-------+-------+-------+
        ||    a |    a | False | False | False |
        ||    a |    b |  True |  True |  True |
        ||    a | null |  null |  null |  null |
        || null |    b |  null |  True |  True |
        |+------+------+-------+-------+-------+
        |"""
    )
    with captured_output() as (stdout, stderr):
        a = f.col("a").alias("a")
        b = f.col("b").alias("b")
        df.withColumn("c", a != b).withColumn("d", "a" != b).withColumn("e", b != "a").show()
        assert stdout.getvalue() == expected


def test_lt(bq: BigQueryBuilder):
    df = bq.sql(
        """
        SELECT
            *
        FROM UNNEST ([
            STRUCT(1 as a, 2 as b),
            STRUCT(2 as a, 2 as b),
            STRUCT(3 as a, 2 as b),
            STRUCT(2 as a, null as b),
            STRUCT(null as a, 2 as b)
        ])
    """
    )
    expected = strip_margin(
        """
        |+------+------+-------+-------+-------+
        ||    a |    b |     c |     d |     e |
        |+------+------+-------+-------+-------+
        ||    1 |    2 |  True |  True | False |
        ||    2 |    2 | False | False | False |
        ||    3 |    2 | False | False |  True |
        ||    2 | null |  null | False | False |
        || null |    2 |  null |  null |  null |
        |+------+------+-------+-------+-------+
        |"""
    )
    with captured_output() as (stdout, stderr):
        a = f.col("a").alias("a")
        b = f.col("b").alias("b")
        df.withColumn("c", a < b).withColumn("d", a < 2).withColumn("e", 2 < a).show()
        assert stdout.getvalue() == expected


def test_le(bq: BigQueryBuilder):
    df = bq.sql(
        """
        SELECT
            *
        FROM UNNEST ([
            STRUCT(1 as a, 2 as b),
            STRUCT(2 as a, 2 as b),
            STRUCT(3 as a, 2 as b),
            STRUCT(2 as a, null as b),
            STRUCT(null as a, 2 as b)
        ])
    """
    )
    expected = strip_margin(
        """
        |+------+------+-------+-------+-------+
        ||    a |    b |     c |     d |     e |
        |+------+------+-------+-------+-------+
        ||    1 |    2 |  True |  True | False |
        ||    2 |    2 |  True |  True |  True |
        ||    3 |    2 | False | False |  True |
        ||    2 | null |  null |  True |  True |
        || null |    2 |  null |  null |  null |
        |+------+------+-------+-------+-------+
        |"""
    )
    with captured_output() as (stdout, stderr):
        a = f.col("a").alias("a")
        b = f.col("b").alias("b")
        df.withColumn("c", a <= b).withColumn("d", a <= 2).withColumn("e", 2 <= a).show()
        assert stdout.getvalue() == expected


def test_gt(bq: BigQueryBuilder):
    df = bq.sql(
        """
        SELECT
            *
        FROM UNNEST ([
            STRUCT(1 as a, 2 as b),
            STRUCT(2 as a, 2 as b),
            STRUCT(3 as a, 2 as b),
            STRUCT(2 as a, null as b),
            STRUCT(null as a, 2 as b)
        ])
    """
    )
    expected = strip_margin(
        """
        |+------+------+-------+-------+-------+
        ||    a |    b |     c |     d |     e |
        |+------+------+-------+-------+-------+
        ||    1 |    2 | False | False |  True |
        ||    2 |    2 | False | False | False |
        ||    3 |    2 |  True |  True | False |
        ||    2 | null |  null | False | False |
        || null |    2 |  null |  null |  null |
        |+------+------+-------+-------+-------+
        |"""
    )
    with captured_output() as (stdout, stderr):
        a = f.col("a").alias("a")
        b = f.col("b").alias("b")
        df.withColumn("c", a > b).withColumn("d", a > 2).withColumn("e", 2 > a).show()
        assert stdout.getvalue() == expected


def test_ge(bq: BigQueryBuilder):
    df = bq.sql(
        """
        SELECT
            *
        FROM UNNEST ([
            STRUCT(1 as a, 2 as b),
            STRUCT(2 as a, 2 as b),
            STRUCT(3 as a, 2 as b),
            STRUCT(2 as a, null as b),
            STRUCT(null as a, 2 as b)
        ])
    """
    )
    expected = strip_margin(
        """
        |+------+------+-------+-------+-------+
        ||    a |    b |     c |     d |     e |
        |+------+------+-------+-------+-------+
        ||    1 |    2 | False | False |  True |
        ||    2 |    2 |  True |  True |  True |
        ||    3 |    2 |  True |  True | False |
        ||    2 | null |  null |  True |  True |
        || null |    2 |  null |  null |  null |
        |+------+------+-------+-------+-------+
        |"""
    )
    with captured_output() as (stdout, stderr):
        a = f.col("a").alias("a")
        b = f.col("b").alias("b")
        df.withColumn("c", a >= b).withColumn("d", a >= 2).withColumn("e", 2 >= a).show()
        assert stdout.getvalue() == expected


def test_isin(bq: BigQueryBuilder):
    df = bq.sql(
        """
        SELECT
            *
        FROM UNNEST ([
            STRUCT(1 as id, 1 as a, 2 as b),
            STRUCT(2 as id, 1 as a, 3 as b)
        ])
    """
    )
    actual = df.withColumn("id equal a or b", f.col("id").isin(f.col("a"), f.col("b")))
    expected = strip_margin(
        """
        |+----+---+---+-----------------+
        || id | a | b | id equal a or b |
        |+----+---+---+-----------------+
        ||  1 | 1 | 2 |            True |
        ||  2 | 1 | 3 |           False |
        |+----+---+---+-----------------+"""
    )
    assert actual.show_string() == expected
