import unittest

from bigquery_frame import BigQueryBuilder
from bigquery_frame.auth import get_bq_client
from bigquery_frame.column import Column
from bigquery_frame.dataframe import strip_margin
from tests.utils import captured_output


class TestColumn(unittest.TestCase):

    def setUp(self) -> None:
        self.bigquery = BigQueryBuilder(get_bq_client())

    def tearDown(self) -> None:
        self.bigquery.close()

    def test_and(self):
        df = self.bigquery.sql("""
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
        """)
        expected = strip_margin("""
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
        |""")
        with captured_output() as (stdout, stderr):
            a = Column("a")
            b = Column("b")
            df.withColumn("c", a & b).show()
            self.assertEqual(expected, stdout.getvalue())

    def test_or(self):
        df = self.bigquery.sql("""
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
        """)
        expected = strip_margin("""
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
        |""")
        with captured_output() as (stdout, stderr):
            a = Column("a")
            b = Column("b")
            df.withColumn("c", a | b).show()
            self.assertEqual(expected, stdout.getvalue())

    def test_add(self):
        df = self.bigquery.sql("""
            SELECT 
                *
            FROM UNNEST ([
                STRUCT(2 as a, 2 as b),
                STRUCT(2 as a, null as b),
                STRUCT(null as a, 2 as b)
            ])
        """)
        expected = strip_margin("""
        |+------+------+------+
        ||    a |    b |    c |
        |+------+------+------+
        ||    2 |    2 |    4 |
        ||    2 | null | null |
        || null |    2 | null |
        |+------+------+------+
        |""")
        with captured_output() as (stdout, stderr):
            a = Column("a")
            b = Column("b")
            df.withColumn("c", a + b).show()
            self.assertEqual(expected, stdout.getvalue())

    def test_sub(self):
        df = self.bigquery.sql("""
            SELECT 
                *
            FROM UNNEST ([
                STRUCT(2 as a, 2 as b),
                STRUCT(2 as a, null as b),
                STRUCT(null as a, 2 as b)
            ])
        """)
        expected = strip_margin("""
        |+------+------+------+
        ||    a |    b |    c |
        |+------+------+------+
        ||    2 |    2 |    0 |
        ||    2 | null | null |
        || null |    2 | null |
        |+------+------+------+
        |""")
        with captured_output() as (stdout, stderr):
            a = Column("a")
            b = Column("b")
            df.withColumn("c", a - b).show()
            self.assertEqual(expected, stdout.getvalue())

    def test_mul(self):
        df = self.bigquery.sql("""
            SELECT 
                *
            FROM UNNEST ([
                STRUCT(2 as a, 2 as b),
                STRUCT(2 as a, null as b),
                STRUCT(null as a, 2 as b)
            ])
        """)
        expected = strip_margin("""
        |+------+------+------+
        ||    a |    b |    c |
        |+------+------+------+
        ||    2 |    2 |    4 |
        ||    2 | null | null |
        || null |    2 | null |
        |+------+------+------+
        |""")
        with captured_output() as (stdout, stderr):
            a = Column("a")
            b = Column("b")
            df.withColumn("c", a * b).show()
            self.assertEqual(expected, stdout.getvalue())

    def test_div(self):
        df = self.bigquery.sql("""
            SELECT 
                *
            FROM UNNEST ([
                STRUCT(2 as a, 2 as b),
                STRUCT(2 as a, null as b),
                STRUCT(null as a, 2 as b)
            ])
        """)
        expected = strip_margin("""
        |+------+------+------+
        ||    a |    b |    c |
        |+------+------+------+
        ||    2 |    2 |  1.0 |
        ||    2 | null | null |
        || null |    2 | null |
        |+------+------+------+
        |""")
        with captured_output() as (stdout, stderr):
            a = Column("a")
            b = Column("b")
            df.withColumn("c", a / b).show()
            self.assertEqual(expected, stdout.getvalue())

    def test_eq(self):
        df = self.bigquery.sql("""
            SELECT 
                *
            FROM UNNEST ([
                STRUCT("a" as a, "a" as b),
                STRUCT("a" as a, "b" as b),
                STRUCT("a" as a, null as b),
                STRUCT(null as a, "b" as b)
            ])
        """)
        expected = strip_margin("""
        |+------+------+-------+
        ||    a |    b |     c |
        |+------+------+-------+
        ||    a |    a |  True |
        ||    a |    b | False |
        ||    a | null |  null |
        || null |    b |  null |
        |+------+------+-------+
        |""")
        with captured_output() as (stdout, stderr):
            a = Column("a")
            b = Column("b")
            df.withColumn("c", a == b).show()
            self.assertEqual(expected, stdout.getvalue())

    def test_neq(self):
        df = self.bigquery.sql("""
            SELECT 
                *
            FROM UNNEST ([
                STRUCT("a" as a, "a" as b),
                STRUCT("a" as a, "b" as b),
                STRUCT("a" as a, null as b),
                STRUCT(null as a, "b" as b)
            ])
        """)
        expected = strip_margin("""
        |+------+------+-------+
        ||    a |    b |     c |
        |+------+------+-------+
        ||    a |    a | False |
        ||    a |    b |  True |
        ||    a | null |  null |
        || null |    b |  null |
        |+------+------+-------+
        |""")
        with captured_output() as (stdout, stderr):
            a = Column("a")
            b = Column("b")
            df.withColumn("c", a != b).show()
            self.assertEqual(expected, stdout.getvalue())

    def test_lt(self):
        df = self.bigquery.sql("""
            SELECT 
                *
            FROM UNNEST ([
                STRUCT(1 as a, 2 as b),
                STRUCT(2 as a, 2 as b),
                STRUCT(3 as a, 2 as b),
                STRUCT(2 as a, null as b),
                STRUCT(null as a, 2 as b)
            ])
        """)
        expected = strip_margin("""
        |+------+------+-------+
        ||    a |    b |     c |
        |+------+------+-------+
        ||    1 |    2 |  True |
        ||    2 |    2 | False |
        ||    3 |    2 | False |
        ||    2 | null |  null |
        || null |    2 |  null |
        |+------+------+-------+
        |""")
        with captured_output() as (stdout, stderr):
            a = Column("a")
            b = Column("b")
            df.withColumn("c", a < b).show()
            self.assertEqual(expected, stdout.getvalue())

    def test_le(self):
        df = self.bigquery.sql("""
            SELECT 
                *
            FROM UNNEST ([
                STRUCT(1 as a, 2 as b),
                STRUCT(2 as a, 2 as b),
                STRUCT(3 as a, 2 as b),
                STRUCT(2 as a, null as b),
                STRUCT(null as a, 2 as b)
            ])
        """)
        expected = strip_margin("""
        |+------+------+-------+
        ||    a |    b |     c |
        |+------+------+-------+
        ||    1 |    2 |  True |
        ||    2 |    2 |  True |
        ||    3 |    2 | False |
        ||    2 | null |  null |
        || null |    2 |  null |
        |+------+------+-------+
        |""")
        with captured_output() as (stdout, stderr):
            a = Column("a")
            b = Column("b")
            df.withColumn("c", a <= b).show()
            self.assertEqual(expected, stdout.getvalue())

    def test_gt(self):
        df = self.bigquery.sql("""
            SELECT 
                *
            FROM UNNEST ([
                STRUCT(1 as a, 2 as b),
                STRUCT(2 as a, 2 as b),
                STRUCT(3 as a, 2 as b),
                STRUCT(2 as a, null as b),
                STRUCT(null as a, 2 as b)
            ])
        """)
        expected = strip_margin("""
        |+------+------+-------+
        ||    a |    b |     c |
        |+------+------+-------+
        ||    1 |    2 | False |
        ||    2 |    2 | False |
        ||    3 |    2 |  True |
        ||    2 | null |  null |
        || null |    2 |  null |
        |+------+------+-------+
        |""")
        with captured_output() as (stdout, stderr):
            a = Column("a")
            b = Column("b")
            df.withColumn("c", a > b).show()
            self.assertEqual(expected, stdout.getvalue())

    def test_ge(self):
        df = self.bigquery.sql("""
            SELECT 
                *
            FROM UNNEST ([
                STRUCT(1 as a, 2 as b),
                STRUCT(2 as a, 2 as b),
                STRUCT(3 as a, 2 as b),
                STRUCT(2 as a, null as b),
                STRUCT(null as a, 2 as b)
            ])
        """)
        expected = strip_margin("""
        |+------+------+-------+
        ||    a |    b |     c |
        |+------+------+-------+
        ||    1 |    2 | False |
        ||    2 |    2 |  True |
        ||    3 |    2 |  True |
        ||    2 | null |  null |
        || null |    2 |  null |
        |+------+------+-------+
        |""")
        with captured_output() as (stdout, stderr):
            a = Column("a")
            b = Column("b")
            df.withColumn("c", a >= b).show()
            self.assertEqual(expected, stdout.getvalue())

