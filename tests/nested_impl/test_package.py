from typing import Callable, Dict

import pytest

import bigquery_frame
import bigquery_frame.exceptions
from bigquery_frame import BigQueryBuilder
from bigquery_frame import functions as f
from bigquery_frame import nested
from bigquery_frame.column import Column
from bigquery_frame.fp.printable_function import PrintableFunction
from bigquery_frame.nested_impl.package import (
    _build_nested_struct_tree,
    _build_transformation_from_tree,
    resolve_nested_fields,
)
from bigquery_frame.utils import strip_margin


def replace_named_functions_with_functions(
    transformations: Dict[str, PrintableFunction],
) -> Dict[str, Callable[[Column], Column]]:
    return {alias: transformation.func for alias, transformation in transformations.items()}


class TestBuildTransformationFromTree:
    """The purpose of this test class is mostly to make sure that PrintableFunctions are properly printed
    The code's logic is mostly tested in TestResolveNestedColumns.
    """

    def test_value_with_string_expr_and_string_alias(self):
        """
        GIVEN a transformation that returns a string expression and has an alias of type str
        WHEN we print the PrintableFunction generated in resolve_nested_columns
        THEN the result should be human-readable
        """
        named_transformations = {"a": PrintableFunction(lambda s: "a", "a")}
        transformations = {"a": "a"}
        actual_named = _build_transformation_from_tree(
            _build_nested_struct_tree(named_transformations),
        )
        actual = _build_transformation_from_tree(
            _build_nested_struct_tree(transformations),
        )
        assert str(actual_named) == """lambda x: [a.alias('a')]"""
        assert str(actual) == """lambda x: [f.col('a').alias('a')]"""

    def test_value_with_col_expr(self):
        """
        GIVEN a transformation that returns a column
        WHEN we print the PrintableFunction generated in resolve_nested_columns
        THEN the result should be human-readable
        """
        named_transformations = {
            "a": PrintableFunction(lambda s: f.col("a"), lambda s: 'f.col("a")'),
        }
        transformations = {"a": f.col("a")}
        actual_named = _build_transformation_from_tree(
            _build_nested_struct_tree(named_transformations),
        )
        actual = _build_transformation_from_tree(
            _build_nested_struct_tree(transformations),
        )
        assert str(actual_named) == """lambda x: [f.col("a").alias('a')]"""
        assert str(actual) == """lambda x: [Column<'`a`'>.alias('a')]"""

    def test_value_with_aliased_col_expr(self):
        """
        GIVEN a transformation that returns an aliased column
        WHEN we print the PrintableFunction generated in resolve_nested_columns
        THEN the result should be human-readable
        """
        named_transformations = {
            "a": PrintableFunction(
                lambda s: f.col("a").alias("other_alias"),
                lambda s: 'f.col("a").alias("other_alias")',
            ),
        }
        transformations = {"a": f.col("a").alias("other_alias")}
        actual_named = _build_transformation_from_tree(
            _build_nested_struct_tree(named_transformations),
        )
        actual = _build_transformation_from_tree(
            _build_nested_struct_tree(transformations),
        )
        assert str(actual_named) == """lambda x: [f.col("a").alias("other_alias").alias('a')]"""
        assert str(actual) == """lambda x: [Column<'`a` AS `other_alias`'>.alias('a')]"""

    def test_struct(self):
        """
        GIVEN a transformation on a struct
        WHEN we print the PrintableFunction generated in resolve_nested_columns
        THEN the result should be human-readable
        """
        named_transformations = {
            "s.a": PrintableFunction(
                lambda s: f.col("s.a").cast("FLOAT64"),
                lambda s: """f.col("s.a").cast("FLOAT64")""",
            ),
        }
        actual_named = _build_transformation_from_tree(
            _build_nested_struct_tree(named_transformations),
        )
        assert str(actual_named) == """lambda x: [f.struct([f.col("s.a").cast("FLOAT64").alias('a')]).alias('s')]"""

    def test_struct_with_static_expression(self):
        """
        GIVEN a transformation on a struct that uses the full name of one of the struct's field (s.a)
        WHEN we print the PrintableFunction generated in resolve_nested_columns
        THEN the result should be human-readable
        """
        named_transformations = {
            "s.a": PrintableFunction(
                lambda s: f.col("s.a").cast("FLOAT64"),
                lambda s: """f.col("s.a").cast("FLOAT64")""",
            ),
        }
        actual_named = _build_transformation_from_tree(
            _build_nested_struct_tree(named_transformations),
        )
        assert str(actual_named) == """lambda x: [f.struct([f.col("s.a").cast("FLOAT64").alias('a')]).alias('s')]"""

    def test_array(self):
        """
        GIVEN a transformation on an array
        WHEN we print the PrintableFunction generated in resolve_nested_columns
        THEN the result should be human-readable
        """
        named_transformations = {
            "e!": PrintableFunction(
                lambda e: e.cast("FLOAT64"),
                lambda e: f'{e}.cast("FLOAT64")',
            ),
        }
        actual_named = _build_transformation_from_tree(
            _build_nested_struct_tree(named_transformations),
        )
        assert str(actual_named) == """lambda x: [f.transform(x['e'], lambda x: x.cast("FLOAT64")).alias('e')]"""

    def test_array_struct(self):
        """
        GIVEN a transformation on an array<struct>
        WHEN we print the PrintableFunction generated in resolve_nested_columns
        THEN the result should be human-readable
        """
        named_transformations = {
            "s!.a": PrintableFunction(
                lambda s: s["a"].cast("FLOAT64"),
                lambda s: f'{s}["a"].cast("FLOAT64")',
            ),
        }
        actual_named = _build_transformation_from_tree(
            _build_nested_struct_tree(named_transformations),
        )
        assert str(actual_named) == (
            """lambda x: [f.transform(x['s'], lambda x: f.struct([x["a"].cast("FLOAT64").alias('a')])).alias('s')]"""
        )

    def test_struct_in_struct(self):
        """
        GIVEN a transformation on a struct inside a struct
        WHEN we print the PrintableFunction generated in resolve_nested_columns
        THEN the result should be human-readable
        """
        named_transformations = {
            "s1.s2.a": PrintableFunction(
                lambda s: f.col("s1")["s2"]["a"].cast("FLOAT64"),
                lambda s: 'f.col("s1")["s2"]["a"].cast("FLOAT64")',
            ),
        }
        actual_named = _build_transformation_from_tree(
            _build_nested_struct_tree(named_transformations),
        )
        assert str(actual_named) == (
            """lambda x: [f.struct([f.struct([f.col("s1")["s2"]["a"]"""
            """.cast("FLOAT64").alias('a')]).alias('s2')]).alias('s1')]"""
        )

    def test_array_in_struct(self):
        """
        GIVEN a transformation on an array inside a struct
        WHEN we print the PrintableFunction generated in resolve_nested_columns
        THEN the result should be human-readable
        """
        named_transformations = {
            "s1.e!": PrintableFunction(
                lambda e: e.cast("FLOAT64"),
                lambda e: f'{e}.cast("FLOAT64")',
            ),
        }
        actual_named = _build_transformation_from_tree(
            _build_nested_struct_tree(named_transformations),
        )
        assert str(actual_named) == (
            """lambda x: [f.struct([f.transform(x['s1']['e'], lambda x: x.cast("FLOAT64")).alias('e')]).alias('s1')]"""
        )

    def test_array_struct_in_struct(self):
        """
        GIVEN a transformation on an array<struct> inside a struct
        WHEN we print the PrintableFunction generated in resolve_nested_columns
        THEN the result should be human-readable
        """
        named_transformations = {
            "s1.s2!.a": PrintableFunction(
                lambda s: s["a"].cast("FLOAT64"),
                lambda s: f'{s}["a"].cast("FLOAT64")',
            ),
        }
        actual_named = _build_transformation_from_tree(
            _build_nested_struct_tree(named_transformations),
        )
        assert str(actual_named) == (
            "lambda x: [f.struct([f.transform(x['s1']['s2'], lambda x: "
            """f.struct([x["a"].cast("FLOAT64").alias('a')])).alias('s2')]).alias('s1')]"""
        )

    def test_array_in_array(self):
        """
        GIVEN a transformation on an array inside an array
        WHEN we print the PrintableFunction generated in resolve_nested_columns
        THEN the result should be human-readable
        """
        named_transformations = {
            "e!!": PrintableFunction(
                lambda e: e.cast("FLOAT64"),
                lambda e: f'{e}.cast("FLOAT64")',
            ),
        }
        actual_named = _build_transformation_from_tree(
            _build_nested_struct_tree(named_transformations),
        )
        assert str(actual_named) == (
            """lambda x: [f.transform(x['e'], lambda x: f.transform(x, lambda x: x.cast("FLOAT64"))).alias('e')]"""
        )

    def test_array_struct_in_array(self):
        """
        GIVEN a transformation on array<struct> inside an array
        WHEN we print the PrintableFunction generated in resolve_nested_columns
        THEN the result should be human-readable
        """
        named_transformations = {
            "s!!.a": PrintableFunction(
                lambda s: s["a"].cast("FLOAT64"),
                lambda s: f'{s}["a"].cast("FLOAT64")',
            ),
        }
        actual_named = _build_transformation_from_tree(
            _build_nested_struct_tree(named_transformations),
        )
        assert str(actual_named) == (
            "lambda x: [f.transform(x['s'], lambda x: f.transform(x, lambda x: "
            """f.struct([x["a"].cast("FLOAT64").alias('a')]))).alias('s')]"""
        )

    def test_struct_in_array_struct(self):
        """
        GIVEN a transformation on struct inside an array<struct>
        WHEN we print the PrintableFunction generated in resolve_nested_columns
        THEN the result should be human-readable
        """
        named_transformations = {
            "s1!.s2.a": PrintableFunction(
                lambda s: s["a"].cast("FLOAT64"),
                lambda s: f'{s}["a"].cast("FLOAT64")',
            ),
        }
        actual_named = _build_transformation_from_tree(
            _build_nested_struct_tree(named_transformations),
        )
        assert str(actual_named) == (
            "lambda x: [f.transform(x['s1'], lambda x: "
            """f.struct([f.struct([x["a"].cast("FLOAT64").alias('a')]).alias('s2')])).alias('s1')]"""
        )

    def test_array_in_array_struct(self):
        """
        GIVEN a transformation on an array inside an array<struct>
        WHEN we print the PrintableFunction generated in resolve_nested_columns
        THEN the result should be human-readable
        """
        named_transformations = {
            "s1!.e!": PrintableFunction(
                lambda e: e.cast("FLOAT64"),
                lambda e: f'{e}.cast("FLOAT64")',
            ),
        }
        actual_named = _build_transformation_from_tree(
            _build_nested_struct_tree(named_transformations),
        )
        assert str(actual_named) == (
            "lambda x: [f.transform(x['s1'], lambda x: f.struct([f.transform(x['e'], lambda "
            """x: x.cast("FLOAT64")).alias('e')])).alias('s1')]"""
        )

    def test_array_struct_in_array_struct(self):
        """
        GIVEN a transformation on an array<struct> inside an array<struct>
        WHEN we print the PrintableFunction generated in resolve_nested_columns
        THEN the result should be human-readable
        """
        named_transformations = {
            "s1!.s2!.a": PrintableFunction(
                lambda s: s["a"].cast("FLOAT64"),
                lambda s: f'{s}["a"].cast("FLOAT64")',
            ),
        }
        actual_named = _build_transformation_from_tree(
            _build_nested_struct_tree(named_transformations),
        )
        assert str(actual_named) == (
            "lambda x: [f.transform(x['s1'], lambda x: f.struct([f.transform(x['s2'], lambda "
            """x: f.struct([x["a"].cast("FLOAT64").alias('a')])).alias('s2')])).alias('s1')]"""
        )

    def test_array_struct_in_array_struct_with_transformation_using_field_from_first_array(
        self,
    ):
        """
        GIVEN a DataFrame with an array<struct> inside another array<struct>
        WHEN we use resolve_nested_columns on it with a transformation that uses data from the first array
        THEN the transformation should work
        """
        named_transformations = {
            "s1!.a": PrintableFunction(lambda s1: s1["a"], lambda s1: f'{s1}["a"]'),
            "s1!.s2!.b": PrintableFunction(
                lambda s1, s2: (s1["a"] + s2["b"]).cast("FLOAT64"),
                lambda s1, s2: f'({s1}["a"]+{s2}["b"]).cast("FLOAT64")',
            ),
        }
        actual_named = _build_transformation_from_tree(
            _build_nested_struct_tree(named_transformations),
        )
        assert str(actual_named) == (
            """lambda x: [f.transform(x['s1'], lambda x: f.struct([x["a"].alias('a'), f.transform(x['s2'], """
            """lambda x: f.struct([(x["a"]+x["b"]).cast("FLOAT64").alias('b')])).alias('s2')])).alias('s1')]"""
        )

    def test_struct_in_array_struct_in_array_struct(self):
        """
        GIVEN a DataFrame with a struct inside an array<struct> inside an array<struct>
        WHEN we use resolve_nested_columns with a transformation that access an element from the outermost struct
        THEN the transformation should work
        """
        # Here, the lambda function is applied to the elements of `s2`, not `s3`
        named_transformations = {
            "s1!.s2!.a": PrintableFunction(lambda s2: s2["a"], lambda s2: f'{s2}["a"]'),
            "s1!.s2!.s3.b": PrintableFunction(
                lambda s2: s2["a"].cast("FLOAT64"),
                lambda s2: f'{s2}["a"].cast("FLOAT64")',
            ),
        }
        actual_named = _build_transformation_from_tree(
            _build_nested_struct_tree(named_transformations),
        )
        assert str(actual_named) == (
            "lambda x: [f.transform(x['s1'], lambda x: f.struct([f.transform(x['s2'], "
            """lambda x: f.struct([x["a"].alias('a'), """
            """f.struct([x["a"].cast("FLOAT64").alias('b')]).alias('s3')])).alias('s2')])).alias('s1')]"""
        )

    def test_struct_in_struct_in_array_struct_in_struct_in_array_struct(self):
        """
        GIVEN a DataFrame with a struct inside a struct inside an array<struct> inside a struct inside an array<struct>
        WHEN we use resolve_nested_columns with a transformation that access an element from the outermost struct
        THEN the transformation should work
        """
        # Here, the lambda function is applied to the elements of `s3`, not `s4` or `s5`
        named_transformations = {
            "s1!.s2.s3!.s4.a": PrintableFunction(
                lambda s3: s3["s4"]["a"],
                lambda s3: f'{s3}["s4"]["a"]',
            ),
            "s1!.s2.s3!.s4.s5.b": PrintableFunction(
                lambda s3: s3["s4"]["a"].cast("FLOAT64"),
                lambda s3: f'{s3}["s4"]["a"].cast("FLOAT64")',
            ),
        }
        actual_named = _build_transformation_from_tree(
            _build_nested_struct_tree(named_transformations),
        )
        assert str(actual_named) == (
            "lambda x: [f.transform(x['s1'], lambda x: "
            "f.struct([f.struct([f.transform(x['s2']['s3'], lambda x: "
            """f.struct([f.struct([x["s4"]["a"].alias('a'), """
            """f.struct([x["s4"]["a"].cast("FLOAT64").alias('b')]).alias('s5')])"""
            """.alias('s4')])).alias('s3')]).alias('s2')])).alias('s1')]"""
        )


class TestResolveNestedFields:
    def test_with_error(self):
        """
        GIVEN a DataFrame with a simple value
        WHEN we use resolve_nested_columns on it with an incorrect expression
        THEN an AnalysisException should be raised
        """
        transformation = {"a!b": None}
        with pytest.raises(bigquery_frame.exceptions.AnalysisException) as e:
            resolve_nested_fields(transformation)
        assert "Invalid field name 'a!b'" in str(e.value)

    def test_value_with_string_expr(self, bq: BigQueryBuilder):
        """
        GIVEN a DataFrame with a simple value
        WHEN we use resolve_nested_columns on it with a string expression
        THEN the transformation should work
        """
        df = bq.sql("SELECT 1 as a")
        assert df.show_string() == strip_margin(
            """
            |+---+
            || a |
            |+---+
            || 1 |
            |+---+""",
        )
        named_transformations = {"a": PrintableFunction(lambda s: "a", lambda s: '"a"')}
        transformations = {"a": "a"}
        expected = strip_margin(
            """
            |+---+
            || a |
            |+---+
            || 1 |
            |+---+""",
        )
        assert df.select(*resolve_nested_fields(named_transformations)).show_string() == expected
        assert df.select(*resolve_nested_fields(transformations)).show_string() == expected

    def test_value_with_col_expr(self, bq: BigQueryBuilder):
        """
        GIVEN a DataFrame with a simple value
        WHEN we use resolve_nested_columns on it with a column expression
        THEN the transformation should work
        """
        df = bq.sql("SELECT 1 as a")
        assert df.show_string() == strip_margin(
            """
            |+---+
            || a |
            |+---+
            || 1 |
            |+---+""",
        )
        named_transformations = {
            "a": PrintableFunction(lambda s: f.col("a"), lambda s: 'f.col("a")'),
        }
        transformations = {"a": f.col("a")}
        expected = strip_margin(
            """
            |+---+
            || a |
            |+---+
            || 1 |
            |+---+""",
        )
        assert df.select(*resolve_nested_fields(named_transformations)).show_string() == expected
        assert df.select(*resolve_nested_fields(transformations)).show_string() == expected

    def test_value_with_aliased_col_expr(self, bq: BigQueryBuilder):
        """
        GIVEN a DataFrame with a simple value
        WHEN we use resolve_nested_columns on it with a column expression using a different alias
        THEN the transformation should work and the alias should be ignored
        """
        df = bq.sql("SELECT 1 as a")
        assert df.show_string() == strip_margin(
            """
            |+---+
            || a |
            |+---+
            || 1 |
            |+---+""",
        )
        named_transformations = {
            "a": PrintableFunction(
                lambda s: f.col("a").alias("other_alias"),
                lambda s: 'f.col("a").alias("other_alias")',
            ),
        }
        transformations = {"a": f.col("a").alias("other_alias")}
        expected = strip_margin(
            """
            |+---+
            || a |
            |+---+
            || 1 |
            |+---+""",
        )
        assert df.select(*resolve_nested_fields(named_transformations)).show_string() == expected
        assert df.select(*resolve_nested_fields(transformations)).show_string() == expected

    def test_value_with_get_expr(self, bq: BigQueryBuilder):
        """
        GIVEN a DataFrame with a simple value
        WHEN we use resolve_nested_columns on it with a lambda function that accesses the root level
        THEN the transformation should work
        """
        df = bq.sql("SELECT 1 as a")
        assert df.show_string() == strip_margin(
            """
            |+---+
            || a |
            |+---+
            || 1 |
            |+---+""",
        )
        named_transformations = {
            "a": PrintableFunction(lambda r: r["a"], lambda r: f'{r}["a"]'),
        }
        transformations = replace_named_functions_with_functions(named_transformations)
        expected = strip_margin(
            """
            |+---+
            || a |
            |+---+
            || 1 |
            |+---+""",
        )
        assert df.select(*resolve_nested_fields(named_transformations, starting_level=df)).show_string() == expected
        assert df.select(*resolve_nested_fields(transformations, starting_level=df)).show_string() == expected

    def test_struct(self, bq: BigQueryBuilder):
        """
        GIVEN a DataFrame with a struct
        WHEN we use resolve_nested_columns on it
        THEN the transformation should work
        """
        df = bq.sql("SELECT STRUCT(2 as a) as s")
        assert df.show_string(simplify_structs=True) == strip_margin(
            """
            |+-----+
            ||   s |
            |+-----+
            || {2} |
            |+-----+""",
        )
        named_transformations = {
            "s.a": PrintableFunction(
                lambda s: f.col("s.a").cast("FLOAT64"),
                lambda s: """f.col("s.a").cast("FLOAT64")""",
            ),
        }
        transformations = replace_named_functions_with_functions(named_transformations)
        expected = strip_margin(
            """
            |+-------+
            ||     s |
            |+-------+
            || {2.0} |
            |+-------+""",
        )
        assert df.select(*resolve_nested_fields(named_transformations)).show_string(simplify_structs=True) == expected
        assert df.select(*resolve_nested_fields(transformations)).show_string(simplify_structs=True) == expected

    def test_struct_with_static_expression(self, bq: BigQueryBuilder):
        """
        GIVEN a DataFrame with a struct
        WHEN we use resolve_nested_columns on it without lambda expression
        THEN the transformation should work
        """
        df = bq.sql("SELECT STRUCT(2 as a) as s")
        assert df.show_string(simplify_structs=True) == strip_margin(
            """
            |+-----+
            ||   s |
            |+-----+
            || {2} |
            |+-----+""",
        )
        named_transformations = {
            "s.a": PrintableFunction(
                lambda s: f.col("s.a").cast("FLOAT64"),
                lambda s: """f.col("s.a").cast("FLOAT64")""",
            ),
        }
        transformations = {"s.a": f.col("s.a").cast("FLOAT64")}
        expected = strip_margin(
            """
            |+-------+
            ||     s |
            |+-------+
            || {2.0} |
            |+-------+""",
        )
        assert df.select(*resolve_nested_fields(named_transformations)).show_string(simplify_structs=True) == expected
        assert df.select(*resolve_nested_fields(transformations)).show_string(simplify_structs=True) == expected

    def test_array(self, bq: BigQueryBuilder):
        """
        GIVEN a DataFrame with an array
        WHEN we use resolve_nested_columns on it
        THEN the transformation should work
        """
        df = bq.sql("SELECT [2, 3] as e")
        assert df.treeString() == strip_margin(
            """
            |root
            | |-- e: INTEGER (REPEATED)
            |""",
        )
        assert df.show_string() == strip_margin(
            """
            |+--------+
            ||      e |
            |+--------+
            || [2, 3] |
            |+--------+""",
        )
        named_transformations = {
            "e!": PrintableFunction(
                lambda e: e.cast("FLOAT64"),
                lambda e: f'{e}.cast("FLOAT64")',
            ),
        }
        transformations = replace_named_functions_with_functions(named_transformations)
        expected_schema = strip_margin(
            """
            |root
            | |-- e: FLOAT (REPEATED)
            |""",
        )
        expected = strip_margin(
            """
            |+------------+
            ||          e |
            |+------------+
            || [2.0, 3.0] |
            |+------------+""",
        )
        actual_named = df.select(*resolve_nested_fields(named_transformations))
        actual = df.select(*resolve_nested_fields(transformations))
        assert actual_named.treeString() == expected_schema
        assert actual_named.show_string() == expected
        assert actual.treeString() == expected_schema
        assert actual.show_string() == expected

    def test_array_with_none(self, bq: BigQueryBuilder):
        """
        GIVEN a DataFrame with an array
        WHEN we use resolve_nested_columns on it with a transformation being None
        THEN the transformation should work
        """
        df = bq.sql("SELECT [2, 3] as e")
        assert df.treeString() == strip_margin(
            """
            |root
            | |-- e: INTEGER (REPEATED)
            |""",
        )
        assert df.show_string() == strip_margin(
            """
            |+--------+
            ||      e |
            |+--------+
            || [2, 3] |
            |+--------+""",
        )
        named_transformations = {"e!": None}
        expected_schema = strip_margin(
            """
            |root
            | |-- e: INTEGER (REPEATED)
            |""",
        )
        expected = strip_margin(
            """
            |+--------+
            ||      e |
            |+--------+
            || [2, 3] |
            |+--------+""",
        )
        actual_named = df.select(*resolve_nested_fields(named_transformations))
        assert actual_named.treeString() == expected_schema
        assert actual_named.show_string() == expected

    def test_array_with_str_expr(self, bq: BigQueryBuilder):
        """
        GIVEN a DataFrame with an array
        WHEN we use resolve_nested_columns on it with a transformation being a string expression
        THEN the transformation should work
        """
        df = bq.sql("SELECT 1 as id, [2, 3] as e")
        assert df.treeString() == strip_margin(
            """
            |root
            | |-- id: INTEGER (NULLABLE)
            | |-- e: INTEGER (REPEATED)
            |""",
        )
        assert df.show_string() == strip_margin(
            """
            |+----+--------+
            || id |      e |
            |+----+--------+
            ||  1 | [2, 3] |
            |+----+--------+""",
        )
        named_transformations = {"id": None, "e!": "id"}
        expected_schema = strip_margin(
            """
            |root
            | |-- id: INTEGER (NULLABLE)
            | |-- e: INTEGER (REPEATED)
            |""",
        )
        expected = strip_margin(
            """
            |+----+--------+
            || id |      e |
            |+----+--------+
            ||  1 | [1, 1] |
            |+----+--------+""",
        )
        actual_named = df.select(*resolve_nested_fields(named_transformations))
        assert actual_named.treeString() == expected_schema
        assert actual_named.show_string() == expected

    def test_array_with_col_expr(self, bq: BigQueryBuilder):
        """
        GIVEN a DataFrame with an array
        WHEN we use resolve_nested_columns on it with a transformation being a Column expression
        THEN the transformation should work
        """
        df = bq.sql("SELECT [2, 3] as e")
        assert df.treeString() == strip_margin(
            """
            |root
            | |-- e: INTEGER (REPEATED)
            |""",
        )
        assert df.show_string() == strip_margin(
            """
            |+--------+
            ||      e |
            |+--------+
            || [2, 3] |
            |+--------+""",
        )
        named_transformations = {"e!": f.lit(1)}
        expected_schema = strip_margin(
            """
            |root
            | |-- e: INTEGER (REPEATED)
            |""",
        )
        expected = strip_margin(
            """
            |+--------+
            ||      e |
            |+--------+
            || [1, 1] |
            |+--------+""",
        )
        actual_named = df.select(*resolve_nested_fields(named_transformations))
        assert actual_named.treeString() == expected_schema
        assert actual_named.show_string() == expected

    def test_array_struct(self, bq: BigQueryBuilder):
        """
        GIVEN a DataFrame with an array<struct>
        WHEN we use resolve_nested_columns on it
        THEN the transformation should work
        """
        df = bq.sql("SELECT [STRUCT(2 as a)] as s")
        assert df.treeString() == strip_margin(
            """
            |root
            | |-- s: RECORD (REPEATED)
            | |    |-- a: INTEGER (NULLABLE)
            |""",
        )
        assert df.show_string(simplify_structs=True) == strip_margin(
            """
            |+-------+
            ||     s |
            |+-------+
            || [{2}] |
            |+-------+""",
        )
        named_transformations = {
            "s!.a": PrintableFunction(
                lambda s: s["a"].cast("FLOAT64"),
                lambda s: f'{s}["a"].cast("FLOAT64")',
            ),
        }
        transformations = replace_named_functions_with_functions(named_transformations)
        expected_schema = strip_margin(
            """
            |root
            | |-- s: RECORD (REPEATED)
            | |    |-- a: FLOAT (NULLABLE)
            |""",
        )
        expected = strip_margin(
            """
            |+---------+
            ||       s |
            |+---------+
            || [{2.0}] |
            |+---------+""",
        )
        actual_named = df.select(*resolve_nested_fields(named_transformations))
        actual = df.select(*resolve_nested_fields(transformations))
        assert actual_named.treeString() == expected_schema
        assert actual_named.show_string(simplify_structs=True) == expected
        assert actual.treeString() == expected_schema
        assert actual.show_string(simplify_structs=True) == expected

    def test_struct_in_struct(self, bq: BigQueryBuilder):
        """
        GIVEN a DataFrame with a struct inside a struct
        WHEN we use resolve_nested_columns on it
        THEN the transformation should work
        """
        df = bq.sql("SELECT STRUCT(STRUCT(2 as a) as s2) as s1")
        assert df.treeString() == strip_margin(
            """
            |root
            | |-- s1: RECORD (NULLABLE)
            | |    |-- s2: RECORD (NULLABLE)
            | |    |    |-- a: INTEGER (NULLABLE)
            |""",
        )
        assert df.show_string(simplify_structs=True) == strip_margin(
            """
            |+-------+
            ||    s1 |
            |+-------+
            || {{2}} |
            |+-------+""",
        )
        named_transformations = {
            "s1.s2.a": PrintableFunction(
                lambda s: f.col("s1")["s2"]["a"].cast("FLOAT64"),
                lambda s: 'f.col("s1")["s2"]["a"].cast("FLOAT64")',
            ),
        }
        transformations = replace_named_functions_with_functions(named_transformations)
        expected_schema = strip_margin(
            """
            |root
            | |-- s1: RECORD (NULLABLE)
            | |    |-- s2: RECORD (NULLABLE)
            | |    |    |-- a: FLOAT (NULLABLE)
            |""",
        )
        expected = strip_margin(
            """
            |+---------+
            ||      s1 |
            |+---------+
            || {{2.0}} |
            |+---------+""",
        )
        actual_named = df.select(*resolve_nested_fields(named_transformations))
        actual = df.select(*resolve_nested_fields(transformations))
        assert actual_named.treeString() == expected_schema
        assert actual_named.show_string(simplify_structs=True) == expected
        assert actual.treeString() == expected_schema
        assert actual.show_string(simplify_structs=True) == expected

    def test_array_in_struct(self, bq: BigQueryBuilder):
        """
        GIVEN a DataFrame with an array inside a struct
        WHEN we use resolve_nested_columns on it
        THEN the transformation should work
        """
        df = bq.sql("SELECT STRUCT([2, 3] as e) as s1")
        assert df.treeString() == strip_margin(
            """
            |root
            | |-- s1: RECORD (NULLABLE)
            | |    |-- e: INTEGER (REPEATED)
            |""",
        )
        assert df.show_string(simplify_structs=True) == strip_margin(
            """
            |+----------+
            ||       s1 |
            |+----------+
            || {[2, 3]} |
            |+----------+""",
        )
        named_transformations = {
            "s1.e!": PrintableFunction(
                lambda e: e.cast("FLOAT64"),
                lambda e: f'{e}.cast("FLOAT64")',
            ),
        }
        transformations = replace_named_functions_with_functions(named_transformations)
        expected_schema = strip_margin(
            """
            |root
            | |-- s1: RECORD (NULLABLE)
            | |    |-- e: FLOAT (REPEATED)
            |""",
        )
        expected = strip_margin(
            """
            |+--------------+
            ||           s1 |
            |+--------------+
            || {[2.0, 3.0]} |
            |+--------------+""",
        )
        actual_named = df.select(*resolve_nested_fields(named_transformations))
        actual = df.select(*resolve_nested_fields(transformations))
        assert actual_named.treeString() == expected_schema
        assert actual_named.show_string(simplify_structs=True) == expected
        assert actual.treeString() == expected_schema
        assert actual.show_string(simplify_structs=True) == expected

    def test_array_struct_in_struct(self, bq: BigQueryBuilder):
        """
        GIVEN a DataFrame with an array<struct> inside a struct
        WHEN we use resolve_nested_columns on it
        THEN the transformation should work
        """
        df = bq.sql("SELECT STRUCT([STRUCT(2 as a)] as s2) as s1")
        assert df.treeString() == strip_margin(
            """
            |root
            | |-- s1: RECORD (NULLABLE)
            | |    |-- s2: RECORD (REPEATED)
            | |    |    |-- a: INTEGER (NULLABLE)
            |""",
        )
        assert df.show_string(simplify_structs=True) == strip_margin(
            """
            |+---------+
            ||      s1 |
            |+---------+
            || {[{2}]} |
            |+---------+""",
        )
        named_transformations = {
            "s1.s2!.a": PrintableFunction(
                lambda s: s["a"].cast("FLOAT64"),
                lambda s: f'{s}["a"].cast("FLOAT64")',
            ),
        }
        transformations = replace_named_functions_with_functions(named_transformations)
        expected_schema = strip_margin(
            """
            |root
            | |-- s1: RECORD (NULLABLE)
            | |    |-- s2: RECORD (REPEATED)
            | |    |    |-- a: FLOAT (NULLABLE)
            |""",
        )
        expected = strip_margin(
            """
            |+-----------+
            ||        s1 |
            |+-----------+
            || {[{2.0}]} |
            |+-----------+""",
        )
        actual_named = df.select(*resolve_nested_fields(named_transformations))
        actual = df.select(*resolve_nested_fields(transformations))
        assert actual_named.treeString() == expected_schema
        assert actual_named.show_string(simplify_structs=True) == expected
        assert actual.treeString() == expected_schema
        assert actual.show_string(simplify_structs=True) == expected

    def test_struct_in_array_struct(self, bq: BigQueryBuilder):
        """
        GIVEN a DataFrame with a struct inside a struct inside an array
        WHEN we use resolve_nested_columns with a transformation that access an element from the outermost struct
        THEN the transformation should work
        """
        df = bq.sql("SELECT [STRUCT(1 as a, STRUCT(2 as b) as s2)] as s1")
        assert df.treeString() == strip_margin(
            """
            |root
            | |-- s1: RECORD (REPEATED)
            | |    |-- a: INTEGER (NULLABLE)
            | |    |-- s2: RECORD (NULLABLE)
            | |    |    |-- b: INTEGER (NULLABLE)
            |""",
        )
        assert df.show_string(simplify_structs=True) == strip_margin(
            """
            |+------------+
            ||         s1 |
            |+------------+
            || [{1, {2}}] |
            |+------------+""",
        )
        # Here, the lambda function is applied to the elements of `s1`, not `s2`
        named_transformations = {
            "s1!.a": PrintableFunction(lambda s1: s1["a"], lambda s1: f'{s1}["a"]'),
            "s1!.s2.b": PrintableFunction(
                lambda s1: s1["a"].cast("FLOAT64"),
                lambda s1: f'{s1}["a"].cast("FLOAT64")',
            ),
        }
        transformations = replace_named_functions_with_functions(named_transformations)
        expected_schema = strip_margin(
            """
            |root
            | |-- s1: RECORD (REPEATED)
            | |    |-- a: INTEGER (NULLABLE)
            | |    |-- s2: RECORD (NULLABLE)
            | |    |    |-- b: FLOAT (NULLABLE)
            |""",
        )
        expected = strip_margin(
            """
            |+--------------+
            ||           s1 |
            |+--------------+
            || [{1, {1.0}}] |
            |+--------------+""",
        )
        actual_named = df.select(*resolve_nested_fields(named_transformations))
        actual = df.select(*resolve_nested_fields(transformations))
        assert actual_named.treeString() == expected_schema
        assert actual_named.show_string(simplify_structs=True) == expected
        assert actual.treeString() == expected_schema
        assert actual.show_string(simplify_structs=True) == expected

    def test_array_in_array_struct(self, bq: BigQueryBuilder):
        """
        GIVEN a DataFrame with an array inside an array<struct>
        WHEN we use resolve_nested_columns on it
        THEN the transformation should work
        """
        df = bq.sql("SELECT [STRUCT([2, 3] as e)] as s1")
        assert df.treeString() == strip_margin(
            """
            |root
            | |-- s1: RECORD (REPEATED)
            | |    |-- e: INTEGER (REPEATED)
            |""",
        )
        assert df.show_string(simplify_structs=True) == strip_margin(
            """
            |+------------+
            ||         s1 |
            |+------------+
            || [{[2, 3]}] |
            |+------------+""",
        )
        named_transformations = {
            "s1!.e!": PrintableFunction(
                lambda e: e.cast("FLOAT64"),
                lambda e: f'{e}.cast("FLOAT64")',
            ),
        }
        transformations = replace_named_functions_with_functions(named_transformations)
        expected_schema = strip_margin(
            """
            |root
            | |-- s1: RECORD (REPEATED)
            | |    |-- e: FLOAT (REPEATED)
            |""",
        )
        expected = strip_margin(
            """
            |+----------------+
            ||             s1 |
            |+----------------+
            || [{[2.0, 3.0]}] |
            |+----------------+""",
        )
        actual_named = df.select(*resolve_nested_fields(named_transformations))
        actual = df.select(*resolve_nested_fields(transformations))
        assert actual_named.treeString() == expected_schema
        assert actual_named.show_string(simplify_structs=True) == expected
        assert actual.treeString() == expected_schema
        assert actual.show_string(simplify_structs=True) == expected

    def test_array_struct_in_array_struct(self, bq: BigQueryBuilder):
        """
        GIVEN a DataFrame with an array<struct> inside another array<struct>
        WHEN we use resolve_nested_columns on it
        THEN the transformation should work
        """
        df = bq.sql("SELECT [STRUCT([STRUCT(2 as a)] as s2)] as s1")
        assert df.treeString() == strip_margin(
            """
            |root
            | |-- s1: RECORD (REPEATED)
            | |    |-- s2: RECORD (REPEATED)
            | |    |    |-- a: INTEGER (NULLABLE)
            |""",
        )
        assert df.show_string(simplify_structs=True) == strip_margin(
            """
            |+-----------+
            ||        s1 |
            |+-----------+
            || [{[{2}]}] |
            |+-----------+""",
        )
        named_transformations = {
            "s1!.s2!.a": PrintableFunction(
                lambda s: s["a"].cast("FLOAT64"),
                lambda s: f'{s}["a"].cast("FLOAT64")',
            ),
        }
        transformations = replace_named_functions_with_functions(named_transformations)
        expected_schema = strip_margin(
            """
            |root
            | |-- s1: RECORD (REPEATED)
            | |    |-- s2: RECORD (REPEATED)
            | |    |    |-- a: FLOAT (NULLABLE)
            |""",
        )
        expected = strip_margin(
            """
            |+-------------+
            ||          s1 |
            |+-------------+
            || [{[{2.0}]}] |
            |+-------------+""",
        )
        actual_named = df.select(*resolve_nested_fields(named_transformations))
        actual = df.select(*resolve_nested_fields(transformations))
        assert actual_named.treeString() == expected_schema
        assert actual_named.show_string(simplify_structs=True) == expected
        assert actual.treeString() == expected_schema
        assert actual.show_string(simplify_structs=True) == expected

    def test_array_struct_in_array_struct_with_transformation_using_field_from_first_array(
        self,
        bq: BigQueryBuilder,
    ):
        """
        GIVEN a DataFrame with an array<struct> inside another array<struct>
        WHEN we use resolve_nested_columns on it with a transformation that uses data from the first array
        THEN the transformation should work
        """
        df = bq.sql("SELECT [STRUCT(1 as a, [STRUCT(2 as b)] as s2)] as s1")
        assert df.treeString() == strip_margin(
            """
            |root
            | |-- s1: RECORD (REPEATED)
            | |    |-- a: INTEGER (NULLABLE)
            | |    |-- s2: RECORD (REPEATED)
            | |    |    |-- b: INTEGER (NULLABLE)
            |""",
        )
        assert df.show_string(simplify_structs=True) == strip_margin(
            """
            |+--------------+
            ||           s1 |
            |+--------------+
            || [{1, [{2}]}] |
            |+--------------+""",
        )
        named_transformations = {
            "s1!.a": PrintableFunction(lambda s1: s1["a"], lambda s1: f'{s1}["a"]'),
            "s1!.s2!.b": PrintableFunction(
                lambda s1, s2: (s1["a"] + s2["b"]).cast("FLOAT64"),
                lambda s1, s2: f'({s1}["a"]+{s2}["b"]).cast("FLOAT64")',
            ),
        }
        transformations = replace_named_functions_with_functions(named_transformations)
        expected_schema = strip_margin(
            """
            |root
            | |-- s1: RECORD (REPEATED)
            | |    |-- a: INTEGER (NULLABLE)
            | |    |-- s2: RECORD (REPEATED)
            | |    |    |-- b: FLOAT (NULLABLE)
            |""",
        )
        expected = strip_margin(
            """
            |+----------------+
            ||             s1 |
            |+----------------+
            || [{1, [{3.0}]}] |
            |+----------------+""",
        )
        actual_named = df.select(*resolve_nested_fields(named_transformations))
        actual = df.select(*resolve_nested_fields(transformations))
        assert actual_named.treeString() == expected_schema
        assert actual_named.show_string(simplify_structs=True) == expected
        assert actual.treeString() == expected_schema
        assert actual.show_string(simplify_structs=True) == expected

    def test_struct_in_array_struct_in_array_struct(self, bq: BigQueryBuilder):
        """
        GIVEN a DataFrame with a struct inside an array<struct> inside an array<struct>
        WHEN we use resolve_nested_columns with a transformation that access an element from the outermost struct
        THEN the transformation should work
        """
        df = bq.sql("""SELECT [STRUCT([STRUCT(1 as a, STRUCT(2 as b) as s3)] as s2)] as s1""")
        assert nested.schema_string(df) == strip_margin(
            """
            |root
            | |-- s1!.s2!.a: INTEGER (nullable = true)
            | |-- s1!.s2!.s3.b: INTEGER (nullable = true)
            |""",
        )
        assert df.show_string(simplify_structs=True) == strip_margin(
            """
            |+----------------+
            ||             s1 |
            |+----------------+
            || [{[{1, {2}}]}] |
            |+----------------+""",
        )
        # Here, the lambda function is applied to the elements of `s2`, not `s3`
        named_transformations = {
            "s1!.s2!.a": PrintableFunction(lambda s2: s2["a"], lambda s2: f'{s2}["a"]'),
            "s1!.s2!.s3.b": PrintableFunction(
                lambda s2: s2["a"].cast("FLOAT64"),
                lambda s2: f'{s2}["a"].cast("FLOAT64")',
            ),
        }
        transformations = replace_named_functions_with_functions(named_transformations)
        expected_schema = strip_margin(
            """
            |root
            | |-- s1!.s2!.a: INTEGER (nullable = true)
            | |-- s1!.s2!.s3.b: FLOAT (nullable = true)
            |""",
        )
        expected = strip_margin(
            """
            |+------------------+
            ||               s1 |
            |+------------------+
            || [{[{1, {1.0}}]}] |
            |+------------------+""",
        )
        actual_named = df.select(*resolve_nested_fields(named_transformations))
        actual = df.select(*resolve_nested_fields(transformations))
        assert nested.schema_string(actual_named) == expected_schema
        assert actual_named.show_string(simplify_structs=True) == expected
        assert nested.schema_string(actual) == expected_schema
        assert actual.show_string(simplify_structs=True) == expected

    def test_struct_in_struct_in_array_struct_in_struct_in_array_struct(
        self,
        bq: BigQueryBuilder,
    ):
        """
        GIVEN a DataFrame with a struct inside a struct inside an array<struct> inside a struct inside an array<struct>
        WHEN we use resolve_nested_columns with a transformation that access an element from the outermost struct
        THEN the transformation should work
        """
        df = bq.sql(
            """SELECT
            [STRUCT(STRUCT(
                [STRUCT(STRUCT(1 as a, STRUCT(2 as b) as s5) as s4)] as s3
            ) as s2)] as s1
        """,
        )
        assert nested.schema_string(df) == strip_margin(
            """
            |root
            | |-- s1!.s2.s3!.s4.a: INTEGER (nullable = true)
            | |-- s1!.s2.s3!.s4.s5.b: INTEGER (nullable = true)
            |""",
        )
        assert df.show_string(simplify_structs=True) == strip_margin(
            """
            |+--------------------+
            ||                 s1 |
            |+--------------------+
            || [{{[{{1, {2}}}]}}] |
            |+--------------------+""",
        )
        # Here, the lambda function is applied to the elements of `s3`, not `s4` or `s5`
        named_transformations = {
            "s1!.s2.s3!.s4.a": PrintableFunction(
                lambda s3: s3["s4"]["a"],
                lambda s3: f'{s3}["s4"]["a"]',
            ),
            "s1!.s2.s3!.s4.s5.b": PrintableFunction(
                lambda s3: s3["s4"]["a"].cast("FLOAT64"),
                lambda s3: f'{s3}["s4"]["a"].cast("FLOAT64")',
            ),
        }
        transformations = replace_named_functions_with_functions(named_transformations)
        expected_schema = strip_margin(
            """
            |root
            | |-- s1!.s2.s3!.s4.a: INTEGER (nullable = true)
            | |-- s1!.s2.s3!.s4.s5.b: FLOAT (nullable = true)
            |""",
        )
        expected = strip_margin(
            """
            |+----------------------+
            ||                   s1 |
            |+----------------------+
            || [{{[{{1, {1.0}}}]}}] |
            |+----------------------+""",
        )
        actual_named = df.select(*resolve_nested_fields(named_transformations))
        actual = df.select(*resolve_nested_fields(transformations))
        assert nested.schema_string(actual_named) == expected_schema
        assert actual_named.show_string(simplify_structs=True) == expected
        assert nested.schema_string(actual) == expected_schema
        assert actual.show_string(simplify_structs=True) == expected

    def test_struct_in_struct_in_array_struct_in_struct_in_array_struct_with_none(
        self,
        bq: BigQueryBuilder,
    ):
        """
        GIVEN a DataFrame with a struct inside a struct inside an array<struct> inside a struct inside an array<struct>
        WHEN we use resolve_nested_columns with a None transformation
        THEN the transformation should work
        """
        df = bq.sql(
            """SELECT
            [STRUCT(STRUCT(
                [STRUCT(STRUCT(1 as a, STRUCT(2 as b) as s5) as s4)] as s3
            ) as s2)] as s1
        """,
        )
        assert nested.schema_string(df) == strip_margin(
            """
            |root
            | |-- s1!.s2.s3!.s4.a: INTEGER (nullable = true)
            | |-- s1!.s2.s3!.s4.s5.b: INTEGER (nullable = true)
            |""",
        )
        assert df.show_string(simplify_structs=True) == strip_margin(
            """
            |+--------------------+
            ||                 s1 |
            |+--------------------+
            || [{{[{{1, {2}}}]}}] |
            |+--------------------+""",
        )
        # Here, the lambda function is applied to the elements of `s3`, not `s4` or `s5`
        named_transformations = {"s1!.s2.s3!.s4.a": None, "s1!.s2.s3!.s4.s5.b": None}
        expected_schema = strip_margin(
            """
            |root
            | |-- s1!.s2.s3!.s4.a: INTEGER (nullable = true)
            | |-- s1!.s2.s3!.s4.s5.b: INTEGER (nullable = true)
            |""",
        )
        expected = strip_margin(
            """
            |+--------------------+
            ||                 s1 |
            |+--------------------+
            || [{{[{{1, {2}}}]}}] |
            |+--------------------+""",
        )
        actual_named = df.select(*resolve_nested_fields(named_transformations))
        assert nested.schema_string(actual_named) == expected_schema
        assert actual_named.show_string(simplify_structs=True) == expected
