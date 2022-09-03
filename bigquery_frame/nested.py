from collections import OrderedDict
from typing import Dict, List, Union

from bigquery_frame.column import Column, StringOrColumn
from bigquery_frame.conf import REPETITION_MARKER, STRUCT_SEPARATOR
from bigquery_frame.utils import str_to_col

OrderedTree = Union["OrderedTree", Dict[str, "OrderedTree"]]


def _build_nested_struct_tree(columns: Dict[str, Column]) -> OrderedTree:
    def rec_insert(node: OrderedDict, alias: str, column: Column) -> None:
        if STRUCT_SEPARATOR in alias:
            struct, subcol = alias.split(STRUCT_SEPARATOR, 1)
            if struct not in node:
                node[struct] = OrderedDict()
            rec_insert(node[struct], subcol, column)
        else:
            node[alias] = column

    tree = OrderedDict()
    for col_name, col_type in columns.items():
        rec_insert(tree, col_name, col_type)
    return tree


def _build_struct_from_tree(node: OrderedTree, sort: bool = False) -> List[Column]:
    """

    >>> from bigquery_frame import functions as f
    >>> tree = OrderedDict([('s!', OrderedDict([('c', f.col("c")), ('d', f.col("d").cast("FLOAT64"))]))])
    >>> for c in _build_struct_from_tree(tree): print(c)
    ARRAY(
      SELECT
        STRUCT(`c` as `c`, CAST(`d` as FLOAT64) as `d`)
      FROM UNNEST(s) as `_`
    ) as `s`

    >>> tree = OrderedDict([('s', OrderedDict([('e!', f.col("_").cast("FLOAT64"))]))])
    >>> for c in _build_struct_from_tree(tree): print(c)
    STRUCT(ARRAY(
      SELECT
        CAST(`_` as FLOAT64)
      FROM UNNEST(`s`.`e`) as `_`
    ) as `e`) as `s`

    >>> tree = OrderedDict([('s!', OrderedDict([('c', 'c'), ('d', 'd')]))])
    >>> for c in _build_struct_from_tree(tree, sort = True): print(c)
    ARRAY(
      SELECT
        STRUCT(`c` as `c`, `d` as `d`)
      FROM UNNEST(s) as `_`
      ORDER BY `c`, `d`
    ) as `s`

    >>> tree = OrderedDict([('s', OrderedDict([('e!', '_')]))])
    >>> for c in _build_struct_from_tree(tree, sort = True): print(c)
    STRUCT(ARRAY(
      SELECT
        `_`
      FROM UNNEST(`s`.`e`) as `_`
      ORDER BY `_`
    ) as `e`) as `s`

    >>> tree = OrderedDict([('l1!', OrderedDict([('l2!', OrderedDict([('a', 'a'), ('b', 'b')]))]))])
    >>> for c in _build_struct_from_tree(tree, sort = True): print(c)
    ARRAY(
      SELECT
        STRUCT(ARRAY(
      SELECT
        STRUCT(`a` as `a`, `b` as `b`)
      FROM UNNEST(l2) as `_`
      ORDER BY `a`, `b`
    ) as `l2`)
      FROM UNNEST(l1) as `_`
      ORDER BY TO_JSON_STRING(ARRAY(
      SELECT
        STRUCT(`a` as `a`, `b` as `b`)
      FROM UNNEST(l2) as `_`
      ORDER BY `a`, `b`
    ))
    ) as `l1`

    >>> tree = OrderedDict([('l1!', OrderedDict([('s', OrderedDict([('a', 's.a'), ('b', 's.b')]))]))])
    >>> for c in _build_struct_from_tree(tree, sort = True): print(c)
    ARRAY(
      SELECT
        STRUCT(STRUCT(`s`.`a` as `a`, `s`.`b` as `b`) as `s`)
      FROM UNNEST(l1) as `_`
      ORDER BY TO_JSON_STRING(STRUCT(`s`.`a` as `a`, `s`.`b` as `b`))
    ) as `l1`

    :param node:
    :param sort: If set to true, will ensure that all arrays are canonically sorted
    :return:
    """
    from bigquery_frame import functions as f

    def aux(node: OrderedTree, prefix: str = ""):
        def json_if_not_sortable(col: Column, is_sortable: bool) -> Column:
            if not is_sortable:
                return f.expr(f"TO_JSON_STRING({col.expr})")
            else:
                return col

        cols = []
        for key, col_or_children in node.items():
            is_repeated = key[-1] == REPETITION_MARKER
            is_struct = col_or_children is not None and not isinstance(col_or_children, (str, Column))
            is_sortable = not (is_repeated or is_struct)
            key_no_sep = key.replace(REPETITION_MARKER, "")
            if not is_struct:
                col = f.col(prefix + key_no_sep)
                transform_col = str_to_col(col_or_children)
                if is_repeated:
                    col = f.transform(col, transform_col)
                    if sort:
                        col = f.sort_array(col, f.col("_"))
                else:
                    col = transform_col
                cols.append((col.alias(key_no_sep), is_sortable))
            else:
                if is_repeated:
                    fields = aux(col_or_children, prefix="")
                    transform_col = f.struct(*[field for field, is_sortable in fields])
                    struct_col = f.transform(Column(prefix + key_no_sep), transform_col)
                    if sort:
                        sort_cols = [json_if_not_sortable(field, is_sortable) for field, is_sortable in fields]
                        struct_col = f.sort_array(struct_col, sort_cols)
                    struct_col = struct_col.alias(key_no_sep)
                else:
                    fields = aux(col_or_children, prefix=prefix + key + STRUCT_SEPARATOR)
                    fields = [field for field, is_array in fields]
                    struct_col = f.struct(*fields).alias(key_no_sep)
                cols.append((struct_col, is_sortable))
        return cols

    cols = aux(node)
    return [col for col, is_array in cols]


def resolve_nested_columns(columns: Dict[str, StringOrColumn], sort: bool = False) -> List[Column]:
    """Builds a list of column expressions to manipulate structs and repeated records

    >>> from bigquery_frame import functions as f
    >>> resolve_nested_columns({
    ...   "s!.c": f.col("c"),
    ...   "s!.d": f.col("d").cast("FLOAT64")
    ... })
    [Column('ARRAY(
      SELECT
        STRUCT(`c` as `c`, CAST(`d` as FLOAT64) as `d`)
      FROM UNNEST(s) as `_`
    )')]

    >>> resolve_nested_columns({
    ...   "s!.e!": f.col("_").cast("FLOAT64"),
    ... })
    [Column('ARRAY(
      SELECT
        STRUCT(ARRAY(
      SELECT
        CAST(`_` as FLOAT64)
      FROM UNNEST(`e`) as `_`
    ) as `e`)
      FROM UNNEST(s) as `_`
    )')]

    >>> resolve_nested_columns({
    ...   "s!.c": f.col("c"),
    ...   "s!.d": f.col("d"),
    ... }, sort = True)
    [Column('ARRAY(
      SELECT
        STRUCT(`c` as `c`, `d` as `d`)
      FROM UNNEST(s) as `_`
      ORDER BY `c`, `d`
    )')]

    >>> resolve_nested_columns({
    ...   "s!.e!": f.col("_"),
    ... }, sort = True)
    [Column('ARRAY(
      SELECT
        STRUCT(ARRAY(
      SELECT
        `_`
      FROM UNNEST(`e`) as `_`
      ORDER BY `_`
    ) as `e`)
      FROM UNNEST(s) as `_`
      ORDER BY TO_JSON_STRING(ARRAY(
      SELECT
        `_`
      FROM UNNEST(`e`) as `_`
      ORDER BY `_`
    ))
    )')]

    :param columns:
    :param sort: If set to true, will ensure that all arrays are canonically sorted
    :return:
    """
    tree = _build_nested_struct_tree(columns)
    return _build_struct_from_tree(tree, sort)
