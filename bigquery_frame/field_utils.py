from bigquery_frame.conf import REPETITION_MARKER


def is_sub_field_or_equal(sub_field: str, field: str) -> bool:
    """Return True if `sub_field` is a sub-field of `field`

    >>> is_sub_field_or_equal("a", "a")
    True
    >>> is_sub_field_or_equal("a", "b")
    False

    >>> is_sub_field_or_equal("a.b", "a")
    True
    >>> is_sub_field_or_equal("a.b", "b")
    False

    >>> is_sub_field_or_equal("a", "a.b")
    False

    """
    return sub_field == field or sub_field.startswith(field + ".")


def is_sub_field_or_equal_to_any(sub_field: str, fields: list[str]) -> bool:
    """Return True if `sub_field` is a sub-field of any field in `fields`

    >>> is_sub_field_or_equal_to_any("a", ["a", "b"])
    True
    >>> is_sub_field_or_equal_to_any("a", ["b", "c"])
    False

    >>> is_sub_field_or_equal_to_any("a.b", ["a", "b"])
    True
    >>> is_sub_field_or_equal_to_any("a.b", ["b", "c"])
    False

    >>> is_sub_field_or_equal_to_any("a", ["a.b"])
    False

    >>> is_sub_field_or_equal_to_any("a", [])
    False

    """
    return any(is_sub_field_or_equal(sub_field, field) for field in fields)


def get_granularity(field: str) -> str:
    """Return True if `field` has the given granularity

    >>> get_granularity("a")
    ''
    >>> get_granularity("s.z")
    ''
    >>> get_granularity("a!")
    'a!'
    >>> get_granularity("a!.b")
    'a!'
    >>> get_granularity("a!.b!.c")
    'a!.b!'

    """
    granularity = substring_before_last_occurrence(field, "!")
    if granularity == "":
        return granularity
    else:
        return granularity + "!"


def has_same_granularity(field: str, other_field: str) -> bool:
    """Return True if `field` is at the same granularity level as `other_field`

    >>> has_same_granularity("a", "a")
    True
    >>> has_same_granularity("a", "b")
    True
    >>> has_same_granularity("a", "a.b")
    True
    >>> has_same_granularity("a.b", "a.a")
    True
    >>> has_same_granularity("a.b", "b.a")
    True
    >>> has_same_granularity("a.b", "a")
    True
    >>> has_same_granularity("a", "a.b")
    True

    >>> has_same_granularity("a!.a", "a!.b")
    True
    >>> has_same_granularity("a!.a", "a!.b.c")
    True
    >>> has_same_granularity("a!.b", "b.a")
    False
    >>> has_same_granularity("a!.b", "a")
    False
    >>> has_same_granularity("a", "a!.b")
    False

    """
    return get_granularity(field) == get_granularity(other_field)


def has_same_granularity_as_any(field: str, other_fields: list[str]) -> bool:
    """Return True if `field` is equal to or has the same parent as any field in `other_fields`

    >>> has_same_granularity_as_any("a", ["a", "b"])
    True

    >>> has_same_granularity_as_any("a.b", ["a.a", "b"])
    True
    >>> has_same_granularity_as_any("a.b", ["b", "c"])
    True

    >>> has_same_granularity_as_any("a!.b", ["b", "c"])
    False

    >>> has_same_granularity_as_any("a!.b!.c", ["a!.b!.d"])
    True

    >>> has_same_granularity_as_any("a!.b.c", ["a"])
    False
    >>> has_same_granularity_as_any("a.b!.c", ["a"])
    False

    >>> has_same_granularity_as_any("a", [])
    False

    """
    return any(has_same_granularity(field, other_field) for other_field in other_fields)


def is_sub_field(sub_field: str, field: str) -> bool:
    """Return True if `sub_field` is a sub-field of `field`, or is equal to `field`

    >>> is_sub_field("a", "a")
    True
    >>> is_sub_field("a", "b")
    False

    >>> is_sub_field("a.b", "a")
    True
    >>> is_sub_field("a!.b", "a!")
    True
    >>> is_sub_field("a.b", "b")
    False
    >>> is_sub_field("a.b!", "a")
    False
    >>> is_sub_field("a!.b", "a")
    False

    >>> is_sub_field("a", "a.b")
    False

    >>> is_sub_field("a.b.c", "a.b")
    True
    >>> is_sub_field("a.b.c", "a")
    True

    """
    return sub_field == field or (
        sub_field.startswith(field + ".") and REPETITION_MARKER not in sub_field[len(field) :]
    )


def is_sub_field_of_any(direct_sub_field: str, fields: list[str]) -> bool:
    """Return True if `direct_sub_field` is a sub-field of any field in `fields`

    >>> is_sub_field_of_any("a", ["a", "b"])
    True

    >>> is_sub_field_of_any("a.b", ["a", "b"])
    True
    >>> is_sub_field_of_any("a!.b", ["a!", "b"])
    True
    >>> is_sub_field_of_any("a!.b!", ["a!", "b!"])
    False
    >>> is_sub_field_of_any("a.b", ["b", "c"])
    False

    >>> is_sub_field_of_any("a.b.c", ["a.b"])
    True

    >>> is_sub_field_of_any("a.b.c", ["a"])
    True

    >>> is_sub_field_of_any("a", [])
    False

    """
    return any(is_sub_field(direct_sub_field, field) for field in fields)


def is_parent_field(field: str, other_field: str) -> bool:
    """Return True if `other_field` is a sub-field of `field`

    >>> is_parent_field("a", "a")
    False
    >>> is_parent_field("a", "b")
    False

    >>> is_parent_field("a", "a.b")
    True
    >>> is_parent_field("b", "a.b")
    False

    >>> is_parent_field("a.b", "a")
    False

    """
    return other_field.startswith((field + ".", field + "!."))


def is_parent_field_of_any(field: str, other_fields: list[str]) -> bool:
    """Return True if any field in `other_fields` is a sub-field of `field`

    >>> is_parent_field_of_any("a", ["a", "b"])
    False
    >>> is_parent_field_of_any("a", ["b", "c"])
    False

    >>> is_parent_field_of_any("a", ["a.b", "b"])
    True
    >>> is_parent_field_of_any("a", ["b.a", "c.a"])
    False

    >>> is_parent_field_of_any("a.b", ["a"])
    False

    >>> is_parent_field_of_any("a", [])
    False

    """
    return any(is_parent_field(field, other_field) for other_field in other_fields)


def substring_before_last_occurrence(s: str, sep: str) -> str:
    """Returns the substring before the last occurrence of `sep` in `s`

    >>> substring_before_last_occurrence("abc", ".")
    ''
    >>> substring_before_last_occurrence("abc.d", ".")
    'abc'
    >>> substring_before_last_occurrence("abc.d.e", ".")
    'abc.d'

    """
    index = s.rfind(sep)
    if index == -1:
        return ""
    else:
        return s[:index]


def substring_after_last_occurrence(s: str, sep: str) -> str:
    """Returns the substring after the last occurrence of `sep` in `s`

    >>> substring_after_last_occurrence("abc", ".")
    'abc'
    >>> substring_after_last_occurrence("abc.d", ".")
    'd'
    >>> substring_after_last_occurrence("abc.d.e", ".")
    'e'

    """
    index = s.rfind(sep)
    if index == -1:
        return s
    else:
        return s[index + 1 :]
