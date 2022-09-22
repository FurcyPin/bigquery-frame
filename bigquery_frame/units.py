KIBI = 1024
MULTIPLES = ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi", "Yi"]


def bytes_to_human_readable(byte_amount: int) -> str:
    """Transform a integer representing an amount of bytes into a human-readable string.

    >>> bytes_to_human_readable(128)
    '128.00 B'
    >>> bytes_to_human_readable(2048)
    '2.00 KiB'
    >>> bytes_to_human_readable(1000000000)
    '953.67 MiB'

    :param byte_amount:
    :return:
    """
    coef = 0
    float_amount = float(byte_amount)
    while float_amount > KIBI and coef < len(MULTIPLES) - 1:
        coef += 1
        float_amount = float_amount / 1024
    return f"{float_amount:0.2f} {MULTIPLES[coef]}B"
