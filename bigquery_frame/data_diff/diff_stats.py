from dataclasses import dataclass


@dataclass
class DiffStats:
    total: int
    """Total number of rows after joining the two DataFrames"""
    no_change: int
    """Number of rows that are identical in both DataFrames"""
    changed: int
    """Number of rows that are present in both DataFrames but that have different values"""
    in_left: int
    """Number of rows in the left DataFrame"""
    in_right: int
    """Number of rows in the right DataFrame"""
    only_in_left: int
    """Number of rows that are only present in the left DataFrame"""
    only_in_right: int
    """Number of rows that are only present in the right DataFrame"""

    @property
    def same_data(self) -> bool:
        return self.no_change == self.total

    @property
    def percent_changed(self) -> float:
        return round(self.changed * 100.0 / self.total, 2)

    @property
    def percent_no_change(self) -> float:
        return round(self.no_change * 100.0 / self.total, 2)

    @property
    def percent_only_in_left(self) -> float:
        return round(self.only_in_left * 100.0 / self.total, 2)

    @property
    def percent_only_in_right(self) -> float:
        return round(self.only_in_right * 100.0 / self.total, 2)


def print_diff_stats(diff_stats: DiffStats, left_df_alias: str, right_df_alias: str):
    if diff_stats.total == diff_stats.no_change:
        print("\ndiff ok!\n")
    else:
        print("\ndiff NOT ok\n")
        print("Summary:")
        nb_row_diff = diff_stats.in_right - diff_stats.in_left
        if nb_row_diff != 0:
            if nb_row_diff > 0:
                more_less = "more"
            else:
                more_less = "less"
            print("\nRow count changed: ")
            print(f"{left_df_alias}: {diff_stats.in_left} rows")
            print(f"{right_df_alias}: {diff_stats.in_right} rows ({abs(nb_row_diff)} {more_less})")
            print("")
        else:
            print(f"\nRow count ok: {diff_stats.in_right} rows")
            print("")
        print(f"{diff_stats.no_change} ({diff_stats.percent_no_change}%) rows are identical")
        print(f"{diff_stats.changed} ({diff_stats.percent_changed}%) rows have changed")
        if diff_stats.only_in_left > 0:
            print(f"{diff_stats.only_in_left} ({diff_stats.percent_only_in_left}%) rows are only in '{left_df_alias}'")
        if diff_stats.only_in_right > 0:
            print(
                f"{diff_stats.only_in_right} ({diff_stats.percent_only_in_right}%) rows are only in '{right_df_alias}"
            )
        print("")
