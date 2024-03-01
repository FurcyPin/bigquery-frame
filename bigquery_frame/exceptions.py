class IllegalArgumentException(Exception):
    """
    Passed an illegal or inappropriate argument.
    """


class AnalysisException(Exception):
    """
    Exception raised when an anomaly is detected during the preparation of a transformation.
    """


class UnsupportedOperationException(Exception):
    """
    When the user does an operation that is not supported.
    """


class UnexpectedException(Exception):
    """Exception raised when something that is not supposed to happen happens"""

    issue_submit_url = "https://github.com/FurcyPin/bigquery-frame/issues/new"

    def __init__(self, error: str) -> None:
        msg = (
            f"An unexpected error occurred: {error}"
            f"\nPlease report a bug with the complete stacktrace at {self.issue_submit_url}"
        )
        Exception.__init__(self, msg)


class DataframeComparatorException(Exception):
    """
    Exception happening during data diff.
    """


class CombinatorialExplosionError(DataframeComparatorException):
    """
    Exception happening before a join when we detect that the join key is incorrect,
    which would lead to a combinatorial explosion.
    """
