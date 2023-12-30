class IllegalArgumentException(Exception):
    """
    Passed an illegal or inappropriate argument.
    """


class AnalysisException(Exception):
    """
    Exception happening during the preparation of a transformation.
    """

    pass


class UnsupportedOperationException(Exception):
    """
    When the user does an operation that is not supported.
    """

    pass


class UnexpectedException(Exception):
    """Exception raised when something that is not supposed to happen happens"""

    issue_submit_url = "https://github.com/FurcyPin/bigquery-frame/issues/new"

    def __init__(self, error: str) -> None:
        msg = (
            f"An unexpected error occurred: {error}"
            f"\nPlease report a bug with the complete stacktrace at {self.issue_submit_url}"
        )
        Exception.__init__(self, msg)
