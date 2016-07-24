import logging


class EmptyBatch(Exception):
    pass


class SendFailed(Exception):
    severity = logging.ERROR


class BadToken(SendFailed):
    """
    This is raised when the remote server notifies that the token is invalid
    """
    severity = logging.CRITICAL


class BatchTooBig(SendFailed):
    """
    This is raised when the remote server closes the connection in the middle
    of the batch, causes the SDK to re-evaluate the proper batch size
    """
    pass


class ConnectionFailed(Exception):
    pass
