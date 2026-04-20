class GnrDaemonException(Exception):
    """Base class for all genro-daemon exceptions."""


class GnrDaemonLocked(GnrDaemonException):
    """Raised when a distributed lock cannot be acquired within the retry budget.

    The caller should either retry the operation after a short delay or surface
    the contention to the user as a transient error.
    """


class GnrDaemonProtoError(Exception):
    """Raised when an incoming message does not conform to the ARS protocol.

    Includes the raw request payload in the message when available so that
    operators can diagnose misbehaving clients from the daemon logs.
    """


class GnrDaemonMethodNotFound(Exception):
    """Raised when the requested method does not exist on the target object.

    The message includes the method name and, when routing via a site register,
    the site name so that the caller can distinguish typos from mis-routing.
    """


class GnrDaemonUnavailable(GnrDaemonException):
    """Raised when the daemon did not respond (timeout or connection error).

    The operation was not completed.  The caller should surface this as a
    transient error (e.g. HTTP 503) rather than silently returning None.
    """


__all__ = [
    "GnrDaemonProtoError",
    "GnrDaemonMethodNotFound",
    "GnrDaemonException",
    "GnrDaemonLocked",
    "GnrDaemonUnavailable",
]
