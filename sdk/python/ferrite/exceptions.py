"""Custom exception hierarchy for the Ferrite Python client.

Every exception raised by this library is a subclass of :class:`FerriteError`,
which itself inherits from the stdlib ``Exception``.  Lower-level
``redis.exceptions.*`` errors are caught internally and re-raised as the
appropriate Ferrite-specific type whenever possible.
"""

from __future__ import annotations


class FerriteError(Exception):
    """Base exception for all Ferrite client errors."""


class FerriteConnectionError(FerriteError):
    """Raised when the client cannot connect to the Ferrite server."""


class FerriteTimeoutError(FerriteError):
    """Raised when an operation exceeds the configured timeout."""


class FerriteCommandError(FerriteError):
    """Raised when a Ferrite-specific command returns an error response."""


class FerriteSerializationError(FerriteError):
    """Raised when a value cannot be serialized or deserialized (e.g. JSON)."""


class FerriteIndexNotFoundError(FerriteError):
    """Raised when a referenced index (vector, semantic, etc.) does not exist."""


class FerriteKeyNotFoundError(FerriteError):
    """Raised when a referenced key does not exist."""
