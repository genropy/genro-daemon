"""Tests for genro_daemon.exceptions."""

import pytest

from genro_daemon.exceptions import (
    GnrDaemonException,
    GnrDaemonLocked,
    GnrDaemonMethodNotFound,
    GnrDaemonProtoError,
)


class TestExceptionHierarchy:
    def test_gnr_daemon_locked_is_gnr_daemon_exception(self):
        assert issubclass(GnrDaemonLocked, GnrDaemonException)

    def test_proto_error_is_exception(self):
        assert issubclass(GnrDaemonProtoError, Exception)

    def test_method_not_found_is_exception(self):
        assert issubclass(GnrDaemonMethodNotFound, Exception)

    def test_proto_error_not_daemon_exception(self):
        assert not issubclass(GnrDaemonProtoError, GnrDaemonException)

    def test_method_not_found_not_daemon_exception(self):
        assert not issubclass(GnrDaemonMethodNotFound, GnrDaemonException)


class TestInstantiation:
    def test_gnr_daemon_exception_message(self):
        exc = GnrDaemonException("something went wrong")
        assert str(exc) == "something went wrong"

    def test_gnr_daemon_locked_message(self):
        exc = GnrDaemonLocked("resource is locked")
        assert str(exc) == "resource is locked"
        assert isinstance(exc, GnrDaemonException)

    def test_proto_error_message(self):
        exc = GnrDaemonProtoError("bad protocol")
        assert str(exc) == "bad protocol"

    def test_method_not_found_message(self):
        exc = GnrDaemonMethodNotFound("no such method 'foo'")
        assert "foo" in str(exc)

    def test_raise_and_catch_locked(self):
        with pytest.raises(GnrDaemonException):
            raise GnrDaemonLocked("locked")

    def test_raise_and_catch_proto_error(self):
        with pytest.raises(GnrDaemonProtoError):
            raise GnrDaemonProtoError("proto")

    def test_raise_and_catch_method_not_found(self):
        with pytest.raises(GnrDaemonMethodNotFound):
            raise GnrDaemonMethodNotFound("nope")
