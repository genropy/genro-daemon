"""Tests for genro_daemon.handler – GnrDaemon routing and daemon-level methods."""

from unittest.mock import patch

import pytest

from genro_daemon.ars import Ars
from genro_daemon.exceptions import GnrDaemonMethodNotFound
from genro_daemon.handler import GnrDaemon
from genro_daemon.siteregister import GnrSiteRegister
from genro_daemon.storage.memory import InMemoryBackend

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_daemon():
    d = GnrDaemon()
    d._options = {}
    return d


def make_register(daemon, sitename="testsite"):
    backend = InMemoryBackend()
    reg = GnrSiteRegister(daemon, sitename=sitename, backend=backend)
    reg.setConfiguration()
    return reg


# ---------------------------------------------------------------------------
# _req_parse routing
# ---------------------------------------------------------------------------


class TestReqParseRouting:
    def setup_method(self):
        self.daemon = make_daemon()
        self.register = make_register(self.daemon)
        self.daemon._siteregisters["testsite"] = self.register

    def test_routes_to_daemon_ping_even_with_sitename(self):
        """ping is on GnrDaemon only; register's __getattr__ must not intercept it."""
        req = [Ars.REQ, 1, "ping", [], {"_sitename": "testsite"}]
        call_id, method, args, kwargs, name, sitename = self.daemon._req_parse(req)
        assert method == self.daemon.ping

    def test_routes_register_method_on_site_stop_with_sitename(self):
        """on_site_stop IS defined on GnrSiteRegister → routed there."""
        req = [Ars.REQ, 1, "on_site_stop", [], {"_sitename": "testsite"}]
        call_id, method, args, kwargs, name, sitename = self.daemon._req_parse(req)
        assert method == self.register.on_site_stop

    @patch("genro_daemon.handler.GnrDaemon._start_site_processes")
    @patch("genro_daemon.handler.get_backend", return_value=InMemoryBackend())
    def test_daemon_method_fallback_unknown_sitename(self, mock_backend, mock_procs):
        """Unknown sitename → auto-create register, ping not in register MRO → fall to daemon."""
        req = [Ars.REQ, 1, "ping", [], {"_sitename": "unknown_site"}]
        call_id, method, args, kwargs, name, sitename = self.daemon._req_parse(req)
        assert method == self.daemon.ping

    def test_daemon_method_without_sitename(self):
        req = [Ars.REQ, 1, "ping", [], {}]
        call_id, method, args, kwargs, name, sitename = self.daemon._req_parse(req)
        assert method == self.daemon.ping

    def test_unknown_method_no_sitename_raises(self):
        # With no sitename and no registered sites, the method is not found.
        daemon = make_daemon()  # fresh daemon with no registers
        req = [Ars.REQ, 1, "unknown_method", [], {}]
        with pytest.raises(GnrDaemonMethodNotFound):
            daemon._req_parse(req)

    def test_private_method_always_raises(self):
        req = [Ars.REQ, 1, "_internal", [], {}]
        with pytest.raises(GnrDaemonMethodNotFound):
            self.daemon._req_parse(req)

    def test_sitename_is_stripped_from_kwargs(self):
        req = [Ars.REQ, 1, "ping", [], {"_sitename": "testsite", "extra": "val"}]
        _, _, _, kwargs, _, _ = self.daemon._req_parse(req)
        assert "_sitename" not in kwargs
        assert kwargs.get("extra") == "val"

    @patch("genro_daemon.handler.GnrDaemon._start_site_processes")
    @patch("genro_daemon.handler.get_backend", return_value=InMemoryBackend())
    def test_unknown_sitename_falls_back_to_daemon(self, mock_backend, mock_procs):
        req = [Ars.REQ, 1, "ping", [], {"_sitename": "unknown_site"}]
        call_id, method, args, kwargs, name, sitename = self.daemon._req_parse(req)
        assert method == self.daemon.ping

    @patch("genro_daemon.handler.GnrDaemon._start_site_processes")
    @patch("genro_daemon.handler.get_backend", return_value=InMemoryBackend())
    def test_unknown_sitename_unknown_method_routes_to_register_decore(
        self, mock_backend, mock_procs
    ):
        """Unknown method with a sitename: auto-create register, route to __getattr__ decore.

        The decore raises AttributeError at *call* time (not during parse), so
        _req_parse itself must succeed and return a callable.
        """
        req = [Ars.REQ, 1, "no_such", [], {"_sitename": "unknown_site"}]
        call_id, method, args, kwargs, name, sitename = self.daemon._req_parse(req)
        assert callable(method)
        # Calling it without register_name causes AttributeError (method missing)
        with pytest.raises(AttributeError):
            method()


# ---------------------------------------------------------------------------
# Daemon-level methods
# ---------------------------------------------------------------------------


class TestDaemonMethods:
    def setup_method(self):
        self.daemon = make_daemon()

    def test_ping_returns_pong(self):
        assert self.daemon.ping() == "pong"

    def test_echo_returns_payload(self):
        assert self.daemon.echo("hello") == "hello"
        assert self.daemon.echo({"key": 42}) == {"key": 42}

    def test_on_site_stop_fallback_is_noop(self):
        # Should not raise; daemon-level fallback for calls without sitename
        self.daemon.on_site_stop()

    def test_get_site_returns_none_for_unknown(self):
        assert self.daemon.getSite(sitename="nope") is None

    def test_site_registers_empty_initially(self):
        assert self.daemon.siteRegisters() == []


class TestAddSiteRegister:
    def setup_method(self):
        self.daemon = make_daemon()

    @patch("genro_daemon.handler.GnrDaemon._start_site_processes")
    @patch("genro_daemon.handler.get_backend", return_value=InMemoryBackend())
    def test_add_creates_register(self, mock_backend, mock_procs):
        self.daemon.addSiteRegister("mysite")
        assert "mysite" in self.daemon._siteregisters

    @patch("genro_daemon.handler.GnrDaemon._start_site_processes")
    @patch("genro_daemon.handler.get_backend", return_value=InMemoryBackend())
    def test_add_is_idempotent(self, mock_backend, mock_procs):
        self.daemon.addSiteRegister("mysite")
        first = self.daemon._siteregisters["mysite"]
        self.daemon.addSiteRegister("mysite")
        assert self.daemon._siteregisters["mysite"] is first  # same object

    @patch("genro_daemon.handler.GnrDaemon._start_site_processes")
    @patch("genro_daemon.handler.get_backend", return_value=InMemoryBackend())
    def test_add_multiple_sites(self, mock_backend, mock_procs):
        mock_backend.side_effect = lambda *a, **kw: InMemoryBackend()
        self.daemon.addSiteRegister("site1")
        self.daemon.addSiteRegister("site2")
        assert "site1" in self.daemon._siteregisters
        assert "site2" in self.daemon._siteregisters

    @patch("genro_daemon.handler.GnrDaemon._start_site_processes")
    @patch("genro_daemon.handler.get_backend", return_value=InMemoryBackend())
    def test_get_site_after_add(self, mock_backend, mock_procs):
        self.daemon.addSiteRegister("mysite")
        result = self.daemon.getSite(sitename="mysite")
        assert result == {"status": "ready", "sitename": "mysite"}

    @patch("genro_daemon.handler.GnrDaemon._start_site_processes")
    @patch("genro_daemon.handler.get_backend", return_value=InMemoryBackend())
    def test_site_registers_lists_added_sites(self, mock_backend, mock_procs):
        self.daemon.addSiteRegister("alpha")
        entries = self.daemon.siteRegisters()
        names = [e[0] for e in entries]
        assert "alpha" in names


class TestSiteregisterStop:
    def setup_method(self):
        self.daemon = make_daemon()
        reg = make_register(self.daemon, "mysite")
        self.daemon._siteregisters["mysite"] = reg

    def test_stop_single_site(self):
        self.daemon.siteregister_stop(sitename="mysite")
        assert "mysite" not in self.daemon._siteregisters

    def test_stop_unknown_site_is_noop(self):
        self.daemon.siteregister_stop(sitename="ghost")  # no KeyError

    def test_stop_wildcard_removes_all(self):
        reg2 = make_register(self.daemon, "other")
        self.daemon._siteregisters["other"] = reg2
        self.daemon.siteregister_stop(sitename="*")
        assert len(self.daemon._siteregisters) == 0

    def test_stop_comma_separated(self):
        reg2 = make_register(self.daemon, "other")
        self.daemon._siteregisters["other"] = reg2
        self.daemon.siteregister_stop(sitename="mysite,other")
        assert "mysite" not in self.daemon._siteregisters
        assert "other" not in self.daemon._siteregisters


# ---------------------------------------------------------------------------
# Auto-create and storage_path update
# ---------------------------------------------------------------------------


class TestAutoCreateAndStoragePath:
    def setup_method(self):
        self.daemon = make_daemon()

    @patch("genro_daemon.handler.GnrDaemon._start_site_processes")
    @patch("genro_daemon.handler.get_backend", return_value=InMemoryBackend())
    def test_req_parse_auto_creates_register_for_unknown_sitename(
        self, mock_backend, mock_procs
    ):
        """First request with an unknown sitename auto-creates the register."""
        req = [Ars.REQ, 1, "ping", [], {"_sitename": "new_site"}]
        self.daemon._req_parse(req)
        assert "new_site" in self.daemon._siteregisters

    @patch("genro_daemon.handler.GnrDaemon._start_site_processes")
    @patch("genro_daemon.handler.get_backend", return_value=InMemoryBackend())
    def test_req_parse_no_auto_create_for_add_site_register(
        self, mock_backend, mock_procs
    ):
        """addSiteRegister itself must not trigger an extra auto-create loop."""
        req = [Ars.REQ, 1, "addSiteRegister", [], {"_sitename": "fresh_site"}]
        call_id, method, args, kwargs, name, sitename = self.daemon._req_parse(req)
        # routed to daemon.addSiteRegister, NOT auto-created beforehand
        assert method == self.daemon.addSiteRegister

    @patch("genro_daemon.handler.GnrDaemon._start_site_processes")
    @patch("genro_daemon.handler.get_backend", return_value=InMemoryBackend())
    def test_add_site_register_updates_storage_path_on_existing(
        self, mock_backend, mock_procs, tmp_path
    ):
        """If register already exists but has no storage_path, the explicit
        addSiteRegister call from SiteRegisterClient should set it."""
        # First: auto-create (no storage_path)
        req = [Ars.REQ, 1, "ping", [], {"_sitename": "sp_site"}]
        self.daemon._req_parse(req)
        register = self.daemon._siteregisters["sp_site"]
        assert register.storage_path is None

        # Then: explicit addSiteRegister with storage_path
        path = str(tmp_path / "register.pik")
        self.daemon.addSiteRegister("sp_site", storage_path=path)
        assert register.storage_path == path

    @patch("genro_daemon.handler.GnrDaemon._start_site_processes")
    @patch("genro_daemon.handler.get_backend", return_value=InMemoryBackend())
    def test_private_method_still_raises_with_sitename(self, mock_backend, mock_procs):
        req = [Ars.REQ, 1, "_secret", [], {"_sitename": "some_site"}]
        with pytest.raises(GnrDaemonMethodNotFound):
            self.daemon._req_parse(req)
