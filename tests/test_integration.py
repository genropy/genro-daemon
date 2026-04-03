"""Integration tests: full daemon + client stack over a real TCP socket."""

from unittest.mock import patch

import pytest

from genro_daemon.exceptions import GnrDaemonMethodNotFound
from genro_daemon.storage.memory import InMemoryBackend

# All tests in this module use the daemon_runner fixture from conftest.py.
# The fixture starts GnrDaemon on a random port and tears it down after each test.


# ---------------------------------------------------------------------------
# Basic connectivity
# ---------------------------------------------------------------------------


class TestConnectivity:
    def test_ping(self, daemon_client):
        assert daemon_client.ping() == "pong"

    def test_echo_string(self, daemon_client):
        assert daemon_client.echo("hello") == "hello"

    def test_echo_int(self, daemon_client):
        assert daemon_client.echo(42) == 42

    def test_echo_dict(self, daemon_client):
        d = {"a": 1, "b": [2, 3]}
        assert daemon_client.echo(d) == d

    def test_echo_none(self, daemon_client):
        assert daemon_client.echo(None) is None

    def test_sequential_calls(self, daemon_client):
        for _ in range(10):
            assert daemon_client.ping() == "pong"

    def test_connection_error_returns_none(self):
        """Calling a client pointing at a dead port returns None gracefully."""
        from genro_daemon.client import GnrDaemonClient

        c = GnrDaemonClient("gnr://127.0.0.1:1", timeout=0.5)
        result = c.ping()
        assert result is None


# ---------------------------------------------------------------------------
# Error propagation
# ---------------------------------------------------------------------------


class TestErrorPropagation:
    def test_unknown_method_raises_exception(self, daemon_client):
        """Calling an unknown daemon method raises the remote exception locally."""
        with pytest.raises(GnrDaemonMethodNotFound):
            daemon_client.no_such_method_xyz()

    def test_private_method_raises_exception(self, daemon_client):
        with pytest.raises(GnrDaemonMethodNotFound):
            daemon_client._internal()


# ---------------------------------------------------------------------------
# Site register lifecycle
# ---------------------------------------------------------------------------


class TestSiteRegisterLifecycle:
    @patch("genro_daemon.handler.GnrDaemon._start_site_processes")
    @patch("genro_daemon.handler.get_backend", return_value=InMemoryBackend())
    def test_add_site_register(self, mock_backend, mock_procs, daemon_runner):
        client = daemon_runner.client()
        client.addSiteRegister("mysite")
        status = client.getSite(sitename="mysite")
        assert status["sitename"] == "mysite"

    @patch("genro_daemon.handler.GnrDaemon._start_site_processes")
    @patch("genro_daemon.handler.get_backend")
    def test_site_methods_after_register(self, mock_backend, mock_procs, daemon_runner):
        mock_backend.return_value = InMemoryBackend()
        site_client = daemon_runner.client(sitename="s1")
        site_client.addSiteRegister("s1")
        site_client.setConfiguration()
        # new_connection should not raise
        site_client.new_connection("c1", user="alice", user_id="u1")
        conns = site_client.connections()
        assert any(c["register_item_id"] == "c1" for c in conns)

    @patch("genro_daemon.handler.GnrDaemon._start_site_processes")
    @patch("genro_daemon.handler.get_backend")
    def test_new_page_and_counters(self, mock_backend, mock_procs, daemon_runner):
        mock_backend.return_value = InMemoryBackend()
        sc = daemon_runner.client(sitename="s2")
        sc.addSiteRegister("s2")
        sc.setConfiguration()
        sc.new_connection("c1", user="bob")
        sc.new_page("p1", pagename="home.py", connection_id="c1", user="bob")
        counters = sc.counters()
        assert counters["pages"] == 1
        assert counters["connections"] == 1

    @patch("genro_daemon.handler.GnrDaemon._start_site_processes")
    @patch("genro_daemon.handler.get_backend")
    def test_multiple_sites_isolated(self, mock_backend, mock_procs, daemon_runner):
        mock_backend.side_effect = lambda *a, **kw: InMemoryBackend()
        sc_a = daemon_runner.client(sitename="site_a")
        sc_b = daemon_runner.client(sitename="site_b")
        sc_a.addSiteRegister("site_a")
        sc_b.addSiteRegister("site_b")
        sc_a.setConfiguration()
        sc_b.setConfiguration()
        sc_a.new_connection("c_a", user="alice")
        sc_b.new_connection("c_b", user="bob")
        assert sc_a.counters()["connections"] == 1
        assert sc_b.counters()["connections"] == 1

    @patch("genro_daemon.handler.GnrDaemon._start_site_processes")
    @patch("genro_daemon.handler.get_backend")
    def test_site_registers_listing(self, mock_backend, mock_procs, daemon_runner):
        mock_backend.side_effect = lambda *a, **kw: InMemoryBackend()
        sc = daemon_runner.client()
        sc.addSiteRegister("alpha")
        sc.addSiteRegister("beta")
        entries = sc.siteRegisters()
        names = [e[0] for e in entries]
        assert "alpha" in names
        assert "beta" in names

    @patch("genro_daemon.handler.GnrDaemon._start_site_processes")
    @patch("genro_daemon.handler.get_backend")
    def test_siteregister_stop(self, mock_backend, mock_procs, daemon_runner):
        mock_backend.return_value = InMemoryBackend()
        sc = daemon_runner.client()
        sc.addSiteRegister("removable")
        sc.siteregister_stop(sitename="removable")
        assert sc.getSite(sitename="removable") is None

    @patch("genro_daemon.handler.GnrDaemon._start_site_processes")
    @patch("genro_daemon.handler.get_backend")
    def test_on_site_stop_without_sitename_does_not_crash(
        self, mock_backend, mock_procs, daemon_runner
    ):
        mock_backend.return_value = InMemoryBackend()
        # Call without _sitename – hits daemon-level fallback
        c = daemon_runner.client()  # no sitename
        # Should not raise; returns None
        c.on_site_stop()

    @patch("genro_daemon.handler.GnrDaemon._start_site_processes")
    @patch("genro_daemon.handler.get_backend")
    def test_on_site_stop_with_sitename_reaches_register(
        self, mock_backend, mock_procs, daemon_runner
    ):
        mock_backend.return_value = InMemoryBackend()
        sc = daemon_runner.client(sitename="stopdemo")
        sc.addSiteRegister("stopdemo")
        sc.on_site_stop()  # should route to register.on_site_stop, no error


# ---------------------------------------------------------------------------
# notifyDbEvents over the wire
# ---------------------------------------------------------------------------


class TestNotifyDbEvents:
    @patch("genro_daemon.handler.GnrDaemon._start_site_processes")
    @patch("genro_daemon.handler.get_backend")
    def test_notify_db_events_extra_kwargs_accepted(
        self, mock_backend, mock_procs, daemon_runner
    ):
        """register_name kwarg (passed by gnrwsgisite) must not cause TypeError."""
        mock_backend.return_value = InMemoryBackend()
        sc = daemon_runner.client(sitename="notify_site")
        sc.addSiteRegister("notify_site")
        sc.setConfiguration()
        sc.new_page("p1", subscribed_tables="lib.user", user="u", connection_id="c1")
        # gnrwsgisite passes register_name='page'; must be silently absorbed
        sc.notifyDbEvents(
            dbeventsDict={"lib.user": [{"action": "I", "pkey": "1"}]},
            register_name="page",
        )
