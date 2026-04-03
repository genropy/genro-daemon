"""Tests for genro_daemon.utils – load_daemon_options."""

from unittest.mock import MagicMock, patch


class TestLoadDaemonOptions:
    def _make_bag(self, data):
        """Return a dict-like mock that simulates Bag.getAttr behaviour."""
        bag = dict(data)
        return bag

    def _patch_gnr(self, gnr_path="/tmp/gnr", env_options=None, is_dir=True):
        """Patch gnr imports needed by load_daemon_options."""
        if env_options is None:
            env_options = {"host": "localhost", "port": "40404"}

        mock_bag_instance = MagicMock()
        mock_bag_instance.getAttr.return_value = env_options
        mock_bag_cls = MagicMock(return_value=mock_bag_instance)

        patches = [
            patch("genro_daemon.utils.gnrConfigPath", return_value=gnr_path),
            patch("genro_daemon.utils.expandpath", side_effect=lambda p: p),
            patch("genro_daemon.utils.Bag", mock_bag_cls),
            patch("os.path.isdir", return_value=is_dir),
            patch("os.makedirs"),
        ]
        return patches, env_options

    def test_basic_options_returned(self):
        patches, env_options = self._patch_gnr(
            env_options={"host": "localhost", "port": "40404"}
        )
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            from genro_daemon.utils import load_daemon_options

            result = load_daemon_options(options={})
        assert result["host"] == "localhost"

    def test_caller_options_override_env(self):
        patches, env_options = self._patch_gnr(
            env_options={"host": "localhost", "port": "40404"}
        )
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            from genro_daemon.utils import load_daemon_options

            result = load_daemon_options(options={"host": "remotehost"})
        assert result["host"] == "remotehost"

    def test_falsy_caller_options_not_applied(self):
        patches, env_options = self._patch_gnr(
            env_options={"host": "localhost", "port": "40404"}
        )
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            from genro_daemon.utils import load_daemon_options

            result = load_daemon_options(options={"host": None})
        assert result["host"] == "localhost"

    def test_sockets_as_path_kept(self):
        env_options = {"host": "localhost", "port": "40404", "sockets": "/tmp/socks"}
        patches, _ = self._patch_gnr(env_options=env_options, is_dir=True)
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            from genro_daemon.utils import load_daemon_options

            result = load_daemon_options(options={})
        assert "socket" in result

    def test_sockets_as_true_converted_to_path(self):
        env_options = {"host": "localhost", "port": "40404", "sockets": "true"}
        patches, _ = self._patch_gnr(
            gnr_path="/tmp/gnr", env_options=env_options, is_dir=True
        )
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            from genro_daemon.utils import load_daemon_options

            result = load_daemon_options(options={})
        assert result["sockets"] == "/tmp/gnr/sockets"

    def test_sockets_dir_created_when_missing(self):
        env_options = {"host": "localhost", "port": "40404", "sockets": "/tmp/socks"}
        patches, _ = self._patch_gnr(env_options=env_options, is_dir=False)
        with (
            patches[0],
            patches[1],
            patches[2],
            patches[3],
            patches[4] as mock_makedirs,
        ):
            from genro_daemon.utils import load_daemon_options

            load_daemon_options(options={})
        mock_makedirs.assert_called_once_with("/tmp/socks")

    def test_socket_path_set_when_missing(self):
        env_options = {"host": "localhost", "port": "40404", "sockets": "/tmp/socks"}
        patches, _ = self._patch_gnr(env_options=env_options, is_dir=True)
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            from genro_daemon.utils import load_daemon_options

            result = load_daemon_options(options={})
        assert result["socket"] == "/tmp/socks/gnrdaemon.sock"

    def test_socket_existing_path_preserved(self):
        env_options = {
            "host": "localhost",
            "port": "40404",
            "sockets": "/tmp/socks",
            "socket": "/tmp/socks/custom.sock",
        }
        patches, _ = self._patch_gnr(env_options=env_options, is_dir=True)
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            from genro_daemon.utils import load_daemon_options

            result = load_daemon_options(options={})
        assert result["socket"] == "/tmp/socks/custom.sock"

    def test_env_var_host_overrides_xml(self, monkeypatch):
        monkeypatch.setenv("GNR_DAEMON_HOST", "envhost")
        patches, _ = self._patch_gnr(env_options={"host": "xmlhost", "port": "40404"})
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            from genro_daemon.utils import load_daemon_options

            result = load_daemon_options(options={})
        assert result["host"] == "envhost"

    def test_env_var_port_overrides_xml(self, monkeypatch):
        monkeypatch.setenv("GNR_DAEMON_PORT", "9999")
        patches, _ = self._patch_gnr(env_options={"host": "localhost", "port": "40404"})
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            from genro_daemon.utils import load_daemon_options

            result = load_daemon_options(options={})
        assert result["port"] == "9999"

    def test_env_var_bind_sets_bind_key(self, monkeypatch):
        monkeypatch.setenv("GNR_DAEMON_BIND", "0.0.0.0")
        patches, _ = self._patch_gnr(env_options={"host": "localhost", "port": "40404"})
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            from genro_daemon.utils import load_daemon_options

            result = load_daemon_options(options={})
        assert result["bind"] == "0.0.0.0"

    def test_cli_option_overrides_env_var(self, monkeypatch):
        monkeypatch.setenv("GNR_DAEMON_HOST", "envhost")
        patches, _ = self._patch_gnr(env_options={"host": "xmlhost", "port": "40404"})
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            from genro_daemon.utils import load_daemon_options

            result = load_daemon_options(options={"host": "clihost"})
        assert result["host"] == "clihost"
