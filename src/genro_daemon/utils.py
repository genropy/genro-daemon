import os
import os.path

from gnr.core.gnrbag import Bag
from gnr.core.gnrconfig import gnrConfigPath
from gnr.core.gnrsys import expandpath

# Environment variables for network/connection settings.
# Precedence: CLI options > env vars > environment.xml > built-in defaults.
_ENV_HOST = "GNR_DAEMON_HOST"
_ENV_PORT = "GNR_DAEMON_PORT"
_ENV_BIND = "GNR_DAEMON_BIND"


def load_daemon_options(options=None):
    gnr_path = gnrConfigPath()
    enviroment_path = os.path.join(gnr_path, "environment.xml")
    env_options = Bag(expandpath(enviroment_path)).getAttr("gnrdaemon")
    if env_options.get("sockets"):
        if env_options["sockets"].lower() in ("t", "true", "y"):
            env_options["sockets"] = os.path.join(gnr_path, "sockets")
        if not os.path.isdir(env_options["sockets"]):
            os.makedirs(env_options["sockets"])
        env_options["socket"] = env_options.get("socket") or os.path.join(
            env_options["sockets"], "gnrdaemon.sock"
        )
    assert env_options, "Missing gnrdaemon configuration."
    # Environment variables override XML config values.
    if os.environ.get(_ENV_HOST):
        env_options["host"] = os.environ[_ENV_HOST]
    if os.environ.get(_ENV_PORT):
        env_options["port"] = os.environ[_ENV_PORT]
    if os.environ.get(_ENV_BIND):
        env_options["bind"] = os.environ[_ENV_BIND]
    # Explicit CLI options (options dict) override everything.
    for k, v in list(options.items()):
        if v:
            env_options[k] = v
    return env_options
