#!/usr/bin/env python

import os.path
import sys
from argparse import ArgumentParser

try:
    from gnr.web.daemon.service import DaemonService
except ModuleNotFoundError:
    # we're running locally!
    local_path = os.path.join(os.path.dirname(__file__), "src")
    sys.path.insert(0, local_path)
    from genro_daemon.service import DaemonService

description = "Main Genropy Daemon for request handling"


def main():
    parser = ArgumentParser(description=description)
    parser.add_argument("sitename", nargs="?")
    parser.add_argument("-C", "--command", help="Command")
    parser.add_argument("-H", "--host", help="The binded host")

    parser.add_argument("-P", "--port", help="The binded port", type=int)

    parser.add_argument("-S", "--socket", help="socket to use")

    parser.add_argument("-K", "--hmac_key", help="The secret key")

    parser.add_argument("-t", "--timeout", type=float, help="Timeout")

    parser.add_argument(
        "-m", "--multiplex", action="store_false", help="Use multiplexed server"
    )

    parser.add_argument(
        "--polltimeout", type=float, help="Use multiplexed server poll timeout"
    )

    parser.add_argument(
        "-c", "--compression", action="store_false", help="Enable compression"
    )

    parser.add_argument("-s", "--savestatus", action="store_true", help="Save status")

    parser.add_argument("-n", "--sitename", help="Sitename")

    parser.add_argument("-l", "--size_limit", type=int, help="Size limit")

    options = parser.parse_args().__dict__

    command = options.pop("command", None)
    sitename = options.pop("sitename", None)

    DaemonService(options, command, sitename).run()


if __name__ == "__main__":
    main()
