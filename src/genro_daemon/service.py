import asyncio

import uvloop  # noqa: F401 – installs uvloop event loop policy
from gnr.web import logger

from .handler import GnrDaemon, GnrDaemonProxy

logger.info("Using new daemon implementation")


class DaemonService:
    """
    Object to control the daemon service
    """

    def __init__(self, options, command=None, sitename=None):
        self.options = options
        self.command = command
        self.sitename = sitename

    def run(self):
        if self.command == "start" or not self.command:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            logger.debug("Starting main daemon")
            server = GnrDaemon(loop=loop)
            server.start(use_environment=True, **self.options)
        else:
            gdc = GnrDaemonProxy(
                use_environment=True,
                host=self.options.get("host"),
                port=self.options.get("port"),
                hmac_key=self.options.get("hmac_key"),
            )
            if self.command == "stop":
                result = gdc.stop(saveStatus=self.options.get("savestatus"))
                if result:
                    print(result)

            elif self.command == "restart":
                result = gdc.restart(sitename="*")
            else:
                result = getattr(gdc, self.command)()
                if result:
                    print(result)
