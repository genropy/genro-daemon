import os
import time
import urllib.request
from multiprocessing import get_logger

from gnr.web.gnrtask import GnrTaskScheduler

from .sitedaemon import GnrSiteRegisterServer


class GnrHeartBeat:
    def __init__(self, site_url=None, interval=None, loglevel=None, **kwargs):
        self.interval = interval
        self.site_url = site_url
        self.url = f"{self.site_url}/sys/heartbeat"
        self.logger = get_logger()

    def start(self):
        os.environ["no_proxy"] = "*"
        while True:
            try:
                self.logger.info(f"Calling {self.url}")
                response = urllib.request.urlopen(self.url)
                response_code = response.getcode()
                if response_code != 200:
                    self.retry(f"WRONG CODE {response_code}")
                else:
                    time.sleep(self.interval)
            except OSError:
                self.retry("IOError")
            except Exception as e:
                self.logger.error(str(e))

    def retry(self, reason):
        self.logger.warning(f"{reason} -> will retry in {3 * self.interval} seconds")
        time.sleep(3 * self.interval)


def createSiteRegisterDaemon(
    sitename=None,
    daemon_uri=None,
    host=None,
    port=None,
    socket=None,
    storage_path=None,
    debug=None,
    autorestore=False,
):
    server = GnrSiteRegisterServer(
        sitename=sitename,
        daemon_uri=daemon_uri,
        storage_path=storage_path,
        debug=debug,
    )
    server.start(host=host, port=port, autorestore=autorestore)


def createHeartBeat(site_url=None, interval=None, **kwargs):
    server = GnrHeartBeat(site_url=site_url, interval=interval, **kwargs)
    time.sleep(interval)
    server.start()


def createTaskScheduler(sitename, interval=None):
    scheduler = GnrTaskScheduler(sitename, interval=interval)
    scheduler.start()
