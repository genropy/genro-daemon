import asyncio
import os
import os.path

import uvloop

uvloop.install()

from gnr.app.gnrdeploy import PathResolver  # noqa: E402
from gnr.core.gnrbag import Bag  # noqa: E402
from gnr.web import logger  # noqa: E402

from .ars import Ars  # noqa: E402
from .client import GnrDaemonClient  # noqa: E402
from .siteregister import GnrSiteRegister  # noqa: E402
from .storage import get_backend  # noqa: E402


class GnrSiteRegisterServer(Ars):
    """Ars-based daemon that manages the site-level register.

    Each site has one ``GnrSiteRegisterServer`` process.  It holds the
    :class:`~genro_daemon.siteregister.GnrSiteRegister` in memory and exposes
    all its public methods via the Ars msgpack protocol.

    Storage backend selection
    -------------------------
    Pass ``storage_backend='redis'`` (and optionally ``redis={…}``) in the
    site configuration to use :class:`~genro_daemon.storage.redis.RedisBackend`
    instead of the default in-memory store.
    """

    def __init__(self, loop=None, sitename=None, **kwargs):
        if not loop:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        super().__init__(loop=loop)

        self.sitename = sitename
        self._config = {}
        self._get_config(sitename)

        logger.info(f"Site daemon for {sitename}")

        self.gnr_daemon_uri = kwargs.get("daemon_uri", self._config.get("daemon_uri"))
        self.debug = kwargs.get("debug", self._config.get("debug", False))
        self.storage_path = kwargs.get("storage_path", self._config.get("storage_path"))
        self._running = False

    def _get_config(self, sitename):
        path_resolver = PathResolver()
        siteconfig = path_resolver.get_siteconfig(sitename)
        sitedaemonconfig = siteconfig.getAttr("sitedaemon") or {}
        sitepath = path_resolver.site_name_to_path(sitename)
        debug = sitedaemonconfig.get("debug", None)
        host = sitedaemonconfig.get("host", "localhost")
        port = sitedaemonconfig.get("port", "*")
        storage_path = os.path.join(sitepath, "siteregister_data.pik")
        storage_backend = sitedaemonconfig.get("storage_backend", "memory")
        redis_config = siteconfig.getAttr("redis") or {}
        self._config.update(
            debug=debug,
            host=host,
            port=port,
            sitepath=sitepath,
            storage_path=storage_path,
            storage_backend=storage_backend,
            redis=redis_config,
        )

    def running(self):
        return self._running

    def stop(self, saveStatus=False, reason="Automatic", **kw):
        logger.info(
            f"Stopping site daemon for {self.sitename}, saving status {saveStatus}"
        )
        if saveStatus:
            logger.info(f"Saving status into {self.storage_path}")
            self.siteregister.dump()
            logger.info("Completed status saving")
        self._running = False
        super().stop(reason=reason)

    def run(self, autorestore=False):
        self._running = True
        if autorestore:
            self.siteregister.load()
        logger.info(f"Starting site daemon for {self.sitename}")
        super().start(**self._config)

    def start(
        self,
        host=None,
        port=None,
        compression=None,
        multiplex=None,
        timeout=None,
        polltimeout=None,
        autorestore=False,
        run_now=True,
        **kwargs,
    ):
        backend = get_backend(self._config, sitename=self.sitename)
        self.siteregister = GnrSiteRegister(
            self,
            sitename=self.sitename,
            storage_path=self.storage_path,
            backend=backend,
        )

        autorestore = autorestore and os.path.exists(self.storage_path or "")
        sp_found = os.path.exists(self.storage_path or "")
        logger.info(
            f"Auto-restoring data: {autorestore}, "
            f"storage path {self.storage_path} exists: {sp_found}"
        )

        # Build listening URI
        _host = host or self._config.get("host", "localhost")
        _port = port or self._config.get("port", 0)
        self.main_uri = f"gnr://{_host}:{_port}"
        self.register_uri = f"gnr://{_host}:{_port}"

        if self.gnr_daemon_uri:
            gclient = GnrDaemonClient(self.gnr_daemon_uri)
            gclient.onRegisterStart(
                self.sitename,
                server_uri=self.main_uri,
                register_uri=self.register_uri,
            )

        # Persist process info so the client can find us without the main daemon
        sitedaemon_xml_path = os.path.join(self._config["sitepath"], "sitedaemon.xml")
        sitedaemon_bag = Bag()
        sitedaemon_bag.setItem(
            "params",
            None,
            register_uri=self.register_uri,
            main_uri=self.main_uri,
            pid=os.getpid(),
        )
        sitedaemon_bag.toXml(sitedaemon_xml_path)

        if run_now:
            self.run(autorestore=autorestore)

    def __getattr__(self, name):
        """Delegate unknown public attribute lookups to the siteregister.

        This makes all public methods of :class:`GnrSiteRegister` accessible
        via the Ars protocol without listing them explicitly.
        """
        if name.startswith("_"):
            raise AttributeError(name)
        sr = self.__dict__.get("siteregister")
        if sr is None:
            raise AttributeError(name)
        attr = getattr(sr, name, None)
        if attr is None:
            raise AttributeError(name)
        return attr

    def remotebag_handler_call(self, method_name, *args, **kwargs):
        """Route ``remotebag_*`` calls to the RemoteStoreBagHandler."""
        return getattr(self.siteregister.remotebag_handler, method_name)(
            *args, **kwargs
        )

    def ping(self, **kwargs):
        return "pong"
