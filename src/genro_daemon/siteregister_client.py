import _thread
import os
import os.path
import time

from gnr.core.gnrbag import Bag
from gnr.web import logger

from .client import GnrDaemonClient
from .exceptions import GnrDaemonLocked, GnrDaemonMethodNotFound

LOCK_MAX_RETRY = 50
RETRY_DELAY = 0.05  # initial delay; doubles each retry up to RETRY_DELAY_MAX
RETRY_DELAY_MAX = 2.0
_BAG_INSTANCE = Bag()

DEFAULT_PAGE_MAX_AGE = 120


class ServerStore:
    def __init__(
        self,
        parent,
        register_name=None,
        register_item_id=None,
        triggered=True,
        max_retry=None,
        retry_delay=None,
    ):
        self.siteregister = parent
        self.register_name = register_name
        self.register_item_id = register_item_id
        self.triggered = triggered
        self.max_retry = max_retry or LOCK_MAX_RETRY
        self.retry_delay = retry_delay or RETRY_DELAY
        self._register_item = "*"
        self.thread_id = _thread.get_ident()

    def __enter__(self):
        self.start_locking_time = time.time()
        delay = self.retry_delay
        for attempt in range(self.max_retry + 1):
            if self.siteregister.lock_item(
                self.register_item_id,
                reason=self.thread_id,
                register_name=self.register_name,
            ):
                self.success_locking_time = time.time()
                return self
            if attempt < self.max_retry:
                time.sleep(delay)
                delay = min(delay * 2, RETRY_DELAY_MAX)
        logger.error(
            "Unable to lock store after %d attempts: %s item %s",
            self.max_retry,
            self.register_name,
            self.register_item_id,
        )
        raise GnrDaemonLocked(
            f"Lock timed out after {self.max_retry} retries for "
            f"{self.register_name!r} item {self.register_item_id!r}"
        )

    def __exit__(self, type, value, tb):
        self.siteregister.unlock_item(
            self.register_item_id,
            reason=self.thread_id,
            register_name=self.register_name,
        )

    def reset_datachanges(self):
        return self.siteregister.reset_datachanges(
            self.register_item_id, register_name=self.register_name
        )

    def set_datachange(
        self,
        path,
        value=None,
        attributes=None,
        fired=False,
        reason=None,
        replace=False,
        delete=False,
    ):
        return self.siteregister.set_datachange(
            self.register_item_id,
            path,
            value=value,
            attributes=attributes,
            fired=fired,
            reason=reason,
            replace=replace,
            delete=delete,
            register_name=self.register_name,
        )

    def drop_datachanges(self, path):
        self.siteregister.drop_datachanges(
            self.register_item_id, path, register_name=self.register_name
        )

    def subscribe_path(self, path):
        self.siteregister.subscribe_path(
            self.register_item_id, path, register_name=self.register_name
        )

    @property
    def register_item(self):
        return self.siteregister.get_item(
            self.register_item_id, include_data="lazy", register_name=self.register_name
        )

    @property
    def data(self):
        if self.register_item:
            return self.register_item["data"]

    @property
    def datachanges(self):
        return self.register_item["datachanges"]

    @property
    def subscribed_paths(self):
        return self.register_item["subscribed_paths"]

    def __getattr__(self, fname):
        if hasattr(_BAG_INSTANCE, fname):

            def decore(*args, **kwargs):
                data = self.data
                if data is not None:
                    return getattr(data, fname)(*args, **kwargs)

            return decore
        else:
            raise AttributeError(f"register_item has no attribute '{fname}'")


class RemoteStoreBag:
    """Client-side proxy for a per-item Bag stored in the site register.

    Uses :class:`~genro_daemon.client.GnrDaemonClient` to call methods on the
    :class:`RemoteStoreBagHandler` that lives inside the site register server.
    Each attribute access results in a network round-trip.
    """

    def __init__(self, client, register_name, register_item_id, rootpath=None):
        # store in __dict__ to avoid hitting __getattr__
        object.__setattr__(self, "_client", client)
        object.__setattr__(self, "register_name", register_name)
        object.__setattr__(self, "register_item_id", register_item_id)
        object.__setattr__(self, "rootpath", rootpath)

    def chunk(self, path):
        return RemoteStoreBag(
            object.__getattribute__(self, "_client"),
            object.__getattribute__(self, "register_name"),
            object.__getattribute__(self, "register_item_id"),
            rootpath=path,
        )

    def __getitem__(self, path):
        return self.getItem(path)

    def __setitem__(self, path, value):
        self.setItem(path, value)

    def __getattr__(self, name):
        client = object.__getattribute__(self, "_client")
        register_name = object.__getattribute__(self, "register_name")
        register_item_id = object.__getattribute__(self, "register_item_id")
        rootpath = object.__getattribute__(self, "rootpath")

        def decore(*args, **kwargs):
            kwargs["_siteregister_register_name"] = register_name
            kwargs["_siteregister_register_item_id"] = register_item_id
            if rootpath:
                kwargs["_pyrosubbag"] = rootpath
            return client._invoke_method(f"remotebag_{name}", *args, **kwargs)

        return decore


class SiteRegisterClient:
    """Client-side façade for the site register hosted inside GnrDaemon.

    Mirrors the interface of the old Pyro4-based ``SiteRegisterClient`` so that
    :mod:`gnr.web.gnrwsgisite` and related modules work without modification.

    All calls are routed to a single GnrDaemon port; the ``sitename`` carried
    by the client is injected as ``_sitename`` into every request so the daemon
    can dispatch to the correct in-process :class:`GnrSiteRegister`.
    """

    STORAGE_PATH = "siteregister_data.pik"
    DEFAULT_POOL_SIZE = 16

    def __init__(self, site):
        self.locked_exception = GnrDaemonLocked
        self.site = site
        self.storage_path = os.path.join(self.site.site_path, self.STORAGE_PATH)

        daemonconfig = self.site.config.getAttr("gnrdaemon")
        daemon_uri = "gnr://{host}:{port}".format(**daemonconfig)
        pool_size = int(daemonconfig.get("pool_size") or self.DEFAULT_POOL_SIZE)
        logger.info(
            "Connecting to daemon at %s for site %r (pool_size=%d)",
            daemon_uri,
            site.site_name,
            pool_size,
        )

        # Single client; sitename is auto-injected into every call
        self.siteregister = GnrDaemonClient(
            daemon_uri, sitename=site.site_name, pool_size=pool_size
        )
        # Daemon-level client (no sitename) used by pages like onering.py
        self.gnrdaemon_proxy = GnrDaemonClient(daemon_uri, pool_size=pool_size)

        # Ensure the register namespace exists in the daemon
        self._register_with_daemon(autorestore=True)

    # ------------------------------------------------------------------
    # Registration helpers
    # ------------------------------------------------------------------

    def _register_with_daemon(self, autorestore=False):
        """Register (or re-register) this site with the daemon."""
        self.siteregister.addSiteRegister(
            self.site.site_name,
            storage_path=self.storage_path,
            autorestore=autorestore,
        )
        self.siteregister.setConfiguration(
            cleanup=self.site.custom_config.getAttr("cleanup")
        )

    def _sr_call(self, method_name, *args, **kwargs):
        """Call a method on the site register, reconnecting once if the daemon
        was restarted and lost the register state."""
        try:
            result = getattr(self.siteregister, method_name)(*args, **kwargs)
        except GnrDaemonMethodNotFound:
            logger.warning(
                "Daemon lost site register for %r; re-registering and retrying %s",
                self.site.site_name,
                method_name,
            )
            self._register_with_daemon(autorestore=False)
            result = getattr(self.siteregister, method_name)(*args, **kwargs)
        return result

    # ------------------------------------------------------------------
    # Page / connection / user management
    # ------------------------------------------------------------------

    def new_page(self, page_id, page, data=None):
        register_item = self._sr_call(
            "new_page",
            page_id,
            pagename=page.pagename,
            connection_id=page.connection_id,
            user=page.user,
            user_ip=page.user_ip,
            user_agent=page.user_agent,
            relative_url=page.request.path_info,
            data=data,
        )
        self._add_data_to_register_item(register_item)
        return register_item

    def new_connection(self, connection_id, connection):
        register_item = self._sr_call(
            "new_connection",
            connection_id,
            connection_name=connection.connection_name,
            user=connection.user,
            user_id=connection.user_id,
            user_tags=connection.user_tags,
            user_ip=connection.ip,
            browser_name=connection.browser_name,
            user_agent=connection.user_agent,
            avatar_extra=connection.avatar_extra,
            electron_static=connection.electron_static,
        )
        self._add_data_to_register_item(register_item)
        return register_item

    def pages(
        self,
        connection_id=None,
        user=None,
        index_name=None,
        filters=None,
        include_data=None,
    ):
        lazy_data = include_data == "lazy"
        if lazy_data:
            include_data = False
        pages = self._sr_call(
            "pages",
            connection_id=connection_id,
            user=user,
            index_name=index_name,
            filters=filters,
            include_data=include_data,
        )
        return self._adapt_list_to_dict(pages, lazy_data=lazy_data)

    def connections(self, user=None, include_data=None):
        lazy_data = include_data == "lazy"
        if lazy_data:
            include_data = False
        connections = self._sr_call("connections", user=user, include_data=include_data)
        return self._adapt_list_to_dict(connections, lazy_data=lazy_data)

    def users(self, include_data=None):
        lazy_data = include_data == "lazy"
        if lazy_data:
            include_data = False
        users = self._sr_call("users", include_data=include_data)
        return self._adapt_list_to_dict(users, lazy_data=lazy_data)

    def counters(self):
        return {
            "users": len(self.users()),
            "connections": len(self.connections()),
            "pages": len(self.pages()),
        }

    def refresh(self, page_id, ts=None, lastRpc=None, pageProfilers=None):
        return self._sr_call(
            "refresh",
            page_id,
            last_user_ts=ts,
            last_rpc_ts=lastRpc,
            pageProfilers=pageProfilers,
        )

    # ------------------------------------------------------------------
    # Store access (context-manager based)
    # ------------------------------------------------------------------

    def connectionStore(self, connection_id, triggered=False):
        return self._make_store("connection", connection_id, triggered=triggered)

    def userStore(self, user, triggered=False):
        return self._make_store("user", user, triggered=triggered)

    def pageStore(self, page_id, triggered=False):
        return self._make_store("page", page_id, triggered=triggered)

    def globalStore(self, triggered=False):
        return self._make_store("global", "*", triggered=triggered)

    def _make_store(self, register_name, register_item_id, triggered=None):
        return ServerStore(
            self,
            register_name,
            register_item_id=register_item_id,
            triggered=triggered,
        )

    # ------------------------------------------------------------------
    # Item retrieval
    # ------------------------------------------------------------------

    def get_item(self, register_item_id, include_data=False, register_name=None):
        lazy_data = include_data == "lazy"
        if lazy_data:
            include_data = False
        register_item = self._sr_call(
            "get_item",
            register_item_id,
            include_data=include_data,
            register_name=register_name,
        )
        if register_item and lazy_data:
            self._add_data_to_register_item(register_item)
        return register_item

    def page(self, page_id, include_data=None):
        return self.get_item(page_id, include_data=include_data, register_name="page")

    def connection(self, connection_id, include_data=None):
        return self.get_item(
            connection_id, include_data=include_data, register_name="connection"
        )

    def user(self, user, include_data=None):
        return self.get_item(user, include_data=include_data, register_name="user")

    # ------------------------------------------------------------------
    # Delegate lock / unlock to siteregister (used by ServerStore)
    # ------------------------------------------------------------------

    def lock_item(self, register_item_id, reason=None, register_name=None):
        return self._sr_call(
            "lock_item", register_item_id, reason=reason, register_name=register_name
        )

    def unlock_item(self, register_item_id, reason=None, register_name=None):
        return self._sr_call(
            "unlock_item", register_item_id, reason=reason, register_name=register_name
        )

    # ------------------------------------------------------------------
    # Dump / load
    # ------------------------------------------------------------------

    def dump(self):
        self.siteregister.dump()
        logger.info("DUMP REGISTER %s", self.site.site_name)

    def load(self):
        result = self.siteregister.load()
        if result:
            logger.info("SITEREGISTER %s LOADED", self.site.site_name)
        else:
            logger.warning("UNABLE TO LOAD REGISTER %s", self.site.site_name)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _adapt_list_to_dict(self, lst, lazy_data=None):
        return {
            c["register_item_id"]: (
                self._add_data_to_register_item(c) if lazy_data else c
            )
            for c in lst
        }

    def _add_data_to_register_item(self, register_item):
        register_item["data"] = RemoteStoreBag(
            self.siteregister,
            register_item["register_name"],
            register_item["register_item_id"],
        )
        return register_item

    def pyroProxy(self, register_uri):
        """Return a context manager yielding a site-specific client.

        Replaces the old ``Pyro4.Proxy(register_uri)`` pattern.  In the new
        single-port daemon, ``register_uri`` is just the sitename string stored
        by ``GnrDaemon.siteRegisters()``.
        """
        from .client import _SiteRegisterProxyContext

        return _SiteRegisterProxyContext(
            self.gnrdaemon_proxy._host,
            self.gnrdaemon_proxy._port,
            register_uri,
        )

    # ------------------------------------------------------------------
    # Catch-all delegation
    # ------------------------------------------------------------------

    def __getattr__(self, name):
        h = getattr(self.siteregister, name)
        if not callable(h):
            return h

        def decore(*args, **kwargs):
            return self._sr_call(name, *args, **kwargs)

        return decore
