"""Site register orchestration.

This module exports :class:`GnrSiteRegister`, the top-level object that
coordinates the four per-site sub-registers (global, user, connection, page)
and implements the full public API used by the Genropy web framework.

Supporting building blocks live in:
- :mod:`genro_daemon.siteregister_base` — :class:`BaseRegister` and constants
- :mod:`genro_daemon.siteregister_registers` — concrete register classes and helpers
"""

from __future__ import annotations

import datetime
import os
import time
from typing import Any

from gnr.core.gnrbag import Bag
from gnr.core.gnrclasses import GnrClassCatalog
from gnr.web import logger

from . import metrics
from .siteregister_base import (
    DEFAULT_CLEANUP_INTERVAL,
    DEFAULT_CONNECTION_MAX_AGE,
    DEFAULT_GUEST_CONNECTION_MAX_AGE,
    DEFAULT_PAGE_MAX_AGE,
    LOCK_EXPIRY_SECONDS,
    LOCK_MAX_RETRY,
    PROCESS_SELFDESTROY_TIMEOUT,
    RETRY_DELAY,
    BaseRegister,
)
from .siteregister_registers import (
    ConnectionRegister,
    GlobalRegister,
    PageRegister,
    RegisterResolver,
    RemoteStoreBagHandler,
    UserRegister,
)
from .storage.base import StorageBackend

# Re-export constants and classes for backward-compatible imports
__all__ = [
    "GnrSiteRegister",
    "BaseRegister",
    "GlobalRegister",
    "UserRegister",
    "ConnectionRegister",
    "PageRegister",
    "RegisterResolver",
    "RemoteStoreBagHandler",
    # constants
    "DEFAULT_CLEANUP_INTERVAL",
    "DEFAULT_CONNECTION_MAX_AGE",
    "DEFAULT_GUEST_CONNECTION_MAX_AGE",
    "DEFAULT_PAGE_MAX_AGE",
    "LOCK_EXPIRY_SECONDS",
    "LOCK_MAX_RETRY",
    "PROCESS_SELFDESTROY_TIMEOUT",
    "RETRY_DELAY",
]


class GnrSiteRegister:
    """Top-level coordinator for a single Genropy site's session state.

    Owns four sub-registers — :attr:`global_register`, :attr:`user_register`,
    :attr:`connection_register`, and :attr:`page_register` — and delegates to
    them via a unified public API.  All public methods are callable over the
    Ars/msgpack protocol by the framework's site process.

    Lifecycle
    ---------
    1. Created by :meth:`GnrDaemon.addSiteRegister`.
    2. :meth:`setConfiguration` is called to apply per-site cleanup settings.
    3. The framework interacts via ``new_connection``, ``new_page``,
       ``refresh``, ``handle_ping``, etc.
    4. :meth:`cleanup` is driven by the framework's keep-alive cycle.
    """

    def __init__(
        self,
        server: Any,
        sitename: str | None = None,
        storage_path: str | None = None,
        backend: StorageBackend | None = None,
    ) -> None:
        self.server = server
        self._backend = backend
        self.global_register = GlobalRegister(self, backend=backend, sitename=sitename)
        self.page_register = PageRegister(self, backend=backend, sitename=sitename)
        self.connection_register = ConnectionRegister(
            self, backend=backend, sitename=sitename
        )
        self.user_register = UserRegister(self, backend=backend, sitename=sitename)
        self.remotebag_handler = RemoteStoreBagHandler(self)
        self.last_cleanup = time.time()
        self.sitename = sitename
        self.storage_path = storage_path
        self.catalog = GnrClassCatalog()
        self.maintenance = False
        self.allowed_users: list[str] | None = None
        self.interproces_commands: dict[int, dict] = dict()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def on_site_stop(self) -> None:
        logger.info("site %s stopped", self.sitename)

    def checkCachedTables(self, table: str) -> None:
        """Invalidate cached resolver data for *table* in all sub-registers."""
        for register in (
            self.page_register,
            self.connection_register,
            self.user_register,
        ):
            if table in register.cached_tables:
                register.invalidateTableCache(table)

    def setConfiguration(self, cleanup: dict | None = None) -> None:
        """Apply per-site cleanup timing parameters.

        Called once after creation (with values from ``environment.xml``) and
        may be called again to hot-reload configuration without a restart.
        """
        cleanup = cleanup or dict()
        self.cleanup_interval: int = int(
            cleanup.get("interval", DEFAULT_CLEANUP_INTERVAL)
        )
        self.page_max_age: int = int(cleanup.get("page_max_age", DEFAULT_PAGE_MAX_AGE))
        self.guest_connection_max_age: int = int(
            cleanup.get("guest_connection_max_age", DEFAULT_GUEST_CONNECTION_MAX_AGE)
        )
        self.connection_max_age: int = int(
            cleanup.get("connection_max_age", DEFAULT_CONNECTION_MAX_AGE)
        )

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def new_connection(
        self,
        connection_id: str,
        connection_name: str | None = None,
        user: str | None = None,
        user_id: str | None = None,
        user_name: str | None = None,
        user_tags: str | None = None,
        user_ip: str | None = None,
        user_agent: str | None = None,
        browser_name: str | None = None,
        avatar_extra: Any | None = None,
        electron_static: Any | None = None,
    ) -> dict:
        if self.connection_register.exists(connection_id):
            self.drop_connection(connection_id)
        if not self.user_register.exists(user):
            self.new_user(
                user,
                user_id=user_id,
                user_name=user_name,
                user_tags=user_tags,
                avatar_extra=avatar_extra,
            )
        connection_item = self.connection_register.create(
            connection_id,
            connection_name=connection_name,
            user=user,
            user_id=user_id,
            user_name=user_name,
            user_tags=user_tags,
            user_ip=user_ip,
            user_agent=user_agent,
            browser_name=browser_name,
            electron_static=electron_static,
        )
        m = metrics.get()
        if m:
            m.register_connections.labels(sitename=self.sitename).inc()
        return connection_item

    def drop_pages(self, connection_id: str) -> None:
        for page_id in self.connection_page_keys(connection_id):
            self.drop_page(page_id)

    def drop_page(self, page_id: str, cascade: bool | None = None) -> Any:
        result = self.page_register.drop(page_id, cascade=cascade)
        m = metrics.get()
        if m:
            m.register_pages.labels(sitename=self.sitename).dec()
        return result

    def drop_connections(self, user: str) -> None:
        for connection_id in self.user_connection_keys(user):
            self.drop_connection(connection_id)

    def drop_connection(self, connection_id: str, cascade: bool | None = None) -> None:
        self.connection_register.drop(connection_id, cascade=cascade)
        m = metrics.get()
        if m:
            m.register_connections.labels(sitename=self.sitename).dec()

    def drop_user(self, user: str) -> None:
        self.user_register.drop(user)
        m = metrics.get()
        if m:
            m.register_users.labels(sitename=self.sitename).dec()

    # ------------------------------------------------------------------
    # Index accessors
    # ------------------------------------------------------------------

    def user_connection_keys(self, user: str) -> list[str]:
        return self.connection_register.user_connection_keys(user)

    def user_connection_items(self, user: str) -> list[tuple[str, dict]]:
        return self.connection_register.user_connection_items(user)

    def user_connections(self, user: str) -> list[dict]:
        return self.connection_register.connections(user=user)

    def connection_page_keys(self, connection_id: str) -> list[str]:
        return self.page_register.connection_page_keys(connection_id=connection_id)

    def connection_page_items(self, connection_id: str) -> list[tuple[str, dict]]:
        return self.page_register.connection_page_items(connection_id=connection_id)

    def connection_pages(self, connection_id: str) -> list[dict]:
        return self.page_register.connection_pages(connection_id=connection_id)

    # ------------------------------------------------------------------
    # Page management
    # ------------------------------------------------------------------

    def new_page(
        self,
        page_id: str,
        pagename: str | None = None,
        connection_id: str | None = None,
        subscribed_tables: str | None = None,
        user: str | None = None,
        user_ip: str | None = None,
        user_agent: str | None = None,
        relative_url: str | None = None,
        data: dict | None = None,
    ) -> dict:
        page_item = self.page_register.create(
            page_id,
            pagename=pagename,
            connection_id=connection_id,
            user=user,
            user_ip=user_ip,
            user_agent=user_agent,
            relative_url=relative_url,
            data=data,
        )
        m = metrics.get()
        if m:
            m.register_pages.labels(sitename=self.sitename).inc()
        return page_item

    # ------------------------------------------------------------------
    # User management
    # ------------------------------------------------------------------

    def new_user(
        self,
        user: str | None = None,
        user_tags: str | None = None,
        user_id: str | None = None,
        user_name: str | None = None,
        avatar_extra: Any | None = None,
    ) -> dict:
        user_item = self.user_register.create(
            user=user,
            user_tags=user_tags,
            user_id=user_id,
            user_name=user_name,
            avatar_extra=avatar_extra,
        )
        m = metrics.get()
        if m:
            m.register_users.labels(sitename=self.sitename).inc()
        return user_item

    # ------------------------------------------------------------------
    # Query API
    # ------------------------------------------------------------------

    def subscribed_table_pages(self, table: str | None = None) -> list[dict]:
        return self.page_register.subscribed_table_pages(table)

    def pages(
        self,
        connection_id: str | None = None,
        user: str | None = None,
        index_name: str | None = None,
        filters: str | None = None,
        include_data: bool | None = None,
    ) -> list[dict]:
        if index_name:
            logger.info("call subscribed_table_pages instead of pages")
            return self.subscribed_table_pages(index_name)
        return self.page_register.pages(
            connection_id=connection_id,
            user=user,
            filters=filters,
            include_data=include_data,
        )

    def page(self, page_id: str) -> dict | None:
        return self.page_register.get_item(page_id)

    def connection(self, connection_id: str) -> dict | None:
        return self.connection_register.get_item(connection_id)

    def user(self, user: str) -> dict | None:
        return self.user_register.get_item(user)

    def counters(self) -> dict[str, int]:
        """Return a snapshot of item counts across all sub-registers."""
        return {
            "users": len(self.users()),
            "connections": len(self.connections()),
            "pages": len(self.pages()),
        }

    def users(self, include_data: bool | None = None) -> list[dict]:
        return self.user_register.values(include_data)

    def connections(
        self, user: str | None = None, include_data: bool | None = None
    ) -> list[dict]:
        return self.connection_register.connections(
            user=user, include_data=include_data
        )

    # ------------------------------------------------------------------
    # User / connection mutations
    # ------------------------------------------------------------------

    def change_connection_user(
        self,
        connection_id: str,
        user: str | None = None,
        user_tags: str | None = None,
        user_id: str | None = None,
        user_name: str | None = None,
        avatar_extra: Any | None = None,
    ) -> None:
        connection_item = self.connection(connection_id)
        if connection_item is None:
            logger.warning(
                "change_connection_user: connection %r not found", connection_id
            )
            return
        olduser = connection_item["user"]
        newuser_item = self.user(user)
        if not newuser_item:
            newuser_item = self.new_user(
                user=user,
                user_tags=user_tags,
                user_id=user_id,
                user_name=user_name,
                avatar_extra=avatar_extra,
            )
        connection_item["user"] = user
        connection_item["user_tags"] = user_tags
        connection_item["user_name"] = user_name
        connection_item["user_id"] = user_id
        connection_item["avatar_extra"] = avatar_extra
        self.connection_register.reindex_multi_index("user")
        for p in self.pages(connection_id=connection_id):
            p["user"] = user
        if not self.connection_register.connections(olduser):
            self.drop_user(olduser)

    # ------------------------------------------------------------------
    # Refresh / ping
    # ------------------------------------------------------------------

    def refresh(
        self,
        page_id: str,
        last_user_ts: datetime.datetime | None = None,
        last_rpc_ts: datetime.datetime | None = None,
        pageProfilers: Any = None,
    ) -> dict | None:
        """Propagate refresh timestamps from a page up through connection to user."""
        refresh_ts = datetime.datetime.now()
        page = self.page_register.refresh(
            page_id,
            last_user_ts=last_user_ts,
            last_rpc_ts=last_rpc_ts,
            refresh_ts=refresh_ts,
        )
        if not page:
            return None
        self.page_register.updatePageProfilers(page_id, pageProfilers)
        connection = self.connection_register.refresh(
            page["connection_id"],
            last_user_ts=last_user_ts,
            last_rpc_ts=last_rpc_ts,
            refresh_ts=refresh_ts,
        )
        if not connection:  # pragma: no cover
            return None
        return self.user_register.refresh(
            connection["user"],
            last_user_ts=last_user_ts,
            last_rpc_ts=last_rpc_ts,
            refresh_ts=refresh_ts,
        )

    def cleanup(self) -> list[str]:
        """Evict stale pages and connections.

        Returns a list of evicted connection IDs.  No-ops if the cleanup
        interval has not elapsed since the last run.
        """
        if time.time() - self.last_cleanup < self.cleanup_interval:
            return []
        now = datetime.datetime.now()
        dropped_pages = 0
        for page in self.pages():
            page_max_age = (
                self.page_max_age
                if not page["user"].startswith("guest_")
                else self.guest_connection_max_age
            )
            last_refresh_ts = page.get("last_refresh_ts") or page.get("start_ts")
            if (now - last_refresh_ts).seconds > page_max_age:
                self.drop_page(page["register_item_id"])
                dropped_pages += 1
        dropped_connections: list[str] = []
        for connection in self.connections():
            last_refresh_ts = connection.get("last_refresh_ts") or connection.get(
                "start_ts"
            )
            connection_max_age = (
                self.connection_max_age
                if not connection["user"].startswith("guest_")
                else self.guest_connection_max_age
            )
            if (now - last_refresh_ts).seconds > connection_max_age:
                dropped_connections.append(connection["register_item_id"])
                self.drop_connection(connection["register_item_id"], cascade=True)
        self.last_cleanup = time.time()
        m = metrics.get()
        if m and (dropped_pages or dropped_connections):
            if dropped_pages:
                m.cleanup_evictions_total.labels(
                    sitename=self.sitename, register="page"
                ).inc(dropped_pages)
            if dropped_connections:
                m.cleanup_evictions_total.labels(
                    sitename=self.sitename, register="connection"
                ).inc(len(dropped_connections))
        return dropped_connections

    # ------------------------------------------------------------------
    # Generic item access
    # ------------------------------------------------------------------

    def get_register(self, register_name: str) -> BaseRegister:
        """Return the sub-register for *register_name* (e.g. ``"page"``)."""
        return getattr(self, f"{register_name}_register")

    def get_item(
        self,
        register_item_id: str,
        include_data: bool = False,
        register_name: str | None = None,
    ) -> dict | None:
        if register_name:
            register = self.get_register(register_name)
            return register.get_item(register_item_id, include_data=include_data)
        for reg in (
            self.page_register,
            self.connection_register,
            self.user_register,
            self.global_register,
        ):
            item = reg.get_item(register_item_id, include_data=include_data)
            if item:
                return item
        return None

    def get_item_data(
        self,
        register_item_id: str,
        register_name: str | None = None,
    ) -> Bag:
        if register_name:
            register = self.get_register(register_name)
            return register.get_item_data(register_item_id)
        for reg in (
            self.page_register,
            self.connection_register,
            self.user_register,
            self.global_register,
        ):
            if reg.exists(register_item_id):
                return reg.get_item_data(register_item_id)
        return Bag()

    # ------------------------------------------------------------------
    # Persistence (legacy pickle path)
    # ------------------------------------------------------------------

    def dump(self) -> None:
        """Serialize all sub-registers to :attr:`storage_path`."""
        with open(self.storage_path, "wb") as storagefile:
            self.user_register.dump(storagefile)
            self.connection_register.dump(storagefile)
            self.page_register.dump(storagefile)

    def load(self) -> bool:
        """Restore all sub-registers from :attr:`storage_path`.

        Returns ``True`` on success, ``False`` on an empty or truncated file.
        """
        try:
            with open(self.storage_path, "rb") as storagefile:
                self.user_register.load(storagefile)
                self.connection_register.load(storagefile)
                self.page_register.load(storagefile)
            loadedpath = self.storage_path.replace(".pik", "_loaded.pik")
            if os.path.exists(loadedpath):
                os.remove(loadedpath)
            os.rename(self.storage_path, loadedpath)
            self._sync_metrics()
            return True
        except EOFError:
            return False

    def _sync_metrics(self) -> None:
        """Reset register gauges to match actual item counts (called after load)."""
        m = metrics.get()
        if not m:
            return
        m.register_pages.labels(sitename=self.sitename).set(len(self.pages()))
        m.register_connections.labels(sitename=self.sitename).set(
            len(self.connections())
        )
        m.register_users.labels(sitename=self.sitename).set(len(self.users()))

    # ------------------------------------------------------------------
    # Inter-process command bus
    # ------------------------------------------------------------------

    def pendingProcessCommands(self) -> list:
        """Consume and return the pending commands for the current PID."""
        pid = os.getpid()
        if pid not in self.interproces_commands:
            self.interproces_commands[pid] = dict(commands=[])
        pidhandler = self.interproces_commands[pid]
        commands = pidhandler["commands"]
        pidhandler["commands"] = []
        pidhandler["ts"] = datetime.datetime.now()
        return commands

    def sendProcessCommand(self, command: Any, pid: int | None = None) -> None:
        """Dispatch *command* to one or all registered PIDs."""
        if pid is None:
            pids = list(self.interproces_commands.keys())
        else:
            pids = [pid]
        now = datetime.datetime.now()
        for p in pids:
            pidhandler = self.interproces_commands[p]
            if (now - pidhandler["ts"]).total_seconds() > PROCESS_SELFDESTROY_TIMEOUT:
                self.interproces_commands.pop(p)
            else:
                if isinstance(command, list):
                    pidhandler["commands"].extend(command)
                else:
                    pidhandler["commands"].append(command)

    # ------------------------------------------------------------------
    # Maintenance mode
    # ------------------------------------------------------------------

    def setMaintenance(
        self, status: bool, allowed_users: list[str] | None = None
    ) -> None:
        if status is False:
            self.allowed_users = None
            self.maintenance = False
        else:
            self.allowed_users = allowed_users
            self.maintenance = True

    def isInMaintenance(self, user: str | None = None) -> bool:
        if not self.maintenance or user == "*forced*":
            return False
        if not user or not self.allowed_users:
            return self.maintenance
        return user not in self.allowed_users

    def allowedUsers(self) -> list[str] | None:
        return self.allowed_users

    # ------------------------------------------------------------------
    # Store subscriptions and data-change helpers (delegating to registers)
    # ------------------------------------------------------------------

    def setStoreSubscription(
        self,
        page_id: str,
        storename: str | None = None,
        client_path: str | None = None,
        active: bool | None = None,
    ) -> None:
        self.page_register.setStoreSubscription(
            page_id, storename=storename, client_path=client_path, active=active
        )

    def subscribeTable(
        self,
        page_id: str,
        table: str,
        subscribe: bool,
        subscribeMode: Any = None,
    ) -> None:
        self.page_register.subscribeTable(
            page_id, table=table, subscribe=subscribe, subscribeMode=subscribeMode
        )

    def subscription_storechanges(self, user: str, page_id: str) -> list:
        """Collect all pending data changes for *page_id*, including user-store subscriptions."""
        external_datachanges = self.page_register.get_datachanges(
            register_item_id=page_id, reset=True
        )
        page_item_data = self.page_register.get_item_data(page_id)
        if not page_item_data:
            return external_datachanges
        user_subscriptions = page_item_data.getItem("_subscriptions.user")
        if not user_subscriptions:
            return external_datachanges
        store_datachanges = []
        datachanges = self.user_register.get_datachanges(user)
        user_item_data = self.user_register.get_item_data(user)
        storesubscriptions_items = list(user_subscriptions.items())
        global_offsets = user_item_data.getItem("_subscriptions.offsets")
        if global_offsets is None:
            global_offsets = {}
            user_item_data.setItem("_subscriptions.offsets", global_offsets)
        for _j, change in enumerate(datachanges):
            changepath = change.path
            change_idx = change.change_idx
            for subpath, subdict in storesubscriptions_items:
                if subdict["on"] and changepath.startswith(subpath):
                    if change_idx > subdict.get("offset", 0):
                        subdict["offset"] = change_idx
                        change.attributes = change.attributes or {}
                        if change_idx > global_offsets.get(subpath, 0):
                            global_offsets[subpath] = change_idx
                            change.attributes["_new_datachange"] = True
                        else:
                            change.attributes.pop("_new_datachange", None)
                        store_datachanges.append(change)
        return external_datachanges + store_datachanges

    def handle_ping(
        self,
        page_id: str | None = None,
        reason: str | None = None,
        _serverstore_changes: dict | None = None,
        **kwargs: Any,
    ) -> Bag | bool:
        """Process a client ping: refresh timestamps, collect data changes.

        Returns a :class:`Bag` envelope with any pending ``dataChanges`` /
        ``childDataChanges``, or ``False`` if the page is no longer registered.
        """
        _children_pages_info = kwargs.get("_children_pages_info")
        _lastUserEventTs = kwargs.get("_lastUserEventTs")
        _lastRpc = kwargs.get("_lastRpc")
        _pageProfilers = kwargs.get("_pageProfilers")
        page_item = self.refresh(
            page_id,
            _lastUserEventTs,
            last_rpc_ts=_lastRpc,
            pageProfilers=_pageProfilers,
        )
        if not page_item:
            return False
        catalog = self.catalog
        if _serverstore_changes:
            self.set_serverstore_changes(page_id, _serverstore_changes)
        if _children_pages_info:
            for k, v in list(_children_pages_info.items()):
                child_lastUserEventTs = v.pop("_lastUserEventTs", None)
                child_lastRpc = v.pop("_lastRpc", None)
                child_pageProfilers = v.pop("_pageProfilers", None)
                if v:
                    self.set_serverstore_changes(k, v)
                if child_lastUserEventTs:
                    child_lastUserEventTs = catalog.fromTypedText(child_lastUserEventTs)
                if child_lastRpc:
                    child_lastRpc = catalog.fromTypedText(child_lastRpc)
                self.refresh(
                    k,
                    child_lastUserEventTs,
                    last_rpc_ts=child_lastRpc,
                    pageProfilers=child_pageProfilers,
                )
        envelope = Bag(dict(result=None))
        user = page_item["user"]
        datachanges = self.handle_ping_get_datachanges(page_id, user=user)
        if datachanges:
            envelope.setItem("dataChanges", datachanges)
        if _children_pages_info:
            for k in list(_children_pages_info.keys()):
                datachanges = self.handle_ping_get_datachanges(k, user=user)
                if datachanges:
                    envelope.setItem(f"childDataChanges.{k}", datachanges)
        user_register_data = self.user_register.get_item_data(user)
        lastBatchUpdate = user_register_data.getItem("lastBatchUpdate")
        if lastBatchUpdate:
            if (datetime.datetime.now() - lastBatchUpdate).seconds < 5:
                envelope.setItem("runningBatch", True)
            else:
                user_register_data.setItem("lastBatchUpdate", None)
        return envelope

    def handle_ping_get_datachanges(self, page_id: str, user: str | None = None) -> Bag:
        result = Bag()
        store_datachanges = self.subscription_storechanges(user, page_id)
        if store_datachanges:
            for j, change in enumerate(store_datachanges):
                result.setItem(
                    f"sc_{j}",
                    change.value,
                    change_path=change.path,
                    change_reason=change.reason,
                    change_fired=change.fired,
                    change_attr=change.attributes,
                    change_ts=change.change_ts,
                    change_delete=change.delete,
                )
        return result

    def set_serverstore_changes(
        self, page_id: str | None = None, datachanges: dict | None = None
    ) -> None:
        page_item_data = self.page_register.get_item_data(page_id)
        for k, v in list(datachanges.items()):
            page_item_data.setItem(k, self._parse_change_value(v))

    def _parse_change_value(self, change_value: Any) -> Any:
        if isinstance(change_value, (bytes, str)):
            try:
                v = self.catalog.fromTypedText(change_value)
                if isinstance(v, (bytes, str)) and hasattr(v, "decode"):
                    v = v.decode("utf-8")
                return v
            except Exception:
                raise
        return change_value

    # ------------------------------------------------------------------
    # __getattr__ delegation
    # ------------------------------------------------------------------

    def __getattr__(self, fname: str) -> Any:
        if fname.startswith("remotebag_"):
            handler = self.__dict__.get("remotebag_handler")
            if handler is not None:
                return getattr(handler, fname[len("remotebag_") :])

        def decore(*args: Any, **kwargs: Any) -> Any:
            register_name = kwargs.pop("register_name", None)
            if not register_name:
                return self.__getattribute__(fname)(*args, **kwargs)
            register = self.get_register(register_name)
            h = getattr(register, fname)
            return h(*args, **kwargs)

        return decore
