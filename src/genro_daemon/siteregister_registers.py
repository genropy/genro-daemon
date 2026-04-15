"""Concrete register classes and supporting helpers.

Exports:
- :class:`GlobalRegister`
- :class:`UserRegister`
- :class:`ConnectionRegister`
- :class:`PageRegister`
- :class:`RegisterResolver`
- :class:`RemoteStoreBagHandler`

This module is internal.  Public consumers should import from
:mod:`genro_daemon.siteregister`.
"""

from __future__ import annotations

import datetime
import re
from collections import defaultdict
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from gnr.core.gnrbag import Bag, BagResolver

from .siteregister_base import BaseRegister

if TYPE_CHECKING:
    from .siteregister import GnrSiteRegister


# ---------------------------------------------------------------------------
# Concrete registers
# ---------------------------------------------------------------------------


class GlobalRegister(BaseRegister):
    """Single-item register that holds site-wide global state.

    The sentinel item ``"*"`` is created automatically and serves as the
    backing store for :meth:`GnrSiteRegister.globalStore`.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        if not self.exists("*"):
            self.create("*")

    def create(self, identifier: str | None = None) -> dict:
        """Create and register the global item under *identifier*."""
        register_item = dict(
            start_ts=datetime.datetime.now(),
            register_item_id=identifier,
            register_name="global",
        )
        self.addRegisterItem(register_item)
        return register_item

    def drop(self, identifier: str) -> None:
        self.drop_item(identifier)


class UserRegister(BaseRegister):
    """Per-user register — one item per authenticated username."""

    def create(
        self,
        user: str,
        user_id: str | None = None,
        user_name: str | None = None,
        user_tags: str | None = None,
        avatar_extra: Any | None = None,
    ) -> dict:
        register_item = dict(
            register_item_id=user,
            start_ts=datetime.datetime.now(),
            user=user,
            user_id=user_id,
            user_name=user_name,
            user_tags=user_tags,
            avatar_extra=avatar_extra,
            register_name="user",
        )
        self.addRegisterItem(register_item)
        return register_item

    def drop(self, user: str, _testing: bool = False) -> None:
        if _testing is False:
            self.siteregister.drop_connections(user)  # pragma: no cover
        self.drop_item(user)


class ConnectionRegister(BaseRegister):
    """Per-browser-session register — one item per connection UUID."""

    multi_index_attrs = ["user"]

    def create(
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
        electron_static: Any | None = None,
        avatar_extra: Any | None = None,
    ) -> dict:
        register_item = dict(
            register_item_id=connection_id,
            start_ts=datetime.datetime.now(),
            connection_name=connection_name,
            user=user,
            user_id=user_id,
            user_name=user_name,
            user_tags=user_tags,
            user_ip=user_ip,
            user_agent=user_agent,
            electron_static=electron_static,
            browser_name=browser_name,
            register_name="connection",
        )
        self.addRegisterItem(register_item)
        return register_item

    def drop(
        self,
        register_item_id: str,
        cascade: bool | None = None,
        _testing: bool = False,
    ) -> None:
        if _testing is False:
            self.siteregister.drop_pages(register_item_id)  # pragma: no cover
        register_item = self.drop_item(register_item_id)
        if cascade and register_item:
            user = register_item["user"]
            keys = self.user_connection_keys(user)
            if not keys and not _testing:  # pragma: no cover
                self.siteregister.drop_user(user)

    def user_connection_keys(self, user: str) -> list[str]:
        return [u["register_item_id"] for u in self._multi_indexes.get("user")[user]]

    def user_connection_items(self, user: str) -> list[tuple[str, dict]]:
        return [
            (i["register_item_id"], i) for i in self._multi_indexes.get("user")[user]
        ]

    def connections(
        self, user: str | None = None, include_data: bool = False
    ) -> list[dict]:
        if user:
            return self._multi_indexes.get("user")[user]
        return self.values(include_data=include_data)


class PageRegister(BaseRegister):
    """Per-page register — one item per open browser tab."""

    multi_index_attrs = ["connection_id", "user"]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._subscribed_table_index: defaultdict[str, list] = defaultdict(list)
        self.pageProfilers: dict[str, Any] = dict()

    def create(
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
        register_item_id = page_id
        start_ts = datetime.datetime.now()
        register_item = dict(
            register_item_id=register_item_id,
            pagename=pagename,
            connection_id=connection_id,
            start_ts=start_ts,
            subscribed_tables=(
                {x for x in subscribed_tables.split(",")}
                if subscribed_tables
                else set()
            ),
            user=user,
            user_ip=user_ip,
            user_agent=user_agent,
            relative_url=relative_url,
            datachanges=list(),
            subscribed_paths=set(),
            register_name="page",
        )
        self.addRegisterItem(register_item, data=data)
        if subscribed_tables:
            for x in subscribed_tables.split(","):
                self._subscribed_table_index[x].append(register_item_id)
        return register_item

    def drop(
        self,
        register_item_id: str | None = None,
        cascade: bool = False,
        _testing: bool = False,
    ) -> None:
        register_item = self.drop_item(register_item_id)
        self.pageProfilers.pop(register_item_id, None)
        if cascade and register_item:
            connection_id = register_item["connection_id"]
            n = self.connection_page_keys(connection_id)
            if not n and not _testing:  # pragma: no cover
                self.siteregister.drop_connection(connection_id)

    def filter_subscribed_tables(self, table_list: Iterable[str]) -> list[str]:
        """Return the subset of *table_list* that has at least one subscriber."""
        s = {x for x in self._subscribed_table_index.keys()}
        return list(s.intersection(table_list))

    def subscribed_table_page_keys(self, table: str) -> list[str]:
        return self._subscribed_table_index.get(table, [])

    def subscribed_table_page_items(self, table: str) -> list[tuple[str, dict]]:
        return [
            (k, item)
            for k in self._subscribed_table_index.get(table, [])
            if (item := self.get_item(k)) is not None
        ]

    def subscribed_table_pages(self, table: str) -> list[dict]:
        return [
            item
            for x in self._subscribed_table_index.get(table, [])
            if (item := self.get_item(x)) is not None
        ]

    def connection_page_keys(self, connection_id: str) -> list[str]:
        return [
            i["register_item_id"]
            for i in self._multi_indexes["connection_id"][connection_id]
        ]

    def connection_page_items(self, connection_id: str) -> list[tuple[str, dict]]:
        return [
            (i["register_item_id"], i)
            for i in self._multi_indexes["connection_id"][connection_id]
        ]

    def connection_pages(self, connection_id: str) -> list[dict]:
        return self._multi_indexes["connection_id"][connection_id]

    def pages(
        self,
        connection_id: str | None = None,
        user: str | None = None,
        include_data: bool | None = None,
        filters: str | None = None,
    ) -> list[dict]:
        """Return pages filtered by connection, user, and/or an ad-hoc filter string."""
        if connection_id and not user:
            pages = [
                item
                for x in self._multi_indexes["connection_id"][connection_id]
                if (item := self.get_item(x["register_item_id"], include_data=include_data)) is not None
            ]
        elif user and not connection_id:
            pages = [
                item
                for x in self._multi_indexes["user"][user]
                if (item := self.get_item(x["register_item_id"], include_data=include_data)) is not None
            ]
        elif connection_id and user:
            pages = [
                item
                for x in self._multi_indexes["connection_id"][connection_id]
                if (item := self.get_item(x["register_item_id"], include_data=include_data)) is not None
            ]
            pages = list(filter(lambda x: x["user"] == user, pages))
        else:
            pages = self.values(include_data=include_data)

        if not filters or filters == "*":
            return pages

        # Parse and pre-compile each filter pattern so we don't recompile per page.
        fltdict: dict[str, re.Pattern | str] = {}
        for flt in filters.split(" AND "):
            fltname, fltvalue = flt.split(":", 1)
            try:
                fltdict[fltname] = re.compile(fltvalue)
            except re.error:
                fltdict[fltname] = fltvalue

        filtered = []

        def checkpage(page: Any, fltname: str, fltpat: re.Pattern | str) -> Any:
            value = page[fltname]
            if not value:
                return None
            if not isinstance(value, (bytes, str)):
                return str(fltpat) == value
            if isinstance(fltpat, re.Pattern):
                return fltpat.match(value)
            return fltpat == value

        for page in pages:
            page = Bag(page)
            for fltname, fltpat in fltdict.items():
                if checkpage(page, fltname, fltpat):
                    filtered.append(page)
        return filtered

    def updatePageProfilers(self, page_id: str, pageProfilers: Any) -> None:
        self.pageProfilers[page_id] = pageProfilers

    def setStoreSubscription(
        self,
        page_id: str,
        storename: str | None = None,
        client_path: str | None = None,
        active: bool | None = None,
    ) -> None:
        register_item_data = self.get_item_data(page_id)
        subscription_path = f"_subscriptions.{storename}"
        storesub = register_item_data.getItem(subscription_path)
        if storesub is None:
            storesub = dict()
            register_item_data.setItem(subscription_path, storesub)
        pathsub = storesub.setdefault(client_path, {})
        pathsub["on"] = active

    def subscribeTable(
        self,
        page_id: str,
        table: str | None = None,
        subscribe: bool = False,
        subscribeMode: Any = None,
    ) -> None:
        subscribed_tables = self.get_item(page_id)["subscribed_tables"]
        if subscribe:
            subscribed_tables.add(table)
            self._subscribed_table_index[table].append(page_id)
        else:
            if table in subscribed_tables:
                subscribed_tables.remove(table)
                if page_id in self._subscribed_table_index[table]:
                    self._subscribed_table_index[table].remove(page_id)

    def notifyDbEvents(
        self,
        dbeventsDict: dict | None = None,
        origin_page_id: str | None = None,
        dbevent_reason: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Push database-change notifications to all subscribed pages."""
        for table, dbevents in list(dbeventsDict.items()):
            if not dbevents:
                continue
            table_code = table.replace(".", "_")
            self.siteregister.checkCachedTables(table)
            subscribers = self.subscribed_table_pages(table)
            if not subscribers:
                continue
            for page in subscribers:
                self.set_datachange(
                    page["register_item_id"],
                    f"gnr.dbchanges.{table_code}",
                    dbevents,
                    attributes=dict(
                        from_page_id=origin_page_id,
                        dbevent_reason=dbevent_reason,
                    ),
                )

    def setPendingContext(self, page_id: str, pendingContext: list) -> None:
        data = self.get_item_data(page_id)
        for serverpath, value, attr in pendingContext:
            data.setItem(serverpath, value, attr)
            if isinstance(value, Bag):
                data.clearBackRef()
                data.setBackRef()
            self.subscribe_path(page_id, serverpath)

    def pageInMaintenance(
        self, page_id: str | None = None, _testing: bool = False
    ) -> bool | None:
        page_item = self.get_item(page_id)
        if not page_item:
            return None
        user = page_item["user"]
        if not _testing:  # pragma: no cover
            return self.siteregister.isInMaintenance(user)
        return None

    def setInClientData(
        self,
        path: str | Bag,
        value: Any = None,
        attributes: dict | None = None,
        page_id: str | None = None,
        filters: str | None = None,
        fired: bool = False,
        reason: str | None = None,
        public: bool = False,
        replace: bool = False,
    ) -> None:
        if filters:
            pages = [p["register_item_id"] for p in self.pages(filters=filters)]
        else:
            pages = [page_id]
        for pid in pages:
            if isinstance(path, Bag):
                for changeNode in path:
                    attr = changeNode.attr
                    self.set_datachange(
                        pid,
                        path=attr.pop("_client_path"),
                        value=changeNode.value,
                        attributes=attr,
                        fired=attr.pop("fired", None),
                    )
            else:
                self.set_datachange(
                    pid,
                    path=path,
                    value=value,
                    reason=reason,
                    attributes=attributes,
                    fired=fired,
                )


# ---------------------------------------------------------------------------
# Resolver and remote-bag helpers
# ---------------------------------------------------------------------------


class RegisterResolver(BagResolver):
    """Lazy resolver that populates a Bag tree with register items.

    Used by the admin UI to browse users → connections → pages without
    loading the full register into memory at once.
    """

    classKwargs = {
        "cacheTime": 1,
        "readOnly": False,
        "user": None,
        "connection_id": None,
        "_page": None,
    }
    classArgs = ["user"]

    def load(self) -> Bag:
        if not self.user:
            return self.list_users()
        elif not self.connection_id:
            return self.list_connections(user=self.user)
        else:
            return self.list_pages(connection_id=self.connection_id)

    @property
    def register(self) -> GnrSiteRegister:
        return self._page.site.register

    def list_users(self) -> Bag:
        usersDict = self.register.users(include_data=True)
        result = Bag()
        for user, item_user in list(usersDict.items()):
            item = Bag()
            data = item_user.pop("data", None)
            item_user.pop("datachanges", None)
            item_user.pop("datachanges_idx", None)
            item["info"] = Bag(item_user)
            item["data"] = data
            item.setItem("connections", RegisterResolver(user=user), cacheTime=3)
            result.setItem(user, item, user=user)
        return result

    def list_connections(self, user: str) -> Bag:
        connectionsDict = self.register.connections(user=user, include_data=True)
        result = Bag()
        for connection_id, connection in list(connectionsDict.items()):
            delta = (datetime.datetime.now() - connection["start_ts"]).seconds
            user = connection["user"] or "Anonymous"
            connection_name = connection["connection_name"]
            itemlabel = f"{connection_name} ({delta})"
            item = Bag()
            data = connection.pop("data", None)
            item["info"] = Bag(connection)
            item["data"] = data
            item.setItem(
                "pages",
                RegisterResolver(user=user, connection_id=connection_id),
                cacheTime=2,
            )
            result.setItem(itemlabel, item, user=user, connection_id=connection_id)
        return result

    def list_pages(self, connection_id: str) -> Bag:
        pagesDict = self.register.pages(connection_id=connection_id, include_data=True)
        result = Bag()
        for page_id, page in list(pagesDict.items()):
            delta = (datetime.datetime.now() - page["start_ts"]).seconds
            pagename = page["pagename"].replace(".py", "")
            itemlabel = f"{pagename} ({delta})"
            item = Bag()
            data = page.pop("data", None)
            item["info"] = Bag(page)
            item["data"] = data
            result.setItem(
                itemlabel,
                item,
                user=item["user"],
                connection_id=item["connection_id"],
                page_id=page_id,
            )
        return result

    def resolverSerialize(self) -> dict:
        attr = super().resolverSerialize()
        attr["kwargs"].pop("_page", None)
        return attr


class RemoteStoreBagHandler:
    """Exposes per-item Bag data via the Ars protocol.

    Methods on this object are reached through :class:`GnrSiteRegister`'s
    ``remotebag_*`` delegating ``__getattr__``.  Callers pass the special
    kwargs ``_siteregister_register_name`` and ``_siteregister_register_item_id``
    to identify which Bag to target, plus an optional ``_pyrosubbag`` path for
    sub-tree access.
    """

    def __init__(self, siteregister: GnrSiteRegister) -> None:
        self.siteregister = siteregister

    def __getattr__(self, name: str) -> Any:
        def decore(*args: Any, **kwargs: Any) -> Any:
            register_name = kwargs.pop("_siteregister_register_name", None)
            register_item_id = kwargs.pop("_siteregister_register_item_id", None)
            subbag_path = kwargs.pop("_pyrosubbag", None)
            store = self.siteregister.get_item_data(
                register_item_id, register_name=register_name
            )
            if subbag_path:
                store = store.getItem(subbag_path)
            h = getattr(store, name, None)
            if not h:
                raise AttributeError(
                    f"SubBag at {subbag_path!r} has no attribute {name!r}"
                )
            return h(*args, **kwargs)

        return decore
