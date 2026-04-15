"""Base register infrastructure: constants, live-field helpers, and BaseRegister.

This module is internal.  Public consumers should import from
:mod:`genro_daemon.siteregister`.
"""

from __future__ import annotations

import datetime
import os
import pickle
import time
from collections import defaultdict
from typing import TYPE_CHECKING, Any

from gnr.core.gnrbag import Bag
from gnr.web import logger
from gnr.web.gnrwebpage import ClientDataChange

if TYPE_CHECKING:
    from .storage.base import StorageBackend

# ---------------------------------------------------------------------------
# Module-level singletons
# ---------------------------------------------------------------------------

BAG_INSTANCE = Bag()

# Fields that hold live Python objects and must be stripped before backend saves.
_LIVE_FIELDS: frozenset[str] = frozenset(
    {"datachanges", "datachanges_idx", "subscribed_paths"}
)

# ---------------------------------------------------------------------------
# Timing / retry constants
# ---------------------------------------------------------------------------

DEFAULT_CLEANUP_INTERVAL: int = 120
DEFAULT_PAGE_MAX_AGE: int = 120
DEFAULT_GUEST_CONNECTION_MAX_AGE: int = 40
DEFAULT_CONNECTION_MAX_AGE: int = 600
DEFAULT_IDLE_OFFLOAD_AGE: int = 10800  # 3 hours

LOCK_MAX_RETRY: int = 50
LOCK_EXPIRY_SECONDS: int = 10
RETRY_DELAY: float = 0.2
PROCESS_SELFDESTROY_TIMEOUT: int = 600


# ---------------------------------------------------------------------------
# BaseRegister
# ---------------------------------------------------------------------------


class BaseRegister:
    """In-memory dataset with locking, multi-index, and pluggable persistence.

    By default all state is kept in plain Python dicts (InMemoryBackend). Pass a
    *backend* instance (e.g. :class:`~genro_daemon.storage.redis.RedisBackend`)
    to store register items in a shared / persistent store.  The live-object
    fields (``itemsData``, ``itemsTS``, ``offloaded_items``, ``locked_items``)
    are always kept in-process because they hold non-serialisable Python objects.

    Subclasses declare which fields to index via :attr:`multi_index_attrs`.
    Each named attribute is kept in a ``defaultdict(list)`` so that lookups by
    that field value are O(1) without a full scan of ``registerItems``.
    """

    multi_index_attrs: list[str] = []

    def __init__(
        self,
        siteregister: Any,
        backend: StorageBackend | None = None,
        sitename: str | None = None,
    ) -> None:
        self.siteregister = siteregister
        self._backend = backend
        self._sitename = sitename
        # Namespace prefix used for all backend key operations:
        #   "{sitename}:{ClassName}"  e.g. "mysite:PageRegister"
        self._ns = (
            f"{sitename}:{self.__class__.__name__}"
            if sitename
            else self.__class__.__name__
        )
        self._persist_dir: str | None = None
        self._reset_all_registers()

    # ------------------------------------------------------------------
    # Backend persistence helpers
    # ------------------------------------------------------------------

    def _b_save(self, register_item_id: str, item: dict) -> None:
        """Persist a register item to the backend, stripping live-only fields."""
        if not self._backend:
            return
        storable = {k: v for k, v in item.items() if k not in _LIVE_FIELDS}
        self._backend.hset(self._ns, str(register_item_id), storable)

    def _setup_live_fields(self, item: dict, existing_data: Bag | None = None) -> None:
        """Add live fields to a register item and wire its itemsData Bag.

        *existing_data* may be passed when restoring a previously persisted item
        so that its content is preserved while the change subscription is
        re-attached (subscriptions do not survive pickling).
        """
        register_item_id = item["register_item_id"]
        item.setdefault("datachanges", list())
        item.setdefault("datachanges_idx", 0)
        item.setdefault("subscribed_paths", set())
        data = existing_data if existing_data is not None else Bag()
        data.subscribe(
            "datachanges",
            any=lambda **kwargs: self._on_data_trigger(register_item=item, **kwargs),
        )
        self.itemsData[register_item_id] = data

    def _reset_all_registers(self) -> None:
        self.registerItems: dict[str, dict] = {}
        self.itemsData: dict[str, Bag] = {}
        self.offloaded_items: dict[str, dict] = {}
        self.itemsTS: dict[str, datetime.datetime] = {}
        self.locked_items: dict[str, dict] = {}
        self.cached_tables: defaultdict[str, dict] = defaultdict(dict)
        self._multi_indexes: dict[str, defaultdict] = {
            x: defaultdict(list) for x in self.multi_index_attrs
        }
        if self._backend:
            stored = self._backend.hgetall(self._ns)
            if stored:
                logger.info(
                    "Loaded %d item(s) from backend into %s", len(stored), self._ns
                )
            for register_item_id, item in stored.items():
                self.registerItems[register_item_id] = item
                for k in self.multi_index_attrs:
                    if k in item:
                        self._multi_indexes[k][item[k]].append(item)
                self._setup_live_fields(item)

    # ------------------------------------------------------------------
    # Disk persistence (idle offloading / restart survival)
    # ------------------------------------------------------------------

    def set_persist_dir(self, path: str) -> None:
        """Set the directory used for on-disk item persistence and create it."""
        self._persist_dir = path
        ns_dir = self._ns.replace(":", "_")
        os.makedirs(os.path.join(path, ns_dir), exist_ok=True)

    def _item_persist_path(self, register_item_id: str) -> str:
        ns_dir = self._ns.replace(":", "_")
        return os.path.join(self._persist_dir, ns_dir, f"{register_item_id}.pkl")

    def _persist_item_to_disk(self, register_item_id: str, item: dict) -> None:
        """Atomically write *item* + its Bag data to a per-item pickle file."""
        path = self._item_persist_path(register_item_id)
        storable_item = {k: v for k, v in item.items() if k not in _LIVE_FIELDS}
        data = self.itemsData.get(register_item_id)
        ts = self.itemsTS.get(register_item_id)
        payload = {"item": storable_item, "data": data, "ts": ts}
        tmp = path + ".tmp"
        try:
            with open(tmp, "wb") as f:
                pickle.dump(payload, f)
            os.replace(tmp, path)  # atomic on POSIX
        except Exception:
            logger.warning(
                "Failed to persist item %s of %s to disk",
                register_item_id,
                self._ns,
                exc_info=True,
            )
            try:
                os.remove(tmp)
            except OSError:
                pass

    def _load_item_from_disk(
        self, register_item_id: str
    ) -> tuple[dict, Bag | None, datetime.datetime | None] | None:
        """Load a previously persisted item from disk.

        Returns ``(item_dict, bag_data, ts)`` or ``None`` if the file is absent
        or corrupt.
        """
        path = self._item_persist_path(register_item_id)
        if not os.path.exists(path):
            return None
        try:
            with open(path, "rb") as f:
                payload = pickle.load(f)
            return payload.get("item"), payload.get("data"), payload.get("ts")
        except Exception:
            logger.warning(
                "Failed to load item %s of %s from disk",
                register_item_id,
                self._ns,
                exc_info=True,
            )
            return None

    def _delete_item_from_disk(self, register_item_id: str) -> None:
        path = self._item_persist_path(register_item_id)
        try:
            os.remove(path)
        except OSError:
            pass

    def offload_idle_items(self, max_age_seconds: int) -> list[str]:
        """Offload items in *registerItems* that have not been accessed recently.

        The last-access time is taken from ``itemsTS`` when available (updated
        by every ``get_item()`` call).  For items that have never been fetched
        via ``get_item()`` — e.g. pages that are only ever refreshed — the
        item's own ``last_refresh_ts`` or ``start_ts`` field is used as a
        fallback so they are not excluded from offloading indefinitely.

        Only active when *max_age_seconds* > 0 and a persist directory is set.
        Returns the list of offloaded item IDs.
        """
        if not self._persist_dir or max_age_seconds <= 0:
            return []
        now = datetime.datetime.now()
        offloaded = []
        for register_item_id in list(self.registerItems.keys()):
            ts = self.itemsTS.get(register_item_id)
            if ts is None:
                # Fall back to the item's own refresh / creation timestamp
                item = self.registerItems.get(register_item_id)
                if item:
                    ts = item.get("last_refresh_ts") or item.get("start_ts")
            if ts and (now - ts).total_seconds() > max_age_seconds:
                self.offload_item(register_item_id)
                offloaded.append(register_item_id)
        if offloaded:
            logger.debug("Offloaded %d idle item(s) from %s", len(offloaded), self._ns)
        return offloaded

    def dump_all_to_disk(self) -> int:
        """Persist every live item (hot + in-memory cold) to disk.

        Used on daemon shutdown so items survive a restart.  Returns the number
        of items written.
        """
        if not self._persist_dir:
            return 0
        count = 0
        for register_item_id, item in list(self.registerItems.items()):
            self._persist_item_to_disk(register_item_id, item)
            count += 1
        for register_item_id, item in list(self.offloaded_items.items()):
            if item and not os.path.exists(self._item_persist_path(register_item_id)):
                self._persist_item_to_disk(register_item_id, item)
                count += 1
        return count

    def load_all_from_disk(self) -> int:
        """Load all per-item pickle files into *registerItems*.

        Called on daemon startup with an in-memory backend so that state from
        the previous run is restored.  Disk files are removed after loading
        (the daemon will re-dump them on next stop).  Returns the number of
        items loaded.
        """
        if not self._persist_dir:
            return 0
        ns_dir = self._ns.replace(":", "_")
        dir_path = os.path.join(self._persist_dir, ns_dir)
        if not os.path.isdir(dir_path):
            return 0
        count = 0
        for filename in os.listdir(dir_path):
            if not filename.endswith(".pkl"):
                continue
            register_item_id = filename[:-4]
            result = self._load_item_from_disk(register_item_id)
            if result is None:
                continue
            item, data, ts = result
            if item is None:
                continue
            self.registerItems[register_item_id] = item
            if ts is not None:
                self.itemsTS[register_item_id] = ts
            self._setup_live_fields(item, existing_data=data)
            for k in self.multi_index_attrs:
                if k in item:
                    self._multi_indexes[k][item[k]].append(item)
            self._delete_item_from_disk(register_item_id)
            count += 1
        return count

    # ------------------------------------------------------------------
    # Multi-index management
    # ------------------------------------------------------------------

    def drop_multi_indexes(self, register_item: dict) -> None:
        """Remove *register_item* from all secondary indexes.

        Matches by ``register_item_id`` rather than dict equality so that
        recharged (pickle-roundtripped) dicts are removed correctly.
        """
        rid = register_item.get("register_item_id")
        for x in self.multi_index_attrs:
            key = register_item.get(x)
            idx_list = self._multi_indexes[x].get(key)
            if idx_list is not None:
                idx_list[:] = [e for e in idx_list if e.get("register_item_id") != rid]

    def reindex_multi_index(self, index_name: str) -> None:
        """Rebuild the secondary index for *index_name* from scratch."""
        if index_name in self.multi_index_attrs:
            newindex: defaultdict = defaultdict(list)
            for _k, v in self.items():
                newindex[v[index_name]].append(v)
            self._multi_indexes[index_name] = newindex

    # ------------------------------------------------------------------
    # Item locking
    # ------------------------------------------------------------------

    def lock_item(self, register_item_id: str, reason: Any = None) -> bool:
        """Try to acquire an in-process lock on *register_item_id*.

        Returns ``True`` if the lock was granted (either newly acquired, or
        re-entered by the same *reason*).  Returns ``False`` if the lock is
        held by a different *reason* and has not yet expired.
        """
        locker = self.locked_items.get(register_item_id)
        if not locker:
            self.locked_items[register_item_id] = dict(
                reason=reason,
                count=1,
                last_lock_ts=time.time(),
            )
            return True
        elif locker["reason"] == reason:
            locker["count"] += 1
            locker["last_lock_ts"] = time.time()
            return True
        if (time.time() - locker["last_lock_ts"]) > LOCK_EXPIRY_SECONDS:
            self.locked_items.pop(register_item_id, None)
        return False

    def unlock_item(self, register_item_id: str, reason: Any = None) -> bool | None:
        """Release one lock count on *register_item_id* held by *reason*."""
        locker = self.locked_items.get(register_item_id)
        if locker:
            if locker["reason"] != reason:
                return False
            locker["count"] -= 1
            if not locker["count"]:
                self.locked_items.pop(register_item_id, None)
        return None

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    def addRegisterItem(self, register_item: dict, data: dict | None = None) -> None:
        """Insert *register_item* into this register, persisting to backend."""
        register_item_id = register_item["register_item_id"]
        self.registerItems[register_item_id] = register_item
        for k in self.multi_index_attrs:
            if k in register_item:
                self._multi_indexes[k][register_item[k]].append(register_item)
        self._b_save(register_item_id, register_item)
        register_item["datachanges"] = list()
        register_item["datachanges_idx"] = 0
        register_item["subscribed_paths"] = set()
        bag_data = Bag(data)
        bag_data.subscribe(
            "datachanges",
            any=lambda **kwargs: self._on_data_trigger(
                register_item=register_item, **kwargs
            ),
        )
        self.itemsData[register_item_id] = bag_data

    def _on_data_trigger(
        self,
        node: Any = None,
        ind: Any = None,
        evt: str | None = None,
        pathlist: list | None = None,
        register_item: dict | None = None,
        **kwargs: Any,
    ) -> None:
        if evt == "ins":
            pathlist.append(node.label)
        path = ".".join(pathlist)
        if evt != "del" and node.attr.get("_caching_table"):
            caching_subscribers = self.cached_tables[node.attr["_caching_table"]]
            register_item_id = register_item["register_item_id"]
            if register_item_id not in caching_subscribers:
                caching_subscribers[register_item_id] = {path}
            else:
                caching_subscribers[register_item_id].add(path)
        for subscribed in register_item["subscribed_paths"]:
            if path.startswith(subscribed):
                register_item["datachanges"].append(
                    ClientDataChange(
                        path=path,
                        value=node.value,
                        reason="serverChange",
                        attributes=node.attr,
                    )
                )
                break

    def invalidateTableCache(self, table: str) -> None:
        """Clear cached resolver data for *table* across all registered items."""
        table_cache = self.cached_tables.pop(table, {})
        for register_item_id, pathset in list(table_cache.items()):
            data = self.get_item_data(register_item_id)
            if not data:
                continue
            for p in pathset:
                data[p] = None

    def updateTS(self, register_item_id: str) -> None:
        """Stamp the last-access timestamp for *register_item_id*."""
        self.itemsTS[register_item_id] = datetime.datetime.now()

    def get_item_data(self, register_item_id: str) -> Bag:
        """Return the live :class:`Bag` data for *register_item_id*, or an empty Bag."""
        return self.itemsData.get(register_item_id, Bag())

    def get_item(
        self, register_item_id: str, include_data: bool = False
    ) -> dict | None:
        """Return the register item dict, optionally embedding its live data Bag."""
        item = self.registerItems.get(register_item_id)
        if item is None:
            item = self._charge_item(register_item_id)
        self.updateTS(register_item_id)
        if item and include_data:
            item["data"] = self.get_item_data(register_item_id)
        return item

    def exists(self, register_item_id: str) -> bool:
        return register_item_id in self.registerItems

    def keys(self) -> list[str]:
        return list(self.registerItems.keys())

    def items(self, include_data: bool = False) -> list[tuple[str, dict]]:
        if not include_data:
            return list(self.registerItems.items())
        return [(k, self.get_item(k, include_data=True)) for k in self.keys()]

    def values(self, include_data: bool = False) -> list[dict]:
        if not include_data:
            return list(self.registerItems.values())
        return [self.get_item(k, include_data=True) for k in self.keys()]

    def refresh(
        self,
        register_item_id: str,
        last_user_ts: datetime.datetime | None = None,
        last_rpc_ts: datetime.datetime | None = None,
        refresh_ts: datetime.datetime | None = None,
    ) -> dict | None:
        """Update the last-seen timestamps on *register_item_id*."""
        item = self.registerItems.get(register_item_id)
        if not item:
            return None
        item["last_user_ts"] = (
            max(item["last_user_ts"], last_user_ts)
            if item.get("last_user_ts")
            else last_user_ts
        )
        item["last_rpc_ts"] = (
            max(item["last_rpc_ts"], last_rpc_ts)
            if item.get("last_rpc_ts")
            else last_rpc_ts
        )
        item["last_refresh_ts"] = (
            max(item["last_refresh_ts"], refresh_ts)
            if item.get("last_refresh_ts")
            else refresh_ts
        )
        self._b_save(register_item_id, item)
        return item

    @property
    def registerName(self) -> str:
        return self.__class__.__name__

    def drop_item(self, register_item_id: str) -> dict | None:
        """Remove *register_item_id* from this register and from all storage."""
        register_item = self.registerItems.pop(register_item_id, None)
        if register_item is None:
            # Check in-memory cold store
            register_item = self.offloaded_items.pop(register_item_id, None)
        if register_item is None and self._persist_dir:
            # Item was offloaded to disk; load it just to get its attributes
            # so we can clean up the multi-indexes correctly.
            result = self._load_item_from_disk(register_item_id)
            if result:
                register_item = result[0]
        if register_item:
            self.drop_multi_indexes(register_item)
        self.offloaded_items.pop(register_item_id, None)
        self.itemsData.pop(register_item_id, None)
        self.itemsTS.pop(register_item_id, None)
        if self._backend:
            self._backend.hdel(self._ns, str(register_item_id))
        if self._persist_dir:
            self._delete_item_from_disk(register_item_id)
        return register_item

    def offload_item(self, register_item_id: str) -> None:
        """Move *register_item_id* out of the hot dict into cold storage.

        When a persist directory is configured the item is written to disk and
        removed from memory entirely (disk is the cold store).  Otherwise the
        existing in-memory ``offloaded_items`` dict is used.
        """
        item = self.registerItems.pop(register_item_id, None)
        if item is None:
            return
        if self._persist_dir:
            self._persist_item_to_disk(register_item_id, item)
            self.itemsData.pop(register_item_id, None)
            logger.debug(
                "Offloaded item %s from %s to disk", register_item_id, self._ns
            )
        else:
            self.offloaded_items[register_item_id] = item
            logger.debug(
                "Offloaded item %s from %s to in-memory cold store",
                register_item_id,
                self._ns,
            )

    def _charge_item(self, register_item_id: str) -> dict | None:
        """Restore a previously offloaded item back into the hot dict.

        Checks in-memory cold store first, then disk (when persist dir is set).
        """
        i = self.offloaded_items.pop(register_item_id, None)
        if i:
            self.registerItems[register_item_id] = i
            logger.debug(
                "Charged item %s in %s from in-memory cold store",
                register_item_id,
                self._ns,
            )
            return i
        if self._persist_dir:
            result = self._load_item_from_disk(register_item_id)
            if result is None:
                return None
            item, data, ts = result
            if item is None:
                return None
            self.registerItems[register_item_id] = item
            if ts is not None:
                self.itemsTS[register_item_id] = ts
            self._setup_live_fields(item, existing_data=data)
            # Update the existing multi-index entry in-place rather than appending.
            # offload_item() keeps the item in the index, so we only need to swap
            # the (now-stale) dict reference with the freshly loaded one.
            for k in self.multi_index_attrs:
                if k in item:
                    idx_list = self._multi_indexes[k].get(item[k], [])
                    for i_idx, entry in enumerate(idx_list):
                        if entry.get("register_item_id") == register_item_id:
                            idx_list[i_idx] = item
                            break
                    else:
                        # Not in index yet (e.g. first load after daemon restart)
                        self._multi_indexes[k][item[k]].append(item)
            self._delete_item_from_disk(register_item_id)
            logger.debug("Charged item %s in %s from disk", register_item_id, self._ns)
            return item
        return None

    def item_is_offloaded(self, register_item_id: str) -> bool:
        return register_item_id in self.offloaded_items

    def update_item(
        self, register_item_id: str, upddict: dict | None = None
    ) -> dict | None:
        """Merge *upddict* into *register_item_id* and persist the result."""
        upddict = upddict or {}
        register_item = self.get_item(register_item_id)
        if not register_item:
            return None
        register_item.update(upddict)
        self._b_save(register_item_id, register_item)
        return register_item

    # ------------------------------------------------------------------
    # Data-change helpers
    # ------------------------------------------------------------------

    def set_datachange(
        self,
        register_item_id: str,
        path: str,
        value: Any = None,
        attributes: dict | None = None,
        fired: bool = False,
        reason: str | None = None,
        replace: bool = False,
        delete: bool = False,
    ) -> None:
        """Append a :class:`ClientDataChange` to *register_item_id*'s change list."""
        register_item = self.get_item(register_item_id)
        if not register_item:
            return
        datachanges = register_item["datachanges"]
        register_item["datachanges_idx"] = register_item.get("datachanges_idx", 0) + 1
        datachange = ClientDataChange(
            path,
            value,
            attributes=attributes,
            fired=fired,
            reason=reason,
            change_idx=register_item["datachanges_idx"],
            delete=delete,
        )
        if replace and datachange in datachanges:
            datachanges.pop(datachanges.index(datachange))
        datachanges.append(datachange)

    def get_datachanges(
        self, register_item_id: str, reset: bool = False
    ) -> list | None:
        """Return pending data changes for *register_item_id*, optionally clearing them."""
        register_item = self.get_item(register_item_id)
        if not register_item:
            return None
        datachanges = register_item["datachanges"]
        if reset:
            register_item["datachanges"] = []
            register_item["datachanges_idx"] = 0
        return datachanges

    def reset_datachanges(self, register_item_id: str) -> dict | None:
        return self.update_item(
            register_item_id, dict(datachanges=list(), datachanges_idx=0)
        )

    def drop_datachanges(self, register_item_id: str, path: str) -> None:
        """Remove all pending data changes whose path starts with *path*."""
        register_item = self.get_item(register_item_id)
        if not register_item:
            return
        datachanges = register_item["datachanges"]
        datachanges[:] = [dc for dc in datachanges if not dc.path.startswith(path)]

    def subscribe_path(self, register_item_id: str, path: str) -> None:
        """Register interest in server-side data changes under *path*."""
        register_item = self.get_item(register_item_id)
        register_item["subscribed_paths"].add(path)

    def get_dbenv(self, register_item_id: str) -> Bag:
        """Build and return the database environment Bag for *register_item_id*."""
        data = self.get_item_data(register_item_id)
        dbenvbag = data.getItem("dbenv") or Bag()
        dbenvbag.update(data.getItem("rootenv") or Bag())

        def addToDbEnv(n: Any, _pathlist: list | None = None) -> None:
            if n.attr.get("dbenv"):
                path = n.label if n.attr["dbenv"] is True else n.attr["dbenv"]
                dbenvbag[path] = n.value

        _pathlist: list = []
        data.walk(addToDbEnv, _pathlist=_pathlist)
        return dbenvbag

    # ------------------------------------------------------------------
    # File-based persistence (legacy pickle path)
    # ------------------------------------------------------------------

    def dump(self, storagefile: Any) -> None:
        """Serialize this register's state to an open file object."""
        pickle.dump(self.registerItems, storagefile)
        pickle.dump(self.itemsData, storagefile)
        pickle.dump(self.itemsTS, storagefile)
        pickle.dump(self.locked_items, storagefile)
        pickle.dump(self.offloaded_items, storagefile)

    def load(self, storagefile: Any) -> None:
        """Restore this register's state from an open file object."""
        self.registerItems = pickle.load(storagefile)
        self.itemsData = pickle.load(storagefile)
        self.itemsTS = pickle.load(storagefile)
        self.locked_items = pickle.load(storagefile)
        self.offloaded_items = pickle.load(storagefile)
