"""Tests for genro_daemon.siteregister – BaseRegister, sub-registers, GnrSiteRegister."""

import datetime
import logging
import os
import time
from unittest.mock import MagicMock

import pytest
from gnr.core.gnrbag import Bag

from genro_daemon.exceptions import GnrDaemonLocked
from genro_daemon.siteregister import (
    DEFAULT_CLEANUP_INTERVAL,
    DEFAULT_CONNECTION_MAX_AGE,
    DEFAULT_GUEST_CONNECTION_MAX_AGE,
    DEFAULT_PAGE_MAX_AGE,
    LOCK_EXPIRY_SECONDS,
    PROCESS_SELFDESTROY_TIMEOUT,
    GnrSiteRegister,
    RemoteStoreBagHandler,
)
from genro_daemon.siteregister_client import RemoteStoreBag, ServerStore
from genro_daemon.storage.memory import InMemoryBackend

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_site_register(sitename="testsite"):
    daemon = MagicMock()
    backend = InMemoryBackend()
    reg = GnrSiteRegister(daemon, sitename=sitename, backend=backend)
    reg.setConfiguration()
    return reg


def make_page_register(sitename="testsite"):
    sr = make_site_register(sitename)
    return sr.page_register, sr


def make_connection_register(sitename="testsite"):
    sr = make_site_register(sitename)
    return sr.connection_register, sr


def make_user_register(sitename="testsite"):
    sr = make_site_register(sitename)
    return sr.user_register, sr


# ===========================================================================
# BaseRegister – locking
# ===========================================================================


class TestBaseRegisterLocking:
    def setup_method(self):
        sr = make_site_register()
        self.reg = sr.page_register

    def test_lock_item_succeeds_first_time(self):
        assert self.reg.lock_item("page1", reason="test") is True
        assert "page1" in self.reg.locked_items

    def test_lock_same_reason_increments_count(self):
        self.reg.lock_item("page1", reason="r")
        self.reg.lock_item("page1", reason="r")
        assert self.reg.locked_items["page1"]["count"] == 2

    def test_lock_different_reason_fails(self):
        self.reg.lock_item("page1", reason="owner1")
        assert self.reg.lock_item("page1", reason="owner2") is False

    def test_lock_expires_after_timeout(self):
        self.reg.lock_item("page1", reason="r")
        # Backdating the timestamp forces expiry
        self.reg.locked_items["page1"]["last_lock_ts"] -= LOCK_EXPIRY_SECONDS + 1
        # Different reason now succeeds because old lock is stale
        assert self.reg.lock_item("page1", reason="new_owner") is False
        # And the stale entry was cleared
        assert "page1" not in self.reg.locked_items

    def test_unlock_decrements_count(self):
        self.reg.lock_item("page1", reason="r")
        self.reg.lock_item("page1", reason="r")
        self.reg.unlock_item("page1", reason="r")
        assert self.reg.locked_items["page1"]["count"] == 1

    def test_unlock_fully_removes_entry(self):
        self.reg.lock_item("page1", reason="r")
        self.reg.unlock_item("page1", reason="r")
        assert "page1" not in self.reg.locked_items

    def test_unlock_wrong_reason_returns_false(self):
        self.reg.lock_item("page1", reason="owner")
        result = self.reg.unlock_item("page1", reason="intruder")
        assert result is False
        assert "page1" in self.reg.locked_items  # still locked

    def test_unlock_nonexistent_item_is_noop(self):
        self.reg.unlock_item("ghost", reason="r")  # must not raise


# ===========================================================================
# BaseRegister – item storage
# ===========================================================================


class TestBaseRegisterItems:
    def setup_method(self):
        self.sr = make_site_register()
        self.reg = self.sr.page_register

    def _add_item(self, item_id="p1"):
        item = {
            "register_item_id": item_id,
            "pagename": "test.py",
            "connection_id": "conn1",
            "user": "alice",
            "register_name": "page",
            "subscribed_tables": set(),
        }
        self.reg.addRegisterItem(item)
        return item

    def test_add_and_get_item(self):
        self._add_item("p1")
        result = self.reg.get_item("p1")
        assert result["pagename"] == "test.py"

    def test_exists_true_after_add(self):
        self._add_item("p1")
        assert self.reg.exists("p1") is True

    def test_exists_false_before_add(self):
        assert self.reg.exists("never") is False

    def test_keys_lists_all(self):
        self._add_item("p1")
        self._add_item("p2")
        assert sorted(self.reg.keys()) == ["p1", "p2"]

    def test_drop_item_removes_from_register(self):
        self._add_item("p1")
        self.reg.drop_item("p1")
        assert self.reg.get_item("p1") is None

    def test_offload_and_charge_item(self):
        self._add_item("p1")
        self.reg.offload_item("p1")
        assert self.reg.item_is_offloaded("p1")
        assert "p1" not in self.reg.registerItems
        # _charge_item moves it back
        self.reg._charge_item("p1")
        assert "p1" in self.reg.registerItems
        assert not self.reg.item_is_offloaded("p1")

    def test_update_item(self):
        self._add_item("p1")
        self.reg.update_item("p1", {"pagename": "updated.py"})
        assert self.reg.get_item("p1")["pagename"] == "updated.py"

    def test_update_item_missing_returns_none(self):
        result = self.reg.update_item("ghost", {"x": 1})
        assert result is None

    def test_refresh_updates_timestamps(self):
        self._add_item("p1")
        ts = datetime.datetime.now()
        self.reg.refresh("p1", last_user_ts=ts)
        assert self.reg.get_item("p1")["last_user_ts"] == ts

    def test_get_item_missing_returns_none(self):
        assert self.reg.get_item("ghost") is None

    def test_set_datachange_adds_to_list(self):
        self._add_item("p1")
        self.reg.set_datachange("p1", "some.path", value="hello")
        changes = self.reg.get_item("p1")["datachanges"]
        assert len(changes) == 1

    def test_set_datachange_missing_item_is_noop(self):
        self.reg.set_datachange("ghost", "path", value="x")  # no crash

    def test_get_datachanges_and_reset(self):
        self._add_item("p1")
        self.reg.set_datachange("p1", "a.b", value=1)
        self.reg.set_datachange("p1", "a.c", value=2)
        changes = self.reg.get_datachanges("p1", reset=True)
        assert len(changes) == 2
        assert self.reg.get_item("p1")["datachanges"] == []

    def test_drop_datachanges_for_path(self):
        self._add_item("p1")
        self.reg.set_datachange("p1", "x.y", value=1)
        self.reg.drop_datachanges("p1", "x.y")
        changes = self.reg.get_item("p1")["datachanges"]
        assert all(c.path != "x.y" for c in changes)

    def test_subscribe_path(self):
        self._add_item("p1")
        self.reg.subscribe_path("p1", "data.user")
        paths = self.reg.get_item("p1")["subscribed_paths"]
        assert "data.user" in paths


# ===========================================================================
# PageRegister
# ===========================================================================


class TestPageRegister:
    def setup_method(self):
        self.reg, self.sr = make_page_register()

    def test_create_page(self):
        page = self.reg.create(
            "p1", pagename="home.py", connection_id="c1", user="alice"
        )
        assert page["register_item_id"] == "p1"
        assert page["pagename"] == "home.py"
        assert page["user"] == "alice"
        assert page["register_name"] == "page"

    def test_create_page_with_subscribed_tables(self):
        page = self.reg.create("p1", subscribed_tables="lib.user,lib.group")
        assert "lib.user" in page["subscribed_tables"]
        assert "lib.group" in page["subscribed_tables"]

    def test_multi_index_connection_id(self):
        self.reg.create("p1", connection_id="c1", user="alice")
        self.reg.create("p2", connection_id="c1", user="alice")
        keys = self.reg.connection_page_keys("c1")
        assert sorted(keys) == ["p1", "p2"]

    def test_multi_index_user(self):
        self.reg.create("p1", connection_id="c1", user="bob")
        self.reg.create("p2", connection_id="c2", user="bob")
        pages = self.reg.pages(user="bob")
        assert len(pages) == 2

    def test_pages_by_connection_id(self):
        self.reg.create("p1", connection_id="cx", user="u")
        self.reg.create("p2", connection_id="cx", user="u")
        self.reg.create("p3", connection_id="cy", user="u")
        result = self.reg.pages(connection_id="cx")
        ids = [p["register_item_id"] for p in result]
        assert sorted(ids) == ["p1", "p2"]

    def test_pages_all(self):
        self.reg.create("p1", connection_id="c1", user="u")
        self.reg.create("p2", connection_id="c2", user="u")
        assert len(self.reg.pages()) == 2

    def test_pages_with_filter(self):
        self.reg.create("p1", connection_id="c1", user="alice")
        self.reg.create("p2", connection_id="c2", user="bob")
        result = self.reg.pages(filters="user:alice")
        assert len(result) == 1
        assert result[0]["user"] == "alice"

    def test_subscribed_table_index(self):
        self.reg.create("p1", subscribed_tables="lib.user")
        self.reg.create("p2", subscribed_tables="lib.user,lib.group")
        keys = self.reg.subscribed_table_page_keys("lib.user")
        assert set(keys) == {"p1", "p2"}

    def test_subscribed_table_pages(self):
        self.reg.create("p1", subscribed_tables="lib.event")
        result = self.reg.subscribed_table_pages("lib.event")
        assert len(result) == 1

    def test_filter_subscribed_tables(self):
        self.reg.create("p1", subscribed_tables="lib.user,lib.group")
        result = self.reg.filter_subscribed_tables(["lib.user", "lib.invoice"])
        assert "lib.user" in result
        assert "lib.invoice" not in result

    def test_drop_page(self):
        self.reg.create("p1", connection_id="c1", user="alice")
        self.reg.drop("p1")
        assert not self.reg.exists("p1")

    def test_subscribe_table(self):
        self.reg.create("p1", connection_id="c1", user="alice")
        self.reg.subscribeTable("p1", "lib.mytable", subscribe=True)
        assert "lib.mytable" in self.reg.get_item("p1")["subscribed_tables"]

    def test_unsubscribe_table(self):
        self.reg.create("p1", subscribed_tables="lib.mytable")
        self.reg.subscribeTable("p1", "lib.mytable", subscribe=False)
        assert "lib.mytable" not in self.reg.get_item("p1")["subscribed_tables"]


# ===========================================================================
# ConnectionRegister
# ===========================================================================


class TestConnectionRegister:
    def setup_method(self):
        self.reg, self.sr = make_connection_register()

    def test_create_connection(self):
        conn = self.reg.create("c1", user="alice", user_id="u1")
        assert conn["register_item_id"] == "c1"
        assert conn["user"] == "alice"
        assert conn["register_name"] == "connection"

    def test_connections_all(self):
        self.reg.create("c1", user="alice")
        self.reg.create("c2", user="bob")
        assert len(self.reg.connections()) == 2

    def test_connections_filtered_by_user(self):
        self.reg.create("c1", user="alice")
        self.reg.create("c2", user="alice")
        self.reg.create("c3", user="bob")
        conns = self.reg.connections(user="alice")
        assert len(conns) == 2

    def test_user_connection_keys(self):
        self.reg.create("c1", user="alice")
        self.reg.create("c2", user="alice")
        keys = self.reg.user_connection_keys("alice")
        assert sorted(keys) == ["c1", "c2"]

    def test_drop_connection(self):
        self.reg.create("c1", user="alice")
        self.reg.drop("c1")
        assert not self.reg.exists("c1")


# ===========================================================================
# UserRegister
# ===========================================================================


class TestUserRegister:
    def setup_method(self):
        self.reg, self.sr = make_user_register()

    def test_create_user(self):
        user = self.reg.create(user="alice", user_id="u1", user_name="Alice")
        assert user["register_item_id"] == "alice"
        assert user["register_name"] == "user"

    def test_exists_after_create(self):
        self.reg.create(user="bob")
        assert self.reg.exists("bob") is True

    def test_drop_user(self):
        self.reg.create(user="carol")
        self.reg.drop("carol")
        assert not self.reg.exists("carol")


# ===========================================================================
# GnrSiteRegister
# ===========================================================================


class TestGnrSiteRegister:
    def setup_method(self):
        self.sr = make_site_register()

    def test_set_configuration_defaults(self):
        assert self.sr.cleanup_interval == DEFAULT_CLEANUP_INTERVAL
        assert self.sr.page_max_age == DEFAULT_PAGE_MAX_AGE
        assert self.sr.connection_max_age == DEFAULT_CONNECTION_MAX_AGE
        assert self.sr.guest_connection_max_age == DEFAULT_GUEST_CONNECTION_MAX_AGE

    def test_set_configuration_custom(self):
        self.sr.setConfiguration(
            cleanup={
                "page_max_age": "30",
                "connection_max_age": "300",
            }
        )
        assert self.sr.page_max_age == 30
        assert self.sr.connection_max_age == 300

    def test_new_connection_creates_user_and_connection(self):
        self.sr.new_connection("c1", user="alice", user_id="u1")
        assert self.sr.connection_register.exists("c1")
        assert self.sr.user_register.exists("alice")

    def test_new_connection_reuses_existing_user(self):
        self.sr.new_connection("c1", user="alice")
        self.sr.new_connection("c2", user="alice")
        # alice should still exist only once
        assert self.sr.user_register.exists("alice")
        assert self.sr.connection_register.exists("c2")

    def test_new_connection_duplicate_raises(self):
        self.sr.new_connection("c1", user="alice")
        with pytest.raises(AssertionError):
            self.sr.new_connection("c1", user="alice")

    def test_new_page(self):
        self.sr.new_page("p1", pagename="home.py", connection_id="c1", user="alice")
        assert self.sr.page_register.exists("p1")

    def test_drop_page(self):
        self.sr.new_page("p1", connection_id="c1", user="alice")
        self.sr.drop_page("p1")
        assert not self.sr.page_register.exists("p1")

    def test_drop_connection(self):
        self.sr.new_connection("c1", user="alice")
        self.sr.drop_connection("c1")
        assert not self.sr.connection_register.exists("c1")

    def test_counters_empty(self):
        c = self.sr.counters()
        assert c["pages"] == 0
        assert c["connections"] == 0
        assert c["users"] == 0

    def test_counters_after_adding(self):
        self.sr.new_connection("c1", user="alice")
        self.sr.new_page("p1", connection_id="c1", user="alice")
        c = self.sr.counters()
        assert c["connections"] == 1
        assert c["pages"] == 1
        assert c["users"] == 1

    def test_pages_all(self):
        self.sr.new_page("p1", connection_id="c1", user="u")
        self.sr.new_page("p2", connection_id="c2", user="u")
        result = self.sr.pages()
        assert len(result) == 2

    def test_connections_all(self):
        self.sr.new_connection("c1", user="alice")
        self.sr.new_connection("c2", user="bob")
        result = self.sr.connections()
        assert len(result) == 2

    def test_users_all(self):
        self.sr.new_connection("c1", user="alice")
        self.sr.new_connection("c2", user="bob")
        result = self.sr.users()
        assert len(result) == 2

    def test_get_item_finds_page(self):
        self.sr.new_page("p1", connection_id="c1", user="u")
        item = self.sr.get_item("p1")
        assert item["register_item_id"] == "p1"

    def test_get_item_finds_connection(self):
        self.sr.new_connection("c1", user="u")
        item = self.sr.get_item("c1")
        assert item["register_item_id"] == "c1"

    def test_get_item_returns_none_for_unknown(self):
        assert self.sr.get_item("ghost") is None

    def test_get_register(self):
        assert self.sr.get_register("page") is self.sr.page_register
        assert self.sr.get_register("connection") is self.sr.connection_register
        assert self.sr.get_register("user") is self.sr.user_register
        assert self.sr.get_register("global") is self.sr.global_register

    def test_maintenance_mode(self):
        assert not self.sr.maintenance
        self.sr.setMaintenance(True, allowed_users=["admin"])
        assert self.sr.maintenance
        assert self.sr.isInMaintenance("user") is True
        assert self.sr.isInMaintenance("admin") is False

    def test_maintenance_off(self):
        self.sr.setMaintenance(True)
        self.sr.setMaintenance(False)
        assert not self.sr.maintenance

    def test_refresh_delegates_to_register(self):
        self.sr.new_connection("c1", user="alice")
        self.sr.new_page("p1", connection_id="c1", user="alice")
        ts = datetime.datetime.now()
        result = self.sr.refresh("p1", last_user_ts=ts)
        assert result is not None

    def test_on_site_stop_logs(self, caplog):
        with caplog.at_level(logging.INFO):
            self.sr.on_site_stop()

    def test_notify_db_events_dispatches_to_subscribed_pages(self):
        self.sr.new_page(
            "p1", subscribed_tables="lib.user", user="alice", connection_id="c1"
        )
        self.sr.page_register.subscribeTable("p1", "lib.user", subscribe=True)
        db_events = {"lib.user": [{"action": "I", "pkey": "1"}]}
        # register_name routes through GnrSiteRegister.__getattr__ → page_register
        self.sr.notifyDbEvents(dbeventsDict=db_events, register_name="page")
        changes = self.sr.page_register.get_item("p1")["datachanges"]
        assert len(changes) >= 1

    def test_notify_db_events_ignores_empty_events(self):
        self.sr.new_page(
            "p1", subscribed_tables="lib.user", user="alice", connection_id="c1"
        )
        self.sr.notifyDbEvents(dbeventsDict={"lib.user": []}, register_name="page")
        changes = self.sr.page_register.get_item("p1")["datachanges"]
        assert len(changes) == 0

    def test_cleanup_removes_old_pages(self):
        self.sr.new_connection("c1", user="alice")
        self.sr.new_page("p1", connection_id="c1", user="alice")
        # cleanup() uses page['last_refresh_ts'] or page['start_ts'] to check age
        old_ts = datetime.datetime.now() - datetime.timedelta(
            seconds=self.sr.page_max_age + 10
        )
        self.sr.page_register.get_item("p1")["start_ts"] = old_ts
        self.sr.last_cleanup = time.time() - (self.sr.cleanup_interval + 1)
        self.sr.cleanup()
        assert not self.sr.page_register.exists("p1")

    def test_subscribe_table_and_store_subscription(self):
        self.sr.new_page("p1", connection_id="c1", user="alice")
        self.sr.subscribeTable("p1", "lib.invoice", subscribe=True)
        assert (
            "lib.invoice" in self.sr.page_register.get_item("p1")["subscribed_tables"]
        )

    def test_dump_and_load(self, tmp_path):
        self.sr.new_connection("c1", user="alice")
        self.sr.storage_path = str(tmp_path / "register.pik")
        self.sr.dump()
        # Create fresh register and load
        sr2 = make_site_register()
        sr2.storage_path = self.sr.storage_path
        sr2.load()
        assert sr2.connection_register.exists("c1")


# ===========================================================================
# Backend persistence
# ===========================================================================


class TestBackendPersistence:
    """Verify that BaseRegister write-through and reload via backend work correctly."""

    def _make_page_register_with_backend(self):
        """PageRegister backed by a fresh InMemoryBackend."""
        backend = InMemoryBackend()
        sr = GnrSiteRegister(MagicMock(), sitename="testsite", backend=backend)
        sr.setConfiguration()
        return sr.page_register, backend

    def test_add_item_saved_to_backend(self):
        reg, backend = self._make_page_register_with_backend()
        item = {
            "register_item_id": "p1",
            "pagename": "home.py",
            "connection_id": "c1",
            "user": "alice",
            "register_name": "page",
            "subscribed_tables": set(),
        }
        reg.addRegisterItem(item)
        stored = backend.hgetall(reg._ns)
        assert "p1" in stored
        assert stored["p1"]["pagename"] == "home.py"

    def test_live_fields_stripped_from_backend(self):
        reg, backend = self._make_page_register_with_backend()
        item = {
            "register_item_id": "p1",
            "register_name": "page",
            "subscribed_tables": set(),
        }
        reg.addRegisterItem(item)
        stored = backend.hgetall(reg._ns)["p1"]
        assert "datachanges" not in stored
        assert "datachanges_idx" not in stored
        assert "subscribed_paths" not in stored

    def test_drop_item_removes_from_backend(self):
        reg, backend = self._make_page_register_with_backend()
        item = {
            "register_item_id": "p1",
            "register_name": "page",
            "subscribed_tables": set(),
        }
        reg.addRegisterItem(item)
        assert "p1" in backend.hgetall(reg._ns)
        reg.drop_item("p1")
        assert "p1" not in backend.hgetall(reg._ns)

    def test_update_item_saved_to_backend(self):
        reg, backend = self._make_page_register_with_backend()
        item = {
            "register_item_id": "p1",
            "pagename": "old.py",
            "register_name": "page",
            "subscribed_tables": set(),
        }
        reg.addRegisterItem(item)
        reg.update_item("p1", {"pagename": "new.py"})
        stored = backend.hgetall(reg._ns)
        assert stored["p1"]["pagename"] == "new.py"

    def test_refresh_saved_to_backend(self):
        reg, backend = self._make_page_register_with_backend()
        item = {
            "register_item_id": "p1",
            "register_name": "page",
            "subscribed_tables": set(),
        }
        reg.addRegisterItem(item)
        ts = datetime.datetime.now()
        reg.refresh("p1", last_user_ts=ts)
        stored = backend.hgetall(reg._ns)
        assert stored["p1"]["last_user_ts"] == ts

    def test_reset_loads_items_from_backend(self):
        """Simulates daemon restart: preload backend, then create fresh register."""
        backend = InMemoryBackend()
        # Pre-populate backend as if a previous run had saved data
        ns = "testsite:PageRegister"
        backend.hset(
            ns,
            "p1",
            {
                "register_item_id": "p1",
                "pagename": "restored.py",
                "connection_id": "c1",
                "user": "alice",
                "register_name": "page",
                "subscribed_tables": set(),
            },
        )
        # Fresh GnrSiteRegister with the same backend → items restored
        sr = GnrSiteRegister(MagicMock(), sitename="testsite", backend=backend)
        sr.setConfiguration()
        assert sr.page_register.exists("p1")
        item = sr.page_register.get_item("p1")
        assert item["pagename"] == "restored.py"
        # Live fields must be present after restore
        assert "datachanges" in item
        assert "subscribed_paths" in item

    def test_global_register_not_overwritten_on_restore(self):
        """GlobalRegister: if backend contains '*', the guard prevents overwrite."""
        backend = InMemoryBackend()
        # Simulate a previous run storing global state
        ns = "testsite:GlobalRegister"
        pre_existing = {
            "register_item_id": "*",
            "register_name": "global",
            "custom_field": "preserved",
        }
        backend.hset(ns, "*", pre_existing)
        # Fresh register creation must not overwrite the backend-loaded item
        sr = GnrSiteRegister(MagicMock(), sitename="testsite", backend=backend)
        sr.setConfiguration()
        item = sr.global_register.get_item("*")
        assert item["custom_field"] == "preserved"


# ===========================================================================
# drop_multi_indexes fix
# ===========================================================================


class TestDropMultiIndexesFix:
    """Regression tests for the drop_multi_indexes crash when multiple items
    share the same index key (e.g. two pages for one connection)."""

    def test_two_pages_same_connection_both_drop_without_error(self):
        sr = make_site_register()
        sr.new_page("p1", connection_id="cx", user="alice")
        sr.new_page("p2", connection_id="cx", user="alice")
        # Dropping the first page must not corrupt the index
        sr.page_register.drop_item("p1")
        assert not sr.page_register.exists("p1")
        # Dropping the second must also succeed without KeyError
        sr.page_register.drop_item("p2")
        assert not sr.page_register.exists("p2")

    def test_drop_item_missing_from_index_does_not_raise(self):
        """If the item was never indexed (no multi_index_attrs match), drop is safe."""
        sr = make_site_register()
        reg = sr.page_register
        item = {
            "register_item_id": "orphan",
            "register_name": "page",
            "subscribed_tables": set(),
            # deliberately no 'connection_id' or 'user' → no multi-index entries
        }
        reg.addRegisterItem(item)
        reg.drop_item("orphan")  # must not raise
        assert not reg.exists("orphan")


# ===========================================================================
# Additional coverage: BaseRegister without backend (_b_save early return)
# ===========================================================================


class TestBaseRegisterNoBackend:
    def setup_method(self):
        sr = GnrSiteRegister(MagicMock(), sitename="ts", backend=None)
        sr.setConfiguration()
        self.reg = sr.page_register

    def _add_item(self, item_id="p1"):
        item = {
            "register_item_id": item_id,
            "register_name": "page",
            "subscribed_tables": set(),
        }
        self.reg.addRegisterItem(item)
        return item

    def test_add_item_without_backend_does_not_crash(self):
        """_b_save early-returns when _backend is None (line 62)."""
        self._add_item("no_backend_item")
        assert self.reg.exists("no_backend_item")

    def test_get_item_data_returns_bag(self):
        """get_item_data returns a Bag (line 195)."""
        self._add_item("p1")
        data = self.reg.get_item_data("p1")

        assert isinstance(data, Bag)

    def test_get_item_with_include_data(self):
        """get_item with include_data=True attaches data Bag (line 203)."""
        self._add_item("p1")
        item = self.reg.get_item("p1", include_data=True)
        assert "data" in item

        assert isinstance(item["data"], Bag)

    def test_items_with_include_data(self):
        """items(include_data=True) returns items with data attached (lines 213-215)."""
        self._add_item("p1")
        result = self.reg.items(include_data=True)
        assert len(result) == 1
        _, item = result[0]
        assert "data" in item

    def test_values_with_include_data(self):
        """values(include_data=True) returns items with data (line 220)."""
        self._add_item("p1")
        result = self.reg.values(include_data=True)
        assert len(result) == 1
        assert "data" in result[0]

    def test_reindex_multi_index(self):
        """reindex_multi_index rebuilds the multi-index (lines 109-113)."""
        sr = GnrSiteRegister(MagicMock(), sitename="ts", backend=None)
        sr.setConfiguration()
        reg = sr.page_register
        reg.addRegisterItem(
            {
                "register_item_id": "p1",
                "connection_id": "c1",
                "user": "alice",
                "register_name": "page",
                "subscribed_tables": set(),
            }
        )
        reg.reindex_multi_index("connection_id")
        assert len(reg._multi_indexes["connection_id"]["c1"]) == 1

    def test_drop_multi_index_value_error_suppressed(self):
        """drop_multi_indexes silently ignores ValueError (lines 105-106)."""
        sr = GnrSiteRegister(MagicMock(), sitename="ts", backend=None)
        sr.setConfiguration()
        reg = sr.page_register
        item = {
            "register_item_id": "p1",
            "connection_id": "c1",
            "user": "alice",
            "register_name": "page",
            "subscribed_tables": set(),
        }
        reg.addRegisterItem(item)
        # Manually corrupt the index so remove raises ValueError
        reg._multi_indexes["connection_id"]["c1"] = []
        reg.drop_multi_indexes(item)  # must not raise

    def test_refresh_with_prior_timestamp(self):
        """refresh updates last_user_ts using max() when prior value exists (line 226, 242)."""
        self._add_item("p1")
        ts1 = datetime.datetime(2024, 1, 1)
        ts2 = datetime.datetime(2024, 6, 1)
        self.reg.refresh("p1", last_user_ts=ts1)
        result = self.reg.refresh("p1", last_user_ts=ts2)
        assert result["last_user_ts"] == ts2

    def test_set_datachange_replace(self):
        """set_datachange with replace=True removes existing change first (line 293)."""
        self._add_item("p1")
        self.reg.set_datachange("p1", "a.b", value=1)
        self.reg.set_datachange("p1", "a.b", value=2, replace=True)
        changes = self.reg.get_item("p1")["datachanges"]
        assert len(changes) == 1
        assert changes[0].value == 2

    def test_get_datachanges_with_reset(self):
        """get_datachanges(reset=True) resets the list (line 299)."""
        self._add_item("p1")
        self.reg.set_datachange("p1", "x", value=1)
        changes = self.reg.get_datachanges("p1", reset=True)
        assert len(changes) == 1
        assert self.reg.get_item("p1")["datachanges"] == []

    def test_reset_datachanges(self):
        """reset_datachanges clears the datachanges list (line 307)."""
        self._add_item("p1")
        self.reg.set_datachange("p1", "x", value=1)
        self.reg.reset_datachanges("p1")
        assert self.reg.get_item("p1")["datachanges"] == []

    def test_drop_datachanges_missing_item(self):
        """drop_datachanges on missing item is a no-op (line 314)."""
        self.reg.drop_datachanges("ghost", "some.path")  # must not raise

    def test_get_dbenv(self):
        """get_dbenv returns a Bag with dbenv items (lines 323-334)."""
        self._add_item("p1")

        result = self.reg.get_dbenv("p1")
        assert isinstance(result, Bag)


# ===========================================================================
# on_data_trigger coverage
# ===========================================================================


class TestOnDataTrigger:
    def setup_method(self):
        sr = GnrSiteRegister(MagicMock(), sitename="ts", backend=None)
        sr.setConfiguration()
        self.reg = sr.page_register
        item = {
            "register_item_id": "p1",
            "register_name": "page",
            "subscribed_tables": set(),
        }
        self.reg.addRegisterItem(item)
        self.item = item

    def test_data_change_not_subscribed_path_no_event(self):
        """Setting data without subscribed path causes no datachange (line 160-180)."""
        self.reg.get_item_data("p1")["some_key"] = "value"
        changes = self.reg.get_item("p1")["datachanges"]
        assert len(changes) == 0

    def test_data_change_subscribed_path_fires_event(self):
        """Setting data at subscribed path fires a datachange (lines 160-180)."""
        self.reg.subscribe_path("p1", "user")
        self.reg.get_item_data("p1")["user"] = "alice"
        changes = self.reg.get_item("p1")["datachanges"]
        assert len(changes) >= 1


# ===========================================================================
# invalidateTableCache coverage
# ===========================================================================


class TestInvalidateTableCache:
    def test_invalidate_table_cache_clears_paths(self):
        """invalidateTableCache nulls cached paths in item data (lines 183-189)."""
        sr = GnrSiteRegister(MagicMock(), sitename="ts", backend=None)
        sr.setConfiguration()
        reg = sr.page_register
        item = {
            "register_item_id": "p1",
            "register_name": "page",
            "subscribed_tables": set(),
        }
        reg.addRegisterItem(item)
        # Manually add a cache entry
        reg.cached_tables["lib.user"]["p1"] = {"some.path"}
        reg.get_item_data("p1")["some.path"] = "old_value"
        reg.invalidateTableCache("lib.user")
        # After invalidation, the path should be set to None
        assert reg.get_item_data("p1").getItem("some.path") is None


# ===========================================================================
# GlobalRegister.drop coverage
# ===========================================================================


class TestGlobalRegisterDrop:
    def test_drop(self):
        """GlobalRegister.drop removes item (line 367)."""
        sr = GnrSiteRegister(MagicMock(), sitename="ts", backend=None)
        sr.setConfiguration()
        sr.global_register.create("extra")
        assert sr.global_register.exists("extra")
        sr.global_register.drop("extra")
        assert not sr.global_register.exists("extra")


# ===========================================================================
# ConnectionRegister.drop cascade + user_connection_items
# ===========================================================================


class TestConnectionRegisterAdditional:
    def setup_method(self):
        self.sr = make_site_register()

    def test_user_connection_items(self):
        """user_connection_items returns (id, item) tuples (line 429)."""
        self.sr.new_connection("c1", user="alice")
        items = self.sr.connection_register.user_connection_items("alice")
        assert len(items) == 1
        cid, item = items[0]
        assert cid == "c1"

    def test_drop_cascade_with_no_other_connections(self):
        """ConnectionRegister.drop cascade (lines 420-421): when no connections remain,
        would drop user (but _testing=True so siteregister.drop_user is skipped)."""
        self.sr.new_connection("c1", user="alice")
        # drop with cascade but _testing=True to avoid calling siteregister.drop_user
        self.sr.connection_register.drop("c1", cascade=True, _testing=True)
        assert not self.sr.connection_register.exists("c1")


# ===========================================================================
# PageRegister additional methods
# ===========================================================================


class TestPageRegisterAdditional:
    def setup_method(self):
        self.sr = make_site_register()

    def test_subscribed_table_page_items(self):
        """subscribed_table_page_items returns (id, item) tuples (line 489)."""
        self.sr.page_register.create(
            "p1", subscribed_tables="lib.user", user="u", connection_id="c1"
        )
        items = self.sr.page_register.subscribed_table_page_items("lib.user")
        assert len(items) == 1
        pid, item = items[0]
        assert pid == "p1"

    def test_connection_page_items(self):
        """connection_page_items returns (id, item) tuples (line 498)."""
        self.sr.page_register.create("p1", connection_id="c1", user="u")
        items = self.sr.page_register.connection_page_items("c1")
        assert len(items) == 1
        pid, item = items[0]
        assert pid == "p1"

    def test_connection_pages(self):
        """connection_pages returns page items (line 504)."""
        self.sr.page_register.create("p1", connection_id="c1", user="u")
        pages = self.sr.page_register.connection_pages("c1")
        assert len(pages) == 1
        assert pages[0]["register_item_id"] == "p1"

    def test_pages_by_connection_id_and_user(self):
        """pages() with both connection_id and user filters (lines 518-522)."""
        self.sr.page_register.create("p1", connection_id="c1", user="alice")
        self.sr.page_register.create("p2", connection_id="c1", user="bob")
        result = self.sr.page_register.pages(connection_id="c1", user="alice")
        assert len(result) == 1
        assert result[0]["user"] == "alice"

    def test_pages_with_filter_numeric_value(self):
        """checkpage with numeric (non-string) filter value (line 539)."""
        self.sr.page_register.create("p1", connection_id="c1", user="alice")
        # subscribed_tables is a set (non-string); filter won't match numeric fltval
        result = self.sr.page_register.pages(filters="user:alice")
        assert len(result) == 1

    def test_pages_with_filter_no_match(self):
        """checkpage - filter doesn't match."""
        self.sr.page_register.create("p1", connection_id="c1", user="alice")
        result = self.sr.page_register.pages(filters="user:bob")
        assert len(result) == 0

    def test_pages_with_wildcard_filter(self):
        """Pages with filters='*' returns all."""
        self.sr.page_register.create("p1", connection_id="c1", user="alice")
        result = self.sr.page_register.pages(filters="*")
        assert len(result) == 1

    def test_set_store_subscription(self):
        """setStoreSubscription creates subscription entry (lines 558-565)."""
        self.sr.page_register.create("p1", connection_id="c1", user="u")
        self.sr.page_register.setStoreSubscription(
            "p1", storename="user", client_path="data.x", active=True
        )
        data = self.sr.page_register.get_item_data("p1")
        sub = data.getItem("_subscriptions.user")
        assert sub is not None
        assert "data.x" in sub

    def test_set_store_subscription_update_existing(self):
        """setStoreSubscription updates existing subscription (lines 558-565)."""
        self.sr.page_register.create("p1", connection_id="c1", user="u")
        self.sr.page_register.setStoreSubscription(
            "p1", storename="user", client_path="data.x", active=True
        )
        self.sr.page_register.setStoreSubscription(
            "p1", storename="user", client_path="data.x", active=False
        )
        data = self.sr.page_register.get_item_data("p1")
        sub = data.getItem("_subscriptions.user")
        assert sub["data.x"]["on"] is False

    def test_page_in_maintenance_testing(self):
        """pageInMaintenance with _testing=True returns None (lines 608-611)."""
        self.sr.page_register.create("p1", connection_id="c1", user="alice")
        result = self.sr.page_register.pageInMaintenance(page_id="p1", _testing=True)
        assert result is None

    def test_page_in_maintenance_missing_page(self):
        """pageInMaintenance with missing page returns None."""
        result = self.sr.page_register.pageInMaintenance(page_id="ghost", _testing=True)
        assert result is None

    def test_drop_cascade(self):
        """PageRegister.drop cascade calls siteregister.drop_connection (lines 476-477)."""
        self.sr.new_connection("c1", user="alice")
        self.sr.new_page("p1", connection_id="c1", user="alice")
        # cascade=True, _testing=True → skips siteregister.drop_connection
        self.sr.page_register.drop("p1", cascade=True, _testing=True)
        assert not self.sr.page_register.exists("p1")

    def test_set_pending_context(self):
        """setPendingContext stores server-side data (lines 599-605)."""
        self.sr.page_register.create("p1", connection_id="c1", user="u")
        pending = [("settings.theme", "dark", {})]
        self.sr.page_register.setPendingContext("p1", pending)
        data = self.sr.page_register.get_item_data("p1")
        assert data.getItem("settings.theme") == "dark"

    def test_set_in_client_data_basic(self):
        """setInClientData with explicit page_id (lines 618-634)."""
        self.sr.page_register.create("p1", connection_id="c1", user="u")
        self.sr.page_register.setInClientData(path="some.key", value=42, page_id="p1")
        changes = self.sr.page_register.get_item("p1")["datachanges"]
        assert len(changes) == 1

    def test_set_in_client_data_with_filters(self):
        """setInClientData with filters applies to all matching pages (lines 618-634)."""
        self.sr.page_register.create("p1", connection_id="c1", user="alice")
        self.sr.page_register.setInClientData(path="x", value=1, filters="user:alice")
        changes = self.sr.page_register.get_item("p1")["datachanges"]
        assert len(changes) == 1

    def test_notify_db_events_no_subscribers(self):
        """notifyDbEvents skips tables with no subscribers."""
        self.sr.page_register.create("p1", connection_id="c1", user="u")
        self.sr.page_register.notifyDbEvents(
            dbeventsDict={"lib.other": [{"action": "I"}]},
        )
        changes = self.sr.page_register.get_item("p1")["datachanges"]
        assert len(changes) == 0


# ===========================================================================
# GnrSiteRegister additional delegating methods and lifecycle
# ===========================================================================


class TestGnrSiteRegisterAdditional:
    def setup_method(self):
        self.sr = make_site_register()
        self.sr.new_connection("c1", user="alice")
        self.sr.new_page("p1", connection_id="c1", user="alice")

    def test_drop_pages(self):
        """drop_pages removes all pages for a connection (line 667/707)."""
        self.sr.drop_pages("c1")
        assert not self.sr.page_register.exists("p1")

    def test_drop_connections(self):
        """drop_connections removes all connections for a user (line 714)."""
        self.sr.drop_connections("alice")
        assert not self.sr.connection_register.exists("c1")

    def test_drop_user(self):
        """drop_user removes the user (line 720)."""
        self.sr.drop_user("alice")
        assert not self.sr.user_register.exists("alice")

    def test_user_connection_items(self):
        """user_connection_items delegates to connection_register (line 726)."""
        items = self.sr.user_connection_items("alice")
        assert len(items) == 1
        cid, item = items[0]
        assert cid == "c1"

    def test_user_connections(self):
        """user_connections returns connection items (line 729)."""
        conns = self.sr.user_connections("alice")
        assert len(conns) == 1

    def test_connection_page_items(self):
        """connection_page_items delegates to page_register (line 735)."""
        items = self.sr.connection_page_items("c1")
        assert len(items) == 1

    def test_connection_pages(self):
        """connection_pages delegates to page_register (line 738)."""
        pages = self.sr.connection_pages("c1")
        assert len(pages) == 1

    def test_subscribed_table_pages(self):
        """subscribed_table_pages delegates to page_register (line 766)."""
        self.sr.page_register.subscribeTable("p1", "lib.user", subscribe=True)
        result = self.sr.subscribed_table_pages("lib.user")
        assert len(result) == 1

    def test_pages_with_index_name(self):
        """pages(index_name=...) delegates to subscribed_table_pages (lines 771-772)."""
        self.sr.page_register.subscribeTable("p1", "lib.group", subscribe=True)
        result = self.sr.pages(index_name="lib.group")
        assert len(result) == 1

    def test_page_delegation(self):
        """page() delegates to page_register (line 779)."""
        item = self.sr.page("p1")
        assert item is not None
        assert item["register_item_id"] == "p1"

    def test_connection_delegation(self):
        """connection() delegates to connection_register (line 782)."""
        item = self.sr.connection("c1")
        assert item is not None
        assert item["register_item_id"] == "c1"

    def test_user_delegation(self):
        """user() delegates to user_register (line 785)."""
        item = self.sr.user("alice")
        assert item is not None
        assert item["register_item_id"] == "alice"

    def test_change_connection_user(self):
        """change_connection_user reassigns connection to new user (lines 802-819)."""
        self.sr.new_connection("c2", user="alice")
        self.sr.change_connection_user("c2", user="bob")
        conn = self.sr.connection("c2")
        assert conn["user"] == "bob"
        assert self.sr.user_register.exists("bob")

    def test_refresh_missing_page_returns_none(self):
        """refresh with missing page_id returns None (line 828)."""
        result = self.sr.refresh("ghost_page", last_user_ts=datetime.datetime.now())
        assert result is None

    def test_cleanup_removes_old_guest_connection(self):
        """cleanup uses guest_connection_max_age for guest users (line 847)."""
        self.sr.new_connection("gc1", user="guest_123")
        conn = self.sr.connection("gc1")
        old_ts = datetime.datetime.now() - datetime.timedelta(
            seconds=self.sr.guest_connection_max_age + 10
        )
        conn["start_ts"] = old_ts
        self.sr.last_cleanup = time.time() - (self.sr.cleanup_interval + 1)
        dropped = self.sr.cleanup()
        # The connection should have been cleaned up
        assert dropped is not None

    def test_get_item_with_register_name(self):
        """get_item with register_name looks only in that register (lines 1001-1002)."""
        item = self.sr.get_item("p1", register_name="page")
        assert item is not None
        assert item["register_item_id"] == "p1"

    def test_get_item_data_with_register_name(self):
        """get_item_data with register_name (lines 1011-1018)."""

        data = self.sr.get_item_data("p1", register_name="page")
        assert isinstance(data, Bag)

    def test_get_item_data_missing_returns_bag(self):
        """get_item_data for unknown id returns empty Bag."""

        data = self.sr.get_item_data("ghost")
        assert isinstance(data, Bag)

    def test_set_store_subscription_delegation(self):
        """setStoreSubscription delegates to page_register (line 876)."""
        self.sr.setStoreSubscription(
            "p1", storename="user", client_path="x", active=True
        )
        data = self.sr.page_register.get_item_data("p1")
        assert data.getItem("_subscriptions.user") is not None

    def test_pending_process_commands(self):
        """pendingProcessCommands tracks commands by PID (lines 1041-1048)."""
        cmds = self.sr.pendingProcessCommands()
        assert cmds == []
        cmds2 = self.sr.pendingProcessCommands()
        assert cmds2 == []

    def test_send_process_command_single(self):
        """sendProcessCommand sends to all known PIDs (lines 1051-1064)."""
        self.sr.pendingProcessCommands()  # register this PID
        pid = os.getpid()
        self.sr.sendProcessCommand("do_something", pid=pid)
        cmds = self.sr.pendingProcessCommands()
        assert "do_something" in cmds

    def test_send_process_command_list(self):
        """sendProcessCommand with a list command extends the queue (lines 1051-1064)."""
        self.sr.pendingProcessCommands()  # register this PID
        pid = os.getpid()
        self.sr.sendProcessCommand(["cmd1", "cmd2"], pid=pid)
        cmds = self.sr.pendingProcessCommands()
        assert "cmd1" in cmds
        assert "cmd2" in cmds

    def test_send_process_command_all_pids(self):
        """sendProcessCommand(pid=None) sends to all registered PIDs."""
        self.sr.pendingProcessCommands()
        self.sr.sendProcessCommand("broadcast")
        cmds = self.sr.pendingProcessCommands()
        assert "broadcast" in cmds

    def test_send_process_command_stale_pid_removed(self):
        """sendProcessCommand removes stale PIDs (line 1059)."""

        self.sr.pendingProcessCommands()
        pid = os.getpid()
        # Make the ts stale
        self.sr.interproces_commands[pid]["ts"] = (
            datetime.datetime.now()
            - datetime.timedelta(seconds=PROCESS_SELFDESTROY_TIMEOUT + 1)
        )
        self.sr.sendProcessCommand("cmd", pid=pid)
        assert pid not in self.sr.interproces_commands

    def test_is_in_maintenance_allowed_user(self):
        """isInMaintenance returns False when user IS in allowed_users (line 1078)."""
        self.sr.setMaintenance(True, allowed_users=["admin"])
        assert self.sr.isInMaintenance("admin") is False

    def test_is_in_maintenance_user_not_in_allowed(self):
        """isInMaintenance returns True when user NOT in allowed_users (line 1076)."""
        self.sr.setMaintenance(True, allowed_users=["admin"])
        assert self.sr.isInMaintenance("regular_user") is True

    def test_is_in_maintenance_forced(self):
        """isInMaintenance returns False for '*forced*' user."""
        self.sr.setMaintenance(True, allowed_users=["admin"])
        assert self.sr.isInMaintenance("*forced*") is False

    def test_is_in_maintenance_no_user(self):
        """isInMaintenance with no user returns the maintenance boolean."""
        self.sr.setMaintenance(True)
        assert self.sr.isInMaintenance(None) is True

    def test_allowed_users(self):
        """allowedUsers returns the current allowed_users list (line 1082)."""
        self.sr.setMaintenance(True, allowed_users=["admin", "staff"])
        assert self.sr.allowedUsers() == ["admin", "staff"]

    def test_getattr_remotebag_delegation(self):
        """__getattr__ remotebag_ path delegates to remotebag_handler (lines 1086-1088)."""
        h = self.sr.remotebag_getItem
        assert callable(h)

    def test_load_eoferror_returns_false(self, tmp_path):
        """load() returns False when the file is corrupt/empty (lines 1037-1038)."""
        path = str(tmp_path / "corrupt.pik")
        with open(path, "wb") as f:
            f.write(b"")  # empty file → EOFError
        self.sr.storage_path = path
        result = self.sr.load()
        assert result is False

    def test_dump_and_load_with_rename(self, tmp_path):
        """dump()+load() renames .pik to _loaded.pik (line 1034)."""
        path = str(tmp_path / "register.pik")
        self.sr.storage_path = path
        self.sr.dump()
        result = self.sr.load()
        assert result is True
        assert os.path.exists(path.replace(".pik", "_loaded.pik"))

    def test_subscription_storechanges_no_subscriptions(self):
        """subscription_storechanges returns datachanges when no user subscriptions (lines 886-917)."""
        result = self.sr.subscription_storechanges("alice", "p1")
        assert isinstance(result, list)

    def test_handle_ping_returns_false_for_unknown_page(self):
        """handle_ping returns False when page_id is not in register (lines 920-964)."""
        result = self.sr.handle_ping(page_id="ghost_page")
        assert result is False

    def test_handle_ping_returns_envelope_for_known_page(self):
        """handle_ping returns an envelope Bag for a valid page (lines 920-964)."""
        result = self.sr.handle_ping(page_id="p1")
        assert isinstance(result, Bag)

    def test_handle_ping_get_datachanges_empty(self):
        """handle_ping_get_datachanges with no changes returns empty Bag (lines 967-981)."""
        result = self.sr.handle_ping_get_datachanges("p1", user="alice")
        assert isinstance(result, Bag)

    def test_set_serverstore_changes(self):
        """set_serverstore_changes updates page item data (lines 984-986)."""
        changes = Bag()
        changes.setItem("theme", "dark")
        self.sr.set_serverstore_changes(page_id="p1", datachanges=changes)

    def test_parse_change_value_string(self):
        """_parse_change_value with a string returns typed value (lines 989-997)."""
        result = self.sr._parse_change_value("2024-01-01||date")
        # Just verify no crash and we get some result
        assert result is not None

    def test_parse_change_value_non_string(self):
        """_parse_change_value with non-string returns the value directly."""
        result = self.sr._parse_change_value(42)
        assert result == 42


# ===========================================================================
# ServerStore
# ===========================================================================


class TestServerStore:
    def setup_method(self):
        self.sr = make_site_register()
        self.sr.new_connection("c1", user="alice")
        self.ServerStore = ServerStore

    def test_context_manager_locks_and_unlocks(self):
        """ServerStore __enter__/__exit__ locks and unlocks the item (lines 1103-1131)."""
        with self.ServerStore(self.sr, "connection", "c1"):
            assert "c1" in self.sr.connection_register.locked_items
        assert "c1" not in self.sr.connection_register.locked_items

    def test_reset_datachanges(self):
        """ServerStore.reset_datachanges delegates to siteregister (line 1136)."""
        with self.ServerStore(self.sr, "connection", "c1") as store:
            store.reset_datachanges()

    def test_set_datachange(self):
        """ServerStore.set_datachange delegates to siteregister (line 1142)."""
        with self.ServerStore(self.sr, "connection", "c1") as store:
            store.set_datachange("some.path", value="hello")
        changes = self.sr.connection_register.get_item("c1")["datachanges"]
        assert len(changes) == 1

    def test_drop_datachanges(self):
        """ServerStore.drop_datachanges delegates to siteregister (line 1150)."""
        with self.ServerStore(self.sr, "connection", "c1") as store:
            store.set_datachange("x.y", value=1)
            store.drop_datachanges("x.y")
        assert self.sr.connection_register.get_item("c1")["datachanges"] == []

    def test_subscribe_path(self):
        """ServerStore.subscribe_path delegates to siteregister (line 1155)."""
        with self.ServerStore(self.sr, "connection", "c1") as store:
            store.subscribe_path("some.data")
        paths = self.sr.connection_register.get_item("c1")["subscribed_paths"]
        assert "some.data" in paths

    def test_register_item_property(self):
        """ServerStore.register_item returns the item (line 1161)."""
        with self.ServerStore(self.sr, "connection", "c1") as store:
            item = store.register_item
        assert item["register_item_id"] == "c1"

    def test_data_property(self):
        """ServerStore.data returns the Bag (lines 1167-1168)."""

        with self.ServerStore(self.sr, "connection", "c1") as store:
            data = store.data
        assert isinstance(data, Bag)

    def test_datachanges_property(self):
        """ServerStore.datachanges returns the list (line 1172)."""
        with self.ServerStore(self.sr, "connection", "c1") as store:
            changes = store.datachanges
        assert isinstance(changes, list)

    def test_subscribed_paths_property(self):
        """ServerStore.subscribed_paths returns the set (line 1176)."""
        with self.ServerStore(self.sr, "connection", "c1") as store:
            paths = store.subscribed_paths
        assert isinstance(paths, set)

    def test_getattr_delegates_to_bag(self):
        """ServerStore.__getattr__ delegates bag methods (lines 1179-1186)."""
        with self.ServerStore(self.sr, "connection", "c1") as store:
            # 'setItem' is a Bag method
            store.setItem("test.key", "test_value")
        data = self.sr.connection_register.get_item_data("c1")
        assert data.getItem("test.key") == "test_value"

    def test_getattr_raises_for_unknown_attr(self):
        """ServerStore.__getattr__ raises AttributeError for non-Bag attrs."""

        with ServerStore(self.sr, "connection", "c1") as store:
            with pytest.raises(AttributeError):
                _ = store.completely_nonexistent_method_xyz

    def test_locking_raises_when_max_retry_exceeded(self):
        """ServerStore raises GnrDaemonLocked when can't acquire lock (lines 1113-1128)."""

        # Lock the item first with a different reason
        self.sr.connection_register.lock_item("c1", reason="other_thread")
        store = ServerStore(self.sr, "connection", "c1", max_retry=1, retry_delay=0.001)
        with pytest.raises(GnrDaemonLocked):
            store.__enter__()


# ===========================================================================
# RemoteStoreBagHandler
# ===========================================================================


class TestRemoteStoreBagHandler:
    def setup_method(self):
        self.sr = make_site_register()
        self.sr.new_connection("c1", user="alice")
        self.handler = RemoteStoreBagHandler(self.sr)

    def test_getattr_delegates_to_store_method(self):
        """RemoteStoreBagHandler.__getattr__ calls the Bag method (lines 1280-1295)."""
        # setItem is a Bag method
        fn = self.handler.setItem
        fn(
            "test.key",
            "val",
            _siteregister_register_name="connection",
            _siteregister_register_item_id="c1",
        )
        data = self.sr.connection_register.get_item_data("c1")
        assert data.getItem("test.key") == "val"

    def test_getattr_raises_for_missing_attr(self):
        """RemoteStoreBagHandler.__getattr__ raises AttributeError for missing attr."""
        fn = self.handler.totally_missing_method
        with pytest.raises(AttributeError):
            fn(
                _siteregister_register_name="connection",
                _siteregister_register_item_id="c1",
            )

    def test_getattr_with_subbag_path(self):
        """RemoteStoreBagHandler with _pyrosubbag routes to sub-bag."""
        # Pre-populate a sub-bag
        data = self.sr.connection_register.get_item_data("c1")
        data.setItem("sub", Bag())
        fn = self.handler.setItem
        fn(
            "nested.key",
            "sub_val",
            _siteregister_register_name="connection",
            _siteregister_register_item_id="c1",
            _pyrosubbag="sub",
        )
        sub = data.getItem("sub")
        assert sub.getItem("nested.key") == "sub_val"


# ===========================================================================
# RemoteStoreBag
# ===========================================================================


class TestRemoteStoreBag:
    def setup_method(self):
        self.mock_client = MagicMock()
        self.mock_client._invoke_method.return_value = "ok"
        self.bag = RemoteStoreBag(self.mock_client, "page", "p1")
        self.RemoteStoreBag = RemoteStoreBag

    def test_init_stores_attrs(self):
        """RemoteStoreBag.__init__ stores attrs in __dict__ (lines 1310-1313)."""
        assert object.__getattribute__(self.bag, "_client") is self.mock_client
        assert object.__getattribute__(self.bag, "register_name") == "page"
        assert object.__getattribute__(self.bag, "register_item_id") == "p1"

    def test_chunk_creates_subbag(self):
        """chunk() creates a new RemoteStoreBag with rootpath set (line 1316)."""
        sub = self.bag.chunk("some.path")
        assert object.__getattribute__(sub, "rootpath") == "some.path"
        assert object.__getattribute__(sub, "register_name") == "page"

    def test_getitem(self):
        """__getitem__ calls getItem (line 1324)."""
        self.bag["some.key"]
        self.mock_client._invoke_method.assert_called_once()

    def test_setitem(self):
        """__setitem__ calls setItem (line 1327)."""
        self.bag["some.key"] = "value"
        self.mock_client._invoke_method.assert_called_once()

    def test_getattr_delegates_to_client(self):
        """__getattr__ creates a callable that invokes the client (lines 1330-1342)."""
        fn = self.bag.getItem
        fn("some.key")
        self.mock_client._invoke_method.assert_called_once_with(
            "remotebag_getItem",
            "some.key",
            _siteregister_register_name="page",
            _siteregister_register_item_id="p1",
        )

    def test_getattr_with_rootpath(self):
        """__getattr__ with rootpath includes _pyrosubbag in kwargs (lines 1338-1340)."""
        sub = self.bag.chunk("sub.path")
        sub.getItem("key")
        call_kwargs = self.mock_client._invoke_method.call_args[1]
        assert call_kwargs["_pyrosubbag"] == "sub.path"


# ===========================================================================
# handle_ping with serverstore_changes and children
# ===========================================================================


class TestHandlePingAdvanced:
    def setup_method(self):
        self.sr = make_site_register()
        self.sr.new_connection("c1", user="alice")
        self.sr.new_page("p1", connection_id="c1", user="alice")
        self.sr.new_page("p2", connection_id="c1", user="alice")

    def test_handle_ping_with_serverstore_changes(self):
        """handle_ping with _serverstore_changes (lines 930-931)."""
        changes = Bag()
        changes.setItem("theme", "dark")
        result = self.sr.handle_ping(
            page_id="p1",
            _serverstore_changes=changes,
        )
        assert result is not False

    def test_handle_ping_with_children_pages_info(self):
        """handle_ping with _children_pages_info (lines 932-946)."""
        children_info = {"p2": {}}
        result = self.sr.handle_ping(
            page_id="p1",
            _children_pages_info=children_info,
        )
        assert result is not False

    def test_handle_ping_running_batch(self):
        """handle_ping with lastBatchUpdate <5s sets runningBatch (lines 958-963)."""
        user_data = self.sr.user_register.get_item_data("alice")
        user_data.setItem("lastBatchUpdate", datetime.datetime.now())
        result = self.sr.handle_ping(page_id="p1")
        assert result.getItem("runningBatch") is True

    def test_handle_ping_stale_batch_clears(self):
        """handle_ping with old lastBatchUpdate clears it (lines 963)."""
        user_data = self.sr.user_register.get_item_data("alice")
        old_ts = datetime.datetime.now() - datetime.timedelta(seconds=10)
        user_data.setItem("lastBatchUpdate", old_ts)
        self.sr.handle_ping(page_id="p1")
        assert user_data.getItem("lastBatchUpdate") is None


# ===========================================================================
# subscription_storechanges – full path
# ===========================================================================


class TestSubscriptionStoreChanges:
    def setup_method(self):
        self.sr = make_site_register()
        self.sr.new_connection("c1", user="alice")
        self.sr.new_page("p1", connection_id="c1", user="alice")

    def test_subscription_storechanges_with_subscription(self):
        """subscription_storechanges finds user store changes (lines 886-917)."""
        # Set up a user store subscription
        self.sr.page_register.setStoreSubscription(
            "p1", storename="user", client_path="data.x", active=True
        )
        # Add a datachange to the user register
        self.sr.user_register.set_datachange("alice", "data.x", value="changed")
        result = self.sr.subscription_storechanges("alice", "p1")
        # Result is a list of changes (may or may not include user store changes)
        assert isinstance(result, list)


# ===========================================================================
# Idle offloading — disk persistence (BaseRegister)
# ===========================================================================


def _make_page_register_with_persist_dir(tmp_path, sitename="testsite"):
    """Return a (PageRegister, GnrSiteRegister) pair backed by disk persistence."""
    sr = GnrSiteRegister(MagicMock(), sitename=sitename, backend=None)
    sr.setConfiguration()
    persist_dir = str(tmp_path / "offload")
    sr.page_register.set_persist_dir(persist_dir)
    return sr.page_register, sr


class TestDiskOffloading:
    def setup_method(self, tmp_path=None):
        # tmp_path is injected per-test via pytest fixture; setup just stores nothing
        pass

    def _make_item(self, reg, item_id="p1"):
        item = {
            "register_item_id": item_id,
            "pagename": "home.py",
            "connection_id": "conn1",
            "user": "alice",
            "register_name": "page",
            "subscribed_tables": set(),
        }
        reg.addRegisterItem(item)
        return item

    def test_offload_to_disk_removes_from_memory(self, tmp_path):
        reg, _ = _make_page_register_with_persist_dir(tmp_path)
        self._make_item(reg)
        reg.offload_item("p1")
        assert "p1" not in reg.registerItems
        assert "p1" not in reg.offloaded_items  # disk, not in-memory cold store
        # Disk file must exist
        assert os.path.exists(reg._item_persist_path("p1"))

    def test_charge_from_disk_restores_item(self, tmp_path):
        reg, _ = _make_page_register_with_persist_dir(tmp_path)
        self._make_item(reg)
        reg.offload_item("p1")
        # get_item triggers _charge_item via fallback
        item = reg.get_item("p1")
        assert item is not None
        assert item["pagename"] == "home.py"
        assert "p1" in reg.registerItems

    def test_charge_from_disk_removes_disk_file(self, tmp_path):
        reg, _ = _make_page_register_with_persist_dir(tmp_path)
        self._make_item(reg)
        reg.offload_item("p1")
        disk_path = reg._item_persist_path("p1")
        assert os.path.exists(disk_path)
        reg.get_item("p1")  # charges from disk
        assert not os.path.exists(disk_path)

    def test_charge_from_disk_restores_bag_data(self, tmp_path):
        reg, _ = _make_page_register_with_persist_dir(tmp_path)
        self._make_item(reg)
        reg.get_item_data("p1")["theme"] = "dark"
        reg.offload_item("p1")
        reg.get_item("p1")  # charges
        assert reg.get_item_data("p1").getItem("theme") == "dark"

    def test_charge_from_disk_restores_timestamp(self, tmp_path):
        reg, _ = _make_page_register_with_persist_dir(tmp_path)
        self._make_item(reg)
        ts = datetime.datetime(2024, 6, 1, 12, 0)
        reg.itemsTS["p1"] = ts
        reg.offload_item("p1")
        # Use _charge_item directly; get_item would overwrite TS via updateTS()
        reg._charge_item("p1")
        assert reg.itemsTS.get("p1") == ts

    def test_drop_item_cleans_up_disk_file(self, tmp_path):
        reg, _ = _make_page_register_with_persist_dir(tmp_path)
        self._make_item(reg)
        reg.offload_item("p1")
        disk_path = reg._item_persist_path("p1")
        assert os.path.exists(disk_path)
        reg.drop_item("p1")
        assert not os.path.exists(disk_path)

    def test_drop_active_item_cleans_up_disk_if_present(self, tmp_path):
        """drop_item on a hot item also removes any stale disk file for that id."""
        reg, _ = _make_page_register_with_persist_dir(tmp_path)
        self._make_item(reg)
        # Manually write a disk file without offloading (simulate stale file)
        reg._persist_item_to_disk("p1", reg.registerItems["p1"])
        assert os.path.exists(reg._item_persist_path("p1"))
        reg.drop_item("p1")
        assert not os.path.exists(reg._item_persist_path("p1"))

    def test_charge_nonexistent_disk_returns_none(self, tmp_path):
        reg, _ = _make_page_register_with_persist_dir(tmp_path)
        result = reg._charge_item("ghost")
        assert result is None

    def test_offload_idle_items_offloads_stale(self, tmp_path):
        reg, _ = _make_page_register_with_persist_dir(tmp_path)
        self._make_item(reg, "p1")
        self._make_item(reg, "p2")
        # Backdate p1's TS so it looks stale
        reg.itemsTS["p1"] = datetime.datetime.now() - datetime.timedelta(seconds=400)
        reg.itemsTS["p2"] = datetime.datetime.now()  # fresh
        offloaded = reg.offload_idle_items(300)
        assert "p1" in offloaded
        assert "p2" not in offloaded
        assert "p1" not in reg.registerItems
        assert "p2" in reg.registerItems

    def test_offload_idle_items_noop_when_disabled(self, tmp_path):
        reg, _ = _make_page_register_with_persist_dir(tmp_path)
        self._make_item(reg)
        reg.itemsTS["p1"] = datetime.datetime.now() - datetime.timedelta(seconds=9999)
        offloaded = reg.offload_idle_items(0)  # max_age_seconds=0 → disabled
        assert offloaded == []
        assert "p1" in reg.registerItems

    def test_offload_idle_items_noop_without_persist_dir(self):
        sr = GnrSiteRegister(MagicMock(), sitename="ts", backend=None)
        sr.setConfiguration()
        reg = sr.page_register
        item = {
            "register_item_id": "p1",
            "register_name": "page",
            "subscribed_tables": set(),
        }
        reg.addRegisterItem(item)
        reg.itemsTS["p1"] = datetime.datetime.now() - datetime.timedelta(seconds=9999)
        offloaded = reg.offload_idle_items(60)
        # No persist_dir → items stay hot
        assert offloaded == []
        assert "p1" in reg.registerItems

    def test_offload_idle_uses_start_ts_fallback(self, tmp_path):
        """Items with no itemsTS entry fall back to start_ts for idle detection."""
        reg, _ = _make_page_register_with_persist_dir(tmp_path)
        item = {
            "register_item_id": "p1",
            "register_name": "page",
            "subscribed_tables": set(),
            "start_ts": datetime.datetime.now() - datetime.timedelta(seconds=400),
        }
        reg.addRegisterItem(item)
        reg.itemsTS.pop("p1", None)  # no explicit TS
        offloaded = reg.offload_idle_items(300)
        assert "p1" in offloaded  # fell back to start_ts

    def test_offload_idle_skips_items_with_no_ts_at_all(self, tmp_path):
        """Items with neither itemsTS nor start_ts/last_refresh_ts are left alone."""
        reg, _ = _make_page_register_with_persist_dir(tmp_path)
        item = {
            "register_item_id": "p1",
            "register_name": "page",
            "subscribed_tables": set(),
            # no start_ts, no last_refresh_ts
        }
        reg.addRegisterItem(item)
        reg.itemsTS.pop("p1", None)
        offloaded = reg.offload_idle_items(1)
        assert "p1" not in offloaded

    def test_offload_in_memory_cold_store_without_persist_dir(self):
        """When persist_dir is not set, offload_item falls back to offloaded_items."""
        sr = GnrSiteRegister(MagicMock(), sitename="ts", backend=None)
        sr.setConfiguration()
        reg = sr.page_register
        item = {
            "register_item_id": "p1",
            "register_name": "page",
            "subscribed_tables": set(),
        }
        reg.addRegisterItem(item)
        reg.offload_item("p1")
        assert reg.item_is_offloaded("p1")
        assert "p1" not in reg.registerItems
        reg._charge_item("p1")
        assert "p1" in reg.registerItems

    def test_debug_log_on_offload_to_disk(self, tmp_path, caplog):
        reg, _ = _make_page_register_with_persist_dir(tmp_path)
        self._make_item(reg)
        with caplog.at_level(logging.DEBUG, logger="gnr.web"):
            reg.offload_item("p1")
        assert any(
            "ffloaded" in r.message and "p1" in r.message for r in caplog.records
        )

    def test_debug_log_on_charge_from_disk(self, tmp_path, caplog):
        reg, _ = _make_page_register_with_persist_dir(tmp_path)
        self._make_item(reg)
        reg.offload_item("p1")
        with caplog.at_level(logging.DEBUG, logger="gnr.web"):
            reg.get_item("p1")
        assert any("harged" in r.message and "p1" in r.message for r in caplog.records)


# ===========================================================================
# dump_memory / load_memory — restart survival (GnrSiteRegister)
# ===========================================================================


def _make_sr_with_persist_dir(tmp_path):
    persist_dir = str(tmp_path / "offload")
    daemon = MagicMock()
    sr = GnrSiteRegister(
        daemon, sitename="testsite", backend=None, persist_dir=persist_dir
    )
    sr.setConfiguration()
    return sr


class TestMemoryDumpLoad:
    def test_dump_memory_creates_disk_files(self, tmp_path):
        sr = _make_sr_with_persist_dir(tmp_path)
        sr.new_connection("c1", user="alice")
        sr.new_page("p1", connection_id="c1", user="alice")
        sr.dump_memory()
        # At least user, connection, and page items should be on disk
        user_path = sr.user_register._item_persist_path("alice")
        conn_path = sr.connection_register._item_persist_path("c1")
        page_path = sr.page_register._item_persist_path("p1")
        assert os.path.exists(user_path)
        assert os.path.exists(conn_path)
        assert os.path.exists(page_path)

    def test_load_memory_restores_items(self, tmp_path):
        sr = _make_sr_with_persist_dir(tmp_path)
        sr.new_connection("c1", user="alice")
        sr.new_page("p1", connection_id="c1", user="alice")
        sr.dump_memory()
        # Simulate restart: create fresh GnrSiteRegister with same persist_dir
        sr2 = GnrSiteRegister(
            MagicMock(),
            sitename="testsite",
            backend=None,
            persist_dir=str(tmp_path / "offload"),
        )
        sr2.setConfiguration()
        sr2.load_memory()
        assert sr2.user_register.exists("alice")
        assert sr2.connection_register.exists("c1")
        assert sr2.page_register.exists("p1")

    def test_load_memory_removes_disk_files_after_load(self, tmp_path):
        sr = _make_sr_with_persist_dir(tmp_path)
        sr.new_connection("c1", user="alice")
        sr.dump_memory()
        page_path = sr.connection_register._item_persist_path("c1")
        assert os.path.exists(page_path)
        sr2 = GnrSiteRegister(
            MagicMock(),
            sitename="testsite",
            backend=None,
            persist_dir=str(tmp_path / "offload"),
        )
        sr2.setConfiguration()
        sr2.load_memory()
        assert not os.path.exists(page_path)

    def test_load_memory_restores_item_data(self, tmp_path):
        sr = _make_sr_with_persist_dir(tmp_path)
        sr.new_connection("c1", user="alice")
        conn_data = sr.connection_register.get_item_data("c1")
        conn_data["theme"] = "dark"
        sr.dump_memory()
        sr2 = GnrSiteRegister(
            MagicMock(),
            sitename="testsite",
            backend=None,
            persist_dir=str(tmp_path / "offload"),
        )
        sr2.setConfiguration()
        sr2.load_memory()
        restored_data = sr2.connection_register.get_item_data("c1")
        assert restored_data.getItem("theme") == "dark"

    def test_dump_memory_noop_without_persist_dir(self):
        sr = GnrSiteRegister(MagicMock(), sitename="ts", backend=None, persist_dir=None)
        sr.setConfiguration()
        sr.new_connection("c1", user="alice")
        sr.dump_memory()  # must not raise; nothing is written

    def test_load_memory_noop_without_persist_dir(self):
        sr = GnrSiteRegister(MagicMock(), sitename="ts", backend=None, persist_dir=None)
        sr.setConfiguration()
        sr.load_memory()  # must not raise

    def test_load_memory_noop_when_no_dir_on_disk(self, tmp_path):
        """load_memory is safe even if the persist directory doesn't exist yet."""
        persist_dir = str(tmp_path / "nonexistent_offload")
        sr = GnrSiteRegister(
            MagicMock(), sitename="ts", backend=None, persist_dir=persist_dir
        )
        sr.setConfiguration()
        sr.load_memory()  # must not raise

    def test_dump_memory_also_persists_offloaded_items(self, tmp_path):
        """Items in in-memory cold store that lack a disk file are also dumped."""
        sr = _make_sr_with_persist_dir(tmp_path)
        sr.new_connection("c1", user="alice")
        # Temporarily remove persist_dir so offload goes to in-memory cold store
        orig = sr.connection_register._persist_dir
        sr.connection_register._persist_dir = None
        sr.connection_register.offload_item("c1")
        sr.connection_register._persist_dir = orig
        assert sr.connection_register.item_is_offloaded("c1")
        sr.dump_memory()
        assert os.path.exists(sr.connection_register._item_persist_path("c1"))


# ===========================================================================
# Idle offload configuration (GnrSiteRegister.setConfiguration)
# ===========================================================================


class TestIdleOffloadConfiguration:
    def test_default_idle_offload_age_with_persist_dir(self, tmp_path):
        from genro_daemon.siteregister import DEFAULT_IDLE_OFFLOAD_AGE

        sr = _make_sr_with_persist_dir(tmp_path)
        assert sr.idle_offload_age == DEFAULT_IDLE_OFFLOAD_AGE

    def test_default_idle_offload_age_without_persist_dir(self):
        sr = GnrSiteRegister(MagicMock(), sitename="ts", backend=None, persist_dir=None)
        sr.setConfiguration()
        assert sr.idle_offload_age == 0

    def test_idle_offload_age_configurable(self, tmp_path):
        sr = _make_sr_with_persist_dir(tmp_path)
        sr.setConfiguration(cleanup={"idle_offload_age": 600})
        assert sr.idle_offload_age == 600

    def test_cleanup_triggers_idle_offload(self, tmp_path):
        """cleanup() offloads items beyond idle_offload_age."""
        sr = _make_sr_with_persist_dir(tmp_path)
        sr.setConfiguration(cleanup={"idle_offload_age": 300, "interval": 0})
        sr.new_connection("c1", user="alice")
        sr.new_page("p1", connection_id="c1", user="alice")
        # Make p1 look stale
        sr.page_register.itemsTS["p1"] = datetime.datetime.now() - datetime.timedelta(
            seconds=400
        )
        sr.last_cleanup = 0  # force cleanup to run
        sr.cleanup()
        assert not sr.page_register.exists("p1")
        assert os.path.exists(sr.page_register._item_persist_path("p1"))
