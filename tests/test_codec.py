"""Tests for genro_daemon.codec – msgpack serialisation helpers."""

import datetime
import pickle


# Module-level class so it can be pickled (local classes can't be pickled)
class _PicklableObj:
    val = 42


import msgpack  # noqa: E402

from genro_daemon.codec import _msgpack_default, _msgpack_object_hook  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def roundtrip(value):
    """Pack *value* with our codec and unpack it back."""
    packed = msgpack.packb(value, default=_msgpack_default, use_bin_type=True)
    return msgpack.unpackb(packed, raw=False, object_hook=_msgpack_object_hook)


# ---------------------------------------------------------------------------
# _msgpack_default
# ---------------------------------------------------------------------------


class TestMsgpackDefault:
    def test_datetime_returns_iso_dict(self):
        dt = datetime.datetime(2024, 6, 15, 12, 30, 0)
        result = _msgpack_default(dt)
        assert result == {"__datetime__": "2024-06-15T12:30:00"}

    def test_date_returns_iso_dict(self):
        d = datetime.date(2024, 6, 15)
        result = _msgpack_default(d)
        assert result == {"__date__": "2024-06-15"}

    def test_set_returns_list_dict(self):
        s = {1, 2, 3}
        result = _msgpack_default(s)
        assert "__set__" in result
        assert set(result["__set__"]) == s

    def test_bytes_returns_string(self):
        result = _msgpack_default(b"hello")
        assert result == "hello"

    def test_bytes_with_non_utf8(self):
        # Should not raise; non-UTF8 bytes are replaced
        result = _msgpack_default(b"\xff\xfe")
        assert isinstance(result, str)

    def test_unknown_object_is_pickled(self):
        obj = _PicklableObj()
        result = _msgpack_default(obj)
        assert "__pickled__" in result
        restored = pickle.loads(result["__pickled__"])
        assert restored.val == 42

    def test_unpicklable_object_returns_none(self):
        # Objects that can't be pickled (e.g. lambdas) should give None
        result = _msgpack_default(lambda: None)
        assert result is None

    def test_bag_is_pickled(self):
        from gnr.core.gnrbag import Bag

        b = Bag()
        b["key"] = "value"
        result = _msgpack_default(b)
        assert "__gnrbag__" in result
        restored = pickle.loads(result["__gnrbag__"])
        assert restored["key"] == "value"


# ---------------------------------------------------------------------------
# _msgpack_object_hook
# ---------------------------------------------------------------------------


class TestMsgpackObjectHook:
    def test_datetime_restored(self):
        obj = {"__datetime__": "2024-06-15T12:30:00"}
        result = _msgpack_object_hook(obj)
        assert result == datetime.datetime(2024, 6, 15, 12, 30, 0)

    def test_date_restored(self):
        obj = {"__date__": "2024-06-15"}
        result = _msgpack_object_hook(obj)
        assert result == datetime.date(2024, 6, 15)

    def test_set_restored(self):
        obj = {"__set__": [1, 2, 3]}
        result = _msgpack_object_hook(obj)
        assert result == {1, 2, 3}

    def test_set_with_tuple_elements(self):
        # msgpack decodes tuples as lists; the hook must handle unhashable lists
        obj = {"__set__": [[1, 2], [3, 4]]}
        result = _msgpack_object_hook(obj)
        assert result == {(1, 2), (3, 4)}

    def test_set_with_nested_tuples(self):
        obj = {"__set__": [[[1, 2], 3], [4, [5, 6]]]}
        result = _msgpack_object_hook(obj)
        assert result == {((1, 2), 3), (4, (5, 6))}

    def test_pickled_object_restored(self):
        payload = pickle.dumps(_PicklableObj(), protocol=4)
        result = _msgpack_object_hook({"__pickled__": payload})
        assert result.val == 42

    def test_pickled_bytes_as_latin1_string(self):
        payload = pickle.dumps(_PicklableObj(), protocol=4)
        payload_str = payload.decode("latin1")
        result = _msgpack_object_hook({"__pickled__": payload_str})
        assert result.val == 42

    def test_gnrbag_restored(self):
        from gnr.core.gnrbag import Bag

        b = Bag()
        b["x"] = 123
        payload = pickle.dumps(b, protocol=4)
        obj = {"__gnrbag__": payload}
        result = _msgpack_object_hook(obj)
        assert result["x"] == 123

    def test_gnrbag_restored_from_latin1_string(self):
        """When __gnrbag__ value is a string (latin1-encoded), it must be decoded first."""
        from gnr.core.gnrbag import Bag

        b = Bag()
        b["y"] = 456
        payload = pickle.dumps(b, protocol=4)
        payload_str = payload.decode("latin1")  # simulate msgpack string encoding
        obj = {"__gnrbag__": payload_str}
        result = _msgpack_object_hook(obj)
        assert result["y"] == 456

    def test_failed_unpickle_returns_none(self):
        obj = {"__pickled__": b"not-valid-pickle"}
        result = _msgpack_object_hook(obj)
        assert result is None

    def test_plain_dict_passthrough(self):
        obj = {"a": 1, "b": 2}
        result = _msgpack_object_hook(obj)
        assert result == {"a": 1, "b": 2}


# ---------------------------------------------------------------------------
# Round-trip tests
# ---------------------------------------------------------------------------


class TestRoundTrip:
    def test_datetime_roundtrip(self):
        dt = datetime.datetime(2023, 1, 2, 3, 4, 5)
        assert roundtrip(dt) == dt

    def test_date_roundtrip(self):
        d = datetime.date(2023, 1, 2)
        assert roundtrip(d) == d

    def test_set_of_ints_roundtrip(self):
        s = {10, 20, 30}
        assert roundtrip(s) == s

    def test_set_of_tuples_roundtrip(self):
        # Tuples inside sets get decoded as lists by msgpack; hook converts back
        s = {(1, 2), (3, 4)}
        result = roundtrip(s)
        assert result == s

    def test_nested_structure_roundtrip(self):
        data = {
            "ts": datetime.datetime(2024, 1, 1),
            "tags": {"alpha", "beta"},
            "count": 42,
        }
        result = roundtrip(data)
        assert result["count"] == 42
        assert result["ts"] == data["ts"]
        assert result["tags"] == data["tags"]

    def test_bag_roundtrip(self):
        from gnr.core.gnrbag import Bag

        b = Bag()
        b["name"] = "test"
        b["value"] = 99
        result = roundtrip(b)
        assert result["name"] == "test"
        assert result["value"] == 99

    def test_none_roundtrip(self):
        assert roundtrip(None) is None

    def test_list_roundtrip(self):
        lst = [1, "two", 3.0]
        assert roundtrip(lst) == lst

    def test_int_roundtrip(self):
        assert roundtrip(-1) == -1
        assert roundtrip(0) == 0
        assert roundtrip(1) == 1
