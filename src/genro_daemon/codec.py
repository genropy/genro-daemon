"""msgpack encoding/decoding helpers shared by the server (ars.py) and client (client.py)."""

import datetime
import pickle as _pickle


def _msgpack_default(obj):
    """Encode types that msgpack cannot handle natively."""
    from gnr.core.gnrbag import Bag

    if isinstance(obj, Bag):
        # Pickle preserves the full Bag state including plain-dict node values.
        return {"__gnrbag__": _pickle.dumps(obj, protocol=4)}
    if isinstance(obj, datetime.datetime):
        return {"__datetime__": obj.isoformat()}
    if isinstance(obj, datetime.date):
        return {"__date__": obj.isoformat()}
    if isinstance(obj, set):
        return {"__set__": list(obj)}
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    # Pickle-encode any other object that's not a basic msgpack type.
    # This handles ClientDataChange and any other custom objects.
    try:
        return {"__pickled__": _pickle.dumps(obj, protocol=4)}
    except Exception:
        return None


def _msgpack_object_hook(obj):
    """Reconstruct types that were encoded by _msgpack_default."""
    if "__gnrbag__" in obj:
        raw = obj["__gnrbag__"]
        if isinstance(raw, str):
            raw = raw.encode("latin1")
        return _pickle.loads(raw)
    if "__datetime__" in obj:
        return datetime.datetime.fromisoformat(obj["__datetime__"])
    if "__date__" in obj:
        return datetime.date.fromisoformat(obj["__date__"])
    if "__set__" in obj:

        def _to_hashable(v):
            if isinstance(v, list):
                return tuple(_to_hashable(i) for i in v)
            return v

        return set(_to_hashable(item) for item in obj["__set__"])
    if "__pickled__" in obj:
        raw = obj["__pickled__"]
        if isinstance(raw, str):
            raw = raw.encode("latin1")
        try:
            return _pickle.loads(raw)
        except Exception:
            return None
    return obj
