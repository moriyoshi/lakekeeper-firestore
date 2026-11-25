"""Several instances on one database: a change made through one evicts the others' caches.

Two more servers share the e2e emulator with the main one, with cache entries that would
live an hour. B invalidates from the cache change log; C has invalidation off, and stays
stale, which shows the reads checked here are cached at all. Warehouses are looked up by name
through the cache; loading a namespace is not cached, so namespaces are not checked here.
"""

import json
import time
import urllib.error
import urllib.request

import pytest

import support
from conftest import CATALOG_PORT, WAREHOUSE, BUCKET

LONG_TTL = {
    "LAKEKEEPER__CACHE__WAREHOUSE__TIME_TO_LIVE_SECS": "3600",
    "LAKEKEEPER__CACHE__NAMESPACE__TIME_TO_LIVE_SECS": "3600",
}
# Within the TTL, and well past the change log's 1 s polling.
EVICTION_WAIT = 15


@pytest.fixture(scope="module")
def instances(server, emulator_env, tmp_path_factory):
    """Servers B (invalidating) and C (not), on the next two ports."""
    logs = tmp_path_factory.mktemp("instances")
    common = {**emulator_env, "LAKEKEEPER__ENABLE_GCP_SYSTEM_CREDENTIALS": "true", **LONG_TTL}
    # The GCS emulator settings of the main server.
    for key in ("LAKEKEEPER__GCS_EMULATOR_ENDPOINT", "LAKEKEEPER__GCS_EMULATOR_GRPC_ENDPOINT"):
        common[key] = server.env[key]
    started = []
    try:
        for offset, invalidate in ((1, "true"), (2, "false")):
            started.append(
                support.Server(
                    CATALOG_PORT + offset,
                    {
                        **common,
                        "LAKEKEEPER__CACHE__INVALIDATION__ENABLED": invalidate,
                        "LAKEKEEPER__METRICS_PORT": str(9100 + offset),
                    },
                    logs / f"server-{offset}.log",
                )
            )
        yield started
    finally:
        for s in started:
            s.stop()
    for s in started:
        errors = s.logged_errors()
        assert not errors, f"{s.url} logged errors:\n" + "\n".join(errors)


def call(url: str, body=None, method=None) -> dict | None:
    request = urllib.request.Request(
        url,
        data=None if body is None else json.dumps(body).encode(),
        method=method,
        headers={"content-type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response) if response.status != 204 else None
    except urllib.error.HTTPError as e:
        e.msg = f"{e.msg}: {e.read().decode(errors='replace')}"
        raise


def status(url: str) -> int:
    try:
        call(url)
        return 200
    except urllib.error.HTTPError as e:
        return e.code


def eventually(read, expected, wait: float = EVICTION_WAIT):
    deadline = time.monotonic() + wait
    while True:
        value = read()
        if value == expected or time.monotonic() > deadline:
            return value
        time.sleep(0.5)


def create_warehouse(server, name: str) -> str:
    body = json.loads(WAREHOUSE.read_text())
    body["warehouse-name"] = name
    body["storage-profile"]["bucket"] = BUCKET
    body["storage-profile"]["key-prefix"] = name
    return call(f"{server.url}/management/v1/warehouse", body)["warehouse-id"]


def found(instance, name: str) -> int:
    """Resolving a warehouse by name, which goes through the warehouse cache."""
    return status(f"{instance.url}/catalog/v1/config?warehouse={name}")


def test_a_renamed_warehouse_reaches_the_other_instances(server, instances):
    b, c = instances
    warehouse_id = create_warehouse(server, "before-rename")
    assert (found(b, "before-rename"), found(c, "before-rename")) == (200, 200)  # now cached

    call(f"{server.url}/management/v1/warehouse/{warehouse_id}/rename", {"new-name": "after-rename"})
    assert eventually(lambda: found(b, "before-rename"), 404) == 404
    assert found(b, "after-rename") == 200
    assert found(c, "before-rename") == 200, "C should still serve its cached entry"


def test_a_dropped_warehouse_leaves_the_other_instances(server, instances):
    b, c = instances
    warehouse_id = create_warehouse(server, "dropped")
    assert (found(b, "dropped"), found(c, "dropped")) == (200, 200)  # now cached on both

    call(f"{server.url}/management/v1/warehouse/{warehouse_id}", method="DELETE")
    assert eventually(lambda: found(b, "dropped"), 404) == 404
    assert found(c, "dropped") == 200, "C should still serve its cached entry"
