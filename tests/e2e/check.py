"""pyiceberg checks for run.sh. `write` runs first; the other modes run in new processes.

Usage: check.py write|reread|purge|tasks|views|rename|search|extras CATALOG_URL

`search` runs the binary named by $LKFS_BIN, with the Firestore settings of the environment.
"""

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.catalog.rest import RestCatalog

SCHEMA = pa.schema([("id", pa.int32()), ("msg", pa.string())])


def catalog(url: str) -> RestCatalog:
    # The server tells clients the emulator address (gcs.service.host). pyiceberg ignores
    # gcs.no-auth, so anonymous access is set here; gcsfs reads "anon" as no credentials.
    return RestCatalog(
        "lk",
        uri=url,
        warehouse="errtraq",
        **{
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
            "gcs.oauth2.token": "anon",
            "gcs.project-id": "demo-lakekeeper",
        },
    )


def check(name: str, actual, expected) -> None:
    if actual != expected:
        sys.exit(f"{name}: expected {expected}, got {actual}")
    print(f"{name}: ok")


def write(cat: RestCatalog) -> None:
    cat.create_namespace("events")
    table = cat.create_table("events.batches", schema=SCHEMA)
    table.append(pa.table({"id": [1, 2], "msg": ["a", "b"]}, schema=SCHEMA))
    table.append(pa.table({"id": [3], "msg": ["c"]}, schema=SCHEMA))
    table.delete("id == 2")
    rows = table.scan().to_arrow().sort_by("id")
    check("check_write", rows["id"].to_pylist(), [1, 3])
    check("check_snapshots", len(table.snapshots()) >= 3, True)


def reread(cat: RestCatalog) -> None:
    table = cat.load_table("events.batches")
    rows = table.scan().to_arrow().sort_by("id")
    check("check_reread", rows["msg"].to_pylist(), ["a", "c"])
    big = table.scan(row_filter="id >= 3").to_arrow()
    check("check_filter", pc.sum(big["id"]).as_py(), 3)


def purge(cat: RestCatalog) -> None:
    table = cat.create_table("events.purged", schema=SCHEMA)
    table.append(pa.table({"id": [1], "msg": ["x"]}, schema=SCHEMA))
    location = table.location()
    fs = table.io.get_fs("gs")
    check("check_purge_files_written", len(fs.find(location)) > 0, True)

    # Drop with purge: the server drops the table and queues a purge task, whose worker
    # deletes the files.
    cat.purge_table("events.purged")
    check("check_purge_dropped", cat.table_exists("events.purged"), False)
    deadline = time.monotonic() + 60
    while True:
        fs.invalidate_cache()
        left = fs.find(location)
        if not left or time.monotonic() > deadline:
            break
        time.sleep(1)
    check("check_purge_files_deleted", left, [])


def rename(cat: RestCatalog) -> None:
    cat.create_namespace("archive")
    cat.create_table("events.old", schema=SCHEMA).append(
        pa.table({"id": [7], "msg": ["moved"]}, schema=SCHEMA)
    )
    cat.rename_table("events.old", "archive.moved")
    rows = cat.load_table("archive.moved").scan().to_arrow()
    check("check_table_renamed", rows["msg"].to_pylist(), ["moved"])
    check("check_old_name_free", cat.table_exists("events.old"), False)


def request(url: str, body=None, method=None, headers=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={"content-type": "application/json", **(headers or {})},
    )
    with urllib.request.urlopen(req) as resp:
        return json.load(resp) if resp.status != 204 else None


def status_of(url: str, body=None, method=None, headers=None) -> int:
    try:
        request(url, body, method, headers)
        return 200
    except urllib.error.HTTPError as e:
        return e.code


def view_version(version_id: int, sql: str) -> dict:
    return {
        "version-id": version_id,
        "schema-id": 0,
        "timestamp-ms": int(time.time() * 1000),
        "summary": {},
        "representations": [{"type": "sql", "sql": sql, "dialect": "spark"}],
        "default-namespace": ["events"],
    }


def views(url: str) -> None:
    # pyiceberg 0.10 cannot create views, so this speaks the REST protocol itself.
    prefix = request(f"{url}/v1/config?warehouse=errtraq")["defaults"]["prefix"]
    base = f"{url}/v1/{prefix}/namespaces/events/views"
    schema = {
        "type": "struct",
        "schema-id": 0,
        "fields": [{"id": 1, "name": "id", "required": False, "type": "int"}],
    }
    created = request(
        base,
        {
            "name": "recent",
            "schema": schema,
            "view-version": view_version(1, "select id from events.batches"),
            "properties": {},
        },
    )
    uuid = created["metadata"]["view-uuid"]
    loaded = request(f"{base}/recent")
    check("check_view_created", loaded["metadata"]["view-uuid"], uuid)

    replaced = request(
        f"{base}/recent",
        {
            "requirements": [{"type": "assert-view-uuid", "uuid": uuid}],
            "updates": [
                {
                    "action": "add-view-version",
                    "view-version": view_version(2, "select id from events.batches where id > 1"),
                },
                {"action": "set-current-view-version", "view-version-id": -1},
            ],
        },
    )
    loaded = request(f"{base}/recent")
    meta = loaded["metadata"]
    current = next(v for v in meta["versions"] if v["version-id"] == meta["current-version-id"])
    sql = current["representations"][0]["sql"]
    check(
        "check_view_replaced",
        (loaded["metadata-location"], loaded["metadata"]["current-version-id"], sql),
        (replaced["metadata-location"], 2, "select id from events.batches where id > 1"),
    )
    names = [v["name"] for v in request(base)["identifiers"]]
    check("check_view_listed", names, ["recent"])
    request(
        f"{url}/v1/{prefix}/views/rename",
        {
            "source": {"namespace": ["events"], "name": "recent"},
            "destination": {"namespace": ["events"], "name": "latest"},
        },
    )
    names = [v["name"] for v in request(base)["identifiers"]]
    check("check_view_renamed", names, ["latest"])
    request(f"{base}/latest", method="DELETE")
    check("check_view_dropped", request(base)["identifiers"], [])


def tasks(url: str) -> None:
    # The management API's view of the purge that `purge` ran.
    base = url.removesuffix("/catalog") + "/management/v1"
    warehouses = request(f"{base}/warehouse")["warehouses"]
    wh = next(w["warehouse-id"] for w in warehouses if w["name"] == "errtraq")
    listed = request(f"{base}/warehouse/{wh}/task/list", {"queue-name": ["tabular_purge"]})
    check("check_tasks_listed", [t["status"] for t in listed["tasks"]], ["SUCCESS"])
    task_id = listed["tasks"][0]["task-id"]
    details = request(f"{base}/warehouse/{wh}/task/by-id/{task_id}")
    check("check_task_details", (details["task-id"], details["status"]), (task_id, "SUCCESS"))

    # A repair of a table's metadata projection, scheduled through this crate's own queue.
    table_id = str(catalog(url).load_table("events.batches").metadata.table_uuid)
    scheduled = request(
        f"{base}/warehouse/{wh}/task-queue/metadata_projection/schedule",
        {"entity": {"type": "table", "table-id": table_id}},
    )
    status = None
    for _ in range(30):
        status = request(f"{base}/warehouse/{wh}/task/by-id/{scheduled['task-id']}")["status"]
        if status != "SCHEDULED" and status != "RUNNING":
            break
        time.sleep(1)
    check("check_projection_repaired", status, "SUCCESS")


def extras(url: str) -> None:
    # Idempotency keys, namespace moves, generic tables, tabular search, endpoint
    # statistics and tags, over REST.
    prefix = request(f"{url}/v1/config?warehouse=errtraq")["defaults"]["prefix"]
    namespaces = f"{url}/v1/{prefix}/namespaces"
    key = {"Idempotency-Key": "01938f4e-7c5a-7d3e-8a1b-2c3d4e5f6a7b"}
    first = request(namespaces, {"namespace": ["staging"]}, headers=key)
    again = request(namespaces, {"namespace": ["staging"]}, headers=key)
    check("check_idempotent_replay", again["namespace"], first["namespace"])
    check(
        "check_idempotency_key_reused",
        status_of(f"{namespaces}/staging", method="DELETE", headers=key),
        400,
    )
    check("check_without_key_conflicts", status_of(namespaces, {"namespace": ["staging"]}), 409)

    base = url.removesuffix("/catalog")
    management = f"{base}/management/v1"
    warehouses = request(f"{management}/warehouse")["warehouses"]
    wh = next(w["warehouse-id"] for w in warehouses if w["name"] == "errtraq")
    return_ids = request(f"{namespaces}?returnUuids=true")
    ns_id = return_ids["namespace-uuids"][return_ids["namespaces"].index(["staging"])]
    request(
        f"{management}/warehouse/{wh}/namespace/{ns_id}/move",
        {"destination": ["events", "staged"]},
    )
    check(
        "check_namespace_moved",
        (status_of(f"{namespaces}/staging"), status_of(f"{namespaces}/events%1Fstaged")),
        (404, 200),
    )

    generic = f"{base}/lakekeeper/v1/{prefix}/namespaces/events%1Fstaged/generic-tables"
    request(
        generic,
        {"name": "vectors", "format": "lance", "doc": "embeddings", "properties": {"a": "b"}},
    )
    loaded = request(f"{generic}/vectors")["table"]
    check(
        "check_generic_table_loaded",
        (loaded["format"], loaded["doc"], loaded["properties"].get("a")),
        ("lance", "embeddings", "b"),
    )
    check(
        "check_generic_table_listed",
        [t["name"] for t in request(generic)["identifiers"]],
        ["vectors"],
    )
    found = request(f"{management}/warehouse/{wh}/search-tabular", {"search": "vectors"})
    check(
        "check_tabular_search",
        [t["tabular-name"] for t in found["tabulars"]][:1],
        ["vectors"],
    )
    request(f"{generic}/vectors", method="DELETE")
    check("check_generic_table_dropped", request(generic)["identifiers"], [])

    stats = request(f"{management}/endpoint-statistics", {"warehouse": {"type": "all"}})
    check("check_endpoint_statistics", stats["called-endpoints"], [])

    # Tags: definitions, tags on a namespace, a table and a column, inheritance, and the
    # reverse lookup.
    owner = request(
        f"{management}/tag-definition",
        {
            "name": "owner",
            "scope": ["warehouse", "namespace", "table", "column"],
            "value-kind": "free-text",
        },
    )
    request(
        f"{management}/tag-definition",
        {"name": "pii", "scope": ["column"], "value-kind": "marker"},
    )
    names = [d["name"] for d in request(f"{management}/tag-definition")["tag-definitions"]]
    check("check_tag_definitions_listed", sorted(names), ["owner", "pii"])
    table_id = request(f"{namespaces}/events/tables/batches")["metadata"]["table-uuid"]
    return_ids = request(f"{namespaces}?returnUuids=true")
    events_id = return_ids["namespace-uuids"][return_ids["namespaces"].index(["events"])]
    table_tags = f"{management}/warehouse/{wh}/table/{table_id}/tags"
    request(f"{management}/warehouse/{wh}/namespace/{events_id}/tags/owner", {"value": "sales"}, "PUT")
    request(f"{table_tags}/owner", {"value": "alice"}, "PUT")
    request(f"{management}/warehouse/{wh}/table/{table_id}/column/id/tags/pii", {}, "PUT")

    def effective_owner() -> list:
        tags = request(f"{table_tags}?effective=true")["tags"]
        return [t["value"] for t in tags if t["name"] == "owner"]

    check("check_table_tag", [t["value"] for t in request(table_tags)["tags"]], ["alice"])
    check("check_effective_tag", effective_owner(), ["alice"])
    columns = request(f"{management}/warehouse/{wh}/table/{table_id}/column-tags")["columns"]
    check(
        "check_column_tags",
        [(c["field-id"], [t["name"] for t in c["tags"]]) for c in columns],
        [(1, ["pii"])],
    )
    attached = request(f"{management}/tag-definition/{owner['id']}/attachments")
    check("check_tag_attachments", len(attached["attachments"]), 2)
    request(f"{table_tags}/owner", method="DELETE")
    check("check_inherited_tag", effective_owner(), ["sales"])


def search_cli(*args: str) -> list[dict]:
    out = subprocess.run(
        [os.environ["LKFS_BIN"], "search", *args, "--json"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return [json.loads(line) for line in out.splitlines() if line.strip()]


def search(url: str) -> None:
    """Evolve a table, then check what `lakekeeper-firestore search` finds against the metadata
    pyiceberg loads back."""
    from pyiceberg.transforms import BucketTransform
    from pyiceberg.types import StringType

    cat = catalog(url)
    table = cat.create_table("events.evolving", schema=SCHEMA)

    def append(rows: list[dict]) -> int:
        table.append(pa.Table.from_pylist(rows, schema=table.schema().as_arrow()))
        return table.current_snapshot().snapshot_id

    first = append([{"id": 1, "msg": "a"}, {"id": 2, "msg": "b"}])
    with table.update_schema() as update:
        update.add_column("region", StringType())
        update.rename_column("msg", "message")
    second = append([{"id": i, "message": "c", "region": "eu"} for i in (3, 4, 5)])
    with table.update_spec() as update:
        update.add_field("id", BucketTransform(4), "id_bucket")
    append([{"id": 6, "message": "d", "region": "us"}])
    table.manage_snapshots().create_branch(snapshot_id=second, branch_name="audit").commit()
    with table.transaction() as tx:
        tx.set_properties(owner="e2e")
    # Snapshot expiry, as an engine commits it.
    prefix = request(f"{url}/v1/config?warehouse=errtraq")["defaults"]["prefix"]
    request(
        f"{url}/v1/{prefix}/namespaces/events/tables/evolving",
        {"requirements": [], "updates": [{"action": "remove-snapshots", "snapshot-ids": [first]}]},
    )
    metadata = cat.load_table("events.evolving").metadata

    snapshots = sorted(metadata.snapshots, key=lambda s: s.sequence_number)
    refs = {}
    for name, ref in metadata.refs.items():
        refs.setdefault(ref.snapshot_id, []).append(name)
    history = search_cli("history", "--warehouse", "errtraq", "events.evolving")
    check(
        "check_search_history",
        [(r["snapshot_id"], r["refs"]) for r in history],
        [(s.snapshot_id, ",".join(sorted(refs.get(s.snapshot_id, [])))) for s in snapshots],
    )
    check("check_search_expired", first in [r["snapshot_id"] for r in history], False)

    big = search_cli(
        "snapshots", "--warehouse", "errtraq", "--table", "events.evolving",
        "--where", "added_records>=2",
    )
    check(
        "check_search_where",
        sorted(r["snapshot_id"] for r in big),
        sorted(
            s.snapshot_id for s in snapshots
            if int(s.summary.get("added-records", "0")) >= 2
        ),
    )

    def tables_with(column: str) -> list[str]:
        return sorted(r["table"] for r in search_cli("columns", column, "--warehouse", "errtraq"))

    check("check_search_renamed_column", "events.evolving" in tables_with("message"), True)
    check("check_search_old_name", "events.evolving" in tables_with("msg"), False)

    added_in = min(
        s.schema_id for s in metadata.schemas if "region" in [f.name for f in s.fields]
    )
    changes = search_cli("schema-changes", "--warehouse", "errtraq", "events.evolving")
    check(
        "check_search_schema_change",
        [(r["schema_id"], r["change"]) for r in changes if r["path"] == "region"],
        [(added_in, "added")],
    )

    partitioned = search_cli("partitioned", "--warehouse", "errtraq", "--transform", "bucket[4]")
    check(
        "check_search_partitioned",
        [(r["table"], r["fields"]) for r in partitioned],
        [("events.evolving", "id_bucket=bucket[4](id)")],
    )
    by_column = search_cli("partitioned", "--warehouse", "errtraq", "--column", "ID")
    check("check_search_partitioned_by_column", [r["table"] for r in by_column], ["events.evolving"])

    owners = search_cli("properties", "owner", "e2e", "--warehouse", "errtraq")
    check("check_search_property", [r["name"] for r in owners], ["events.evolving"])

    largest = search_cli("largest", "--warehouse", "errtraq", "--by", "records")
    current = metadata.snapshot_by_id(metadata.current_snapshot_id)
    check(
        "check_search_largest",
        [r["total_records"] for r in largest if r["table"] == "events.evolving"],
        [int(current.summary["total-records"])],
    )


if __name__ == "__main__":
    mode, url = sys.argv[1], sys.argv[2]
    if mode == "tasks":
        tasks(url)
    elif mode == "views":
        views(url)
    elif mode == "search":
        search(url)
    elif mode == "extras":
        extras(url)
    else:
        {"write": write, "reread": reread, "purge": purge, "rename": rename}[mode](catalog(url))
