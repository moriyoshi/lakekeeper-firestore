"""pyiceberg checks for run.sh. `write` runs first; the other modes run in new processes.

Usage: check.py write|reread|purge|tasks|views|rename|search CATALOG_URL

`search` runs the binary named by $LKFS_BIN, with the Firestore settings of the environment.
"""

import json
import os
import subprocess
import sys
import time
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


def request(url: str, body=None, method=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        url, data=data, method=method, headers={"content-type": "application/json"}
    )
    with urllib.request.urlopen(req) as resp:
        return json.load(resp) if resp.status != 204 else None


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
    else:
        {"write": write, "reread": reread, "purge": purge, "rename": rename}[mode](catalog(url))
