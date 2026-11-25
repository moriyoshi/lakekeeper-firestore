"""End-to-end fixtures: this server on the Firestore emulator, storage-testbench as GCS.

The session starts (or reuses) the lkfs-firestore emulator container and wipes it, restarts
the lkfs-gcs storage-testbench container (its state is in memory), builds and runs the server
from target/debug, bootstraps it and creates the warehouse of warehouse.json. When the session
ends, it fails if the server logged an error.

The server needs the GCS emulator support of the patched Lakekeeper (see [patch] in
Cargo.toml). storage-testbench must serve its JSON API on host port 80: the Google Cloud
Storage SDK sends the Host header without the port, and the testbench builds upload URLs from
it. The other ports can be overridden through the environment. The default containers are
the ones the console demo of the README uses, so a run wipes the demo's catalog; to leave a
demo alone, set FIRESTORE_CONTAINER and FIRESTORE_PORT to another emulator, GCS_BUCKET to a new
bucket and GCS_RESTART=0.
"""

import json
import os
import subprocess
from pathlib import Path

import pytest
from pyiceberg.catalog.rest import RestCatalog

import support

FIRESTORE_PORT = int(os.environ.get("FIRESTORE_PORT", "28080"))
GCS_GRPC_PORT = int(os.environ.get("GCS_GRPC_PORT", "18888"))
CATALOG_PORT = int(os.environ.get("CATALOG_PORT", "18181"))
# Unset: upstream's default, 9000.
METRICS_PORT = os.environ.get("METRICS_PORT")
FIRESTORE_CONTAINER = os.environ.get("FIRESTORE_CONTAINER", "lkfs-firestore")
BUCKET = os.environ.get("GCS_BUCKET", "lkfs-test")
GCS_RESTART = os.environ.get("GCS_RESTART", "1") != "0"
PROJECT = "demo-lakekeeper"
WAREHOUSE = Path(__file__).with_name("warehouse.json")

# gcsfs otherwise probes the bucket type over gRPC on port 443, waiting 60 s per bucket.
os.environ["GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT"] = "false"


@pytest.fixture(scope="session")
def emulator_env() -> dict[str, str]:
    """Settings that point this crate (the server, the CLI, the Rust tests) at the emulator."""
    support.start_firestore(FIRESTORE_CONTAINER, FIRESTORE_PORT)
    if not support.docker("ps", "-q", "-f", "name=^lkfs-gcs$"):
        support.docker("rm", "-f", "lkfs-gcs", check=False)
        support.docker(
            "run", "-d", "--name", "lkfs-gcs", "-p", "80:9000", "-p", f"{GCS_GRPC_PORT}:8888",
            "gcr.io/cloud-devrel-public-resources/storage-testbench:latest",
        )
    if GCS_RESTART:
        support.docker("restart", "lkfs-gcs")
    support.wait_until("storage-testbench", lambda: support.http_ok("http://localhost:80/"))
    support.wipe_firestore(FIRESTORE_PORT, PROJECT)
    support.http("GET", "http://localhost:80/start_grpc?port=8888")
    try:
        support.http(
            "POST", f"http://localhost:80/storage/v1/b?project={PROJECT}", {"name": BUCKET}
        )
    except RuntimeError as e:
        # Left from an earlier run without GCS_RESTART.
        if ": 409 " not in str(e):
            raise
    return {
        "FIRESTORE_EMULATOR_HOST": f"localhost:{FIRESTORE_PORT}",
        "LAKEKEEPER__FIRESTORE__PROJECT_ID": PROJECT,
    }


@pytest.fixture(scope="session")
def server(emulator_env, tmp_path_factory):
    support.build()
    log = tmp_path_factory.mktemp("server") / "server.log"
    url = f"http://localhost:{CATALOG_PORT}"
    server = support.Server(
        CATALOG_PORT,
        {
            **emulator_env,
            "LAKEKEEPER__GCS_EMULATOR_ENDPOINT": "http://localhost:80",
            "LAKEKEEPER__GCS_EMULATOR_GRPC_ENDPOINT": f"http://localhost:{GCS_GRPC_PORT}",
            "LAKEKEEPER__ENABLE_GCP_SYSTEM_CREDENTIALS": "true",
            "LAKEKEEPER__TASK_POLL_INTERVAL": "1s",
            "LAKEKEEPER__BASE_URI": url,
            **({"LAKEKEEPER__METRICS_PORT": METRICS_PORT} if METRICS_PORT else {}),
        },
        log,
    )
    try:
        server.bootstrap()
        warehouse = json.loads(WAREHOUSE.read_text())
        warehouse["storage-profile"]["bucket"] = BUCKET
        support.http("POST", f"{url}/management/v1/warehouse", warehouse)
        yield server
    finally:
        server.stop()
        support.keep_log(log)
    errors = server.logged_errors()
    assert not errors, "the server logged errors:\n" + "\n".join(errors)


@pytest.fixture(scope="session")
def url(server) -> str:
    """The catalog's REST URL."""
    return f"{server.url}/catalog"


@pytest.fixture
def catalog(url) -> RestCatalog:
    """A new client per test, so a test reads what the server stored, not a client's state."""
    # The server tells clients the emulator address (gcs.service.host). pyiceberg ignores
    # gcs.no-auth, so anonymous access is set here; gcsfs reads "anon" as no credentials.
    return RestCatalog(
        "lk",
        uri=url,
        warehouse="errtraq",
        **{
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
            "gcs.oauth2.token": "anon",
            "gcs.project-id": PROJECT,
        },
    )


@pytest.fixture(scope="session")
def search_cli(server, emulator_env):
    """`lakekeeper-firestore search ARGS --json` against the emulator, as parsed rows."""

    def run(*args: str) -> list[dict]:
        out = subprocess.run(
            [str(support.BINARY), "search", *args, "--json"],
            env={**os.environ, **emulator_env, "RUST_LOG": "error"},
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        return [json.loads(line) for line in out.splitlines() if line.strip()]

    return run
