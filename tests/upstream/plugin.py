"""A pytest plugin that runs upstream Lakekeeper's pyiceberg REST suite against this server.

    uv run --project tests python -m pytest -p tests.upstream.plugin [pytest arguments]

Without test paths, it runs `tests/python/tests/test_pyiceberg.py` of the Lakekeeper revision
in Cargo.lock, as upstream's `bash run.sh pyiceberg` runs it against Postgres, with the
settings of upstream's tox environment `pyiceberg`. Before upstream's conftest.py reads its
settings from the environment, the plugin starts the Firestore emulator (container
lkfs-firestore-upstream, removed afterwards), winterbaume-server (`cargo install
winterbaume-server`, an in-memory AWS mock) for S3 and STS in place of upstream's MinIO, and
this server; it stops them when the session ends.

Upstream's suite gets its token from Keycloak, and skips every test without an OpenID
provider. This server runs without authentication and ignores the token, so the plugin serves
a stand-in token endpoint that hands out a placeholder. The suite's S3 credentials are keys,
which the Firestore secret store refuses, so the server keeps secrets in memory here.
`KEEP_LOG=path` keeps the server's log. `LAKEKEEPER_TEST__S3_STS_MODE=disabled` leaves out the
vended-credential half of the suite, which winterbaume cannot serve yet (its AssumeRole wants a
RoleArn, and the suite's MinIO-flavoured storage profiles send none; see
https://github.com/moriyoshi/winterbaume/issues/77). The ports can be overridden through the environment,
and differ from the e2e test's, so a running console demo is left alone.
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import boto3
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import support  # noqa: E402

FIRESTORE_PORT = int(os.environ.get("FIRESTORE_PORT", "28082"))
S3_PORT = int(os.environ.get("S3_PORT", "25555"))
CATALOG_PORT = int(os.environ.get("CATALOG_PORT", "18183"))
METRICS_PORT = int(os.environ.get("METRICS_PORT", "9002"))
PROJECT = "demo-lakekeeper"
CONTAINER = "lkfs-firestore-upstream"
BUCKET = "tests"

_cleanup: list = []


def upstream_suite() -> Path:
    """`tests/python` of the Lakekeeper checkout that Cargo.lock pins."""
    metadata = json.loads(
        subprocess.run(
            ["cargo", "metadata", "--format-version", "1", "--locked"],
            cwd=support.ROOT,
            check=True,
            capture_output=True,
        ).stdout
    )
    package = next(p for p in metadata["packages"] if p["name"] == "lakekeeper")
    # The crate is crates/lakekeeper inside the repository.
    return Path(package["manifest_path"]).parents[2] / "tests" / "python"


def winterbaume() -> str:
    found = shutil.which("winterbaume-server")
    if found:
        return found
    path = Path(os.environ.get("CARGO_HOME", Path.home() / ".cargo")) / "bin" / "winterbaume-server"
    if not path.exists():
        raise pytest.UsageError("winterbaume-server not found: cargo install winterbaume-server")
    return str(path)


class _TokenEndpoint(BaseHTTPRequestHandler):
    """Just enough of an OpenID provider for the suite's client-credentials request."""

    def _json(self, body: dict):
        data = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        host = f"http://localhost:{self.server.server_port}"
        self._json({"issuer": host, "token_endpoint": f"{host}/token"})

    def do_POST(self):
        self.rfile.read(int(self.headers.get("content-length", 0)))
        self._json({"access_token": "unauthenticated", "token_type": "bearer", "expires_in": 3600})

    def log_message(self, *args):
        pass


def start_services(work: Path) -> dict[str, str]:
    """Start the emulators, the token endpoint and the server; return the suite's settings."""
    support.docker("rm", "-f", CONTAINER, check=False)
    _cleanup.append(lambda: support.docker("rm", "-f", CONTAINER, check=False))
    support.start_firestore(CONTAINER, FIRESTORE_PORT)

    s3_log = work / "s3.log"
    s3 = subprocess.Popen(
        [winterbaume(), "--port", str(S3_PORT), "--region", "us-east-1"],
        stdout=s3_log.open("w"),
        stderr=subprocess.STDOUT,
    )
    _cleanup.append(lambda: support.stop(s3))
    support.wait_until("winterbaume-server", lambda: support.port_open(S3_PORT), s3, s3_log)
    boto3.client(
        "s3",
        endpoint_url=f"http://localhost:{S3_PORT}",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    ).create_bucket(Bucket=BUCKET)

    tokens = ThreadingHTTPServer(("localhost", 0), _TokenEndpoint)
    threading.Thread(target=tokens.serve_forever, daemon=True).start()
    _cleanup.append(tokens.shutdown)

    support.build()
    log = work / "server.log"
    _cleanup.append(lambda: support.keep_log(log))
    server = support.Server(
        CATALOG_PORT,
        {
            "FIRESTORE_EMULATOR_HOST": f"localhost:{FIRESTORE_PORT}",
            "LAKEKEEPER__FIRESTORE__PROJECT_ID": PROJECT,
            "LAKEKEEPER__DEBUG__IN_MEMORY_SECRETS": "true",
            "LAKEKEEPER__TASK_POLL_INTERVAL": "1s",
            "LAKEKEEPER__METRICS_PORT": str(METRICS_PORT),
        },
        log,
    )
    _cleanup.append(server.stop)

    # The settings of upstream's tox environment `pyiceberg`, with this server and winterbaume.
    return {
        "LAKEKEEPER_TEST__MANAGEMENT_URL": f"{server.url}/management",
        "LAKEKEEPER_TEST__CATALOG_URL": f"{server.url}/catalog",
        "LAKEKEEPER_TEST__OPENID_PROVIDER_URI": f"http://localhost:{tokens.server_port}/",
        "LAKEKEEPER_TEST__OPENID_CLIENT_ID": "unused",
        "LAKEKEEPER_TEST__OPENID_CLIENT_SECRET": "unused",
        "LAKEKEEPER_TEST__S3_BUCKET": BUCKET,
        "LAKEKEEPER_TEST__S3_ACCESS_KEY": "test",
        "LAKEKEEPER_TEST__S3_SECRET_KEY": "test",
        "LAKEKEEPER_TEST__S3_ENDPOINT": f"http://localhost:{S3_PORT}",
        "LAKEKEEPER_TEST__S3_PATH_STYLE_ACCESS": "1",
        "LAKEKEEPER_TEST__S3_REGION": "us-east-1",
        # `disabled` tests remote signing only: winterbaume's AssumeRole wants a RoleArn,
        # which the suite's MinIO-flavoured profiles leave out.
        "LAKEKEEPER_TEST__S3_STS_MODE": os.environ.get("LAKEKEEPER_TEST__S3_STS_MODE", "both"),
    }


@pytest.hookimpl(tryfirst=True)
def pytest_load_initial_conftests(early_config, parser, args):
    # Upstream's files stay as they are in the Cargo checkout.
    sys.dont_write_bytecode = True
    if not any(Path(a.split("::")[0]).exists() for a in args if not a.startswith("-")):
        args.append(str(upstream_suite() / "tests" / "test_pyiceberg.py"))
    # Upstream's tox runs report skips; this run has no ini file of its own.
    args.append("-rs")
    work = Path(tempfile.mkdtemp())
    _cleanup.append(lambda: shutil.rmtree(work, ignore_errors=True))
    try:
        os.environ.update(start_services(work))
    except BaseException:
        pytest_unconfigure(early_config)
        raise


def pytest_unconfigure(config):
    while _cleanup:
        _cleanup.pop()()
