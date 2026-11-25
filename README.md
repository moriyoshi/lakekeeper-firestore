# lakekeeper-firestore

[Lakekeeper](https://github.com/lakekeeper/lakekeeper) (an Apache Iceberg REST catalog) with a Firestore
catalog store. It is built as an independent crate on top of Lakekeeper's generic server. It patches
Lakekeeper only for testing on emulators (see `[patch]` in `Cargo.toml`).

Status: early, tested on the emulators only. Projects, warehouses, namespaces (including moves),
tables, views, generic tables, tags, tasks, users, idempotency keys and the cache change log (for
several instances) work. Storage credentials must use the server's own identity: keys are refused.
Roles and grants are out of scope, as this is meant for a small, self-contained installation behind
the allow-all authorizer. See
[DESIGN.md](./DESIGN.md) for the approach, the plan and the known gaps.

```sh
cargo build

# Against the emulator (FIRESTORE_EMULATOR_HOST selects it):
FIRESTORE_EMULATOR_HOST=localhost:8080 \
LAKEKEEPER__FIRESTORE__PROJECT_ID=demo-lakekeeper \
  ./target/debug/lakekeeper-firestore serve

# End-to-end check on GCS (Docker, uv; host port 80 must be free for the
# storage-testbench emulator). Builds against the patched Lakekeeper from branch
# gcs-emulator-0.15.1 of github.com/moriyoshi/lakekeeper (see [patch] in Cargo.toml):
uv run --project tests pytest tests/e2e

# Upstream Lakekeeper's pyiceberg REST suite (Docker, uv, `cargo install winterbaume-server`):
uv run --project tests python -m pytest -p tests.upstream.plugin

# Smoke test of a built image against the Firestore emulator (BuildKit; add
# `--platform linux/amd64` on an arm64 machine for Cloud Run's platform, cross-compiled):
docker build --build-arg FEATURES=ui -t lakekeeper-firestore:dev .
IMAGE=lakekeeper-firestore:dev uv run --project tests pytest tests/image
```

Configuration is upstream Lakekeeper's `LAKEKEEPER__*` settings, plus:

| Variable | Meaning |
| --- | --- |
| `LAKEKEEPER__FIRESTORE__PROJECT_ID` | Google Cloud project of the database (falls back to `GOOGLE_CLOUD_PROJECT`) |
| `LAKEKEEPER__FIRESTORE__DATABASE_ID` | Firestore database; default `(default)` |
| `LAKEKEEPER__FIRESTORE__COLLECTION_PREFIX` | Prefix for every collection, so several catalogs can share a database |
| `LAKEKEEPER__FIRESTORE__PROJECTION_WORKERS` | Workers of the `metadata_projection` task queue in this process; default 2 |
| `LAKEKEEPER__DEBUG__IN_MEMORY_SECRETS` | `true` keeps storage credentials in memory instead of Firestore, keys included (testing only; lost on restart) |

Only the allow-all authorizer is wired up.

## Deploying

`Dockerfile` builds the server image (`--build-arg FEATURES=ui` adds the console), and
[DEPLOY.md](./DEPLOY.md) walks through Cloud Run: Firestore and its indexes, IAM, the service
and client access through Cloud Run IAM.

## Trying the console on emulators

Build with `--features ui` to serve the Lakekeeper console at `/ui`. With the Firestore and
Cloud Storage emulators (see `tests/e2e/conftest.py` for the containers and settings), its query
engine (LoQE) also works on GCS warehouses whose storage profile sets `"sts-enabled": true`:
the server vends a placeholder token and proxies the browser's storage requests to the
emulator under `/gcs-emulator`. This needs fork commit `227554f2` (see DESIGN.md, "Object
storage for tests"), and is for testing only.

## Searching table metadata

`lakekeeper-firestore search` queries a searchable projection of every table's metadata in
Firestore directly, with the same `LAKEKEEPER__FIRESTORE__*` settings and Google application
default credentials. It bypasses the server and its authorizer. For example:

```sh
lakekeeper-firestore search snapshots --warehouse prod --since 2026-11-01 --where 'deleted_records>0'
lakekeeper-firestore search history --warehouse prod sales.orders
lakekeeper-firestore search columns customer_id --type long
lakekeeper-firestore search partitioned --transform day
lakekeeper-firestore search properties owner team-a
lakekeeper-firestore search largest --by files-size --json
```

`lakekeeper-firestore search indexes` prints the `firestore.indexes.json` that the catalog and
these searches need, for the configured collection prefix; `firestore.indexes.json` in this
repository is the one for no prefix. It is in the Firebase CLI's format, for
`firebase deploy --only firestore:indexes`.
