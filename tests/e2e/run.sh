#!/bin/sh
# End-to-end check: this server on the Firestore emulator, storage-testbench as GCS, pyiceberg
# as the client.
#
# Starts (or reuses) the lkfs-firestore emulator container and wipes it, restarts the lkfs-gcs
# storage-testbench container (its state is in memory), and runs the Rust tests on the Firestore
# emulator. Then it runs the server from target/debug, bootstraps it, creates a warehouse, and
# runs check.py in a new process per mode: write, read back, purge a table, and see the purge
# through the task management API, create, replace, rename and drop a view, move a table
# into another namespace, evolve a table and check what `lakekeeper-firestore search` finds, and
# replay a request by its idempotency key, move a namespace, create, list and drop a generic table,
# search tabulars, read endpoint statistics, and tag a namespace, a table and a column.
# Needs docker, curl and uv.
#
# The server needs the GCS emulator support of the patched Lakekeeper (see [patch] in
# Cargo.toml). storage-testbench must serve its JSON API on host port 80: the Google Cloud
# Storage SDK sends the Host header without the port, and the testbench builds upload URLs
# from it. The other ports can be overridden through the environment.
set -eu

cd "$(dirname "$0")/../.."
E2E=tests/e2e
FIRESTORE_PORT=${FIRESTORE_PORT:-28080}
GCS_GRPC_PORT=${GCS_GRPC_PORT:-18888}
CATALOG_PORT=${CATALOG_PORT:-18181}
PROJECT=demo-lakekeeper
BUCKET=lkfs-test
WORK=$(mktemp -d)
trap 'kill ${SERVER_PID:-} 2>/dev/null || true; rm -rf "$WORK"' EXIT

start() {
    name=$1
    shift
    if [ -z "$(docker ps -q -f "name=^$name$")" ]; then
        docker rm -f "$name" >/dev/null 2>&1 || true
        docker run -d --name "$name" "$@" >/dev/null
    fi
}
start lkfs-firestore -p "$FIRESTORE_PORT:8080" \
    gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators \
    gcloud emulators firestore start --host-port=0.0.0.0:8080
start lkfs-gcs -p 80:9000 -p "$GCS_GRPC_PORT:8888" \
    gcr.io/cloud-devrel-public-resources/storage-testbench:latest
docker restart lkfs-gcs >/dev/null

until curl -sf "localhost:$FIRESTORE_PORT/" >/dev/null; do sleep 1; done
until curl -sf localhost:80/ >/dev/null; do sleep 1; done
curl -sf -X DELETE \
    "localhost:$FIRESTORE_PORT/emulator/v1/projects/$PROJECT/databases/(default)/documents" >/dev/null
curl -sf "localhost:80/start_grpc?port=8888" >/dev/null
curl -sf -X POST "localhost:80/storage/v1/b?project=$PROJECT" \
    -H 'content-type: application/json' -d "{\"name\": \"$BUCKET\"}" >/dev/null

FIRESTORE_EMULATOR_HOST="localhost:$FIRESTORE_PORT" cargo test --quiet
cargo build --quiet
FIRESTORE_EMULATOR_HOST="localhost:$FIRESTORE_PORT" \
    LAKEKEEPER__FIRESTORE__PROJECT_ID=$PROJECT \
    LAKEKEEPER__GCS_EMULATOR_ENDPOINT=http://localhost:80 \
    LAKEKEEPER__GCS_EMULATOR_GRPC_ENDPOINT="http://localhost:$GCS_GRPC_PORT" \
    LAKEKEEPER__ENABLE_GCP_SYSTEM_CREDENTIALS=true \
    LAKEKEEPER__TASK_POLL_INTERVAL=1s \
    LAKEKEEPER__LISTEN_PORT="$CATALOG_PORT" \
    LAKEKEEPER__BASE_URI="http://localhost:$CATALOG_PORT" \
    ./target/debug/lakekeeper-firestore serve >"$WORK/server.log" 2>&1 &
SERVER_PID=$!
until curl -sf "localhost:$CATALOG_PORT/health" >/dev/null; do
    kill -0 "$SERVER_PID" 2>/dev/null || { cat "$WORK/server.log" >&2; exit 1; }
    sleep 1
done

B="localhost:$CATALOG_PORT"
curl -sS --fail-with-body -X POST "$B/management/v1/bootstrap" -H 'content-type: application/json' \
    -d '{"accept-terms-of-use": true}'
curl -sS --fail-with-body -X POST "$B/management/v1/warehouse" -H 'content-type: application/json' \
    -d @$E2E/warehouse.json >"$WORK/warehouse.out" || { cat "$WORK/warehouse.out" >&2; exit 1; }

# gcsfs otherwise probes the bucket type over gRPC on port 443, waiting 60 s per bucket.
export GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT=false
# For `search`, which runs the CLI against the emulator.
export FIRESTORE_EMULATOR_HOST="localhost:$FIRESTORE_PORT" LAKEKEEPER__FIRESTORE__PROJECT_ID=$PROJECT
export LKFS_BIN="$PWD/target/debug/lakekeeper-firestore" RUST_LOG=error
for mode in write reread purge tasks views rename search extras; do
    uv run --quiet --no-project --with 'pyiceberg[gcsfs,pyarrow]==0.10.0' \
        python $E2E/check.py "$mode" "http://$B/catalog"
done

if grep -q '"level":"ERROR"' "$WORK/server.log"; then
    grep '"level":"ERROR"' "$WORK/server.log"
    echo "FAIL: the server logged errors" >&2
    exit 1
fi
echo "OK"
