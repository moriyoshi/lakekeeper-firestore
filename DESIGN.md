# Design: a Firestore catalog store for Lakekeeper

## Goal

Run the Lakekeeper Iceberg REST catalog with Firestore as its metadata store. The result is a catalog
that costs almost nothing when idle (Cloud Run with no minimum instances, plus Firestore), instead of
Cloud SQL's fixed cost. The first user is errtraq's archive tier on GCP: it appends batches, writes v3
deletion vectors for erasure, and serves federated queries through DuckDB.

## No fork needed (checked at v0.15.1)

- **The server core is generic.** `lakekeeper::serve::serve` takes a
  `ServeConfiguration<C, S, A, …>` with `C: CatalogStore`, `S: SecretStore` and `A: Authorizer`.
  Only the upstream `lakekeeper-bin` crate selects `PostgresBackend`.
- **The storage traits are open.** `CatalogStore` and `Transaction` (in
  `crates/lakekeeper/src/service/catalog_store.rs`) are public and unsealed, and their signatures
  don't mention Postgres or sqlx.
- **No sqlx.** sqlx enters the core crate only through the optional `sqlx-postgres` feature, so this
  crate needs neither that feature nor the workspace's `[patch.crates-io]` sqlx entries, which don't
  apply to dependents.
- **Dependency.** Lakekeeper isn't published to crates.io, so this crate depends on it through Cargo's
  git dependency, pinned to a release tag.
- **Build flags.** Lakekeeper's crates must be compiled with `--cfg tracing_unstable --cfg tokio_unstable`
  (`iceberg-ext` uses `tracing::field::valuable`). Upstream sets these in its own `.cargo/config.toml`,
  which Cargo doesn't apply to dependents, so this repository carries the same flags in
  `.cargo/config.toml`. Re-check upstream's config at every tag bump.
- **License.** Upstream is Apache-2.0. Any code adapted from `lakekeeper-bin` keeps its license
  notices.

## Scope of the store

At v0.15.1, the store and transaction traits have about 149 methods. They cover:
- server info and bootstrap;
- projects and warehouses;
- namespaces, tables, views and generic tables;
- roles, grants, role assignments and users;
- tags;
- tasks;
- idempotency;
- endpoint statistics;
- the cache change log.

For comparison, the Postgres backend is about 2.5 MB of Rust in 48 files, including inline tests, plus
79 SQL migrations.

## Mapping onto Firestore

| Concern | Postgres backend | Firestore approach |
| --- | --- | --- |
| Write transactions | Interactive `begin_write` / statements / `commit` | Firestore transactions buffer writes until commit, so reads inside a transaction don't see its own earlier writes. Wrap them in a transaction object that applies pending writes to reads, then commits them in one `Commit`. |
| Commit outcome unknown | Supported by the trait (`COMMIT_STATE_UNKNOWN`) | Map a Firestore deadline or unavailable error during commit to this case. |
| Unique names | Unique constraints | Marker documents keyed by the natural name (warehouse/namespace/table), written in the same transaction. |
| Listing and paging | SQL with keyset pagination | Ordered queries with cursors; composite indexes declared in `firestore.indexes.json`. |
| Task queue | Postgres row locks that let workers skip rows already claimed by others | Lease documents claimed with a transaction (or Cloud Tasks). |
| Exclusive locks | `pg_try_advisory_lock` | Lease document with an expiry and owner token. |
| Cache change log | Table plus reader / retention | Change documents plus Firestore listeners or polling. |
| Large Iceberg metadata | Stored normalized in tables | Firestore documents are capped at 1 MiB: keep metadata files in object storage (as Iceberg already does) and store pointers. |
| Write rate | Not an issue | About 1 sustained write per second per document. This suits errtraq's commit rate (about one per batch per project); per-table hot spots need watching. |
| Migrations | 79 SQL migrations | Schemaless; keep a data-version document and write explicit upgrade steps. |

## Plan

1. **Skeleton (done, 2026-11-23).** This crate depends on `lakekeeper` v0.15.1 and type-checks on
   stable Rust 1.99.1. A cold `cargo check` takes about 3.5 minutes.
2. **Server wiring (done, 2026-11-23).** Adapted from `lakekeeper-bin` (each adapted file keeps the
   upstream copyright and license notice and lists its changes):
   - `src/main.rs`: the CLI, with `serve`, `healthcheck` and `version`. The Postgres-only commands
     (`migrate`, `wait-for-db`, `reopen-bootstrap`), the OpenFGA and OpenAPI commands, and jemalloc
     are dropped.
   - `src/config.rs`: `LAKEKEEPER__DEBUG__*` plus `LAKEKEEPER__FIRESTORE__{PROJECT_ID,DATABASE_ID,COLLECTION_PREFIX,PROJECTION_WORKERS}`.
   - `src/serve.rs`: allow-all authorizer only (any other `LAKEKEEPER__AUTHZ_BACKEND` is refused), the
     upstream config-driven authenticator, no endpoint statistics sink, and only the in-core tracing
     event backend (no NATS or Kafka).
   - `src/healthcheck.rs` and `src/ui.rs` (behind the `ui` feature, which pulls `lakekeeper-console`
     v0.31.0 as upstream does).
   - `src/secrets.rs`: a secret store that failed every call (replaced in step 5).
   - `src/store/catalog.rs`: the `CatalogStore` impl. Its stubs were generated from the trait at
     v0.15.1, and each one returns a `CatalogBackendError` naming the method. Two exceptions:
     `record_cache_changes_impl` succeeded and wrote nothing, and `cache_change_log` never
     reported a change, which was correct for a single instance only. Both are implemented
     now (see "Cache change log" below).
3. **Minimum for errtraq (working on the emulator, 2026-11-23).** These are implemented in
   `src/store/`:
   - **Server info and bootstrap.** The server document is created on first use.
   - **Projects.**
   - **Warehouses**, except statistics, which always come back empty.
   - **Namespaces**, except move.
   - **Tabulars:** lookup by ident, ID or location, listing, protection, soft and hard delete.
   - **Tables:** create, load, commit.

   `tests/e2e` (pytest: `uv run --project tests pytest tests/e2e`) runs the whole path on GCS.
   Its fixtures start the Firestore emulator and Google's storage-testbench in Docker and run
   the server with the patched Lakekeeper (see "Object storage for tests"); its tests use
   pyiceberg 0.10 to:
   - create a namespace and a table, append twice, delete a row, and read the rows back;
   - re-read with a new client.

   An earlier version ran against S3 (moto, then winterbaume) with DuckDB 1.5.4. It also wrote
   a format v3 table whose delete became a deletion vector (a puffin file). DuckDB cannot write
   to storage-testbench (see below), so the GCS test does not cover v3 yet.

   Gaps found along the way:
   - **REST `DELETE` of a table returned 503** until the task queue existed (2026-11-24, see
     "Task queue" below). The e2e test now purges a table and waits for the purge worker to
     delete its files.
   - **Done later:** everything on this list but roles and grants (out of scope), in step 5.
   - **Secrets.** Warehouse credentials need a secret store; see step 5 for the one built.
4. **Tests.** There is no backend-independent Rust suite upstream (see "Upstream test suites"
   below).
   - **Rust tests on the emulator (2026-11-24)** live in `src/store/tests/`. They are skipped
     unless `FIRESTORE_EMULATOR_HOST` is set; `tests/e2e` runs them first. They cover:
     - overlay reads, and queries merged with pending writes (limit, cursor, array-contains);
     - `create` preconditions, and a create deleted again in the same transaction;
     - concurrent read-modify-write, and the locks of a dropped transaction;
     - case-insensitive warehouse names, and the warehouse name race;
     - namespace uniqueness and paging;
     - tables: create, load and commit round trip, stale and racing commits, location
       overlap (inside, equal, around, and a sibling sharing a name prefix), staged tables
       replaced by a create, a staged table's metadata kept until its first commit, and a
       load failing when the metadata file is gone. Store tests read metadata files from an
       in-memory stand-in for object storage (`tests/mod.rs`); the e2e test reads real ones.

     They found two bugs, both fixed:
     - A lost `create` race returned 503, because Firestore reports it as `ALREADY_EXISTS`,
       not `FAILED_PRECONDITION`. It now returns 409.
     - A transaction dropped on an error path stayed open and kept its read locks, so the next
       writer of those documents waited and was aborted. Upstream's handlers usually drop
       instead of calling `rollback` (sqlx rolls back on drop). `Txn` now rolls back in the
       background when dropped.

     `src/store/tests/soft_delete.rs` covers soft delete, undrop, and the soft-deletion task
     on listings and namespace drop. Not covered yet: the rest of namespace drop, and listing
     pagination.
   - **Upstream's pyiceberg REST suite (2026-11-25):** `tests/upstream/plugin.py`, a pytest
     plugin, runs `tests/python/tests/test_pyiceberg.py` of the pinned Lakekeeper against this
     server (`uv run --project tests python -m pytest -p tests.upstream.plugin`), instead of a
     compose overlay. It starts the Firestore emulator, winterbaume-server for S3 and STS in
     place of MinIO, and the server with in-memory secrets (the suite's credentials are keys),
     and stands in for Keycloak with a token endpoint handing out a placeholder, as the server
     runs without authentication. With remote signing: 23 passed, and 1 skipped (OpenFGA only).
     It found a bug: the S3 signer resolves `tabular-id/{uuid}` by asking for the UUID as a
     table, a view and a generic table at once, and the store read the shared document once
     per candidate, so the signer got three answers and failed every signed write with 500.
     The store now reads each document once, as Postgres returns a row once. With vended
     credentials every test fails at warehouse creation: winterbaume's AssumeRole wants a
     `RoleArn`, as AWS does, and the suite's MinIO-flavoured profiles send none (filed as
     https://github.com/moriyoshi/winterbaume/issues/77, a MinIO-compatibility request).
5. **Remaining methods.** Namespace move, roles and grants, statistics and the cache change
   log. Add the secret store on Google Secret Manager. Done (2026-11-24):
   - the task queue (see "Task queue");
   - views: create, load, commit and, through the shared tabular code, list, drop, soft
     delete and undrop;
   - renaming tables and views, within a namespace or into another. As upstream, the rename
     finds nothing (404) unless the tabular is still in the source namespace under the source
     name and the destination namespace ID still bears the destination path, so a rename that
     lost a race fails. Names compare ignoring case, as the name markers do; a change of case
     alone keeps the marker. Staged tables cannot be renamed.

   The e2e test creates, replaces, renames, lists and drops a view over REST, and pyiceberg
   moves a table into another namespace and reads it back.

   Done (2026-11-25), each following upstream's Postgres store unless noted:
   - **Idempotency keys** (`src/store/idempotency.rs`): one `idempotency_records` document
     per committed success, `{warehouse}:{key}`, written in the mutation's transaction.
     Records carry `expire_at` (a Firestore timestamp, lifetime plus grace period) for a TTL
     policy, declared in `firestore.indexes.json`; TTL deletion can lag by a day, so an
     expired record counts as absent. The check reads outside a transaction; Firestore reads
     are strongly consistent, which is what upstream requires of it. Of two transactions
     inserting the same key, one commits and the other fails with 409 where upstream's
     replays; the client's retry then replays. Idempotency is on by default upstream, and
     the server advertises it in `getConfig`, so a client sending the header got 503 before.
   - **Namespace move** (`namespace::move_to`): only a namespace without children moves, as
     upstream, so the move rewrites the namespace document and its name marker; tabulars
     refer to it by ID. The ancestors keep their stored spelling, a retry is a no-op, and
     the guards match upstream's (protection, children, into itself, destination taken).
   - **Generic tables** (`src/store/generic_table.rs`): tabular documents of type
     `generic-table` whose format, doc, schema and statistics sit in a `generic` field (the
     JSON as strings, exempt from indexing). They share the name and location markers with
     tables and views, so the shared code renames, protects, soft-deletes, lists and drops
     them, and the projection skips them. Listing needs one more composite index.
   - **Users** (`src/store/user.rs`): needed once authentication is on, because every
     `getConfig` looks up the caller and registers a new one. Deletes are soft and scrub
     the name and email. No role assignments are stored, so a delete affects no role. A
     name filter and the user search read every active user and match substrings in
     memory; upstream's search ranks by trigram similarity in an index, this one puts an
     exact ID first and then users whose name or email contains the term.
   - **Tabular search** (the management API's, used by the console): a UUID finds that
     tabular and the tabulars of the namespace with that ID; other terms rank `ns.name` by
     `pg_trgm`'s trigram distance, computed here over every active tabular of the warehouse.
   - **Endpoint statistics**: always empty, because no statistics sink is configured; the
     page tokens step through windows as upstream's do.
   - **The cache change log** (see "Cache change log").
   - **The secret store** (`src/store/secrets.rs`, decided 2026-11-25): creating a
     warehouse stores its storage credential even when the credential only says "use the
     server's own identity", so a deployment needs a real store. Credentials are
     `secrets/{id}` documents, and only those without secret material are kept
     (`gcp-system-identity`, `aws-system-identity`, `azure-system-identity`): a key or
     password is refused with 400 `StorageCredentialNotStorable`, as anyone who can read
     the database could read it. On Cloud Run the service account reaches the buckets.
     `LAKEKEEPER__DEBUG__IN_MEMORY_SECRETS=true` still keeps any credential in memory, for
     local testing with keys; the e2e test now uses the Firestore store.
   - **The `snapshots=refs` load**: only the snapshots a branch or tag points to, filtered
     from the parsed metadata file (through its JSON form, as its fields are private); the
     snapshot log stays whole, as upstream's does.
   - **Tags** (`src/store/tag.rs`): definitions in `tag_definitions` (allowed values inside,
     sorted) with a case-insensitive name marker per project, and attachments in `tags`,
     keyed by target, definition and source, so applying a tag again finds it and the same
     value changes nothing. Attachments go with their warehouse, namespace or tabular, as
     upstream's cascading foreign keys make them go; a soft-deleted tabular keeps them but
     leaves the reverse lookup. Inherited tags walk the namespace path and the warehouse,
     with upstream's distances. The reverse lookup reads a definition's attachments a page
     at a time and filters in memory. Upstream also refuses a column tag for a field the
     table never had, and deletes column tags once the field leaves the table's last schema;
     this store does neither, since the core resolves the column in the current schema
     first and nothing removes old schemas yet.

   The e2e test replays a namespace create by its idempotency key, moves a namespace,
   creates, loads, lists, searches and drops a generic table, reads endpoint statistics,
   and tags a namespace, a table and a column (checking inheritance and the reverse
   lookup), over REST.

   Out of scope (decided 2026-11-25): roles and grants, with the role assignments and
   role-provider syncs that go with them. This catalog is meant for a small, self-contained
   installation behind the allow-all authorizer, with access to the server controlled
   outside it (Cloud Run IAM). Nothing on the catalog paths reaches them under allow-all
   (it declares no bootstrap grants), only their own management endpoints, which answer
   with an error naming the method.
6. **Deployment.** Container image for Cloud Run, Firestore indexes, and IAM.
   - **Drafted (2026-11-25):** `Dockerfile` and DEPLOY.md. The image builds with plain
     `docker build` (no BuildKit-only syntax, as the local Docker has no buildx), on
     `distroless/cc`, since the Azure SDK's `native-tls` links libssl dynamically; the
     console's build needs Node.js in the builder. The service runs with CPU always
     allocated and at least one instance, for the task queues and the change-log listener,
     behind Cloud Run IAM with `LAKEKEEPER__BASE_URI` unset, so URLs follow the request.
   - **Checked on a sandbox (2026-11-25):** `deploy/terraform` holds the whole deployment,
     Firestore indexes, exemptions and TTL policy included (from `firestore.indexes.json`,
     replacing the Firebase CLI), and a build account for Cloud Build, as new projects no
     longer give the default compute account the roles a build needs. Applied, checked and
     destroyed: private access, bootstrap, a keyless GCS warehouse with STS-vended tokens,
     pyiceberg writes and reads with an ID token, a background purge, the console, and clean
     logs (DEPLOY.md, "Checked so far"). Findings:
     - The build's memory, measured: about 3 GB for the console's `npm ci` (it builds
       upstream's console-components from git, with `vite-plugin-dts`), and 2.7 GB for the
       largest rustc. `CARGO_BUILD_JOBS` (default 2) and a 2 GB Node heap bound it; a 1.5 GB
       heap is too small. On `E2_HIGHCPU_8` (8 GB), 4 jobs built in 19 minutes. A local
       build on a VM with 3 GB free ran out of memory at 2 jobs, in the main crate, and
       built with 1 job; `tests/image` passed against that image (270 MB).
     - The new project's Cloud Build quota refused `E2_HIGHCPU_32`.
     - Terraform with a one-hour access token outlived it on the index builds; indexes left
       building were imported afterwards.
     - Not checked: a keyed credential refused by the store on a real project, and
       `gcloud run services proxy` (it needs a CLI component). From the source of the proxy
       (`GoogleCloudPlatform/cloud-run-proxy`): it forwards the service's own `Host` and no
       `X-Forwarded-Host`, so URLs the server builds from the request name the service. The
       console embeds one (`determine_base_uri`, unless `LAKEKEEPER__UI__LAKEKEEPER_URL` is set),
       and so does a table load's GCS refresh endpoint; DEPLOY.md gives the setting.
   - **Several instances (2026-11-25, on the emulator):** `tests/e2e/test_instances.py` runs
     two more servers on the e2e database, with hour-long cache entries: one evicts a renamed
     or dropped warehouse within seconds, from the change log, while one with invalidation off
     keeps serving it, which shows the lookup is cached. Loading a namespace is not cached, so
     namespaces are not checked that way. With three servers polling their task queues, the
     emulator aborted warehouse deletes ("Transaction lock timeout"): the delete counted
     unfinished tasks inside its transaction, contending with every claim. It now counts them
     outside it, as Postgres counts them under READ COMMITTED without locking out new tasks.

## Plan: normalized table metadata (drafted 2026-11-24)

Status: phase 0 (measurement) first; the rest is decided after it. Tables only; views stay on
GCS pointers.

**Goal.** Store table metadata in Firestore in parts, so that a commit writes only what changed,
a load can return only the snapshots refs point to (`snapshots=refs`), and per-field rows for
column tags have a base. The core still writes metadata files for engines; the catalog stops
reading them.

**What the core provides** (checked at v0.15.1): every `TableCommit` carries
`TableMetadataDiffs` (added and removed snapshots, schemas, partition specs, sort orders,
statistics, partition statistics and encryption keys, and snapshot-log and metadata-log
changes). Upstream reassembles metadata with the public declarative
`TableMetadata::builder()…build_unchecked()` and then `try_normalize()`
(`lakekeeper-storage-postgres/src/tabular/table/mod.rs`).

**Layout.** What grows gets documents of its own; the rest is one small head per table.

| Part | Documents | Why |
| --- | --- | --- |
| Format version, `last_*` counters, current ids, location, refs, partition specs, sort orders, metadata log | One head document per table | Small or bounded (the metadata log by `write.metadata.previous-versions-max`, default 100); rewritten by every commit |
| Snapshots | One per snapshot | What grows; added and removed by the diffs |
| Snapshot log | One per entry, ordered | Grows with snapshots; trimmed from the head |
| Schemas | One gzipped document per schema id | Immutable once added; one document per field, as upstream's `schema_field`, would be tens of thousands for wide tables |
| Statistics, partition statistics | One per snapshot id | Can be replaced for the same snapshot |
| Encryption keys | One per key id | v3 encryption |

Per-field anchors for column tags wait until tags are implemented.

**Phases.**
0. Measure the GCS-pointer store on tables with 100, 1,000 and 10,000 snapshots: load latency,
   metadata size, and reads and writes per commit. Check which loads the core's commit path
   needs in full.
1. Encoding and round trip: document types and split/reassemble functions, tested on v1, v2
   and v3 metadata, branches and tags, statistics, encryption keys, row lineage, and
   iceberg-rust's test files. No Firestore needed; most of the risk is here.
2. Create, including staged tables (which removes their special case).
3. Load, full and refs-only (`LoadTableFilters`): a refs load reads the head and the referenced
   snapshots by id, without a query.
4. Commit: apply the diffs in the transaction that checks the pointer, rewrite the head, and in
   tests assert that the reassembled metadata equals the `new_metadata` the core passed in.
5. Drop and purge: delete what fits in the drop transaction and leave the rest to a cleanup task
   on the task queue, with the head marked deleted until then.
6. Views stay on GCS pointers.
7. A store-wide setting chooses pointer or normalized mode, to compare them; nothing is deployed,
   so no migration.
8. Indexes for the per-table queries; e2e checks for many commits, snapshot expiry, a branch
   and a refs load; then upstream's pyiceberg suite.

**Risks.**
- Cost per load: Firestore bills per document read, so a full load of a table with 5,000
  snapshots is 5,000 reads where the pointer mode reads one object. This alone may make
  normalized mode the more expensive one; phase 0 decides.
- Atomic size: creating or registering a table with a long history writes many documents in one
  transaction. Check Firestore's current per-commit limits; very large registrations may need a
  two-step create.
- Diffs coverage: refs, properties, location and format upgrades are not in the diffs, but the
  head is rewritten by every commit. The phase 4 equality check backs this.
- Reassembly fidelity: `try_normalize` may fill in or reorder fields; the round-trip tests must
  define equality (struct or iceberg JSON).

**Open:** keep both modes or replace the pointer mode, decided after phase 0.

**Phase 0 results (2026-11-24).**
- *Every commit loads every snapshot.* The core loads the previous metadata of a commit with
  `LoadTableFilters::default()`, which is all snapshots (`server/tables.rs`, at v0.15.1), and
  without a fork the store cannot tell it otherwise. In normalized mode each commit would read
  the head, every snapshot document and every snapshot-log document: about 2 × snapshots + 1
  reads, where the pointer mode reads one object. Conditional `loadTable` requests
  (`If-None-Match`) are answered with 304 before any metadata is loaded, in either mode.
- *Sizes* (`src/store/tests/metadata_size.rs`, ignored test; appends with pyiceberg-style
  summaries, one small schema, no metadata log, release build on a laptop):

  | Snapshots | File (JSON) | File (gzip) | Parse | Head part | Per snapshot | Per log entry |
  | --- | --- | --- | --- | --- | --- | --- |
  | 100 | 52 KB | 5 KB | 0.4 ms | 565 B | 526 B | 55 B |
  | 1,000 | 520 KB | 53 KB | 2.9 ms | 566 B | 530 B | 55 B |
  | 10,000 | 5,234 KB | 610 KB | 33 ms | 567 B | 534 B | 55 B |

  A real head is larger (wide schemas, up to 100 metadata-log entries of about 150 B), but the
  growth is all in snapshots and the snapshot log.
- *Cost, roughly* (list prices to re-check: Firestore about $0.03–0.06 per 100,000 document
  reads by location; a GCS class B read about $0.004 per 10,000): at 1,000 snapshots a
  normalized commit reads about 2,000 documents, so one commit a minute is about 86 million
  reads a month, tens of dollars; the pointer mode's object reads cost cents. Firestore's free
  tier (50,000 reads a day) would be used up by about 25 such commits a day.
- *Pointer-mode commit cost* stays one object read plus a parse that is 33 ms at 10,000
  snapshots; a cache keyed by metadata location (files are immutable) removes most of it,
  since the previous metadata of a commit is usually the file the server last wrote or read.
- *Not measured:* latency against real GCS and Firestore; the emulators are not
  representative. That needs a Google Cloud project.

**Recommendation from phase 0:** do not build the normalized store while the core loads full
metadata for every commit. Superseded by the searchable projection (next section), which keeps
the pointer for loads. Instead, in the pointer mode: add the location-keyed metadata cache,
and apply the `snapshots=refs` filter to the parsed file. Revisit normalizing if upstream lets
the store load commits partially, or when column tags need per-field documents (which do not
need the rest normalized).

## Plan: searchable metadata projection (2026-11-24; done 2026-11-25)

Chosen over normalizing (previous section): loads keep reading the metadata file through the
pointer, and a projection of the metadata into queryable documents serves search. Nothing on
the load or commit path reads the projection, so its size never makes commits expensive; the
commit diffs keep its writes proportional to what changed.

**What must be searchable** (owner, 2026-11-24): columns and schemas, snapshots and history,
partitioning and properties, and data statistics. **Access:** a companion CLI of this
project, reading the projection in Firestore directly.

**Projection documents.** Keyed by table ID, so renames and namespace moves touch only the
table document. Searchable fields are stored typed and indexed; everything else is stored
unindexed (single-field exemptions), to keep index entries and write cost down.

| Collection | One document per | Searchable fields | Maintained from |
| --- | --- | --- | --- |
| `idx_tables` | table | warehouse, namespace, name, format version, current snapshot totals (records, data files, delete files, file size), last updated, current schema and default spec ids, ref names | rewritten by every commit; rename and soft delete |
| `idx_snapshots` | snapshot | table, warehouse, timestamp, operation, parent, sequence number, summary numbers (added and deleted records and files, totals), schema id, refs pointing at it, statistics file present | `added_snapshots` / `removed_snapshots`; refs moved by the commit |
| `idx_fields` | table or view, and field ID | table or view, warehouse, kind (`table` or `view`), folded name and path, type, required, current, oldest and newest schema with the field | comparing the field projections before and after the commit: only fields that changed |
| `idx_partition_specs` | table and spec | default flag, and arrays of source field IDs, transforms, names and `{source}:{transform}` pairs (array-contains queries) | added and removed partition specs, and the default spec |
| `idx_views` | view | warehouse, namespace, name, current version and schema ids, when the current version was made, version count, SQL dialects (folded), default namespace | rewritten by every replace; rename and soft delete |

Properties stay on the tabular document, whose map fields Firestore already indexes.
`idx_fields` doubles as the per-field anchor that column tags need.

**Views** (phase 5) get a head document in `idx_views` and their columns in `idx_fields`, with
`kind = view`. The core passes a view commit both the previous and the new metadata, so the
field projections before and after are computed from them, with no reads but the head
document; views have no snapshots or partition specs. The SQL text is not projected:
Firestore has no substring search.

**Per-part changes, not a JSON diff.** The metadata is made of independent parts with keys,
and each part's changes are found by key; the values always come from the new metadata, and
nothing is replayed. The core's `TableMetadataDiffs` (`calculate_diffs`, `server/tables.rs`)
compares previous and new metadata part by part, but by ID sets only, so it misses parts that
change under the same key. Change sources per part:

| Part | Keyed by | Changes found by |
| --- | --- | --- |
| Snapshots, schemas, partition specs, sort orders | ID; immutable once added | the core's diffs |
| Fields | field ID | comparing the field projections before and after; see below |
| Refs | name; moved by most commits, not in the diffs | comparing the new refs with those in `idx_tables` |
| Statistics, partition statistics | snapshot ID; replaceable, which the diffs miss | the commit's `SetStatistics` / `RemoveStatistics` / `SetPartitionStatistics` / `RemovePartitionStatistics` updates (`TableCommit.updates`), plus the diffs for removed snapshots |
| Properties, location, counters, current IDs | the table | rewritten with `idx_tables` (small) |

**Fields** are a pure function of the metadata's schema list and current schema ID: the
attributes come from the newest schema with the field, with the oldest and newest schema IDs
(`None` when the newest schema has it) and a current flag. Without schema removals, the schema
list before a commit is the list after it minus the added schemas, so both field projections
are computed in memory and only the fields that differ are written; adding a schema to a wide
table writes only the fields it adds, drops, renames or retypes. A commit that removes schemas
needs the stored fields (`NeedsPreviousFields`); removing the oldest schema rewrites the
oldest-schema ID of every field it had.

**Consistency.** Projection writes go in the transaction that swaps the pointer, so the
projection always matches the current metadata. The commit gets the new metadata and the
diffs but not the previous metadata. The previous current schema and refs come from
`idx_tables`; if the previous current schema was removed in the same commit, its fields come
from a query of the table's current `idx_fields`.

**Visibility.** Soft deletion, undrop and rename update only the head document (`idx_tables`
or `idx_views`). The CLI resolves
liveness and names through it: Firestore has no joins, and flagging every snapshot document
would make soft deletion O(snapshots).

**Large histories.** Creating or registering a table with a long history, and dropping one,
touch many documents. Up to 400 documents are written or deleted in the transaction. Beyond
that, the change sets `idx_tables.pending` and enqueues a task on the `metadata_projection`
queue, which this crate registers with the core (`register_additional_task_queues_fn`):
- **One kind of task: reconcile.** If the table exists with a metadata file, the stored
  projection becomes the projection of that file; otherwise all of it is deleted. The task
  compares the stored documents with the target and writes only those that differ, so the same
  task repairs a projection. It can be scheduled for a table through the management API's
  schedule endpoint (user scheduling is enabled).
- **While pending,** commits rewrite only `idx_tables` (still pending). A commit, drop or
  rename always finds the task: an active one is left in place, since it checks the table
  again before it finishes.
- **Batches** of 400 documents, each in a transaction that first reads the tabular document
  and starts the pass over if its metadata location (or its existence) changed. The last
  transaction makes the same check, writes `idx_tables` from the table as it is then
  (`pending` cleared, or the document deleted), and records the task's success, so a drop that
  comes after it sees no active task and enqueues a new one. After 5 passes the attempt fails
  and is retried by the queue.
- **Stopping** a task through the management API ends it without a retry; the projection
  stays pending until another task reconciles it.
- **Namespace drops** do not report these tasks as open tasks for the core to cancel: they
  delete what the dropped tables leave. Warehouse deletion waits for them (the queue's
  `on_warehouse_delete` is the default, `Block`).
- **Workers:** `LAKEKEEPER__FIRESTORE__PROJECTION_WORKERS` per process, default 2; none in
  read-only maintenance mode.
- `idx_tables.documents` counts the part documents, so that a drop knows its size without a
  query.

**The CLI** is the `search` subcommand of this crate's binary (`src/store/search/`), not a
second binary as first planned: a second binary of a binary-only crate cannot reach the
store's modules without splitting the crate into a library, and as a subcommand it shares the
document types and the `LAKEKEEPER__FIRESTORE__*` settings. Commands, with `--json` for one
object per row:

| Command | Answers | Query |
| --- | --- | --- |
| `snapshots` | snapshots of a warehouse or table, newest first, by time range, operation and summary numbers | `idx_snapshots` by warehouse or table, operation, `timestamp_ms` range; `--where added_records>=N` conditions apply to the results |
| `history` | a table's snapshots in commit order, with sizes, delete-file counts and refs | `idx_snapshots` by table, by `sequence_number` |
| `columns` | tables and views with a column, by name or path, type, kind, current or past | `idx_fields` equalities |
| `schema-changes` | when each column of a table was added and dropped | the table's `idx_fields`, timed by the oldest kept snapshot of each schema |
| `partitioned` | tables by transform or source column, in default specs or all | `idx_partition_specs` array-contains; by column, the column's `idx_fields` first, since field IDs differ per table |
| `properties` | tables and views by property, or property and value | `tabulars.properties.<key>` |
| `largest` | the largest tables by files size, records, data files or delete files | `idx_tables` by `current_summary.*`, descending |
| `indexes` | prints `firestore.indexes.json` for the configured prefix | none |

Tables are named `namespace.table` (or by ID) and resolved through the name markers; results
are named and filtered through `idx_tables`, leaving out soft-deleted tables unless
`--include-deleted`, marking pending projections. Schema changes do not show renames or type
changes: a field keeps the attributes of its newest schema. Summary-number conditions are not
query filters, because Firestore needs a composite index per combination of inequality fields;
a time range or a table keeps them cheap.

It authenticates with application default credentials (or the emulator) and bypasses
Lakekeeper's authorizer: anyone who can read the Firestore database sees all metadata.

**Indexes** (`src/store/search/indexes.rs`): one list of the composite indexes and
single-field exemptions of the whole catalog, store and CLI, which `search indexes` prints as
`firestore.indexes.json` with the configured collection prefix. The file checked in is the one
for no prefix; a test keeps it current. Equality-only queries rely on Firestore merging
single-field indexes, so they have no composite index; that includes property searches, whose
map keys cannot be listed in advance. The exemptions keep unsearched fields (summary entries
other than the numbers, manifest lists, statistics files, unfolded names and paths, refs and
locations) out of every index.

**Phases.**
1. **Done (2026-11-24),** `src/store/projection.rs`, tested by `tests/projection.rs` (no
   emulator; checked to fail when ref or statistics changes are ignored). Projection types and
   two pure functions: the full projection of a metadata, and the writes
   a commit's changes imply, per part as above. Property test: over a sequence of commits
   (appends, deletes, schema and partition evolution, branches, a ref moved without a new
   snapshot, statistics replaced for the same snapshot, expiry), the incrementally maintained
   projection equals the full projection of the final metadata.
2. **Done (2026-11-25),** `src/store/projection_store.rs`, tested on the emulator by
   `tests/projection_store.rs` (checked to fail when rename or a recursive namespace drop skips
   the projection). Wired into create, commit, drop (of a table, and of a namespace with
   `recursive`), soft delete, undrop and rename, in the same transaction:
   - A table created with a metadata file stores its whole projection; a staged table has none
     until its first commit. A commit to a table without an `idx_tables` document stores the
     whole projection, which also covers a staged table's first commit and tables created
     before the projection existed.
   - A commit reads `idx_tables` (one document read), plus the table's `idx_fields` (one
     query) only when it removed schemas.
   - Rename, soft delete and undrop rewrite `idx_tables` only: name, namespace and an `active`
     flag.
   - A drop queries the table's documents in the three part collections and deletes them.
   Phase 3 moves what is too large for the transaction to a task.
3. **Done (2026-11-25),** `src/store/projection_task.rs`, as in "Large histories" above.
   Tested on the emulator with the inline limit lowered to 16 documents: a large create and a
   commit while pending, a large commit and a large drop, a repair of lost, stale and stray
   documents, and a commit landing while a task runs (checked to fail without the re-checks,
   or without deletes in the comparison). e2e (`check_projection_repaired`) schedules a task
   through the management API on the running server and waits for its success.
4. **Done (2026-11-25),** `src/store/search/` as above, tested on the emulator by
   `tests/search.rs` with pages of 2 documents, so that every search crosses pages (checked to
   fail without the page cursor, or without the liveness filter), and against the e2e data by
   hand. Composite indexes are untested until a real Firestore database: the emulator ignores
   them.
5. **Done (2026-11-25),** views as above, through the same paths as tables: create, replace,
   rename, soft delete, undrop, drop (also of a namespace), and the task for large views
   (with view entities). Tested by `tests/projection.rs` (replacements, without the
   emulator) and `tests/view_projection.rs` (on the emulator, including the task and the CLI;
   checked to fail when replace or rename skips the projection).
6. **Done (2026-11-25),** `test_search` of `tests/e2e/test_catalog.py`: pyiceberg appends to a new
   table, adds and renames a column, adds a `bucket[4]` partition field, creates a branch on an
   older snapshot and sets a property; a REST commit expires the first snapshot (pyiceberg 0.10
   has no expiry). The CLI's `history`, `snapshots --where`, `columns`, `schema-changes`,
   `partitioned`, `properties` and `largest` answers are compared with the metadata pyiceberg
   loads back (10 checks; checked to fail when commits ignore ref moves).

All phases are done. Not covered: the composite indexes against a real Firestore database
(the emulator ignores them), and latency there.

**Cost, measured (2026-11-25)** on the emulator by `tests/projection_cost.rs`, which counts
what Firestore bills (document reads, writes and deletes) per operation, split between the
store's collections and the projection's, with the production inline limit. Tables from
`metadata_size.rs` (pyiceberg-like appends); reads / writes / deletes:

| Operation | Store | Projection, 10 / 100 / 1,000 snapshots |
| --- | --- | --- |
| append | 3 / 1 / 0 | 1 / 3 / 0 at every size |
| add a column | 3 / 1 / 0 | 1 / 2 / 0 |
| create a branch | 3 / 1 / 0 | 1 / 2 / 0 |
| expire k snapshots | 3 / 1 / 0 | 1 / 1 / k |
| rename, soft delete | 6 / 2 / 1, 3 / 1 / 1 | 1 / 1 / 0 |
| create | 14 / 3 / 0 (30 / 10 / 2 with the task) | 0 / N+3 / 0 (by a task at 1,000) |
| drop | 4 / 0 / 2 (19 / 7 / 4 with the task) | N+4 / 0 / N+4 (by a task at 1,000) |
| `search history` | 4 / 0 / 0 | N+2 reads |
| `search largest` | 1 / 0 / 0 | 2 reads |

So a commit's projection cost does not grow with the table: an append adds one read and three
writes (the table document, the new snapshot, and the snapshot `main` moved from) to the
store's three reads and one write. Creating, registering and dropping a table cost one write
or one read and delete per snapshot, once. A task adds about 16 reads and 7 writes of task
bookkeeping.

Stored sizes, by Firestore's documented size rules: a snapshot document is about 760 bytes,
a field about 360, a partition spec about 310 and the table document about 790. Index entries
dominate storage: a snapshot document has about 36 single-field index entries, mostly the
ascending and descending entries of its 19 summary numbers, although no query filters or
orders by them (`--where` conditions are applied to results). Exempting `idx_snapshots.summary`
would leave about 6; `idx_tables.current_summary` is ordered by `search largest` (descending
only) and needs only that.

Decided 2026-11-25: `idx_snapshots.summary` is exempt by default, and
`search indexes --index-snapshot-summary` keeps it indexed, for queries of one's own.

**Separately**, the pointer mode gets the location-keyed metadata cache and the
`snapshots=refs` filter applied to the parsed file.

## Findings

### Firestore client

Use the [`firestore`](https://crates.io/crates/firestore) crate (0.67, Apache-2.0), for its
connection setup and its serde conversion only:
- **Connection.** `FirestoreDb::with_options` handles ADC auth and `FIRESTORE_EMULATOR_HOST`.
- **Serde conversion.** `firestore_document_from_serializable` and `firestore_document_to_serializable`.
- **Transactions go through the raw gRPC API.** The crate's re-exported `gcloud_sdk` client is used
  directly (`BeginTransaction`, `BatchGetDocuments`, `RunQuery`, `Commit`, `Rollback`). The crate's
  own `FirestoreTransaction<'a>` borrows the database handle, and it has no read-your-writes
  overlay, so it cannot be the owned `Transaction` that Lakekeeper's trait needs.

Google's official Rust SDK (`googleapis/google-cloud-rust`) publishes only the Firestore admin API
(`google-cloud-firestore-admin-v1`), with no data-plane client.

The crate builds alongside Lakekeeper with no version conflicts, but it brings its own
tonic/prost stack.

### Upstream test suites (checked at v0.15.1)

No upstream suite checks a third-party `CatalogStore` at the Rust level:
- **`crates/lakekeeper-integration-tests` is tied to Postgres.** It has about 800 `#[sqlx::test]`
  functions taking a `PgPool`, and its fixtures (`setup`, `SetupTestCatalog`, `get_api_context`)
  are re-exported from `lakekeeper_storage_postgres::test_utils`. Its pagination macro also expands
  to `#[sqlx::test]`. Reusing these tests means copying them and swapping in a Firestore fixture.
- **The core `test-utils` feature has fixtures only**, such as `MemoryProfile`,
  `RequestMetadataTestBuilder` and `new_random()` constructors.

The suite that works against any backend is `tests/python`. It runs pytest through the REST API
(pyiceberg, Spark, Trino, StarRocks). Its docker-compose starts Postgres and a `migrate` job, and
replacing those with this server is enough. `bash run.sh pyiceberg` (MinIO only) is the cheapest
place to start. Plan: adopt the pyiceberg suite through a compose overlay, and add Rust tests
against the emulator for the store methods.

### Firestore emulator

The Java emulator runs from Docker (`gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators`,
`gcloud emulators firestore start --host-port=0.0.0.0:8080`), so no local Java is needed.

Under lock contention the emulator can abort both of two racing transactions (409), so a
test of racing claims retries an aborted claim, as the core's workers do on their next poll.

### Object storage for tests (checked at v0.15.1)

Warehouse creation validates the bucket, the server writes `metadata.json` itself, and clients
write data files, so REST end-to-end tests need real object storage. A GCS emulator cannot
stand in: `GcsProfile` has no endpoint field, `GCSSettings` is empty, and
`crates/io/src/gcs.rs` passes `endpoint: None` to `build_clients` outside upstream's unit tests.
The `google-cloud-storage` SDK reads no emulator variable, and token exchange is fixed to
`oauth2.googleapis.com`. Supporting a GCS emulator needs an upstream change. Store-level tests
need only the Firestore emulator.

That change is commit `37e65201` on branch `gcs-emulator` of the fork,
github.com/moriyoshi/lakekeeper (based on upstream `main`, after 0.15.1). Branch
`gcs-emulator-0.15.1` (`95bdd50d`) carries the same commit on top of v0.15.1, and the
`[patch]` section of `Cargo.toml` builds against that branch from GitHub (since 2026-11-25;
before, from a checkout at `../lakekeeper`), with `Cargo.lock` pinning the commit. The change:
`LAKEKEEPER__GCS_EMULATOR_ENDPOINT` and `LAKEKEEPER__GCS_EMULATOR_GRPC_ENDPOINT` send all GCS
requests to an emulator with anonymous credentials, and tables return `gcs.service.host` and
`gcs.no-auth`. The first commit made GCS profiles set `sts-enabled: false`; commit `227554f2` on the same
branch (2026-11-25, for the console's query engine, below; pushed) lets them enable it
instead: STS-enabled profiles vend the placeholder token `emulator`, which the
emulator accepts, and skip the check that vended credentials cannot write outside the table,
which no emulator token can pass. It was checked against Google's
storage-testbench (`gcr.io/cloud-devrel-public-resources/storage-testbench`) with pyiceberg.
What the emulator needs:

- Both APIs: the SDK reads, lists and deletes object metadata over gRPC (testbench:
  `GET /start_grpc?port=8888`) and transfers data over the JSON API.
- Port 80 for the JSON API: the SDK sends the Host header without the port, and the testbench
  builds resumable upload URLs from it.
- Resumable uploads only: SDK 1.22 adds a third `checksums` part, and leaves out the media
  part's content type, in single-request uploads, which the testbench rejects. The patch sets
  the resumable threshold to 0 for the emulator.
- pyiceberg ignores `gcs.no-auth`; set `gcs.oauth2.token=anon` on the client. gcsfs also
  probes the bucket type over gRPC on port 443 and waits 60 s per bucket, unless
  `GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT=false`.
- DuckDB writes large `gs://` objects with S3-style multipart uploads, which the testbench
  does not implement ("Unexpected response when uploading part"); small writes are single
  `PUT`s, which work (the console's query engine, below).

**The console's query engine with the emulator** (2026-11-25, `src/ui.rs`, `ui` feature). The
console's LoQE runs DuckDB-WASM in the browser and reads and writes table files itself, so it
needs vended credentials (`gcs.oauth2.token`; the warehouse must enable STS) and reaches
storage at a fixed `https://storage.googleapis.com`: the console's scripts call the JSON API,
DuckDB's Iceberg extension builds path-style object URLs, and neither reads
`gcs.service.host`. With `LAKEKEEPER__GCS_EMULATOR_ENDPOINT` set, the server:

- serves the console's scripts with that address replaced by `/gcs-emulator`, and DuckDB's
  worker scripts with a shim first that redirects their `XMLHttpRequest` and `fetch` requests
  for it there (the console starts DuckDB in a `blob:` worker that imports
  `duckdb/duckdb-browser-eh.worker.js`);
- proxies `/gcs-emulator/*` to the emulator, outside the console router's compression and
  timeout layers (compression drops the `Content-Length` of `HEAD` answers, which DuckDB reads
  sizes from). Same origin, so the emulator needs no CORS configuration.

Checked in a browser: a count over `sales.orders` and an `INSERT` (data file and manifests
written through the proxy, then committed through the catalog). The rewritten scripts get a
new `ETag`, but their `max-age` of an hour can keep an earlier copy in a browser that loaded
the console before the emulator was configured.

### Semantics to keep (from the Postgres backend)

- **Case-insensitive names.** Warehouse names, namespace paths and tabular names use a
  case-insensitive collation. Marker document IDs therefore use the lowercased name, while the
  stored spelling stays canonical.
- **Tabular name uniqueness** covers only tabulars that aren't soft-deleted (`deleted_at` is part of
  the unique key), so a soft delete releases the name marker.
- **Server row.** Postgres creates the server row (server ID, `open_for_bootstrap`) when it migrates.
  Here it is created on first use.

### Store layout and transactions (`src/store/`)

- **`fs.rs` is the access layer.** `Txn` holds the Firestore transaction ID, started lazily with
  `new_transaction` on the first read, plus the pending writes:
  - Reads are answered from the pending writes first.
  - Queries are re-filtered locally against them: equality and array-contains filters, ordered by
    one field. Only these query shapes are used, so the overlay can evaluate them.
  - `Commit` sends one write per document. A `create` carries an `exists = false` precondition.
  - A failed `create` precondition comes back as `ALREADY_EXISTS` and maps to 409
    (`FAILED_PRECONDITION` too). `DEADLINE_EXCEEDED`, `UNAVAILABLE` and `UNKNOWN` map to
    `CommitStateUnknownException`. `ABORTED` maps to a concurrent-modification backend error
    (409).
  - Read-write transactions lock what they read. When two transactions read the same document
    (or the same missing marker) and then both write it, each waits for the other's read lock,
    and Firestore can abort both ("Transaction lock timeout"). Both callers then get 409, so a
    race can end with no winner; it never ends with two.
  - Reads through `State` use no transaction.
- **`docs.rs` holds the document layout**, with a table in its module docs. All collections are
  top-level and carry `LAKEKEEPER__FIRESTORE__COLLECTION_PREFIX`. Natural keys (warehouse name,
  namespace path, tabular name, tabular location) are made unique by marker documents written in
  the same transaction.
- **Table and view metadata stay in object storage** (`src/store/files.rs`, 2026-11-24), as
  planned in "Mapping onto Firestore": Firestore keeps the metadata location, and a load reads
  the file. The core writes each metadata file under a new name before the transaction that
  points to it commits (checked at v0.15.1 for table create and commit, and view create and
  replace), and never rewrites one, so a pointer read in a transaction names a complete file
  that does not change. The store reaches storage as the core does: the warehouse's storage
  profile, its credential from the server's secret store, and `StorageProfile::file_io`.
  `.gz.metadata.json` files are gunzipped.

  A commit therefore writes only the pointer, whatever the metadata's size. Each load costs
  an object read, including the load inside every commit; there is no cache yet. Files
  written are immutable, so a cache keyed by location would need no invalidation.

  Step 3 first stored the metadata itself in Firestore, gzipped and chunked, departing from
  this plan without saying so; it was moved back. Upstream's Postgres store normalizes
  metadata for two reasons, neither of which needs it here: a `loadTable` that returns only
  the snapshots refs point to (#478; here it can filter the parsed file), and column-level
  tags that reference fields (#1897; per-field anchor documents can be written at commit
  from the new metadata the core passes in).
- **The `idx_` collections** hold the searchable projection of table metadata ("Plan:
  searchable metadata projection"); only commits read them.
- **Staged tables** have no metadata file until their first commit, so their initial metadata
  is a gzipped JSON document in `table_metadata`, deleted by that commit or a drop.
- **Properties limit.** The tabular document copies the table properties, so properties over
  about 1 MiB in total make the write fail. Postgres has no such limit.
- **View default namespaces.** Upstream's Postgres store keeps each view version's default
  namespace as a namespace ID, so a renamed namespace is renamed in the view too. The metadata
  file keeps the name it was committed with, so a namespace move does not reach it.
- **Dropped transactions** are rolled back in the background (`Drop for Txn`), so a failed
  request does not leave locks behind. The rollback needs a live runtime: a transaction
  dropped while the process shuts down (or at the end of a `#[tokio::test]`) keeps its locks
  until Firestore expires it, so tests roll back explicitly.
- **Location overlap** is checked two ways. Marker reads cover the location and its parents. An
  array-contains query on `location_prefixes` covers tabulars stored below it.
- **Paging.** Page tokens are `{created_at}|{id}`, read by this store only.
- **Indexes** are declared in `src/store/search/indexes.rs`, next to the query that needs each,
  and deployed from the generated `firestore.indexes.json` (see the projection plan's
  "Indexes"). The emulator needs none of them.

### Task queue (`src/store/tasks.rs`, 2026-11-24)

The contract comes from upstream's trait docs and its Postgres implementation. Layout:

- **`tasks`** holds active tasks (scheduled, running, should-stop).
- **`task_keys`** holds one marker per project, warehouse, entity and exact queue name, which
  keeps one active task per entity and queue. A schedule's tasks use the schedule ID as their
  entity, so each schedule has at most one active instance.
- **`task_logs`** holds one document per finished attempt, keyed by task and attempt and created
  with `exists = false`. The first recorder of an attempt wins. Later ones get the errors the
  core treats as "attempt closed": 409 `TaskAttemptAlreadyCompleted` or
  `TaskAttemptAlreadyFailed` when a log exists, 404 `TaskNotFound` when neither exists.
- **`task_configs`** and **`task_schedules`**: schedule IDs are upstream's UUIDv5 derivation, so
  a schedule deleted and created again keeps its ID.

**Picking** has no `SKIP LOCKED`. Each queue name gives its oldest due task in the demand range
(a query by `scheduled_for`, filtered locally for demand). A claim then runs in its own
transaction: it re-reads the task and flips it to running only if it is still scheduled at the
same attempt. A claim lost to another worker (409) moves on to the next candidate, up to three
times. Before that, one attempt whose heartbeat expired is failed through the retry policy, as
upstream does. Times come from this server's clock, not a database clock.

**Done:**
- enqueue (one and batch), pick, success, failure with retry, backoff and give-up;
- heartbeat, cancel, queue configs, and task-log cleanup (at most 400 log documents per run;
  the rest go in later runs);
- schedules: upsert with conditions, list, delete, trigger, materialize, and per-project
  states;
- around tasks: warehouse delete refuses while tasks are unfinished, namespace drop reports
  child tabulars' open tasks for the core to cancel, project delete removes the project's
  tasks, schedules and configs, and a new tabular location may not be at, in or around a
  pending purge's location;
- soft deletion: listings of deleted tabulars carry their soft-deletion task (read through the
  task key markers, under the current and the legacy queue name); undrop takes the name back,
  failing with `TabularAlreadyExists` if a new tabular holds it, and returns the task for the
  core to cancel; the task-cancel variant undrops only tabulars of the given tasks; namespace
  drop refuses (409) while a child's soft-deletion task is running, as upstream does;
- the management API (`src/store/task_admin.rs`): list, details, resolve, stop, run-at and
  re-parent, following upstream's Postgres store. A listing has each matching active task and
  the newest matching log of each task, so a task being retried is listed twice, as upstream
  does. Details have the newest attempt and up to `num_attempts` earlier ones. Resolving falls
  back to the newest log of a finished task. Rescheduling a stopping task logs its attempt as
  failed.

  A listing reads every active task and log of the warehouse (or project), one single-field
  query each, and filters, sorts and pages in memory: no composite index, but the cost grows
  with the task log, which log cleanup bounds. The page token is
  `created_at|task_id|attempt|active`; upstream's token stops at the task ID, so a page
  boundary between a retried task's two rows drops one of them, which ours does not;
- queue demand metrics (`task_queue_demand`, `task_queue_oldest_due`): due, heartbeat-expired
  and running tasks of a queue (with its legacy names) in a resource demand range, and since
  when the longest waiting one can be taken. A heartbeat expires by the same rule as the
  pick's sweep, including the queue config's timeout. Upstream counts a running task that has
  never heartbeated as running; here its timeout runs from `scheduled_for`, as the sweep's
  does. A claim sets the heartbeat, so only a corrupt task differs. The counts read every
  due and every running task of the queue; the oldest-due variant reads due tasks only up to
  the first in the range.

Tests are in `src/store/tests/tasks.rs`, `task_admin.rs` and `soft_delete.rs`, plus the
purge-location test in `table.rs`. The e2e test lists the purge task and reads its details
over REST.

Every task method of the store is implemented.

### Cache change log (`src/store/cache_change_log.rs`, 2026-11-25)

Lakekeeper caches warehouses, namespaces, secrets and more per instance, and evicts what
other instances changed by reading a change log the writes record. With it, several
instances can serve one catalog.

- **Recording.** A write records its changed keys in `cache_changes` documents, in its own
  transaction: one per domain and up to 1000 keys (none for a whole domain), with
  `recorded_at` from this server's clock. `domain` and `keys` are exempt from indexing.
- **Reading.** Postgres reads its log by transaction ID. Firestore has nothing like it: a
  field written in a transaction does not hold the commit time (`REQUEST_TIME` is when the
  commit request arrived, so a read between that and the commit would miss the document,
  and a later query by time would skip it). Each instance therefore reads the log through
  a Firestore listener, which delivers every committed document once, in consistent
  snapshots. Changes wait in the listener until the poller takes them.
- **Start and window.** The listener's query is "recorded after ten minutes before the
  listener started". The first snapshot is the state the log starts at, so the window only
  bounds that first read (and clock skew between instances must stay under it).
- **Target resets.** The emulator answers every change with a target reset and resends the
  whole result set; Firestore may reset a target too. The listener keeps the names of the
  documents it has passed on, so only new documents count.
- **Reconnects.** A stream that ends resumes from its last consistent snapshot. One that
  cannot resume starts again, and the next poll returns a reset (`ServerChanged`), so the
  instance treats every cached entry as stale.
- **Pruning** deletes documents older than upstream's retention (twice the longest cache
  TTL, at least two hours), up to 10 000 per run, in plain batch writes that another
  pruner's deletes cannot conflict with.
- **Cost.** One read per change document per listening instance, plus the first snapshot,
  instead of a query per poll interval. On Cloud Run with request-based billing, an idle
  instance gets no CPU; it catches up when the next request arrives, as Postgres' poller
  would.

`src/store/tests/cache_change_log.rs` checks that a second log sees committed changes, and
does not see rolled-back ones or the ones before it started.

## Open questions
- **Secret store.** Settled: keyless credentials in Firestore (step 5). Secret Manager or
  encrypted documents would be needed only to store keys.
- Can DuckDB write through the catalog with GCS credentials vended by Lakekeeper? Real GCS is
  needed to find out; the emulator cannot answer it (see "Object storage for tests").
- What is the upgrade policy? The trait is internal and changes between releases, so each tag bump
  needs review.
