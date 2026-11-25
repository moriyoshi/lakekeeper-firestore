# Deploying on Cloud Run

A guide for one catalog in one Google Cloud project: a Firestore database, a GCS bucket for
the warehouse, and a Cloud Run service running as its own service account. Steps 1 to 5 are
also a Terraform stack, `deploy/terraform` (see "With Terraform"), which was applied to a
sandbox project, checked and destroyed on 2026-11-25 (see "Checked so far").

The catalog has no access control of its own: it runs Lakekeeper's allow-all authorizer, so
anyone who reaches the server administers every warehouse. Cloud Run IAM is the gate. Keep
the service private (`--no-allow-unauthenticated`) and grant `roles/run.invoker` only to
those who may use the catalog.

```sh
PROJECT=my-project       # the Google Cloud project
REGION=asia-northeast1   # Cloud Run, Artifact Registry and Firestore location
BUCKET=my-warehouse      # the warehouse bucket
SA=lakekeeper@$PROJECT.iam.gserviceaccount.com
IMAGE=$REGION-docker.pkg.dev/$PROJECT/lakekeeper/lakekeeper-firestore:0.1.0
```

## 1. APIs, Firestore and the bucket

```sh
gcloud services enable --project $PROJECT \
  run.googleapis.com firestore.googleapis.com artifactregistry.googleapis.com \
  cloudbuild.googleapis.com sts.googleapis.com

# A Firestore database in Native mode. For a named database, pass --database=NAME here and
# set LAKEKEEPER__FIRESTORE__DATABASE_ID=NAME on the service.
gcloud firestore databases create --project $PROJECT --location $REGION

gcloud storage buckets create gs://$BUCKET --project $PROJECT --location $REGION \
  --uniform-bucket-level-access
```

Several catalogs can share one database with different
`LAKEKEEPER__FIRESTORE__COLLECTION_PREFIX` values; each needs the indexes for its prefix.

## 2. Indexes and the TTL policy

The catalog's queries need composite indexes, single-field exemptions (for large or
unqueried fields) and a TTL policy on idempotency records. `search indexes` prints them all,
for the collection prefix in its environment, in the Firebase CLI's format;
`firestore.indexes.json` in this repository is the output for no prefix.

```sh
# Only with a collection prefix:
LAKEKEEPER__FIRESTORE__COLLECTION_PREFIX=prod_ \
  ./target/release/lakekeeper-firestore search indexes > firestore.indexes.json

cat > firebase.json <<'EOF'
{ "firestore": { "indexes": "firestore.indexes.json" } }
EOF
npx firebase-tools deploy --only firestore:indexes --project $PROJECT
```

The Firebase CLI may ask to add Firebase to the project first. For a named database,
`firebase.json` takes a list: `{"firestore": [{"database": "NAME", "indexes":
"firestore.indexes.json"}]}`. Indexes build in the background: wait until
`gcloud firestore indexes composite list --project $PROJECT` shows them all `READY`
before serving traffic, or queries fail with `FAILED_PRECONDITION`.

The snapshot summary map is exempt from indexing by default, as no built-in query filters
on it. `search indexes --index-snapshot-summary` indexes it, for queries of your own.

## 3. Service account and IAM

```sh
gcloud iam service-accounts create lakekeeper --project $PROJECT \
  --display-name "Lakekeeper catalog"

# Firestore reads, writes, transactions and listeners.
gcloud projects add-iam-policy-binding $PROJECT \
  --member serviceAccount:$SA --role roles/datastore.user

# The warehouse's objects: table data and metadata, and the check object Lakekeeper
# writes when a warehouse is created.
gcloud storage buckets add-iam-policy-binding gs://$BUCKET \
  --member serviceAccount:$SA --role roles/storage.objectUser
```

Storage credentials are kept in Firestore, and only those that name the server's own
identity are accepted (see DESIGN.md, step 5): a service account key is refused with 400
`StorageCredentialNotStorable`. To serve another bucket, grant this service account access
to it.

People and service accounts who use the catalog need `roles/run.invoker` on the service
(step 5). Deploying needs `roles/run.admin`, `roles/iam.serviceAccountUser` on `$SA`,
`roles/artifactregistry.writer` and `roles/cloudbuild.builds.editor`; deploying the indexes
needs `roles/datastore.indexAdmin`.

## 4. Build the image

```sh
gcloud artifacts repositories create lakekeeper --project $PROJECT --location $REGION \
  --repository-format docker

# The console is included; add `,_FEATURES=` to the substitutions to leave it out.
gcloud builds submit --project $PROJECT --region $REGION \
  --config deploy/cloudbuild.yaml --substitutions _IMAGE=$IMAGE .
```

`deploy/cloudbuild.yaml` builds on an `E2_HIGHCPU_8` machine (8 GB) with 4 parallel jobs, in
about 19 minutes; a new project may not be allowed larger machine types. The build
needs memory for its largest rustc process (about 2.7 GB) and for the console's `npm ci`
(about 3 GB, capped through `NODE_OPTIONS`), which overlaps the first crates. The `Dockerfile`
defaults to 2 jobs, which keeps a local build near 4 GB; pass `--build-arg CARGO_BUILD_JOBS=N`
to go faster on a larger machine. For a local build of the Cloud Run image:
`docker build --platform linux/amd64 --build-arg FEATURES=ui -t $IMAGE . && docker push $IMAGE`,
after `gcloud auth configure-docker $REGION-docker.pkg.dev`.

## 5. Deploy

```sh
gcloud run deploy lakekeeper --project $PROJECT --region $REGION --image $IMAGE \
  --service-account $SA --no-allow-unauthenticated \
  --port 8181 --cpu 1 --memory 1Gi \
  --no-cpu-throttling --min-instances 1 --max-instances 3 \
  --set-env-vars "LAKEKEEPER__FIRESTORE__PROJECT_ID=$PROJECT,LAKEKEEPER__ENABLE_GCP_SYSTEM_CREDENTIALS=true"

gcloud run services add-iam-policy-binding lakekeeper --project $PROJECT --region $REGION \
  --member user:someone@example.com --role roles/run.invoker
```

- `--no-cpu-throttling` and `--min-instances 1`: between requests the server still runs
  the task queues (metadata projection, tabular expiry and purge) and listens to the cache
  change log, which a throttled CPU would stall.
- Several instances are safe: tasks are claimed in transactions, and each instance
  invalidates its caches from the change log that the others write.
- Leave `LAKEKEEPER__BASE_URI` unset. The server then builds the URLs it hands to clients
  from each request: the service URL for direct clients, with `https` (Cloud Run's front end
  sets `X-Forwarded-Proto`), and a forwarded host when one is sent. A fixed base URI would
  send every client to that one address.
- Other settings are upstream Lakekeeper's `LAKEKEEPER__*` variables and the ones in the
  README. Firestore has no schema migrations: a new image only needs `search indexes`
  re-run and deployed if the indexes changed.

## 6. Bootstrap and the first warehouse

```sh
URL=$(gcloud run services describe lakekeeper --project $PROJECT --region $REGION \
  --format 'value(status.url)')
AUTH="Authorization: Bearer $(gcloud auth print-identity-token)"

curl -sS --fail-with-body -X POST "$URL/management/v1/bootstrap" -H "$AUTH" \
  -H 'content-type: application/json' -d '{"accept-terms-of-use": true}'

curl -sS --fail-with-body -X POST "$URL/management/v1/warehouse" -H "$AUTH" \
  -H 'content-type: application/json' -d @- <<EOF
{
  "warehouse-name": "main",
  "storage-profile": {"type": "gcs", "bucket": "$BUCKET", "key-prefix": "warehouse",
                      "sts-enabled": true},
  "storage-credential": {"type": "gcs", "credential-type": "gcp-system-identity"}
}
EOF
```

With `"sts-enabled": true`, clients receive short-lived, downscoped GCS tokens for the
table they load, and need no bucket access of their own. With `false`, clients reach the
bucket with their own credentials.

## 7. Clients

Without authentication of its own, the server ignores the `Authorization` header, so it can
carry the Google ID token that Cloud Run checks:

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "lakekeeper",
    type="rest",
    uri=f"{URL}/catalog",
    warehouse="main",
    token=ID_TOKEN,  # `gcloud auth print-identity-token`, or for a service account
                     # `gcloud auth print-identity-token --audiences=$URL`
)
```

ID tokens last an hour, and clients given a fixed `token` do not renew it. For longer jobs,
or for the console in a browser, which cannot add the header, run a local proxy that adds a
fresh token to each request, and point clients at it:

```sh
gcloud run services proxy lakekeeper --project $PROJECT --region $REGION --port 8181
# Catalog: http://localhost:8181/catalog   Console (ui builds): http://localhost:8181/ui
```

The proxy needs the `cloud-run-proxy` component of the Google Cloud CLI. It sends each
request on with the service's own `Host` and no `X-Forwarded-Host`, so the server builds the
URLs it hands out for the service, not for the proxy (read from the proxy's and Lakekeeper's
source; not run):

- The console takes its API address from the page the server serves, so behind the proxy it
  would call the service directly, without a token, and fail. Set
  `LAKEKEEPER__UI__LAKEKEEPER_URL=http://localhost:8181` on the service for the port the proxy
  listens on (with Terraform, `console_url`). It changes only the console's address, and a
  browser cannot reach the service directly anyway.
- pyiceberg talks to the `uri` it was given, so catalog calls go through the proxy. The
  `gcs.oauth2.refresh-credentials-endpoint` a table load hands out names the service, so a
  client that refreshes its GCS token there does so without the proxy's token.

## With Terraform

`deploy/terraform` creates what steps 1 to 5 do, in one stack: the APIs, the Firestore
database with the indexes, single-field exemptions and TTL policy of a `firestore.indexes.json`
(no Firebase CLI needed), the bucket, the repository, the service's account and roles, a build
account for Cloud Build (new projects no longer give the default compute account the roles a
build needs), and the private service with its invokers. It applies in two phases, as the
image is built in between:

```sh
cat > deploy/terraform/my.tfvars <<VARS
project  = "$PROJECT"
region   = "$REGION"
bucket   = "$BUCKET"
invokers = ["user:someone@example.com"]
VARS
terraform -chdir=deploy/terraform init
terraform -chdir=deploy/terraform apply -var-file=my.tfvars   # everything but the service

REPO=$(terraform -chdir=deploy/terraform output -raw image_repository)
gcloud builds submit --project $PROJECT --region $REGION --config deploy/cloudbuild.yaml \
  --service-account "$(terraform -chdir=deploy/terraform output -raw builder_service_account)" \
  --substitutions _IMAGE=$REPO/lakekeeper-firestore:0.1.0 .

terraform -chdir=deploy/terraform apply -var-file=my.tfvars \
  -var image=$REPO/lakekeeper-firestore:0.1.0
```

Firestore builds each composite index for a few minutes, so the first apply takes about half
an hour. `protect_data = false` lets `terraform destroy` delete the database and the bucket's
objects too; by default they are kept. Destroying leaves the APIs enabled, and the
`PROJECT_cloudbuild` bucket that `gcloud builds submit` creates for its uploads.

## Checked so far

On a sandbox project (2026-11-25), with the Terraform stack, then destroyed:

- The stack applied (68 resources, 34 composite indexes), and the image built on Cloud Build
  with the console in 19 minutes on `E2_HIGHCPU_8`. `E2_HIGHCPU_32` was refused: the new
  project's quota does not allow it.
- Cloud Run refused requests without a token (403). With a user's ID token from
  `gcloud auth print-identity-token`, `/health` reported Firestore and the secret store
  healthy, bootstrap worked, and the console was served at `/ui`.
- A GCS warehouse with `gcp-system-identity` and `"sts-enabled": true` was created: the
  service account reached the bucket, and STS issued a downscoped token during validation.
- pyiceberg 0.10 created a namespace and a table, appended, deleted a row, and a second client
  read the rest back, with the ID token as `token` and the vended GCS token
  (`gcs.oauth2.token`, with a refresh endpoint) for the data. Listings ran on the deployed
  indexes.
- A purge ran as a background task between requests and deleted the table's files within
  10 seconds; the task reported `SUCCESS`.
- The service logged no errors but those provoked on purpose, and no missing-index errors.

Not checked: `gcloud run services proxy` (above, only read from the source); a key refused by the secret store, which
storage validation reaches first with a made-up key; several instances at once on Cloud Run
(on the emulator, `tests/e2e/test_instances.py` checks that their caches follow each other's
changes); a service account's ID token (`--audiences`); and the Firebase CLI route of step 2, which the Terraform
stack replaces.
