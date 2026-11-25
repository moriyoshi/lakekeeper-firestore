# Deploying on Cloud Run

A guide for one catalog in one Google Cloud project: a Firestore database, a GCS bucket for
the warehouse, and a Cloud Run service running as its own service account. Nothing here has
been run against a real project yet; the image is checked on the emulators only (see
"Checked so far").

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

# A release build of Lakekeeper is large: give Cloud Build a big machine and an hour.
gcloud builds submit --project $PROJECT --region $REGION \
  --machine-type e2-highcpu-32 --timeout 3600s --tag $IMAGE .
```

The `Dockerfile` builds without the console. For the console at `/ui`, build with
`--build-arg FEATURES=ui`, which Cloud Build's `--tag` cannot pass: use
`docker build --platform linux/amd64 --build-arg FEATURES=ui -t $IMAGE . && docker push $IMAGE`
instead, after `gcloud auth configure-docker $REGION-docker.pkg.dev`.

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
  from each request's `Host` and `X-Forwarded-Proto`. A fixed base URI would send clients
  that come through `gcloud run services proxy` straight to the service, without the token
  the proxy adds.
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

## Checked so far

Nothing in this guide is verified yet. To check next: building the image from this
`Dockerfile` and running it against the Firestore emulator (`/health`, bootstrap, and an
ignored `Authorization` header). Then, on a real project: Cloud Build, the index deploy with
the Firebase CLI, IAM, the Cloud Run flags, ID tokens through the catalog, and STS-vended GCS
tokens.
