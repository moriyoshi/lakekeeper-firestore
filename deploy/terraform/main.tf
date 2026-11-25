# The Cloud Run deployment of DEPLOY.md as one Terraform stack: APIs, the Firestore database
# with its indexes and TTL policy (from ../../firestore.indexes.json), the warehouse bucket,
# the Artifact Registry repository, the service account and its roles, and the Cloud Run
# service. The image is built outside the stack (see ../cloudbuild.yaml); the service is
# created once `image` is set.

terraform {
  required_version = ">= 1.6"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 6.0, < 8.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
}

locals {
  apis = [
    "run.googleapis.com",
    "firestore.googleapis.com",
    "artifactregistry.googleapis.com",
    "cloudbuild.googleapis.com",
    "sts.googleapis.com",
    "iam.googleapis.com",
  ]
  indexes_file = jsondecode(file(var.indexes_file))
  # Keyed by collection and fields, which identify an index.
  indexes = {
    for i in local.indexes_file.indexes :
    join("|", concat([i.collectionGroup], [for f in i.fields : "${f.fieldPath}:${try(f.order, f.arrayConfig)}"])) => i
  }
  field_overrides = {
    for o in local.indexes_file.fieldOverrides : "${o.collectionGroup}.${o.fieldPath}" => o
  }
  service_account = "serviceAccount:${google_service_account.lakekeeper.email}"
}

resource "google_project_service" "apis" {
  for_each = toset(local.apis)
  service  = each.value
  # Leaving an API enabled on destroy is harmless, and disabling one can break other users.
  disable_on_destroy = false
}

# --- Firestore ---------------------------------------------------------------------------

resource "google_firestore_database" "catalog" {
  name                    = var.database_id
  location_id             = var.region
  type                    = "FIRESTORE_NATIVE"
  delete_protection_state = var.protect_data ? "DELETE_PROTECTION_ENABLED" : "DELETE_PROTECTION_DISABLED"
  deletion_policy         = var.protect_data ? "ABANDON" : "DELETE"
  depends_on              = [google_project_service.apis]
}

resource "google_firestore_index" "catalog" {
  for_each    = local.indexes
  database    = google_firestore_database.catalog.name
  collection  = each.value.collectionGroup
  query_scope = each.value.queryScope

  dynamic "fields" {
    for_each = each.value.fields
    content {
      field_path   = fields.value.fieldPath
      order        = try(fields.value.order, null)
      array_config = try(fields.value.arrayConfig, null)
    }
  }
}

# Exemptions from single-field indexing (an empty index_config), and the TTL policy.
resource "google_firestore_field" "catalog" {
  for_each   = local.field_overrides
  database   = google_firestore_database.catalog.name
  collection = each.value.collectionGroup
  field      = each.value.fieldPath

  index_config {}

  dynamic "ttl_config" {
    for_each = try(each.value.ttl, false) ? [1] : []
    content {}
  }
}

# --- Storage, registry, identity ------------------------------------------------------------

resource "google_storage_bucket" "warehouse" {
  name                        = var.bucket
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = !var.protect_data
}

resource "google_artifact_registry_repository" "images" {
  repository_id = var.repository
  location      = var.region
  format        = "DOCKER"
  depends_on    = [google_project_service.apis]
}

resource "google_service_account" "lakekeeper" {
  account_id   = var.service_account_id
  display_name = "Lakekeeper catalog"
  depends_on   = [google_project_service.apis]
}

# Firestore reads, writes, transactions and listeners.
resource "google_project_iam_member" "datastore_user" {
  project = var.project
  role    = "roles/datastore.user"
  member  = local.service_account
}

# The warehouse's objects, and the check object Lakekeeper writes when a warehouse is created.
resource "google_storage_bucket_iam_member" "warehouse_objects" {
  bucket = google_storage_bucket.warehouse.name
  role   = "roles/storage.objectUser"
  member = local.service_account
}

# Cloud Build runs as its own account: new projects no longer give the default compute
# account the roles a build needs. `gcloud builds submit --service-account` names it.
resource "google_service_account" "builder" {
  account_id   = var.builder_account_id
  display_name = "Lakekeeper image builds"
  depends_on   = [google_project_service.apis]
}

resource "google_project_iam_member" "builder" {
  for_each = toset([
    # The uploaded source, in the `PROJECT_cloudbuild` bucket.
    "roles/storage.objectViewer",
    "roles/logging.logWriter",
  ])
  project = var.project
  role    = each.value
  member  = "serviceAccount:${google_service_account.builder.email}"
}

resource "google_artifact_registry_repository_iam_member" "builder" {
  repository = google_artifact_registry_repository.images.name
  location   = var.region
  role       = "roles/artifactregistry.writer"
  member     = "serviceAccount:${google_service_account.builder.email}"
}

# --- The service ------------------------------------------------------------------------------

resource "google_cloud_run_v2_service" "lakekeeper" {
  count               = var.image == "" ? 0 : 1
  name                = var.service_name
  location            = var.region
  ingress             = "INGRESS_TRAFFIC_ALL"
  deletion_protection = false

  template {
    service_account = google_service_account.lakekeeper.email
    # Between requests the server runs the task queues and listens to the cache change log.
    scaling {
      min_instance_count = var.min_instances
      max_instance_count = var.max_instances
    }

    containers {
      image = var.image
      args  = ["serve"]

      ports {
        container_port = 8181
      }

      resources {
        cpu_idle = false
        limits = {
          cpu    = "1"
          memory = "1Gi"
        }
      }

      env {
        name  = "LAKEKEEPER__FIRESTORE__PROJECT_ID"
        value = var.project
      }
      env {
        name  = "LAKEKEEPER__FIRESTORE__DATABASE_ID"
        value = google_firestore_database.catalog.name
      }
      env {
        name  = "LAKEKEEPER__ENABLE_GCP_SYSTEM_CREDENTIALS"
        value = "true"
      }
      dynamic "env" {
        for_each = var.console_url == null ? [] : [var.console_url]
        content {
          name  = "LAKEKEEPER__UI__LAKEKEEPER_URL"
          value = env.value
        }
      }

      startup_probe {
        http_get {
          path = "/health"
          port = 8181
        }
        period_seconds    = 5
        failure_threshold = 24
      }
    }
  }

  depends_on = [
    google_project_iam_member.datastore_user,
    google_firestore_index.catalog,
    google_firestore_field.catalog,
  ]
}

# Cloud Run IAM is the only access control: the catalog runs the allow-all authorizer.
resource "google_cloud_run_v2_service_iam_member" "invokers" {
  for_each = var.image == "" ? toset([]) : toset(var.invokers)
  name     = google_cloud_run_v2_service.lakekeeper[0].name
  location = var.region
  role     = "roles/run.invoker"
  member   = each.value
}
