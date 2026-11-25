output "url" {
  description = "The service's URL; the catalog is at /catalog, the console (ui builds) at /ui."
  value       = var.image == "" ? null : google_cloud_run_v2_service.lakekeeper[0].uri
}

output "image_repository" {
  description = "Where to push the image."
  value       = "${var.region}-docker.pkg.dev/${var.project}/${google_artifact_registry_repository.images.repository_id}"
}

output "service_account" {
  description = "The service's identity, to grant access to further buckets."
  value       = google_service_account.lakekeeper.email
}

output "builder_service_account" {
  description = "For `gcloud builds submit --service-account`."
  value       = google_service_account.builder.id
}
