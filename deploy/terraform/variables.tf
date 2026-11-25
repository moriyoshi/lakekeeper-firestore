variable "project" {
  description = "Google Cloud project."
  type        = string
}

variable "region" {
  description = "Location of Firestore, the bucket, the repository and the service."
  type        = string
}

variable "bucket" {
  description = "Name of the warehouse bucket (globally unique)."
  type        = string
}

variable "image" {
  description = "Image of the server; empty to create everything but the service, before the first build."
  type        = string
  default     = ""
}

variable "invokers" {
  description = "Members allowed to call the service, such as `user:someone@example.com`."
  type        = list(string)
  default     = []
}

variable "database_id" {
  description = "Firestore database. A named one needs indexes for it, which this stack creates either way."
  type        = string
  default     = "(default)"
}

variable "indexes_file" {
  description = "Output of `lakekeeper-firestore search indexes` for the collection prefix in use."
  type        = string
  default     = "../../firestore.indexes.json"
}

variable "repository" {
  description = "Artifact Registry repository for the image."
  type        = string
  default     = "lakekeeper"
}

variable "service_account_id" {
  description = "Account ID of the service's identity."
  type        = string
  default     = "lakekeeper"
}

variable "builder_account_id" {
  description = "Account ID of the identity Cloud Build runs as."
  type        = string
  default     = "lakekeeper-builder"
}

variable "service_name" {
  description = "Cloud Run service."
  type        = string
  default     = "lakekeeper"
}

variable "min_instances" {
  description = "At least 1 keeps the task queues and the cache change log running between requests."
  type        = number
  default     = 1
}

variable "max_instances" {
  description = "Several instances are safe: tasks are claimed in transactions, caches follow the change log."
  type        = number
  default     = 3
}

variable "protect_data" {
  description = "Keep the database and the bucket's objects when the stack is destroyed."
  type        = bool
  default     = true
}

variable "console_url" {
  description = "Where the console sends its API calls, such as http://localhost:8181 for `gcloud run services proxy`; unset, the service's own URL."
  type        = string
  default     = null
}
