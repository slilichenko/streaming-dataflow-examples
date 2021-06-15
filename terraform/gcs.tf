resource "google_storage_bucket" "data-generator-template" {
  name = "${var.project_id}-data-generator-template"
  uniform_bucket_level_access = true
}

resource "google_storage_bucket_object" "event-generator-template" {
  name = "event-generator-template.json"
  bucket = google_storage_bucket.data-generator-template.name
  source = "../data-generator/event-generator-template.json"
}

output "event-generator-template" {
  value = "gs://${google_storage_bucket_object.event-generator-template.bucket}/${google_storage_bucket_object.event-generator-template.name}"
}

resource "google_storage_bucket" "dataflow-temp" {
  name = "${var.project_id}-dataflow-temp"
  uniform_bucket_level_access = true
}

output "dataflow-temp-bucket" {
  value = google_storage_bucket.dataflow-temp.id
}
